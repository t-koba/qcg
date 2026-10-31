use super::*;
use model::{
    ProviderSearchAction, ProviderSearchEvent, ProviderWebSearch, ProviderWebSearchOutcome,
    UrlCitation,
};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A provider whose response carries a hosted web search: the calls it
/// made, one of them with its action, and the citation it attached.
#[derive(Default)]
struct SearchProvider {
    calls: Option<u64>,
    omit_usage: bool,
    requests: AtomicUsize,
}

#[async_trait]
impl LlmProvider for SearchProvider {
    fn id(&self) -> &str {
        "search-metered"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, request: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let web_search = request
            .provider_web_search
            .filter(|_| !self.omit_usage)
            .map(|_| ProviderWebSearchOutcome {
                calls: self.calls,
                events: vec![ProviderSearchEvent {
                    id: Some("ws_1".into()),
                    action: ProviderSearchAction::Search,
                    queries: vec!["qcg".into()],
                    url: None,
                }],
                citations: vec![UrlCitation {
                    url: "https://example.com/a".into(),
                    title: Some("Example".into()),
                    start_index: Some(0),
                    end_index: Some(6),
                }],
            });
        Ok(ChatResponse {
            content: vec![ChatContent::Text("ok".into())],
            usage: TokenUsage {
                input: 100,
                output: 10,
                ..Default::default()
            },
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search,
        })
    }
}

fn search_request() -> ChatRequest {
    let mut request = test_request();
    request.provider = "search-metered".into();
    request.provider_web_search = Some(ProviderWebSearch::default());
    request
}

fn search_pricing(price: Option<f64>) -> ModelRef {
    ModelRef {
        provider: "search-metered".into(),
        model: "test".into(),
        input_cost_per_million_usd: Some(1.0),
        output_cost_per_million_usd: Some(1.0),
        cache_read_input_cost_per_million_usd: None,
        cache_creation_input_cost_per_million_usd: None,
        web_search_call_cost_usd: price,
        cost_tiers: Vec::new(),
    }
}

/// The journaled `llm_call` event of the run.
fn llm_call_event(dir: &Utf8PathBuf) -> serde_json::Value {
    persisted_text(dir)
        .lines()
        .map(|line| serde_json::from_str(line).expect("journal line should be valid JSON"))
        .find(|event: &serde_json::Value| event["t"] == "llm_call")
        .expect("the run should journal an llm_call event")
}

#[tokio::test]
async fn a_priced_hosted_search_is_charged_per_call() {
    let dir = test_dir("search-priced");
    let journal = test_journal(&dir, &format!("search-priced-{}", uuid::Uuid::now_v7()));
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(SearchProvider {
            calls: Some(2),
            ..Default::default()
        }),
        &secrets,
        &journal,
        CancellationToken::new(),
        LlmCostBudget {
            max_tokens: None,
            max_cost_microusd: None,
            require_pricing: true,
        },
        vec![],
    );
    let request = search_request();
    gateway
        .complete(&test_node(), request, &[search_pricing(Some(0.5))], |_| {
            json!({})
        })
        .await
        .expect("a priced hosted search must settle");
    let event = llm_call_event(&dir);
    assert_eq!(event["web_search"]["calls"], json!(2));
    assert_eq!(event["web_search"]["cost_microusd"], json!(1_000_000));
    assert_eq!(event["web_search"]["events"][0]["action"], json!("search"));
    assert_eq!(
        event["web_search"]["citations"][0]["url"],
        json!("https://example.com/a")
    );
    // The two hosted calls at half a US cent each are part of the call's
    // cost, not a separate figure the run can ignore.
    assert_eq!(event["cost_microusd"], json!(1_000_000 + 110));
    assert!(!journal.state().budget.unresolved_search_cost);
    assert!(
        !crate::RunState::fold_journal(&dir.join("journal.jsonl"))
            .unwrap()
            .budget
            .unresolved_search_cost
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn a_known_zero_hosted_search_has_zero_cost_without_a_rate() {
    let dir = test_dir("search-zero");
    let journal = test_journal(&dir, &format!("search-zero-{}", uuid::Uuid::now_v7()));
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(SearchProvider {
            calls: Some(0),
            ..Default::default()
        }),
        &secrets,
        &journal,
        CancellationToken::new(),
        LlmCostBudget {
            max_tokens: None,
            max_cost_microusd: None,
            require_pricing: true,
        },
        vec![],
    );
    let request = search_request();
    gateway
        .complete(
            &test_node(),
            request,
            &[search_pricing(None)],
            |_| json!({}),
        )
        .await
        .expect("a zero billable-unit count needs no search price");
    let event = llm_call_event(&dir);
    assert_eq!(event["web_search"]["calls"], json!(0));
    assert_eq!(event["web_search"]["cost_microusd"], json!(0));
    assert_eq!(event["cost_microusd"], json!(110));
    assert!(!journal.state().budget.unresolved_search_cost);
    assert!(
        !crate::RunState::fold_journal(&dir.join("journal.jsonl"))
            .unwrap()
            .budget
            .unresolved_search_cost
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn unresolved_search_is_journaled_before_failure_and_blocks_required_pricing_after_replay() {
    for (calls, price, omit_usage) in [
        (None, Some(0.5), false),
        (None, Some(0.5), true),
        (Some(2), None, false),
    ] {
        for max_cost_microusd in [None, Some(1_000_000)] {
            for require_pricing in [false, true] {
                let dir = test_dir("search-unresolved");
                let run_id = format!("search-unresolved-{}", uuid::Uuid::now_v7());
                let journal = test_journal(&dir, &run_id);
                let secrets = SecretStore::default();
                let provider = Arc::new(SearchProvider {
                    calls,
                    omit_usage,
                    ..Default::default()
                });
                let budget = LlmCostBudget {
                    max_tokens: None,
                    max_cost_microusd,
                    require_pricing,
                };
                let gateway = LlmGateway::new(
                    provider.clone(),
                    &secrets,
                    &journal,
                    CancellationToken::new(),
                    budget,
                    vec![],
                );
                let result = gateway
                    .complete(
                        &test_node(),
                        search_request(),
                        &[search_pricing(price)],
                        |_| json!({}),
                    )
                    .await;
                if require_pricing {
                    let message = result.unwrap_err().to_string();
                    let detail = if calls.is_none() {
                        "unknown billable hosted search usage"
                    } else {
                        "web_search_call_cost_usd"
                    };
                    assert!(message.contains(detail), "{message}");
                    assert!(message.contains("search-metered/test"), "{message}");
                } else {
                    result.expect("optional pricing retains the priced portion");
                }
                let event = llm_call_event(&dir);
                assert_eq!(event["tokens"]["input"], 100);
                assert_eq!(event["tokens"]["output"], 10);
                assert_eq!(event["cost_microusd"], 110);
                assert_eq!(event["web_search"]["calls"], json!(calls));
                assert!(event["web_search"]["cost_microusd"].is_null());
                if omit_usage {
                    assert_eq!(event["web_search"]["events"], json!([]));
                    assert_eq!(event["web_search"]["citations"], json!([]));
                } else {
                    assert_eq!(event["web_search"]["events"][0]["action"], "search");
                    assert_eq!(event["web_search"]["events"][0]["queries"], json!(["qcg"]));
                    assert_eq!(
                        event["web_search"]["citations"][0]["url"],
                        "https://example.com/a"
                    );
                }
                let replay = crate::RunState::fold_journal(&dir.join("journal.jsonl")).unwrap();
                assert!(replay.budget.unresolved_search_cost);
                assert_eq!(
                    (
                        replay.budget.tokens_input,
                        replay.budget.tokens_output,
                        replay.budget.cost_microusd,
                        replay.budget.llm_calls
                    ),
                    (100, 10, 110, 1)
                );
                let saved: crate::RunState =
                    serde_json::from_str(&std::fs::read_to_string(dir.join("state.json")).unwrap())
                        .unwrap();
                assert!(saved.budget.unresolved_search_cost);
                assert!(journal.state().budget.unresolved_search_cost);
                drop(gateway);
                drop(journal);
                let resumed = test_journal(&dir, &run_id);
                let gateway = LlmGateway::new(
                    provider.clone(),
                    &secrets,
                    &resumed,
                    CancellationToken::new(),
                    LlmCostBudget {
                        max_tokens: None,
                        max_cost_microusd,
                        require_pricing: true,
                    },
                    vec![],
                );
                // Even disabling search and supplying its price cannot settle the old charge.
                let mut request = search_request();
                request.provider_web_search = None;
                let error = gateway
                    .complete(&test_node(), request, &[search_pricing(Some(0.5))], |_| {
                        json!({})
                    })
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("unresolved hosted search cost"));
                assert_eq!(provider.requests.load(Ordering::SeqCst), 1);
                assert_eq!(resumed.state().budget.llm_calls, 1);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn required_pricing_does_not_require_hosted_search_when_none_was_requested() {
    let dir = test_dir("search-not-requested");
    let journal = test_journal(
        &dir,
        &format!("search-not-requested-{}", uuid::Uuid::now_v7()),
    );
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(SearchProvider {
            calls: None,
            ..Default::default()
        }),
        &secrets,
        &journal,
        CancellationToken::new(),
        LlmCostBudget {
            max_tokens: None,
            max_cost_microusd: Some(1_000_000),
            require_pricing: true,
        },
        vec![],
    );
    let mut request = test_request();
    request.provider = "search-metered".into();
    gateway
        .complete(
            &test_node(),
            request,
            &[search_pricing(None)],
            |_| json!({}),
        )
        .await
        .expect("an absent search request has no hosted usage to price");
    let event = llm_call_event(&dir);
    assert!(event.get("web_search").is_none());
    assert_eq!(event["cost_microusd"], json!(110));
    assert!(!journal.state().budget.unresolved_search_cost);
    assert!(
        !crate::RunState::fold_journal(&dir.join("journal.jsonl"))
            .unwrap()
            .budget
            .unresolved_search_cost
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn an_unpriced_hosted_search_is_unmetered_not_free() {
    let dir = test_dir("search-unpriced");
    let journal = test_journal(&dir, &format!("search-unpriced-{}", uuid::Uuid::now_v7()));
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(SearchProvider {
            calls: Some(2),
            ..Default::default()
        }),
        &secrets,
        &journal,
        CancellationToken::new(),
        LlmCostBudget {
            max_tokens: None,
            max_cost_microusd: None,
            require_pricing: false,
        },
        vec![],
    );
    let request = search_request();
    gateway
        .complete(
            &test_node(),
            request,
            &[search_pricing(None)],
            |_| json!({}),
        )
        .await
        .expect("an unpriced hosted search still runs when the run does not demand pricing");
    let event = llm_call_event(&dir);
    assert_eq!(
        event["web_search"]["calls"],
        json!(2),
        "the count is derived from the provider's report"
    );
    assert!(
        event["web_search"]["cost_microusd"].is_null(),
        "an unpriced search is unmetered, never free: {event}"
    );
    let _ = std::fs::remove_dir_all(dir);
}
