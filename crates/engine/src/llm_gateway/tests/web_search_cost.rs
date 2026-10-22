use super::*;
use model::{ProviderSearchAction, ProviderSearchEvent, ProviderWebSearchOutcome, UrlCitation};

/// A provider whose response carries a hosted web search: the calls it
/// made, one of them with its action, and the citation it attached.
struct SearchProvider;

#[async_trait]
impl LlmProvider for SearchProvider {
    fn id(&self) -> &str {
        "search-metered"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, _: ChatRequest) -> Result<ChatResponse, LlmError> {
        Ok(ChatResponse {
            content: vec![ChatContent::Text("ok".into())],
            usage: TokenUsage {
                input: 100,
                output: 10,
                ..Default::default()
            },
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search: Some(ProviderWebSearchOutcome {
                calls: Some(2),
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
            }),
        })
    }
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
        Arc::new(SearchProvider),
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
    let mut request = test_request();
    request.provider = "search-metered".into();
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
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn an_unpriced_hosted_search_is_unmetered_not_free() {
    let dir = test_dir("search-unpriced");
    let journal = test_journal(&dir, &format!("search-unpriced-{}", uuid::Uuid::now_v7()));
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(SearchProvider),
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

#[tokio::test]
async fn a_run_that_demands_pricing_refuses_an_unpriced_hosted_search() {
    let dir = test_dir("search-refused");
    let journal = test_journal(&dir, &format!("search-refused-{}", uuid::Uuid::now_v7()));
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(SearchProvider),
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
    let mut request = test_request();
    request.provider = "search-metered".into();
    let error = gateway
        .complete(
            &test_node(),
            request,
            &[search_pricing(None)],
            |_| json!({}),
        )
        .await
        .expect_err("a run that demands pricing cannot settle an unpriced hosted search");
    let message = error.to_string();
    assert!(
        message.contains("web_search_call_cost_usd"),
        "the refusal must name the missing price: {message}"
    );
    assert!(message.contains("search-metered/test"), "{message}");
    let _ = std::fs::remove_dir_all(dir);
}
