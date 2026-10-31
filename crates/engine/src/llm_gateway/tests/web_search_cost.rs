use super::*;
use model::{
    ProviderSearchAction, ProviderSearchEvent, ProviderWebSearch, ProviderWebSearchOutcome,
    UrlCitation,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default, Clone, Copy)]
enum StreamTerminal {
    #[default]
    End,
    Retryable,
    ChannelEnd,
    Cancel,
}

/// A provider whose response carries a hosted web search: the calls it
/// made, one of them with its action, and the citation it attached.
#[derive(Default)]
struct SearchProvider {
    calls: Option<u64>,
    omit_usage: bool,
    reject_response: bool,
    unsafe_search_field: Option<&'static str>,
    requests: AtomicUsize,
    reject_delta: bool,
    omit_completion: bool,
    stream_terminal: StreamTerminal,
    cancellation: CancellationToken,
}

#[async_trait]
impl LlmProvider for SearchProvider {
    fn id(&self) -> &str {
        "search-metered"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            streaming: true,
            web_search: true,
            ..Capabilities::default()
        }
    }
    fn stream_retry_attempts(&self) -> usize {
        3
    }
    fn retry_base_backoff(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }
    async fn complete(&self, request: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let mut web_search = request
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
        if let Some(outcome) = &mut web_search {
            match self.unsafe_search_field {
                Some("events") => outcome.events[0].queries.push("secret-value".into()),
                Some("citations") => outcome.citations[0].title = Some("secret-value".into()),
                _ => {}
            }
        }
        Ok(ChatResponse {
            content: vec![ChatContent::Text(
                if self.reject_response && !request.stream {
                    "secret-value"
                } else {
                    "ok"
                }
                .into(),
            )],
            usage: TokenUsage {
                input: 100,
                output: 10,
                ..Default::default()
            },
            stop: StopReason::EndTurn,
            provider_state: (self.reject_response && request.stream)
                .then(|| json!({"opaque": "secret-value"})),
            web_search,
        })
    }

    async fn stream(
        &self,
        request: ChatRequest,
        events: tokio::sync::mpsc::Sender<llm::ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        let response = self.complete(request).await?;
        let deltas: &[&str] = if self.reject_delta {
            &["sec", "ret-value", "unsafe after rejection"]
        } else {
            &["verified prefix; held tail"]
        };
        for text in deltas {
            events
                .send(llm::ChatStreamEvent::TextDelta {
                    text: (*text).into(),
                })
                .await
                .map_err(|_| LlmError::new("closed"))?;
        }
        if self.reject_delta {
            // Let the gateway latch rejection before this operation continues.
            tokio::task::yield_now().await;
        }
        if !self.omit_completion {
            events
                .send(llm::ChatStreamEvent::Completed {
                    response: response.clone(),
                })
                .await
                .map_err(|_| LlmError::new("closed"))?;
            if self.reject_delta {
                // A queued duplicate must not charge usage twice.
                events
                    .send(llm::ChatStreamEvent::Completed { response })
                    .await
                    .map_err(|_| LlmError::new("closed"))?;
            }
        }
        match self.stream_terminal {
            StreamTerminal::End => {}
            StreamTerminal::Retryable => {
                return Err(LlmError {
                    message: "retryable failure after deltas".into(),
                    kind: llm::LlmErrorKind::HttpStatus(503),
                    ..Default::default()
                });
            }
            StreamTerminal::ChannelEnd => {
                drop(events);
                return std::future::pending().await;
            }
            StreamTerminal::Cancel => {
                self.cancellation.cancel();
                return std::future::pending().await;
            }
        }
        Ok(())
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

/// Rejected payloads must not discard a completed call's billable usage.
/// Exercise both the ordinary response scan and the stream's final gate;
/// Incremental TextDelta rejection is covered separately below.
#[tokio::test]
async fn secret_rejected_completions_account_safely_and_block_unresolved_search_replay() {
    for stream in [false, true] {
        for (calls, price, omit_usage) in [
            (None, Some(0.5), false),
            (None, Some(0.5), true),
            (Some(2), None, false),
            (Some(2), Some(0.5), false),
        ] {
            for unsafe_search_field in [None, Some("events"), Some("citations")] {
                let dir = test_dir("search-secret-rejected");
                let run_id = format!("search-secret-rejected-{}", uuid::Uuid::now_v7());
                let journal = test_journal(&dir, &run_id);
                let secrets = test_secret_store();
                let provider = Arc::new(SearchProvider {
                    calls,
                    omit_usage,
                    reject_response: true,
                    unsafe_search_field,
                    ..Default::default()
                });
                let mut gateway = test_gateway(provider.clone(), &secrets, &journal);
                gateway.budget.require_pricing = true;
                let mut request = search_request();
                request.stream = stream;
                let error = gateway
                    .complete(&test_node(), request, &[search_pricing(price)], |_| {
                        json!({})
                    })
                    .await
                    .expect_err("a secret payload must fail even with known search pricing");
                assert!(error.to_string().contains("secret `token`"), "{error}");
                let text = persisted_text(&dir);
                assert!(!text.contains("secret-value"));
                let events: Vec<serde_json::Value> = text
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                assert_eq!(
                    events
                        .iter()
                        .filter(|event| event["t"] == "llm_call")
                        .count(),
                    1
                );
                assert!(!events.iter().any(|event| event["t"] == "llm_route_failed"));
                let event = llm_call_event(&dir);
                assert!(event.get("content").is_none());
                assert!(event.get("provider_state").is_none());
                let search_cost = calls
                    .zip(price)
                    .map(|(n, p)| (n as f64 * p * 1_000_000.0) as u64);
                assert_eq!(
                    event["cost_microusd"],
                    110 + search_cost.unwrap_or_default()
                );
                assert_eq!(event["web_search"]["calls"], json!(calls));
                assert_eq!(event["web_search"]["cost_microusd"], json!(search_cost));
                for field in ["events", "citations"] {
                    let value = event["web_search"].get(field);
                    if !omit_usage && unsafe_search_field == Some(field) {
                        assert!(value.is_none(), "unsafe {field} must be omitted");
                    } else {
                        assert!(value.unwrap().is_array(), "safe {field} must be retained");
                    }
                }
                let deltas: String = events
                    .iter()
                    .filter(|event| event["t"] == "llm_delta")
                    .map(|event| event["text"].as_str().unwrap())
                    .collect();
                if stream {
                    let text = "verified prefix; held tail";
                    let holdback = secrets.max_value_len() - 1;
                    assert_eq!(deltas, &text[..text.len() - holdback]);
                } else {
                    assert!(deltas.is_empty());
                }
                let replay = crate::RunState::fold_journal(&dir.join("journal.jsonl")).unwrap();
                for state in [journal.state(), replay] {
                    assert_eq!(state.budget.unresolved_search_cost, search_cost.is_none());
                    assert_eq!(
                        (
                            state.budget.tokens_input,
                            state.budget.tokens_output,
                            state.budget.cost_microusd,
                            state.budget.llm_calls
                        ),
                        (100, 10, 110 + search_cost.unwrap_or_default(), 1)
                    );
                }
                drop(gateway);
                drop(journal);
                if search_cost.is_none() {
                    let resumed = test_journal(&dir, &run_id);
                    let mut gateway = test_gateway(provider.clone(), &secrets, &resumed);
                    gateway.budget.require_pricing = true;
                    let mut request = search_request();
                    request.provider_web_search = None;
                    let error = gateway
                        .complete(&test_node(), request, &[search_pricing(Some(0.5))], |_| {
                            json!({})
                        })
                        .await
                        .unwrap_err();
                    assert!(error.to_string().contains("unresolved hosted search cost"));
                    assert_eq!(resumed.state().budget.llm_calls, 1);
                }
                assert_eq!(provider.requests.load(Ordering::SeqCst), 1);
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }
}

#[tokio::test]
async fn rejected_delta_drains_completion_and_accounts_once_without_publication() {
    for terminal in [
        StreamTerminal::End,
        StreamTerminal::Retryable,
        StreamTerminal::ChannelEnd,
        StreamTerminal::Cancel,
    ] {
        for unsafe_completion in [false, true] {
            let dir = test_dir("delta-rejected-completed");
            let journal = test_journal(&dir, "delta-rejected-completed");
            let secrets = test_secret_store();
            let provider = Arc::new(SearchProvider {
                calls: Some(2),
                reject_delta: true,
                reject_response: unsafe_completion,
                stream_terminal: terminal,
                ..Default::default()
            });
            let mut gateway = test_gateway(provider.clone(), &secrets, &journal);
            gateway.cancellation = provider.cancellation.clone();
            gateway.budget.require_pricing = true;
            let mut request = search_request();
            request.stream = true;
            let error = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                gateway.complete(
                    &test_node(),
                    request,
                    &[search_pricing(Some(0.5)), search_pricing(Some(0.5))],
                    |_| json!({}),
                ),
            )
            .await
            .expect("terminal stream must settle")
            .unwrap_err();
            if matches!(terminal, StreamTerminal::Cancel) {
                assert!(
                    matches!(error, StepError::Cancelled),
                    "{terminal:?}: cancellation status lost"
                );
            } else {
                assert!(
                    error.to_string().contains("secret `token`"),
                    "{terminal:?}: {error}"
                );
            }
            assert_eq!(provider.requests.load(Ordering::SeqCst), 1);
            let text = persisted_text(&dir);
            assert!(!text.contains("secret-value"));
            let events: Vec<serde_json::Value> = text
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert!(
                !events
                    .iter()
                    .any(|e| e["t"] == "llm_delta" || e["t"] == "llm_route_failed")
            );
            assert_eq!(events.iter().filter(|e| e["t"] == "llm_call").count(), 1);
            let call = llm_call_event(&dir);
            assert_eq!(call["tokens"]["input"], 100);
            assert_eq!(call["tokens"]["output"], 10);
            assert_eq!(call["cost_microusd"], 1_000_110);
            assert_eq!(call["web_search"]["calls"], 2);
            assert_eq!(call["web_search"]["cost_microusd"], 1_000_000);
            assert_eq!(call["web_search"]["events"][0]["action"], "search");
            assert!(call.get("content").is_none() && call.get("provider_state").is_none());
            for state in [
                journal.state(),
                crate::RunState::fold_journal(&dir.join("journal.jsonl")).unwrap(),
            ] {
                assert!(!state.budget.unresolved_search_cost);
                assert_eq!(state.budget.llm_calls, 1);
                assert_eq!(state.budget.cost_microusd, 1_000_110);
            }
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

#[tokio::test]
async fn rejected_delta_without_completion_records_search_liability_and_blocks_replay() {
    for terminal in [
        StreamTerminal::End,
        StreamTerminal::Retryable,
        StreamTerminal::ChannelEnd,
        StreamTerminal::Cancel,
    ] {
        for search_enabled in [false, true] {
            let dir = test_dir("delta-rejected-unsettled");
            let run_id = format!("delta-rejected-{}", uuid::Uuid::now_v7());
            let journal = JournalWriter::create_with_policies(
                &dir.join("journal.jsonl"),
                &run_id,
                false,
                None,
                crate::JournalLimits::default(),
                policy::AuditPolicy {
                    default_mode: if search_enabled {
                        policy::AuditMode::Off
                    } else {
                        policy::AuditMode::Full
                    },
                    ..Default::default()
                },
                policy::AuditLimits::default(),
            )
            .unwrap();
            let secrets = test_secret_store();
            let provider = Arc::new(SearchProvider {
                reject_delta: true,
                omit_completion: true,
                stream_terminal: terminal,
                ..Default::default()
            });
            let routed: Arc<dyn LlmProvider> = if matches!(terminal, StreamTerminal::Retryable) {
                let mut router = llm::LlmRouter::parse_text("").unwrap();
                router.register(provider.clone());
                Arc::new(router)
            } else {
                provider.clone()
            };
            let mut gateway = test_gateway(routed, &secrets, &journal);
            gateway.cancellation = provider.cancellation.clone();
            gateway.budget.require_pricing = true;
            let mut request = search_request();
            request.stream = true;
            if !search_enabled {
                request.provider_web_search = None;
            }
            let error = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                gateway.complete(
                    &test_node(),
                    request,
                    &[search_pricing(Some(0.5)), search_pricing(Some(0.5))],
                    |_| panic!("no completed call"),
                ),
            )
            .await
            .expect("terminal stream must settle")
            .unwrap_err();
            if matches!(terminal, StreamTerminal::Cancel) {
                assert!(
                    matches!(error, StepError::Cancelled),
                    "{terminal:?}: cancellation status lost"
                );
            } else {
                assert!(
                    error.to_string().contains("secret `token`"),
                    "{terminal:?}: {error}"
                );
            }
            assert_eq!(
                provider.requests.load(Ordering::SeqCst),
                1,
                "must not retry or fall back"
            );
            let text = persisted_text(&dir);
            assert!(!text.contains("secret-value"));
            let events: Vec<serde_json::Value> = text
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert!(
                !events
                    .iter()
                    .any(|e| e["t"] == "llm_delta" || e["t"] == "llm_call")
            );
            let failures: Vec<_> = events
                .iter()
                .filter(|e| e["t"] == "llm_route_failed")
                .collect();
            assert_eq!(failures.len(), 1);
            let failure = failures[0];
            assert_eq!(failure["kind"], "partial_stream");
            assert_eq!(failure["fallback_taken"], false);
            assert_eq!(
                failure["retry_decisions"]
                    .as_array()
                    .unwrap()
                    .last()
                    .unwrap()["action"],
                "fail_fast"
            );
            if search_enabled {
                assert_eq!(
                    failure["web_search"],
                    json!({"calls": null, "cost_microusd": null})
                );
            } else {
                assert!(failure.get("web_search").is_none());
            }
            for state in [
                journal.state(),
                crate::RunState::fold_journal(&dir.join("journal.jsonl")).unwrap(),
            ] {
                assert_eq!(
                    state.budget.unresolved_search_cost, search_enabled,
                    "{terminal:?}: seq={} text={text}",
                    state.last_seq
                );
                assert_eq!(
                    (
                        state.budget.llm_calls,
                        state.budget.tokens_input,
                        state.budget.tokens_output,
                        state.budget.cost_microusd
                    ),
                    (0, 0, 0, 0)
                );
            }
            if search_enabled {
                let mut state = crate::RunState::fold_journal(&dir.join("journal.jsonl")).unwrap();
                let mut settled = json!({"t": "llm_call", "tokens": TokenUsage::default(), "cost_microusd": 0,
                    "web_search": {"calls": 0, "cost_microusd": 0}});
                settled["seq"] = json!(state.last_seq + 1);
                state.apply(&settled).unwrap();
                assert!(
                    state.budget.unresolved_search_cost,
                    "later known-zero usage cannot settle the old liability"
                );
            }
            drop(gateway);
            drop(journal);
            if search_enabled {
                let resumed = test_journal(&dir, &run_id);
                let mut gateway = test_gateway(provider.clone(), &secrets, &resumed);
                gateway.budget.require_pricing = true;
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
                assert!(resumed.state().budget.unresolved_search_cost);
            }
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
