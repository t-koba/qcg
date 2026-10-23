use super::*;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};

struct ContextExhaustedProvider {
    calls: AtomicUsize,
    input: u64,
    content: Vec<ChatContent>,
}

#[async_trait]
impl LlmProvider for ContextExhaustedProvider {
    fn id(&self) -> &str {
        "exhausted"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, request: ChatRequest) -> Result<ChatResponse, LlmError> {
        assert_eq!(request.provider, "exhausted", "fallback must not run");
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ChatResponse {
            content: self.content.clone(),
            usage: TokenUsage {
                input: self.input,
                output: 3,
                cached_input: 4,
                cache_creation_input: 2,
                reasoning: 0,
            },
            stop: StopReason::ModelContextWindowExceeded,
            provider_state: None,
            web_search: None,
        })
    }
    async fn stream(
        &self,
        request: ChatRequest,
        events: tokio::sync::mpsc::Sender<llm::ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        events
            .send(llm::ChatStreamEvent::Completed {
                response: self.complete(request).await?,
            })
            .await
            .unwrap();
        Ok(())
    }
}

#[tokio::test]
async fn context_exhaustion_accounts_once_without_fallback_or_persisting_partial_content() {
    for stream in [false, true] {
        for content in [
            vec![],
            vec![ChatContent::Text("secret-value".into())],
            vec![ChatContent::ToolCall {
                id: "t1".into(),
                name: "write_file".into(),
                args: json!({"content":"secret-value"}),
            }],
        ] {
            let dir = test_dir("context-exhaustion");
            let journal = test_journal(&dir, "context-exhaustion-test");
            let secrets = test_secret_store();
            let provider = Arc::new(ContextExhaustedProvider {
                calls: AtomicUsize::new(0),
                input: 16,
                content,
            });
            let mut routes = test_routes(&[("exhausted", "test"), ("fallback", "test")]);
            routes[0].input_cost_per_million_usd = Some(2.0);
            routes[0].cache_read_input_cost_per_million_usd = Some(0.5);
            routes[0].cache_creation_input_cost_per_million_usd = Some(3.0);
            routes[0].output_cost_per_million_usd = Some(5.0);
            let gateway = test_gateway(provider.clone(), &secrets, &journal);
            let mut request = test_request();
            request.provider = "exhausted".into();
            request.stream = stream;
            let error = gateway
                .complete(&test_node(), request, &routes, |_| json!({}))
                .await
                .unwrap_err();
            assert!(matches!(error, StepError::ProviderContextExhausted { .. }));
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
            let state = journal.state();
            assert_eq!(
                (
                    state.budget.tokens_input,
                    state.budget.tokens_output,
                    state.budget.llm_calls,
                    state.budget.cost_microusd
                ),
                (16, 3, 1, 43)
            );
            let text = persisted_text(&dir);
            assert!(!text.contains("secret-value"));
            let events: Vec<Value> = text
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let calls: Vec<_> = events.iter().filter(|e| e["t"] == "llm_call").collect();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0]["stop_reason"], "model_context_window_exceeded");
            assert_eq!(calls[0]["cost_microusd"], 43);
            assert!(!events.iter().any(|e| e["t"] == "llm_route_failed"));
            // The exact same durable event settles to exactly the same usage on replay.
            let replay = crate::RunState::fold_journal(&dir.join("journal.jsonl")).unwrap();
            assert_eq!(replay.budget.llm_calls, 1);
            assert_eq!(replay.budget.cost_microusd, 43);
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }
}

#[tokio::test]
async fn context_exhaustion_still_enforces_reported_run_budget_after_accounting() {
    let dir = test_dir("context-budget");
    let journal = test_journal(&dir, "context-budget");
    let secrets = test_secret_store();
    let provider = Arc::new(ContextExhaustedProvider {
        calls: AtomicUsize::new(0),
        input: 10000,
        content: vec![],
    });
    let mut gateway = test_gateway(provider, &secrets, &journal);
    gateway.budget.max_tokens = Some(5000);
    let mut request = test_request();
    request.provider = "exhausted".into();
    let routes = test_routes(&[("exhausted", "test")]);
    // Reported usage exceeds estimated admission and must still enforce the cap.
    let error = gateway
        .complete(&test_node(), request, &routes, |_| json!({}))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StepError::BudgetExceeded {
            resource: "tokens",
            used: 10003,
            limit: 5000
        }
    ));
    assert_eq!(journal.state().budget.llm_calls, 1);
    let text = persisted_text(&dir);
    assert!(text.contains("model_context_window_exceeded"));
    std::fs::remove_dir_all(dir).unwrap();
}
