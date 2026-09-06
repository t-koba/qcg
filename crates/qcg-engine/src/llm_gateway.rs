mod complete;
mod cost;
mod merge;
mod scan;
mod types;

pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JournalWriter, SecretStore, StepError};
    use async_trait::async_trait;
    use camino::Utf8PathBuf;
    use qcg_contract::{ModelRef, NodeDef, OnDeps, StepType};
    use qcg_llm::{Capabilities, ChatMessage, LlmErrorKind, StopReason, TokenUsage, ToolSpec};
    use qcg_llm::{ChatContent, ChatRequest, ChatResponse, LlmError, LlmProvider};
    use qcg_policy::LlmCostBudget;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    struct LeakingProvider;

    struct MeteredProvider;

    struct ToolLeakingProvider;

    struct RouteProvider;

    #[async_trait]
    impl LlmProvider for LeakingProvider {
        fn id(&self) -> &str {
            "leak"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }

        async fn complete(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
            Ok(ChatResponse {
                content: vec![qcg_llm::ChatContent::Text("secret-value".into())],
                usage: TokenUsage {
                    input: 0,
                    output: 1,
                    reasoning: 0,
                    cached_input: 0,
                },
                stop: StopReason::EndTurn,
                provider_state: None,
            })
        }
    }

    #[async_trait]
    impl LlmProvider for MeteredProvider {
        fn id(&self) -> &str {
            "metered"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }

        async fn complete(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
            Ok(ChatResponse {
                content: vec![qcg_llm::ChatContent::Text("safe response".into())],
                usage: TokenUsage {
                    input: 1_000_001,
                    output: 0,
                    reasoning: 0,
                    cached_input: 0,
                },
                stop: StopReason::EndTurn,
                provider_state: None,
            })
        }
    }

    #[async_trait]
    impl LlmProvider for ToolLeakingProvider {
        fn id(&self) -> &str {
            "tool-leak"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }

        async fn complete(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
            Ok(ChatResponse {
                content: vec![ChatContent::ToolCall {
                    id: "call-1".into(),
                    name: "leak".into(),
                    args: json!({"value": "secret-\"value\nline"}),
                }],
                usage: TokenUsage {
                    input: 0,
                    output: 1,
                    reasoning: 0,
                    cached_input: 0,
                },
                stop: StopReason::ToolUse,
                provider_state: None,
            })
        }
    }

    #[async_trait]
    impl LlmProvider for RouteProvider {
        fn id(&self) -> &str {
            "router"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }

        async fn complete(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
            match req.provider.as_str() {
                "primary" => {
                    return Err(LlmError {
                        message: "primary route timed out".into(),
                        kind: LlmErrorKind::TimedOut,
                    });
                }
                "final-failure" => {
                    return Err(LlmError {
                        message: "final route returned a server error".into(),
                        kind: LlmErrorKind::HttpStatus(503),
                    });
                }
                "nonretryable" => {
                    return Err(LlmError {
                        message: "route returned an invalid response".into(),
                        kind: LlmErrorKind::InvalidResponse,
                    });
                }
                _ => {}
            }
            Ok(ChatResponse {
                content: vec![ChatContent::Text("fallback response".into())],
                usage: TokenUsage::default(),
                stop: StopReason::EndTurn,
                provider_state: None,
            })
        }
    }

    #[tokio::test]
    async fn gateway_routes_retryable_failures_to_declared_fallback_model() {
        let dir = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-llm-route-test-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        )))
        .expect("temporary path must be UTF-8");
        let journal = JournalWriter::create(&dir.join("journal.jsonl"), "route-test", false, None)
            .expect("journal should be created");
        let secrets = SecretStore::default();
        let gateway = LlmGateway::new(
            Arc::new(RouteProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            LlmCostBudget {
                max_tokens: None,
                max_cost_microusd: None,
                require_pricing: false,
            },
            Vec::new(),
        );
        let node = test_node();
        let mut request = test_request();
        request.provider = "primary".into();
        let response = gateway
            .complete(
                &node,
                request,
                &test_routes(&[("primary", "test"), ("fallback", "safe")]),
                |_| json!({}),
            )
            .await
            .expect("fallback route should succeed");
        assert!(matches!(
            response.content.first(),
            Some(ChatContent::Text(text)) if text == "fallback response"
        ));
        let journal_text =
            std::fs::read_to_string(dir.join("journal.jsonl")).expect("journal should be readable");
        assert!(journal_text.contains("llm_route_failed"));
    }

    #[tokio::test]
    async fn gateway_records_failure_for_a_single_route_once() {
        let dir = route_test_dir("single");
        let journal =
            JournalWriter::create(&dir.join("journal.jsonl"), "single-route-test", false, None)
                .expect("journal should be created");
        let secrets = SecretStore::default();
        let gateway = LlmGateway::new(
            Arc::new(RouteProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            LlmCostBudget {
                max_tokens: None,
                max_cost_microusd: None,
                require_pricing: false,
            },
            Vec::new(),
        );
        let mut request = test_request();
        request.provider = "primary".into();

        let error = gateway
            .complete(
                &test_node(),
                request,
                &test_routes(&[("primary", "test")]),
                |_| json!({}),
            )
            .await
            .expect_err("a failed route without fallback must fail");
        assert!(error.to_string().contains("primary route timed out"));

        let events = route_failure_events(&dir);
        assert_eq!(events.len(), 1, "a route failure must be journaled once");
        assert_eq!(events[0]["provider"], "primary");
        assert_eq!(events[0]["attempt"], 1);
        assert_eq!(events[0]["kind"], "timed_out");
    }

    #[tokio::test]
    async fn gateway_records_nonretryable_failure_without_trying_fallback() {
        let dir = route_test_dir("nonretryable");
        let journal = JournalWriter::create(
            &dir.join("journal.jsonl"),
            "nonretryable-route-test",
            false,
            None,
        )
        .expect("journal should be created");
        let secrets = SecretStore::default();
        let gateway = LlmGateway::new(
            Arc::new(RouteProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            LlmCostBudget {
                max_tokens: None,
                max_cost_microusd: None,
                require_pricing: false,
            },
            Vec::new(),
        );
        let node = test_node();
        let mut request = test_request();
        request.provider = "nonretryable".into();

        let error = gateway
            .complete(
                &node,
                request,
                &test_routes(&[("nonretryable", "test"), ("fallback", "safe")]),
                |_| json!({}),
            )
            .await
            .expect_err("a non-retryable route failure must stop routing");
        assert!(error.to_string().contains("invalid response"));

        let events = route_failure_events(&dir);
        assert_eq!(
            events.len(),
            1,
            "a non-retryable failure must not be duplicated"
        );
        assert_eq!(events[0]["provider"], "nonretryable");
        assert_eq!(events[0]["attempt"], 1);
        assert_eq!(events[0]["kind"], "invalid_response");
    }

    #[tokio::test]
    async fn gateway_records_the_final_fallback_failure_without_duplication() {
        let dir = route_test_dir("final");
        let journal =
            JournalWriter::create(&dir.join("journal.jsonl"), "final-route-test", false, None)
                .expect("journal should be created");
        let secrets = SecretStore::default();
        let gateway = LlmGateway::new(
            Arc::new(RouteProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            LlmCostBudget {
                max_tokens: None,
                max_cost_microusd: None,
                require_pricing: false,
            },
            Vec::new(),
        );
        let node = test_node();
        let mut request = test_request();
        request.provider = "primary".into();

        let error = gateway
            .complete(
                &node,
                request,
                &test_routes(&[("primary", "test"), ("final-failure", "safe")]),
                |_| json!({}),
            )
            .await
            .expect_err("all retryable route failures must fail after the last route");
        assert!(error.to_string().contains("server error"));

        let events = route_failure_events(&dir);
        assert_eq!(
            events.len(),
            2,
            "each failed route must be journaled exactly once"
        );
        assert_eq!(events[0]["provider"], "primary");
        assert_eq!(events[0]["attempt"], 1);
        assert_eq!(events[0]["kind"], "timed_out");
        assert_eq!(events[1]["provider"], "final-failure");
        assert_eq!(events[1]["attempt"], 2);
        assert_eq!(events[1]["kind"], json!({"http_status": 503}));
    }

    fn route_test_dir(label: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-llm-route-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        )))
        .expect("temporary path must be UTF-8")
    }

    fn route_failure_events(dir: &Utf8PathBuf) -> Vec<serde_json::Value> {
        std::fs::read_to_string(dir.join("journal.jsonl"))
            .expect("journal should be readable")
            .lines()
            .map(|line| serde_json::from_str(line).expect("journal line should be valid JSON"))
            .filter(|event: &serde_json::Value| event["t"] == "llm_route_failed")
            .collect()
    }

    #[tokio::test]
    async fn gateway_scans_response_text_for_secrets() {
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-llm-gateway-test-{}", std::process::id())),
        )
        .expect("temporary path must be UTF-8");
        let _ = std::fs::remove_dir_all(&dir);
        let journal = JournalWriter::create(&dir.join("journal.jsonl"), "secret-test", false, None)
            .expect("journal should be created");
        let secrets =
            SecretStore::from_values(BTreeMap::from([("token".into(), "secret-value".into())]));
        let gateway = LlmGateway::new(
            Arc::new(LeakingProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            LlmCostBudget {
                max_tokens: None,
                max_cost_microusd: None,
                require_pricing: false,
            },
            Vec::new(),
        );
        let node = test_node();
        let error = gateway
            .complete(
                &node,
                test_request(),
                &test_routes(&[("leak", "test")]),
                |_| json!({}),
            )
            .await
            .expect_err("gateway should reject leaked secret output");
        assert!(error.to_string().contains("secret `token`"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn gateway_scans_decoded_tool_arguments_for_secrets() {
        let dir = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-llm-tool-gateway-test-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        )))
        .expect("temporary path must be UTF-8");
        let journal =
            JournalWriter::create(&dir.join("journal.jsonl"), "tool-secret-test", false, None)
                .expect("journal should be created");
        let secrets = SecretStore::from_values(BTreeMap::from([(
            "token".into(),
            "secret-\"value\nline".into(),
        )]));
        let gateway = LlmGateway::new(
            Arc::new(ToolLeakingProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            LlmCostBudget {
                max_tokens: None,
                max_cost_microusd: None,
                require_pricing: false,
            },
            Vec::new(),
        );
        let node = test_node();

        let error = gateway
            .complete(
                &node,
                test_request(),
                &test_routes(&[("leak", "test")]),
                |_| json!({}),
            )
            .await
            .expect_err("decoded tool arguments must not leak secrets");
        assert!(error.to_string().contains("secret `token`"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn gateway_enforces_accumulated_cost_budget_after_recording_usage() {
        let dir = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-llm-budget-test-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        )))
        .expect("temporary path must be UTF-8");
        let journal = JournalWriter::create(&dir.join("journal.jsonl"), "budget-test", false, None)
            .expect("journal should be created");
        let secrets = SecretStore::default();
        let gateway = LlmGateway::new(
            Arc::new(MeteredProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            LlmCostBudget {
                max_tokens: None,
                max_cost_microusd: Some(1_000_000),
                require_pricing: true,
            },
            vec![ModelRef {
                provider: "metered".into(),
                model: "test".into(),
                input_cost_per_million_usd: Some(1.0),
                output_cost_per_million_usd: Some(1.0),
            }],
        );
        let node = test_node();
        let mut request = test_request();
        request.provider = "metered".into();
        let error = gateway
            .complete(&node, request, &test_routes(&[("metered", "test")]), |_| {
                json!({})
            })
            .await
            .expect_err("cost budget should be exceeded");
        assert!(matches!(
            error,
            StepError::BudgetExceeded {
                resource: "cost_microusd",
                ..
            }
        ));
        assert_eq!(journal.state().budget.cost_microusd, 1_000_001);
    }

    fn test_request() -> ChatRequest {
        ChatRequest {
            provider: "leak".into(),
            model: "test".into(),
            system: None,
            messages: vec![ChatMessage::text("user", "safe request")],
            tools: Vec::<ToolSpec>::new(),
            response_schema: None,
            structured_output: qcg_types::StructuredOutputMode::Auto,
            temperature: None,
            top_p: None,
            max_tokens: 8,
            stop_sequences: vec![],
            seed: None,
            reasoning_effort: None,
            tool_choice: None,
            parallel_tool_calls: None,
            verbosity: None,
            stream: false,
        }
    }

    fn test_routes(routes: &[(&str, &str)]) -> Vec<ModelRef> {
        routes
            .iter()
            .map(|(provider, model)| ModelRef {
                provider: (*provider).into(),
                model: (*model).into(),
                input_cost_per_million_usd: None,
                output_cost_per_million_usd: None,
            })
            .collect()
    }

    fn test_node() -> NodeDef {
        NodeDef {
            id: "llm".into(),
            kind: StepType::from("llm.generate"),
            needs: vec![],
            when: None,
            on_deps: OnDeps::default(),
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            retry: None,
            params: Default::default(),
        }
    }
}
