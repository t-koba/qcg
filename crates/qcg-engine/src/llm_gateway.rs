use qcg_contract::{ModelRef, NodeDef, RunBudget};
use qcg_llm::{ChatContent, ChatRequest, ChatResponse, LlmProvider, TokenUsage};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::{JournalWriter, ResultExt, SecretStore, StepError};

pub struct LlmGateway<'a> {
    provider: Arc<dyn LlmProvider>,
    secrets: &'a SecretStore,
    journal: &'a JournalWriter,
    cancellation: CancellationToken,
    budget: &'a RunBudget,
    model: Option<&'a ModelRef>,
}

impl<'a> LlmGateway<'a> {
    pub fn new(
        provider: Arc<dyn LlmProvider>,
        secrets: &'a SecretStore,
        journal: &'a JournalWriter,
        cancellation: CancellationToken,
        budget: &'a RunBudget,
        model: Option<&'a ModelRef>,
    ) -> Self {
        Self {
            provider,
            secrets,
            journal,
            cancellation,
            budget,
            model,
        }
    }

    pub async fn complete<F>(
        &self,
        node: &NodeDef,
        request: ChatRequest,
        event_extra: F,
    ) -> Result<ChatResponse, StepError>
    where
        F: FnOnce(&TokenUsage) -> Value,
    {
        self.scan_request(node, &request)?;
        let provider_id = request.provider.clone();
        let model_id = request.model.clone();
        let seed = request.seed;
        let response = tokio::select! {
            _ = self.cancellation.cancelled() => {
                return Err(StepError::Cancelled);
            }
            response = self.provider.complete(request) => response.step_err(&node.id)?,
        };
        self.scan_response(node, &response)?;
        let usage = response.usage.clone();
        let cost_microusd = self.cost_microusd(node, &provider_id, &model_id, &usage)?;
        let mut event = json!({
            "node": node.id,
            "provider": provider_id,
            "seed": seed,
            "tokens": usage,
            "cost_microusd": cost_microusd,
        });
        merge_event_extra(&mut event, event_extra(&response.usage));
        self.journal.event("llm_call", event).step_err(&node.id)?;
        let state = self.journal.state();
        let tokens = state
            .budget
            .tokens_input
            .saturating_add(state.budget.tokens_output);
        if let Some(limit) = self.budget.max_tokens
            && tokens > limit
        {
            return Err(StepError::BudgetExceeded {
                resource: "tokens",
                used: tokens,
                limit,
            });
        }
        if let Some(limit) = self.budget.max_cost_usd {
            let limit = crate::step::usd_to_microusd(limit);
            if state.budget.cost_microusd > limit {
                return Err(StepError::BudgetExceeded {
                    resource: "cost_microusd",
                    used: state.budget.cost_microusd,
                    limit,
                });
            }
        }
        Ok(response)
    }

    fn cost_microusd(
        &self,
        node: &NodeDef,
        provider: &str,
        model: &str,
        usage: &TokenUsage,
    ) -> Result<u64, StepError> {
        let Some(pricing) = self
            .model
            .filter(|entry| entry.provider == provider && entry.model == model)
        else {
            if self.budget.max_cost_usd.is_some() {
                return Err(StepError::failed(
                    &node.id,
                    format!("model `{provider}/{model}` has no pricing for the run cost budget"),
                ));
            }
            return Ok(0);
        };
        let input = pricing.input_cost_per_million_usd;
        let output = pricing.output_cost_per_million_usd;
        if self.budget.max_cost_usd.is_some() && (input.is_none() || output.is_none()) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "model `{provider}/{model}` has incomplete pricing for the run cost budget"
                ),
            ));
        }
        let microusd = usage.input as f64 * input.unwrap_or_default()
            + usage.output as f64 * output.unwrap_or_default();
        Ok(microusd.round().clamp(0.0, u64::MAX as f64) as u64)
    }

    pub fn scan_text(&self, node: &NodeDef, text: &str) -> Result<(), StepError> {
        self.assert_absent(node, text)
    }

    fn scan_request(&self, node: &NodeDef, request: &ChatRequest) -> Result<(), StepError> {
        if let Some(system) = &request.system {
            self.assert_absent(node, system)?;
        }
        for message in &request.messages {
            self.assert_absent(node, &message.content)?;
        }
        Ok(())
    }

    fn scan_response(&self, node: &NodeDef, response: &ChatResponse) -> Result<(), StepError> {
        for content in &response.content {
            match content {
                ChatContent::Text(text) => self.assert_absent(node, text)?,
                ChatContent::ToolCall { args, .. } => self.assert_value_absent(node, args)?,
            }
        }
        Ok(())
    }

    fn assert_value_absent(&self, node: &NodeDef, value: &Value) -> Result<(), StepError> {
        match value {
            Value::String(value) => self.assert_absent(node, value),
            Value::Array(values) => {
                for value in values {
                    self.assert_value_absent(node, value)?;
                }
                Ok(())
            }
            Value::Object(values) => {
                for (key, value) in values {
                    self.assert_absent(node, key)?;
                    self.assert_value_absent(node, value)?;
                }
                Ok(())
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => Ok(()),
        }
    }

    fn assert_absent(&self, node: &NodeDef, text: &str) -> Result<(), StepError> {
        self.secrets.assert_absent(text).step_err(&node.id)
    }
}

fn merge_event_extra(event: &mut Value, extra: Value) {
    let (Some(event), Some(extra)) = (event.as_object_mut(), extra.as_object()) else {
        return;
    };
    for (key, value) in extra {
        event.insert(key.clone(), value.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use camino::Utf8PathBuf;
    use qcg_contract::{NodeDef, OnDeps, StepType};
    use qcg_llm::{Capabilities, ChatMessage, LlmError, StopReason, TokenUsage, ToolSpec};
    use std::collections::BTreeMap;

    struct LeakingProvider;

    struct MeteredProvider;

    struct ToolLeakingProvider;

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
                },
                stop: StopReason::EndTurn,
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
                },
                stop: StopReason::EndTurn,
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
                },
                stop: StopReason::ToolUse,
            })
        }
    }

    #[tokio::test]
    async fn gateway_scans_response_text_for_secrets() {
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-llm-gateway-test-{}", std::process::id())),
        )
        .expect("temporary path must be UTF-8");
        let _ = std::fs::remove_dir_all(&dir);
        let journal = JournalWriter::create(&dir.join("journal.jsonl"), false, None)
            .expect("journal should be created");
        let secrets =
            SecretStore::from_values(BTreeMap::from([("token".into(), "secret-value".into())]));
        let budget = RunBudget::default();
        let model = None;
        let gateway = LlmGateway::new(
            Arc::new(LeakingProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            &budget,
            model,
        );
        let node = test_node();
        let error = gateway
            .complete(&node, test_request(), |_| json!({}))
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
        let journal = JournalWriter::create(&dir.join("journal.jsonl"), false, None)
            .expect("journal should be created");
        let secrets = SecretStore::from_values(BTreeMap::from([(
            "token".into(),
            "secret-\"value\nline".into(),
        )]));
        let budget = RunBudget::default();
        let gateway = LlmGateway::new(
            Arc::new(ToolLeakingProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            &budget,
            None,
        );
        let node = test_node();

        let error = gateway
            .complete(&node, test_request(), |_| json!({}))
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
        let journal = JournalWriter::create(&dir.join("journal.jsonl"), false, None)
            .expect("journal should be created");
        let secrets = SecretStore::default();
        let budget = RunBudget {
            max_cost_usd: Some(1.0),
            ..RunBudget::default()
        };
        let models = [ModelRef {
            provider: "metered".into(),
            model: "test".into(),
            input_cost_per_million_usd: Some(1.0),
            output_cost_per_million_usd: Some(1.0),
        }];
        let gateway = LlmGateway::new(
            Arc::new(MeteredProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            &budget,
            models.first(),
        );
        let node = test_node();
        let mut request = test_request();
        request.provider = "metered".into();
        let error = gateway
            .complete(&node, request, |_| json!({}))
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
            messages: vec![ChatMessage {
                role: "user".into(),
                content: "safe request".into(),
            }],
            tools: Vec::<ToolSpec>::new(),
            response_schema: None,
            temperature: None,
            max_tokens: 8,
            seed: None,
        }
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
            params: Default::default(),
        }
    }
}
