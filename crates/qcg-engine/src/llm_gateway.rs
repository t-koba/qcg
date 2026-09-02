use qcg_contract::{LlmConfig, ModelRef, NodeDef, RunBudget};
use qcg_llm::{
    ChatContent, ChatContentPart, ChatRequest, ChatResponse, ChatStreamEvent, LlmError,
    LlmErrorKind, LlmProvider, TokenUsage,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::{JournalWriter, ResultExt, SecretStore, StepError};

const LLM_STREAM_CHANNEL_CAPACITY: usize = 64;

pub struct LlmGateway<'a> {
    provider: Arc<dyn LlmProvider>,
    secrets: &'a SecretStore,
    journal: &'a JournalWriter,
    cancellation: CancellationToken,
    budget: &'a RunBudget,
    llm: Option<&'a LlmConfig>,
}

impl<'a> LlmGateway<'a> {
    pub fn new(
        provider: Arc<dyn LlmProvider>,
        secrets: &'a SecretStore,
        journal: &'a JournalWriter,
        cancellation: CancellationToken,
        budget: &'a RunBudget,
        llm: Option<&'a LlmConfig>,
    ) -> Self {
        Self {
            provider,
            secrets,
            journal,
            cancellation,
            budget,
            llm,
        }
    }

    pub async fn complete<F>(
        &self,
        node: &NodeDef,
        request: ChatRequest,
        routes: &[ModelRef],
        event_extra: F,
    ) -> Result<ChatResponse, StepError>
    where
        F: FnOnce(&TokenUsage) -> Value,
    {
        self.scan_request(node, &request)?;
        let provider_id = request.provider.clone();
        let model_id = request.model.clone();
        let seed = request.seed;
        let reasoning_effort = request.reasoning_effort;
        let temperature = request.temperature;
        let top_p = request.top_p;
        let max_tokens = request.max_tokens;
        let stop_sequences = request.stop_sequences.clone();
        let structured_output = request.structured_output;
        let tool_choice = request.tool_choice.clone();
        let parallel_tool_calls = request.parallel_tool_calls;
        let verbosity = request.verbosity;
        let stream = request.stream;
        if routes
            .first()
            .is_none_or(|route| route.provider != provider_id || route.model != model_id)
        {
            return Err(StepError::failed(
                &node.id,
                "LLM route policy does not start with the request model",
            ));
        }
        let mut response = None;
        let mut last_error = None;
        for (attempt, route) in routes.iter().enumerate() {
            let provider = &route.provider;
            let model = &route.model;
            let mut routed_request = request.clone();
            routed_request.provider.clone_from(provider);
            routed_request.model.clone_from(model);
            self.scan_request(node, &routed_request)?;
            let result = self.complete_route(node, routed_request).await;
            match result {
                Ok(completed) => {
                    response = Some((provider.clone(), model.clone(), completed));
                    break;
                }
                Err(error) => {
                    self.record_route_failure(node, provider, model, attempt + 1, &error)?;
                    if error.kind == LlmErrorKind::Canceled {
                        return Err(StepError::Cancelled);
                    }
                    if error.is_retryable() && attempt + 1 < routes.len() {
                        last_error = Some(error);
                    } else {
                        return Err(error).step_err(&node.id);
                    }
                }
            }
        }
        let (provider_id, model_id, response) = response.ok_or_else(|| {
            StepError::failed(
                &node.id,
                last_error
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "no LLM route was available".into()),
            )
        })?;
        self.scan_response(node, &response)?;
        let usage = response.usage.clone();
        let cost_microusd = self.cost_microusd(node, routes, &provider_id, &model_id, &usage)?;
        let mut event = json!({
            "node": node.id,
            "provider": provider_id,
            "model": model_id,
            "seed": seed,
            "reasoning_effort": reasoning_effort,
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_tokens,
            "stop_sequences": stop_sequences,
            "structured_output": structured_output,
            "tool_choice": tool_choice,
            "parallel_tool_calls": parallel_tool_calls,
            "verbosity": verbosity,
            "stream": stream,
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

    fn record_route_failure(
        &self,
        node: &NodeDef,
        provider: &str,
        model: &str,
        attempt: usize,
        error: &LlmError,
    ) -> Result<(), StepError> {
        self.journal
            .event(
                "llm_route_failed",
                json!({
                    "node": node.id,
                    "provider": provider,
                    "model": model,
                    "attempt": attempt,
                    "kind": error.kind,
                }),
            )
            .step_err(&node.id)
    }

    async fn complete_route(
        &self,
        node: &NodeDef,
        request: ChatRequest,
    ) -> Result<ChatResponse, LlmError> {
        if !request.stream {
            return tokio::select! {
                _ = self.cancellation.cancelled() => Err(LlmError {
                    message: "LLM call canceled".into(),
                    kind: LlmErrorKind::Canceled,
                }),
                response = self.provider.complete(request) => response,
            };
        }
        let provider_id = request.provider.clone();
        let model_id = request.model.clone();
        let (events, mut receiver) = tokio::sync::mpsc::channel(LLM_STREAM_CHANNEL_CAPACITY);
        let stream = self.provider.stream(request, events);
        tokio::pin!(stream);
        let mut response = None;
        let mut index = 0_usize;
        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    return Err(LlmError {
                        message: "LLM stream canceled".into(),
                        kind: LlmErrorKind::Canceled,
                    });
                }
                result = &mut stream => {
                    while let Ok(event) = receiver.try_recv() {
                        self.record_stream_event(
                            node,
                            &provider_id,
                            &model_id,
                            event,
                            &mut index,
                            &mut response,
                        )?;
                    }
                    if let Err(error) = result {
                        if index > 0 {
                            return Err(LlmError {
                                message: format!(
                                    "LLM stream failed after {index} emitted deltas; route fallback is unsafe: {error}"
                                ),
                                kind: LlmErrorKind::PartialStream,
                            });
                        }
                        return Err(error);
                    }
                    return response.ok_or_else(|| LlmError::new("LLM stream ended without a completed response"));
                }
                event = receiver.recv() => {
                    match event {
                        Some(event) => self.record_stream_event(
                            node,
                            &provider_id,
                            &model_id,
                            event,
                            &mut index,
                            &mut response,
                        )?,
                        None => {
                            return response.ok_or_else(|| LlmError::new("LLM stream channel closed before completion"));
                        }
                    }
                }
            }
        }
    }

    fn record_stream_event(
        &self,
        node: &NodeDef,
        provider: &str,
        model: &str,
        event: ChatStreamEvent,
        index: &mut usize,
        response: &mut Option<ChatResponse>,
    ) -> Result<(), LlmError> {
        match event {
            ChatStreamEvent::TextDelta { text } => {
                self.scan_text(node, &text)
                    .map_err(|error| LlmError::new(error.to_string()))?;
                self.journal
                    .event(
                        "llm_delta",
                        json!({
                            "node": node.id,
                            "provider": provider,
                            "model": model,
                            "index": *index,
                            "text": text,
                        }),
                    )
                    .map_err(|error| LlmError::new(error.to_string()))?;
                *index = index.saturating_add(1);
            }
            ChatStreamEvent::Completed {
                response: completed,
            } => *response = Some(completed),
        }
        Ok(())
    }

    fn cost_microusd(
        &self,
        node: &NodeDef,
        routes: &[ModelRef],
        provider: &str,
        model: &str,
        usage: &TokenUsage,
    ) -> Result<u64, StepError> {
        let Some(pricing) = self.pricing_for(routes, provider, model) else {
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

    fn pricing_for(&self, routes: &[ModelRef], provider: &str, model: &str) -> Option<ModelRef> {
        let candidates = routes
            .iter()
            .cloned()
            .chain(self.llm.and_then(|llm| llm.model.clone()))
            .chain(self.llm.into_iter().flat_map(|llm| llm.models.clone()))
            .filter(|entry| entry.provider == provider && entry.model == model)
            .collect::<Vec<_>>();
        candidates
            .iter()
            .find(|entry| {
                entry.input_cost_per_million_usd.is_some()
                    && entry.output_cost_per_million_usd.is_some()
            })
            .or_else(|| candidates.first())
            .cloned()
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
            for part in &message.parts {
                match part {
                    ChatContentPart::Text { text } => self.assert_absent(node, text)?,
                    ChatContentPart::InputImage { media_type, .. }
                    | ChatContentPart::InputAudio { media_type, .. }
                    | ChatContentPart::InputFile { media_type, .. } => {
                        self.assert_absent(node, media_type)?;
                    }
                }
                if let ChatContentPart::InputFile { filename, .. } = part {
                    self.assert_absent(node, filename)?;
                }
            }
            if let Some(id) = &message.tool_call_id {
                self.assert_absent(node, id)?;
            }
            for call in &message.tool_calls {
                self.assert_absent(node, &call.id)?;
                self.assert_absent(node, &call.name)?;
                self.assert_value_absent(node, &call.args)?;
            }
            if let Some(state) = &message.provider_state {
                self.assert_value_absent(node, &Value::Array(state.clone()))?;
            }
        }
        Ok(())
    }

    fn scan_response(&self, node: &NodeDef, response: &ChatResponse) -> Result<(), StepError> {
        for content in &response.content {
            match content {
                ChatContent::Text(text) => self.assert_absent(node, text)?,
                ChatContent::ToolCall { id, name, args } => {
                    self.assert_absent(node, id)?;
                    self.assert_absent(node, name)?;
                    self.assert_value_absent(node, args)?;
                }
            }
        }
        if let Some(state) = &response.provider_state {
            self.assert_value_absent(node, state)?;
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
    use qcg_llm::{
        Capabilities, ChatMessage, LlmError, LlmErrorKind, StopReason, TokenUsage, ToolSpec,
    };
    use std::collections::BTreeMap;

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
        let budget = RunBudget::default();
        let secrets = SecretStore::default();
        let gateway = LlmGateway::new(
            Arc::new(RouteProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            &budget,
            None,
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
        let budget = RunBudget::default();
        let secrets = SecretStore::default();
        let gateway = LlmGateway::new(
            Arc::new(RouteProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            &budget,
            None,
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
        let budget = RunBudget::default();
        let secrets = SecretStore::default();
        let gateway = LlmGateway::new(
            Arc::new(RouteProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            &budget,
            None,
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
        let budget = RunBudget::default();
        let secrets = SecretStore::default();
        let gateway = LlmGateway::new(
            Arc::new(RouteProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            &budget,
            None,
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
        let budget = RunBudget {
            max_cost_usd: Some(1.0),
            ..RunBudget::default()
        };
        let model = ModelRef {
            provider: "metered".into(),
            model: "test".into(),
            input_cost_per_million_usd: Some(1.0),
            output_cost_per_million_usd: Some(1.0),
        };
        let llm: LlmConfig = serde_json::from_value(json!({ "model": model }))
            .expect("test LLM config should deserialize");
        let gateway = LlmGateway::new(
            Arc::new(MeteredProvider),
            &secrets,
            &journal,
            CancellationToken::new(),
            &budget,
            Some(&llm),
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
            params: Default::default(),
        }
    }
}
