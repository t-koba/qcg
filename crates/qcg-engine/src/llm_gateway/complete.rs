use crate::{JournalWriter, ResultExt, SecretStore, StepError};
use qcg_contract::{ModelRef, NodeDef};
use qcg_llm::{
    ChatRequest, ChatResponse, ChatStreamEvent, LlmError, LlmErrorKind, LlmProvider, TokenUsage,
};
use qcg_policy::LlmCostBudget;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use super::merge::merge_event_extra;
use super::types::{LLM_STREAM_CHANNEL_CAPACITY, LlmGateway};

impl<'a> LlmGateway<'a> {
    pub fn new(
        provider: Arc<dyn LlmProvider>,
        secrets: &'a SecretStore,
        journal: &'a JournalWriter,
        cancellation: CancellationToken,
        budget: LlmCostBudget,
        pricing: Vec<ModelRef>,
    ) -> Self {
        Self {
            provider,
            secrets,
            journal,
            cancellation,
            budget,
            pricing,
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
                    self.record_route_failure(
                        node,
                        provider,
                        model,
                        attempt + 1,
                        routes.len(),
                        &error,
                    )?;
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
        if let Some(limit) = self.budget.max_cost_microusd
            && state.budget.cost_microusd > limit
        {
            return Err(StepError::BudgetExceeded {
                resource: "cost_microusd",
                used: state.budget.cost_microusd,
                limit,
            });
        }
        Ok(response)
    }

    fn record_route_failure(
        &self,
        node: &NodeDef,
        provider: &str,
        model: &str,
        attempt: usize,
        routes_total: usize,
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
                    "fallback_index": attempt.saturating_sub(1),
                    "routes_total": routes_total,
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
}
