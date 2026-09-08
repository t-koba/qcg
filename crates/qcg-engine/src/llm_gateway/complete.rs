use crate::{JournalWriter, ResultExt, SecretStore, StepError};
use qcg_contract::{ModelRef, NodeDef};
use qcg_llm::{
    ChatRequest, ChatResponse, ChatStreamEvent, LlmError, LlmErrorKind, LlmProvider, TokenUsage,
};
use qcg_policy::LlmCostBudget;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio_util::sync::CancellationToken;

/// Per-run pending LLM reservations shared across parallel tasks in this
/// process. Each entry tracks conservatively reserved tokens and cost that
/// have not yet settled into the journal budget. Reservations prevent
/// parallel calls from each passing a post-hoc check and jointly overspending
/// a hard cap (strongest budget guarantee).
#[derive(Debug, Default, Clone)]
struct PendingReservation {
    tokens: u64,
    cost_microusd: u64,
}

fn reservations() -> &'static Mutex<BTreeMap<String, PendingReservation>> {
    static RESERVATIONS: OnceLock<Mutex<BTreeMap<String, PendingReservation>>> = OnceLock::new();
    RESERVATIONS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn run_id_of(journal: &JournalWriter) -> String {
    journal.state().run_id.clone().unwrap_or_default()
}

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
        // Hard-cap reservation before spending: estimate worst-case tokens
        // and cost, then hold them so parallel calls cannot jointly exceed
        // the cap. Soft post-hoc enforcement alone lets N parallel calls
        // each pass and overspend N-fold.
        let reservation = self.estimate_reservation(node, &request)?;
        self.try_reserve(reservation.clone())?;
        let complete_result = self
            .complete_inner(node, request, routes, event_extra)
            .await;
        self.release_reservation(reservation);
        complete_result
    }

    /// Conservative worst-case estimate for one call: prompt bytes/4 input
    /// tokens plus the full requested max output, priced when known.
    /// Unknown pricing with an enforced cost cap fails closed here instead
    /// of spending blindly.
    fn estimate_reservation(
        &self,
        node: &NodeDef,
        request: &ChatRequest,
    ) -> Result<Option<PendingReservation>, StepError> {
        if self.budget.max_tokens.is_none() && self.budget.max_cost_microusd.is_none() {
            return Ok(None);
        }
        // Non-finite floats (NaN temperature and friends) fail
        // serialization: under-reserving budget as zero would overspend,
        // so fail the reservation instead.
        let prompt_bytes = serde_json::to_vec(request)
            .map(|bytes| bytes.len())
            .map_err(|error| {
                StepError::failed("budget", format!("request is not serializable: {error}"))
            })?;
        let input_est = (prompt_bytes as u64).div_ceil(4);
        let max_output = u64::from(request.max_tokens);
        let tokens = input_est.saturating_add(max_output);
        let mut cost_microusd = 0_u64;
        if self.budget.max_cost_microusd.is_some() {
            let pricing = self
                .pricing
                .iter()
                .find(|model| model.provider == request.provider && model.model == request.model);
            match pricing {
                Some(model) => {
                    match (
                        model.input_cost_per_million_usd,
                        model.output_cost_per_million_usd,
                    ) {
                        (Some(input_price), Some(output_price)) => {
                            let input_cost = (input_est as f64) * input_price / 1_000_000.0;
                            let output_cost = (max_output as f64) * output_price / 1_000_000.0;
                            cost_microusd =
                                ((input_cost + output_cost) * 1_000_000.0).ceil() as u64;
                        }
                        _ => {
                            return Err(StepError::failed(
                                &node.id,
                                format!(
                                    "model `{}/{}` lacks pricing while budget.max_cost_usd is enforced; refusing blind spend",
                                    request.provider, request.model
                                ),
                            ));
                        }
                    }
                }
                None => {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "model `{}/{}` has no priced entry while budget.max_cost_usd is enforced; refusing blind spend",
                            request.provider, request.model
                        ),
                    ));
                }
            }
        }
        Ok(Some(PendingReservation {
            tokens,
            cost_microusd,
        }))
    }

    fn try_reserve(&self, reservation: Option<PendingReservation>) -> Result<(), StepError> {
        let Some(reservation) = reservation else {
            return Ok(());
        };
        let run_id = run_id_of(self.journal);
        let state = self.journal.state();
        let spent_tokens = state
            .budget
            .tokens_input
            .saturating_add(state.budget.tokens_output);
        let spent_cost = state.budget.cost_microusd;
        let mut map = reservations()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = map.entry(run_id).or_default();
        let projected_tokens = spent_tokens
            .saturating_add(entry.tokens)
            .saturating_add(reservation.tokens);
        let projected_cost = spent_cost
            .saturating_add(entry.cost_microusd)
            .saturating_add(reservation.cost_microusd);
        if let Some(limit) = self.budget.max_tokens
            && projected_tokens > limit
        {
            return Err(StepError::BudgetExceeded {
                resource: "tokens",
                used: projected_tokens,
                limit,
            });
        }
        if let Some(limit) = self.budget.max_cost_microusd
            && projected_cost > limit
        {
            return Err(StepError::BudgetExceeded {
                resource: "cost_microusd",
                used: projected_cost,
                limit,
            });
        }
        entry.tokens = entry.tokens.saturating_add(reservation.tokens);
        entry.cost_microusd = entry
            .cost_microusd
            .saturating_add(reservation.cost_microusd);
        Ok(())
    }

    fn release_reservation(&self, reservation: Option<PendingReservation>) {
        let Some(reservation) = reservation else {
            return;
        };
        let run_id = run_id_of(self.journal);
        let mut map = reservations()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = map.get_mut(&run_id) {
            entry.tokens = entry.tokens.saturating_sub(reservation.tokens);
            entry.cost_microusd = entry
                .cost_microusd
                .saturating_sub(reservation.cost_microusd);
            if entry.tokens == 0 && entry.cost_microusd == 0 {
                map.remove(&run_id);
            }
        }
    }

    async fn complete_inner<F>(
        &self,
        node: &NodeDef,
        request: ChatRequest,
        routes: &[ModelRef],
        event_extra: F,
    ) -> Result<ChatResponse, StepError>
    where
        F: FnOnce(&TokenUsage) -> Value,
    {
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
        // Unverified suffix withheld from publication so split secrets stay
        // unrecoverable until the completed response passes its final scan.
        // published_tail keeps the last holdback bytes of published text so
        // secrets spanning a publish boundary are still detected.
        let mut pending = String::new();
        let mut published_tail = String::new();
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
                            &mut pending,
                            &mut published_tail,
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
                            &mut pending,
                            &mut published_tail,
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

    #[allow(clippy::too_many_arguments)]
    fn record_stream_event(
        &self,
        node: &NodeDef,
        provider: &str,
        model: &str,
        event: ChatStreamEvent,
        index: &mut usize,
        pending: &mut String,
        published_tail: &mut String,
        response: &mut Option<ChatResponse>,
    ) -> Result<(), LlmError> {
        match event {
            ChatStreamEvent::TextDelta { text } => {
                if self.secrets.is_empty() {
                    self.scan_text(node, &text)
                        .map_err(|error| LlmError::new(error.to_string()))?;
                    self.publish_delta(node, provider, model, &text, index)?;
                    return Ok(());
                }
                // Buffered publication: only a prefix that no future text
                // can complete into a registered secret becomes visible.
                // Split secrets can bypass per-delta checks while the
                // concatenated journal and SSE stream stay recoverable, so
                // the unverified suffix is withheld until the completed
                // response passes its final scan. Every arrival is examined
                // jointly with the published tail so secrets spanning a
                // publish boundary are still detected before the bytes that
                // complete them become visible.
                pending.push_str(&text);
                let holdback = self.secrets.max_value_len().saturating_sub(1);
                let mut window = published_tail.clone();
                window.push_str(pending);
                self.scan_text(node, &window)
                    .map_err(|error| LlmError::new(error.to_string()))?;
                let mut publish_len = pending.len().saturating_sub(holdback);
                while publish_len > 0 && !pending.is_char_boundary(publish_len) {
                    publish_len = publish_len.saturating_sub(1);
                }
                if publish_len == 0 {
                    return Ok(());
                }
                let publish = pending[..publish_len].to_string();
                // Drain before I/O so a journal failure cannot double
                // publish the same bytes on retry.
                pending.drain(..publish_len);
                self.publish_delta(node, provider, model, &publish, index)?;
                published_tail.push_str(&publish);
                let trimmed = holdback_tail(published_tail.as_str(), holdback);
                *published_tail = trimmed;
            }
            ChatStreamEvent::Completed {
                response: completed,
            } => {
                if !self.secrets.is_empty() {
                    // Final gate before the withheld suffix becomes visible.
                    // A rejection here publishes nothing further, and the
                    // already-visible prefixes were each verified jointly
                    // with their predecessors, so no recoverable secret is
                    // left behind.
                    self.scan_response(node, &completed)
                        .map_err(|error| LlmError::new(error.to_string()))?;
                    if !pending.is_empty() {
                        let mut window = published_tail.clone();
                        window.push_str(pending);
                        self.scan_text(node, &window)
                            .map_err(|error| LlmError::new(error.to_string()))?;
                        let flush = std::mem::take(pending);
                        self.publish_delta(node, provider, model, &flush, index)?;
                        published_tail.clear();
                    }
                }
                *response = Some(completed);
            }
        }
        Ok(())
    }

    fn publish_delta(
        &self,
        node: &NodeDef,
        provider: &str,
        model: &str,
        text: &str,
        index: &mut usize,
    ) -> Result<(), LlmError> {
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
        Ok(())
    }
}

/// Last `holdback` bytes of published text at a UTF-8 boundary, used as the
/// joint-scan window for boundary-spanning secrets.
fn holdback_tail(published: &str, holdback: usize) -> String {
    if published.len() <= holdback {
        return published.to_string();
    }
    let mut start = published.len() - holdback;
    while start < published.len() && !published.is_char_boundary(start) {
        start = start.saturating_add(1);
    }
    published[start..].to_string()
}
