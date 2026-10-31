use crate::{JournalWriter, ResultExt, SecretStore, StepError};
use contract::{ModelRef, NodeDef};
use llm::{
    ChatRequest, ChatResponse, ChatStreamEvent, LlmError, LlmErrorKind, LlmProvider, TokenUsage,
};
use policy::LlmCostBudget;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio_util::sync::CancellationToken;

/// Per-run pending LLM reservations shared across parallel tasks in this
/// process. Each entry tracks estimated tokens and cost that have not yet
/// settled into the journal budget. Reservations are estimated preflight
/// admission: they reduce (but do not bound) joint parallel overspend.
/// Enforcement is post-hoc from reported usage, not a hard upstream
/// spending cap.
#[derive(Debug, Default, Clone)]
struct PendingReservation {
    tokens: u64,
    cost_microusd: u64,
}

fn reservations() -> &'static Mutex<BTreeMap<String, PendingReservation>> {
    static RESERVATIONS: OnceLock<Mutex<BTreeMap<String, PendingReservation>>> = OnceLock::new();
    RESERVATIONS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

struct DecisionReservationGuard {
    run_id: String,
    reservation: PendingReservation,
}

/// Per-route stream state: unverified text stays pending, while the published
/// tail retains the joint-scan window for boundary-spanning secrets.
#[derive(Default)]
struct StreamAccumulator {
    index: usize,
    pending: String,
    published_tail: String,
    response: Option<ChatResponse>,
    rejection: Option<LlmError>,
    canceled: bool,
}

impl StreamAccumulator {
    fn reject(&mut self, error: StepError) {
        self.pending.clear();
        self.published_tail.clear();
        self.rejection = Some(LlmError {
            message: error.to_string(),
            kind: LlmErrorKind::PartialStream,
            ..Default::default()
        });
    }
}

impl Drop for DecisionReservationGuard {
    fn drop(&mut self) {
        release_pending(&self.run_id, &self.reservation);
    }
}

fn release_pending(run_id: &str, reservation: &PendingReservation) {
    let mut map = reservations()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(entry) = map.get_mut(run_id) {
        entry.tokens = entry.tokens.saturating_sub(reservation.tokens);
        entry.cost_microusd = entry
            .cost_microusd
            .saturating_sub(reservation.cost_microusd);
        if entry.tokens == 0 && entry.cost_microusd == 0 {
            map.remove(run_id);
        }
    }
}

fn run_id_of(journal: &JournalWriter) -> String {
    // Pre-admission journals carry no run id yet; reservations then share
    // the empty key. That only over-accounts (fail-closed toward earlier
    // BudgetExceeded), never under-accounts across runs.
    journal.state().run_id.clone().unwrap_or_default()
}

use super::merge::merge_event_extra;
use super::retry::RetryTrace;
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

    pub async fn decide(
        &self,
        node: &NodeDef,
        request: llm::DecisionRequest,
        model: &ModelRef,
        max_tokens: u32,
    ) -> Result<llm::DecisionResponse, StepError> {
        request.validate().step_err(&node.id)?;
        if request.provider != model.provider || request.model != model.model || max_tokens == 0 {
            return Err(StepError::failed(
                &node.id,
                "invalid decision model or token budget",
            ));
        }
        // The contract owns the price rule, so the decision path rejects
        // exactly the prices the manifest rejects.
        if let Some(field) = model.invalid_price() {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "model `{}/{}` {field} must be finite and non-negative",
                    model.provider, model.model
                ),
            ));
        }
        self.assert_value_absent(node, &serde_json::to_value(&request)?)?;
        let routes = std::slice::from_ref(model);
        let tokens = u64::from(max_tokens);
        let worst_usage = TokenUsage {
            input: tokens,
            output: tokens,
            ..TokenUsage::default()
        };
        let cost_microusd = self.cost_microusd(
            node,
            routes,
            &model.provider,
            &model.model,
            &worst_usage,
            None,
            None,
        )?;
        let reservation = PendingReservation {
            tokens,
            cost_microusd,
        };
        self.try_reserve(Some(reservation.clone()))?;
        let _guard = DecisionReservationGuard {
            run_id: run_id_of(self.journal),
            reservation,
        };
        let trace = RetryTrace::default();
        let response = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(LlmError { kind: LlmErrorKind::Canceled, message: "LLM decision canceled".into(), ..Default::default() }),
            result = self.provider.decide_observed(request.clone(), trace.observer()) => result,
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                let error = trace.finish_error(self.provider.as_ref(), error, false);
                self.record_route_failure(node, model, 1, 1, &error, false)?;
                if error.kind == LlmErrorKind::Canceled {
                    return Err(StepError::Cancelled);
                }
                return Err(error).step_err(&node.id);
            }
        };
        let usage = response.token_usage();
        let cost_microusd = self.cost_microusd(
            node,
            routes,
            &model.provider,
            &model.model,
            &usage,
            None,
            None,
        )?;
        self.journal
            .event(
                "llm_call",
                json!({
                    "node": node.id, "provider": model.provider, "model": model.model,
                    "max_tokens": max_tokens, "tokens": usage, "cost_microusd": cost_microusd,
                    "retry_decisions": trace.decisions()
                }),
            )
            .step_err(&node.id)?;
        let used = usage.input.saturating_add(usage.output);
        if used > tokens {
            return Err(StepError::BudgetExceeded {
                resource: "tokens",
                used,
                limit: tokens,
            });
        }
        self.check_budget()?;
        response.validate(&request).step_err(&node.id)?;
        self.assert_value_absent(node, &serde_json::to_value(&response)?)?;
        Ok(response)
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
        // Estimated preflight admission before spending: hold estimated
        // tokens and cost so parallel calls are less likely to jointly
        // exceed the budget. Post-hoc enforcement alone lets N parallel
        // calls each pass and overspend N-fold; the estimate itself is not
        // a bound on real spend.
        let reservation = self.estimate_reservation(node, &request, routes)?;
        self.try_reserve(reservation.clone())?;
        let complete_result = self
            .complete_inner(node, request, routes, event_extra)
            .await;
        self.release_reservation(reservation);
        complete_result
    }

    /// Estimated admission for one call: prompt bytes/4 input tokens plus
    /// the full requested max output. The selected row's highest input-category
    /// rate prices the estimate; input bytes/4 is not a worst-case bound:
    /// token-dense input can exceed it, a pricier fallback route can exceed
    /// the initial-route price, and retries can incur unreported usage.
    /// Required cache pricing is checked before dispatch; unknown pricing
    /// with a cost cap also fails closed. Reservations cannot guarantee
    /// upstream spend; caps remain post-hoc.
    fn estimate_reservation(
        &self,
        node: &NodeDef,
        request: &ChatRequest,
        routes: &[ModelRef],
    ) -> Result<Option<PendingReservation>, StepError> {
        let bounded = self.budget.max_tokens.is_some() || self.budget.max_cost_microusd.is_some();
        if !bounded && !self.budget.require_pricing {
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
        let mut cost_microusd = 0;
        // Required cache completeness is independent of reservation limits.
        if self.budget.require_pricing || self.budget.max_cost_microusd.is_some() {
            let provider = &request.provider;
            let model = &request.model;
            let unpriced = |detail: &str| {
                StepError::failed(
                    &node.id,
                    format!("model `{provider}/{model}` {detail}; refusing blind spend"),
                )
            };
            let route = self
                .selected_route(routes, provider, model)
                .ok_or_else(|| unpriced("has no priced entry"))?;
            // Reserve at the dearest rate: tier and cache split are unknown.
            let base = route.rates();
            let ttl = self.cache_write_multiplier(provider, model, request.cache_retention);
            for tier in std::iter::once(None).chain(route.cost_tiers.iter().map(Some)) {
                let row = base.resolve(tier);
                if self.budget.require_pricing
                    && (row.cache_read.is_none() || (ttl.is_none() && row.cache_creation.is_none()))
                {
                    return Err(unpriced("lacks cache input pricing in a base or tier row"));
                }
                // TTL writes require ordinary input in each row, not a cache price.
                if let Some(multiplier) = ttl {
                    let input = row.input.ok_or_else(|| {
                        unpriced("lacks ordinary input pricing for extended cache TTL")
                    })?;
                    if !input.is_finite() || input < 0.0 || !(input * multiplier).is_finite() {
                        return Err(StepError::failed(&node.id, "invalid model pricing"));
                    }
                }
            }
            let mut rates = policy::max_cost_rates(&base, &route.cost_tiers);
            if let Some(multiplier) = ttl {
                rates.cache_creation = rates.input.map(|input| input * multiplier);
            }
            let (Some(input_rate), Some(output)) = (rates.max_input_rate(), rates.output) else {
                return Err(unpriced("lacks pricing"));
            };
            if [input_rate, output]
                .into_iter()
                .any(|rate| !rate.is_finite() || rate < 0.0)
            {
                return Err(StepError::failed(&node.id, "invalid model pricing"));
            }
            cost_microusd = self.budget.max_cost_microusd.map_or(0, |_| {
                ((input_est as f64 * input_rate) + (max_output as f64 * output)).ceil() as u64
            });
        }
        Ok(bounded.then_some(PendingReservation {
            tokens,
            cost_microusd,
        }))
    }

    fn try_reserve(&self, reservation: Option<PendingReservation>) -> Result<(), StepError> {
        let state = self.journal.state();
        if self.budget.require_pricing && state.budget.unresolved_search_cost {
            return Err(StepError::failed(
                "budget",
                "run has unresolved hosted search cost; refusing blind spend",
            ));
        }
        let Some(reservation) = reservation else {
            return Ok(());
        };
        let run_id = run_id_of(self.journal);
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
        self.check_budget_values(projected_tokens, projected_cost)?;
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
        release_pending(&run_id_of(self.journal), &reservation);
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
        for (attempt, route) in routes.iter().enumerate() {
            let provider = &route.provider;
            let model = &route.model;
            let mut routed_request = request.clone();
            routed_request.provider.clone_from(provider);
            routed_request.model.clone_from(model);
            self.scan_request(node, &routed_request)?;
            if attempt > 0 && self.budget.require_pricing {
                self.estimate_reservation(node, &routed_request, routes)?;
            }
            let trace = RetryTrace::default();
            let validation = if attempt == 0 {
                Ok(())
            } else {
                llm::validate_route_capabilities(self.provider.as_ref(), &routed_request)
            };
            let mut acc = StreamAccumulator::default();
            let result = match validation {
                Ok(()) => {
                    self.complete_route(node, routed_request, trace.observer(), &mut acc)
                        .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(completed) => {
                    response = Some((
                        provider.clone(),
                        model.clone(),
                        completed,
                        trace.decisions(),
                        acc.rejection,
                        acc.canceled,
                    ));
                    break;
                }
                Err(error) => {
                    let canceled = error.kind == LlmErrorKind::Canceled;
                    let rejected = acc.rejection.is_some();
                    let error = trace.finish_error(
                        self.provider.as_ref(),
                        acc.rejection.unwrap_or(error),
                        request.stream,
                    );
                    self.record_route_failure(
                        node,
                        route,
                        attempt + 1,
                        routes.len(),
                        &error,
                        rejected && request.provider_web_search.is_some(),
                    )?;
                    if canceled {
                        return Err(StepError::Cancelled);
                    }
                    if !error.allows_next_route() || attempt + 1 == routes.len() {
                        return Err(error).step_err(&node.id);
                    }
                }
            }
        }
        let (provider_id, model_id, response, retry_decisions, rejection, canceled) =
            response.ok_or_else(|| StepError::failed(&node.id, "no LLM route was available"))?;
        let usage = response.usage.clone();
        // A requested search remains visible even if the provider omitted usage.
        let web_search = response.web_search.clone().or_else(|| {
            request
                .provider_web_search
                .as_ref()
                .map(|_| Default::default())
        });
        let web_search_calls = web_search.as_ref().and_then(|outcome| outcome.calls);
        let search_cost =
            self.web_search_cost_microusd(routes, &provider_id, &model_id, web_search_calls);
        let cost_microusd = self.cost_microusd(
            node,
            routes,
            &provider_id,
            &model_id,
            &usage,
            request.cache_retention,
            web_search_calls,
        )?;
        let mut event = json!({
            "node": node.id,
            "provider": provider_id,
            "model": model_id,
            "seed": request.seed,
            "reasoning_effort": request.reasoning_effort,
            "temperature": request.temperature,
            "top_p": request.top_p,
            "max_tokens": request.max_tokens,
            "stop_sequences": request.stop_sequences,
            "structured_output": request.structured_output,
            "tool_choice": request.tool_choice,
            "parallel_tool_calls": request.parallel_tool_calls,
            "verbosity": request.verbosity,
            "stream": request.stream,
            "tokens": usage,
            "stop_reason": response.stop,
            "cost_microusd": cost_microusd,
        });
        // The requested cache policy is the TTL cost basis: usage totals may
        // omit the TTL breakdown, so the requested tier rides with the call.
        // Defaults stay absent so older journals keep decoding.
        if request.cache_write == llm::CacheWritePolicy::Forbid {
            event["cache_write_policy"] = json!("forbid");
        }
        if request.cache_retention == Some(llm::CacheRetention::Extended) {
            event["cache_retention"] = json!("extended");
        }
        // Hosted-search usage carries a provider-specific count and charge;
        // unknown or unpriced usage remains unmetered, never free.
        if let Some(outcome) = &web_search {
            event["web_search"] = json!({"calls": outcome.calls, "cost_microusd": search_cost});
            // Omit secret-bearing strings without dropping numeric accounting.
            for (field, v) in [
                ("events", json!(outcome.events)),
                ("citations", json!(outcome.citations)),
            ] {
                if self.assert_value_absent(node, &v).is_ok() {
                    event["web_search"][field] = v;
                }
            }
        }
        merge_event_extra(&mut event, event_extra(&response.usage));
        event["retry_decisions"] = json!(retry_decisions);
        self.journal.event("llm_call", event).step_err(&node.id)?;
        if canceled {
            return Err(StepError::Cancelled);
        }
        // A rejected delta remains terminal even if the completion is clean.
        if let Some(error) = rejection {
            return Err(error).step_err(&node.id);
        }
        // Rejected payloads are still billable; report the scan after journaling.
        if response.stop != llm::StopReason::ModelContextWindowExceeded {
            self.scan_response(node, &response)?;
        }
        if self.budget.require_pricing && web_search.is_some() && search_cost.is_none() {
            let detail = web_search_calls.map_or(
                "unknown billable hosted search usage",
                |_| "billable hosted search units without a `web_search_call_cost_usd` price",
            );
            return Err(StepError::failed(
                &node.id,
                format!("model `{provider_id}/{model_id}` has {detail} for the run cost budget"),
            ));
        }
        self.check_budget()?;
        if response.stop == llm::StopReason::ModelContextWindowExceeded {
            return Err(StepError::ProviderContextExhausted {
                node: node.id.clone(),
            });
        }
        Ok(response)
    }

    fn check_budget(&self) -> Result<(), StepError> {
        let state = self.journal.state();
        let used = state
            .budget
            .tokens_input
            .saturating_add(state.budget.tokens_output);
        self.check_budget_values(used, state.budget.cost_microusd)
    }

    fn check_budget_values(&self, tokens: u64, cost: u64) -> Result<(), StepError> {
        for (resource, used, limit) in [
            ("tokens", tokens, self.budget.max_tokens),
            ("cost_microusd", cost, self.budget.max_cost_microusd),
        ] {
            if let Some(limit) = limit
                && used > limit
            {
                return Err(StepError::BudgetExceeded {
                    resource,
                    used,
                    limit,
                });
            }
        }
        Ok(())
    }

    fn record_route_failure(
        &self,
        node: &NodeDef,
        route: &ModelRef,
        attempt: usize,
        routes_total: usize,
        error: &LlmError,
        unknown_search: bool,
    ) -> Result<(), StepError> {
        let mut event = json!({
            "node": node.id,
            "provider": route.provider,
            "model": route.model,
            "attempt": attempt,
            "fallback_index": attempt.saturating_sub(1),
            "routes_total": routes_total,
            "kind": error.kind,
            "retry_decisions": error.decisions,
            "fallback_taken": attempt < routes_total && error.allows_next_route(),
        });
        if unknown_search {
            event["web_search"] = json!({"calls": null, "cost_microusd": null});
        }
        // Ordinary failures remain optional audit records; a search liability
        // changes the run budget and must survive audit filtering and replay.
        if unknown_search {
            self.journal.durable_event("llm_route_failed", event)
        } else {
            self.journal.event("llm_route_failed", event)
        }
        .step_err(&node.id)
    }

    async fn complete_route(
        &self,
        node: &NodeDef,
        request: ChatRequest,
        observer: llm::RetryObserver,
        acc: &mut StreamAccumulator,
    ) -> Result<ChatResponse, LlmError> {
        if !request.stream {
            return tokio::select! {
                _ = self.cancellation.cancelled() => Err(LlmError {
                    message: "LLM call canceled".into(),
                    kind: LlmErrorKind::Canceled,
                ..Default::default()
                }),
                response = self.provider.complete_observed(request, observer) => response,
            };
        }
        let provider_id = request.provider.clone();
        let model_id = request.model.clone();
        let (events, mut receiver) = tokio::sync::mpsc::channel(LLM_STREAM_CHANNEL_CAPACITY);
        let stream = self.provider.stream_observed(request, events, observer);
        tokio::pin!(stream);
        let record = |event, acc: &mut StreamAccumulator| {
            self.record_stream_event(node, &provider_id, &model_id, event, acc)
        };
        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    // Cancellation can beat a buffered completion, even before rejection;
                    // drain queued events first so available usage is settled before aborting.
                    receiver.close();
                    while let Ok(event) = receiver.try_recv() {
                        record(event, acc)?;
                    }
                    if let Some(response) = acc.response.take() {
                        acc.canceled = true;
                        return Ok(response);
                    }
                    return Err(LlmError {
                        message: "LLM stream canceled".into(),
                        kind: LlmErrorKind::Canceled,
                ..Default::default()
                    });
                }
                result = &mut stream => {
                    let canceled = result
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.kind == LlmErrorKind::Canceled);
                    receiver.close();
                    while let Ok(event) = receiver.try_recv() {
                        record(event, acc)?;
                    }
                    if acc.rejection.is_some() && let Some(response) = acc.response.take() {
                        acc.canceled = canceled;
                        return Ok(response);
                    }
                    if let Err(error) = result {
                        if acc.index > 0 {
                            return Err(LlmError {
                                message: format!(
                                    "LLM stream failed after {} emitted deltas; route fallback is unsafe: {error}", acc.index
                                ),
                                kind: LlmErrorKind::PartialStream,
                ..Default::default()
                            });
                        }
                        return Err(error);
                    }
                    return acc.response.take().ok_or_else(|| LlmError::new("LLM stream ended without a completed response"));
                }
                event = receiver.recv() => {
                    match event {
                        Some(event) => record(event, acc)?,
                        None => {
                            return acc.response.take().ok_or_else(|| LlmError::new("LLM stream channel closed before completion"));
                        }
                    }
                }
            }
        }
    }

    /// Scan events before publication, withholding any unverified suffix.
    fn record_stream_event(
        &self,
        node: &NodeDef,
        provider: &str,
        model: &str,
        event: ChatStreamEvent,
        acc: &mut StreamAccumulator,
    ) -> Result<(), LlmError> {
        // The first completion settles this operation; queued duplicates and
        // post-completion deltas cannot publish or settle it again.
        if acc.response.is_some() {
            return Ok(());
        }
        match event {
            ChatStreamEvent::TextDelta { text } => {
                if acc.rejection.is_some() {
                    return Ok(());
                }
                if self.secrets.is_empty() {
                    self.scan_text(node, &text)
                        .map_err(|error| LlmError::new(error.to_string()))?;
                    self.publish_delta(node, provider, model, &text, &mut acc.index)?;
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
                acc.pending.push_str(&text);
                let holdback = self.secrets.max_value_len().saturating_sub(1);
                let mut window = acc.published_tail.clone();
                window.push_str(&acc.pending);
                if let Err(error) = self.scan_text(node, &window) {
                    acc.reject(error);
                    return Ok(());
                }
                let mut publish_len = acc.pending.len().saturating_sub(holdback);
                while publish_len > 0 && !acc.pending.is_char_boundary(publish_len) {
                    publish_len = publish_len.saturating_sub(1);
                }
                if publish_len == 0 {
                    return Ok(());
                }
                let publish = acc.pending[..publish_len].to_string();
                // Drain before I/O so a journal failure cannot double
                // publish the same bytes on retry.
                acc.pending.drain(..publish_len);
                self.publish_delta(node, provider, model, &publish, &mut acc.index)?;
                acc.published_tail.push_str(&publish);
                let trimmed = holdback_tail(acc.published_tail.as_str(), holdback);
                acc.published_tail = trimmed;
            }
            ChatStreamEvent::Completed {
                response: completed,
            } => {
                if acc.rejection.is_none()
                    && !self.secrets.is_empty()
                    && completed.stop != llm::StopReason::ModelContextWindowExceeded
                {
                    // Final gate: withheld suffix stays unpublished and visible
                    // prefixes were jointly verified, so nothing recoverable remains.
                    // Rejected completions still need accounting via the common path.
                    if self.scan_response(node, &completed).is_err() {
                        acc.response = Some(completed);
                        return Ok(());
                    }
                    if !acc.pending.is_empty() {
                        let mut window = acc.published_tail.clone();
                        window.push_str(&acc.pending);
                        if let Err(error) = self.scan_text(node, &window) {
                            acc.reject(error);
                            acc.response = Some(completed);
                            return Ok(());
                        }
                        let flush = std::mem::take(&mut acc.pending);
                        self.publish_delta(node, provider, model, &flush, &mut acc.index)?;
                        acc.published_tail.clear();
                    }
                }
                acc.response = Some(completed);
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

#[cfg(test)]
mod decision_tests {
    use super::*;
    use camino::Utf8PathBuf;
    use llm::{DecisionRequest, LlmRouter};

    struct Fixture {
        journal: JournalWriter,
        root: Utf8PathBuf,
        provider: Arc<dyn LlmProvider>,
        credential_env: String,
        secrets: SecretStore,
        node: NodeDef,
        model: ModelRef,
    }

    impl Fixture {
        fn new() -> Self {
            let id = uuid::Uuid::now_v7().simple().to_string().to_uppercase();
            let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .unwrap()
                .join(format!("decision-gateway-{id}"));
            let credential_env = format!("DECISION_MISSING_{id}");
            assert!(std::env::var_os(&credential_env).is_none());
            let provider = Arc::new(
                LlmRouter::parse_text(&format!(
                    r#"
[[provider]]
id = "decisions"
api = "system_one"
base_url = "https://example.invalid/v1"
api_key_env = "{credential_env}"
"#
                ))
                .unwrap(),
            );
            Self {
                journal: JournalWriter::create(&root.join("journal.jsonl"), id, false, None)
                    .unwrap(),
                root,
                provider,
                credential_env,
                secrets: SecretStore::from_values(BTreeMap::from([(
                    "generator".into(),
                    "generator-secret-value".into(),
                )])),
                node: serde_json::from_value(json!({"id": "decide", "type": "llm.decide"}))
                    .unwrap(),
                model: ModelRef {
                    provider: "decisions".into(),
                    model: "decision-model".into(),
                    input_cost_per_million_usd: Some(1.0),
                    cache_creation_input_cost_per_million_usd: None,
                    cache_read_input_cost_per_million_usd: None,
                    output_cost_per_million_usd: Some(1.0),
                    web_search_call_cost_usd: None,
                    cost_tiers: Vec::new(),
                },
            }
        }

        fn gateway(&self) -> LlmGateway<'_> {
            LlmGateway::new(
                self.provider.clone(),
                &self.secrets,
                &self.journal,
                CancellationToken::new(),
                LlmCostBudget {
                    max_tokens: Some(16),
                    max_cost_microusd: Some(32),
                    require_pricing: true,
                },
                vec![],
            )
        }

        fn request(&self) -> DecisionRequest {
            serde_json::from_value(json!({
                "provider": self.model.provider, "model": self.model.model,
                "state": {"ready": true},
                "questions": {"accept": {"type": "noul", "instructions": "Accept?"}}
            }))
            .unwrap()
        }

        fn assert_released(&self) {
            assert!(
                !reservations()
                    .lock()
                    .unwrap()
                    .contains_key(&run_id_of(&self.journal))
            );
        }

        fn assert_no_events(&self) {
            assert_eq!(
                std::fs::metadata(self.root.join("journal.jsonl"))
                    .unwrap()
                    .len(),
                0
            );
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[tokio::test]
    async fn precancelled_decision_releases_reservation() {
        let fixture = Fixture::new();
        let gateway = fixture.gateway();
        gateway.cancellation.cancel();
        for _ in 0..2 {
            assert!(matches!(
                gateway
                    .decide(&fixture.node, fixture.request(), &fixture.model, 16)
                    .await,
                Err(StepError::Cancelled)
            ));
            fixture.assert_released();
        }
        fixture.assert_no_events();
    }

    #[tokio::test]
    async fn insufficient_run_tokens_reject_before_transport() {
        let fixture = Fixture::new();
        let mut gateway = fixture.gateway();
        gateway.budget.max_tokens = Some(15);
        assert!(matches!(
            gateway
                .decide(&fixture.node, fixture.request(), &fixture.model, 16)
                .await,
            Err(StepError::BudgetExceeded {
                resource: "tokens",
                used: 16,
                limit: 15
            })
        ));
        fixture.assert_no_events();
    }

    #[tokio::test]
    async fn unpriced_decision_refuses_cost_cap() {
        let fixture = Fixture::new();
        for (input, output) in [(None, None), (Some(1.0), None), (None, Some(1.0))] {
            let mut model = fixture.model.clone();
            model.input_cost_per_million_usd = input;
            model.output_cost_per_million_usd = output;
            let error = fixture
                .gateway()
                .decide(&fixture.node, fixture.request(), &model, 16)
                .await
                .unwrap_err();
            assert!(
                matches!(error, StepError::Failed { message, .. } if message.contains("pricing"))
            );
            fixture.assert_released();
        }
        fixture.assert_no_events();
    }

    #[tokio::test]
    async fn nonfinite_decision_prices_reject_before_transport() {
        let fixture = Fixture::new();
        for price in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            for input in [true, false] {
                let mut model = fixture.model.clone();
                if input {
                    model.input_cost_per_million_usd = Some(price);
                } else {
                    model.output_cost_per_million_usd = Some(price);
                }
                let error = fixture
                    .gateway()
                    .decide(&fixture.node, fixture.request(), &model, 16)
                    .await
                    .unwrap_err();
                let field = if input {
                    "input_cost_per_million_usd"
                } else {
                    "output_cost_per_million_usd"
                };
                assert!(
                    matches!(error, StepError::Failed { message, .. } if !message.contains(&fixture.credential_env)
                        && message.contains(field)
                        && message.contains("must be finite and non-negative"))
                );
                fixture.assert_released();
                fixture.assert_no_events();
            }
        }
    }

    #[tokio::test]
    async fn generator_secrets_reject_nested_state_keys_and_questions() {
        let fixture = Fixture::new();
        let secret = "generator-secret-value";
        for (pointer, value) in [
            ("/state", json!({"nested": [{"value": secret}]})),
            ("/state", json!({"nested": [{(secret): "safe"}]})),
            (
                "/questions/accept/instructions",
                json!({"nested": [secret]}),
            ),
            ("/questions/accept/instructions", json!({(secret): "safe"})),
            (
                "/questions",
                json!({(secret): {"type": "noul", "instructions": "Accept?"}}),
            ),
            (
                "/questions/accept",
                json!({"type": "noul", "instructions": "Accept?", "criteria": {"true": secret}}),
            ),
        ] {
            let mut value_request = serde_json::to_value(fixture.request()).unwrap();
            *value_request.pointer_mut(pointer).unwrap() = value;
            let request: DecisionRequest = serde_json::from_value(value_request).unwrap();
            request.validate().unwrap();
            let error = fixture
                .gateway()
                .decide(&fixture.node, request, &fixture.model, 16)
                .await
                .unwrap_err();
            assert!(matches!(error, StepError::Failed { message, .. }
                if message == "secret `generator` value was found in LLM context"));
            fixture.assert_released();
        }
        fixture.assert_no_events();
    }

    #[tokio::test]
    async fn provider_configuration_failures_release_shared_reservations() {
        let fixture = Fixture::new();
        let gateways = [fixture.gateway(), fixture.gateway()];
        for gateway in gateways.iter().cycle().take(4) {
            let error = gateway
                .decide(&fixture.node, fixture.request(), &fixture.model, 16)
                .await
                .unwrap_err();
            assert!(matches!(error, StepError::Failed { message, .. }
                if message.contains(&format!("set `{}` before running the generator", fixture.credential_env))));
            fixture.assert_released();
        }
        let budget = fixture.journal.state().budget;
        assert_eq!(
            (
                budget.tokens_input,
                budget.tokens_output,
                budget.cost_microusd
            ),
            (0, 0, 0)
        );
    }
}
