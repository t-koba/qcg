use crate::{JournalWriter, StepContext, StepError, StepOutcome};
use qcg_contract::NodeDef;
use qcg_contract::{NodeState, RetryPolicy, ValueBag};
use qcg_types::{FailureCode, FailureDetail};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Grace period for a timed-out node to settle cooperatively after its
/// node-scoped stop signal before the future is abandoned. Matches the
/// service shutdown and cancel deadlines so every layer converges on the
/// same bound (A09).
const NODE_TIMEOUT_GRACE_SECS: u64 = 5;

use super::checkpoint::pin_files;
use super::repair_support::failure_from_findings;
use super::replay::BudgetTracker;
use super::run_context::elapsed_limit_secs;
use super::types::{Engine, EngineError, RunContext, failure_code_for_error, with_failed_evidence};

/// Per-attempt bounds read once per admission and shared by the
/// backoff arm and every attempt arm, so all branches report one
/// configured limit instead of re-reading it per branch (E10).
/// `None` means no budget is configured. The backoff is read once here
/// and passed down, never re-read per retry (E10).
#[derive(Clone, Copy)]
struct AttemptBounds {
    timeout_secs: Option<u64>,
    elapsed_limit: Option<u64>,
    backoff_ms: u64,
}

/// Classifies a spawned parallel task that never returned an outcome.
/// Panic payloads keep their detail (downcast to string forms, with an
/// explicit unknown-payload case) instead of collapsing to a bare
/// "task failed"; cancellations never reach here (E10).
pub(crate) fn join_task_error(error: tokio::task::JoinError) -> EngineError {
    debug_assert!(!error.is_cancelled());
    match error.try_into_panic() {
        Ok(payload) => {
            if let Some(message) = payload.downcast_ref::<&str>() {
                EngineError::Step(StepError::failed(
                    "scheduler",
                    format!("parallel task panicked: {message}"),
                ))
            } else if let Some(message) = payload.downcast_ref::<String>() {
                EngineError::Step(StepError::failed(
                    "scheduler",
                    format!("parallel task panicked: {message}"),
                ))
            } else {
                EngineError::Step(StepError::failed(
                    "scheduler",
                    "parallel task panicked with a non-string payload",
                ))
            }
        }
        Err(error) => EngineError::Step(StepError::failed(
            "scheduler",
            format!("parallel task failed: {error}"),
        )),
    }
}

/// Combines every terminal failure of a parallel wave into one error.
/// Interactions (questions, confirmations) cannot join into a message, so
/// the first one wins while every failure is still journaled per node;
/// plain failures report joined so no sibling failure is silently
/// discarded (E10).
fn combine_terminal_errors(errors: Vec<EngineError>) -> Result<(), EngineError> {
    if errors.is_empty() {
        return Ok(());
    }
    let mut errors = errors;
    if let Some(pos) = errors.iter().position(|error| {
        matches!(
            error,
            EngineError::NeedsUser { .. } | EngineError::NeedsConfirm { .. }
        )
    }) {
        return Err(errors.remove(pos));
    }
    if errors.len() == 1 {
        return Err(errors.swap_remove(0));
    }
    Err(EngineError::Failed(
        errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; "),
    ))
}

/// Bounded cooperative grace after a fired deadline, shared by the
/// per-attempt node timeout and the run-wide elapsed deadline so the grace
/// bound can never diverge between them (E10). Returns the settled outcome
/// when the attempt finishes inside the window, else `None` (the caller
/// records the deadline error). Parent cancellation is classified by the
/// caller so every path agrees on who wins a race. Generic over the pinned
/// future so both the inner node future and the outer attempt future share
/// it without trait-object lifetime friction.
async fn await_grace_settlement<Fut>(
    execution: &mut std::pin::Pin<Box<Fut>>,
) -> Option<Result<StepOutcome, EngineError>>
where
    Fut: std::future::Future<Output = Result<StepOutcome, EngineError>> + ?Sized,
{
    tokio::select! {
        result = &mut *execution => Some(result),
        _ = tokio::time::sleep(std::time::Duration::from_secs(NODE_TIMEOUT_GRACE_SECS)) => None,
    }
}

/// Single shared cancel-race entry for both deadline paths (E11): a parent
/// cancel racing a fired deadline wins, so both the node-timeout and the
/// elapsed arms consult this one helper at grace entry (before awaiting
/// cooperative settlement) instead of diverging pre-checks. Returns true
/// when the parent cancellation already fired.
fn grace_entry_parent_cancelled(context: &RunContext) -> bool {
    context.cancellation.is_cancelled()
}

impl Engine {
    pub(crate) async fn execute_parallel_wave(
        &self,
        context: &RunContext,
        journal: &JournalWriter,
        vars: &mut ValueBag,
        states: &mut BTreeMap<String, NodeState>,
        budget: &mut BudgetTracker,
        nodes: Vec<NodeDef>,
    ) -> Result<(), EngineError> {
        let context = Arc::new(context.clone());
        let journal = Arc::new(journal.clone_for_parallel()?);
        let mut tasks = JoinSet::new();
        for node in &nodes {
            // Check the run-wide deadline before journaling start, matching
            // the sequential loop (E11). Budget is charged per attempt
            // inside the retry wrapper (unified rule, E10), not here.
            context.run_checkpoint()?;
            states.insert(node.id.clone(), NodeState::Running);
            journal.event(
                "step_started",
                json!({ "node": node.id, "type": node.kind.to_string(), "attempt": 1, "parallel": true }),
            )?;
            let engine = self.clone();
            let context = Arc::clone(&context);
            let journal = Arc::clone(&journal);
            let node = node.clone();
            let mut vars_snapshot = vars.clone();
            let mut task_budget = budget.clone();
            tasks.spawn(async move {
                let outcome = engine
                    .execute_node_with_retry(
                        &context,
                        &journal,
                        &mut vars_snapshot,
                        &mut task_budget,
                        &node,
                    )
                    .await;
                (node, outcome)
            });
        }

        let mut outcomes = BTreeMap::new();
        // Every terminal failure is collected and reported joined: keeping
        // only the first (via get_or_insert) would silently discard sibling
        // failures, and a panic payload must keep its detail instead of a
        // bare "task failed" (E10).
        let mut join_errors: Vec<EngineError> = Vec::new();
        // Fail-fast with deterministic settlement: the first terminal
        // failure or HITL suspension aborts siblings promptly instead of
        // letting side effects continue in the background. Aborted tasks
        // report cancellation and are ignored; completed steps replay on
        // resume. External cancellation aborts the whole wave.
        let mut abort_on_settle = false;
        // Full-quiescence invariant: this loop never exits while tasks
        // remain, on any path. Settlement below assumes no live writer can
        // still append, so a break-on-timeout here would let a wedged
        // sibling race settlement. Aborted tasks are drained to resolution;
        // late completions win explicitly through the outcome map, and a
        // missing outcome journals a scheduler failure instead of silently
        // dropping the node.
        loop {
            if tasks.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => {
                    tasks.abort_all();
                    while let Some(result) = tasks.join_next().await {
                        if let Ok((node, _)) = result {
                            states.insert(
                                node.id.clone(),
                                NodeState::Failed(FailureDetail::new(
                                    FailureCode::Canceled,
                                    "parallel wave canceled",
                                )),
                            );
                        }
                    }
                    return Err(EngineError::Canceled);
                }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    let Some(result) = result else { break };
                    match result {
                        Ok((node, outcome)) => {
                            let terminal = !matches!(&outcome, Ok(StepOutcome::Success { .. }));
                            outcomes.insert(node.id.clone(), (node, outcome));
                            if terminal && !abort_on_settle {
                                abort_on_settle = true;
                                tasks.abort_all();
                            }
                        }
                        Err(error) => {
                            if error.is_cancelled() {
                                // Sibling aborted after fail-fast; ignore.
                                continue;
                            }
                            join_errors.push(join_task_error(error));
                            if !abort_on_settle {
                                abort_on_settle = true;
                                tasks.abort_all();
                            }
                        }
                    }
                }
                else => break,
            }
        }

        let mut terminal_errors: Vec<EngineError> = join_errors;
        for node in nodes {
            let Some((node, outcome)) = outcomes.remove(&node.id) else {
                states.insert(
                    node.id.clone(),
                    NodeState::Failed(FailureDetail::new(
                        FailureCode::SchedulerFailed,
                        "parallel task did not report an outcome",
                    )),
                );
                let reason = FailureDetail::new(
                    FailureCode::SchedulerFailed,
                    "parallel task did not report an outcome",
                );
                journal.event(
                    "step_finished",
                    json!({ "node": node.id, "status": "failed", "reason": reason, "parallel": true }),
                )?;
                terminal_errors.push(EngineError::Step(StepError::failed(
                    "scheduler",
                    format!("parallel node `{}` did not report an outcome", node.id),
                )));
                continue;
            };
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    let reason =
                        FailureDetail::new(failure_code_for_error(&error), error.to_string());
                    states.insert(node.id.clone(), NodeState::Failed(reason.clone()));
                    journal.event(
                        "step_finished",
                        json!({ "node": node.id, "status": "failed", "reason": reason, "parallel": true }),
                    )?;
                    terminal_errors.push(error);
                    continue;
                }
            };
            match outcome {
                StepOutcome::Success { output, files } => {
                    let file_pins = pin_files(
                        &context.workspace,
                        &context.metadata,
                        &files,
                        &context.contract.manifest.runtime,
                        &context.checkpoint_accounting,
                    )?;
                    let output_name = super::types::output_name_for(&node);
                    vars.publish_step_output(&node.id, node.output.as_deref(), &output, None);
                    states.insert(node.id.clone(), NodeState::Success);
                    journal.event(
                        "step_finished",
                        json!({ "node": node.id, "status": "success", "files": file_pins, "output": output, "output_name": output_name, "parallel": true }),
                    )?;
                }
                StepOutcome::CheckFailed {
                    findings,
                    output,
                    files,
                } => {
                    let reason = failure_from_findings(&findings, FailureCode::CheckFailed);
                    let file_pins = pin_files(
                        &context.workspace,
                        &context.metadata,
                        &files,
                        &context.contract.manifest.runtime,
                        &context.checkpoint_accounting,
                    )?;
                    states.insert(node.id.clone(), NodeState::Failed(reason.clone()));
                    // Parallel check failures share the single failed-evidence
                    // helper with the sequential loop, so the journal keeps
                    // one `failed_output` / `failed_files` notation on every
                    // path instead of drifting to `output` / `files` (E06).
                    journal.event(
                        "step_finished",
                        with_failed_evidence(json!({ "node": node.id, "status": "check_failed", "findings": findings, "reason": reason, "parallel": true }), &output, &file_pins)?,
                    )?;
                }
                StepOutcome::NeedsUser { question } => {
                    // Parallel suspensions without a failed attempt still
                    // carry the unified keys (null plus an empty list) so the
                    // notation matches the failure paths (E06).
                    const NO_OUTPUT: Option<serde_json::Value> = None;
                    journal.event(
                        "step_finished",
                        with_failed_evidence(json!({ "node": node.id, "status": "needs_user", "question": question, "parallel": true }), &NO_OUTPUT, &[])?,
                    )?;
                    terminal_errors.push(EngineError::NeedsUser {
                        question_id: question.id.clone(),
                        question: Box::new(question),
                    });
                }
                StepOutcome::NeedsConfirm { confirm } => {
                    // `confirm_request` keeps its strict FOREIGN schema
                    // (`qcg_api::ConfirmRequestEventData` allows only
                    // `confirm` plus `parallel`): failed-evidence keys would
                    // be rejected at journal validation, so they are not
                    // attached here. See `with_failed_evidence`.
                    journal.event(
                        "confirm_request",
                        json!({ "node": node.id, "confirm": confirm, "parallel": true }),
                    )?;
                    terminal_errors.push(EngineError::NeedsConfirm {
                        confirm_id: confirm.id.clone(),
                        confirm: Box::new(confirm),
                    });
                }
            }
        }
        combine_terminal_errors(terminal_errors)
    }

    /// Execute a node honoring its declared retry policy. Only execution
    /// failures are retried; contract, budget, and cancellation errors fail
    /// fast. Cancellation during backoff aborts the wait immediately.
    pub(crate) async fn execute_node_with_retry<'a>(
        &'a self,
        context: &'a RunContext,
        journal: &'a JournalWriter,
        vars: &'a mut ValueBag,
        budget: &'a mut BudgetTracker,
        node: &'a NodeDef,
    ) -> Result<StepOutcome, EngineError> {
        self.execute_node_with_retry_charged(context, journal, vars, budget, node, true)
            .await
    }

    /// Retry wrapper with an explicit budget-charge switch (E10). Top-level
    /// nodes, parallel-wave nodes, and repair/regenerate attempts charge
    /// every attempt (unified rule: no free retries). Foreach children pass
    /// `charge = false` so the outer foreach node is charged once total and
    /// children share that budget without per-child consume (single-charge
    /// rule). Retry, timeout, and elapsed handling are identical in both
    /// cases; only budget accounting differs.
    pub(crate) async fn execute_node_with_retry_charged<'a>(
        &'a self,
        context: &'a RunContext,
        journal: &'a JournalWriter,
        vars: &'a mut ValueBag,
        budget: &'a mut BudgetTracker,
        node: &'a NodeDef,
        charge: bool,
    ) -> Result<StepOutcome, EngineError> {
        let retry: RetryPolicy = node.retry.clone().unwrap_or_default();
        // Contract validation rejects 0; a programmatically built node that
        // bypasses validation fails closed here instead of being silently
        // rewritten to 1 (E10).
        if retry.max_attempts == 0 || retry.max_attempts > qcg_policy::MAX_RETRY_ATTEMPTS {
            return Err(EngineError::Failed(format!(
                "node `{}` retry.max_attempts must be between 1 and {}",
                node.id,
                qcg_policy::MAX_RETRY_ATTEMPTS
            )));
        }
        let max_attempts = retry.max_attempts as usize;
        // Read once per admission: the backoff arm and every attempt arm
        // below report this same configured limit instead of re-reading
        // it per branch (E10). `None` means no budget is configured.
        let elapsed_limit = elapsed_limit_secs(&context.contract);
        let bounds = AttemptBounds {
            timeout_secs: retry.timeout_secs,
            elapsed_limit,
            backoff_ms: retry.backoff_ms,
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            // Unified budget rule (E10): every charged attempt consumes the
            // run-wide step budget, including ordinary retries. Foreach
            // children pass charge=false (single-charge outer only), all
            // other paths pass true so no path gets a free retry.
            if charge {
                budget.consume(&node.id)?;
            }
            let result = self
                .execute_attempt(context, journal, vars, budget, node, bounds)
                .await;
            // Retry ordinary failures and timeouts; the operation guard
            // on the next attempt enforces the indeterminate policy
            // (refuse without opt-in) and invocation separation. Refusals
            // never retry: re-guarding would refuse identically.
            match &result {
                Err(EngineError::Step(
                    error @ (StepError::Failed { .. } | StepError::TimedOut { .. }),
                )) if attempt < max_attempts => {
                    journal.event(
                        "step_retry",
                        json!({
                            "node": node.id,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "error": error.to_string(),
                        }),
                    )?;
                    // Uniform wait path (E10): even a zero backoff awaits
                    // the elapsed deadline and the cancellation signal
                    // before the next attempt, so `backoff_ms = 0` can
                    // neither sleep past a fired deadline nor spin past a
                    // cancel. `biased` makes a simultaneous cancel or
                    // deadline deterministic instead of dependent on poll
                    // order (E11). The backoff below is the single
                    // admission-time read in `bounds`, never re-read (E10).
                    let backoff =
                        tokio::time::sleep(std::time::Duration::from_millis(bounds.backoff_ms));
                    tokio::pin!(backoff);
                    if let Some(deadline) = context.elapsed_deadline {
                        // The hard elapsed deadline wins over backoff: a
                        // retry must not sleep past it.
                        tokio::select! {
                            biased;
                            _ = context.cancellation.cancelled() => {
                                return Err(EngineError::Canceled);
                            }
                            _ = tokio::time::sleep_until(deadline) => {
                                let Some(limit_secs) = elapsed_limit else {
                                    return Err(EngineError::Failed(
                                        "elapsed deadline fired without a configured elapsed limit".into(),
                                    ));
                                };
                                return Err(EngineError::Step(StepError::ElapsedExceeded {
                                    node: node.id.clone(),
                                    limit_secs,
                                }));
                            }
                            _ = &mut backoff => {}
                        }
                    } else {
                        tokio::select! {
                            biased;
                            _ = context.cancellation.cancelled() => {
                                return Err(EngineError::Canceled);
                            }
                            _ = &mut backoff => {}
                        }
                    }
                    continue;
                }
                _ => return result,
            }
        }
    }

    /// One execution attempt with its node scope, optional per-attempt
    /// timeout, and the run-wide elapsed deadline. The retry wrapper and
    /// repair/regenerate runs share it, so every entry applies the same
    /// NodeDef constraints (E10/E11).
    ///
    /// Enforcement uses the monotonic clock only: the run-wide deadline is
    /// a `tokio::time::Instant` captured at run start and the per-attempt
    /// timeout is a monotonic sleep. Wall-clock time appears solely in
    /// durable records (`budget.started_at`) and diagnostic messages, never
    /// in an enforcement decision, so an NTP step cannot stretch or shrink
    /// a timeout (E11).
    async fn execute_attempt<'a>(
        &'a self,
        context: &'a RunContext,
        journal: &'a JournalWriter,
        vars: &'a mut ValueBag,
        budget: &'a mut BudgetTracker,
        node: &'a NodeDef,
        bounds: AttemptBounds,
    ) -> Result<StepOutcome, EngineError> {
        // Node-scoped execution context, rebuilt every attempt: the
        // stop signal is always a child of the run token, so parent
        // cancellation reaches every path uniformly, while a node
        // timeout cancels only this attempt's scope and never the run
        // (B11). Gateways observe the same scope, so command, HTTP,
        // LLM, and MCP paths stop under identical semantics.
        let node_stop = context.cancellation.child_token();
        let mut node_context = context.clone();
        node_context.cancellation = node_stop.clone();
        node_context.cmd = context.cmd.clone().with_cancellation(node_stop.clone());
        node_context.http = context.http.clone().with_cancellation(node_stop.clone());
        // Evaluated once per admission by the caller: every deadline arm
        // below reports the same configured limit instead of re-reading
        // it per branch (E10). `None` means no budget is configured; arms
        // that fired a deadline handle it explicitly instead of
        // misreading 0 (E11).
        let attempt_execution = async {
            let mut execution =
                self.execute_node_after_budget(&node_context, journal, vars, budget, node);
            if let Some(timeout_secs) = bounds.timeout_secs {
                // On timeout the child is cancelled and the node gets a
                // bounded grace period to settle cooperatively; only then
                // is the future abandoned and the timeout recorded.
                // Explicit wait and settlement live in this layer instead
                // of a bare inner-future drop (A09). The grace wait is the
                // shared `await_grace_settlement` helper so both deadline
                // paths settle identically (E10).
                tokio::select! {
                    // The declared timeout wins a simultaneous completion so
                    // the outcome is deterministic (E11).
                    biased;
                    _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
                        node_stop.cancel();
                        // Unified cancel-race entry (E11): a parent cancel
                        // racing the deadline wins before cooperative grace,
                        // via the single shared helper used by both deadline
                        // paths.
                        if grace_entry_parent_cancelled(context) {
                            return Err(EngineError::Canceled);
                        }
                        match await_grace_settlement(&mut execution).await {
                            // The deadline fired and is remembered by this
                            // branch: a cooperative executor reports the
                            // child-scope stop as cancellation, which
                            // normalizes to TimedOut (retryable) unless an
                            // actual parent cancel wins (C06). Passing the
                            // raw cancellation through would misreport a
                            // cooperative timeout as a user cancel and skip
                            // the retry this policy declares.
                            Some(result) => {
                                if grace_entry_parent_cancelled(context) {
                                    result
                                } else if result
                                    .as_ref()
                                    .is_err_and(EngineError::is_canceled)
                                {
                                    Err(EngineError::Step(StepError::TimedOut {
                                        node: node.id.clone(),
                                        timeout_secs,
                                    }))
                                } else {
                                    result
                                }
                            }
                            None => {
                                // A parent cancel racing the deadline wins:
                                // reporting timeout for a canceled run
                                // would misclassify the outcome.
                                if grace_entry_parent_cancelled(context) {
                                    Err(EngineError::Canceled)
                                } else {
                                    Err(EngineError::Step(StepError::TimedOut {
                                        node: node.id.clone(),
                                        timeout_secs,
                                    }))
                                }
                            }
                        }
                    }
                    result = &mut execution => result,
                }
            } else {
                execution.await
            }
        };
        let mut attempt_execution = Box::pin(attempt_execution);
        // Run-wide elapsed deadline: stops the attempt with a distinct
        // error (never retried, never reported as a node timeout) so the
        // budget acts as a hard limit for any executor (E11).
        if let Some(deadline) = context.elapsed_deadline {
            tokio::select! {
                // The deadline wins a simultaneous completion: the limit is
                // hard, and cancellation is checked inside the deadline arm
                // (E11).
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    node_stop.cancel();
                    // Give the attempt the same bounded cooperative grace as
                    // the node timeout path, then record the elapsed error:
                    // an executor that settles on the child stop must not be
                    // misreported as a plain cancellation (E11). Cancel-race
                    // entry uses the single shared helper, matching the
                    // node-timeout arm above (E11).
                    if grace_entry_parent_cancelled(context) {
                        return Err(EngineError::Canceled);
                    }
                    let Some(limit_secs) = bounds.elapsed_limit else {
                        return Err(EngineError::Failed(
                            "elapsed deadline fired without a configured elapsed limit".into(),
                        ));
                    };
                    match await_grace_settlement(&mut attempt_execution).await {
                        Some(result) => {
                            if grace_entry_parent_cancelled(context) {
                                return Err(EngineError::Canceled);
                            }
                            // A grace-period interaction is preserved, not
                            // discarded: the human answer is still required
                            // to continue, so the suspension is delivered
                            // with an elapsed-exceeded marker instead of
                            // being dropped (E11). Any other cooperative
                            // finish loses to the already-fired hard limit:
                            // unlike a node timeout (an attempt budget whose
                            // cooperative finish is adopted), the elapsed
                            // limit is a run budget, so a post-limit success
                            // must not commit (E11). When the inner attempt
                            // already classified itself as a node timeout,
                            // both classifications are preserved: the inner
                            // timeout is journaled and the outer elapsed is
                            // returned, never overwriting one with the other
                            // (E10).
                            let is_interaction = matches!(
                                &result,
                                Ok(StepOutcome::NeedsUser { .. })
                                    | Ok(StepOutcome::NeedsConfirm { .. })
                            );
                            if is_interaction {
                                // Preserving the interaction does not preserve
                                // budget (E11): the elapsed limit has already
                                // fired and `elapsed_exceeded` is journaled
                                // alongside, so resuming still requires a new
                                // budget or limit extension — do not mistake
                                // the delivered suspension for remaining time.
                                journal.event(
                                    "elapsed_exceeded",
                                    json!({
                                        "node": node.id,
                                        "limit_secs": limit_secs,
                                        "during": "interaction",
                                    }),
                                )?;
                                return result;
                            }
                            if let Err(EngineError::Step(StepError::TimedOut {
                                timeout_secs: inner_timeout,
                                ..
                            })) = &result
                            {
                                journal.event(
                                    "step_timeout",
                                    json!({
                                        "node": node.id,
                                        "timeout_secs": inner_timeout,
                                        "during": "elapsed",
                                    }),
                                )?;
                            }
                            Err(EngineError::Step(StepError::ElapsedExceeded {
                                node: node.id.clone(),
                                limit_secs,
                            }))
                        }
                        None => {
                            if grace_entry_parent_cancelled(context) {
                                Err(EngineError::Canceled)
                            } else {
                                Err(EngineError::Step(StepError::ElapsedExceeded {
                                    node: node.id.clone(),
                                    limit_secs,
                                }))
                            }
                        }
                    }
                }
                result = &mut attempt_execution => result,
            }
        } else {
            attempt_execution.await
        }
    }

    /// Private by construction: every node execution must enter through
    /// `execute_node_with_retry` so retry, timeout, and elapsed policies
    /// cannot be bypassed (E10/E11).
    fn execute_node_after_budget<'a>(
        &'a self,
        context: &'a RunContext,
        journal: &'a JournalWriter,
        vars: &'a mut ValueBag,
        budget: &'a mut BudgetTracker,
        node: &'a NodeDef,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<StepOutcome, EngineError>> + Send + 'a>,
    > {
        Box::pin(async move {
            if self.is_foreach_node(node) {
                return self
                    .execute_foreach(context, journal, vars, budget, node)
                    .await;
            }
            self.execute_plain_node(context, journal, vars, node).await
        })
    }

    async fn execute_plain_node(
        &self,
        context: &RunContext,
        journal: &JournalWriter,
        vars: &mut ValueBag,
        node: &NodeDef,
    ) -> Result<StepOutcome, EngineError> {
        let executor = self
            .registry
            .get(&node.kind)
            .ok_or_else(|| StepError::failed(&node.id, "validated executor is missing"))?;
        let mut step_context = StepContext {
            run: context,
            journal,
            vars,
            llm: context.llm_provider.as_ref().map(|provider| {
                let budget = &context.contract.manifest.budget;
                let pricing = context
                    .contract
                    .manifest
                    .llm
                    .as_ref()
                    .map(|llm| {
                        llm.model
                            .clone()
                            .into_iter()
                            .chain(llm.models.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                crate::LlmGateway::new(
                    Arc::clone(provider),
                    &context.secrets,
                    journal,
                    context.cancellation.clone(),
                    qcg_policy::LlmCostBudget {
                        max_tokens: budget.max_tokens,
                        max_cost_microusd: budget.max_cost_usd.map(crate::step::usd_to_microusd),
                        require_pricing: budget.max_cost_usd.is_some(),
                    },
                    pricing,
                )
            }),
        };
        step_context.step_checkpoint(node).await?;
        Ok(executor.execute(&mut step_context, node).await?)
    }
}
