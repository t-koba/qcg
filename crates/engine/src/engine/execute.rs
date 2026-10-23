use crate::{JournalWriter, StepContext, StepError, StepOutcome};
use contract::NodeDef;
use contract::{NodeState, RetryPolicy, ValueBag};
use model::FailureCode;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Cooperative timeout grace; matches service shutdown/cancel bounds (A09).
const NODE_TIMEOUT_GRACE_SECS: u64 = 5;

use super::checkpoint::pin_attempt_files;
use super::repair_support::failure_from_findings;
use super::replay::BudgetTracker;
use super::run_context::elapsed_limit_secs;
use super::types::{
    Engine, EngineError, RunContext, failure_detail_for_error, with_failed_evidence,
};

/// Admission-time bounds shared by backoff and attempts; None means unconfigured (E10).
#[derive(Clone, Copy)]
struct AttemptBounds {
    timeout_secs: Option<u64>,
    elapsed_limit: Option<u64>,
    backoff_ms: u64,
}

/// The compatibility policy controls only whether terminal reports stop sibling work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParallelTerminalDecision {
    AbortAndDrain,
}

pub(super) fn parallel_terminal_decision(
    policy: policy::scheduling::ParallelCompatibility,
) -> ParallelTerminalDecision {
    match policy {
        policy::scheduling::ParallelCompatibility::FailFast => {
            ParallelTerminalDecision::AbortAndDrain
        }
    }
}

/// Preserve panic detail (including unknown payloads); callers handle cancellation (E10).
pub(crate) fn join_task_error(error: tokio::task::JoinError) -> EngineError {
    debug_assert!(!error.is_cancelled());
    let message = match error.try_into_panic() {
        Ok(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
            match message {
                Some(message) => format!("parallel task panicked: {message}"),
                None => "parallel task panicked with a non-string payload".into(),
            }
        }
        Err(error) => format!("parallel task failed: {error}"),
    };
    EngineError::Step(StepError::failed("scheduler", message))
}

/// Join all failures, but return the first interaction; each node still journals its outcome (E10).
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
    let resumable_agent = errors.iter().any(EngineError::is_resumable_agent);
    let combined = EngineError::Failed(
        errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; "),
    );
    // Ordinary sibling failures have already been settled per node. Do not
    // erase a continuation marker when joining their diagnostics.
    Err(if resumable_agent {
        EngineError::ResumableAgent(Box::new(combined))
    } else {
        combined
    })
}

/// Shared deadline grace returns cooperative outcomes or None; callers classify cancellation and expiry (E10/E11).
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

/// Parent cancellation wins at grace entry for both node and elapsed deadlines (E11).
fn grace_entry_parent_cancelled(context: &RunContext) -> bool {
    context.cancellation.is_cancelled()
}

// Tokio IDs only correlate joins in memory; persisted facts use node and never copy payload or authorize replay.
type WaveJoin =
    Result<(tokio::task::Id, (NodeDef, Result<StepOutcome, EngineError>)), tokio::task::JoinError>;

fn record_wave_join(
    journal: &JournalWriter,
    task_nodes: &mut BTreeMap<tokio::task::Id, String>,
    result: &WaveJoin,
    drain_cause: &str,
    abort_cause: &str,
) -> Result<(), EngineError> {
    let id = match result {
        Ok((id, _)) => *id,
        Err(error) => error.id(),
    };
    let node = task_nodes
        .remove(&id)
        .expect("registered parallel wave task");
    let mut record = json!({ "node": node, "drain_cause": drain_cause });
    let classification = match result {
        Ok((_, (_, Ok(outcome)))) => {
            let (kind, interaction) = match outcome {
                StepOutcome::Success { .. } => ("success", None),
                StepOutcome::CheckFailed { .. } => ("check_failed", None),
                StepOutcome::NeedsUser { question, .. } => {
                    ("needs_user", Some(("question_id", &question.id)))
                }
                StepOutcome::NeedsConfirm { confirm, .. } => {
                    ("needs_confirm", Some(("confirm_id", &confirm.id)))
                }
            };
            record["outcome"] = json!(kind);
            if let Some((key, id)) = interaction {
                record[key] = json!(id);
            }
            if matches!(outcome, StepOutcome::Success { .. }) {
                "completed_success"
            } else {
                "completed_terminal"
            }
        }
        Ok((_, (_, Err(error)))) => match error {
            EngineError::NeedsUser { question_id, .. } => {
                record["question_id"] = json!(question_id);
                "interaction_pending"
            }
            EngineError::NeedsConfirm { confirm_id, .. } => {
                record["confirm_id"] = json!(confirm_id);
                "interaction_pending"
            }
            error => {
                record["code"] = json!(failure_detail_for_error(error).code);
                if error.is_resumable_agent() {
                    record["continuation_owner"] = json!("agent");
                }
                if error.is_canceled() {
                    record["cancellation_cause"] = json!("reported_by_task");
                    "task_canceled"
                } else {
                    "task_error"
                }
            }
        },
        Err(error) if error.is_cancelled() => {
            record["cancellation_cause"] = json!(abort_cause);
            match abort_cause {
                "parent_cancellation" => "parent_canceled",
                "none" => "task_canceled",
                _ => "scheduler_abort",
            }
        }
        Err(error) if error.is_panic() => "task_panic",
        Err(_) => "task_join_error",
    };
    record["classification"] = json!(classification);
    journal.event("parallel_task_joined", record)?;
    Ok(())
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
        let mut nodes = nodes;
        let mut tasks = JoinSet::new();
        let mut task_nodes = BTreeMap::new();
        // Defer journal errors until all writers are quiescent.
        let mut fact_result = Ok(());
        let mut outcomes = BTreeMap::new();
        let mut join_errors: Vec<EngineError> = Vec::new();
        let mut abort_on_settle = false;
        let mut parent_cancelled = false;
        let mut start_error = None;
        let mut started_count = 0usize;
        for node in &nodes {
            // Check elapsed before start (E11); charge attempts in the retry wrapper (E10).
            let started = context.run_checkpoint().and_then(|()| {
                states.insert(node.id.clone(), NodeState::Running);
                journal
                    .event(
                        "step_started",
                        json!({ "node": node.id, "type": node.kind.to_string(), "attempt": 1, "parallel": true }),
                    )
                    .map_err(EngineError::from)
            });
            if let Err(error) = started {
                let cause = if error.is_canceled() {
                    "parent_cancellation"
                } else {
                    "wave_start_failure"
                };
                states.insert(node.id.clone(), NodeState::Pending);
                match parallel_terminal_decision(context.parallel_compatibility) {
                    ParallelTerminalDecision::AbortAndDrain => tasks.abort_all(),
                }
                while let Some(result) = tasks.join_next_with_id().await {
                    let fact = record_wave_join(&journal, &mut task_nodes, &result, cause, cause);
                    fact_result = fact_result.and(fact);
                    if let Ok((_, (node, outcome))) = result {
                        outcomes.insert(node.id.clone(), (node, outcome));
                    }
                }
                start_error = Some(error);
                break;
            }
            let engine = self.clone();
            let context = Arc::clone(&context);
            let journal = Arc::clone(&journal);
            let node = node.clone();
            let mut vars_snapshot = vars.clone();
            let mut task_budget = budget.clone();
            let node_id = node.id.clone();
            let task = tasks.spawn(async move {
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
            task_nodes.insert(task.id(), node_id);
            started_count += 1;
        }
        if start_error.is_some() {
            nodes.truncate(started_count);
        }

        // Abort siblings on the first terminal outcome; external cancellation aborts all.
        // Join facts retain unresolved execution separately from settlement.
        // Drain to quiescence; late outcomes win, missing reports prove neither failure nor replay safety.
        loop {
            if tasks.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => {
                    tasks.abort_all();
                    // Preserve returned outcomes through the normal per-owner settlement.
                    while let Some(result) = tasks.join_next_with_id().await {
                        let fact = record_wave_join(&journal, &mut task_nodes, &result, "parent_cancellation",
                            if abort_on_settle { "fail_fast" } else { "parent_cancellation" });
                        fact_result = fact_result.and(fact);
                        if let Ok((_, (node, outcome))) = result {
                            outcomes.insert(node.id.clone(), (node, outcome));
                        }
                    }
                    parent_cancelled = true;
                    break;
                }
                result = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    let Some(result) = result else { break };
                    let fact = record_wave_join(&journal, &mut task_nodes, &result,
                        if abort_on_settle { "fail_fast" } else { "none" },
                        if abort_on_settle { "fail_fast" } else { "none" });
                    fact_result = fact_result.and(fact);
                    let terminal = match result {
                        Ok((_, (node, outcome))) => {
                            let terminal = !matches!(&outcome, Ok(StepOutcome::Success { .. }));
                            outcomes.insert(node.id.clone(), (node, outcome));
                            terminal
                        }
                        Err(error) if error.is_cancelled() => {
                            // Unrequested cancellation starts fail-fast; a sibling abort does not.
                            if abort_on_settle { continue; }
                            join_errors.push(EngineError::Step(StepError::Refused {
                                node: "scheduler".into(),
                                message: "parallel task cancellation had no parent or fail-fast cause".into(),
                            }));
                            true
                        }
                        Err(error) => {
                            join_errors.push(join_task_error(error));
                            true
                        }
                    };
                    if terminal && !abort_on_settle {
                        abort_on_settle = true;
                        match parallel_terminal_decision(context.parallel_compatibility) {
                            ParallelTerminalDecision::AbortAndDrain => tasks.abort_all(),
                        }
                    }
                }
                else => break,
            }
        }

        fact_result?;
        let mut terminal_errors: Vec<EngineError> = join_errors;
        // Reported cancellations accompanying an interaction remain interruptions.
        let hitl_pending = outcomes.values().any(|(_, outcome)| {
            matches!(
                outcome,
                Ok(StepOutcome::NeedsUser { .. }) | Ok(StepOutcome::NeedsConfirm { .. })
            )
        });
        for node in nodes {
            let Some((node, outcome)) = outcomes.remove(&node.id) else {
                states.insert(node.id.clone(), NodeState::Pending);
                journal.event(
                    "step_interrupted",
                    json!({ "node": node.id, "reason": "parallel task did not report an outcome; effects unknown", "parallel": true }),
                )?;
                continue;
            };
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    // A scheduler-aborted sibling is an interruption, not a failure (F12).
                    if hitl_pending && error.is_canceled() {
                        states.insert(node.id.clone(), NodeState::Pending);
                        journal.event(
                            "step_interrupted",
                            json!({ "node": node.id, "reason": "parallel sibling canceled for user input; will resume", "parallel": true }),
                        )?;
                        continue;
                    }
                    // Explicit continuations defer to sequential re-entry; node kind
                    // alone is not continuation evidence and never synthesizes one.
                    if error.is_resumable_agent() {
                        states.insert(node.id.clone(), NodeState::Pending);
                        journal.event(
                            "step_interrupted",
                            json!({ "node": node.id, "reason": "agent failure defers to sequential re-entry; will resume", "parallel": true }),
                        )?;
                        terminal_errors.push(error);
                        continue;
                    }
                    let reason = failure_detail_for_error(&error);
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
                    let file_pins = pin_attempt_files(context.as_ref(), &files)?;
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
                    let file_pins = pin_attempt_files(context.as_ref(), &files)?;
                    states.insert(node.id.clone(), NodeState::Failed(reason.clone()));
                    // Use the same failed_output/failed_files notation as sequential settlement (E06).
                    journal.event(
                        "step_finished",
                        with_failed_evidence(json!({ "node": node.id, "status": "check_failed", "findings": findings, "reason": reason, "parallel": true }), &output, &file_pins)?,
                    )?;
                }
                StepOutcome::NeedsUser { question, files } => {
                    // Suspensions carry null/empty failed evidence; produced files are pinned (E06).
                    const NO_OUTPUT: Option<serde_json::Value> = None;
                    let file_pins = pin_attempt_files(context.as_ref(), &files)?;
                    journal.event(
                        "step_finished",
                        with_failed_evidence(json!({ "node": node.id, "status": "needs_user", "question": question, "files": file_pins, "parallel": true }), &NO_OUTPUT, &[])?,
                    )?;
                    terminal_errors.push(EngineError::NeedsUser {
                        question_id: question.id.clone(),
                        question: Box::new(question),
                    });
                }
                StepOutcome::NeedsConfirm { confirm, files } => {
                    // The strict confirm_request schema excludes failed evidence. Pin produced files.
                    let file_pins = pin_attempt_files(context.as_ref(), &files)?;
                    journal.event(
                        "confirm_request",
                        json!({ "node": node.id, "confirm": confirm, "files": file_pins, "parallel": true }),
                    )?;
                    terminal_errors.push(EngineError::NeedsConfirm {
                        confirm_id: confirm.id.clone(),
                        confirm: Box::new(confirm),
                    });
                }
            }
        }
        if parent_cancelled {
            return Err(EngineError::Canceled);
        }
        start_error.map_or_else(|| combine_terminal_errors(terminal_errors), Err)
    }

    /// Run a node under its retry policy: only execution failures retry; other classes fail fast, and cancellation interrupts backoff.
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

    /// Shared retry/deadline wrapper; foreach children share their parent's single charge (E10).
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
        // Reject invalid programmatic bounds; never rewrite zero to one (E10).
        if retry.max_attempts == 0 || retry.max_attempts > policy::MAX_RETRY_ATTEMPTS {
            return Err(EngineError::Failed(format!(
                "node `{}` retry.max_attempts must be between 1 and {}",
                node.id,
                policy::MAX_RETRY_ATTEMPTS
            )));
        }
        let max_attempts = retry.max_attempts as usize;
        // Read limits once per admission; every arm uses them and None means unconfigured (E10).
        let elapsed_limit = elapsed_limit_secs(&context.contract);
        let bounds = AttemptBounds {
            timeout_secs: retry.timeout_secs,
            elapsed_limit,
            backoff_ms: retry.backoff_ms,
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            // Persist each charged attempt; foreach children share the outer charge, not step counts (F13).
            if charge {
                budget.consume(&node.id)?;
                journal.event("budget_charged", json!({ "node": node.id, "amount": 1 }))?;
            }
            let result = self
                .execute_attempt(context, journal, vars, budget, node, bounds)
                .await;
            // Retry failures/timeouts only; indeterminate effects need opt-in and refusals never retry (E10).
            match &result {
                Err(EngineError::Step(
                    error @ (StepError::Failed { .. }
                    | StepError::TimedOut { .. }
                    | StepError::AgentIterationsExhausted { .. }
                    | StepError::AgentResourceExhausted { .. }),
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
                    // Check elapsed/cancel even for zero backoff; biased polling resolves races (E10/E11).
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

    /// One attempt applies node scope, optional timeout and run elapsed deadline; retries and repair/regeneration share it (E10/E11).
    /// Deadlines use monotonic `tokio::time::Instant`/sleep; wall-clock records are diagnostic only and cannot alter expiry (E11).
    async fn execute_attempt<'a>(
        &'a self,
        context: &'a RunContext,
        journal: &'a JournalWriter,
        vars: &'a mut ValueBag,
        budget: &'a mut BudgetTracker,
        node: &'a NodeDef,
        bounds: AttemptBounds,
    ) -> Result<StepOutcome, EngineError> {
        // Rebuild a child token per attempt: parent cancel reaches every gateway; a node timeout cancels only this attempt (B11).
        let node_stop = context.cancellation.child_token();
        let mut node_context = context.clone();
        node_context.cancellation = node_stop.clone();
        node_context.cmd = context.cmd.clone().with_cancellation(node_stop.clone());
        node_context.http = context.http.clone().with_cancellation(node_stop.clone());
        // Deadline arms share the admission-time limit; None is unconfigured and 0 is never inferred (E10/E11).
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
                    policy::LlmCostBudget {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combined_wave_errors_preserve_resumable_agent_marker() {
        let error = combine_terminal_errors(vec![
            EngineError::ResumableAgent(Box::new(StepError::failed("agent", "agent error").into())),
            StepError::failed("sibling", "sibling error").into(),
        ])
        .expect_err("wave must fail");
        assert!(error.is_resumable_agent());
        assert!(error.to_string().contains("agent error"));
        assert!(error.to_string().contains("sibling error"));
    }
}

#[cfg(test)]
mod parallel_fact_payloads {
    include!("tests/parallel_fact_payloads.rs");
}
