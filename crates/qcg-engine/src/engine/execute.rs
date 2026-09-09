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
use super::types::{Engine, EngineError, RunContext};

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
            budget.consume(&node.id)?;
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
        let mut join_error = None;
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
                            join_error.get_or_insert_with(|| {
                                EngineError::Step(StepError::failed(
                                    "scheduler",
                                    format!("parallel task failed: {error}"),
                                ))
                            });
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

        let mut terminal_error = join_error;
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
                terminal_error.get_or_insert_with(|| {
                    EngineError::Step(StepError::failed(
                        "scheduler",
                        format!("parallel node `{}` did not report an outcome", node.id),
                    ))
                });
                continue;
            };
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    let reason = FailureDetail::execution(error.to_string());
                    states.insert(node.id.clone(), NodeState::Failed(reason.clone()));
                    journal.event(
                        "step_finished",
                        json!({ "node": node.id, "status": "failed", "reason": reason, "parallel": true }),
                    )?;
                    if terminal_error.is_none() {
                        terminal_error = Some(error);
                    }
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
                    let output_name = node.output.as_deref().unwrap_or(&node.id);
                    if let Some(output_name) = &node.output {
                        if let Some(value) = output.clone() {
                            vars.set_step_output(output_name, value);
                        }
                    } else if let Some(value) = output.clone() {
                        vars.set_step_output(&node.id, value);
                    }
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
                    journal.event(
                        "step_finished",
                        json!({ "node": node.id, "status": "check_failed", "findings": findings, "reason": reason, "output": output, "files": file_pins, "parallel": true }),
                    )?;
                }
                StepOutcome::NeedsUser { question } => {
                    journal.event(
                        "step_finished",
                        json!({ "node": node.id, "status": "needs_user", "question": question, "parallel": true }),
                    )?;
                    if terminal_error.is_none() {
                        terminal_error = Some(EngineError::NeedsUser {
                            question_id: question.id.clone(),
                            question: Box::new(question),
                        });
                    }
                }
                StepOutcome::NeedsConfirm { confirm } => {
                    journal.event(
                        "confirm_request",
                        json!({ "node": node.id, "confirm": confirm, "parallel": true }),
                    )?;
                    if terminal_error.is_none() {
                        terminal_error = Some(EngineError::NeedsConfirm {
                            confirm_id: confirm.id.clone(),
                            confirm: Box::new(confirm),
                        });
                    }
                }
            }
        }
        terminal_error.map_or(Ok(()), Err)
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
        let retry: RetryPolicy = node.retry.clone().unwrap_or_default();
        let max_attempts = retry.max_attempts.max(1) as usize;
        let mut attempt = 0;
        loop {
            attempt += 1;
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
            let result = if let Some(timeout_secs) = retry.timeout_secs {
                // On timeout the child is cancelled and the node gets a
                // bounded grace period to settle cooperatively; only then
                // is the future abandoned and the timeout recorded.
                // Explicit wait and settlement live in this layer instead
                // of a bare inner-future drop (A09).
                let mut execution =
                    self.execute_node_after_budget(&node_context, journal, vars, budget, node);
                tokio::select! {
                    result = &mut execution => result,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
                        node_stop.cancel();
                        tokio::select! {
                            result = &mut execution => result,
                            _ = tokio::time::sleep(std::time::Duration::from_secs(
                                NODE_TIMEOUT_GRACE_SECS,
                            )) => {
                                // A parent cancel racing the deadline wins:
                                // reporting timeout for a canceled run
                                // would misclassify the outcome.
                                if context.cancellation.is_cancelled() {
                                    Err(EngineError::Canceled)
                                } else {
                                    Err(EngineError::Step(StepError::TimedOut {
                                        node: node.id.clone(),
                                        timeout_secs,
                                    }))
                                }
                            },
                        }
                    }
                }
            } else {
                self.execute_node_after_budget(&node_context, journal, vars, budget, node)
                    .await
            };
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
                    if retry.backoff_ms > 0 {
                        tokio::select! {
                            _ = tokio::time::sleep(std::time::Duration::from_millis(retry.backoff_ms)) => {}
                            _ = context.cancellation.cancelled() => {
                                return Err(EngineError::Canceled);
                            }
                        }
                    }
                    continue;
                }
                _ => return result,
            }
        }
    }

    pub(crate) fn execute_node_after_budget<'a>(
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

    pub(crate) async fn execute_node(
        &self,
        context: &RunContext,
        journal: &JournalWriter,
        vars: &mut ValueBag,
        budget: &mut BudgetTracker,
        node: &NodeDef,
    ) -> Result<StepOutcome, EngineError> {
        budget.consume(&node.id)?;
        self.execute_node_after_budget(context, journal, vars, budget, node)
            .await
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
        step_context.checkpoint().await?;
        Ok(executor.execute(&mut step_context, node).await?)
    }
}
