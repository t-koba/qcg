use crate::{JournalWriter, StepControlFlow, StepError, StepOutcome};
use camino::Utf8PathBuf;
use qcg_contract::NodeDef;
use qcg_contract::ValueBag;
use qcg_types::{FailureCode, FailureDetail, NodePath};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::task::JoinSet;

use super::checkpoint::{existing_regular_files, pin_files};
use super::repair_support::failure_from_findings;
use super::replay::BudgetTracker;
use super::types::{
    Engine, EngineError, ForeachControlParams, ForeachIteration, RunContext, with_failed_evidence,
};
use qcg_policy::{MAX_FOREACH_ITERATIONS, MAX_FOREACH_PARALLELISM};

impl Engine {
    pub(crate) async fn execute_foreach(
        &self,
        context: &RunContext,
        journal: &JournalWriter,
        vars: &mut ValueBag,
        budget: &mut BudgetTracker,
        node: &NodeDef,
    ) -> Result<StepOutcome, EngineError> {
        let params: ForeachControlParams = node.deserialize_params().map_err(|error| {
            StepError::failed(&node.id, format!("invalid foreach params: {error}"))
        })?;
        if params.items.trim().is_empty() {
            return Err(StepError::failed(&node.id, "foreach items is required").into());
        }
        if params.subflow.trim().is_empty() {
            return Err(StepError::failed(&node.id, "foreach subflow is required").into());
        }
        if !(1..=MAX_FOREACH_ITERATIONS).contains(&params.max_iterations) {
            return Err(StepError::failed(
                &node.id,
                format!("foreach max_iterations must be from 1 through {MAX_FOREACH_ITERATIONS}"),
            )
            .into());
        }
        if !(1..=MAX_FOREACH_PARALLELISM).contains(&params.parallel) {
            return Err(StepError::failed(
                &node.id,
                format!("foreach parallel must be from 1 through {MAX_FOREACH_PARALLELISM}"),
            )
            .into());
        }
        let items_ref = params.items.as_str();
        // Plain dotted paths resolve by direct lookup. Anything else
        // (for example `sort(inputs.tags)`) evaluates as an expression.
        let items_value = match vars.get_path(items_ref) {
            Some(value) => value.clone(),
            None => vars.eval_value(items_ref).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("foreach items `{items_ref}` was not found: {error}"),
                )
            })?,
        };
        let mut items = match &items_value {
            Value::Array(items) => items.clone(),
            Value::Object(items) => items
                .iter()
                .map(|(key, value)| json!({ "key": key, "value": value }))
                .collect(),
            _ => {
                return Err(StepError::failed(
                    &node.id,
                    format!("foreach items `{items_ref}` is not an array or object"),
                )
                .into());
            }
        };
        let requested_item_count = items.len();
        let max_iterations = params.max_iterations;
        if items.len() > max_iterations {
            items.truncate(max_iterations);
            journal.event(
                "foreach_budget_exhausted",
                json!({
                    "node": node.id,
                    "requested_iterations": requested_item_count,
                    "executed_iterations": items.len(),
                    "max_iterations": max_iterations,
                }),
            )?;
        }
        let item_count = items.len();
        let subflow = params.subflow.as_str();
        let block = context
            .contract
            .manifest
            .blocks
            .get(subflow)
            .ok_or_else(|| StepError::failed(&node.id, format!("unknown subflow `{subflow}`")))?;
        // Parallel iterations share the workspace, the journal, and the
        // run-wide budget atomics (shared-budget/shared-journal, E10): there
        // is no per-iteration isolation for side effects, only for the
        // variable scope cloned per iteration. Parallel-unsafe leaf children
        // are therefore refused here exactly like top-level parallel waves
        // refuse them: every current executor is parallel-safe, so this gate
        // changes nothing today and future-proofs against executors that opt
        // out (E10). Fail-closed for unknown kinds (E10): an unregistered
        // step type has no traits to prove safety, so it forces sequential
        // exactly like the top-level wave gate does.
        // Sequential iterations need no gate: one child runs at a time.
        // Nested foreach children are exempt: they re-enter this same
        // gated path for their own leaves, so gating them here would
        // forbid all nesting instead of unsafe leaves. `on_fail` targets
        // are part of this gate (E10): a child with repair/regenerate/route
        // routing executes extra nodes whose parallel-safety must also
        // hold; conservatively any `on_fail` forces sequential, matching
        // the top-level `is_parallel_safe_node` rule (`on_fail.is_none()`).
        if params.parallel > 1 && item_count > 1 {
            for child in block.iter() {
                let traits = self.registry.traits(&child.kind);
                let leaf_unsafe = match traits {
                    None => true,
                    Some(traits) => {
                        !traits.parallel_safe && traits.control_flow == StepControlFlow::Plain
                    }
                };
                if leaf_unsafe {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "foreach block child `{}` is not parallel-safe; use parallel = 1",
                            child.id
                        ),
                    )
                    .into());
                }
                if child.on_fail.is_some() {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "foreach block child `{}` declares on_fail routing; parallel execution requires parallel = 1",
                            child.id
                        ),
                    )
                    .into());
                }
            }
        }

        let mut files = Vec::new();
        if params.parallel == 1 || item_count <= 1 {
            for (index, item) in items.into_iter().enumerate() {
                // The sequential path clones and absorbs exactly like the
                // parallel path: each iteration runs in an isolated variable
                // scope (ValueBag clone) while sharing the workspace, the
                // journal, and the run-wide budget with all siblings (E10).
                // Only successful iterations contribute their outputs to the
                // parent scope, so the two paths can never diverge through
                // shared-mutation ordering (E10).
                let mut iteration_vars = vars.clone();
                let (iteration_files, outcome) = self
                    .execute_foreach_iteration(
                        context,
                        journal,
                        &mut iteration_vars,
                        budget,
                        ForeachIteration {
                            node,
                            block,
                            index,
                            item,
                        },
                    )
                    .await?;
                // Only successful iterations contribute outputs to the
                // parent scope; a failed or suspended child must not pollute
                // it (E10). Early returns preserve accumulated and failed
                // files in the returned `CheckFailed` (E10): dropping them
                // would lose the failed revision for exhaustion evidence.
                // Per-iteration file pins already journaled as
                // `step_finished` events persist for resume verification.
                if outcome.is_none() {
                    vars.absorb(&iteration_vars);
                }
                files.extend(iteration_files);
                if let Some(outcome) = outcome {
                    let outcome = match outcome {
                        StepOutcome::CheckFailed {
                            findings,
                            output,
                            files: failed_files,
                        } => {
                            // `iteration_files` were already extended above;
                            // `failed_files` from the iteration already
                            // contains its own accumulated files (see the
                            // iteration return below), so union without
                            // duplication: outer `files` is authoritative. The
                            // per-iteration binding is intentionally dropped (not
                            // silently ignored) to keep the outer accumulator
                            // authoritative (E10).
                            drop(failed_files);
                            StepOutcome::CheckFailed {
                                findings,
                                output,
                                files: files.clone(),
                            }
                        }
                        other => other,
                    };
                    return Ok(outcome);
                }
            }
        } else {
            let context = Arc::new(context.clone());
            let journal = Arc::new(journal.clone_for_parallel()?);
            let block = Arc::new(block.clone());
            let mut tasks = JoinSet::new();
            let mut pending = items.into_iter().enumerate();
            loop {
                while tasks.len() < params.parallel {
                    let Some((index, item)) = pending.next() else {
                        break;
                    };
                    let engine = self.clone();
                    let context = Arc::clone(&context);
                    let journal = Arc::clone(&journal);
                    let block = Arc::clone(&block);
                    let mut iteration_vars = vars.clone();
                    // Shared budget atomics, not an isolated budget: the
                    // clone shares the run-wide counters, so every iteration
                    // charges the same budget (E10).
                    let mut iteration_budget = budget.clone();
                    let foreach_node = node.clone();
                    tasks.spawn(async move {
                        let outcome = engine
                            .execute_foreach_iteration(
                                &context,
                                &journal,
                                &mut iteration_vars,
                                &mut iteration_budget,
                                ForeachIteration {
                                    node: &foreach_node,
                                    block: &block,
                                    index,
                                    item,
                                },
                            )
                            .await;
                        (iteration_vars, outcome)
                    });
                }
                let Some(result) = tasks.join_next().await else {
                    break;
                };
                match result {
                    // A failed or suspended iteration fails the loop fast:
                    // remaining iterations are aborted and drained before the
                    // outcome is returned (E10). Already-completed sibling
                    // results are still absorbed first: the budget they
                    // consumed stays consumed and their outputs are
                    // preserved instead of silently discarded (E10).
                    Ok((iteration_vars, Ok((iteration_files, Some(outcome))))) => {
                        // Match the sequential path: a failed or suspended
                        // iteration drops its scope instead of polluting the
                        // parent; only completed sibling outputs merged by
                        // the drain below are preserved (E10). Early returns
                        // preserve accumulated and failed files in the
                        // returned `CheckFailed` (E10).
                        drop(iteration_vars);
                        files.extend(iteration_files);
                        drain_foreach_tasks(&mut tasks, vars, &mut files, &journal, &node.id)
                            .await?;
                        let outcome = match outcome {
                            StepOutcome::CheckFailed {
                                findings,
                                output,
                                files: _,
                            } => StepOutcome::CheckFailed {
                                findings,
                                output,
                                files: files.clone(),
                            },
                            other => other,
                        };
                        return Ok(outcome);
                    }
                    // Only successful iterations contribute outputs to the
                    // parent scope; a failed child must not pollute it (E10).
                    Ok((iteration_vars, Ok((iteration_files, None)))) => {
                        vars.absorb(&iteration_vars);
                        files.extend(iteration_files);
                    }
                    Ok((_iteration_vars, Err(error))) => {
                        drain_foreach_tasks(&mut tasks, vars, &mut files, &journal, &node.id)
                            .await?;
                        return Err(error);
                    }
                    Err(error) => {
                        drain_foreach_tasks(&mut tasks, vars, &mut files, &journal, &node.id)
                            .await?;
                        let classified = if error.is_cancelled() {
                            EngineError::Canceled
                        } else {
                            // Preserve the panic payload like the wave path
                            // instead of collapsing it to a bare message
                            // (E10).
                            super::execute::join_task_error(error)
                        };
                        return Err(classified);
                    }
                }
            }
        }
        let files = existing_regular_files(files)?;
        Ok(StepOutcome::Success {
            output: Some(json!({
                "iterations": item_count,
                "requested_iterations": requested_item_count,
                "truncated": item_count < requested_item_count,
            })),
            files,
        })
    }

    async fn execute_foreach_iteration(
        &self,
        context: &RunContext,
        journal: &JournalWriter,
        vars: &mut ValueBag,
        budget: &mut BudgetTracker,
        iteration: ForeachIteration<'_>,
    ) -> Result<(Vec<Utf8PathBuf>, Option<StepOutcome>), EngineError> {
        let ForeachIteration {
            node,
            block,
            index,
            item,
        } = iteration;
        let mut files = Vec::new();
        context.run_checkpoint()?;
        let parent_item = vars.item().cloned();
        vars.set_item(Some(item));
        // Checkpoint events name the node block id (the static block child
        // id), not a runtime display name: resume addresses each iteration
        // by its per-iteration path derived from the block id (E10).
        journal.event(
            "foreach_iteration",
            json!({ "node": node.id, "index": index }),
        )?;
        let foreach_path = NodePath::root(node.id.clone());
        for block_node in block {
            context.run_checkpoint()?;
            let block_path = foreach_path.foreach_child(index, &block_node.id);
            let mut addressed_node = block_node.clone();
            // Specified iteration namespacing (E10): the block node's id is
            // rewritten to its per-iteration path (`foreach[i].child`), so
            // step outputs from different iterations never collide in the
            // parent scope and resume can address each iteration's output
            // independently. Clearing `output` pins the step output under
            // that rewritten id: the block author's output alias (if any)
            // must not leak iteration outputs into the shared namespace
            // under a stable name that later iterations would overwrite.
            addressed_node.id = block_path.to_string();
            addressed_node.output = None;
            if !vars
                .eval_bool(block_node.when.as_ref())
                .map_err(|message| EngineError::Expr {
                    node: block_path.to_string(),
                    message,
                })?
            {
                journal.event(
                    "step_skipped",
                    json!({
                        "node": block_path,
                        "reason": FailureDetail::new(
                            FailureCode::WhenFalse,
                            "when expression evaluated false",
                        ),
                    }),
                )?;
                continue;
            }
            let block_id = block_path.to_string();
            if let Some(replayed) = context.replayed_steps.get(&block_id) {
                if let Some(output) = &replayed.output {
                    vars.set_step_output(&block_id, output.clone());
                }
                journal.event(
                    "step_replayed",
                    json!({ "node": block_id, "status": replayed.status }),
                )?;
                continue;
            }
            // Single attempt counter per logical node (E10): `attempt: 1`
            // names the journal start marker for this iteration's retry
            // sequence, and the wrapper's `step_retry` events count the same
            // logical attempts (failed attempt N, next is N+1), never a
            // second overlapping counter.
            journal.event(
                "step_started",
                json!({ "node": block_id, "type": addressed_node.kind.to_string(), "attempt": 1 }),
            )?;
            // Single-charge rule (E10): the outer foreach node is charged
            // once total via the top-level wrapper; children share that
            // budget without per-child consume. The retry wrapper is still
            // shared for retry/timeout/elapsed semantics, called here with
            // charge=false so attempts do not double-count.
            // Attempt limit and budget limit are separate axes (E10): retry
            // max_attempts (1 through 16) bounds executions, while
            // charge=false bounds budget to the single outer charge. A child
            // with max_attempts=16 therefore executes up to 16 times for one
            // budget consume; budget exhaustion and attempt exhaustion stay
            // distinct.
            // Children run through the same retry wrapper as top-level
            // nodes so `retry.max_attempts`, `backoff_ms`, `timeout_secs`,
            // and `on_indeterminate` all apply identically in sequential,
            // parallel, and nested foreach (E10). The bound is
            // multiplicative: a child with max_attempts=2 inside a foreach
            // with max_attempts=2 runs at most 4 attempts total.
            match self
                .execute_node_with_retry_charged(
                    context,
                    journal,
                    vars,
                    budget,
                    &addressed_node,
                    false,
                )
                .await?
            {
                StepOutcome::Success {
                    output,
                    files: block_files,
                } => {
                    let file_pins = pin_files(
                        &context.workspace,
                        &context.metadata,
                        &block_files,
                        &context.contract.manifest.runtime,
                        &context.checkpoint_accounting,
                    )?;
                    if let Some(value) = output.clone() {
                        vars.set_step_output(&block_id, value);
                    }
                    files.extend(block_files);
                    // Single shared output-name helper (E10): the addressed
                    // node has no declared output (cleared above), so the
                    // helper resolves to the per-iteration block id.
                    let output_name = super::types::output_name_for(&addressed_node);
                    journal.event(
                        "step_finished",
                        json!({ "node": block_id, "status": "success", "output": output, "output_name": output_name, "files": file_pins }),
                    )?;
                }
                StepOutcome::CheckFailed {
                    findings,
                    output,
                    files: failed_files,
                } => {
                    // Journal every child outcome with its block id (E10),
                    // matching the wave path which records all outcomes.
                    // The failed revision is pinned so its blob verifies as
                    // historical; the unified `failed_output` / `failed_files`
                    // notation matches top-level failures (E06).
                    let failed_pins = pin_files(
                        &context.workspace,
                        &context.metadata,
                        &failed_files,
                        &context.contract.manifest.runtime,
                        &context.checkpoint_accounting,
                    )?;
                    let reason = failure_from_findings(&findings, FailureCode::CheckFailed);
                    journal.event(
                        "step_finished",
                        with_failed_evidence(
                            json!({ "node": block_id.clone(), "status": "check_failed", "findings": findings.clone(), "reason": reason }),
                            &output,
                            &failed_pins,
                        )?,
                    )?;
                    vars.set_item(parent_item);
                    let mut all_files = files;
                    all_files.extend(failed_files);
                    return Ok((
                        all_files.clone(),
                        Some(StepOutcome::CheckFailed {
                            findings,
                            output,
                            files: all_files,
                        }),
                    ));
                }
                StepOutcome::NeedsUser { question } => {
                    // Journal the suspension with its block id and unified
                    // failed-evidence keys (null plus empty list) so the
                    // notation matches failure paths (E06).
                    const NO_OUTPUT: Option<serde_json::Value> = None;
                    journal.event(
                        "step_finished",
                        with_failed_evidence(
                            json!({ "node": block_id.clone(), "status": "needs_user", "question": question.clone() }),
                            &NO_OUTPUT,
                            &[],
                        )?,
                    )?;
                    vars.set_item(parent_item);
                    return Ok((files, Some(StepOutcome::NeedsUser { question })));
                }
                StepOutcome::NeedsConfirm { confirm } => {
                    // `confirm_request` keeps its strict FOREIGN schema
                    // (allows only `confirm`): no failed-evidence keys here.
                    // Still journaled with the block id (E10).
                    journal.event(
                        "confirm_request",
                        json!({ "node": block_id.clone(), "confirm": confirm.clone() }),
                    )?;
                    vars.set_item(parent_item);
                    return Ok((files, Some(StepOutcome::NeedsConfirm { confirm })));
                }
            }
        }
        vars.set_item(parent_item);
        Ok((files, None))
    }
}

/// Outcome of one spawned iteration task: its isolated variable scope plus
/// its result. Successful completions carry their files and no onward
/// outcome; fail-fast outcomes carry the outcome to return.
type ForeachTaskOutcome = (
    ValueBag,
    Result<(Vec<Utf8PathBuf>, Option<StepOutcome>), EngineError>,
);

/// Drains remaining iteration tasks after fail-fast, aborting the
/// in-flight ones first. Siblings that already completed successfully are
/// still absorbed (outputs preserved, budget stays consumed). Failures,
/// suspensions, and aborts found during the drain do not change the already
/// reported first terminal outcome (fail-fast), but they are journaled as
/// `foreach_sibling_ignored` diagnostics so no failure detail disappears
/// from the journal, and a count is warned for operators (E10).
/// Monitoring note: the returned outcome carries only the first terminal
/// result; operators must alert on `foreach_sibling_ignored` events,
/// otherwise later sibling failures are invisible in the parent result.
pub(crate) async fn drain_foreach_tasks(
    tasks: &mut JoinSet<ForeachTaskOutcome>,
    vars: &mut ValueBag,
    files: &mut Vec<Utf8PathBuf>,
    journal: &JournalWriter,
    node_id: &str,
) -> Result<(), EngineError> {
    tasks.abort_all();
    let mut ignored: usize = 0;
    while let Some(result) = tasks.join_next().await {
        let Ok((iteration_vars, Ok((iteration_files, None)))) = result else {
            ignored += 1;
            let detail = match &result {
                Ok((_, Err(error))) => error.to_string(),
                Ok((_, Ok((_, Some(_))))) => "sibling terminal outcome after fail-fast".to_string(),
                Err(error) => {
                    if error.is_cancelled() {
                        "sibling aborted during drain".to_string()
                    } else {
                        format!("sibling join error during drain: {error}")
                    }
                }
                _ => "sibling non-success during drain".to_string(),
            };
            journal
                .event(
                    "foreach_sibling_ignored",
                    serde_json::json!({ "node": node_id, "detail": detail }),
                )
                .map_err(EngineError::Journal)?;
            continue;
        };
        vars.absorb(&iteration_vars);
        files.extend(iteration_files);
    }
    if ignored > 0 {
        tracing::warn!(node = %node_id, ignored, "drain_foreach_tasks ignored sibling outcomes after fail-fast; see foreach_sibling_ignored journal events");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_absorbs_completed_siblings_and_ignores_the_rest() {
        // E10: after fail-fast, already-completed sibling results are
        // absorbed (not silently discarded) while failures, suspensions,
        // and aborted tasks are journaled as `foreach_sibling_ignored`
        // diagnostics and contribute nothing more to the outcome.
        let dir = std::env::temp_dir().join(format!("qcg-foreach-drain-{}", uuid::Uuid::now_v7()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let journal_path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temp path must be UTF-8");
        let journal = JournalWriter::create(&journal_path, "drain-test", false, None)
            .expect("journal must open");
        let mut vars = ValueBag::default();
        let mut files: Vec<Utf8PathBuf> = Vec::new();
        let mut tasks: JoinSet<ForeachTaskOutcome> = JoinSet::new();
        tasks.spawn(async {
            let mut scope = ValueBag::default();
            scope.set_step_output("each[0]/child", json!({"value": "ok"}));
            (scope, Ok((vec![Utf8PathBuf::from("out0.txt")], None)))
        });
        tasks.spawn(async {
            (
                ValueBag::default(),
                Err(EngineError::Failed("sibling already failed".into())),
            )
        });
        tasks.spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            (ValueBag::default(), Ok((Vec::new(), None)))
        });
        // Let the two immediate tasks finish; the sleeper stays in flight.
        // A short settle is enough for spawned immediates, and the drain
        // below aborts anything still running, so this cannot hang.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        drain_foreach_tasks(&mut tasks, &mut vars, &mut files, &journal, "foreach-test")
            .await
            .expect("drain journaling must succeed");
        assert!(
            vars.get_path("steps.each[0]/child.output.value")
                .and_then(Value::as_str)
                == Some("ok"),
            "the completed sibling output must be absorbed"
        );
        assert_eq!(
            files,
            vec![Utf8PathBuf::from("out0.txt")],
            "the completed sibling files must be preserved"
        );
        assert!(tasks.is_empty(), "the drain must leave no live task");
        drop(journal);
        let journal_bytes = std::fs::read_to_string(&journal_path).expect("journal must read");
        assert!(
            journal_bytes.contains("foreach_sibling_ignored"),
            "sibling failures during drain must leave journal diagnostics"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
