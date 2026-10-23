use crate::{JournalWriter, StepControlFlow, StepError, StepOutcome};
use camino::Utf8PathBuf;
use contract::NodeDef;
use contract::ValueBag;
use model::{FailureCode, FailureDetail, NodePath};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::{Id, JoinSet};

use super::checkpoint::{existing_regular_files, pin_attempt_files};
use super::repair_support::failure_from_findings;
use super::replay::BudgetTracker;
use super::types::{
    Engine, EngineError, ForeachControlParams, ForeachIteration, RunContext,
    failure_detail_for_error, with_failed_evidence,
};
use policy::{MAX_FOREACH_ITERATIONS, MAX_FOREACH_PARALLELISM};

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
        // Parallel iterations isolate variables, but share workspace, journal
        // and run-wide budget. Refuse unsafe/unknown leaves and on_fail routing,
        // just like top-level waves. Nested control flow gates its own leaves.
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
                let mut iteration_vars = vars.clone();
                iteration_vars.clear_step_outcome_subtree(&node.id);
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
                // Keep accumulated/failed files as exhaustion evidence.
                if outcome.is_none() {
                    vars.absorb_outputs(&iteration_vars);
                }
                files.extend(iteration_files);
                if let Some(outcome) = outcome {
                    let outcome = match outcome {
                        StepOutcome::CheckFailed {
                            findings,
                            output,
                            files: failed_files,
                        } => {
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
            let mut task_indices = HashMap::new();
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
                    iteration_vars.clear_step_outcome_subtree(&node.id);
                    // Shared budget atomics, not an isolated budget: the
                    // clone shares the run-wide counters, so every iteration
                    // charges the same budget (E10).
                    let mut iteration_budget = budget.clone();
                    let foreach_node = node.clone();
                    let task = tasks.spawn(async move {
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
                        (index, iteration_vars, outcome)
                    });
                    task_indices.insert(task.id(), index);
                }
                let Some(result) = tasks.join_next_with_id().await else {
                    break;
                };
                let task_id = match &result {
                    Ok((id, _)) => *id,
                    Err(error) => error.id(),
                };
                task_indices.remove(&task_id);
                let result = match result {
                    Ok((_, (_, iteration_vars, Ok((iteration_files, None))))) => {
                        vars.absorb_outputs(&iteration_vars);
                        files.extend(iteration_files);
                        continue;
                    }
                    other => other,
                };
                let parent_cancelled = match &result {
                    Ok((_, (_, _, Err(error)))) => error.is_canceled(),
                    Err(error) => error.is_cancelled(),
                    _ => false,
                };
                // Preserve the first report's files before draining sibling files.
                if let Ok((_, (_, _, Ok((iteration_files, Some(_)))))) = &result {
                    files.extend(iteration_files.iter().cloned());
                }
                match super::execute::parallel_terminal_decision(context.parallel_compatibility) {
                    super::execute::ParallelTerminalDecision::AbortAndDrain => {
                        drain_foreach_tasks(
                            &mut tasks,
                            &mut task_indices,
                            vars,
                            &mut files,
                            &journal,
                            &node.id,
                            parent_cancelled,
                        )
                        .await?;
                    }
                }
                return match result {
                    Ok((_, (_, _, Ok((_, Some(outcome)))))) => Ok(match outcome {
                        StepOutcome::CheckFailed {
                            findings, output, ..
                        } => StepOutcome::CheckFailed {
                            findings,
                            output,
                            files: files.clone(),
                        },
                        other => other,
                    }),
                    Ok((_, (_, _, Err(error)))) => Err(error),
                    Err(error) if error.is_cancelled() => Err(EngineError::Canceled),
                    Err(error) => Err(super::execute::join_task_error(error)),
                    // Successful iterations were absorbed above, before applying policy.
                    Ok((_, (_, _, Ok((_, None))))) => unreachable!("success already absorbed"),
                };
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
        // Runtime paths use stable parent/index/child IDs for replay.
        journal.event(
            "foreach_iteration",
            json!({ "node": node.id, "index": index }),
        )?;
        let foreach_path = NodePath::root(node.id.clone());
        let iteration_scope = format!("{}[{index}]", node.id);
        for block_node in block {
            vars.sync_step_outcome_subtree(&journal.state().vars, &iteration_scope);
            context.run_checkpoint()?;
            let block_path = foreach_path.foreach_child(index, &block_node.id);
            let mut addressed_node = block_node.clone();
            // Per-iteration IDs isolate replay/output; clear aliases to avoid sibling collisions.
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
                vars.sync_step_outcome_subtree(&journal.state().vars, &iteration_scope);
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
                vars.sync_step_outcome_subtree(&journal.state().vars, &iteration_scope);
                continue;
            }
            // One retry sequence and attempt counter per logical child.
            journal.event(
                "step_started",
                json!({ "node": block_id, "type": addressed_node.kind.to_string(), "attempt": 1 }),
            )?;
            // Charge the foreach once; child retries/timeouts still apply, and nested attempt bounds multiply.
            let child_outcome = self
                .execute_node_with_retry_charged(
                    context,
                    journal,
                    vars,
                    budget,
                    &addressed_node,
                    false,
                )
                .await;
            let child_outcome = match child_outcome {
                Ok(outcome) => outcome,
                Err(error)
                    if error.is_canceled()
                        || matches!(
                            error,
                            EngineError::NeedsUser { .. } | EngineError::NeedsConfirm { .. }
                        ) =>
                {
                    vars.set_item(parent_item);
                    return Err(error);
                }
                Err(error) if error.is_resumable_agent() => {
                    vars.set_item(parent_item);
                    // Preserve explicit continuation ownership through enclosing
                    // scheduler boundaries; node kind alone never synthesizes one.
                    return Err(error);
                }
                Err(error) => {
                    let reason = failure_detail_for_error(&error);
                    journal.event(
                        "step_finished",
                        json!({ "node": block_id, "status": "failed", "reason": reason }),
                    )?;
                    vars.sync_step_outcome_subtree(&journal.state().vars, &iteration_scope);
                    vars.set_item(parent_item);
                    return Err(error);
                }
            };
            match child_outcome {
                StepOutcome::Success {
                    output,
                    files: block_files,
                } => {
                    let file_pins = pin_attempt_files(context, &block_files)?;
                    if let Some(value) = output.clone() {
                        vars.set_step_output(&block_id, value);
                    }
                    files.extend(block_files);
                    // Resolve the output name to the addressed per-iteration block id.
                    let output_name = super::types::output_name_for(&addressed_node);
                    journal.event(
                        "step_finished",
                        json!({ "node": block_id, "status": "success", "output": output, "output_name": output_name, "files": file_pins }),
                    )?;
                    vars.sync_step_outcome_subtree(&journal.state().vars, &iteration_scope);
                }
                StepOutcome::CheckFailed {
                    findings,
                    output,
                    files: failed_files,
                } => {
                    // Preserve the child's failed revision using the top-level evidence keys.
                    let failed_pins = pin_attempt_files(context, &failed_files)?;
                    let reason = failure_from_findings(&findings, FailureCode::CheckFailed);
                    journal.event(
                        "step_finished",
                        with_failed_evidence(
                            json!({ "node": block_id.clone(), "status": "check_failed", "findings": findings.clone(), "reason": reason }),
                            &output,
                            &failed_pins,
                        )?,
                    )?;
                    vars.sync_step_outcome_subtree(&journal.state().vars, &iteration_scope);
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
                StepOutcome::NeedsUser {
                    question,
                    files: child_files,
                } => {
                    // Journal the suspension and its pinned files under the child block id.
                    const NO_OUTPUT: Option<serde_json::Value> = None;
                    let file_pins = pin_attempt_files(context, &child_files)?;
                    journal.event(
                        "step_finished",
                        with_failed_evidence(
                            json!({ "node": block_id.clone(), "status": "needs_user", "question": question.clone(), "files": file_pins }),
                            &NO_OUTPUT,
                            &[],
                        )?,
                    )?;
                    vars.set_item(parent_item);
                    files.extend(child_files.clone());
                    return Ok((
                        files,
                        Some(StepOutcome::NeedsUser {
                            question,
                            files: child_files,
                        }),
                    ));
                }
                StepOutcome::NeedsConfirm {
                    confirm,
                    files: child_files,
                } => {
                    let file_pins = pin_attempt_files(context, &child_files)?;
                    journal.event(
                        "confirm_request",
                        json!({ "node": block_id.clone(), "confirm": confirm.clone(), "files": file_pins }),
                    )?;
                    vars.set_item(parent_item);
                    files.extend(child_files.clone());
                    return Ok((
                        files,
                        Some(StepOutcome::NeedsConfirm {
                            confirm,
                            files: child_files,
                        }),
                    ));
                }
            }
        }
        vars.set_item(parent_item);
        Ok((files, None))
    }
}

type ForeachTaskOutcome = (
    usize,
    ValueBag,
    Result<(Vec<Utf8PathBuf>, Option<StepOutcome>), EngineError>,
);

/// Record drained sibling facts by iteration; child events retain effects and prompts.
/// Unknown interrupted effects remain unsettled; this diagnostic authorizes no replay.
async fn drain_foreach_tasks(
    tasks: &mut JoinSet<ForeachTaskOutcome>,
    task_indices: &mut HashMap<Id, usize>,
    vars: &mut ValueBag,
    files: &mut Vec<Utf8PathBuf>,
    journal: &JournalWriter,
    node_id: &str,
    parent_cancelled: bool,
) -> Result<(), EngineError> {
    tasks.abort_all();
    let mut ignored: usize = 0;
    // Keep joining all owners after a journal failure; return it only after quiescence.
    let mut write_error: Option<EngineError> = None;
    while let Some(result) = tasks.join_next_with_id().await {
        let task_id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let index = task_indices
            .remove(&task_id)
            .expect("registered foreach task");
        let mut record = json!({
            "node": node_id,
            "index": index,
            "iteration": format!("{node_id}[{index}]"),
            "drain_cause": if parent_cancelled { "parent_cancellation" } else { "fail_fast" },
        });
        match result {
            Ok((_, (reported_index, iteration_vars, Ok((iteration_files, None))))) => {
                debug_assert_eq!(index, reported_index);
                vars.absorb_outputs(&iteration_vars);
                files.extend(iteration_files);
                record["classification"] = json!("completed_success");
            }
            Ok((_, (reported_index, _, Ok((_, Some(outcome)))))) => {
                debug_assert_eq!(index, reported_index);
                record["classification"] = json!("completed_terminal");
                let (outcome, interaction) = match &outcome {
                    StepOutcome::CheckFailed { .. } => ("check_failed", None),
                    StepOutcome::NeedsUser { question, .. } => {
                        ("needs_user", Some(("question_id", question.id.as_str())))
                    }
                    StepOutcome::NeedsConfirm { confirm, .. } => {
                        ("needs_confirm", Some(("confirm_id", confirm.id.as_str())))
                    }
                    StepOutcome::Success { .. } => ("success", None),
                };
                record["outcome"] = json!(outcome);
                if let Some((key, id)) = interaction {
                    record[key] = json!(id);
                }
                ignored += 1;
            }
            Ok((_, (reported_index, _, Err(error)))) => {
                debug_assert_eq!(index, reported_index);
                let (classification, code, interaction) = match &error {
                    EngineError::NeedsUser { question_id, .. } => (
                        "interaction_pending",
                        None,
                        Some(("question_id", question_id.as_str())),
                    ),
                    EngineError::NeedsConfirm { confirm_id, .. } => (
                        "interaction_pending",
                        None,
                        Some(("confirm_id", confirm_id.as_str())),
                    ),
                    _ if error.is_resumable_agent() => {
                        record["continuation_owner"] = json!("agent");
                        ("agent_continuation", None, None)
                    }
                    _ if error.is_canceled() => {
                        record["cancellation_cause"] = json!("reported_by_task");
                        ("task_canceled", None, None)
                    }
                    _ => (
                        "task_error",
                        Some(failure_detail_for_error(&error).code),
                        None,
                    ),
                };
                record["classification"] = json!(classification);
                if let Some(code) = code {
                    record["code"] = json!(code);
                }
                if let Some((key, id)) = interaction {
                    record[key] = json!(id);
                }
                record["detail"] = json!(error.to_string());
                ignored += 1;
            }
            Err(error) => {
                let classification = if error.is_cancelled() {
                    record["cancellation_cause"] = json!(if parent_cancelled {
                        "parent_cancellation"
                    } else {
                        "fail_fast"
                    });
                    if parent_cancelled {
                        "parent_canceled"
                    } else {
                        "scheduler_abort"
                    }
                } else {
                    "task_join_error"
                };
                record["classification"] = json!(classification);
                record["detail"] = json!(error.to_string());
                ignored += 1;
            }
        }
        if let Err(error) = journal.event("foreach_sibling_drained", record) {
            write_error.get_or_insert_with(|| error.into());
        }
    }
    if ignored > 0 {
        tracing::warn!(node = %node_id, ignored, "drain_foreach_tasks ignored sibling outcomes after fail-fast; see foreach_sibling_drained journal events");
    }
    write_error.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JournalLimits, RunState, read_journal_values};

    async fn completed(task: &tokio::task::AbortHandle) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("immediate test task must finish before drain");
    }

    #[tokio::test]
    async fn drain_records_stable_sibling_facts_and_replays_child_history() {
        let dir = std::env::temp_dir().join(format!("foreach-drain-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).expect("UTF-8 path");
        let journal = JournalWriter::create(&path, "drain-test", false, None).expect("journal");
        let mut vars = ValueBag::default();
        let mut files = Vec::new();
        let mut tasks: JoinSet<ForeachTaskOutcome> = JoinSet::new();
        let mut indices = HashMap::new();

        // Consume the first terminal report before the drain, just as the
        // scheduler does. The other reports must not replace this outcome.
        let first = tasks.spawn(async {
            (
                0,
                ValueBag::default(),
                Ok((vec![], Some(StepOutcome::check_failed(vec![])))),
            )
        });
        indices.insert(first.id(), 0);
        completed(&first).await;
        let (first_id, (_, _, first_outcome)) = tasks.join_next_with_id().await.unwrap().unwrap();
        indices.remove(&first_id);

        let question = api::FormSpec {
            id: "each[7]/ask.question".into(),
            title: "Child prompt must not be copied".into(),
            title_i18n: Default::default(),
            fields: vec![],
        };
        // Underlying child records remain authoritative. Drain references
        // the iteration/interaction, not copies of prompts or outputs.
        journal
            .event(
                "step_finished",
                json!({
                    "node": "each[2]/child", "status": "success", "output": {"value": "ok"},
                    "output_name": "each[2]/child", "files": [],
                }),
            )
            .unwrap();
        journal
            .event(
                "step_finished",
                json!({
                    "node": "each[7]/ask", "status": "needs_user", "question": question,
                    "files": [], "failed_output": null, "failed_files": [],
                }),
            )
            .unwrap();
        let mut before = RunState::fold_journal(&path).expect("child history before drain");

        let mut scope = ValueBag::default();
        scope.set_item(Some(json!("identical user item")));
        scope.set_step_output("each[2]/child", json!({"value": "ok"}));
        let success =
            tasks.spawn(async move { (2, scope, Ok((vec![Utf8PathBuf::from("out.txt")], None))) });
        indices.insert(success.id(), 2);
        let terminal = tasks.spawn(async move {
            let mut scope = ValueBag::default();
            scope.set_item(Some(json!("identical user item")));
            scope.set_step_output("must_not_merge", json!(true));
            (
                7,
                scope,
                Ok((
                    vec![],
                    Some(StepOutcome::NeedsUser {
                        question,
                        files: vec![],
                    }),
                )),
            )
        });
        indices.insert(terminal.id(), 7);
        let error = tasks.spawn(async {
            (
                11,
                ValueBag::default(),
                Err(EngineError::ResumableAgent(Box::new(EngineError::Failed(
                    "agent continuation retained".into(),
                )))),
            )
        });
        indices.insert(error.id(), 11);
        let panic = tasks.spawn(async { panic!("sibling panic diagnostic") });
        indices.insert(panic.id(), 13);
        for task in [&success, &terminal, &error, &panic] {
            completed(task).await;
        }
        let aborted = tasks.spawn(std::future::pending());
        indices.insert(aborted.id(), 19);

        drain_foreach_tasks(
            &mut tasks,
            &mut indices,
            &mut vars,
            &mut files,
            &journal,
            "each",
            false,
        )
        .await
        .expect("drain");
        assert!(matches!(
            first_outcome,
            Ok((_, Some(StepOutcome::CheckFailed { .. })))
        ));
        assert_eq!(
            vars.get_path("steps.each[2]/child.output.value"),
            Some(&json!("ok"))
        );
        assert!(vars.get_path("steps.must_not_merge.output").is_none());
        assert_eq!(files, vec![Utf8PathBuf::from("out.txt")]);
        assert!(tasks.is_empty());
        assert!(indices.is_empty());

        let events = read_journal_values(&path, JournalLimits::default())
            .unwrap()
            .events;
        let records: HashMap<_, _> = events
            .iter()
            .filter(|event| event["t"] == "foreach_sibling_drained")
            .map(|event| (event["index"].as_u64().unwrap(), event))
            .collect();
        assert_eq!(records.len(), 5);
        for (index, event) in &records {
            assert_eq!(event["node"], "each");
            assert_eq!(event["iteration"], format!("each[{index}]"));
            let public =
                api::RunEvent::from_flat(event).expect("internal event stays stream-compatible");
            assert_eq!(public.path.unwrap().to_string(), "each");
            assert!(!event.to_string().contains("Child prompt"));
            assert!(!event.to_string().contains("identical user item"));
        }
        assert_eq!(records[&2]["classification"], "completed_success");
        assert_eq!(records[&7]["classification"], "completed_terminal");
        assert_eq!(records[&7]["outcome"], "needs_user");
        assert_eq!(records[&7]["question_id"], "each[7]/ask.question");
        assert_eq!(records[&11]["classification"], "agent_continuation");
        assert_eq!(records[&11]["continuation_owner"], "agent");
        assert_eq!(
            records[&11]["detail"],
            "run failed: agent continuation retained"
        );
        assert_eq!(records[&13]["classification"], "task_join_error");
        assert!(
            records[&13]["detail"]
                .as_str()
                .unwrap()
                .contains("sibling panic diagnostic")
        );
        assert_eq!(records[&19]["classification"], "scheduler_abort");
        assert_eq!(records[&19]["cancellation_cause"], "fail_fast");
        assert_eq!(records[&19]["drain_cause"], "fail_fast");
        assert!(records[&19].get("outcome").is_none());
        assert!(records[&19].get("code").is_none());

        // Typed replay preserves child history and unresolved iteration identity,
        // without inventing failed child outcomes.
        let replayed = RunState::fold_journal(&path).expect("journal replay");
        before.last_seq = replayed.last_seq;
        assert_eq!(replayed.unresolved_tasks.len(), 2);
        assert_eq!(replayed.unresolved_tasks["each[13]"], *records[&13]);
        assert_eq!(replayed.unresolved_tasks["each[19]"], *records[&19]);
        before.unresolved_tasks = replayed.unresolved_tasks.clone();
        assert_eq!(
            serde_json::to_value(before).unwrap(),
            serde_json::to_value(&replayed).unwrap()
        );
        assert!(
            replayed
                .vars
                .get_path("steps.each[19]/child.status")
                .is_none()
        );
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn drain_quiesces_every_task_after_a_fact_write_failure() {
        let dir =
            std::env::temp_dir().join(format!("foreach-write-failure-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let limits = JournalLimits {
            max_event_count: Some(1),
            ..JournalLimits::default()
        };
        let journal =
            JournalWriter::create_with_limits(&path, "write-failure", false, None, limits).unwrap();
        let mut tasks = JoinSet::new();
        let mut indices = HashMap::new();
        for index in 0..3 {
            let task = tasks.spawn(async move {
                (
                    index,
                    ValueBag::default(),
                    Err(EngineError::Failed("reported".into())),
                )
            });
            indices.insert(task.id(), index);
            completed(&task).await;
        }
        let result = drain_foreach_tasks(
            &mut tasks,
            &mut indices,
            &mut ValueBag::default(),
            &mut vec![],
            &journal,
            "outer",
            false,
        )
        .await;
        assert!(
            result.is_err(),
            "the event-count limit must reject later facts"
        );
        assert!(
            tasks.is_empty(),
            "all task joins must be consumed before return"
        );
        assert!(indices.is_empty(), "no task owner may remain unjoined");
        let events = read_journal_values(&path, JournalLimits::default())
            .unwrap()
            .events;
        assert_eq!(
            events
                .iter()
                .filter(|event| event["t"] == "foreach_sibling_drained")
                .count(),
            1,
            "the bounded journal must retain the first fact and reject the rest"
        );
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn drain_records_parent_cancellation_separately_from_fail_fast_abort() {
        let dir = std::env::temp_dir().join(format!("foreach-cancel-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let journal = JournalWriter::create(&path, "cancel-test", false, None).unwrap();
        let mut tasks = JoinSet::new();
        let mut indices = HashMap::new();
        let task = tasks.spawn(std::future::pending::<ForeachTaskOutcome>());
        indices.insert(task.id(), 4);
        drain_foreach_tasks(
            &mut tasks,
            &mut indices,
            &mut ValueBag::default(),
            &mut vec![],
            &journal,
            "outer",
            true,
        )
        .await
        .unwrap();
        let event = read_journal_values(&path, JournalLimits::default())
            .unwrap()
            .events
            .remove(0);
        assert_eq!(event["classification"], "parent_canceled");
        assert_eq!(event["drain_cause"], "parent_cancellation");
        assert_eq!(event["cancellation_cause"], "parent_cancellation");
        RunState::fold_journal(&path).expect("cancellation fact replays");
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn drain_records_confirm_and_error_interaction_references() {
        let dir = std::env::temp_dir().join(format!("foreach-refs-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let journal = JournalWriter::create(&path, "refs-test", false, None).unwrap();
        let confirm = api::ConfirmSpec {
            id: "confirm-id".into(),
            title: "Do not copy this prompt".into(),
            kind: "command".into(),
            target: "private target".into(),
            dry_run: false,
            details: None,
            operation_digest: "a".repeat(64),
            scope: contract::SideEffectScope::Invocation,
        };
        let question = api::FormSpec {
            id: "question-id".into(),
            title: "Do not copy this prompt".into(),
            title_i18n: Default::default(),
            fields: vec![],
        };
        let outcomes = [
            Ok((
                vec![],
                Some(StepOutcome::NeedsConfirm {
                    confirm: confirm.clone(),
                    files: vec![],
                }),
            )),
            Ok((vec![], Some(StepOutcome::check_failed(vec![])))),
            Err(EngineError::NeedsUser {
                question_id: question.id.clone(),
                question: Box::new(question),
            }),
            Err(EngineError::NeedsConfirm {
                confirm_id: confirm.id.clone(),
                confirm: Box::new(confirm),
            }),
        ];
        let mut tasks = JoinSet::new();
        let mut indices = HashMap::new();
        for (index, outcome) in outcomes.into_iter().enumerate() {
            let task = tasks.spawn(async move { (index, ValueBag::default(), outcome) });
            indices.insert(task.id(), index);
            completed(&task).await;
        }
        drain_foreach_tasks(
            &mut tasks,
            &mut indices,
            &mut ValueBag::default(),
            &mut vec![],
            &journal,
            "outer[3]/inner",
            false,
        )
        .await
        .unwrap();
        let events = read_journal_values(&path, JournalLimits::default())
            .unwrap()
            .events;
        let records: HashMap<_, _> = events
            .iter()
            .map(|event| (event["index"].as_u64().unwrap(), event))
            .collect();
        assert_eq!(records[&0]["classification"], "completed_terminal");
        assert_eq!(records[&0]["outcome"], "needs_confirm");
        assert_eq!(records[&0]["confirm_id"], "confirm-id");
        assert_eq!(records[&1]["outcome"], "check_failed");
        assert_eq!(records[&2]["classification"], "interaction_pending");
        assert_eq!(records[&2]["question_id"], "question-id");
        assert_eq!(records[&3]["classification"], "interaction_pending");
        assert_eq!(records[&3]["confirm_id"], "confirm-id");
        for event in &events {
            assert!(!event.to_string().contains("Do not copy this prompt"));
            assert!(!event.to_string().contains("private target"));
            assert!(
                event["iteration"]
                    .as_str()
                    .unwrap()
                    .starts_with("outer[3]/inner[")
            );
        }
        RunState::fold_journal(&path).expect("interaction reference records replay");
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
