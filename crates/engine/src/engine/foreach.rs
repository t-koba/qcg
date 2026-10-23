use crate::{JournalWriter, StepControlFlow, StepError, StepOutcome};
use camino::Utf8PathBuf;
use contract::{NodeDef, ValueBag};
use model::{FailureCode, FailureDetail, NodePath};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
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
        for (field, value) in [("items", &params.items), ("subflow", &params.subflow)] {
            if value.trim().is_empty() {
                return Err(
                    StepError::failed(&node.id, format!("foreach {field} is required")).into(),
                );
            }
        }
        for (field, value, max) in [
            (
                "max_iterations",
                params.max_iterations,
                MAX_FOREACH_ITERATIONS,
            ),
            ("parallel", params.parallel, MAX_FOREACH_PARALLELISM),
        ] {
            if !(1..=max).contains(&value) {
                return Err(StepError::failed(
                    &node.id,
                    format!("foreach {field} must be from 1 through {max}"),
                )
                .into());
            }
        }
        let items_ref = params.items.as_str();
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
        if params.parallel > 1 && item_count > 1 {
            for child in block.iter() {
                let unsafe_leaf = self.registry.traits(&child.kind).is_none_or(|traits| {
                    !traits.parallel_safe && traits.control_flow == StepControlFlow::Plain
                });
                let message = if unsafe_leaf {
                    format!(
                        "foreach block child `{}` is not parallel-safe; use parallel = 1",
                        child.id
                    )
                } else if child.on_fail.is_some() {
                    format!(
                        "foreach block child `{}` declares on_fail routing; parallel execution requires parallel = 1",
                        child.id
                    )
                } else {
                    continue;
                };
                return Err(StepError::failed(&node.id, message).into());
            }
        }
        let mut files = Vec::new();
        // Reuse settled checks without letting them abort an answered owner's path.
        let mut terminals = BTreeMap::new();
        let state = journal.state();
        for fact in state.foreach_reports.values().filter(|fact| {
            fact["node"] == node.id
                && fact["seq"]
                    .as_u64()
                    .is_some_and(|seq| seq <= context.replay_through)
        }) {
            let owner = fact["owner"].as_str().unwrap_or("");
            if state.foreach_reports.values().any(|child| {
                child["node"].as_str().is_some_and(|node| {
                    !owner.is_empty() && (node == owner || node.starts_with(&format!("{owner}[")))
                }) && (child["question_id"]
                    .as_str()
                    .is_some_and(|id| context.answers.contains_key(id))
                    || child["confirm_id"]
                        .as_str()
                        .is_some_and(|id| context.confirmations.contains_key(id)))
            }) {
                continue;
            }
            if let Some(result) = fact.get("result") {
                let result: Result<StepOutcome, FailureDetail> =
                    serde_json::from_value(result.clone()).map_err(crate::JournalError::from)?;
                files.extend(
                    serde_json::from_value::<Vec<Utf8PathBuf>>(fact["files"].clone())
                        .map_err(crate::JournalError::from)?
                        .into_iter()
                        .map(|path| context.workspace.join(path)),
                );
                let index = fact["index"]
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| StepError::failed(&node.id, "invalid recorded foreach index"))?;
                terminals.insert(
                    index,
                    result
                        .map(|outcome| (vec![], Some(outcome)))
                        .map_err(EngineError::Settled),
                );
            }
        }
        let mut reporter = ForeachReporter {
            vars,
            files: &mut files,
            journal,
            node: &node.id,
            workspace: &context.workspace,
        };
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
                if terminals.contains_key(&index) {
                    continue;
                }
                let engine = self.clone();
                let context = Arc::clone(&context);
                let journal = Arc::clone(&journal);
                let block = Arc::clone(&block);
                let mut iteration_vars = reporter.vars.clone();
                iteration_vars.clear_step_outcome_subtree(&node.id);
                let mut iteration_budget = budget.clone();
                let foreach_node = node.clone();
                let task = tasks.spawn(async move {
                    let mut owner = format!("{}[{index}]", foreach_node.id);
                    let outcome = engine
                        .execute_foreach_iteration(
                            &context,
                            &journal,
                            &mut iteration_vars,
                            &mut iteration_budget,
                            &mut owner,
                            ForeachIteration {
                                node: &foreach_node,
                                block: &block,
                                index,
                                item,
                            },
                        )
                        .await;
                    (index, owner, iteration_vars, outcome)
                });
                task_indices.insert(task.id(), index);
            }
            let Some(result) = tasks.join_next_with_id().await else {
                break;
            };
            let id = match &result {
                Ok((id, _)) => *id,
                Err(error) => error.id(),
            };
            let index = task_indices.remove(&id).expect("registered foreach task");
            let terminal = !matches!(&result, Ok((_, (_, _, _, Ok((_, None))))));
            let unreported = result.is_err();
            let parent_cancelled = match &result {
                Ok((_, (_, _, _, Err(error)))) => error.is_canceled(),
                Err(error) => error.is_cancelled(),
                _ => false,
            };
            if terminal {
                match super::execute::parallel_terminal_decision(context.parallel_compatibility) {
                    super::execute::ParallelTerminalDecision::AbortAndDrain => tasks.abort_all(),
                }
            }
            let first = reporter.record(
                result.map(|(_, report)| report),
                index,
                parent_cancelled,
                false,
            );
            if !terminal && first.is_ok() {
                continue;
            }
            // Join even if persisting the first report failed.
            let drained = reporter
                .drain(&mut tasks, &mut task_indices, parent_cancelled)
                .await;
            terminals.extend(drained?);
            if let Some(result) = first?
                && (!unreported || terminals.is_empty())
            {
                terminals.insert(index, result);
            }
            return select_foreach_terminal(terminals, files);
        }
        if !terminals.is_empty() {
            return select_foreach_terminal(terminals, files);
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
        owner: &mut String,
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
            *owner = block_path.to_string();
            let mut addressed_node = block_node.clone();
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
                files.extend(
                    replayed
                        .files
                        .iter()
                        .map(|pin| context.workspace.join(&pin.path)),
                );
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
            journal.event(
                "step_started",
                json!({ "node": block_id, "type": addressed_node.kind.to_string(), "attempt": 1 }),
            )?;
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
                        || error.is_resumable_agent()
                        || matches!(
                            error,
                            EngineError::NeedsUser { .. } | EngineError::NeedsConfirm { .. }
                        ) =>
                {
                    vars.set_item(parent_item);
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
            let block_files = match &child_outcome {
                StepOutcome::Success { files, .. }
                | StepOutcome::CheckFailed { files, .. }
                | StepOutcome::NeedsUser { files, .. }
                | StepOutcome::NeedsConfirm { files, .. } => files,
            };
            let file_pins = pin_attempt_files(context, block_files)?;
            files.extend(block_files.iter().cloned());
            match &child_outcome {
                StepOutcome::Success { output, .. } => {
                    if let Some(value) = output {
                        vars.set_step_output(&block_id, value.clone());
                    }
                    journal.event("step_finished", json!({"node": block_id, "status": "success", "output": output, "output_name": block_id, "files": file_pins}))?;
                }
                StepOutcome::CheckFailed {
                    findings, output, ..
                } => {
                    let reason = failure_from_findings(findings, FailureCode::CheckFailed);
                    journal.event("step_finished", with_failed_evidence(json!({"node": block_id, "status": "check_failed", "findings": findings, "reason": reason}), output, &file_pins)?)?;
                }
                StepOutcome::NeedsUser { question, .. } => {
                    journal.event("step_finished", with_failed_evidence(json!({"node": block_id, "status": "needs_user", "question": question, "files": file_pins}), &None, &[])?)?;
                }
                StepOutcome::NeedsConfirm { confirm, .. } => {
                    journal.event(
                        "confirm_request",
                        json!({"node": block_id, "confirm": confirm, "files": file_pins}),
                    )?;
                }
            }
            vars.sync_step_outcome_subtree(&journal.state().vars, &iteration_scope);
            if !matches!(child_outcome, StepOutcome::Success { .. }) {
                vars.set_item(parent_item);
                return Ok((files, Some(child_outcome)));
            }
        }
        vars.set_item(parent_item);
        Ok((files, None))
    }
}
type IterationResult = Result<(Vec<Utf8PathBuf>, Option<StepOutcome>), EngineError>;
type ForeachTaskOutcome = (usize, String, ValueBag, IterationResult);
type JoinedIteration = Result<ForeachTaskOutcome, tokio::task::JoinError>;
fn select_foreach_terminal(
    terminals: BTreeMap<usize, IterationResult>,
    mut files: Vec<Utf8PathBuf>,
) -> Result<StepOutcome, EngineError> {
    let (_, outcome) = terminals.into_values().next().expect("terminal report")?;
    let mut outcome = outcome.expect("terminal outcome");
    if let StepOutcome::CheckFailed { files: failed, .. } = &mut outcome {
        files.sort();
        files.dedup();
        *failed = files;
    }
    Ok(outcome)
}
struct ForeachReporter<'a> {
    vars: &'a mut ValueBag,
    files: &'a mut Vec<Utf8PathBuf>,
    journal: &'a JournalWriter,
    node: &'a str,
    workspace: &'a camino::Utf8Path,
}
impl ForeachReporter<'_> {
    fn record(
        &mut self,
        result: JoinedIteration,
        index: usize,
        parent_cancelled: bool,
        drained: bool,
    ) -> Result<Option<IterationResult>, EngineError> {
        let node = self.node;
        let cause = if parent_cancelled {
            "parent_cancellation"
        } else {
            "fail_fast"
        };
        let mut record = json!({"node": node, "index": index, "iteration": format!("{node}[{index}]"), "drain_cause": cause});
        let result = match result {
            Ok((reported, owner, iteration_vars, result)) => {
                debug_assert_eq!(index, reported);
                record["owner"] = json!(owner);
                match &result {
                    Ok((iteration_files, outcome)) => {
                        self.files.extend(iteration_files.iter().cloned());
                        record["files"] = json!(
                            iteration_files
                                .iter()
                                .map(|path| path.strip_prefix(self.workspace).unwrap_or(path))
                                .collect::<Vec<_>>()
                        );
                        record["classification"] = json!(if outcome.is_some() {
                            "completed_terminal"
                        } else {
                            "completed_success"
                        });
                        match outcome {
                            None | Some(StepOutcome::Success { .. }) => {
                                self.vars.absorb_outputs(&iteration_vars)
                            }
                            Some(outcome @ StepOutcome::CheckFailed { .. }) => {
                                record["outcome"] = json!("check_failed");
                                record["result"] = json!({"Ok": outcome});
                                record["result"]["Ok"]["files"] = json!([]);
                            }
                            Some(StepOutcome::NeedsUser { question, .. }) => {
                                record["outcome"] = json!("needs_user");
                                record["question_id"] = json!(question.id);
                            }
                            Some(StepOutcome::NeedsConfirm { confirm, .. }) => {
                                record["outcome"] = json!("needs_confirm");
                                record["confirm_id"] = json!(confirm.id);
                            }
                        }
                    }
                    Err(error) => {
                        record["classification"] = json!(match error {
                            EngineError::NeedsUser { question_id, .. } => {
                                record["question_id"] = json!(question_id);
                                "interaction_pending"
                            }
                            EngineError::NeedsConfirm { confirm_id, .. } => {
                                record["confirm_id"] = json!(confirm_id);
                                "interaction_pending"
                            }
                            _ if error.is_resumable_agent() => {
                                record["continuation_owner"] = json!(owner);
                                "agent_continuation"
                            }
                            _ if error.is_canceled() => {
                                record["cancellation_cause"] = json!("reported_by_task");
                                "task_canceled"
                            }
                            _ => {
                                record["code"] = json!(failure_detail_for_error(error).code);
                                record["result"] = json!({"Err": failure_detail_for_error(error)});
                                record["files"] = json!([]);
                                "task_error"
                            }
                        });
                        record["detail"] = json!(error.to_string());
                    }
                }
                if matches!(result, Ok((_, None))) {
                    None
                } else {
                    Some(result)
                }
            }
            Err(error) => {
                record["classification"] = json!(if error.is_cancelled() {
                    record["cancellation_cause"] = json!(cause);
                    if parent_cancelled {
                        "parent_canceled"
                    } else {
                        "scheduler_abort"
                    }
                } else {
                    "task_join_error"
                });
                record["detail"] = json!(error.to_string());
                if drained {
                    None
                } else {
                    Some(Err(if error.is_cancelled() {
                        EngineError::Canceled
                    } else {
                        super::execute::join_task_error(error)
                    }))
                }
            }
        };
        self.journal.event(
            if drained {
                "foreach_sibling_drained"
            } else {
                "foreach_iteration_settled"
            },
            record,
        )?;
        Ok(result)
    }

    /// Abort immediately, then join every owner even when persistence fails.
    async fn drain(
        &mut self,
        tasks: &mut JoinSet<ForeachTaskOutcome>,
        task_indices: &mut HashMap<Id, usize>,
        parent_cancelled: bool,
    ) -> Result<BTreeMap<usize, IterationResult>, EngineError> {
        tasks.abort_all();
        let mut terminals = BTreeMap::new();
        let mut write_error = None;
        while let Some(result) = tasks.join_next_with_id().await {
            let id = match &result {
                Ok((id, _)) => *id,
                Err(error) => error.id(),
            };
            let index = task_indices.remove(&id).expect("registered foreach task");
            match self.record(
                result.map(|(_, report)| report),
                index,
                parent_cancelled,
                true,
            ) {
                Ok(Some(result)) => {
                    terminals.insert(index, result);
                }
                Ok(None) => {}
                Err(error) => {
                    write_error.get_or_insert(error);
                }
            }
        }
        write_error.map_or(Ok(terminals), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JournalLimits, RunState, read_journal_values};
    mod settlement;

    async fn drain_foreach_tasks(
        tasks: &mut JoinSet<ForeachTaskOutcome>,
        indices: &mut HashMap<Id, usize>,
        vars: &mut ValueBag,
        files: &mut Vec<Utf8PathBuf>,
        journal: &JournalWriter,
        node: &str,
        canceled: bool,
    ) -> Result<BTreeMap<usize, IterationResult>, EngineError> {
        ForeachReporter {
            vars,
            files,
            journal,
            node,
            workspace: camino::Utf8Path::new("."),
        }
        .drain(tasks, indices, canceled)
        .await
    }

    pub(super) async fn completed(task: &tokio::task::AbortHandle) {
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
                "each[0]/child".into(),
                ValueBag::default(),
                Ok((vec![], Some(StepOutcome::check_failed(vec![])))),
            )
        });
        indices.insert(first.id(), 0);
        completed(&first).await;
        let (first_id, (_, _, _, first_outcome)) =
            tasks.join_next_with_id().await.unwrap().unwrap();
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
        let success = tasks.spawn(async move {
            (
                2,
                "each[2]/child".into(),
                scope,
                Ok((vec![Utf8PathBuf::from("out.txt")], None)),
            )
        });
        indices.insert(success.id(), 2);
        let terminal = tasks.spawn(async move {
            let mut scope = ValueBag::default();
            scope.set_item(Some(json!("identical user item")));
            scope.set_step_output("must_not_merge", json!(true));
            (
                7,
                "each[7]/child".into(),
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
                "each[11]/child".into(),
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
        assert_eq!(records[&11]["continuation_owner"], "each[11]/child");
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
        before.foreach_reports = replayed.foreach_reports.clone();
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
                    format!("outer[{index}]/child"),
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
            let task = tasks.spawn(async move {
                (
                    index,
                    format!("outer[3]/inner[{index}]/child"),
                    ValueBag::default(),
                    outcome,
                )
            });
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
