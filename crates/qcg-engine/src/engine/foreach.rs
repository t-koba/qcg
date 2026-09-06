use crate::{JournalWriter, StepError, StepOutcome};
use camino::Utf8PathBuf;
use qcg_contract::NodeDef;
use qcg_contract::ValueBag;
use qcg_types::{FailureCode, FailureDetail, NodePath};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::JoinSet;

use super::checkpoint::{existing_regular_files, pin_files};
use super::replay::BudgetTracker;
use super::types::{Engine, EngineError, ForeachControlParams, ForeachIteration, RunContext};
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

        let mut files = Vec::new();
        if params.parallel == 1 || item_count <= 1 {
            for (index, item) in items.into_iter().enumerate() {
                let (iteration_files, outcome) = self
                    .execute_foreach_iteration(
                        context,
                        journal,
                        vars,
                        budget,
                        ForeachIteration {
                            node,
                            block,
                            index,
                            item,
                        },
                    )
                    .await?;
                files.extend(iteration_files);
                if let Some(outcome) = outcome {
                    return Ok(outcome);
                }
            }
        } else {
            let context = Arc::new(context.clone());
            let journal = Arc::new(journal.clone_for_parallel()?);
            let block = Arc::new(block.clone());
            let mut tasks = JoinSet::new();
            let mut pending = items.into_iter().enumerate();
            let mut outcomes = BTreeMap::new();
            let mut task_error = None;
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
                        (index, outcome)
                    });
                }
                let Some(result) = tasks.join_next().await else {
                    break;
                };
                match result {
                    Ok((index, outcome)) => {
                        outcomes.insert(index, outcome);
                    }
                    Err(error) => {
                        task_error.get_or_insert_with(|| {
                            EngineError::Step(StepError::failed(
                                &node.id,
                                format!("foreach task failed: {error}"),
                            ))
                        });
                    }
                }
            }
            if let Some(error) = task_error {
                return Err(error);
            }
            for (_, outcome) in outcomes {
                let (iteration_files, outcome) = outcome?;
                files.extend(iteration_files);
                if let Some(outcome) = outcome {
                    return Ok(outcome);
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
        context.checkpoint()?;
        let parent_item = vars.item().cloned();
        vars.set_item(Some(item));
        journal.event(
            "foreach_iteration",
            json!({ "node": node.id, "index": index }),
        )?;
        let foreach_path = NodePath::root(node.id.clone());
        for block_node in block {
            context.checkpoint()?;
            let block_path = foreach_path.foreach_child(index, &block_node.id);
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
            journal.event(
                "step_started",
                json!({ "node": block_id, "type": addressed_node.kind.to_string(), "attempt": 1 }),
            )?;
            budget.consume(&block_id)?;
            match self
                .execute_node_after_budget(context, journal, vars, budget, &addressed_node)
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
                    journal.event(
                        "step_finished",
                        json!({ "node": block_id, "status": "success", "output": output, "output_name": block_id, "files": file_pins }),
                    )?;
                }
                StepOutcome::CheckFailed {
                    findings,
                    output,
                    files: failed_files,
                } => {
                    vars.set_item(parent_item);
                    let mut all_files = files;
                    all_files.extend(failed_files);
                    return Ok((
                        all_files,
                        Some(StepOutcome::CheckFailed {
                            findings,
                            output,
                            files: vec![],
                        }),
                    ));
                }
                StepOutcome::NeedsUser { question } => {
                    vars.set_item(parent_item);
                    return Ok((files, Some(StepOutcome::NeedsUser { question })));
                }
                StepOutcome::NeedsConfirm { confirm } => {
                    vars.set_item(parent_item);
                    return Ok((files, Some(StepOutcome::NeedsConfirm { confirm })));
                }
            }
        }
        vars.set_item(parent_item);
        Ok((files, None))
    }
}
