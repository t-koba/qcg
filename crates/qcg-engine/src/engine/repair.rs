use crate::{StepControlFlow, StepError, StepOutcome};
use qcg_contract::{ExhaustedAction, NodeDef, OnFail};
use qcg_contract::{NodeState, ValueBag};
use qcg_types::{FailureCode, FailureDetail, Finding, NodePath};
use serde_json::json;
use std::collections::BTreeMap;

use super::checkpoint::pin_files;
use super::repair_support::{RepairCycleOutcome, exhausted_question, failure_from_findings};
use super::replay::{BudgetTracker, ExecutionEnv};
use super::types::{Engine, EngineError, NamedNodeTarget};

impl Engine {
    pub(crate) async fn execute_repair_cycle(
        &self,
        env: ExecutionEnv<'_>,
        vars: &mut ValueBag,
        states: &mut BTreeMap<String, NodeState>,
        budget: &mut BudgetTracker,
        failed_node: &NodeDef,
        initial_findings: Vec<Finding>,
    ) -> Result<RepairCycleOutcome, EngineError> {
        let Some(OnFail::Repair {
            repair,
            recheck,
            max_attempts,
            on_exhausted,
        }) = &failed_node.on_fail
        else {
            return Ok(RepairCycleOutcome::Failed {
                reason: FailureDetail::new(
                    FailureCode::RepairExhausted,
                    "repair cycle requested for non-repair on_fail",
                ),
            });
        };
        if *max_attempts == 0 {
            return Ok(RepairCycleOutcome::Failed {
                reason: FailureDetail::new(
                    FailureCode::RepairExhausted,
                    "repair max_attempts must be greater than 0",
                ),
            });
        }
        vars.set_step_output(
            failed_node.id.as_str(),
            json!({ "status": "check_failed", "findings": initial_findings }),
        );
        let mut last_reason = failure_from_findings(&initial_findings, FailureCode::CheckFailed);
        for attempt in 1..=*max_attempts {
            env.context.checkpoint()?;
            env.journal.event(
                "repair_attempt_started",
                json!({
                    "node": failed_node.id,
                    "repair": repair,
                    "recheck": recheck,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                }),
            )?;
            let repair_output = self
                .execute_named_node(
                    env,
                    vars,
                    states,
                    budget,
                    NamedNodeTarget {
                        owner_path: &failed_node.id,
                        node_id: repair,
                        attempt,
                    },
                )
                .await?;
            if !matches!(repair_output, StepOutcome::Success { .. }) {
                last_reason = FailureDetail::new(
                    FailureCode::RepairExhausted,
                    format!("repair node `{repair}` did not succeed"),
                );
                env.journal.event(
                    "repair_attempt_finished",
                    json!({ "node": failed_node.id, "attempt": attempt, "status": "repair_failed", "reason": last_reason }),
                )?;
                continue;
            }
            match self
                .execute_named_node(
                    env,
                    vars,
                    states,
                    budget,
                    NamedNodeTarget {
                        owner_path: &failed_node.id,
                        node_id: recheck,
                        attempt,
                    },
                )
                .await?
            {
                StepOutcome::Success { output, .. } => {
                    env.journal.event(
                        "repair_attempt_finished",
                        json!({ "node": failed_node.id, "attempt": attempt, "status": "repaired" }),
                    )?;
                    return Ok(RepairCycleOutcome::Repaired {
                        output: Some(json!({
                            "status": "repaired",
                            "attempts": attempt,
                            "recheck": output,
                        })),
                    });
                }
                StepOutcome::CheckFailed { findings, .. } => {
                    last_reason = failure_from_findings(&findings, FailureCode::CheckFailed);
                    vars.set_step_output(
                        failed_node.id.as_str(),
                        json!({ "status": "check_failed", "findings": findings }),
                    );
                    env.journal.event(
                        "repair_attempt_finished",
                        json!({ "node": failed_node.id, "attempt": attempt, "status": "recheck_failed", "findings": findings }),
                    )?;
                }
                StepOutcome::NeedsUser { question } => {
                    env.journal.event(
                        "repair_attempt_finished",
                        json!({ "node": failed_node.id, "attempt": attempt, "status": "needs_user", "question": question }),
                    )?;
                    return Err(EngineError::NeedsUser {
                        question_id: question.id.clone(),
                        question: Box::new(question),
                    });
                }
                StepOutcome::NeedsConfirm { confirm } => {
                    env.journal.event(
                        "repair_attempt_finished",
                        json!({ "node": failed_node.id, "attempt": attempt, "status": "needs_confirm", "confirm": confirm }),
                    )?;
                    return Err(EngineError::NeedsConfirm {
                        confirm_id: confirm.id.clone(),
                        confirm: Box::new(confirm),
                    });
                }
            }
        }
        match on_exhausted {
            ExhaustedAction::Route { to } => {
                states.insert(
                    repair.clone(),
                    NodeState::Skipped("repair cycle exhausted".into()),
                );
                states.insert(
                    recheck.clone(),
                    NodeState::Skipped("repair cycle exhausted".into()),
                );
                Ok(RepairCycleOutcome::Routed {
                    to: to.clone(),
                    output: json!({
                        "routed_to": to,
                        "status": "repair_exhausted",
                        "attempts": max_attempts,
                        "reason": last_reason,
                    }),
                })
            }
            ExhaustedAction::AskUser { title, fields } => {
                states.insert(
                    repair.clone(),
                    NodeState::Skipped("repair cycle exhausted".into()),
                );
                states.insert(
                    recheck.clone(),
                    NodeState::Skipped("repair cycle exhausted".into()),
                );
                let question = exhausted_question(failed_node, "repair", title.as_deref(), fields);
                if let Some(answer) = env.context.answers.get(&question.id) {
                    Ok(RepairCycleOutcome::Answered {
                        output: json!({
                            "status": "repair_exhausted_answered",
                            "attempts": max_attempts,
                            "answer": answer,
                            "reason": last_reason,
                        }),
                    })
                } else {
                    Err(EngineError::NeedsUser {
                        question_id: question.id.clone(),
                        question: Box::new(question),
                    })
                }
            }
            ExhaustedAction::Fail => {
                states.insert(
                    repair.clone(),
                    NodeState::Skipped("repair cycle exhausted".into()),
                );
                states.insert(
                    recheck.clone(),
                    NodeState::Skipped("repair cycle exhausted".into()),
                );
                Ok(RepairCycleOutcome::Failed {
                    reason: FailureDetail::new(
                        FailureCode::RepairExhausted,
                        format!(
                            "repair cycle exhausted after {max_attempts} attempt(s): {last_reason}"
                        ),
                    ),
                })
            }
        }
    }

    pub(crate) async fn execute_regenerate(
        &self,
        env: ExecutionEnv<'_>,
        vars: &mut ValueBag,
        budget: &mut BudgetTracker,
        node: &NodeDef,
        max_attempts: u32,
        initial_findings: Vec<Finding>,
    ) -> Result<StepOutcome, EngineError> {
        if max_attempts == 0 {
            return Ok(StepOutcome::CheckFailed {
                findings: initial_findings,
                output: None,
                files: vec![],
            });
        }
        let mut last_findings = initial_findings;
        vars.set_step_output(
            node.id.as_str(),
            json!({ "status": "check_failed", "findings": last_findings }),
        );
        for attempt in 1..=max_attempts {
            env.journal.event(
                "regenerate_attempt_started",
                json!({ "node": node.id, "attempt": attempt, "max_attempts": max_attempts }),
            )?;
            match self
                .execute_node(env.context, env.journal, vars, budget, node)
                .await?
            {
                StepOutcome::CheckFailed { findings, .. } => {
                    vars.set_step_output(
                        node.id.as_str(),
                        json!({ "status": "check_failed", "findings": findings }),
                    );
                    env.journal.event(
                        "regenerate_attempt_finished",
                        json!({ "node": node.id, "attempt": attempt, "status": "check_failed", "findings": findings }),
                    )?;
                    last_findings = findings;
                }
                outcome @ StepOutcome::Success { .. } => {
                    env.journal.event(
                        "regenerate_attempt_finished",
                        json!({ "node": node.id, "attempt": attempt, "status": "success" }),
                    )?;
                    return Ok(outcome);
                }
                outcome @ StepOutcome::NeedsUser { .. }
                | outcome @ StepOutcome::NeedsConfirm { .. } => return Ok(outcome),
            }
        }
        Ok(StepOutcome::CheckFailed {
            findings: last_findings,
            output: None,
            files: vec![],
        })
    }

    async fn execute_named_node(
        &self,
        env: ExecutionEnv<'_>,
        vars: &mut ValueBag,
        states: &mut BTreeMap<String, NodeState>,
        budget: &mut BudgetTracker,
        target: NamedNodeTarget<'_>,
    ) -> Result<StepOutcome, EngineError> {
        let mut node = env
            .context
            .contract
            .graph
            .nodes
            .get(target.node_id)
            .ok_or_else(|| StepError::failed(target.node_id, "referenced node was not found"))?
            .clone();
        let graph_node_id = node.id.clone();
        let path = NodePath::root(target.owner_path).repair_child(target.attempt, &graph_node_id);
        node.id = path.to_string();
        node.output = None;
        if let Some(replayed) = env.context.replayed_steps.get(&node.id) {
            if let Some(output) = replayed.output.clone() {
                vars.set_step_output(&node.id, output.clone());
                vars.set_step_output(&graph_node_id, output.clone());
            }
            states.insert(graph_node_id, NodeState::Success);
            env.journal.event(
                "step_replayed",
                json!({ "node": node.id, "status": replayed.status }),
            )?;
            return Ok(StepOutcome::Success {
                output: replayed.output.clone(),
                files: replayed
                    .files
                    .iter()
                    .map(|pin| env.context.workspace.join(&pin.path))
                    .collect(),
            });
        }
        budget.consume(&node.id)?;
        states.insert(graph_node_id.clone(), NodeState::Running);
        env.journal.event(
            "step_started",
            json!({ "node": node.id, "type": node.kind.to_string(), "attempt": target.attempt }),
        )?;
        let outcome = self
            .execute_node_after_budget(env.context, env.journal, vars, budget, &node)
            .await?;
        match &outcome {
            StepOutcome::Success { output, files } => {
                let file_pins = pin_files(
                    &env.context.workspace,
                    &env.context.metadata,
                    files,
                    &env.context.contract.manifest.runtime,
                    &env.context.checkpoint_accounting,
                )?;
                let output_name = node.output.as_deref().unwrap_or(&node.id);
                if let Some(output_name) = &node.output {
                    if let Some(value) = output.clone() {
                        vars.set_step_output(output_name, value.clone());
                        vars.set_step_output(&graph_node_id, value);
                    }
                } else if let Some(value) = output.clone() {
                    vars.set_step_output(&node.id, value.clone());
                    vars.set_step_output(&graph_node_id, value);
                }
                states.insert(graph_node_id.clone(), NodeState::Success);
                env.journal.event(
                    "step_finished",
                    json!({ "node": node.id, "status": "success", "files": file_pins, "output": output, "output_name": output_name }),
                )?;
            }
            StepOutcome::CheckFailed {
                findings,
                output,
                files,
            } => {
                let reason = failure_from_findings(findings, FailureCode::CheckFailed);
                let file_pins = pin_files(
                    &env.context.workspace,
                    &env.context.metadata,
                    files,
                    &env.context.contract.manifest.runtime,
                    &env.context.checkpoint_accounting,
                )?;
                states.insert(graph_node_id.clone(), NodeState::Failed(reason.clone()));
                env.journal.event(
                    "step_finished",
                    json!({ "node": node.id, "status": "check_failed", "findings": findings, "reason": reason, "output": output, "files": file_pins }),
                )?;
            }
            StepOutcome::NeedsUser { question } => {
                env.journal.event(
                    "step_finished",
                    json!({ "node": node.id, "status": "needs_user", "question": question }),
                )?;
            }
            StepOutcome::NeedsConfirm { confirm } => {
                env.journal.event(
                    "confirm_request",
                    json!({ "node": node.id, "confirm": confirm }),
                )?;
            }
        }
        Ok(outcome)
    }

    pub(crate) fn is_parallel_safe_node(&self, node: &NodeDef) -> bool {
        node.on_fail.is_none()
            && self
                .registry
                .traits(&node.kind)
                .is_some_and(|traits| traits.parallel_safe)
    }

    pub(crate) fn is_foreach_node(&self, node: &NodeDef) -> bool {
        self.registry
            .traits(&node.kind)
            .is_some_and(|traits| traits.control_flow == StepControlFlow::Foreach)
    }
}
