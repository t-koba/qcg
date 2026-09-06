use crate::{
    CmdGateway, CommandBounds, FsGateway, HttpGateway, JournalLimits, JournalWriter, NodeOutcome,
    SecretStore, StepError, StepOutcome, StepRegistry, TemplateService, collect_outputs,
    collect_resource_hashes, write_output_manifest_with_limits,
};
use camino::Utf8PathBuf;
use qcg_api::{FormSpec, RunNodeFailureEventData};
use qcg_contract::{Contract, NodeState, ValueBag};
use qcg_contract::{ExhaustedAction, OnFail};
use qcg_contract::{FieldType, InputField};
use qcg_types::{FailureCode, FailureDetail, NodePath, OutputManifest};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use super::checkpoint::{
    CheckpointAccounting, default_metadata_dir, pin_files, verify_resource_pins,
};
use super::repair_support::{RepairCycleOutcome, exhausted_question, failure_from_findings};
use super::replay::{BudgetTracker, ExecutionEnv, JournalReplay};
use super::types::{
    Engine, EngineError, Progress, RunContext, RunFailure, RunFailureKind, RunOptions,
    RunSnapshotSource, canonical_file_inputs, materialize_file_inputs,
};

impl Engine {
    pub fn new(registry: StepRegistry) -> Self {
        Self {
            registry,
            snapshot_source: None,
        }
    }

    /// Serve other runs' states to `await` nodes. The service sets this;
    /// direct runs leave it empty.
    pub fn with_snapshot_source(mut self, snapshot_source: Arc<dyn RunSnapshotSource>) -> Self {
        self.snapshot_source = Some(snapshot_source);
        self
    }

    pub async fn run(
        &self,
        contract: Contract,
        inputs: BTreeMap<String, serde_json::Value>,
        options: RunOptions,
    ) -> Result<OutputManifest, EngineError> {
        let run_id = Uuid::now_v7().to_string();
        let metadata_dir = default_metadata_dir(&options.output_dir, &run_id);
        let journal_limits = JournalLimits::from(&contract.manifest.runtime);
        let cancellation = options.cancellation.clone();
        let event_sender = options.event_sender.clone();
        let result = self
            .run_inner(
                contract,
                inputs,
                options,
                run_id.clone(),
                metadata_dir.clone(),
            )
            .await;
        if result.as_ref().is_err_and(|error| error.is_canceled()) {
            let journal = JournalWriter::create_with_limits(
                &metadata_dir.join("journal.jsonl"),
                run_id,
                false,
                event_sender,
                journal_limits,
            )?;
            if !matches!(
                journal.state().terminal,
                Some(crate::TerminalState::Canceled)
            ) {
                journal.event(
                    "run_canceled",
                    json!({ "reason": FailureDetail::new(FailureCode::Canceled, "cancellation requested") }),
                )?;
            }
            cancellation.cancel();
        }
        result
    }

    pub async fn run_with_id(
        &self,
        run_id: String,
        metadata_dir: Utf8PathBuf,
        contract: Contract,
        inputs: BTreeMap<String, serde_json::Value>,
        options: RunOptions,
    ) -> Result<OutputManifest, EngineError> {
        let cancellation = options.cancellation.clone();
        let journal_limits = JournalLimits::from(&contract.manifest.runtime);
        let event_sender = options.event_sender.clone();
        let result = self
            .run_inner(
                contract,
                inputs,
                options,
                run_id.clone(),
                metadata_dir.clone(),
            )
            .await;
        if result.as_ref().is_err_and(|error| error.is_canceled()) {
            let journal = JournalWriter::create_with_limits(
                &metadata_dir.join("journal.jsonl"),
                run_id,
                false,
                event_sender,
                journal_limits,
            )?;
            if !matches!(
                journal.state().terminal,
                Some(crate::TerminalState::Canceled)
            ) {
                journal.event(
                    "run_canceled",
                    json!({ "reason": FailureDetail::new(FailureCode::Canceled, "cancellation requested") }),
                )?;
            }
            cancellation.cancel();
        }
        result
    }

    pub async fn advance_with_id(
        &self,
        run_id: String,
        metadata_dir: Utf8PathBuf,
        contract: Contract,
        inputs: BTreeMap<String, serde_json::Value>,
        options: RunOptions,
    ) -> Progress {
        match self
            .run_with_id(run_id, metadata_dir, contract, inputs, options)
            .await
        {
            Ok(manifest) => Progress::Done(manifest),
            Err(EngineError::NeedsUser { question, .. }) => {
                Progress::Suspended(crate::Interaction::Question {
                    question: *question,
                })
            }
            Err(EngineError::NeedsConfirm { confirm, .. }) => {
                Progress::Suspended(crate::Interaction::Confirmation { confirm: *confirm })
            }
            Err(error) => Progress::Failed(RunFailure {
                kind: if error.is_canceled() {
                    RunFailureKind::Canceled
                } else {
                    RunFailureKind::Execution
                },
                message: error.to_string(),
            }),
        }
    }

    async fn run_inner(
        &self,
        contract: Contract,
        inputs: BTreeMap<String, serde_json::Value>,
        options: RunOptions,
        requested_run_id: String,
        metadata_dir: Utf8PathBuf,
    ) -> Result<OutputManifest, EngineError> {
        let journal_limits = JournalLimits::from(&contract.manifest.runtime);
        let mut validation_registry = self.registry.clone();
        if let Some(provider) = options.llm_provider.as_deref() {
            validation_registry.reserve_secret_env_names(provider.credential_env_names());
        }
        validation_registry.validate_contract(&contract)?;
        let inputs = contract.manifest.resolve_inputs(inputs)?;
        let inputs = canonical_file_inputs(&contract, inputs)?;
        std::fs::create_dir_all(&options.output_dir)?;
        let workspace = Utf8PathBuf::from_path_buf(dunce::canonicalize(&options.output_dir)?)
            .map_err(|path| {
                EngineError::Failed(format!(
                    "workspace path is not valid UTF-8: {}",
                    path.display()
                ))
            })?;
        std::fs::create_dir_all(&metadata_dir)?;
        let journal = JournalWriter::create_with_limits(
            &metadata_dir.join("journal.jsonl"),
            &requested_run_id,
            options.json_events,
            options.event_sender,
            journal_limits,
        )?;
        let replay = JournalReplay::from_state(journal.state());
        let run_id = match &replay.state.run_id {
            Some(existing) if existing != &requested_run_id => {
                return Err(EngineError::Failed(format!(
                    "run id mismatch: journal has `{existing}`, driver requested `{requested_run_id}`"
                )));
            }
            Some(existing) => existing.clone(),
            None => requested_run_id,
        };
        if let Some(expected) = &replay.state.contract_sha256
            && expected != &contract.sha256
        {
            return Err(EngineError::Failed(format!(
                "contract changed while resuming run `{run_id}`: expected {expected}, got {}",
                contract.sha256
            )));
        }
        let context = RunContext {
            run_id: run_id.clone(),
            fs: FsGateway::new(workspace.clone(), &contract.manifest.permissions),
            cmd: CmdGateway::new(contract.manifest.permissions.clone(), workspace.clone())
                .with_bounds(CommandBounds {
                    timeout_seconds: contract.manifest.runtime.command_timeout_seconds,
                    input_limit_bytes: contract.manifest.runtime.command_input_limit_bytes,
                    output_limit_bytes: contract.manifest.runtime.command_output_limit_bytes,
                })
                .with_cancellation(options.cancellation.clone()),
            http: HttpGateway::new(
                contract.manifest.permissions.clone(),
                std::time::Duration::from_secs(contract.manifest.runtime.http_timeout_seconds),
                contract.manifest.runtime.http_body_limit_bytes,
                contract.manifest.runtime.http_redirect_limit,
            )?
            .with_cancellation(options.cancellation.clone()),
            secrets: SecretStore::try_from_env(&contract.manifest.secrets)
                .map_err(EngineError::Failed)?,
            interactive: options.interactive,
            answers: options.answers,
            confirmations: options.confirmations,
            llm_provider: options.llm_provider,
            llm_seed_override: options.llm_seed_override,
            templates: TemplateService,
            cancellation: options.cancellation.clone(),
            snapshot_source: self.snapshot_source.clone(),
            replayed_steps: Arc::new(replay.steps.clone()),
            checkpoint_accounting: Arc::new(Mutex::new(CheckpointAccounting::default())),
            contract,
            workspace,
            metadata: metadata_dir.clone(),
        };
        let canonical_inputs = replay.state.inputs.clone().unwrap_or(inputs);
        let resource_hashes = collect_resource_hashes(&context).await?;
        if !replay.state.execution_started {
            journal.event(
                "run_started",
                json!({
                    "run_id": run_id,
                    "generator": format!("{}@{}", context.contract.manifest.generator.id, context.contract.manifest.generator.version),
                    "generator_path": context.contract.root,
                    "contract_sha256": context.contract.sha256,
                    "inputs": &canonical_inputs,
                    "resource_hashes": resource_hashes,
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": 1,
                    "retain_days": context.contract.manifest.journal.retain_days,
                }),
            )?;
            for resource in &resource_hashes {
                journal.event("resource", resource)?;
            }
            journal.event(
                "graph_resolved",
                json!({ "nodes": context.contract.graph.order }),
            )?;
        } else {
            journal.event("run_resumed", json!({ "run_id": run_id }))?;
        }

        let materialized_inputs =
            materialize_file_inputs(&context.contract, &canonical_inputs, &context.workspace)?;
        replay.verify_files(
            &context.workspace,
            &context.contract.manifest.runtime,
            &context.checkpoint_accounting,
        )?;
        verify_resource_pins(&replay.state, &resource_hashes)?;

        let mut vars = if replay.state.run_id.is_some() {
            let mut vars = replay.state.vars.clone();
            vars.set_inputs(materialized_inputs);
            vars
        } else {
            ValueBag::with_inputs(materialized_inputs)
        };
        let mut budget = BudgetTracker::new(
            options
                .max_total_steps
                .min(context.contract.manifest.budget.max_steps),
            replay.state.budget.steps_executed,
        );
        let max_parallel_steps = options.max_parallel_steps.max(1);
        let mut states: BTreeMap<String, NodeState> = context
            .contract
            .graph
            .nodes
            .keys()
            .map(|id| (id.clone(), NodeState::Pending))
            .collect();
        for (path, outcome) in &replay.state.nodes {
            if !context.contract.graph.nodes.contains_key(path.as_str()) {
                continue;
            }
            let state = match outcome {
                NodeOutcome::Success { .. } => continue,
                NodeOutcome::Skipped { reason } => NodeState::Skipped(reason.clone()),
                NodeOutcome::Failed { reason } => NodeState::Failed(reason.clone()),
            };
            states.insert(path.as_str().to_string(), state);
        }

        loop {
            if context.cancellation.is_cancelled() {
                return Err(EngineError::Canceled);
            }
            let mut progressed = false;
            let mut ready = Vec::new();
            for id in context.contract.graph.order.clone() {
                if !matches!(states.get(&id), Some(NodeState::Pending)) {
                    continue;
                }
                let node = context.contract.graph.nodes.get(&id).ok_or_else(|| {
                    StepError::failed("scheduler", format!("node `{id}` disappeared"))
                })?;
                if let Some(reason) = context
                    .contract
                    .graph
                    .should_skip_by_dependencies(node, &states)
                {
                    states.insert(id.clone(), NodeState::Skipped(reason.clone()));
                    journal.event("step_skipped", json!({ "node": id, "reason": reason }))?;
                    progressed = true;
                    continue;
                }
                if !context.contract.graph.needs_satisfied(node, &states) {
                    continue;
                }
                let when =
                    vars.eval_bool(node.when.as_ref())
                        .map_err(|message| EngineError::Expr {
                            node: id.clone(),
                            message,
                        })?;
                if !when {
                    let reason = FailureDetail::new(
                        FailureCode::WhenFalse,
                        format!("when expression evaluated false: {:?}", node.when),
                    );
                    states.insert(id.clone(), NodeState::Skipped(reason.clone()));
                    journal.event("step_skipped", json!({ "node": id, "reason": reason }))?;
                    progressed = true;
                    continue;
                }
                if let Some(replayed) = replay.steps.get(&id) {
                    if let Some(output_name) = &node.output
                        && let Some(output) = &replayed.output
                    {
                        vars.set_step_output(output_name, output.clone());
                    } else if let Some(output) = &replayed.output {
                        vars.set_step_output(&node.id, output.clone());
                    }
                    states.insert(id.clone(), NodeState::Success);
                    journal.event(
                        "step_replayed",
                        json!({ "node": id, "status": replayed.status }),
                    )?;
                    progressed = true;
                    continue;
                }
                ready.push(node.clone());
            }

            if ready.len() > 1
                && ready.len() <= max_parallel_steps
                && ready.iter().all(|node| self.is_parallel_safe_node(node))
            {
                self.execute_parallel_wave(
                    &context,
                    &journal,
                    &mut vars,
                    &mut states,
                    &mut budget,
                    ready,
                )
                .await?;
                progressed = true;
            } else if let Some(node) = ready.first() {
                let id = node.id.clone();
                budget.consume(&id)?;
                states.insert(id.clone(), NodeState::Running);
                let outcome = self
                    .execute_node_with_retry(&context, &journal, &mut vars, &mut budget, node)
                    .await?;
                if !matches!(&outcome, StepOutcome::NeedsConfirm { .. }) {
                    journal.event(
                        "step_started",
                        json!({ "node": id, "type": node.kind.to_string(), "attempt": 1 }),
                    )?;
                    tracing::debug!(run_id = %context.run_id, node = %id, "step started");
                }
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
                        states.insert(id.clone(), NodeState::Success);
                        journal.event("step_finished", json!({ "node": id, "status": "success", "files": file_pins, "output": output, "output_name": output_name }))?;
                    }
                    StepOutcome::CheckFailed {
                        findings,
                        output,
                        files,
                    } => {
                        let reason = failure_from_findings(&findings, FailureCode::CheckFailed);
                        let failure_file_pins = pin_files(
                            &context.workspace,
                            &context.metadata,
                            &files,
                            &context.contract.manifest.runtime,
                            &context.checkpoint_accounting,
                        )?;
                        let failure_output = output.clone();
                        match &node.on_fail {
                            Some(OnFail::Repair { .. }) => {
                                let env = ExecutionEnv {
                                    context: &context,
                                    journal: &journal,
                                };
                                match self
                                    .execute_repair_cycle(
                                        env,
                                        &mut vars,
                                        &mut states,
                                        &mut budget,
                                        node,
                                        findings,
                                    )
                                    .await?
                                {
                                    RepairCycleOutcome::Repaired { output } => {
                                        if let Some(value) = output.clone() {
                                            vars.set_step_output(&node.id, value);
                                        }
                                        states.insert(id.clone(), NodeState::Success);
                                        journal.event(
                                            "step_finished",
                                            json!({ "node": id, "status": "repaired", "output": output, "output_name": node.id, "failed_output": failure_output, "failed_files": failure_file_pins }),
                                        )?;
                                    }
                                    RepairCycleOutcome::Routed { to, output } => {
                                        vars.set_step_output(&node.id, output.clone());
                                        states.insert(id.clone(), NodeState::Success);
                                        journal.event(
                                            "step_finished",
                                            json!({ "node": id, "status": "routed", "to": to, "output": output, "output_name": node.id, "failed_output": failure_output, "failed_files": failure_file_pins }),
                                        )?;
                                    }
                                    RepairCycleOutcome::Answered { output } => {
                                        vars.set_step_output(&node.id, output.clone());
                                        states.insert(id.clone(), NodeState::Success);
                                        journal.event(
                                            "step_finished",
                                            json!({ "node": id, "status": "answered_on_fail", "output": output, "output_name": node.id, "failed_output": failure_output, "failed_files": failure_file_pins }),
                                        )?;
                                    }
                                    RepairCycleOutcome::Failed { reason } => {
                                        states
                                            .insert(id.clone(), NodeState::Failed(reason.clone()));
                                        journal.event(
                                            "step_finished",
                                            json!({ "node": id, "status": "repair_exhausted", "reason": reason }),
                                        )?;
                                    }
                                }
                            }
                            Some(OnFail::Route { to }) => {
                                let output = json!({ "routed_to": to, "findings": findings, "failed_output": failure_output, "failed_files": failure_file_pins });
                                vars.set_step_output(&node.id, output.clone());
                                states.insert(id.clone(), NodeState::Success);
                                journal.event(
                                    "step_finished",
                                    json!({ "node": id, "status": "routed", "to": to, "findings": findings, "output": output, "output_name": node.id }),
                                )?;
                            }
                            Some(OnFail::AskUser) => {
                                let question_id = format!("{}:on_fail", node.id);
                                if let Some(answer) = context.answers.get(&question_id) {
                                    let output = json!({ "answer": answer, "findings": findings });
                                    vars.set_step_output(&node.id, output.clone());
                                    states.insert(id.clone(), NodeState::Success);
                                    journal.event(
                                        "step_finished",
                                        json!({ "node": id, "status": "answered_on_fail", "answer": answer, "output": output, "output_name": node.id, "failed_output": failure_output, "failed_files": failure_file_pins }),
                                    )?;
                                } else {
                                    let question = FormSpec {
                                        id: question_id,
                                        title: format!(
                                            "Resolve check failure for node `{}`",
                                            node.id
                                        ),
                                        title_i18n: Default::default(),
                                        fields: vec![InputField {
                                            id: "answer".into(),
                                            label: None,
                                            label_i18n: Default::default(),
                                            description: None,
                                            description_i18n: Default::default(),
                                            placeholder: None,
                                            placeholder_i18n: Default::default(),
                                            kind: FieldType::String,
                                            required: true,
                                            default: None,
                                            pattern: None,
                                            options: vec![],
                                            option_labels_i18n: Default::default(),
                                            min_items: None,
                                            item_type: None,
                                            schema: None,
                                            ui: Default::default(),
                                        }],
                                    };
                                    journal.event(
                                        "step_finished",
                                        json!({ "node": id, "status": "needs_user", "question": question, "findings": findings, "failed_output": failure_output, "failed_files": failure_file_pins }),
                                    )?;
                                    return Err(EngineError::NeedsUser {
                                        question_id: question.id.clone(),
                                        question: Box::new(question),
                                    });
                                }
                            }
                            Some(OnFail::Regenerate {
                                max_attempts,
                                on_exhausted,
                            }) => {
                                let env = ExecutionEnv {
                                    context: &context,
                                    journal: &journal,
                                };
                                match self
                                    .execute_regenerate(
                                        env,
                                        &mut vars,
                                        &mut budget,
                                        node,
                                        *max_attempts,
                                        findings,
                                    )
                                    .await?
                                {
                                    StepOutcome::Success { output, files } => {
                                        let file_pins = pin_files(
                                            &context.workspace,
                                            &context.metadata,
                                            &files,
                                            &context.contract.manifest.runtime,
                                            &context.checkpoint_accounting,
                                        )?;
                                        let output_name =
                                            node.output.as_deref().unwrap_or(&node.id);
                                        if let Some(output_name) = &node.output {
                                            if let Some(value) = output.clone() {
                                                vars.set_step_output(output_name, value);
                                            }
                                        } else if let Some(value) = output.clone() {
                                            vars.set_step_output(&node.id, value);
                                        }
                                        states.insert(id.clone(), NodeState::Success);
                                        journal.event(
                                            "step_finished",
                                            json!({ "node": id, "status": "regenerated", "files": file_pins, "output": output, "output_name": output_name }),
                                        )?;
                                    }
                                    StepOutcome::CheckFailed { findings, .. } => match on_exhausted
                                    {
                                        ExhaustedAction::Fail => {
                                            let reason = failure_from_findings(
                                                &findings,
                                                FailureCode::CheckFailed,
                                            );
                                            states.insert(
                                                id.clone(),
                                                NodeState::Failed(reason.clone()),
                                            );
                                            journal.event(
                                                    "step_finished",
                                                    json!({ "node": id, "status": "regenerate_exhausted", "findings": findings, "reason": reason }),
                                                )?;
                                        }
                                        ExhaustedAction::Route { to } => {
                                            let output = json!({
                                                "status": "regenerate_exhausted",
                                                "routed_to": to,
                                                "findings": findings,
                                            });
                                            vars.set_step_output(&node.id, output.clone());
                                            states.insert(id.clone(), NodeState::Success);
                                            journal.event(
                                                    "step_finished",
                                                    json!({ "node": id, "status": "routed", "to": to, "output": output, "output_name": node.id }),
                                                )?;
                                        }
                                        ExhaustedAction::AskUser { title, fields } => {
                                            let question = exhausted_question(
                                                node,
                                                "regenerate",
                                                title.as_deref(),
                                                fields,
                                            );
                                            if let Some(answer) = context.answers.get(&question.id)
                                            {
                                                let output = json!({
                                                    "status": "regenerate_exhausted_answered",
                                                    "answer": answer,
                                                    "findings": findings,
                                                });
                                                vars.set_step_output(&node.id, output.clone());
                                                states.insert(id.clone(), NodeState::Success);
                                                journal.event(
                                                        "step_finished",
                                                        json!({ "node": id, "status": "answered_on_fail", "answer": answer, "output": output, "output_name": node.id }),
                                                    )?;
                                            } else {
                                                journal.event(
                                                        "step_finished",
                                                        json!({ "node": id, "status": "needs_user", "question": question, "findings": findings }),
                                                    )?;
                                                return Err(EngineError::NeedsUser {
                                                    question_id: question.id.clone(),
                                                    question: Box::new(question),
                                                });
                                            }
                                        }
                                    },
                                    StepOutcome::NeedsUser { question } => {
                                        journal.event(
                                            "step_finished",
                                            json!({ "node": id, "status": "needs_user", "question": question }),
                                        )?;
                                        return Err(EngineError::NeedsUser {
                                            question_id: question.id.clone(),
                                            question: Box::new(question),
                                        });
                                    }
                                    StepOutcome::NeedsConfirm { confirm } => {
                                        journal.event(
                                            "confirm_request",
                                            json!({ "node": id, "confirm": confirm }),
                                        )?;
                                        return Err(EngineError::NeedsConfirm {
                                            confirm_id: confirm.id.clone(),
                                            confirm: Box::new(confirm),
                                        });
                                    }
                                }
                            }
                            Some(OnFail::Fail) | None => {
                                states.insert(id.clone(), NodeState::Failed(reason.clone()));
                                journal.event(
                                    "step_finished",
                                json!({ "node": id, "status": "check_failed", "findings": findings, "reason": reason, "output": failure_output, "files": failure_file_pins }),
                                )?;
                            }
                        }
                    }
                    StepOutcome::NeedsUser { question } => {
                        journal.event(
                            "step_finished",
                            json!({ "node": id, "status": "needs_user", "question": question }),
                        )?;
                        return Err(EngineError::NeedsUser {
                            question_id: question.id.clone(),
                            question: Box::new(question),
                        });
                    }
                    StepOutcome::NeedsConfirm { confirm } => {
                        journal
                            .event("confirm_request", json!({ "node": id, "confirm": confirm }))?;
                        return Err(EngineError::NeedsConfirm {
                            confirm_id: confirm.id.clone(),
                            confirm: Box::new(confirm),
                        });
                    }
                }
                progressed = true;
            }
            if states
                .values()
                .all(|state| !matches!(state, NodeState::Pending | NodeState::Running))
            {
                break;
            }
            if !progressed {
                return Err(StepError::failed("scheduler", "no runnable node remains").into());
            }
        }

        let failed: Vec<_> = states
            .iter()
            .filter_map(|(id, state)| match state {
                NodeState::Failed(reason) => Some(RunNodeFailureEventData {
                    path: NodePath::root(id),
                    failure: reason.clone(),
                }),
                _ => None,
            })
            .collect();
        if !failed.is_empty() {
            journal.event(
                "run_finished",
                json!({ "status": "failed", "failures": failed }),
            )?;
            return Err(EngineError::Failed(
                failed
                    .iter()
                    .map(|failure| format!("{}: {}", failure.path, failure.failure))
                    .collect::<Vec<_>>()
                    .join("; "),
            ));
        }

        let outputs = collect_outputs(
            &context.workspace,
            &context.contract.manifest,
            &vars,
            &context.templates,
        )?;
        write_output_manifest_with_limits(
            &metadata_dir,
            &outputs,
            &context.contract.manifest.runtime,
        )?;
        for artifact in &outputs.artifacts {
            journal.event("artifact", artifact)?;
        }
        journal.event("run_finished", json!({ "status": "success" }))?;
        Ok(outputs)
    }
}
