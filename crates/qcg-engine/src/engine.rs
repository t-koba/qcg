use crate::{
    CmdGateway, ConfirmSpec, FailureCode, FailureDetail, FilePin, FormSpec, FsGateway, HttpGateway,
    JournalLimits, JournalWriter, NodeOutcome, NodePath, OutputManifest, ResourceSnapshot,
    RunEvent, RunNodeFailureEventData, RunState, SecretStore, StepContext, StepControlFlow,
    StepError, StepOutcome, StepRegistry, TemplateService, collect_outputs,
    collect_resource_hashes, write_output_manifest_with_limits,
};
use camino::Utf8PathBuf;
use qcg_contract::{Contract, NodeState, RuntimeLimits, SideEffects, ValueBag};
use qcg_contract::{ExhaustedAction, NodeDef, OnFail};
use qcg_types::{FieldType, FileValue, Finding, InputField};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const MAX_FOREACH_ITERATIONS: usize = 10_000;
pub const MAX_FOREACH_PARALLELISM: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Contract(#[from] qcg_contract::ContractError),
    #[error(transparent)]
    Step(#[from] StepError),
    #[error(transparent)]
    Gateway(#[from] crate::GatewayError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Journal(#[from] crate::JournalError),
    #[error("expression error in node `{node}`: {message}")]
    Expr { node: String, message: String },
    #[error("run failed: {0}")]
    Failed(String),
    #[error("run was canceled")]
    Canceled,
    #[error("run is waiting for user input: {question_id}")]
    NeedsUser {
        question_id: String,
        question: Box<FormSpec>,
    },
    #[error("run is waiting for side-effect confirmation: {confirm_id}")]
    NeedsConfirm {
        confirm_id: String,
        confirm: Box<ConfirmSpec>,
    },
}

#[derive(Debug)]
pub enum Progress {
    Advanced,
    Suspended(crate::Interaction),
    Done(OutputManifest),
    Failed(RunFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunFailureKind {
    Canceled,
    Execution,
}

#[derive(Debug)]
pub struct RunFailure {
    pub kind: RunFailureKind,
    pub message: String,
}

#[derive(Clone, Copy)]
struct NamedNodeTarget<'a> {
    owner_path: &'a str,
    node_id: &'a str,
    attempt: u32,
}

struct ForeachIteration<'a> {
    node: &'a NodeDef,
    block: &'a [NodeDef],
    index: usize,
    item: Value,
}

impl EngineError {
    pub fn is_canceled(&self) -> bool {
        matches!(
            self,
            Self::Canceled | Self::Gateway(crate::GatewayError::Canceled)
        ) || matches!(self, Self::Step(error) if error.is_cancelled())
    }
}

#[derive(Clone)]
pub struct RunOptions {
    pub output_dir: Utf8PathBuf,
    pub json_events: bool,
    pub event_sender: Option<broadcast::Sender<RunEvent>>,
    pub interactive: bool,
    pub answers: BTreeMap<String, Value>,
    pub confirmations: BTreeMap<String, bool>,
    pub max_total_steps: usize,
    pub max_parallel_steps: usize,
    pub llm_provider: Option<Arc<dyn qcg_llm::LlmProvider>>,
    pub llm_seed_override: Option<u64>,
    pub cancellation: CancellationToken,
}

impl RunOptions {
    pub fn default_max_total_steps() -> usize {
        10_000
    }

    pub fn default_max_parallel_steps() -> usize {
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .max(1)
    }
}

#[derive(Clone)]
pub struct RunContext {
    pub run_id: String,
    pub contract: Contract,
    pub workspace: Utf8PathBuf,
    pub metadata: Utf8PathBuf,
    pub fs: FsGateway,
    pub cmd: CmdGateway,
    pub http: HttpGateway,
    pub secrets: SecretStore,
    pub interactive: bool,
    pub answers: BTreeMap<String, Value>,
    pub confirmations: BTreeMap<String, bool>,
    pub llm_provider: Option<Arc<dyn qcg_llm::LlmProvider>>,
    pub llm_seed_override: Option<u64>,
    pub templates: TemplateService,
    pub cancellation: CancellationToken,
    replayed_steps: Arc<BTreeMap<String, ReplayedStep>>,
    checkpoint_accounting: Arc<Mutex<CheckpointAccounting>>,
}

#[derive(Clone)]
pub struct Engine {
    registry: StepRegistry,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForeachControlParams {
    items: String,
    subflow: String,
    max_iterations: usize,
    #[serde(default = "default_foreach_parallelism")]
    parallel: usize,
}

fn default_foreach_parallelism() -> usize {
    1
}

fn canonical_file_inputs(
    contract: &Contract,
    mut inputs: BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>, EngineError> {
    for field in contract
        .manifest
        .inputs
        .stages
        .iter()
        .flat_map(|stage| &stage.fields)
        .filter(|field| matches!(field.kind, FieldType::File))
    {
        let Some(value) = inputs.get(&field.id) else {
            continue;
        };
        let file = FileValue::from_value_with_limit(
            value,
            contract.manifest.runtime.file_input_limit_bytes,
        )
        .map_err(|error| {
            EngineError::Failed(format!("invalid file input `{}`: {error}", field.id))
        })?;
        inputs.insert(
            field.id.clone(),
            serde_json::to_value(file).map_err(|error| EngineError::Failed(error.to_string()))?,
        );
    }
    Ok(inputs)
}

fn materialize_file_inputs(
    contract: &Contract,
    inputs: &BTreeMap<String, Value>,
    workspace: &camino::Utf8Path,
) -> Result<BTreeMap<String, Value>, EngineError> {
    let mut materialized = inputs.clone();
    for field in contract
        .manifest
        .inputs
        .stages
        .iter()
        .flat_map(|stage| &stage.fields)
        .filter(|field| matches!(field.kind, FieldType::File))
    {
        let Some(value) = inputs.get(&field.id) else {
            continue;
        };
        let file = FileValue::from_value_with_limit(
            value,
            contract.manifest.runtime.file_input_limit_bytes,
        )
        .map_err(|error| {
            EngineError::Failed(format!("invalid file input `{}`: {error}", field.id))
        })?;
        let relative = format!("files/{}/{}", field.id, file.name);
        let target = workspace.join(&relative);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(
            &target,
            file.decode_with_limit(contract.manifest.runtime.file_input_limit_bytes)
                .map_err(|error| {
                    EngineError::Failed(format!("invalid file input `{}`: {error}", field.id))
                })?,
        )?;
        materialized.insert(field.id.clone(), Value::String(relative));
    }
    Ok(materialized)
}

impl Engine {
    pub fn new(registry: StepRegistry) -> Self {
        Self { registry }
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
                .with_limits(contract.manifest.runtime.clone())
                .with_cancellation(options.cancellation.clone()),
            http: HttpGateway::new(
                contract.manifest.permissions.clone(),
                &contract.manifest.runtime,
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
                    .execute_node_after_budget(&context, &journal, &mut vars, &mut budget, node)
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

    async fn execute_parallel_wave(
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
                    .execute_node_after_budget(
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
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((node, outcome)) => {
                    outcomes.insert(node.id.clone(), (node, outcome));
                }
                Err(error) => {
                    join_error.get_or_insert_with(|| {
                        EngineError::Step(StepError::failed(
                            "scheduler",
                            format!("parallel task failed: {error}"),
                        ))
                    });
                }
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

    async fn execute_node(
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
                crate::LlmGateway::new(
                    Arc::clone(provider),
                    &context.secrets,
                    journal,
                    context.cancellation.clone(),
                    &context.contract.manifest.budget,
                    context.contract.manifest.llm.as_ref(),
                )
            }),
        };
        step_context.checkpoint().await?;
        Ok(executor.execute(&mut step_context, node).await?)
    }

    async fn execute_foreach(
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
        let items_value = vars.get_path(items_ref).ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("foreach items `{items_ref}` was not found"),
            )
        })?;
        let mut items = match items_value {
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

    async fn execute_repair_cycle(
        &self,
        env: ExecutionEnv<'_>,
        vars: &mut ValueBag,
        states: &mut BTreeMap<String, NodeState>,
        budget: &mut BudgetTracker,
        failed_node: &NodeDef,
        initial_findings: Vec<crate::Finding>,
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

    async fn execute_regenerate(
        &self,
        env: ExecutionEnv<'_>,
        vars: &mut ValueBag,
        budget: &mut BudgetTracker,
        node: &NodeDef,
        max_attempts: u32,
        initial_findings: Vec<crate::Finding>,
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

    fn is_parallel_safe_node(&self, node: &NodeDef) -> bool {
        node.on_fail.is_none()
            && self
                .registry
                .traits(&node.kind)
                .is_some_and(|traits| traits.parallel_safe)
    }

    fn is_foreach_node(&self, node: &NodeDef) -> bool {
        self.registry
            .traits(&node.kind)
            .is_some_and(|traits| traits.control_flow == StepControlFlow::Foreach)
    }
}

impl RunContext {
    fn checkpoint(&self) -> Result<(), EngineError> {
        if self.cancellation.is_cancelled() {
            Err(EngineError::Canceled)
        } else {
            Ok(())
        }
    }

    pub fn require_side_effect(
        &self,
        journal: &JournalWriter,
        node: &NodeDef,
        kind: &str,
        target: &str,
        details: Option<Value>,
    ) -> Result<Option<ConfirmSpec>, StepError> {
        let id = format!("{}:{kind}", node.id);
        let policy = &self.contract.manifest.permissions.side_effects;
        match policy {
            SideEffects::None => {
                journal
                    .event(
                        "side_effect",
                        json!({ "node": node.id, "kind": kind, "target": target, "decision": "denied", "policy": "none", "details": details }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                Err(StepError::failed(
                    &node.id,
                    format!(
                        "side effect `{kind}` to `{target}` is not allowed by permissions.side_effects=none"
                    ),
                ))
            }
            SideEffects::Allowed => {
                journal
                    .event(
                        "side_effect",
                        json!({ "node": node.id, "kind": kind, "target": target, "decision": "allowed", "policy": "allowed", "details": details }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                Ok(None)
            }
            SideEffects::Confirm | SideEffects::DryRunFirst => {
                if self.confirmations.get(&id).copied().unwrap_or(false) {
                    journal
                        .event(
                            "side_effect",
                            json!({ "node": node.id, "kind": kind, "target": target, "decision": "approved_by_user", "policy": format!("{policy:?}"), "details": details }),
                        )
                        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                    return Ok(None);
                }
                let dry_run = matches!(policy, SideEffects::DryRunFirst);
                if dry_run {
                    journal
                        .event(
                            "dry_run",
                            json!({ "node": node.id, "kind": kind, "target": target, "details": details.clone() }),
                        )
                        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                }
                journal
                    .event(
                        "side_effect",
                        json!({ "node": node.id, "kind": kind, "target": target, "decision": "confirmation_required", "policy": format!("{policy:?}"), "dry_run": dry_run, "details": details.clone() }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                Ok(Some(ConfirmSpec {
                    id,
                    title: format!("Confirm side effect `{kind}` for node `{}`", node.id),
                    kind: kind.to_string(),
                    target: target.to_string(),
                    dry_run,
                    details,
                }))
            }
        }
    }
}

enum RepairCycleOutcome {
    Repaired { output: Option<Value> },
    Routed { to: String, output: Value },
    Answered { output: Value },
    Failed { reason: FailureDetail },
}

fn exhausted_question(
    node: &NodeDef,
    phase: &str,
    title: Option<&str>,
    fields: &[InputField],
) -> FormSpec {
    let fields = if fields.is_empty() {
        vec![InputField {
            id: "answer".into(),
            label: Some("Resolution".into()),
            label_i18n: Default::default(),
            description: Some(format!(
                "Provide the resolution after {phase} attempts were exhausted"
            )),
            description_i18n: Default::default(),
            placeholder: None,
            placeholder_i18n: Default::default(),
            kind: FieldType::Text,
            required: true,
            default: None,
            pattern: None,
            options: Vec::new(),
            option_labels_i18n: Default::default(),
            min_items: None,
            item_type: None,
            schema: None,
            ui: Default::default(),
        }]
    } else {
        fields.to_vec()
    };
    FormSpec {
        id: format!("{}:{phase}_exhausted", node.id),
        title: title
            .map(str::to_string)
            .unwrap_or_else(|| format!("Resolve exhausted {phase} for node `{}`", node.id)),
        title_i18n: Default::default(),
        fields,
    }
}

fn failure_from_findings(findings: &[Finding], code: FailureCode) -> FailureDetail {
    let message = findings
        .iter()
        .map(|finding| finding.message.as_str())
        .filter(|message| !message.is_empty())
        .collect::<Vec<_>>()
        .join("; ");
    FailureDetail::new(
        code,
        if message.is_empty() {
            "check failed".into()
        } else {
            message
        },
    )
}

fn default_metadata_dir(workspace: &camino::Utf8Path, run_id: &str) -> Utf8PathBuf {
    workspace
        .parent()
        .unwrap_or_else(|| camino::Utf8Path::new("."))
        .join(".qcg/runs")
        .join(run_id)
        .join("meta")
}

#[derive(Default)]
struct CheckpointAccounting {
    bytes_by_path: BTreeMap<String, u64>,
    total_bytes: u64,
}

impl CheckpointAccounting {
    fn record(
        &mut self,
        path: &camino::Utf8Path,
        bytes: u64,
        limits: &RuntimeLimits,
    ) -> Result<Option<u64>, EngineError> {
        let file_limit = u64::try_from(limits.output_file_limit_bytes).map_err(|_| {
            EngineError::Failed("runtime.output_file_limit_bytes does not fit in u64".into())
        })?;
        let total_limit = u64::try_from(limits.output_total_limit_bytes).map_err(|_| {
            EngineError::Failed("runtime.output_total_limit_bytes does not fit in u64".into())
        })?;
        if file_limit == 0 || total_limit == 0 || limits.output_artifact_limit == 0 {
            return Err(EngineError::Failed(
                "runtime output limits must be greater than zero".into(),
            ));
        }
        if bytes > file_limit {
            return Err(EngineError::Failed(format!(
                "output file `{path}` exceeds {file_limit} bytes"
            )));
        }
        let key = path.as_str().to_owned();
        let previous = self.bytes_by_path.get(&key).copied();
        let next_count = self
            .bytes_by_path
            .len()
            .checked_add(if previous.is_some() { 0 } else { 1 })
            .ok_or_else(|| EngineError::Failed("output artifact count overflowed".into()))?;
        if next_count > limits.output_artifact_limit {
            return Err(EngineError::Failed(format!(
                "output artifact count exceeds {}",
                limits.output_artifact_limit
            )));
        }
        let total_without_previous = self
            .total_bytes
            .checked_sub(previous.unwrap_or(0))
            .ok_or_else(|| EngineError::Failed("output byte accounting underflowed".into()))?;
        let next_total = total_without_previous
            .checked_add(bytes)
            .ok_or_else(|| EngineError::Failed("output byte accounting overflowed".into()))?;
        if next_total > total_limit {
            return Err(EngineError::Failed(format!(
                "output bytes exceed {total_limit}"
            )));
        }
        self.total_bytes = next_total;
        self.bytes_by_path.insert(key, bytes);
        Ok(previous)
    }

    fn rollback(&mut self, path: &camino::Utf8Path, bytes: u64, previous: Option<u64>) {
        let key = path.as_str();
        self.total_bytes = self
            .total_bytes
            .saturating_sub(bytes)
            .saturating_add(previous.unwrap_or(0));
        match previous {
            Some(previous) => {
                self.bytes_by_path.insert(key.to_owned(), previous);
            }
            None => {
                self.bytes_by_path.remove(key);
            }
        }
    }
}

fn pin_files(
    workspace: &camino::Utf8Path,
    metadata: &camino::Utf8Path,
    files: &[Utf8PathBuf],
    limits: &RuntimeLimits,
    accounting: &Arc<Mutex<CheckpointAccounting>>,
) -> Result<Vec<FilePin>, EngineError> {
    let canonical_workspace = dunce::canonicalize(workspace)?;
    let canonical_workspace = Utf8PathBuf::from_path_buf(canonical_workspace).map_err(|path| {
        EngineError::Failed(format!(
            "workspace path is not valid UTF-8: {}",
            path.display()
        ))
    })?;
    files
        .iter()
        .map(|path| {
            let canonical_path = dunce::canonicalize(path)?;
            let canonical_path = Utf8PathBuf::from_path_buf(canonical_path).map_err(|path| {
                EngineError::Failed(format!(
                    "step output path is not valid UTF-8: {}",
                    path.display()
                ))
            })?;
            let relative = if canonical_path.is_absolute() {
                canonical_path
                    .strip_prefix(&canonical_workspace)
                    .map_err(|_| {
                        EngineError::Failed(format!(
                            "step output `{path}` is outside workspace `{workspace}`"
                        ))
                    })?
            } else {
                canonical_path.as_path()
            };
            if relative
                .components()
                .any(|component| matches!(component, camino::Utf8Component::ParentDir))
            {
                return Err(EngineError::Failed(format!(
                    "step output `{relative}` escapes the workspace"
                )));
            }
            let source = workspace.join(relative);
            let digest = hash_file(&source, limits.output_file_limit_bytes)?;
            let previous = {
                let mut accounting = accounting.lock().map_err(|_| {
                    EngineError::Failed("checkpoint accounting mutex was poisoned".into())
                })?;
                accounting.record(relative, digest.bytes, limits)?
            };
            if let Err(error) = persist_checkpoint_blob(
                metadata,
                &digest.sha256,
                &source,
                limits.output_file_limit_bytes,
            ) {
                let mut accounting = accounting.lock().map_err(|_| {
                    EngineError::Failed("checkpoint accounting mutex was poisoned".into())
                })?;
                accounting.rollback(relative, digest.bytes, previous);
                return Err(error);
            }
            Ok(FilePin {
                path: relative.to_path_buf(),
                sha256: digest.sha256,
            })
        })
        .collect()
}

fn existing_regular_files(files: Vec<Utf8PathBuf>) -> Result<Vec<Utf8PathBuf>, EngineError> {
    let mut existing = Vec::with_capacity(files.len());
    for path in files {
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => existing.push(path),
            Ok(_) => {
                return Err(EngineError::Failed(format!(
                    "step output `{path}` is not a regular file"
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(existing)
}

fn persist_checkpoint_blob(
    metadata: &camino::Utf8Path,
    sha256: &str,
    source: &camino::Utf8Path,
    file_limit: usize,
) -> Result<(), EngineError> {
    let blobs = metadata.join("checkpoint-blobs");
    std::fs::create_dir_all(&blobs)?;
    let destination = blobs.join(sha256);
    if destination.exists() {
        if hash_file(&destination, file_limit)?.sha256 != sha256 {
            return Err(EngineError::Failed(format!(
                "checkpoint blob `{sha256}` does not match its content digest"
            )));
        }
        return Ok(());
    }
    let temporary = blobs.join(format!(".{sha256}.tmp-{}", Uuid::now_v7()));
    let copy_result = (|| -> Result<(), std::io::Error> {
        let mut input = std::fs::File::open(source)?;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        {
            let mut writer = LimitedFileWriter {
                file: &mut file,
                bytes: 0,
                limit: u64::try_from(file_limit)
                    .map_err(|_| std::io::Error::other("output file limit does not fit in u64"))?,
            };
            std::io::copy(&mut input, &mut writer)?;
        }
        file.sync_all()?;
        Ok(())
    })();
    if let Err(error) = copy_result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    match std::fs::rename(&temporary, &destination) {
        Ok(()) => Ok(()),
        Err(_error) if destination.exists() => {
            let _ = std::fs::remove_file(&temporary);
            if hash_file(&destination, file_limit)?.sha256 == sha256 {
                Ok(())
            } else {
                Err(EngineError::Failed(format!(
                    "checkpoint blob `{sha256}` was replaced with invalid content"
                )))
            }
        }
        Err(error) => {
            let _ = std::fs::remove_file(&temporary);
            Err(error.into())
        }
    }
}

struct FileDigest {
    sha256: String,
    bytes: u64,
}

fn hash_file(path: &camino::Utf8Path, file_limit: usize) -> Result<FileDigest, std::io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let bytes = {
        let mut writer = Sha256Writer {
            digest: &mut digest,
            bytes: 0,
            limit: u64::try_from(file_limit)
                .map_err(|_| std::io::Error::other("output file limit does not fit in u64"))?,
        };
        std::io::copy(&mut file, &mut writer)?;
        writer.bytes
    };
    Ok(FileDigest {
        sha256: hex::encode(digest.finalize()),
        bytes,
    })
}

struct Sha256Writer<'a> {
    digest: &'a mut Sha256,
    bytes: u64,
    limit: u64,
}

impl std::io::Write for Sha256Writer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| std::io::Error::other("output byte count does not fit in u64"))?,
            )
            .ok_or_else(|| std::io::Error::other("output byte count overflowed"))?;
        let limit = self.limit;
        if next > limit {
            return Err(std::io::Error::other(format!(
                "output file exceeds {limit} bytes"
            )));
        }
        self.digest.update(bytes);
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct LimitedFileWriter<'a> {
    file: &'a mut std::fs::File,
    bytes: u64,
    limit: u64,
}

impl std::io::Write for LimitedFileWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| std::io::Error::other("output byte count does not fit in u64"))?,
            )
            .ok_or_else(|| std::io::Error::other("output byte count overflowed"))?;
        if next > self.limit {
            return Err(std::io::Error::other(format!(
                "output file exceeds {} bytes",
                self.limit
            )));
        }
        self.file.write_all(bytes)?;
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn verify_resource_pins(
    state: &RunState,
    snapshots: &[ResourceSnapshot],
) -> Result<(), EngineError> {
    if state.run_id.is_none() {
        return Ok(());
    }
    let current = snapshots
        .iter()
        .map(|snapshot| (snapshot.name.clone(), snapshot.sha256.clone()))
        .collect::<BTreeMap<_, _>>();
    for (name, expected) in &state.resource_pins {
        let actual = current.get(name).ok_or_else(|| {
            EngineError::Failed(format!(
                "pinned resource `{name}` is unavailable while resuming"
            ))
        })?;
        if actual != expected {
            return Err(EngineError::Failed(format!(
                "resource `{name}` changed while resuming: expected {expected}, got {actual}"
            )));
        }
    }
    Ok(())
}

#[derive(Debug)]
struct JournalReplay {
    steps: BTreeMap<String, ReplayedStep>,
    state: RunState,
}

#[derive(Debug, Clone)]
struct ReplayedStep {
    status: String,
    output: Option<Value>,
    files: Vec<FilePin>,
}

impl JournalReplay {
    fn from_state(state: RunState) -> Self {
        let steps = state
            .nodes
            .iter()
            .filter_map(|(path, outcome)| match outcome {
                NodeOutcome::Success { output, files } => Some((
                    path.as_str().to_string(),
                    ReplayedStep {
                        status: "success".into(),
                        output: output.clone(),
                        files: files.clone(),
                    },
                )),
                NodeOutcome::Skipped { .. } | NodeOutcome::Failed { .. } => None,
            })
            .collect();
        Self { steps, state }
    }

    fn verify_files(
        &self,
        workspace: &camino::Utf8Path,
        limits: &RuntimeLimits,
        accounting: &Arc<Mutex<CheckpointAccounting>>,
    ) -> Result<(), EngineError> {
        for (path, step) in &self.steps {
            step.verify_files(workspace, limits, accounting)
                .map_err(|message| {
                    EngineError::Failed(format!("cannot safely resume node `{path}`: {message}"))
                })?;
        }
        Ok(())
    }
}

impl ReplayedStep {
    fn verify_files(
        &self,
        workspace: &camino::Utf8Path,
        limits: &RuntimeLimits,
        accounting: &Arc<Mutex<CheckpointAccounting>>,
    ) -> Result<(), String> {
        for pin in &self.files {
            if pin.path.is_absolute() {
                return Err(format!(
                    "journal contains absolute output path `{}`",
                    pin.path
                ));
            }
            let candidate = workspace.join(&pin.path);
            let digest = hash_file(&candidate, limits.output_file_limit_bytes)
                .map_err(|error| format!("output `{}` is unavailable: {error}", pin.path))?;
            if digest.sha256 != pin.sha256 {
                return Err(format!(
                    "output `{}` digest changed: expected {}, got {}",
                    pin.path, pin.sha256, digest.sha256
                ));
            }
            let mut accounting = accounting
                .lock()
                .map_err(|_| "checkpoint accounting mutex was poisoned".to_owned())?;
            accounting
                .record(&pin.path, digest.bytes, limits)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ExecutionEnv<'a> {
    context: &'a RunContext,
    journal: &'a JournalWriter,
}

#[derive(Clone)]
struct BudgetTracker {
    max_total_steps: usize,
    executed_steps: Arc<AtomicUsize>,
}

impl BudgetTracker {
    fn new(max_total_steps: usize, executed_steps: usize) -> Self {
        Self {
            max_total_steps: max_total_steps.max(1),
            executed_steps: Arc::new(AtomicUsize::new(executed_steps)),
        }
    }

    fn consume(&self, node_id: &str) -> Result<(), StepError> {
        let result =
            self.executed_steps
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |executed| {
                    (executed < self.max_total_steps).then_some(executed.saturating_add(1))
                });
        if let Err(executed) = result {
            return Err(StepError::failed(
                node_id,
                format!(
                    "global step budget exceeded: {} >= {}",
                    executed, self.max_total_steps
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StepExecutor, StepTraits};
    use async_trait::async_trait;
    use qcg_contract::{
        AssetSpec, FailurePolicy, GeneratorMeta, Graph, InputSpec, JournalPolicy, Manifest,
        NodeDef, OnDeps, OnFail, OutputSpec, Permissions, SecretRef, StepType,
    };
    use qcg_types::Finding;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn budget_tracker_rejects_after_global_limit() {
        let budget = BudgetTracker::new(2, 0);
        assert!(budget.consume("first").is_ok());
        assert!(budget.consume("second").is_ok());
        let error = budget
            .consume("third")
            .expect_err("third step should exceed the run budget");
        assert!(error.to_string().contains("global step budget exceeded"));
    }

    #[test]
    fn checkpoint_accounting_enforces_output_bounds() {
        let limits = RuntimeLimits {
            output_file_limit_bytes: 4,
            output_total_limit_bytes: 5,
            output_artifact_limit: 2,
            ..RuntimeLimits::default()
        };
        let mut accounting = CheckpointAccounting::default();
        accounting
            .record(camino::Utf8Path::new("one.txt"), 4, &limits)
            .expect("first output should fit");
        let error = accounting
            .record(camino::Utf8Path::new("two.txt"), 2, &limits)
            .expect_err("total output limit should be enforced");
        assert!(error.to_string().contains("output bytes exceed 5"));
        let error = accounting
            .record(camino::Utf8Path::new("too-large"), 5, &limits)
            .expect_err("per-file output limit should be enforced");
        assert!(error.to_string().contains("exceeds 4 bytes"));
    }

    #[test]
    fn generator_cannot_claim_llm_provider_credential_environment() {
        let provider = qcg_llm::LlmRouter::parse_text(
            r#"
[[provider]]
id = "secure"
api = "chat_completions"
base_url = "https://example.test/v1"
api_key_env = "QCG_SECURE_API_KEY"
"#,
        )
        .expect("provider registry should parse");
        let mut manifest = manifest(vec![]);
        manifest.secrets.insert(
            "stolen_provider_key".into(),
            SecretRef {
                env: Some("QCG_SECURE_API_KEY".into()),
                file_env: None,
            },
        );
        let contract = Contract {
            root: Utf8PathBuf::from("generator"),
            graph: Graph::build(&manifest).expect("empty graph should build"),
            manifest,
            sha256: "test".into(),
        };

        let mut registry = StepRegistry::new();
        registry.reserve_secret_env_names(qcg_llm::LlmProvider::credential_env_names(&provider));
        let error = registry
            .validate_contract(&contract)
            .expect_err("provider credentials must remain unavailable to generator secrets");
        assert!(error.to_string().contains("reserved provider credential"));
        assert!(error.to_string().contains("QCG_SECURE_API_KEY"));
    }

    #[tokio::test]
    async fn scheduler_join_truth_table_and_skip_propagation() {
        struct Case {
            name: &'static str,
            left: &'static str,
            right: &'static str,
            on_deps: OnDeps,
            expect_join_runs: bool,
            expect_success: bool,
        }

        let cases = vec![
            Case {
                name: "all_success_runs",
                left: "test.pass",
                right: "test.pass",
                on_deps: OnDeps::AllSucceeded,
                expect_join_runs: true,
                expect_success: true,
            },
            Case {
                name: "all_failed_skips_join",
                left: "test.pass",
                right: "test.check_fail",
                on_deps: OnDeps::AllSucceeded,
                expect_join_runs: false,
                expect_success: false,
            },
            Case {
                name: "any_one_success_runs",
                left: "test.pass",
                right: "test.check_fail",
                on_deps: OnDeps::AnySucceeded,
                expect_join_runs: true,
                expect_success: false,
            },
            Case {
                name: "any_no_success_skips_join",
                left: "test.check_fail",
                right: "test.check_fail",
                on_deps: OnDeps::AnySucceeded,
                expect_join_runs: false,
                expect_success: false,
            },
        ];

        for case in cases {
            let manifest = manifest(vec![
                node("left", case.left),
                node("right", case.right),
                NodeDef {
                    id: "join".into(),
                    kind: StepType::from("test.pass"),
                    needs: vec!["left".into(), "right".into()],
                    on_deps: case.on_deps,
                    ..node("join", "test.pass")
                },
            ]);
            let run_dir = temp_run_dir(case.name);
            let result = run_manifest(manifest, run_dir.clone(), 1).await;
            assert_eq!(
                result.is_ok(),
                case.expect_success,
                "case {} should have expected final status",
                case.name
            );
            let events = journal_events(&run_dir);
            assert_eq!(
                has_event(&events, "step_finished", "join"),
                case.expect_join_runs,
                "case {} join execution mismatch",
                case.name
            );
            if !case.expect_join_runs {
                assert!(
                    has_event(&events, "step_skipped", "join"),
                    "case {} should skip join",
                    case.name
                );
            }
        }
    }

    #[tokio::test]
    async fn scheduler_when_skip_propagates_to_dependents() {
        let mut skipped = node("skipped", "test.pass");
        skipped.when = Some(qcg_contract::Expr("inputs.enabled".into()));
        let manifest = manifest(vec![
            skipped,
            NodeDef {
                id: "dependent".into(),
                kind: StepType::from("test.pass"),
                needs: vec!["skipped".into()],
                ..node("dependent", "test.pass")
            },
        ]);
        let run_dir = temp_run_dir("when-skip-propagates");
        let result = run_manifest(manifest, run_dir.clone(), 1).await;
        assert!(result.is_ok(), "skip propagation failed: {result:?}");
        let events = journal_events(&run_dir);
        assert!(has_event(&events, "step_skipped", "skipped"));
        assert!(has_event(&events, "step_skipped", "dependent"));
    }

    #[tokio::test]
    async fn scheduler_repair_cycle_marks_original_step_repaired() {
        let mut broken = node("broken", "test.check_fail");
        broken.on_fail = Some(OnFail::Repair {
            repair: "repair".into(),
            recheck: "recheck".into(),
            max_attempts: 1,
            on_exhausted: ExhaustedAction::Fail,
        });
        let manifest = manifest(vec![
            broken,
            node("repair", "test.pass"),
            node("recheck", "test.pass"),
        ]);
        let run_dir = temp_run_dir("repair-cycle-repaired");
        let result = run_manifest(manifest, run_dir.clone(), 1).await;
        assert!(result.is_ok(), "repair cycle failed: {result:?}");
        let events = journal_events(&run_dir);
        assert!(has_status(&events, "step_finished", "broken", "repaired"));
        assert!(has_event(&events, "repair_attempt_started", "broken"));
        assert!(has_status(
            &events,
            "repair_attempt_finished",
            "broken",
            "repaired"
        ));
    }

    #[tokio::test]
    async fn repair_exhaustion_returns_the_declared_typed_form() {
        let mut broken = node("broken", "test.check_fail");
        broken.on_fail = Some(OnFail::Repair {
            repair: "repair".into(),
            recheck: "recheck".into(),
            max_attempts: 1,
            on_exhausted: ExhaustedAction::AskUser {
                title: Some("Choose a recovery".into()),
                fields: vec![InputField {
                    id: "decision".into(),
                    label: Some("Decision".into()),
                    label_i18n: Default::default(),
                    description: Some("Select the recovery action".into()),
                    description_i18n: Default::default(),
                    placeholder: None,
                    placeholder_i18n: Default::default(),
                    kind: FieldType::Select,
                    required: true,
                    default: None,
                    pattern: None,
                    options: vec!["retry".into(), "stop".into()],
                    option_labels_i18n: Default::default(),
                    min_items: None,
                    item_type: None,
                    schema: None,
                    ui: Default::default(),
                }],
            },
        });
        let manifest = manifest(vec![
            broken,
            node("repair", "test.pass"),
            node("recheck", "test.check_fail"),
        ]);
        let run_dir = temp_run_dir("repair-exhausted-ask-user");
        let error = run_manifest(manifest, run_dir, 1)
            .await
            .expect_err("repair exhaustion should pause for the declared form");
        let EngineError::NeedsUser {
            question_id,
            question,
        } = error
        else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(question_id, "broken:repair_exhausted");
        assert_eq!(question.title, "Choose a recovery");
        assert_eq!(question.fields[0].id, "decision");
        assert_eq!(question.fields[0].options, ["retry", "stop"]);
    }

    #[tokio::test]
    async fn scheduler_parallel_wave_records_parallel_execution() {
        let mut manifest = manifest(vec![node("a", "test.pass"), node("b", "test.pass")]);
        manifest.parallel = vec!["a".into(), "b".into()];
        let run_dir = temp_run_dir("parallel-wave-records");
        let result = run_manifest(manifest, run_dir.clone(), 4).await;
        assert!(result.is_ok());
        let events = journal_events(&run_dir);
        let parallel_finished = events.iter().filter(|event| {
            event.get("t").and_then(Value::as_str) == Some("step_finished")
                && event.get("parallel").and_then(Value::as_bool) == Some(true)
        });
        assert_eq!(parallel_finished.count(), 2);
    }

    #[tokio::test]
    async fn parallel_wave_records_every_started_node_before_reporting_failure() {
        let mut manifest = manifest(vec![
            node("broken", "test.check_fail"),
            node("sibling", "test.pass"),
        ]);
        manifest.parallel = vec!["broken".into(), "sibling".into()];
        let run_dir = temp_run_dir("parallel-wave-failure-records");
        let result = run_manifest(manifest, run_dir.clone(), 4).await;
        assert!(result.is_err());
        let events = journal_events(&run_dir);
        assert!(has_event(&events, "step_finished", "broken"));
        assert!(has_event(&events, "step_finished", "sibling"));
    }

    #[tokio::test]
    async fn journal_replay_reuses_successful_steps_without_reexecuting_them() {
        let manifest = manifest(vec![node("a", "test.pass"), node("b", "test.pass")]);
        let run_dir = temp_run_dir("journal-replay");
        let _ = std::fs::remove_dir_all(&run_dir);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut registry = StepRegistry::new();
        registry.register(CountingPassStep {
            calls: Arc::clone(&counter),
        });
        for _ in 0..2 {
            let graph = Graph::build(&manifest).expect("test graph should build");
            let contract = Contract {
                root: run_dir.clone(),
                manifest: manifest.clone(),
                graph,
                sha256: "test".into(),
            };
            Engine::new(registry.clone())
                .run_with_id(
                    "journal-replay".into(),
                    run_dir.join("meta"),
                    contract,
                    BTreeMap::new(),
                    RunOptions {
                        output_dir: run_dir.join("workspace"),
                        json_events: false,
                        event_sender: None,
                        interactive: false,
                        answers: BTreeMap::new(),
                        confirmations: BTreeMap::new(),
                        max_total_steps: 100,
                        max_parallel_steps: 1,
                        llm_provider: None,
                        llm_seed_override: None,
                        cancellation: CancellationToken::new(),
                    },
                )
                .await
                .expect("both runs should finish");
        }
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        let events = journal_events(&run_dir);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.get("t").and_then(Value::as_str) == Some("step_replayed"))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn journal_replay_rejects_contract_drift_before_executing_steps() {
        let manifest = manifest(vec![node("a", "test.pass")]);
        let run_dir = temp_run_dir("journal-contract-drift");
        let _ = std::fs::remove_dir_all(&run_dir);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut registry = StepRegistry::new();
        registry.register(CountingPassStep {
            calls: Arc::clone(&counter),
        });
        let graph = Graph::build(&manifest).expect("test graph should build");
        let options = || RunOptions {
            output_dir: run_dir.join("workspace"),
            json_events: false,
            event_sender: None,
            interactive: false,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            max_total_steps: 100,
            max_parallel_steps: 1,
            llm_provider: None,
            llm_seed_override: None,
            cancellation: CancellationToken::new(),
        };
        Engine::new(registry.clone())
            .run_with_id(
                "journal-contract-drift".into(),
                run_dir.join("meta"),
                Contract {
                    root: run_dir.clone(),
                    manifest: manifest.clone(),
                    graph: graph.clone(),
                    sha256: "original".into(),
                },
                BTreeMap::new(),
                options(),
            )
            .await
            .expect("initial run should finish");
        let error = Engine::new(registry)
            .run_with_id(
                "journal-contract-drift".into(),
                run_dir.join("meta"),
                Contract {
                    root: run_dir.clone(),
                    manifest,
                    graph,
                    sha256: "changed".into(),
                },
                BTreeMap::new(),
                options(),
            )
            .await
            .expect_err("changed contract must not resume");
        assert!(
            error
                .to_string()
                .contains("contract changed while resuming")
        );
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn resume_rejects_resource_content_divergence() {
        let mut state = RunState {
            run_id: Some("resource-drift".into()),
            ..RunState::default()
        };
        state
            .resource_pins
            .insert("guide".into(), "original".into());
        let snapshots = vec![ResourceSnapshot {
            name: "guide".into(),
            resource_type: "file".into(),
            source: crate::ResourceSnapshotSource::Path {
                path: Utf8PathBuf::from("guide.md"),
            },
            snapshot: None,
            sha256: "changed".into(),
            bytes: 7,
            files: Vec::new(),
            cache: crate::ResourceCacheStatus::Local,
            pin_sha256: None,
            trust: "trusted".into(),
            llm_visible: true,
        }];
        let error = verify_resource_pins(&state, &snapshots)
            .expect_err("changed resource content must not resume");
        assert!(error.to_string().contains("resource `guide` changed"));
    }

    async fn run_manifest(
        manifest: Manifest,
        output_dir: Utf8PathBuf,
        max_parallel_steps: usize,
    ) -> Result<OutputManifest, EngineError> {
        let graph = Graph::build(&manifest).expect("test graph should build");
        let contract = Contract {
            root: output_dir.clone(),
            manifest,
            graph,
            sha256: "test".into(),
        };
        Engine::new(test_registry())
            .run_with_id(
                format!("test-{}", output_dir.file_name().unwrap_or("run")),
                output_dir.join("meta"),
                contract,
                BTreeMap::new(),
                RunOptions {
                    output_dir: output_dir.join("workspace"),
                    json_events: false,
                    event_sender: None,
                    interactive: false,
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    max_total_steps: 100,
                    max_parallel_steps,
                    llm_provider: None,
                    llm_seed_override: None,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
    }

    fn test_registry() -> StepRegistry {
        let mut registry = StepRegistry::new();
        registry.register(TestPassStep);
        registry.register(TestCheckFailStep);
        registry
    }

    struct TestPassStep;

    #[derive(Clone)]
    struct CountingPassStep {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StepExecutor for CountingPassStep {
        fn type_id(&self) -> &'static str {
            "test.pass"
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(StepOutcome::Success {
                output: Some(json!({ "node": node.id })),
                files: vec![],
            })
        }
    }

    #[async_trait]
    impl StepExecutor for TestPassStep {
        fn type_id(&self) -> &'static str {
            "test.pass"
        }

        fn traits(&self) -> StepTraits {
            StepTraits {
                parallel_safe: true,
                ..StepTraits::default()
            }
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            Ok(StepOutcome::Success {
                output: Some(json!({ "node": node.id })),
                files: vec![],
            })
        }
    }

    struct TestCheckFailStep;

    #[async_trait]
    impl StepExecutor for TestCheckFailStep {
        fn type_id(&self) -> &'static str {
            "test.check_fail"
        }

        fn traits(&self) -> StepTraits {
            StepTraits {
                parallel_safe: true,
                ..StepTraits::default()
            }
        }

        async fn execute(
            &self,
            _ctx: &mut StepContext<'_>,
            _node: &NodeDef,
        ) -> Result<StepOutcome, StepError> {
            Ok(StepOutcome::CheckFailed {
                findings: vec![Finding {
                    severity: qcg_types::Severity::Error,
                    message: "check failed".into(),
                    location: None,
                    raw_output: None,
                }],
                output: None,
                files: vec![],
            })
        }
    }

    fn manifest(flow: Vec<NodeDef>) -> Manifest {
        Manifest {
            generator: GeneratorMeta {
                id: "scheduler-test".into(),
                name: "Scheduler Test".into(),
                version: "0.1.0".into(),
                description: String::new(),
                authors: vec![],
                qcg_version: String::new(),
            },
            permissions: Permissions {
                fs_write: vec!["workspace".into()],
                ..Permissions::default()
            },
            llm: None,
            inputs: InputSpec::default(),
            resources: BTreeMap::new(),
            tools: BTreeMap::new(),
            secrets: BTreeMap::new(),
            runtime: Default::default(),
            budget: Default::default(),
            flow,
            parallel: Vec::new(),
            blocks: BTreeMap::new(),
            outputs: OutputSpec { extras: vec![] },
            failure: FailurePolicy::default(),
            journal: JournalPolicy::default(),
            assets: AssetSpec::default(),
        }
    }

    fn node(id: &str, kind: &str) -> NodeDef {
        NodeDef {
            id: id.into(),
            kind: StepType::from(kind),
            needs: vec![],
            when: None,
            on_deps: OnDeps::default(),
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            params: Default::default(),
        }
    }

    fn temp_run_dir(name: &str) -> Utf8PathBuf {
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-engine-{name}-{}", std::process::id())),
        )
        .expect("temporary path must be UTF-8");
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn journal_events(run_dir: &Utf8PathBuf) -> Vec<Value> {
        let source = std::fs::read_to_string(run_dir.join("meta/journal.jsonl"))
            .expect("journal should be readable");
        source
            .lines()
            .map(|line| serde_json::from_str(line).expect("journal line should be JSON"))
            .collect()
    }

    fn has_event(events: &[Value], kind: &str, node: &str) -> bool {
        events.iter().any(|event| {
            event.get("t").and_then(Value::as_str) == Some(kind)
                && event.get("node").and_then(Value::as_str) == Some(node)
        })
    }

    fn has_status(events: &[Value], kind: &str, node: &str, status: &str) -> bool {
        events.iter().any(|event| {
            event.get("t").and_then(Value::as_str) == Some(kind)
                && event.get("node").and_then(Value::as_str) == Some(node)
                && event.get("status").and_then(Value::as_str) == Some(status)
        })
    }
}
