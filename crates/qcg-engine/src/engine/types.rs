use crate::{
    CmdGateway, FsGateway, HttpGateway, SecretStore, StepError, StepRegistry, TemplateService,
};
use camino::Utf8PathBuf;
use qcg_api::RunStatus;
use qcg_api::{ConfirmSpec, FormSpec, RunEvent};
use qcg_contract::Contract;
use qcg_contract::FieldType;
use qcg_contract::NodeDef;
use qcg_types::{FileValue, OutputManifest};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use super::checkpoint::CheckpointAccounting;
use super::replay::ReplayedStep;
use qcg_policy::DEFAULT_MAX_TOTAL_STEPS;

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
pub(crate) struct NamedNodeTarget<'a> {
    pub(crate) owner_path: &'a str,
    pub(crate) node_id: &'a str,
    pub(crate) attempt: u32,
}

pub(crate) struct ForeachIteration<'a> {
    pub(crate) node: &'a NodeDef,
    pub(crate) block: &'a [NodeDef],
    pub(crate) index: usize,
    pub(crate) item: Value,
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
        DEFAULT_MAX_TOTAL_STEPS
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
    /// Server-side run status source for `await` nodes. Absent in
    /// single-process direct runs, where cross-run waiting is unsupported.
    pub snapshot_source: Option<Arc<dyn RunSnapshotSource>>,
    pub(crate) replayed_steps: Arc<BTreeMap<String, ReplayedStep>>,
    pub(crate) checkpoint_accounting: Arc<Mutex<CheckpointAccounting>>,
}

/// Provides other runs' states to `await` nodes without coupling the
/// engine to the service layer.
#[async_trait::async_trait]
pub trait RunSnapshotSource: Send + Sync {
    async fn run_status(&self, run_id: &str) -> Option<RunStatus>;
}

#[derive(Clone)]
pub struct Engine {
    pub(crate) registry: StepRegistry,
    pub(crate) snapshot_source: Option<Arc<dyn RunSnapshotSource>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ForeachControlParams {
    pub(crate) items: String,
    pub(crate) subflow: String,
    pub(crate) max_iterations: usize,
    #[serde(default = "default_foreach_parallelism")]
    pub(crate) parallel: usize,
}

fn default_foreach_parallelism() -> usize {
    1
}

pub(crate) fn canonical_file_inputs(
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
        let file = FileValue::from_value_optional_limit(
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

pub(crate) fn materialize_file_inputs(
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
        let file = FileValue::from_value_optional_limit(
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
            file.decode_optional_limit(contract.manifest.runtime.file_input_limit_bytes)
                .map_err(|error| {
                    EngineError::Failed(format!("invalid file input `{}`: {error}", field.id))
                })?,
        )?;
        materialized.insert(field.id.clone(), Value::String(relative));
    }
    Ok(materialized)
}
