use crate::artifacts::VerifiedArtifact;
use crate::queue::PriorityPermits;
use crate::summaries::fold_run_state;
use camino::Utf8PathBuf;
use qcg_api::{ApiError, RunSnapshot, RunStatus};
use qcg_api::{ConfirmSpec, FormSpec, RunEvent};
use qcg_contract::Contract;
use qcg_types::OutputManifest;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::{Arc, Mutex};
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStoreMode {
    Exclusive,
    SharedFilesystem,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
}

impl From<ServiceError> for ApiError {
    fn from(error: ServiceError) -> Self {
        Self::internal(error.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct LocalQcgService {
    pub(crate) inner: Arc<LocalQcgServiceInner>,
}

#[derive(Debug, Clone)]
pub struct DirectRun {
    pub generator_path: Utf8PathBuf,
    pub inputs: BTreeMap<String, Value>,
    pub output_dir: Utf8PathBuf,
    pub json_events: bool,
    pub interactive: bool,
    pub answers: BTreeMap<String, Value>,
    pub confirmations: BTreeMap<String, bool>,
    pub llm_seed_override: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct DirectRunEvents {
    pub manifest: OutputManifest,
    pub events: Vec<RunEvent>,
}

#[derive(Debug)]
pub(crate) struct LocalQcgServiceInner {
    /// All generator roots in precedence order; the first root containing an
    /// id wins and is also the writable install target. Later roots (for
    /// example the bundled `share/qcg/generators`) are read-only catalogs.
    pub(crate) generator_roots: Vec<Utf8PathBuf>,
    pub(crate) runs_dir: Utf8PathBuf,
    pub(crate) runs: RwLock<BTreeMap<String, RunRecord>>,
    pub(crate) llm_runtime: Arc<qcg_llm::LlmRuntime>,
    pub(crate) execution_permits: Arc<PriorityPermits>,
    pub(crate) max_active_runs: usize,
    pub(crate) max_tracked_runs: usize,
    pub(crate) run_store_mode: RunStoreMode,
    pub(crate) _runs_lock: Option<File>,
    pub(crate) queue_notify: Arc<tokio::sync::Notify>,
    /// Stable identity of this service process for shared-store ownership.
    pub(crate) owner_id: String,
}
#[derive(Debug, Clone)]
pub(crate) struct RunRecord {
    pub(crate) contract: Contract,
    pub(crate) contract_sha256: String,
    pub(crate) inputs: BTreeMap<String, Value>,
    pub(crate) answers: BTreeMap<String, Value>,
    pub(crate) confirmations: BTreeMap<String, bool>,
    pub(crate) priority: i32,
    pub(crate) parent_run_id: Option<String>,
    /// Set while a preemption is in flight; the engine task must skip its
    /// terminal settlement so the requeued run can resume.
    pub(crate) preempted: bool,
    pub(crate) state: RunStatus,
    pub(crate) run_dir: Utf8PathBuf,
    pub(crate) artifacts: Option<OutputManifest>,
    pub(crate) question: Option<FormSpec>,
    pub(crate) confirm: Option<ConfirmSpec>,
    pub(crate) events: broadcast::Sender<RunEvent>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) task: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(crate) queued_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Owning service process for shared-store coordination. Empty means
    /// unowned (recovered); the first process to spawn claims ownership.
    pub(crate) owner_id: String,
}

/// Owned inputs for a self-contained run bundle export.
#[derive(Debug, Clone)]
pub struct RunBundleParts {
    pub snapshot: RunSnapshot,
    pub inputs: BTreeMap<String, Value>,
    pub journal: Utf8PathBuf,
    pub outputs: Option<OutputManifest>,
    pub verified: Vec<VerifiedArtifact>,
}

/// Exposes sibling run states to engine `await` nodes: live memory first,
// then the durable journal fold.
#[derive(Debug, Clone)]
pub(crate) struct ServiceSnapshotSource {
    pub(crate) service: LocalQcgService,
}

#[async_trait::async_trait]
impl qcg_engine::RunSnapshotSource for ServiceSnapshotSource {
    async fn run_status(&self, run_id: &str) -> Option<RunStatus> {
        if let Some(record) = self.service.inner.runs.read().await.get(run_id) {
            return Some(record.state);
        }
        let run_dir = self.service.run_dir_for(run_id).await.ok()?;
        fold_run_state(&run_dir)
            .ok()?
            .terminal
            .map(|terminal| match terminal {
                qcg_engine::TerminalState::Succeeded => RunStatus::Succeeded,
                qcg_engine::TerminalState::Failed => RunStatus::Failed,
                qcg_engine::TerminalState::Canceled => RunStatus::Canceled,
                qcg_engine::TerminalState::Interrupted => RunStatus::Interrupted,
            })
    }
}

/// State update computed outside the run-map lock, then applied under it.
pub(crate) struct FinishTransition {
    pub(crate) state: Option<RunStatus>,
    pub(crate) artifacts: Option<OutputManifest>,
    pub(crate) question: Option<FormSpec>,
    pub(crate) confirm: Option<ConfirmSpec>,
    pub(crate) writes: Vec<(&'static str, Value)>,
}

impl FinishTransition {
    pub(crate) fn finished(state: RunStatus) -> Self {
        Self {
            state: Some(state),
            artifacts: None,
            question: None,
            confirm: None,
            writes: Vec::new(),
        }
    }
}
