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

/// Deployment policy applied atomically at service construction (E04).
/// Bundled so constructors stay within the argument limit and policy can
/// never be half-applied by a racing setter during recovery.
#[derive(Debug, Clone, Copy)]
pub struct ServiceDeploymentPolicy {
    pub max_total_steps: Option<usize>,
    pub preemption_enabled: bool,
    /// Deployment audit floor. It can only raise observation persistence:
    /// a `standard` floor overrides any generator-level reduction.
    pub audit_floor: qcg_policy::AuditFloor,
    /// Terminal runs kept by the automatic retention sweep.
    pub gc_keep: usize,
    /// Additional failed runs kept for post-mortems.
    pub gc_keep_failed: usize,
    /// Seconds between automatic retention sweeps.
    pub gc_interval_secs: u64,
    /// Deployment cap on parallel wave scheduling. `None` uses the CPU
    /// count.
    pub max_parallel_steps: Option<usize>,
    /// Capacity of the per-run live event broadcast channel.
    pub live_event_channel_capacity: usize,
    /// Shared-mode journal follow cadence, in milliseconds.
    pub journal_poll_interval_millis: u64,
    /// Directory scan cap for run/generator roots.
    pub max_directory_scan_entries: usize,
    /// Shared-store abandoned-run rescan cadence, in seconds.
    pub shared_store_rescan_secs: u64,
    /// Queued-run resumer cadence, in seconds.
    pub queued_resumer_secs: u64,
}

impl Default for ServiceDeploymentPolicy {
    fn default() -> Self {
        Self {
            max_total_steps: None,
            preemption_enabled: true,
            audit_floor: qcg_policy::AuditFloor::Minimal,
            gc_keep: qcg_policy::DEFAULT_GC_KEEP,
            gc_keep_failed: qcg_policy::DEFAULT_GC_KEEP_FAILED,
            gc_interval_secs: qcg_policy::DEFAULT_GC_INTERVAL_SECS,
            max_parallel_steps: None,
            live_event_channel_capacity: qcg_policy::LIVE_EVENT_CHANNEL_CAPACITY,
            journal_poll_interval_millis: qcg_policy::JOURNAL_POLL_INTERVAL_MILLIS,
            max_directory_scan_entries: qcg_policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
            shared_store_rescan_secs: 5,
            queued_resumer_secs: 5,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("{0}")]
    Invalid(String),
    /// A journal check-and-append precondition lost a race. Carried typed
    /// (never string-matched) so rejection classifiers cannot mistake other
    /// journal failures for a lost race.
    #[error("journal precondition failed: {0}")]
    PreconditionFailed(String),
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
    /// Deployment policy frozen at construction (E04): `Copy` by design,
    /// stored by value and read without locking. Post-construction setters
    /// were removed so policy can never race recovery execution.
    pub(crate) deployment_policy: ServiceDeploymentPolicy,
    /// Set by the embedding server when graceful shutdown starts. New
    /// admissions (start, fork, answer, confirm) are refused from then on,
    /// so internal callers cannot race the shutdown snapshot (E05).
    pub(crate) shutdown: CancellationToken,
    /// Shared journal pollers, one per run with active subscribers.
    /// Subscribers share the underlying poll task instead of each spawning
    /// their own 250 ms loop, so N subscribers cost one task (E12).
    pub(crate) journal_pollers:
        std::sync::Mutex<BTreeMap<String, tokio::sync::broadcast::Sender<qcg_api::RunEvent>>>,
}
/// A task slot owns execution only while its handle is present AND
/// unfinished. A finished handle (completed, aborted, or panicked) no
/// longer owns anything. Every liveness check must use this instead of a
/// bare `is_some`: a parked finished handle otherwise reads as a live
/// owner, starving refresh merges, resumer adoption, and early answers
/// while `delete_run` (which already checks `is_finished`) disagrees
/// (E12).
pub(crate) fn task_slot_is_live(slot: &Mutex<Option<JoinHandle<()>>>) -> bool {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .is_some_and(|handle| !handle.is_finished())
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
    // Plain `std` mutex by design: every critical section only takes or
    // inspects the handle without awaiting while held, so it never blocks
    // the executor; a Tokio mutex would add no safety here (E03).
    pub(crate) task: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub(crate) queued_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Owning service process for shared-store coordination. Empty means
    /// unowned (recovered); the first process to spawn claims ownership.
    pub(crate) owner_id: String,
    /// Direct-execution placeholder registered only for unified queue
    /// ordering. The inline direct driver owns execution, so the queue
    /// resumres must never adopt it into a second engine task.
    pub(crate) ephemeral: bool,
}

/// Open journal handle for constant-memory HTTP delivery. The configured
/// total bound (when any) was already enforced against the file size before
/// the first byte is served.
#[derive(Debug)]
pub struct JournalStream {
    pub file: tokio::fs::File,
    pub len: u64,
    pub limit: Option<usize>,
    /// Observation stream opened alongside the durable journal when it
    /// exists. The HTTP journal view merges both by seq so clients keep
    /// seeing one ordered record stream (ADR 0001).
    pub audit: Option<tokio::fs::File>,
    pub audit_len: u64,
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
        // Unknown run status blocks awaiters instead of proceeding: a
        // failed fold must never read as terminated. An unsettled run reads
        // as queued so awaiters keep waiting instead of proceeding on a
        // possibly live run misreported as dead.
        Some(
            fold_run_state(&run_dir)
                .ok()?
                .terminal
                .map(|terminal| match terminal {
                    qcg_engine::TerminalState::Succeeded => RunStatus::Succeeded,
                    qcg_engine::TerminalState::Failed => RunStatus::Failed,
                    qcg_engine::TerminalState::Canceled => RunStatus::Canceled,
                    qcg_engine::TerminalState::Interrupted => RunStatus::Interrupted,
                })
                .unwrap_or(RunStatus::Queued),
        )
    }
}

/// Effective execution policy resolved once at admission from the contract
/// request and the deployment ceiling (3.4). Values are never silently
/// truncated: when the ceiling caps the contract, the origin records it so
/// CLI plan output, API snapshots, and the journal agree on the enforced
/// value instead of each layer reinterpreting environment or constants.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedExecutionPolicy {
    pub(crate) max_total_steps: usize,
    pub(crate) max_parallel_steps: usize,
    pub(crate) origin: String,
}

impl ResolvedExecutionPolicy {
    /// Resolves the contract request against the deployment ceiling. The
    /// ceiling comes from explicit host configuration (`None` means the
    /// engine default), never from environment reads inside the service.
    pub(crate) fn resolve(
        contract_max_steps: usize,
        service_ceiling: Option<usize>,
        service_max_parallel: Option<usize>,
    ) -> Self {
        let ceiling =
            service_ceiling.unwrap_or_else(qcg_engine::RunOptions::default_max_total_steps);
        let effective = ceiling.min(contract_max_steps);
        let origin = if effective < contract_max_steps {
            format!("service-ceiling:{ceiling} caps contract:{contract_max_steps}")
        } else {
            format!("contract:{contract_max_steps}")
        };
        if effective < contract_max_steps {
            tracing::warn!(
                ceiling,
                contract_max_steps,
                "deployment ceiling caps the contract step budget; effective value is reported, not silently applied"
            );
        }
        Self {
            max_total_steps: effective,
            max_parallel_steps: service_max_parallel
                .unwrap_or_else(qcg_engine::RunOptions::default_max_parallel_steps)
                .max(1),
            origin,
        }
    }

    /// Reuses the admission instant recorded in the journal so a ceiling
    /// change mid-run cannot diverge admission from execution (E04).
    /// A journal without the recorded field is an explicit error, never a
    /// silent fresh resolution: every admission path records the field, so
    /// its absence means a legacy or corrupt journal (E04).
    pub(crate) fn for_execution(
        journal_values: &[serde_json::Value],
        service_max_parallel: Option<usize>,
    ) -> Result<Self, String> {
        // Journal values use `t` for the event kind with top-level fields
        // (see summaries readers and writer.rs); `kind`/`data` never occur
        // here, so matching them would always miss (E04).
        // Fork-aware (E03): a fork checkpoint copy carries the source's
        // `run_queued` ahead of a `run_forked` marker, then the fork's own
        // admission `run_queued` follows. Resolving against the first
        // `run_queued` would apply the source's ceiling to the fork, so the
        // fork's own admission (the last `run_queued` sequenced after every
        // `run_forked`) wins. Start journals carry a single `run_queued`,
        // where first and last coincide. Position-based (not seq-based) so
        // in-memory synthesized admissions without seq still resolve.
        let last_fork_pos = journal_values.iter().rposition(|value| {
            value.get("t").and_then(serde_json::Value::as_str) == Some("run_forked")
        });
        let mut matched: Option<usize> = None;
        for (index, value) in journal_values.iter().enumerate() {
            if last_fork_pos.is_some_and(|fork| index <= fork) {
                continue;
            }
            let is_queued = value
                .get("t")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| kind == "run_queued");
            if !is_queued {
                continue;
            }
            if value
                .get("effective_max_total_steps")
                .and_then(serde_json::Value::as_u64)
                .and_then(|steps| usize::try_from(steps).ok())
                .is_some()
            {
                matched = Some(index);
            }
        }
        if let Some(index) = matched
            && let Some(steps) = journal_values[index]
                .get("effective_max_total_steps")
                .and_then(serde_json::Value::as_u64)
                .and_then(|steps| usize::try_from(steps).ok())
        {
            return Ok(Self {
                max_total_steps: steps,
                max_parallel_steps: service_max_parallel
                    .unwrap_or_else(qcg_engine::RunOptions::default_max_parallel_steps)
                    .max(1),
                origin: "journaled-admission".to_string(),
            });
        }
        Err("journaled run_queued event lacks effective_max_total_steps; refusing to run under a re-resolved ceiling".to_string())
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

#[cfg(test)]
mod policy_tests {
    use super::ResolvedExecutionPolicy;

    #[test]
    fn service_ceiling_caps_contract_with_origin() {
        let policy = ResolvedExecutionPolicy::resolve(100, Some(40), None);
        assert_eq!(policy.max_total_steps, 40);
        assert_eq!(policy.origin, "service-ceiling:40 caps contract:100");
        let unlimited = ResolvedExecutionPolicy::resolve(100, None, None);
        assert!(unlimited.max_total_steps >= 100);
        assert_eq!(unlimited.origin, "contract:100");
    }
}

#[cfg(test)]
mod send_probe_tests {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    #[test]
    fn probe_send_bounds() {
        assert_send::<crate::LocalQcgService>();
        assert_sync::<crate::LocalQcgService>();
        assert_send::<crate::lifecycle::SpawnRun>();
        assert_sync::<crate::lifecycle::SpawnRun>();
        assert_send::<crate::types::RunRecord>();
        assert_sync::<crate::types::RunRecord>();
        assert_send::<qcg_contract::Contract>();
        assert_sync::<qcg_contract::Contract>();
        assert_send::<qcg_contract::Manifest>();
        assert_sync::<qcg_contract::Manifest>();
        assert_send::<qcg_contract::Graph>();
        assert_send::<qcg_api::RunEvent>();
        assert_sync::<qcg_api::RunEvent>();
        assert_send::<qcg_api::FormSpec>();
        assert_send::<qcg_api::ConfirmSpec>();
        assert_send::<qcg_types::OutputManifest>();
        assert_send::<std::fs::File>();
        assert_send::<tokio::task::JoinHandle<()>>();
        assert_send::<crate::types::ServiceError>();
        assert_sync::<crate::types::ServiceError>();
        assert_send::<std::fs::File>();
        assert_send::<tokio::sync::broadcast::Sender<qcg_api::RunEvent>>();
        assert_send::<qcg_api::ToolCallEventData>();
    }
}
