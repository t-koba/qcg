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
    /// The workspace was rewound to an older revision instead of projecting
    /// the latest pinned revision. Distinct from corruption so operators can
    /// tell a rollback apart from damaged bookkeeping (E06).
    #[error("run workspace was rewound: {0}")]
    Rewound(String),
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
    /// The workspace was rewound to an older pinned revision instead of
    /// projecting the latest pin. Distinct from generic execution failure
    /// so operators can tell a rollback apart from corruption (E06). The
    /// durable `FailureCode` stays `ExecutionFailed` because that enum is
    /// FOREIGN (`qcg-types`); the kind preserves the distinction to
    /// `Progress::Failed`.
    Rewound,
}

#[derive(Debug)]
pub struct RunFailure {
    pub kind: RunFailureKind,
    /// Dedicated failure code so consumers can distinguish a hard elapsed
    /// deadline (never retried) from ordinary execution failures (E11).
    pub code: qcg_types::FailureCode,
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

/// Single canonical output-name resolution shared by the sequential loop,
/// the parallel wave, and repair/regenerate paths (E10). A node's declared
/// `output` alias wins; otherwise the node id names its own output.
pub(crate) fn output_name_for(node: &qcg_contract::NodeDef) -> &str {
    node.output.as_deref().unwrap_or(&node.id)
}

/// Single mapping from engine errors to durable failure codes so parallel
/// and sequential paths never diverge: run cancel, node timeout, elapsed
/// exceedance, budget exceedance, guard refusal, and ordinary execution
/// failures stay distinguishable (E11). `Rewound` maps to `ExecutionFailed`
/// for the FOREIGN code (no dedicated `Rewound` code exists there) while
/// `RunFailureKind::Rewound` preserves the distinct observable to `Progress`.
pub(crate) fn failure_code_for_error(error: &EngineError) -> qcg_types::FailureCode {
    match error {
        EngineError::Step(crate::StepError::ElapsedExceeded { .. }) => {
            qcg_types::FailureCode::ElapsedExceeded
        }
        EngineError::Step(crate::StepError::TimedOut { .. }) => qcg_types::FailureCode::TimedOut,
        EngineError::Step(crate::StepError::BudgetExceeded { .. }) => {
            qcg_types::FailureCode::BudgetExceeded
        }
        EngineError::Step(crate::StepError::Refused { .. }) => qcg_types::FailureCode::Refused,
        EngineError::Rewound(_) => qcg_types::FailureCode::ExecutionFailed,
        _ if error.is_canceled() => qcg_types::FailureCode::Canceled,
        _ => qcg_types::FailureCode::ExecutionFailed,
    }
}

/// Maps an engine error to its distinct `RunFailureKind` without collapsing
/// `Rewound` into generic execution failure (E06).
pub(crate) fn failure_kind_for_error(error: &EngineError) -> RunFailureKind {
    if error.is_canceled() {
        RunFailureKind::Canceled
    } else if matches!(error, EngineError::Rewound(_)) {
        RunFailureKind::Rewound
    } else {
        RunFailureKind::Execution
    }
}

/// Single shared helper for ALL step_finished exhaustion/failure/suspension
/// determinations (E06): every `step_finished` failure or suspension journal
/// event carries the failed attempt output and file pins under the unified
/// `failed_output` / `failed_files` keys, never `output` / `files` for failed
/// revisions. Suspensions without a failed attempt pass `None` and an empty
/// slice, which still journals the unified keys for notation uniformity. The
/// function touches only its payload value, so it is safe to call from
/// parallel tasks. `confirm_request` events are intentionally out of scope:
/// their FOREIGN schema (`qcg_api::ConfirmRequestEventData`) allows only
/// `confirm` plus `parallel`, so failed-evidence keys would be rejected at
/// journal validation there.
pub(crate) fn with_failed_evidence(
    mut payload: serde_json::Value,
    failed_output: &Option<serde_json::Value>,
    failed_files: &[crate::FilePin],
) -> Result<serde_json::Value, EngineError> {
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "failed_output".into(),
            failed_output.clone().unwrap_or(serde_json::Value::Null),
        );
        // Fail closed (E11/G-1): a serialization failure must surface instead
        // of journaling an empty array that is indistinguishable from
        // genuinely-no-files evidence.
        let failed_files = serde_json::to_value(failed_files).map_err(|error| {
            EngineError::Failed(format!(
                "failed_files evidence is not serializable: {error}"
            ))
        })?;
        object.insert("failed_files".into(), failed_files);
    }
    Ok(payload)
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
    /// Resolved `run_ref` resources by resource name. An unresolved
    /// referenced resource fails the run instead of silently skipping it.
    pub run_refs: BTreeMap<String, RunRefMaterial>,
    pub cancellation: CancellationToken,
    /// Service shutdown token for shutdown-aware cancel settlement (E05).
    /// When already cancelled at cancel-settle time, the engine journals
    /// `run_interrupted` (not `run_canceled`) so shutdown converges to
    /// Interrupted. `None` means no shutdown state (direct runs), which
    /// keeps settling as Canceled.
    pub shutdown: Option<CancellationToken>,
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
    /// Monotonic deadline derived from the run's `max_elapsed_seconds`
    /// budget (a `tokio::time::Instant`, never wall time). Nodes stop with
    /// a distinct elapsed error instead of running past the limit and only
    /// noticing at the next checkpoint (E11).
    pub elapsed_deadline: Option<tokio::time::Instant>,
    /// Server-side run status source for `await` nodes. Absent in
    /// single-process direct runs, where cross-run waiting is unsupported.
    pub snapshot_source: Option<Arc<dyn RunSnapshotSource>>,
    /// Resolved run references for this execution; immutable for the run.
    pub run_refs: Arc<BTreeMap<String, RunRefMaterial>>,
    pub(crate) replayed_steps: Arc<BTreeMap<String, ReplayedStep>>,
    pub(crate) checkpoint_accounting: Arc<Mutex<CheckpointAccounting>>,
}

/// Provides other runs' states to `await` nodes without coupling the
/// engine to the service layer.
#[async_trait::async_trait]
pub trait RunSnapshotSource: Send + Sync {
    async fn run_status(&self, run_id: &str) -> Option<RunStatus>;
}

/// Resolved material for one `run_ref` resource: the source identity, the
/// declared artifact path, the verified revision, and the bytes to copy
/// into this run's workspace. Resolution is policy; the engine treats this
/// as an immutable, hash-pinned input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRefMaterial {
    pub source_run_id: String,
    pub artifact: String,
    pub sha256: String,
    pub bytes: Vec<u8>,
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
    // No wire-compat default (Q1): manifests that predate `parallel` fail
    // closed at contract validation and here instead of silently running
    // sequentially.
    pub(crate) parallel: usize,
}

/// Normalizes file inputs to canonical `FileValue` form at admission so
/// the journal, memory records, and engine execution observe identical
/// inputs on every path.
pub fn canonical_file_inputs(
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

/// Verified workspace bytes handed from replay verification to input
/// materialization (E06 handoff): `verify_files` already opened each pinned
/// workspace projection once (`O_NOFOLLOW` → `fstat` → read FROM FD → hash
/// THOSE bytes). The caller must not re-hash the same source; it threads
/// these digests through `preverified` and compares without re-reading.
/// Absent entries mean the path was not verified (untracked inputs) and the
/// caller falls back to a single-handle read below.
pub(crate) type PreverifiedWorkspace = BTreeMap<String, String>;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn materialize_file_inputs_with_preverified(
    contract: &Contract,
    inputs: &BTreeMap<String, Value>,
    fs: &crate::FsGateway,
    resume: bool,
    pinned: &BTreeMap<String, String>,
    historical: &BTreeMap<String, std::collections::BTreeSet<String>>,
    preverified: &PreverifiedWorkspace,
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
        // Field ids participate in workspace paths and must satisfy the same
        // relative-path invariant as file names (A04).
        if !qcg_policy::is_safe_relative_path(&field.id) || field.id.contains('/') {
            return Err(EngineError::Failed(format!(
                "invalid file input `{}`: field id is not a safe relative path",
                field.id
            )));
        }
        let file = FileValue::from_value_optional_limit(
            value,
            contract.manifest.runtime.file_input_limit_bytes,
        )
        .map_err(|error| {
            EngineError::Failed(format!("invalid file input `{}`: {error}", field.id))
        })?;
        let relative = format!("files/{}/{}", field.id, file.name);
        // Route internal input placement through the same workspace I/O
        // isolation as ordinary writes: parent/terminal symlink checks and
        // atomic replace (A04). Privileged placement shares the isolation
        // invariant without sharing the general fs_write permission.
        let target = fs.resolve_internal_write(&relative)?;
        // On resume, an existing input file is the current workspace state:
        // a completed step may have legitimately updated it, so restoring
        // the original upload bytes would roll the checkpoint back (E06).
        // An untracked path (no successful step pinned it) must still hold
        // the admitted bytes, so tampering outside the journal is refused
        // instead of silently executing on forged input. Single-handle
        // semantics (E06): one `O_NOFOLLOW` open → `fstat` → read FROM THE
        // FD → hash THOSE bytes; never existence-check then re-open by
        // path. The replay verifier already hashed pinned projections; when
        // `preverified` names this path its digest is reused without a
        // second open (handoff documented on `PreverifiedWorkspace`).
        // An input path is either a regular file, absent, or corrupt:
        // a directory or symlink where upload bytes should be is refused
        // instead of being treated as present (E06).
        // Single open classifies absence vs presence without a second
        // syscall: `NotFound` reads as absent, symlink/non-regular reads
        // as corrupt, other IO errors propagate fail-closed (E06).
        enum InputPresence {
            Absent,
            Present(Vec<u8>),
        }
        fn read_input_once(
            target: &camino::Utf8Path,
            relative: &str,
            limit: Option<usize>,
            field: &str,
        ) -> Result<InputPresence, EngineError> {
            match qcg_fs::read_nofollow_bounded(target, limit) {
                Ok(bytes) => Ok(InputPresence::Present(bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Ok(InputPresence::Absent)
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::PermissionDenied
                    ) || error.raw_os_error() == Some(libc::ELOOP) =>
                {
                    Err(EngineError::Failed(format!(
                        "cannot safely resume: file input `{field}` is not a regular file: {error} (path `{relative}`)"
                    )))
                }
                Err(error) => Err(EngineError::Failed(format!(
                    "cannot safely resume: file input `{field}` is unreadable: {error}"
                ))),
            }
        }
        if resume {
            // Handoff: a replay-verified digest avoids a second open+hash
            // of the same source (E06). The digest was hashed FROM THE FD
            // during verification; comparing it here uses THOSE bytes.
            if let Some(verified) = preverified.get(relative.as_str()) {
                if let Some(expected) = pinned.get(relative.as_str()) {
                    if verified != expected {
                        return Err(EngineError::Rewound(format!(
                            "cannot safely resume: file input `{}` does not match its latest pinned revision {expected} (got {verified})",
                            field.id
                        )));
                    }
                    materialized.insert(field.id.clone(), Value::String(relative));
                    continue;
                }
                if let Some(digests) = historical.get(relative.as_str()) {
                    if !digests.contains(verified) {
                        return Err(EngineError::Failed(format!(
                            "cannot safely resume: file input `{}` does not match any journaled revision",
                            field.id
                        )));
                    }
                    materialized.insert(field.id.clone(), Value::String(relative));
                    continue;
                }
                // Verified but unpinned (for example a workspace projection
                // verified as absent-absent): fall through to the
                // single-handle read for untracked comparison below.
                // A verified digest for an untracked path still proves
                // nothing about admission bytes, so it must not short-circuit.
            }
            match read_input_once(
                &target,
                &relative,
                contract.manifest.runtime.file_input_limit_bytes,
                &field.id,
            )? {
                InputPresence::Absent => {
                    // A journaled path that disappeared cannot be
                    // re-materialized from the admitted bytes: that would
                    // roll the checkpoint back instead of failing closed
                    // (E06). Both latest pins and historical pins
                    // (including failed-step-only updates) count as
                    // journaled.
                    if pinned.contains_key(relative.as_str())
                        || historical.contains_key(relative.as_str())
                    {
                        return Err(EngineError::Failed(format!(
                            "cannot safely resume: pinned file input `{}` is missing from the workspace",
                            field.id
                        )));
                    }
                    // Absent and unpinned: fall through to fresh placement
                    // below.
                }
                InputPresence::Present(actual) => {
                    // Journal ownership is key presence plus bytes comparison
                    // against the pinned digest (E06): a latest-owned path must
                    // hash to its latest pin, while a historical-only path
                    // (for example a failed-step-only update) must hash to
                    // one of its historical digests. Key presence alone would
                    // accept tampered bytes that happen to share a journaled
                    // path; bytes comparison fails closed instead. The hash
                    // below is over THOSE bytes just read, never a re-open.
                    use sha2::{Digest as _, Sha256};
                    if let Some(expected) = pinned.get(relative.as_str()) {
                        let actual_digest = hex::encode(Sha256::digest(&actual));
                        if &actual_digest != expected {
                            return Err(EngineError::Rewound(format!(
                                "cannot safely resume: file input `{}` does not match its latest pinned revision {expected} (got {actual_digest})",
                                field.id
                            )));
                        }
                    } else if let Some(digests) = historical.get(relative.as_str()) {
                        let actual_digest = hex::encode(Sha256::digest(&actual));
                        if !digests.contains(&actual_digest) {
                            return Err(EngineError::Failed(format!(
                                "cannot safely resume: file input `{}` does not match any journaled revision",
                                field.id
                            )));
                        }
                    } else {
                        let expected = file
                            .decode_optional_limit(contract.manifest.runtime.file_input_limit_bytes)
                            .map_err(|error| {
                                EngineError::Failed(format!(
                                    "invalid file input `{}`: {error}",
                                    field.id
                                ))
                            })?;
                        if actual != expected {
                            return Err(EngineError::Failed(format!(
                                "cannot safely resume: file input `{}` was modified without a journaled pin",
                                field.id
                            )));
                        }
                    }
                    materialized.insert(field.id.clone(), Value::String(relative));
                    continue;
                }
            }
        }
        // Absent-and-unpinned falls through to fresh placement below; the
        // journaled-missing case already returned inside the `Absent` arm
        // above, so no second pinned check is needed here (E06).
        let bytes = file
            .decode_optional_limit(contract.manifest.runtime.file_input_limit_bytes)
            .map_err(|error| {
                EngineError::Failed(format!("invalid file input `{}`: {error}", field.id))
            })?;
        fs.write_file_atomic(&target, &bytes).await?;
        materialized.insert(field.id.clone(), Value::String(relative));
    }
    Ok(materialized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_codes_distinguish_cancel_timeout_elapsed_and_execution() {
        // E11: run cancel, node timeout, elapsed exceedance, and ordinary
        // failures must never collapse into one code.
        assert_eq!(
            failure_code_for_error(&EngineError::Canceled),
            qcg_types::FailureCode::Canceled
        );
        assert_eq!(
            failure_code_for_error(&EngineError::Step(crate::StepError::Cancelled)),
            qcg_types::FailureCode::Canceled
        );
        assert_eq!(
            failure_code_for_error(&EngineError::Step(crate::StepError::TimedOut {
                node: "n".into(),
                timeout_secs: 1,
            })),
            qcg_types::FailureCode::TimedOut
        );
        assert_eq!(
            failure_code_for_error(&EngineError::Step(crate::StepError::ElapsedExceeded {
                node: "n".into(),
                limit_secs: 1,
            })),
            qcg_types::FailureCode::ElapsedExceeded
        );
        assert_eq!(
            failure_code_for_error(&EngineError::Step(crate::StepError::failed("n", "boom"))),
            qcg_types::FailureCode::ExecutionFailed
        );
        assert_eq!(
            failure_code_for_error(&EngineError::Step(crate::StepError::BudgetExceeded {
                resource: "tokens",
                used: 10,
                limit: 5,
            })),
            qcg_types::FailureCode::BudgetExceeded
        );
        // A guard refusal carries its own durable code with the refusal
        // message preserved alongside it.
        assert_eq!(
            failure_code_for_error(&EngineError::Step(crate::StepError::Refused {
                node: "n".into(),
                message: "indeterminate outcome without opt-in".into(),
            })),
            qcg_types::FailureCode::Refused
        );
    }

    #[test]
    fn rewound_stays_distinct_from_generic_execution_failure() {
        // E06: a rewound workspace (older revision restored) must not
        // collapse into generic `Failed`. The error variant is distinct and
        // the `Progress` kind preserves it even though the FOREIGN
        // `FailureCode` stays `ExecutionFailed` (no dedicated code there).
        let rewound = EngineError::Rewound("workspace was rewound".into());
        let failed = EngineError::Failed("boom".into());
        assert!(matches!(rewound, EngineError::Rewound(_)));
        assert_eq!(
            failure_kind_for_error(&rewound),
            RunFailureKind::Rewound,
            "rewound must map to its own kind"
        );
        assert_eq!(
            failure_kind_for_error(&failed),
            RunFailureKind::Execution,
            "generic failure must stay execution"
        );
        assert_eq!(
            failure_kind_for_error(&EngineError::Canceled),
            RunFailureKind::Canceled
        );
        assert_ne!(
            failure_kind_for_error(&rewound),
            failure_kind_for_error(&failed),
            "rewound and generic failure must be observably distinct"
        );
    }
}
