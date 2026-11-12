use crate::summaries::{read_events_from_meta, run_meta_dir, run_workspace_dir};
use crate::types::{RunRecord, ServiceError};
use api::ForkStatePatch;
use camino::{Utf8Path, Utf8PathBuf};
use engine::{JournalError, JournalLimits, JournalWriter, RunState};
use policy::is_safe_relative_path;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd as _, FromRawFd as _};
use std::sync::Arc;
use steps::deterministic_registry;
use tokio_util::sync::CancellationToken;

fn journal_target(
    record: &RunRecord,
) -> Result<(String, Utf8PathBuf, JournalLimits), ServiceError> {
    let run_id = record
        .run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory must have a run id".into()))?
        .to_string();
    let path = run_meta_dir(&record.run_dir).join("journal.jsonl");
    let limits = JournalLimits::from(&record.contract.manifest.runtime);
    Ok((run_id, path, limits))
}

/// Settle any active interval at a known service handoff. The caller holds
/// the execution lease after the prior engine task has exited; this opens the
/// journal with the run's configured limits and live event sender so the
/// durable ledger and subscribers observe the same checked recovery event.
pub(crate) fn settle_active_elapsed(record: &RunRecord) -> Result<(), ServiceError> {
    let (run_id, journal_path, limits) = journal_target(record)?;
    let journal = JournalWriter::create_with_limits(
        &journal_path,
        run_id,
        false,
        Some(record.events.clone()),
        limits,
    )
    .map_err(ServiceError::invalid)?;
    engine::settle_active_elapsed(&journal, chrono::Utc::now()).map_err(ServiceError::invalid)
}

pub(crate) fn write_run_event(
    record: &RunRecord,
    kind: &str,
    payload: Value,
) -> Result<(), ServiceError> {
    write_run_event_if(record, kind, payload, |_| Ok(()))
}

/// Check-and-append variant of [`write_run_event`]: `check` observes the
/// freshly folded durable state under the journal lock, so racing peers
/// serialize and exactly one conflicting acceptance wins.
pub(crate) fn write_run_event_if(
    record: &RunRecord,
    kind: &str,
    payload: Value,
    check: impl FnOnce(&RunState) -> Result<(), JournalError>,
) -> Result<(), ServiceError> {
    let (run_id, journal_path, limits) = journal_target(record)?;
    JournalWriter::append_single_event_if(
        &journal_path,
        &run_id,
        kind,
        payload,
        limits,
        Some(record.events.clone()),
        check,
    )
    .map_err(|error| match error {
        JournalError::PreconditionFailed(detail) => ServiceError::PreconditionFailed(detail),
        error => ServiceError::Invalid(error.to_string()),
    })?;
    Ok(())
}

/// Complete only an unambiguous accepted denial. Selection and both appends
/// share the journal lock; every surviving prefix is retryable. This is process
/// crash reconciliation, not a power-loss transaction.
pub(crate) fn settle_confirmed_denial(
    run_dir: &Utf8Path,
    confirmation_id: &str,
    limits: JournalLimits,
    event_sender: Option<tokio::sync::broadcast::Sender<api::RunEvent>>,
) -> Result<(), ServiceError> {
    let run_id = run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?;
    let path = run_meta_dir(run_dir).join("journal.jsonl");
    JournalWriter::append_events_with(&path, run_id, limits, event_sender, |state| {
        let ambiguous = || {
            JournalError::PreconditionFailed(
                "accepted denial identity or settlement is ambiguous".into(),
            )
        };
        if !list_pending_cancel_controls(run_dir)
            .map_err(|error| JournalError::InvalidEvent(error.to_string()))?
            .is_empty()
            || state.confirmations.get(confirmation_id) != Some(&false)
        {
            return Err(ambiguous());
        }
        // Reconstruct the exact prompt at acceptance, not a current generator
        // or an id-only guess. The extra bounded scan is only for denial
        // settlement; ordinary reads and approvals do not retain this history.
        let values = engine::read_journal_values(&path, limits)?.events;
        let mut prefix = RunState::default();
        let mut accepted = None;
        for (index, value) in values.iter().enumerate() {
            if value.get("t").and_then(Value::as_str) == Some("user_confirmed")
                && value.get("confirmation_id").and_then(Value::as_str) == Some(confirmation_id)
                && value.get("approved").and_then(Value::as_bool) == Some(false)
            {
                let Some(engine::Interaction::Confirmation { confirm }) = prefix.settled_pending()
                else {
                    return Err(ambiguous());
                };
                if confirm.id != confirmation_id
                    || prefix.terminal.is_some()
                    || prefix.cancel_requested
                    || prefix.confirmations.contains_key(confirmation_id)
                {
                    return Err(ambiguous());
                }
                accepted = Some((index, confirm.clone()));
                break;
            }
            prefix.apply(value)?;
        }
        let (index, confirm) = accepted.ok_or_else(ambiguous)?;
        let side_effect = json!({
            "kind": confirm.kind,
            "target": confirm.target,
            "decision": "denied_by_user",
        });
        let finished = json!({
            "status": "failed",
            "metrics": {},
            "reason": model::FailureDetail::new(
                model::FailureCode::ExecutionFailed, "side effect denied by user",
            ),
        });
        let settlement = [("side_effect", side_effect), ("run_finished", finished)];
        let suffix = &values[index + 1..];
        if suffix.len() > settlement.len() {
            return Err(ambiguous());
        }
        for (value, (kind, payload)) in suffix.iter().zip(&settlement) {
            if value.get("t").and_then(Value::as_str) != Some(*kind)
                || !payload
                    .as_object()
                    .ok_or_else(ambiguous)?
                    .iter()
                    .all(|(key, expected)| value.get(key) == Some(expected))
                || !value.as_object().ok_or_else(ambiguous)?.keys().all(|key| {
                    payload.get(key).is_some()
                        || matches!(
                            key.as_str(),
                            "t" | "ts" | "seq" | "run_id" | "trace_id" | "span_id"
                        )
                })
            {
                return Err(ambiguous());
            }
        }
        Ok(settlement.into_iter().skip(suffix.len()).collect())
    })
    .map_err(ServiceError::invalid)?;
    Ok(())
}

pub(crate) fn direct_run_id(workspace: &Utf8Path) -> String {
    let digest = hex::encode(Sha256::digest(workspace.as_str().as_bytes()));
    format!("direct-{digest}")
}

/// Direct CLI journals are owned only by their original invocation, never
/// adopted by the service scheduler. A missing package alone is not enough:
/// malformed or legacy service admissions must still fail closed.
pub(crate) fn is_direct_run(run_dir: &Utf8Path, state: &engine::RunState) -> bool {
    let Some(id) = run_dir.file_name() else {
        return false;
    };
    state.package_sha256.is_none()
        && state.run_id.as_deref() == Some(id)
        && id.strip_prefix("direct-").is_some_and(|digest| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

pub(crate) fn is_regular_directory(path: &Utf8Path) -> Result<bool, ServiceError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_dir()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

/// Refuses a symbolic link at `path`; a missing path passes only when `missing_ok`.
pub(crate) fn refuse_symlink(
    path: impl AsRef<std::path::Path>,
    missing_ok: bool,
    message: impl FnOnce() -> String,
) -> Result<(), ServiceError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(ServiceError::Invalid(message())),
        Ok(_) => Ok(()),
        Err(error) if missing_ok && error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

pub(crate) fn is_regular_file(path: &Utf8Path) -> Result<bool, ServiceError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_file()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

fn unlock_advisory_file(file: &File, owner: &'static str) {
    if let Err(error) = file.unlock() {
        tracing::warn!(%error, owner, "failed to unlock advisory file lock");
    }
}

/// Store-wide service ownership. Explicitly unlock on drop because a
/// concurrent fork may temporarily retain the descriptor until exec.
#[derive(Debug)]
pub(crate) struct RunStoreLock(File);

impl Drop for RunStoreLock {
    fn drop(&mut self) {
        unlock_advisory_file(&self.0, "run store");
    }
}

pub(crate) fn lock_runs_directory(runs_dir: &Utf8Path) -> Result<RunStoreLock, ServiceError> {
    lock_store(runs_dir, false)
}

/// Shared peers coexist on the same stable service-lock inode.
pub(crate) fn lock_runs_directory_shared(
    runs_dir: &Utf8Path,
) -> Result<RunStoreLock, ServiceError> {
    lock_store(runs_dir, true)
}

fn lock_store(runs_dir: &Utf8Path, shared: bool) -> Result<RunStoreLock, ServiceError> {
    std::fs::create_dir_all(runs_dir)?;
    let file = open_lock_file(&runs_dir.join(".service.lock"))?;
    let result = if shared {
        file.try_lock_shared()
    } else {
        file.try_lock()
    };
    match result {
        Ok(()) => Ok(RunStoreLock(file)),
        Err(std::fs::TryLockError::WouldBlock) => {
            let ownership = if shared { "exclusively" } else { "already" };
            Err(ServiceError::Invalid(format!(
                "runs directory `{runs_dir}` is {ownership} owned by another qcg service"
            )))
        }
        Err(std::fs::TryLockError::Error(error)) => Err(ServiceError::Io(error)),
    }
}

/// Cross-process prepare/adopt serialization. Fixed FNV shards outside deletable
/// run directories prevent stale-inode races and per-run lock accumulation.
/// Collisions wait cancellably; admission checks identity only after acquisition.
/// Legacy per-run lock files are ignored by scans and never created here.
pub struct RunAdmissionLock(File);

impl Drop for RunAdmissionLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

/// Fixed shard count; power of two for the index bitmask.
pub(crate) const RUN_COORDINATION_SHARD_COUNT: usize = 64;

/// FNV-1a maps each run-id directory name to one of the fixed shards;
/// the directory name is hashed, never a path.
fn run_coordination_shard_index(run_id: &str) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in run_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) & (RUN_COORDINATION_SHARD_COUNT - 1)
}

/// Coordination metadata (`.service.lock`, other hidden entries and
/// `idempotency`) is excluded from scans and scan budgets, including shutdown
/// orphan scanning. This keeps fixed shards and lifetime admissions bounded.
pub(crate) fn is_store_coordination_name(name: &str) -> bool {
    name.starts_with('.') || name == "idempotency"
}

/// Store-coordination entry probe (G03): coordination never consumes scan budgets.
pub(crate) fn is_coordination_entry(entry: &std::fs::DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .is_some_and(is_store_coordination_name)
}

/// Shared lock-file open for coordination locks: create without truncating, readable
/// and writable for `flock` semantics. Error mapping matches direct `OpenOptions` use.
fn open_lock_file(path: &Utf8Path) -> Result<std::fs::File, ServiceError> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(ServiceError::Io)
}

/// Advances a store scan budget after coordination skipping; every call counts one entry (G03).
pub(crate) fn bump_scan_count(scanned: &mut usize, max: usize) -> Result<(), ServiceError> {
    *scanned = scanned.saturating_add(1);
    if *scanned > max {
        return Err(ServiceError::Invalid(format!(
            "runs directory contains more than {max} entries"
        )));
    }
    Ok(())
}

pub(crate) fn try_lock_run_admission(
    run_dir: &Utf8Path,
) -> Result<Option<RunAdmissionLock>, ServiceError> {
    Ok(try_lock_run_shard(run_dir, ".admission-shards")?.map(RunAdmissionLock))
}

/// Permanent, bounded handoff shards outside deletable run directories.
/// Separate from admission so long drains do not block it; order is handoff then
/// execution (GC probes admission first), without waiting under maintenance.
pub(crate) struct RunHandoffLock(File);

impl Drop for RunHandoffLock {
    fn drop(&mut self) {
        unlock_advisory_file(&self.0, "run handoff");
    }
}

pub(crate) fn try_lock_run_handoff(
    run_dir: &Utf8Path,
) -> Result<Option<RunHandoffLock>, ServiceError> {
    Ok(try_lock_run_shard(run_dir, ".handoff-shards")?.map(RunHandoffLock))
}

pub(crate) async fn lock_run_handoff(
    run_dir: &Utf8Path,
    shutdown: &CancellationToken,
) -> Result<Option<RunHandoffLock>, ServiceError> {
    wait_for_run_lock(run_dir, shutdown, try_lock_run_handoff, true).await
}

/// Admission probes once even after shutdown; handoff/execution stop before probing.
/// A successful acquisition always transfers settlement responsibility to the caller.
async fn wait_for_run_lock<T>(
    run_dir: &Utf8Path,
    shutdown: &CancellationToken,
    probe: impl Fn(&Utf8Path) -> Result<Option<T>, ServiceError>,
    stop_before_probe: bool,
) -> Result<Option<T>, ServiceError> {
    loop {
        if stop_before_probe && shutdown.is_cancelled() {
            return Ok(None);
        }
        if let Some(lock) = probe(run_dir)? {
            return Ok(Some(lock));
        }
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(None),
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }
    }
}

fn try_lock_file(file: File) -> Result<Option<File>, ServiceError> {
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(ServiceError::Io(error)),
    }
}

fn try_lock_run_shard(run_dir: &Utf8Path, directory: &str) -> Result<Option<File>, ServiceError> {
    // Open the stable shard; never unlink it or split ownership.
    let runs_dir = run_dir
        .parent()
        .ok_or_else(|| ServiceError::Invalid(format!("run directory `{run_dir}` has no parent")))?;
    let shards_dir = runs_dir.join(directory);
    std::fs::create_dir_all(&shards_dir)?;
    let run_id = run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid(format!("run directory `{run_dir}` has no name")))?;
    let shard = run_coordination_shard_index(run_id);
    let file = open_lock_file(&shards_dir.join(format!("shard-{shard:02x}.lock")))?;
    try_lock_file(file)
}

/// Wait cancellably for the admission shard; contention is not a same-run conflict.
/// Shutdown returns None without admitting work. Never unlink shard files.
pub(crate) async fn lock_run_admission(
    run_dir: &Utf8Path,
    shutdown: &tokio_util::sync::CancellationToken,
) -> Result<Option<RunAdmissionLock>, ServiceError> {
    wait_for_run_lock(run_dir, shutdown, try_lock_run_admission, false).await
}

/// Explicit unlock ends ownership despite fork/pre-exec descriptor inheritance.
pub(crate) struct RunExecutionLock(File);

impl Drop for RunExecutionLock {
    fn drop(&mut self) {
        unlock_advisory_file(&self.0, "run execution");
    }
}

/// Complete durable handoff despite transient competition; shutdown cancels waiting.
pub(crate) async fn lock_run_execution(
    run_dir: &Utf8Path,
    shutdown: &CancellationToken,
) -> Result<Option<RunExecutionLock>, ServiceError> {
    wait_for_run_lock(run_dir, shutdown, try_lock_run_execution, true).await
}

pub(crate) fn try_lock_run_execution(
    run_dir: &Utf8Path,
) -> Result<Option<RunExecutionLock>, ServiceError> {
    let metadata = run_meta_dir(run_dir);
    std::fs::create_dir_all(&metadata)?;
    try_lock_existing_run_execution(run_dir)
}

/// Probe without recreating a missing run or metadata directory (retention).
/// Older journals may not yet have an execution.lock, so create the file only.
pub(crate) fn try_lock_existing_run_execution(
    run_dir: &Utf8Path,
) -> Result<Option<RunExecutionLock>, ServiceError> {
    let file = open_lock_file(&run_meta_dir(run_dir).join("execution.lock"))?;
    Ok(try_lock_file(file)?.map(RunExecutionLock))
}

/// Persist an advisory owner hint inside the lease file for shared-peer refresh.
/// The file lock, not this bounded hint, remains authoritative.
pub(crate) fn claim_run_execution_owner(
    lock: &RunExecutionLock,
    owner_id: &str,
) -> Result<(), ServiceError> {
    use std::io::Seek as _;
    let mut lock = &lock.0;
    lock.rewind().map_err(ServiceError::Io)?;
    lock.set_len(0).map_err(ServiceError::Io)?;
    lock.write_all(owner_id.as_bytes())
        .map_err(ServiceError::Io)?;
    lock.sync_all().map_err(ServiceError::Io)?;
    Ok(())
}

/// Read a bounded advisory owner hint; missing, linked or malformed means unknown.
pub(crate) fn read_run_execution_owner(run_dir: &Utf8Path) -> Option<String> {
    let path = run_meta_dir(run_dir).join("execution.lock");
    let meta = std::fs::symlink_metadata(&path).ok()?;
    if meta.file_type().is_symlink() || !meta.is_file() {
        return None;
    }
    #[cfg(unix)]
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
        .ok()?;
    #[cfg(not(unix))]
    let file = std::fs::File::open(&path).ok()?;
    use std::io::Read as _;
    let mut bytes = Vec::new();
    file.take(256).read_to_end(&mut bytes).ok()?;
    let owner = String::from_utf8(bytes).ok()?;
    if owner.is_empty()
        || owner.len() > 128
        || !owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return None;
    }
    Some(owner)
}

/// Store-wide maintenance ownership. Explicitly unlock on drop because a
/// concurrent fork may temporarily retain the descriptor until exec.
pub struct StoreMaintenanceLock(File, pub(crate) Utf8PathBuf);

impl Drop for StoreMaintenanceLock {
    fn drop(&mut self) {
        unlock_advisory_file(&self.0, "run store maintenance");
    }
}

/// Try the stable store maintenance lease. Acquire before any execution lease.
pub fn try_lock_store_maintenance(
    runs_dir: &Utf8Path,
) -> Result<Option<StoreMaintenanceLock>, ServiceError> {
    std::fs::create_dir_all(runs_dir)?;
    let file = open_lock_file(&runs_dir.join(".maintenance.lock"))?;
    Ok(try_lock_file(file)?.map(|file| StoreMaintenanceLock(file, runs_dir.to_owned())))
}

pub(crate) fn prepare_api_run_directory(run_dir: &Utf8Path) -> Result<(), ServiceError> {
    let metadata = run_meta_dir(run_dir);
    std::fs::create_dir_all(&metadata)?;
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(metadata.join("journal.jsonl"))?;
    Ok(())
}

/// Returns whether this call created the target directory (cleanup ownership).
pub(crate) fn prepare_checkpoint_fork(
    source_dir: &Utf8Path,
    source_id: &str,
    target_dir: &Utf8Path,
    target_id: &str,
    at_seq: u64,
    patch: &ForkStatePatch,
) -> Result<bool, ServiceError> {
    // An incomplete admission may retain its directory for a cancel mailbox.
    // Record actual create-dir ownership before populating; whole-directory
    // cleanup is permitted only for a fresh target, never a pre-existing one.
    let created_fresh = match std::fs::create_dir(target_dir) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if !is_regular_directory(target_dir)? {
                return Err(ServiceError::Invalid(
                    "fork target is not a regular directory".into(),
                ));
            }
            false
        }
        Err(error) => return Err(ServiceError::Io(error)),
    };
    let result = prepare_api_run_directory(target_dir).and_then(|()| {
        populate_checkpoint_fork(source_dir, source_id, target_dir, target_id, at_seq, patch)
    });
    if let Err(error) = result {
        if created_fresh && let Err(cleanup) = std::fs::remove_dir_all(target_dir) {
            return Err(ServiceError::Invalid(format!(
                "{error}; failed to clean fresh fork: {cleanup}"
            )));
        }
        return Err(error);
    }
    Ok(created_fresh)
}

fn populate_checkpoint_fork(
    source_dir: &Utf8Path,
    source_id: &str,
    target_dir: &Utf8Path,
    target_id: &str,
    at_seq: u64,
    patch: &ForkStatePatch,
) -> Result<(), ServiceError> {
    let source_meta = run_meta_dir(source_dir);
    let source_events = read_events_from_meta(&source_meta)?;
    if !source_events
        .iter()
        .any(|event| event.get("seq").and_then(Value::as_u64) == Some(at_seq))
    {
        return Err(ServiceError::Invalid(format!(
            "run `{source_id}` has no checkpoint sequence {at_seq}"
        )));
    }
    let selected = source_events
        .into_iter()
        .filter(|event| {
            event
                .get("seq")
                .and_then(Value::as_u64)
                .is_some_and(|seq| seq <= at_seq)
        })
        // Project confirmation pins without carrying the source's pending interaction.
        .filter_map(|mut event| {
            let kind = event.get("t").and_then(Value::as_str).unwrap_or_default();
            if matches!(
                kind,
                "run_finished" | "run_error" | "run_canceled" | "run_interrupted" | "run_waiting"
            ) {
                return None;
            }
            if kind == "confirm_request" {
                event.get("node").and_then(Value::as_str)?;
                let object = event.as_object_mut()?;
                object.remove("confirm");
                object.insert("t".into(), "step_finished".into());
                object.insert("status".into(), "needs_confirm".into());
            }
            Some(event)
        })
        .collect::<Vec<_>>();
    if !selected.iter().any(|event| {
        matches!(
            event.get("t").and_then(Value::as_str),
            Some("run_started" | "run_queued")
        )
    }) {
        return Err(ServiceError::Invalid(format!(
            "checkpoint {source_id}@{at_seq} predates run initialization"
        )));
    }

    let target_meta = run_meta_dir(target_dir);
    let target_workspace = run_workspace_dir(target_dir);
    std::fs::create_dir_all(&target_workspace)?;
    std::fs::create_dir_all(target_meta.join("checkpoint-blobs"))?;

    let mut latest_files = BTreeMap::<Utf8PathBuf, String>::new();
    // Retain all historical blobs for resume verification; validate digests before stat so malformed pins
    // cannot probe arbitrary store paths. (E06)
    fn validate_blob_digest(path: &Utf8PathBuf, digest: &str) -> Result<(), ServiceError> {
        if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{path}` has a malformed sha256 digest"
            )));
        }
        Ok(())
    }
    let mut required_blobs = BTreeMap::<String, Utf8PathBuf>::new();
    for event in &selected {
        if event["t"] == "agent_file_changes" {
            for path in event["removed_files"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                latest_files.remove(Utf8Path::new(path));
            }
        }
        if event.get("t").and_then(Value::as_str) != Some("step_finished") {
            continue;
        }
        // Keep success and failure pins: resume verifies every historical blob.
        // Only successful or suspended step outputs project into the workspace.
        let pins = ["files", "failed_files"].into_iter().flat_map(|key| {
            event
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        });
        let status = event.get("status").and_then(Value::as_str);
        let projects_workspace = matches!(
            status,
            Some("success" | "repaired" | "routed" | "answered_on_fail" | "regenerated")
        ) || matches!(status, Some("needs_user" | "needs_confirm"));
        for file in pins {
            let path = file
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| ServiceError::Invalid("checkpoint file pin has no path".into()))?;
            let path = Utf8PathBuf::from(path);
            if !is_safe_relative_path(path.as_str()) {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint file path `{path}` is unsafe"
                )));
            }
            let digest = file.get("sha256").and_then(Value::as_str).ok_or_else(|| {
                ServiceError::Invalid(format!("checkpoint file `{path}` has no sha256"))
            })?;
            validate_blob_digest(&path, digest)?;
            required_blobs
                .entry(digest.to_string())
                .or_insert_with(|| path.clone());
            if projects_workspace {
                latest_files.insert(path, digest.to_string());
            }
        }
    }
    policy::path::validate_path_tree_set(latest_files.keys().map(|p| (p.as_str(), false)))?;
    // The target is fresh, but validate every parent to block symlink redirection.
    // This preserves FsGateway's workspace-root confinement invariant.
    std::fs::create_dir_all(&target_workspace)?;
    let canonical_target_root = dunce::canonicalize(&target_workspace).map_err(|error| {
        ServiceError::Invalid(format!("fork target workspace is not canonical: {error}"))
    })?;
    let mut verified_blobs = BTreeSet::<String>::new();
    for (path, digest) in latest_files {
        safe_join_under_root(&target_workspace, &canonical_target_root, &path)?;
        let destination = target_workspace.join(path.as_str());
        if let Some(parent) = destination.parent() {
            ensure_dir_under_root(&target_workspace, &canonical_target_root, parent)?;
        }
        // Reuse prior digest verification across paths to avoid redundant copies/hashes (E06).
        if verified_blobs.contains(&digest) {
            let target_blob = target_meta.join("checkpoint-blobs").join(&digest);
            copy_verified_blob_to_workspace(&target_blob, &destination, &path, &digest)?;
            continue;
        }
        // Fail-closed: a missing blob refuses the fork. The
        // workspace-current fallback was deleted: it let a mutated workspace
        // satisfy a historical pin without the blob store proof the replay
        // side requires (E06).
        let source_blob = source_meta.join("checkpoint-blobs").join(&digest);
        if !regular_file_exists(&source_blob)? {
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` is missing; refusing the fork"
            )));
        }
        // O_NOFOLLOW open plus handle-based hashing: the source workspace
        // and blob store are attacker-influenced, so path-based
        // symlink_metadata-then-open would TOCTOU through a swapped symlink
        // (E06).
        verified_source(&source_blob, &digest, &path)?;
        // Atomic copy via temp + rename with post-copy verification. The
        // bytes come from the verified handle, never a second path open.
        // The repeated hashes below (tmp, destination, blob store) are
        // deliberate defense, not deduplicatable work: each verifies a
        // different durability step (copy fidelity, commit fidelity, store
        // fidelity), and digests already verified for another path are
        // skipped via `verified_blobs` (E06). Note: this copy path opens
        // several handles by design (source handle, tmp, destination), so
        // the replay-side single-handle claim does not extend here — every
        // handle here is independently verified instead (E06).
        let tmp = destination.with_file_name(format!(
            ".{}.part-{}",
            destination.file_name().unwrap_or("blob"),
            uuid::Uuid::now_v7().as_simple()
        ));
        copy_source_handle_to_tmp(&source_blob, &tmp, &digest, &path)?;
        if files::hash_file_sha256(&tmp, None).map(|(hex, _)| hex)? != digest {
            let _ = std::fs::remove_file(&tmp);
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{path}` changed while it was copied"
            )));
        }
        std::fs::rename(&tmp, &destination)?;
        if let Some(parent) = destination.parent() {
            sync_dir_entry(parent).map_err(ServiceError::Io)?;
        }
        if files::hash_file_sha256(&destination, None).map(|(hex, _)| hex)? != digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{path}` changed while it was committed"
            )));
        }
        verified_blobs.insert(digest.clone());
        let target_blob = target_meta.join("checkpoint-blobs").join(&digest);
        std::fs::copy(&destination, &target_blob)?;
        sync_dir_entry(&target_meta.join("checkpoint-blobs")).map_err(ServiceError::Io)?;
        if files::hash_file_sha256(&target_blob, None).map(|(hex, _)| hex)? != digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` failed integrity verification after the copy"
            )));
        }
    }
    // Historical revisions come only from the blob store: a missing blob
    // refuses the fork instead of falling back to the mutable workspace
    // (E06).
    for (digest, path) in &required_blobs {
        if !verified_blobs.insert(digest.clone()) {
            continue;
        }
        let target_blob = target_meta.join("checkpoint-blobs").join(digest);
        if regular_file_exists(&target_blob)? {
            // An existing blob is never trusted blindly: verify it like the
            // freshly copied ones so a corrupted store cannot seed a fork.
            verified_source(&target_blob, digest, path)?;
            continue;
        }
        let source_blob = source_meta.join("checkpoint-blobs").join(digest);
        if !regular_file_exists(&source_blob)? {
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` is missing; refusing the fork"
            )));
        }
        verified_source(&source_blob, digest, path)?;
        // Stage through a temp plus rename, never directly at the final
        // path: a crash mid-copy must leave no partial blob that later
        // verifies as fixed corruption (E03/E06).
        let blob_tmp = target_blob.with_file_name(format!(
            ".{digest}.part-{}",
            uuid::Uuid::now_v7().as_simple()
        ));
        copy_source_handle_to_tmp(&source_blob, &blob_tmp, digest, path)?;
        if files::hash_file_sha256(&blob_tmp, None).map(|(hex, _)| hex)? != *digest {
            let _ = std::fs::remove_file(&blob_tmp);
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` failed integrity verification after the copy"
            )));
        }
        std::fs::rename(&blob_tmp, &target_blob).map_err(ServiceError::Io)?;
        sync_dir_entry(&target_meta.join("checkpoint-blobs")).map_err(ServiceError::Io)?;
        if files::hash_file_sha256(&target_blob, None).map(|(hex, _)| hex)? != *digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` failed integrity verification after the copy"
            )));
        }
    }

    let journal_path = target_meta.join("journal.jsonl");
    // Direct history build in the fresh fork directory: no concurrent reader
    // exists yet (the directory was just created by this admission), so temp
    // staging is unnecessary; a final sync_all makes the whole prefix durable
    // at once (E06). Durable and observation records are renumbered together
    // in their original seq order and routed back to their own stream, so
    // the fork preserves the merged history the source exposed (ADR 0001).
    let mut history: Vec<(u64, bool, Value)> = selected
        .into_iter()
        .map(|event| {
            let seq = event.get("seq").and_then(Value::as_u64).unwrap_or(0);
            (seq, false, event)
        })
        .collect();
    let source_audit_path = source_meta.join("audit.jsonl");
    if source_audit_path.exists() {
        let audit_events =
            engine::read_journal_values(&source_audit_path, JournalLimits::default())
                .map(|scan| scan.events)
                .map_err(ServiceError::invalid)?;
        for event in audit_events {
            let Some(seq) = event.get("seq").and_then(Value::as_u64) else {
                continue;
            };
            if seq <= at_seq {
                history.push((seq, true, event));
            }
        }
    }
    history.sort_by_key(|(seq, is_audit, _)| (*seq, *is_audit));
    let mut journal = OpenOptions::new().append(true).open(&journal_path)?;
    let audit_path = target_meta.join("audit.jsonl");
    let mut audit_writer = if history.iter().any(|(_, is_audit, _)| *is_audit) {
        Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&audit_path)?,
        )
    } else {
        None
    };
    let mut next_seq = 0_u64;
    for (_, is_audit, mut event) in history {
        next_seq = next_seq.saturating_add(1);
        let object = event.as_object_mut().ok_or_else(|| {
            ServiceError::Invalid("checkpoint journal event is not an object".into())
        })?;
        object.insert("seq".into(), Value::Number(next_seq.into()));
        object.insert("run_id".into(), Value::String(target_id.to_string()));
        object.insert(
            "trace_id".into(),
            Value::String(api::trace_id_for_run(target_id)),
        );
        object.insert(
            "span_id".into(),
            Value::String(api::span_id_for_seq(next_seq)),
        );
        if object.contains_key("parent_span_id") {
            let scope = object
                .get("node")
                .and_then(Value::as_str)
                .map(|node| format!("step:{node}"))
                .unwrap_or_else(|| "run".to_string());
            object.insert(
                "parent_span_id".into(),
                Value::String(api::span_id_for_scope(target_id, &scope)),
            );
        }
        let writer: &mut std::fs::File = if is_audit {
            audit_writer
                .as_mut()
                .ok_or_else(|| ServiceError::Invalid("fork audit stream is missing".into()))?
        } else {
            &mut journal
        };
        serde_json::to_writer(&mut *writer, &event)?;
        writer.write_all(b"\n")?;
    }
    next_seq = next_seq.saturating_add(1);
    let fork_event = json!({
        "t": "run_forked",
        "ts": chrono::Utc::now().to_rfc3339(),
        "seq": next_seq,
        "run_id": target_id,
        "trace_id": api::trace_id_for_run(target_id),
        "span_id": api::span_id_for_seq(next_seq),
        "source_run_id": source_id,
        "source_seq": at_seq,
    });
    serde_json::to_writer(&mut journal, &fork_event)?;
    journal.write_all(b"\n")?;
    if !patch.inputs.is_empty() || !patch.step_outputs.is_empty() || !patch.step_statuses.is_empty()
    {
        next_seq = next_seq.saturating_add(1);
        let patch_event = json!({
            "t": "state_patched",
            "ts": chrono::Utc::now().to_rfc3339(),
            "seq": next_seq,
            "run_id": target_id,
            "trace_id": api::trace_id_for_run(target_id),
            "span_id": api::span_id_for_seq(next_seq),
            "inputs": patch.inputs,
            "step_outputs": patch.step_outputs,
            "step_statuses": patch.step_statuses,
        });
        serde_json::to_writer(&mut journal, &patch_event)?;
        journal.write_all(b"\n")?;
    }
    journal.sync_all()?;
    if let Some(audit) = audit_writer.as_mut() {
        audit.sync_all()?;
    }
    // Sync the directory entry like every other journal publication path:
    // without it a power loss can lose the fork journal itself (Q2).
    sync_dir_entry(&target_meta).map_err(ServiceError::Io)?;
    let state = RunState::fold_journal(&journal_path).map_err(ServiceError::invalid)?;
    state
        .persist_atomic(&target_meta.join("state.json"))
        .map_err(ServiceError::invalid)?;
    Ok(())
}

/// Distinguishes "absent" from "unreadable" for fork inputs: a permission
/// error must surface instead of being misread as missing (E06).
fn regular_file_exists(path: &Utf8Path) -> Result<bool, ServiceError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(ServiceError::Invalid(format!(
            "checkpoint path `{path}` is a symbolic link"
        ))),
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

/// No-follow checkpoint source; validate the opened handle as well as the path.
/// Non-Unix retains a pre-open symlink probe, not a race-safe guarantee.
fn open_regular_file_nofollow(path: &Utf8Path) -> Result<std::fs::File, ServiceError> {
    let map_error = |error: std::io::Error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ServiceError::Invalid(format!("checkpoint path `{path}` is missing"))
        } else {
            ServiceError::Io(error)
        }
    };
    #[cfg(not(unix))]
    if std::fs::symlink_metadata(path)
        .map_err(map_error)?
        .file_type()
        .is_symlink()
    {
        return Err(ServiceError::Invalid(format!(
            "checkpoint path `{path}` is a symbolic link"
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path).map_err(map_error)?;
    if !file.metadata()?.is_file() {
        return Err(ServiceError::Invalid(format!(
            "checkpoint path `{path}` is not a regular file"
        )));
    }
    Ok(file)
}

/// Hashes an already-open handle without reopening by path, so a symlink
/// swap between validation and hashing cannot redirect the bytes (E06).
fn hash_opened_file(file: &mut std::fs::File) -> Result<String, ServiceError> {
    use std::io::{Read as _, Seek as _};
    file.rewind().map_err(ServiceError::Io)?;
    let mut hasher = Sha256::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut chunk).map_err(ServiceError::Io)?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Verifies a checkpoint source digest through its O_NOFOLLOW handle. The
/// digest comparison happens on handle-read bytes, never a second path
/// open that could follow a swapped link (E06).
fn verified_source(
    source: &Utf8Path,
    expected_digest: &str,
    path_label: &Utf8Path,
) -> Result<File, ServiceError> {
    let mut handle = open_regular_file_nofollow(source)?;
    let actual = hash_opened_file(&mut handle)?;
    if actual != expected_digest {
        return Err(ServiceError::Invalid(format!(
            "checkpoint blob `{expected_digest}` for `{path_label}` failed integrity verification"
        )));
    }
    Ok(handle)
}

/// Copy from a verified no-follow handle into an exclusive, synced temp file.
fn copy_source_handle_to_tmp(
    source: &Utf8Path,
    tmp: &Utf8Path,
    expected_digest: &str,
    path_label: &Utf8Path,
) -> Result<(), ServiceError> {
    use std::io::Seek as _;
    let mut handle = verified_source(source, expected_digest, path_label)?;
    handle.rewind().map_err(ServiceError::Io)?;
    let mut tmp_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp)
        .map_err(ServiceError::Io)?;
    std::io::copy(&mut handle, &mut tmp_file).map_err(ServiceError::Io)?;
    tmp_file.sync_all().map_err(ServiceError::Io)?;
    drop(tmp_file);
    Ok(())
}

/// Project a verified target-store blob through the same synced staging path.
/// Verify both copy and committed destination, without reopening the original source.
fn copy_verified_blob_to_workspace(
    verified_blob: &Utf8Path,
    destination: &Utf8Path,
    path_label: &Utf8Path,
    expected_digest: &str,
) -> Result<(), ServiceError> {
    let tmp = destination.with_file_name(format!(
        ".{}.part-{}",
        destination.file_name().unwrap_or("blob"),
        uuid::Uuid::now_v7().as_simple()
    ));
    copy_source_handle_to_tmp(verified_blob, &tmp, expected_digest, path_label)?;
    if files::hash_file_sha256(&tmp, None).map(|(hex, _)| hex)? != expected_digest {
        let _ = std::fs::remove_file(&tmp);
        return Err(ServiceError::Invalid(format!(
            "checkpoint file `{path_label}` changed while it was copied"
        )));
    }
    std::fs::rename(&tmp, destination)?;
    if let Some(parent) = destination.parent() {
        sync_dir_entry(parent).map_err(ServiceError::Io)?;
    }
    if files::hash_file_sha256(destination, None).map(|(hex, _)| hex)? != expected_digest {
        return Err(ServiceError::Invalid(format!(
            "checkpoint file `{path_label}` changed while it was committed"
        )));
    }
    Ok(())
}

pub(crate) fn journal_is_empty(path: &Utf8Path) -> std::io::Result<bool> {
    Ok(std::fs::metadata(path)?.len() == 0)
}

/// Wipe an incomplete admission under its admission lease, keeping meta/control.
/// Cancel publication can race preparation, so never remove the whole directory.
/// Partial idempotency sidecars/blobs carry no completed admission and are removed;
/// host-cgroup evidence instead refuses the wipe until exclusively reconciled.
fn wipe_incomplete_run_dir(run_dir: &Utf8Path) -> Result<(), ServiceError> {
    use crate::summaries::run_meta_dir;
    let binding = engine::host_cgroup::binding_path(&run_meta_dir(run_dir));
    match std::fs::symlink_metadata(&binding) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
        Ok(_) => {
            return Err(ServiceError::Invalid(format!(
                "host-cgroup evidence at `{binding}`; refusing incomplete-admission wipe; reconcile/delete under exclusive ownership"
            )));
        }
    }
    let keep = run_meta_dir(run_dir).join("control");
    // Refuse linked mailboxes, including swaps; never follow them during wiping.
    refuse_symlink(&keep, true, || {
        format!("cancel control directory `{keep}` is a symlink; refusing wipe")
    })?;
    // Enumerated no-follow deletion preserves concurrent mailbox publication.
    // Recurse only into real ancestors of keep; remove links as links.
    fn wipe_except(dir: &Utf8Path, keep: &Utf8Path) -> Result<(), ServiceError> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(ServiceError::Io(error)),
        };
        for entry in entries {
            let entry = entry.map_err(ServiceError::Io)?;
            let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                ServiceError::Invalid(format!("run path is not UTF-8: {}", path.display()))
            })?;
            if path == keep {
                // Revalidate against swaps since the outer probe.
                match std::fs::symlink_metadata(&path) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        return Err(ServiceError::Invalid(format!(
                            "cancel control directory `{keep}` became a symlink; refusing wipe"
                        )));
                    }
                    Ok(_) => continue,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(ServiceError::Io(error)),
                }
            }
            // Vanished entries are races; other inspection failures must surface.
            let is_real_dir = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata.is_dir(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(ServiceError::Io(error)),
            };
            if is_real_dir && keep.starts_with(&path) {
                wipe_except(&path, keep)?;
            } else if is_real_dir {
                std::fs::remove_dir_all(&path)?;
            } else {
                std::fs::remove_file(&path)?;
            }
        }
        Ok(())
    }
    wipe_except(run_dir, &keep)?;
    // Remove only empty directories; concurrent mailbox publication survives.
    // No-follow probing must not mistake a linked mailbox for absence.
    let keep_missing = match std::fs::symlink_metadata(&keep) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Ok(_) => false,
        Err(error) => return Err(ServiceError::Io(error)),
    };
    if keep_missing {
        // Cleanup failure leaves an adoptable directory; warn without losing the mailbox.
        let meta = run_meta_dir(run_dir);
        if let Err(error) = std::fs::remove_dir(meta.as_std_path()) {
            tracing::warn!(dir = %meta, %error, "incomplete wipe left an empty meta directory");
        }
        if let Err(error) = std::fs::remove_dir(run_dir.as_std_path()) {
            tracing::warn!(dir = %run_dir, %error, "incomplete wipe left an empty run directory");
        }
    }
    Ok(())
}

/// Reusable admission snapshot: events, seed/queue inputs and one state fold.
pub(crate) struct AdoptSnapshot {
    pub(crate) adopted: bool,
    pub(crate) events: Vec<Value>,
    pub(crate) state: Option<RunState>,
}

impl AdoptSnapshot {
    fn empty() -> Self {
        Self {
            adopted: false,
            events: Vec::new(),
            state: None,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Adopt only the reserved identity with its own run_queued after all run_forked
/// events. Copied fork history is not completed admission. Partial preparation
/// is wiped with its cancel mailbox preserved; foreign identities fail closed.
pub(crate) fn try_adopt_run_dir_with_snapshot(
    run_dir: &Utf8Path,
    run_id: &str,
) -> Result<AdoptSnapshot, ServiceError> {
    use crate::summaries::{read_events_from_meta, run_meta_dir};
    // Single attempt (E03): every caller holds the per-run admission lock,
    // which serializes prepare/wipe/adopt for one run id, so no concurrent
    // admission can complete a journal while this probe runs.
    match std::fs::symlink_metadata(run_dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(AdoptSnapshot::empty());
        }
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(ServiceError::Invalid(
                "run directory is not a plain directory; refusing adoption".into(),
            ));
        }
        Ok(_) => {}
        Err(error) => return Err(ServiceError::Io(error)),
    }
    let metadata_dir = run_meta_dir(run_dir);
    match std::fs::symlink_metadata(&metadata_dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Incomplete preparation: preserve the cancel mailbox.
            wipe_incomplete_run_dir(run_dir)?;
            return Ok(AdoptSnapshot::empty());
        }
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(ServiceError::Invalid(
                "run metadata is not a plain directory; refusing adoption".into(),
            ));
        }
        Ok(_) => {}
        Err(error) => return Err(ServiceError::Io(error)),
    }
    let journal_path = metadata_dir.join("journal.jsonl");
    // No-follow metadata suffices under admission ownership.
    match std::fs::symlink_metadata(&journal_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Incomplete preparation: preserve the cancel mailbox.
            wipe_incomplete_run_dir(run_dir)?;
            return Ok(AdoptSnapshot::empty());
        }
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(ServiceError::Invalid(format!(
                "run journal `{journal_path}` is a symlink; refusing adoption"
            )));
        }
        Ok(metadata) if metadata.len() == 0 => {
            // Empty preparation: preserve the cancel mailbox.
            wipe_incomplete_run_dir(run_dir)?;
            return Ok(AdoptSnapshot::empty());
        }
        Ok(_) => {}
        Err(error) => return Err(ServiceError::Io(error)),
    }
    // Reuse one read for folding and admission identity.
    let events = read_events_from_meta(&run_meta_dir(run_dir))?;
    let state = RunState::fold_values(&events).map_err(ServiceError::invalid)?;
    match state.run_id.as_deref() {
        Some(existing) if existing == run_id => {}
        Some(_) => {
            return Err(ServiceError::Invalid(format!(
                "run directory for `{run_id}` holds a different run; refusing adoption"
            )));
        }
        None => {
            return Err(ServiceError::Invalid(format!(
                "run `{run_id}` journal has no initialization event; refusing adoption"
            )));
        }
    }
    // Only an admission after the last fork marker completes this run.
    let last_fork_seq = events
        .iter()
        .filter(|event| event.get("t").and_then(Value::as_str) == Some("run_forked"))
        .filter_map(|event| event.get("seq").and_then(Value::as_u64))
        .max();
    let initialized = events.iter().any(|event| {
        event.get("t").and_then(Value::as_str) == Some("run_queued")
            && event.get("run_id").and_then(Value::as_str) == Some(run_id)
            && last_fork_seq.is_none_or(|fork_seq| {
                event
                    .get("seq")
                    .and_then(Value::as_u64)
                    .is_some_and(|seq| seq > fork_seq)
            })
    });
    if !initialized {
        // Under admission ownership, non-admission writers cannot change this verdict.
        wipe_incomplete_run_dir(run_dir)?;
        return Ok(AdoptSnapshot::empty());
    }
    Ok(AdoptSnapshot {
        adopted: true,
        events,
        state: Some(state),
    })
}

/// Rejects absolute paths, parent traversal, and terminal symlinks while
/// resolving `relative` under an already-canonicalized `root`.
fn safe_join_under_root(
    workspace: &Utf8Path,
    canonical_root: &std::path::PathBuf,
    relative: &Utf8PathBuf,
) -> Result<Utf8PathBuf, ServiceError> {
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, camino::Utf8Component::Normal(_)))
    {
        return Err(ServiceError::Invalid(format!(
            "checkpoint file path `{relative}` is unsafe"
        )));
    }
    let joined = workspace.join(relative);
    // Walk parents level by level, refusing symlinks and escapes.
    let mut current = canonical_root.clone();
    let parts: Vec<String> = relative
        .components()
        .filter_map(|component| match component {
            camino::Utf8Component::Normal(part) => Some(part.to_string()),
            _ => None,
        })
        .collect();
    let Some((_, parents)) = parts.split_last() else {
        return Err(ServiceError::Invalid(format!(
            "checkpoint file path `{relative}` is unsafe"
        )));
    };
    for part in parents {
        let next = camino::Utf8PathBuf::from_path_buf(current.join(part)).map_err(|_| {
            ServiceError::Invalid(format!("checkpoint path `{relative}` is not UTF-8"))
        })?;
        match std::fs::symlink_metadata(&next) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint parent `{next}` is a symlink"
                )));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint parent `{next}` is not a directory"
                )));
            }
            Ok(_) => {
                let canonical = dunce::canonicalize(&next)?;
                if !canonical.starts_with(canonical_root) {
                    return Err(ServiceError::Invalid(format!(
                        "checkpoint path `{relative}` escapes the workspace"
                    )));
                }
                current = canonical;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(joined);
            }
            Err(error) => return Err(ServiceError::Io(error)),
        }
    }
    // A missing leaf is a fresh output path and stays admissible; any
    // other probe failure (permissions, I/O) fails closed instead of
    // treating an unscannable leaf as safe (E06).
    refuse_symlink(&joined, true, || {
        format!("checkpoint file `{relative}` is a symlink")
    })?;
    Ok(joined)
}

fn ensure_dir_under_root(
    workspace: &Utf8Path,
    canonical_root: &std::path::PathBuf,
    dir: &Utf8Path,
) -> Result<(), ServiceError> {
    let relative = dir.strip_prefix(workspace).map_err(|_| {
        ServiceError::Invalid(format!(
            "checkpoint directory `{dir}` escapes the workspace"
        ))
    })?;
    if relative.as_str().is_empty() {
        std::fs::create_dir_all(dir)?;
        return Ok(());
    }
    let mut current = canonical_root.clone();
    for part in relative
        .components()
        .filter_map(|component| match component {
            camino::Utf8Component::Normal(part) => Some(part.to_string()),
            _ => None,
        })
    {
        let next = camino::Utf8PathBuf::from_path_buf(current.join(&part)).map_err(|_| {
            ServiceError::Invalid(format!("checkpoint directory `{dir}` is not UTF-8"))
        })?;
        match std::fs::symlink_metadata(&next) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint parent `{next}` is a symlink"
                )));
            }
            Ok(metadata) if metadata.is_dir() => {
                let canonical = dunce::canonicalize(&next)?;
                if !canonical.starts_with(canonical_root) {
                    return Err(ServiceError::Invalid(format!(
                        "checkpoint directory `{dir}` escapes the workspace"
                    )));
                }
                current = canonical;
            }
            Ok(_) => {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint parent `{next}` is not a directory"
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&next)?;
                let canonical = dunce::canonicalize(&next)?;
                if !canonical.starts_with(canonical_root) {
                    let _ = std::fs::remove_dir(&canonical);
                    return Err(ServiceError::Invalid(format!(
                        "checkpoint directory `{dir}` escapes the workspace"
                    )));
                }
                current = canonical;
            }
            Err(error) => return Err(ServiceError::Io(error)),
        }
    }
    Ok(())
}

/// Durable cross-process cancel mailbox. Peers never append
/// `user_cancel_requested` directly while an owner may be running; they
/// record a control file and the owner converts it to a single journal
/// event under its own writer (A01/A02). Control files carry a stable
/// `operation_id` so duplicate deliveries collapse to one journal event.
pub(crate) fn control_dir(run_dir: &Utf8Path) -> Utf8PathBuf {
    crate::summaries::run_meta_dir(run_dir).join("control")
}

/// Age after which an unconfirmed publish temp is considered abandoned.
/// A live publisher's temp is fresh, so the scanner side never touches it.
const CANCEL_TMP_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Pin the mailbox inode with O_DIRECTORY | O_NOFOLLOW; staging and Linux
/// publication use directory-relative names to resist pathname swaps.
#[cfg(unix)]
fn open_control_dir_pinned(dir: &Utf8Path) -> Result<File, ServiceError> {
    std::fs::create_dir_all(dir)?;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(dir)
        .map_err(ServiceError::Io)
}

/// Create an owner-only, no-follow temp relative to the pin; EEXIST retries.
#[cfg(unix)]
fn create_cancel_temp_at(dir_fd: &File, name: &str) -> Result<File, ServiceError> {
    let owned = std::ffi::CString::new(name)
        .map_err(|_| ServiceError::Invalid("cancel staging name contains a NUL byte".into()))?;
    // SAFETY: dir_fd is an open directory fd and `owned` is valid.
    let raw = unsafe {
        libc::openat(
            dir_fd.as_raw_fd(),
            owned.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600 as libc::mode_t as libc::c_uint,
        )
    };
    if raw < 0 {
        return Err(ServiceError::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: `raw` is a freshly opened owned fd.
    Ok(unsafe { File::from_raw_fd(raw) })
}

/// Unlink relative to the pin; callers surface cleanup failures.
#[cfg(unix)]
fn remove_cancel_temp_at(dir_fd: &File, name: &str) -> std::io::Result<()> {
    let Ok(owned) = std::ffi::CString::new(name) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cancel staging name contains a NUL byte",
        ));
    };
    // SAFETY: dir_fd is an open directory fd and `owned` is valid.
    let rc = unsafe { libc::unlinkat(dir_fd.as_raw_fd(), owned.as_ptr(), 0) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn request_remote_cancel(
    run_dir: &Utf8Path,
    run_id: &str,
    requester: &str,
    limits: JournalLimits,
) -> Result<String, ServiceError> {
    let operation_id = uuid::Uuid::now_v7().to_string();
    request_remote_cancel_with_id(
        run_dir,
        run_id,
        requester,
        &operation_id,
        limits,
        None,
        false,
    )?;
    Ok(operation_id)
}

/// Returns true only for the first publication; retries preserve original attribution.
pub(crate) fn request_remote_cancel_with_id(
    run_dir: &Utf8Path,
    run_id: &str,
    requester: &str,
    operation_id: &str,
    limits: JournalLimits,
    event_sender: Option<tokio::sync::broadcast::Sender<api::RunEvent>>,
    journal_acceptance: bool,
) -> Result<bool, ServiceError> {
    if !uuid::Uuid::parse_str(operation_id).is_ok_and(|id| id.to_string() == operation_id) {
        return Err(ServiceError::Invalid(
            "invalid cancel operation identity".into(),
        ));
    }
    let dir = control_dir(run_dir);
    // Unix pins staging, publication, and retry reads to one directory inode.
    #[cfg(unix)]
    let control_fd = open_control_dir_pinned(&dir)?;
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(&dir)?;
        // create_dir_all follows links: refuse a symlinked mailbox.
        refuse_symlink(&dir, false, || {
            format!("cancel control directory `{dir}` is a symlink; refusing publish")
        })?;
        // Non-Unix pathname races require trusted concurrent writers.
    }
    let mut attempts = 0;
    let (temporary, path, mut file) = loop {
        if attempts >= 3 {
            return Err(ServiceError::Invalid(
                "cancel staging collided repeatedly; refusing to overwrite".into(),
            ));
        }
        attempts += 1;
        let temporary = dir.join(format!(".cancel-{}.tmp", uuid::Uuid::now_v7()));
        let open_result = {
            #[cfg(unix)]
            {
                let tmp_name = temporary
                    .file_name()
                    .ok_or_else(|| ServiceError::Invalid("cancel staging has no file name".into()))?
                    .to_string();
                create_cancel_temp_at(&control_fd, &tmp_name)
            }
            #[cfg(not(unix))]
            {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary);
                // Recheck after open; non-Unix retains a pathname race.
                match file {
                    Ok(file) => match std::fs::symlink_metadata(&temporary) {
                        Ok(meta) if meta.file_type().is_symlink() => {
                            let _ = std::fs::remove_file(&temporary);
                            Err(ServiceError::Invalid(format!(
                                "cancel staging `{temporary}` is a symlink; refusing publish"
                            )))
                        }
                        Ok(_) => Ok(file),
                        Err(error) => {
                            let _ = std::fs::remove_file(&temporary);
                            Err(ServiceError::Io(error))
                        }
                    },
                    Err(error) => Err(ServiceError::Io(error)),
                }
            }
        };
        match open_result {
            Ok(file) => {
                let path = dir.join(format!("cancel-{operation_id}.json"));
                break (temporary, path, file);
            }
            Err(ServiceError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                tracing::warn!(path = %temporary, "cancel staging collision; retrying with a fresh id");
                continue;
            }
            Err(error) => return Err(error),
        }
    };
    let payload = serde_json::json!({
        "op": "cancel",
        "operation_id": operation_id,
        "run_id": run_id,
        "requester": requester,
        "ts": chrono::Utc::now().to_rfc3339(),
    });
    let bytes = serde_json::to_vec(&payload)?;
    // Hold the stage lock through sync and publication to exclude reaping.
    let cleanup_unpublished = |temporary: &Utf8Path| {
        #[cfg(unix)]
        {
            if let Some(name) = temporary.file_name()
                && let Err(cleanup) = remove_cancel_temp_at(&control_fd, name)
                && cleanup.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(path = %temporary, %cleanup, "failed to clean unpublished cancel temp");
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(cleanup) = std::fs::remove_file(temporary) {
                tracing::warn!(path = %temporary, %cleanup, "failed to clean unpublished cancel temp");
            }
        }
    };
    if let Err(error) = file.lock() {
        cleanup_unpublished(&temporary);
        return Err(ServiceError::Io(error));
    }
    // Sync mtime under lock so incomplete staging is never fresh-looking.
    if let Err(error) = file
        .set_modified(std::time::SystemTime::now())
        .and_then(|()| file.sync_all())
    {
        cleanup_unpublished(&temporary);
        return Err(ServiceError::Io(error));
    }
    {
        if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
            cleanup_unpublished(&temporary);
            return Err(ServiceError::Io(error));
        }
    }
    let mut published = None;
    let path_journal = run_meta_dir(run_dir).join("journal.jsonl");
    // Stage outside the journal lock; serialize publication and acceptance with denials.
    let checked = JournalWriter::append_events_with(
        &path_journal,
        run_id,
        limits,
        event_sender,
        |state| {
            if state.confirmations.values().any(|approved| !approved) {
                return Err(JournalError::PreconditionFailed(
                    "accepted denial must be reconciled before cancellation".into(),
                ));
            }
            if journal_acceptance && state.terminal.is_some() {
                return Err(JournalError::PreconditionFailed(
                    "run is already terminal".into(),
                ));
            }
            #[cfg(target_os = "linux")]
            let publication = publish_cancel_control(&control_fd, &temporary, &path, &file);
            #[cfg(not(target_os = "linux"))]
            let publication = publish_cancel_control(&dir, &temporary, &path, &file);
            let mut event_requester = requester.to_owned();
            let valid = match publication {
                Ok(()) => {
                    published = Some(Ok(true));
                    true
                }
                Err(ServiceError::Io(error))
                    if error.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    match matching_cancel_control(
                        #[cfg(not(unix))]
                        &dir,
                        #[cfg(unix)]
                        &control_fd,
                        run_id,
                        operation_id,
                    ) {
                        Ok(Some(original_requester)) => {
                            if !original_requester.is_empty() {
                                event_requester = original_requester;
                            }
                            published = Some(Ok(false));
                            true
                        }
                        Ok(None) => {
                            published = Some(Err(ServiceError::Invalid(
                                "cancel control vanished during retry".into(),
                            )));
                            false
                        }
                        Err(error) => {
                            published = Some(Err(error));
                            false
                        }
                    }
                }
                Err(error) => {
                    published = Some(Err(error));
                    false
                }
            };
            Ok(
                if journal_acceptance && valid && !state.cancel_operations.contains(operation_id) {
                    vec![(
                        "user_cancel_requested",
                        json!({ "run_id": run_id, "operation_id": operation_id, "requester": event_requester }),
                    )]
                } else {
                    Vec::new()
                },
            )
        },
    );
    if let Err(error) = checked {
        cleanup_unpublished(&temporary);
        return Err(match error {
            JournalError::PreconditionFailed(detail) => ServiceError::PreconditionFailed(detail),
            error => ServiceError::Invalid(error.to_string()),
        });
    }
    let newly_published = published.expect("publication selected under journal lock")?;
    if !newly_published {
        return Ok(false);
    }
    // Directory-sync failure cannot revoke an already published control.
    if let Err(error) = sync_dir_entry(&dir) {
        tracing::warn!(path = %path, %error, "cancel control published but directory sync failed");
    }
    Ok(true)
}

/// Bounded mailbox identity check; Unix pins the directory, unlike non-Unix.
fn matching_cancel_control(
    #[cfg(not(unix))] dir: &Utf8Path,
    #[cfg(unix)] control_fd: &File,
    run_id: &str,
    operation_id: &str,
) -> Result<Option<String>, ServiceError> {
    if operation_id.is_empty() || operation_id.contains(['/', '\\', '\0']) {
        return Err(ServiceError::Invalid(
            "invalid cancel operation identity".into(),
        ));
    }
    let name = format!("cancel-{operation_id}.json");
    #[cfg(unix)]
    let existing = {
        let name = std::ffi::CString::new(name).unwrap();
        // SAFETY: pinned directory and generated single-leaf name.
        let fd = unsafe {
            libc::openat(
                control_fd.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(ServiceError::Io(error));
        }
        // SAFETY: fd is a newly opened owned descriptor.
        unsafe { File::from_raw_fd(fd) }
    };
    #[cfg(not(unix))]
    let existing = {
        let path = dir.join(name);
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ServiceError::Io(error)),
        };
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(ServiceError::Invalid("invalid cancel control file".into()));
        }
        let file = File::open(&path)?;
        if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err(ServiceError::Invalid("cancel control is a symlink".into()));
        }
        file
    };
    use std::io::Read as _;
    let metadata = existing.metadata()?;
    if !metadata.is_file() || metadata.len() > 64 * 1024 {
        return Err(ServiceError::Invalid(
            "invalid existing cancel control".into(),
        ));
    }
    let mut bytes = Vec::new();
    existing.take(64 * 1024 + 1).read_to_end(&mut bytes)?;
    let control: Value = serde_json::from_slice(&bytes)?;
    if bytes.len() > 64 * 1024
        || control["op"] != "cancel"
        || control["run_id"] != run_id
        || control["operation_id"] != operation_id
    {
        return Err(ServiceError::Invalid(
            "cancel control identity mismatch".into(),
        ));
    }
    Ok(Some(
        control["requester"].as_str().unwrap_or_default().to_owned(),
    ))
}

/// No-overwrite publication holds the stage lock across rename/link. Payload
/// sync precedes publication; directory sync follows. Failures remove the temp;
/// AlreadyExists requires identity validation, never overwrite.
#[cfg(target_os = "linux")]
fn publish_cancel_control(
    dir_fd: &File,
    temporary: &Utf8Path,
    path: &Utf8Path,
    _stage_lock: &File,
) -> Result<(), ServiceError> {
    {
        // Only leaf names may be resolved relative to the pinned fd.
        let Some(tmp_name) = temporary.file_name() else {
            let _ = remove_cancel_temp_at(dir_fd, "");
            return Err(ServiceError::Invalid(
                "cancel staging has no file name".into(),
            ));
        };
        let tmp = std::ffi::CString::new(tmp_name.as_bytes())
            .map_err(|_| ServiceError::Invalid("cancel control path contains a NUL byte".into()))?;
        let Some(dst_name) = path.file_name() else {
            let _ = remove_cancel_temp_at(dir_fd, tmp_name);
            return Err(ServiceError::Invalid(
                "cancel control has no file name".into(),
            ));
        };
        let dst = std::ffi::CString::new(dst_name.as_bytes())
            .map_err(|_| ServiceError::Invalid("cancel control path contains a NUL byte".into()))?;
        // SAFETY: fds and CStrings are valid for the call.
        let rc = unsafe {
            libc::renameat2(
                dir_fd.as_raw_fd(),
                tmp.as_ptr(),
                dir_fd.as_raw_fd(),
                dst.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if rc != 0 {
            let error = std::io::Error::last_os_error();
            let _ = remove_cancel_temp_at(dir_fd, tmp_name);
            if error.kind() == std::io::ErrorKind::AlreadyExists
                || error.raw_os_error() == Some(libc::EEXIST)
            {
                return Err(ServiceError::Io(std::io::ErrorKind::AlreadyExists.into()));
            }
            return Err(ServiceError::Io(error));
        }
        Ok(())
    }
}
#[cfg(not(target_os = "linux"))]
fn publish_cancel_control(
    _dir: &Utf8Path,
    temporary: &Utf8Path,
    path: &Utf8Path,
    _stage_lock: &File,
) -> Result<(), ServiceError> {
    {
        // Portable no-overwrite publication: link then unlink the staging name.
        match std::fs::symlink_metadata(temporary) {
            Ok(meta) if meta.file_type().is_symlink() => {
                let _ = std::fs::remove_file(temporary);
                return Err(ServiceError::Invalid(format!(
                    "cancel staging `{temporary}` is a symlink; refusing publish"
                )));
            }
            Ok(_) => {}
            Err(error) => {
                let _ = std::fs::remove_file(temporary);
                return Err(ServiceError::Io(error));
            }
        }
        // Probe for diagnostics; the no-overwrite link remains authoritative.
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                if let Err(cleanup) = std::fs::remove_file(temporary) {
                    tracing::warn!(path = %temporary, %cleanup, "failed to clean cancel staging after overwrite refusal");
                }
                return Err(ServiceError::Io(std::io::ErrorKind::AlreadyExists.into()));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                if let Err(cleanup) = std::fs::remove_file(temporary) {
                    tracing::warn!(path = %temporary, %cleanup, "failed to clean cancel staging after probe failure");
                }
                return Err(ServiceError::Io(error));
            }
        }
        match std::fs::hard_link(temporary, path) {
            Ok(()) => {
                // Already published: failed staging cleanup must not refuse it.
                if let Err(cleanup) = std::fs::remove_file(temporary) {
                    tracing::warn!(path = %temporary, %cleanup, "published cancel control but failed to unlink staging temp");
                }
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Err(cleanup) = std::fs::remove_file(temporary) {
                    tracing::warn!(path = %temporary, %cleanup, "failed to clean cancel staging after overwrite refusal");
                }
                Err(ServiceError::Io(std::io::ErrorKind::AlreadyExists.into()))
            }
            Err(error) => {
                if let Err(cleanup) = std::fs::remove_file(temporary) {
                    tracing::warn!(path = %temporary, %cleanup, "failed to clean unpublished cancel temp after link failure");
                }
                Err(ServiceError::Io(error))
            }
        }
    }
}

/// Flush a directory entry after publication. Unix pins O_DIRECTORY |
/// O_NOFOLLOW; non-Unix is a no-op (weaker power-loss durability). Non-Unix
/// mailbox operations require trusted writers because pathname races remain.
/// Shared with idempotency storage to keep platform behavior in one place.
#[cfg(unix)]
pub fn sync_dir_entry(dir: &Utf8Path) -> std::io::Result<()> {
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(dir)?
        .sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
pub fn sync_dir_entry(_dir: &Utf8Path) -> std::io::Result<()> {
    Ok(())
}

/// Reap temps only when stale AND proven idle: the age check handles crashed
/// publishers and the lock probe protects live ones even under clock skew.
/// Symlink metadata avoids following links; the drain shares this predicate.
fn cancel_temp_is_stale(meta: &std::fs::Metadata, now: std::time::SystemTime) -> bool {
    match meta.modified() {
        Ok(modified) => now
            .duration_since(modified)
            .ok()
            .is_some_and(|age| age >= CANCEL_TMP_TTL),
        // An unknown mtime fails closed toward reclamation: it counts as
        // stale, but removal still requires a proven-unlocked probe below,
        // and a temp whose liveness cannot be determined is never deleted
        // (E01).
        Err(_) => true,
    }
}

/// Liveness of one cancel temp: live (lock-held by a publisher), proven
/// idle, or undeterminable. Only a proven-idle temp may be reaped; any lock
/// or open *error* (as opposed to clean contention or clean acquisition)
/// yields `Unknown` and the temp is skipped, never reaped and never
/// declared live (E01).
enum CancelTempLiveness {
    Live,
    Reapable,
    Unknown,
}

/// Lock probe shared by the standalone sweep and the drain-path inline
/// reap: a live publisher holds the exclusive lock for its whole stage,
/// including publication (E01).
fn probe_cancel_temp_liveness(path: &std::path::Path) -> CancelTempLiveness {
    #[cfg(unix)]
    {
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
        {
            Ok(file) => file,
            // A vanished temp races a concurrent drain; there is nothing to
            // prove, so skip instead of reaping or declaring (E01).
            // Any other open error (permissions, I/O) is undeterminable:
            // skip, never declare (E01).
            Err(_) => return CancelTempLiveness::Unknown,
        };
        match file.try_lock() {
            // Acquired: no publisher holds it. The guard drops here and
            // releases immediately; the temp is stale by the caller's age
            // gate, so no live publisher can be staging it.
            Ok(()) => CancelTempLiveness::Reapable,
            Err(std::fs::TryLockError::WouldBlock) => CancelTempLiveness::Live,
            // A lock *error* (as opposed to contention) is undeterminable:
            // skip so neither a live publisher is reaped nor a dead temp
            // is declared live (E01).
            Err(std::fs::TryLockError::Error(_)) => CancelTempLiveness::Unknown,
        }
    }
    // Non-Unix has no O_NOFOLLOW: re-probe symlink_metadata after the
    // open decision window and treat any link as foreign. A residual
    // pathname race remains; untrusted concurrent writers are
    // unsupported on non-Unix (E01).
    #[cfg(not(unix))]
    {
        if std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink()) {
            return CancelTempLiveness::Unknown;
        }
        let file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(_) => return CancelTempLiveness::Unknown,
        };
        if std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink()) {
            return CancelTempLiveness::Unknown;
        }
        match file.try_lock() {
            Ok(()) => CancelTempLiveness::Reapable,
            Err(std::fs::TryLockError::WouldBlock) => CancelTempLiveness::Live,
            Err(std::fs::TryLockError::Error(_)) => CancelTempLiveness::Unknown,
        }
    }
}

/// Byte-exact cancel-temp name match shared by the standalone sweep and the
/// drain-path inline reap (E01). On Unix the match runs on raw bytes so a
/// non-UTF8 temp name is classified exactly like a UTF8 one instead of
/// being skipped by a lossy conversion; on non-Unix only valid UTF-8 names
/// can match. Both paths converge through this one predicate.
fn is_cancel_temp_name(name: &std::ffi::OsStr) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        let bytes = name.as_bytes();
        bytes.starts_with(b".cancel-") && bytes.ends_with(b".tmp")
    }
    #[cfg(not(unix))]
    {
        name.to_str()
            .is_some_and(|name| name.starts_with(".cancel-") && name.ends_with(".tmp"))
    }
}

/// The ONE shared temp-reap implementation used by both the test-only
/// standalone sweep and the drain-path inline reclamation (E01): identical
/// symlink handling (remove the link itself, never follow), identical
/// non-file handling (remove via `remove_dir`/`remove_file` after the
/// symlink check), and identical stale-plus-proven-idle gating. `meta` is
/// the single `symlink_metadata` probe the caller already took: no second
/// stat runs on the same path.
fn reap_cancel_temp_entry(path: &std::path::Path, meta: &std::fs::Metadata) {
    // Never follow a symlink: remove the link itself without opening its
    // target, so a lock probe can never reach foreign bytes (E01).
    if meta.file_type().is_symlink() {
        if let Err(error) = std::fs::remove_file(path) {
            tracing::warn!(path = %path.display(), %error, "failed to reap symlinked cancel temp");
        }
        return;
    }
    if !meta.is_file() {
        // A directory (or other non-file) squatting on a temp name can
        // never become a control: remove it so it neither leaks nor blocks
        // a future temp, and report failures (E01).
        tracing::warn!(path = %path.display(), "removing non-file cancel temp");
        let removed = if meta.is_dir() {
            std::fs::remove_dir(path)
        } else {
            std::fs::remove_file(path)
        };
        if let Err(error) = removed {
            tracing::warn!(path = %path.display(), %error, "failed to reap non-file cancel temp");
        }
        return;
    }
    if !cancel_temp_is_stale(meta, std::time::SystemTime::now()) {
        return;
    }
    match probe_cancel_temp_liveness(path) {
        CancelTempLiveness::Live | CancelTempLiveness::Unknown => {}
        CancelTempLiveness::Reapable => {
            if let Err(error) = std::fs::remove_file(path) {
                tracing::warn!(path = %path.display(), %error, "failed to reap stale cancel temp");
            }
        }
    }
}

/// Shared symlink-safe removal for foreign (non-temp) controls: the same
/// no-follow rule as the temp core above, so every mailbox removal
/// converges through one no-follow discipline (E01). Symlinks are removed
/// as links, directories via `remove_dir`, files via `remove_file`; failures
/// warn with the same shape as the temp core.
fn reap_foreign_control_entry(path: &std::path::Path, meta: &std::fs::Metadata, reason: &str) {
    tracing::warn!(path = %path.display(), "{reason}");
    let removed = if meta.file_type().is_symlink() {
        std::fs::remove_file(path)
    } else if meta.is_dir() {
        std::fs::remove_dir(path)
    } else {
        std::fs::remove_file(path)
    };
    if let Err(error) = removed {
        tracing::warn!(path = %path.display(), %error, "failed to remove foreign cancel control");
    }
}

/// Well-formed pending cancel controls as `(operation_id, descriptor)`.
/// Malformed or unparseable files are reaped with a warning instead of
/// leaking forever; they can never journal. Oversize removals only affect
/// foreign damage: legitimate controls are a few hundred bytes against a
/// 64 KiB bound, so no legitimate request is lost by the cap (E01).
/// Content/name mismatches are planted damage: a writer with control-dir
/// write permission can already unlink directly, so reaping grants no new
/// destructive capability (E01). Scan and read failures are
/// errors, never an empty mailbox: callers must not mistake a failed scan
/// for "no cancel requested" (A02).
pub(crate) fn list_pending_cancel_controls(
    run_dir: &Utf8Path,
) -> Result<Vec<(String, Value)>, ServiceError> {
    let dir = control_dir(run_dir);
    // One scan reaps abandoned staging temps and lists controls. Refuse a
    // symlinked directory, matching no-follow publication/consumption.
    match std::fs::symlink_metadata(&dir) {
        Ok(meta) if meta.file_type().is_symlink() => {
            tracing::warn!(dir = %dir, "cancel control directory is a symlink; failing scan");
            return Err(ServiceError::Invalid(format!(
                "cancel control directory `{dir}` is a symlink; refusing scan"
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            tracing::warn!(dir = %dir, %error, "cancel control scan failed");
            return Err(ServiceError::Io(error));
        }
    }
    let unreadable = |path: &std::path::Path, error: std::io::Error| {
        tracing::warn!(path = %path.display(), %error, "cancel control unreadable; failing scan");
        ServiceError::Io(error)
    };
    let mut pending = Vec::new();
    // Bind controls to the containing run directory, never an empty fallback.
    let Some(expected_run_id) = run_dir.file_name() else {
        return Err(ServiceError::Invalid(format!(
            "run directory `{run_dir}` has no file name; refusing cancel scan"
        )));
    };
    let expected_run_id = expected_run_id.to_string();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(pending),
        Err(error) => {
            tracing::warn!(dir = %dir, %error, "cancel control scan failed");
            return Err(ServiceError::Io(error));
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                // A directory iteration error hides entries; fail the scan
                // so the caller retries instead of draining a partial view.
                tracing::warn!(dir = %dir, %error, "cancel control scan failed");
                return Err(ServiceError::Io(error));
            }
        };
        let path = entry.path();
        // Inspect without following links; disappearance is a racing drain,
        // while other inspection errors fail the entire scan closed.
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(unreadable(&path, error)),
        };
        // Classify temps byte-exactly before UTF-8 conversion. All scan cleanup
        // uses the shared reapers and their symlink/age/lock discipline.
        if is_cancel_temp_name(entry.file_name().as_os_str()) {
            // Inline stale-temp reap through the ONE shared core also used
            // by the standalone sweep, without a second directory scan
            // (E01). Temps are never honored, however complete they look:
            // only a published rename is a request, so a torn write can
            // never be listed.
            reap_cancel_temp_entry(&path, &meta);
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            // Non-UTF8 non-temp names can never be a legitimate control
            // (our names are UUID hex): reap through the shared foreign
            // core so one planted file cannot wedge the mailbox, and never
            // honor it (E01).
            reap_foreign_control_entry(&path, &meta, "removing non-UTF8 cancel control name");
            continue;
        };
        if meta.file_type().is_symlink() {
            reap_foreign_control_entry(&path, &meta, "removing symlinked cancel control");
            continue;
        }
        if !meta.is_file() {
            // Non-file entries under a control name can never journal and
            // would otherwise linger unreported: remove and report (E01).
            reap_foreign_control_entry(&path, &meta, "removing non-file cancel control");
            continue;
        }
        if !name.starts_with("cancel-") || !name.ends_with(".json") {
            // Unknown names are left for forward compatibility: a future
            // control kind must not be reaped as damage. Proven-malicious
            // shapes (symlinks, non-UTF8, non-files, oversize, unparseable,
            // misnamed) are reaped above; unknown regular names stay.
            continue;
        }
        // Read errors fail the scan: the file is retained and the next
        // drain retries. Only proven-unparseable content is reaped, since
        // it can never journal. Reads are bounded: our own controls are a
        // few hundred bytes, so anything larger is foreign damage, not a
        // cancel request.
        const MAX_CONTROL_FILE_BYTES: u64 = 64 * 1024;
        let mut bytes = Vec::new();
        // Opened `O_NOFOLLOW` on Unix so a symlink swapped in after the
        // metadata probe above cannot redirect the read outside the
        // control directory. The name/content binding below remains the
        // real defense: only a control whose file name, `op`, `run_id`,
        // and `operation_id` all agree is honored (E01). Non-Unix has no
        // `O_NOFOLLOW`: re-probe `symlink_metadata` after open and fail
        // closed on any link; a residual pathname race remains and
        // untrusted concurrent writers are unsupported there (E01).
        #[cfg(unix)]
        let open_result = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path);
        #[cfg(not(unix))]
        let open_result = std::fs::File::open(&path);
        match open_result {
            Ok(file) => {
                #[cfg(not(unix))]
                {
                    match std::fs::symlink_metadata(&path) {
                        Ok(meta) if meta.file_type().is_symlink() => {
                            tracing::warn!(path = %path.display(), "cancel control became a symlink during open; failing scan");
                            return Err(ServiceError::Invalid(format!(
                                "cancel control `{}` changed to a symlink during scan",
                                path.display()
                            )));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            // Consumed by a racing drain between the probe
                            // and the open: skip, not a scan failure (E01).
                            continue;
                        }
                        Err(error) => {
                            tracing::warn!(path = %path.display(), %error, "cancel control unreadable; failing scan");
                            return Err(ServiceError::Io(error));
                        }
                        _ => {}
                    }
                }
                use std::io::Read as _;
                if let Err(error) = file
                    .take(MAX_CONTROL_FILE_BYTES.saturating_add(1))
                    .read_to_end(&mut bytes)
                {
                    // A control consumed by a racing drain reads as gone:
                    // skip instead of failing the whole mailbox (E01). Any
                    // other read failure still fails the scan so a
                    // partially observed mailbox is never mistaken for
                    // clean.
                    if error.kind() == std::io::ErrorKind::NotFound {
                        continue;
                    }
                    return Err(unreadable(&path, error));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A control consumed by a racing drain between the metadata
                // probe and the open is gone: skip it instead of failing the
                // whole mailbox (E01).
                continue;
            }
            Err(error) => return Err(unreadable(&path, error)),
        }
        if bytes.len() as u64 > MAX_CONTROL_FILE_BYTES {
            reap_foreign_control_entry(&path, &meta, "removing oversize cancel control");
            continue;
        }
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "removing unparseable cancel control");
                // Reap through the shared foreign core (no-follow): an
                // unparseable file can never journal, so it is foreign
                // damage by construction (E01). Re-stat without a stale
                // fallback: a racing drain that consumed the file reads as
                // gone and skips, faithful to the observation (E01). Any
                // other re-stat failure fails the scan like every other
                // unreadable control above.
                let reap_meta = match std::fs::symlink_metadata(&path) {
                    Ok(reap_meta) => reap_meta,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(unreadable(&path, error)),
                };
                reap_foreign_control_entry(
                    &path,
                    &reap_meta,
                    "removing unparseable cancel control body",
                );
                continue;
            }
        };
        // A control is only honored for the run directory that holds
        // it: a foreign `op` or `run_id` is planted damage, never a
        // cancellation (E01).
        let valid = matches!(value.get("op").and_then(Value::as_str), Some("cancel"))
            && value.get("run_id").and_then(Value::as_str) == Some(expected_run_id.as_str());
        match value.get("operation_id").and_then(Value::as_str) {
            Some(operation_id) if !operation_id.is_empty() && valid => {
                // The file name must name the operation it carries: a
                // `cancel-evil.json` holding `operation_id=legit` is
                // planted damage that must neither consume `legit` nor
                // journal a cancel (E01).
                let expected_name = format!("cancel-{operation_id}.json");
                if name != expected_name {
                    reap_foreign_control_entry(&path, &meta, "removing misnamed cancel control");
                    continue;
                }
                pending.push((operation_id.to_string(), value));
            }
            _ => {
                reap_foreign_control_entry(
                    &path,
                    &meta,
                    "removing foreign or malformed cancel control",
                );
            }
        }
    }
    // Operation ids are UUIDv7, so lexicographic order approximates
    // publication order but is not exact within the same millisecond, when
    // only the random tail differs. Draining oldest-first keeps the journal
    // causally ordered well enough without trusting file mtimes.
    pending.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(pending)
}

/// Removes only matching evidence; malformed or unrelated controls stay put.
/// Unix reads and unlinks use one no-follow directory pin. Service publishers
/// never overwrite controls, so racing drains/retries are idempotent. Success
/// covers process crashes, not power-loss durability after sync failure.
pub(crate) fn consume_cancel_control(run_dir: &Utf8Path, operation_id: &str) -> bool {
    let dir = control_dir(run_dir);
    let result = (|| -> Result<(), ServiceError> {
        #[cfg(unix)]
        let control_fd = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&dir)?;
        #[cfg(not(unix))]
        if std::fs::symlink_metadata(&dir)?.file_type().is_symlink() {
            return Err(ServiceError::Invalid(
                "cancel control directory is a symlink".into(),
            ));
        }
        let run_id = run_dir
            .file_name()
            .ok_or_else(|| ServiceError::Invalid("run directory must have a run id".into()))?;
        if matching_cancel_control(
            #[cfg(not(unix))]
            &dir,
            #[cfg(unix)]
            &control_fd,
            run_id,
            operation_id,
        )?
        .is_none()
        {
            return Ok(());
        }
        #[cfg(unix)]
        remove_cancel_temp_at(&control_fd, &format!("cancel-{operation_id}.json"))?;
        #[cfg(not(unix))]
        std::fs::remove_file(dir.join(format!("cancel-{operation_id}.json")))?;
        #[cfg(unix)]
        let sync = control_fd.sync_all();
        #[cfg(not(unix))]
        let sync = sync_dir_entry(&dir);
        if let Err(error) = sync {
            tracing::warn!(path = %dir, %error, "consumed cancel control but directory sync failed");
        }
        Ok(())
    })();
    match result {
        Ok(()) => true,
        Err(ServiceError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            tracing::warn!(path = %dir, %error, "cancel control retained: consumption failed");
            false
        }
    }
}

pub(crate) fn has_pending_cancel_control(run_dir: &Utf8Path) -> bool {
    match list_pending_cancel_controls(run_dir) {
        Ok(pending) => !pending.is_empty(),
        // Fail closed: an unscannable mailbox may hold a cancel request.
        Err(error) => {
            tracing::warn!(run_dir = %run_dir, %error, "cancel control scan failed; assuming pending");
            true
        }
    }
}

/// Direct and service executions use the same lease, including retention and
/// deletion. Admission fences creation against removal of the lock directory.
pub(crate) fn lock_direct_run(metadata_dir: &Utf8Path) -> Result<RunExecutionLock, ServiceError> {
    let run_dir = metadata_dir.parent().expect("direct metadata has a parent");
    try_lock_run_execution(run_dir)?.ok_or_else(|| {
        ServiceError::Invalid(format!(
            "output metadata `{metadata_dir}` is already active in another qcg run"
        ))
    })
}

pub fn direct_run_meta_dir(workspace: &Utf8Path) -> Utf8PathBuf {
    let run_id = direct_run_id(workspace);
    workspace
        .parent()
        .unwrap_or_else(|| Utf8Path::new("."))
        .join(".data/runs")
        .join(&run_id)
        .join("meta")
}

/// Best-effort warning when a direct execution shares its runs directory
/// with a live server process. Cross-process runs coordinate only through
/// provider quotas; the server's `max_active_runs` does not apply here.
pub(crate) fn warn_if_shared_runs_dir_owned(workspace: &Utf8Path) {
    let lock_path = workspace
        .parent()
        .unwrap_or_else(|| Utf8Path::new("."))
        .join(".data/runs/.service.lock");
    let lock_file = match OpenOptions::new().read(true).write(true).open(&lock_path) {
        Ok(lock_file) => lock_file,
        Err(_) => return,
    };
    match lock_file.try_lock() {
        Ok(_) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            tracing::warn!(
                workspace = %workspace,
                "runs directory is owned by a running server; direct execution bypasses its max_active_runs limit"
            );
        }
        Err(std::fs::TryLockError::Error(_)) => {}
    }
}

pub(crate) fn app_registry(runtime: Arc<llm::LlmRuntime>) -> engine::StepRegistry {
    let mut registry = steps::deterministic_registry_with_mcp(Arc::new(runtime.mcp.clone()));
    llm_steps::register_llm_steps(&mut registry, runtime);
    registry
}

/// Loads the LLM runtime for step registry assembly, falling back to
/// built-in capabilities when no providers registry is found. This is the
/// single choke point for registry assembly; binaries resolve the registry
/// through here instead of composing runtimes directly.
pub fn load_llm_runtime(
    providers_path: Option<&Utf8Path>,
) -> Result<Arc<llm::LlmRuntime>, ServiceError> {
    load_llm_runtime_with_container_timeouts(providers_path, policy::ContainerTimeouts::default())
}

pub(crate) fn load_llm_runtime_with_container_timeouts(
    providers_path: Option<&Utf8Path>,
    timeouts: policy::ContainerTimeouts,
) -> Result<Arc<llm::LlmRuntime>, ServiceError> {
    let mut runtime = match llm::LlmRouter::load_optional(providers_path) {
        Ok(Some(router)) => router.into_runtime(),
        Ok(None) => llm::LlmRuntime::builtins(),
        Err(error) => return Err(ServiceError::Invalid(error.to_string())),
    };
    runtime.mcp = runtime.mcp.with_container_timeouts(timeouts);
    Ok(Arc::new(runtime))
}

/// Assembles the full application step registry, resolving the LLM runtime
/// from an explicit providers path or the standard search locations.
pub fn app_registry_with_providers(
    providers_path: Option<&Utf8Path>,
) -> Result<engine::StepRegistry, ServiceError> {
    Ok(app_registry(load_llm_runtime(providers_path)?))
}

pub fn built_in_step_param_schemas() -> BTreeMap<String, Value> {
    let mut registry = deterministic_registry();
    llm_steps::register_fake_llm_steps(&mut registry);
    registry.params_schemas()
}

pub fn step_param_schemas_markdown() -> Result<String, ServiceError> {
    let mut markdown = String::new();
    for (step_type, schema) in built_in_step_param_schemas() {
        markdown.push_str("### `");
        markdown.push_str(&step_type);
        markdown.push_str("`\n\n```json\n");
        markdown.push_str(&serde_json::to_string_pretty(&schema)?);
        markdown.push_str("\n```\n\n");
    }
    Ok(markdown)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04). A removal failure warns instead of being silently
    /// ignored (E01).
    struct TempGuard(Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(self.0.as_std_path()) {
                tracing::warn!(path = %self.0, %error, "test temp cleanup failed");
            }
        }
    }

    fn temp_root(name: &str) -> Utf8PathBuf {
        let root = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("run-dirs-{name}-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        std::fs::create_dir_all(root.as_std_path()).expect("temp root should be created");
        root
    }

    #[test]
    fn handoff_shards_are_bounded_independent_and_explicitly_released() {
        let root = temp_root("handoff-shards");
        let _temp = TempGuard(root.clone());
        let run_dir = root.join("victim");
        let handoff = try_lock_run_handoff(&run_dir).unwrap().unwrap();
        let inherited = handoff.0.try_clone().unwrap();
        assert!(try_lock_run_handoff(&run_dir).unwrap().is_none());
        let _admission = try_lock_run_admission(&run_dir).unwrap().unwrap();
        assert!(
            !run_dir.exists(),
            "coordination must not create run metadata"
        );
        drop(handoff);
        let next = try_lock_run_handoff(&run_dir).unwrap().unwrap();
        drop(inherited);
        assert!(try_lock_run_handoff(&run_dir).unwrap().is_none());
        drop(next);
        for id in 0..1000 {
            drop(try_lock_run_handoff(&root.join(format!("run-{id}"))).unwrap());
        }
        assert!(
            std::fs::read_dir(root.join(".handoff-shards"))
                .unwrap()
                .count()
                <= RUN_COORDINATION_SHARD_COUNT
        );
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn handoff_wait_is_shutdown_cancellable() {
        let root = temp_root("handoff-shutdown");
        let _temp = TempGuard(root.clone());
        let run_dir = root.join("victim");
        let _held = try_lock_run_handoff(&run_dir).unwrap().unwrap();
        let shutdown = CancellationToken::new();
        let wait = lock_run_handoff(&run_dir, &shutdown);
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut wait)
                .await
                .is_err()
        );
        shutdown.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), wait)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
    }

    #[cfg(unix)]
    #[test]
    fn execution_lease_release_does_not_wait_for_inherited_descriptors() {
        let root = temp_root("execution-lease-inheritance");
        let lease = try_lock_run_execution(&root).unwrap().unwrap();
        // dup shares the open file description just as fork does before exec
        // closes CLOEXEC descriptors in a concurrently spawning command.
        let inherited = lease.0.try_clone().unwrap();
        assert!(try_lock_run_execution(&root).unwrap().is_none());
        drop(lease);
        let next = try_lock_run_execution(&root).unwrap();
        assert!(
            next.is_some(),
            "a finished execution owner must release its lease even while a fork/exec retains a descriptor"
        );
        drop(inherited);
        assert!(
            try_lock_run_execution(&root).unwrap().is_none(),
            "closing an inherited descriptor must not release the next owner's lease"
        );
        drop(next);
        assert!(try_lock_run_execution(&root).unwrap().is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn execution_lease_waiter_retries_transient_contention() {
        let root = temp_root("execution-lease-contention");
        let held = try_lock_run_execution(&root).unwrap().unwrap();
        let waiter_root = root.clone();
        let shutdown = CancellationToken::new();
        let waiter_shutdown = shutdown.clone();
        let mut waiter = tokio::spawn(async move {
            lock_run_execution(&waiter_root, &waiter_shutdown)
                .await
                .unwrap()
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut waiter)
                .await
                .is_err(),
            "the waiter must not claim a lease while another owner holds it"
        );
        drop(held);
        let acquired = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("the waiter should acquire after the prior owner releases")
            .expect("the waiter task should complete");
        assert!(acquired.is_some());
        drop(acquired);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn acquired_execution_lease_remains_held_after_shutdown() {
        let root = temp_root("execution-lease-acquired-shutdown");
        let shutdown = CancellationToken::new();
        let lease = lock_run_execution(&root, &shutdown)
            .await
            .unwrap()
            .expect("a live service should acquire a free lease");
        shutdown.cancel();
        assert!(
            try_lock_run_execution(&root).unwrap().is_none(),
            "shutdown cannot release a lease after ownership has transferred to its caller"
        );
        drop(lease);
        drop(try_lock_run_execution(&root).unwrap().unwrap());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn execution_lease_waiter_stops_on_shutdown() {
        let root = temp_root("execution-lease-shutdown");
        let held = try_lock_run_execution(&root).unwrap().unwrap();
        let shutdown = CancellationToken::new();
        let waiter_root = root.clone();
        let waiter_shutdown = shutdown.clone();
        let mut waiter = tokio::spawn(async move {
            lock_run_execution(&waiter_root, &waiter_shutdown)
                .await
                .unwrap()
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut waiter)
                .await
                .is_err(),
            "the waiter must remain blocked while another owner holds the lease"
        );
        shutdown.cancel();
        let stopped = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("shutdown must cancel the lease wait")
            .expect("the waiter task should complete");
        assert!(stopped.is_none());
        assert!(try_lock_run_execution(&root).unwrap().is_none());
        drop(held);
        drop(try_lock_run_execution(&root).unwrap().unwrap());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn maintenance_lease_release_does_not_wait_for_inherited_descriptors() {
        let root = temp_root("maintenance-lease-inheritance");
        let lease = try_lock_store_maintenance(&root).unwrap().unwrap();
        // dup shares the open file description just as fork does before exec
        // closes CLOEXEC descriptors in a concurrently spawning command.
        let inherited = lease.0.try_clone().unwrap();
        assert!(try_lock_store_maintenance(&root).unwrap().is_none());
        drop(lease);
        let next = try_lock_store_maintenance(&root).unwrap();
        assert!(
            next.is_some(),
            "a finished maintenance owner must release its lease while an inherited descriptor remains open"
        );
        drop(inherited);
        assert!(
            try_lock_store_maintenance(&root).unwrap().is_none(),
            "closing an inherited descriptor must not release the next owner's lease"
        );
        drop(next);
        assert!(try_lock_store_maintenance(&root).unwrap().is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn service_store_lock_release_does_not_wait_for_inherited_descriptors() {
        let root = temp_root("service-store-lock-inheritance");
        let _temp_guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let owner = lock_runs_directory(&runs).unwrap();
        // dup shares the open file description just as fork does before exec
        // closes CLOEXEC descriptors in a concurrently spawning command.
        let inherited = owner.0.try_clone().unwrap();
        assert!(lock_runs_directory_shared(&runs).is_err());
        drop(owner);
        let next = lock_runs_directory_shared(&runs).unwrap();
        drop(inherited);
        assert!(lock_runs_directory(&runs).is_err());
        drop(next);
        lock_runs_directory(&runs).unwrap();
    }

    #[test]
    fn store_lock_modes_are_mutually_exclusive() {
        // E12c: Exclusive and SharedFilesystem participate in the same
        // lock: shared peers coexist, but any Exclusive contender is
        // refused while a shared peer holds the store and vice versa.
        let root = temp_root("store-lock");
        let _temp_guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let shared_a = lock_runs_directory_shared(&runs).expect("first shared lock");
        let _shared_b = lock_runs_directory_shared(&runs).expect("shared peers coexist");
        assert!(
            lock_runs_directory(&runs).is_err(),
            "exclusive must be refused while shared peers hold the store"
        );
        drop(shared_a);
        drop(_shared_b);
        let exclusive = lock_runs_directory(&runs).expect("exclusive after release");
        assert!(
            lock_runs_directory_shared(&runs).is_err(),
            "shared must be refused while exclusive holds the store"
        );
        drop(exclusive);
        lock_runs_directory_shared(&runs).expect("shared after release");
    }

    #[test]
    fn stable_cancel_mailbox_reuses_exact_identity_and_refuses_damage() {
        let root = temp_root("stable-cancel");
        let _guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        std::fs::create_dir_all(run_meta_dir(&run_dir)).unwrap();
        std::fs::write(run_meta_dir(&run_dir).join("journal.jsonl"), b"").unwrap();
        let operation = uuid::Uuid::now_v7().to_string();
        let path = control_dir(&run_dir).join(format!("cancel-{operation}.json"));
        assert!(
            request_remote_cancel_with_id(
                &run_dir,
                "run-1",
                "original",
                &operation,
                engine::JournalLimits::default(),
                None,
                false
            )
            .unwrap()
        );
        let original = std::fs::read(&path).unwrap();
        assert!(
            !request_remote_cancel_with_id(
                &run_dir,
                "run-1",
                "retry",
                &operation,
                engine::JournalLimits::default(),
                None,
                true
            )
            .unwrap()
        );
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let journal =
            std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl")).unwrap();
        let events: Vec<Value> = journal
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["requester"], "original");
        for field in ["op", "run_id", "operation_id"] {
            let mut damaged: Value = serde_json::from_slice(&original).unwrap();
            damaged[field] = "mismatch".into();
            let bytes = serde_json::to_vec(&damaged).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            assert!(
                request_remote_cancel_with_id(
                    &run_dir,
                    "run-1",
                    "retry",
                    &operation,
                    engine::JournalLimits::default(),
                    None,
                    false
                )
                .is_err()
            );
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        std::fs::write(&path, b"{").unwrap();
        assert!(
            request_remote_cancel_with_id(
                &run_dir,
                "run-1",
                "retry",
                &operation,
                engine::JournalLimits::default(),
                None,
                false
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"{");
        #[cfg(unix)]
        {
            std::fs::remove_file(&path).unwrap();
            let target = root.join("target");
            std::fs::write(&target, &original).unwrap();
            std::os::unix::fs::symlink(&target, &path).unwrap();
            assert!(
                request_remote_cancel_with_id(
                    &run_dir,
                    "run-1",
                    "retry",
                    &operation,
                    engine::JournalLimits::default(),
                    None,
                    false
                )
                .is_err()
            );
            assert_eq!(std::fs::read(&target).unwrap(), original);
        }
        assert!(
            request_remote_cancel_with_id(
                &run_dir,
                "run-1",
                "retry",
                "../bad",
                engine::JournalLimits::default(),
                None,
                false
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read_dir(control_dir(&run_dir)).unwrap().count(),
            1,
            "fresh staging temps must not leak on refusal or reuse"
        );
    }

    #[test]
    fn unconfirmed_cancel_temps_are_not_scanned_or_reaped() {
        // E01: a publisher stopped before the rename leaves only a temp
        // that the scanner must ignore instead of treating as corruption.
        let root = temp_root("cancel-temp");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("control dir should be created");
        let temporary = dir.join(".cancel-11111111-1111-7111-8111-111111111111.tmp");
        std::fs::write(temporary.as_std_path(), b"{\"op\":\"cancel\"")
            .expect("partial temp should be written");
        let pending = list_pending_cancel_controls(&run_dir).expect("scan should succeed");
        assert!(
            pending.is_empty(),
            "an unconfirmed temp must not become a pending request"
        );
        assert!(
            temporary.exists(),
            "a consumer must not delete another publisher's live temp"
        );
    }

    #[test]
    fn misnamed_cancel_control_never_consumes_a_legit_request() {
        // E01: a `cancel-evil.json` carrying `operation_id=legit` is
        // planted damage. It must be removed without honoring or
        // consuming the legit request published under its own name.
        let root = temp_root("cancel-plant");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        let legit = request_remote_cancel(
            &run_dir,
            "run-1",
            "tester",
            engine::JournalLimits::default(),
        )
        .expect("publish");
        let dir = control_dir(&run_dir);
        std::fs::write(
            dir.join("cancel-evil.json").as_std_path(),
            serde_json::to_vec(&serde_json::json!({
                "op": "cancel",
                "operation_id": legit,
                "run_id": "run-1",
                "requester": "attacker",
            }))
            .expect("plant should serialize"),
        )
        .expect("plant should write");
        let pending = list_pending_cancel_controls(&run_dir).expect("scan should succeed");
        assert_eq!(
            pending.len(),
            1,
            "only the correctly named control must be honored"
        );
        assert_eq!(pending[0].0, legit);
        assert!(
            !dir.join("cancel-evil.json").exists(),
            "the planted file must be reaped"
        );
        assert!(
            dir.join(format!("cancel-{legit}.json")).exists(),
            "the legit request must survive the planted file"
        );
    }

    #[test]
    fn incomplete_wipe_preserves_the_cancel_mailbox() {
        // E01/E03 intersection: a cancel that landed before any journal
        // existed must survive the partial-directory wipe (mailbox side)
        // and drain into the fresh run (adopt side).
        let root = temp_root("wipe-mailbox");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("control dir should be created");
        std::fs::write(run_dir.join("partial-output.txt"), b"junk")
            .expect("partial artifact should be written");
        std::fs::write(
            dir.join("cancel-op-1.json").as_std_path(),
            serde_json::to_vec(&serde_json::json!({
                "op": "cancel",
                "operation_id": "op-1",
                "run_id": "run-1",
                "requester": "tester",
            }))
            .expect("control should serialize"),
        )
        .expect("control should be written");
        assert!(
            !try_adopt_run_dir_with_snapshot(&run_dir, "run-1")
                .expect("adopt check should succeed")
                .adopted,
            "a journal-less directory must not adopt"
        );
        assert!(
            !run_dir.join("partial-output.txt").exists(),
            "partial artifacts must be wiped"
        );
        assert!(
            dir.join("cancel-op-1.json").exists(),
            "the cancel mailbox must survive the wipe"
        );
        let pending = list_pending_cancel_controls(&run_dir).expect("mailbox should still scan");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, "op-1");
    }

    #[test]
    fn torn_write_crash_tmps_are_never_listed() {
        // E01: a publisher crashed mid-write leaves a partial temp (fresh
        // mtime, torn JSON). The drain must neither list it as a request
        // nor mistake it for corruption: it is ignored while fresh and
        // reaped only once stale and proven idle.
        use std::time::{Duration, SystemTime};
        let root = temp_root("cancel-torn");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("control dir should be created");
        let torn = dir.join(".cancel-torn.tmp");
        std::fs::write(torn.as_std_path(), b"{\"op\":\"cancel\",\"operation_id\":")
            .expect("torn temp should be written");
        let pending = list_pending_cancel_controls(&run_dir).expect("drain scan should succeed");
        assert!(
            pending.is_empty(),
            "a torn temp must never become a pending request"
        );
        assert!(
            torn.exists(),
            "a fresh torn temp must survive the drain for its publisher's TTL"
        );
        let aged = SystemTime::now()
            .checked_sub(Duration::from_secs(3600))
            .expect("aged time should exist");
        std::fs::File::options()
            .write(true)
            .open(torn.as_std_path())
            .expect("torn temp should open")
            .set_modified(aged)
            .expect("torn temp should age");
        let pending = list_pending_cancel_controls(&run_dir).expect("drain scan should succeed");
        assert!(
            pending.is_empty(),
            "an aged torn temp must still never be listed"
        );
        assert!(
            !torn.exists(),
            "an aged torn temp must be reaped once proven idle"
        );
    }

    #[test]
    fn stale_cancel_temps_reap_by_age_only() {
        // E01: only abandoned (>60s) publisher temps are reaped; a fresh
        // temp from a live publisher is never touched. Drives the
        // PRODUCTION drain-inline path (`list_pending_cancel_controls`),
        // never the test-only standalone sweep (E01).
        use std::time::{Duration, SystemTime};
        let root = temp_root("cancel-reap");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("control dir should be created");
        let stale = dir.join(".cancel-stale.tmp");
        let fresh = dir.join(".cancel-fresh.tmp");
        std::fs::write(stale.as_std_path(), b"{}").expect("stale temp should be written");
        std::fs::write(fresh.as_std_path(), b"{}").expect("fresh temp should be written");
        let aged = SystemTime::now()
            .checked_sub(Duration::from_secs(3600))
            .expect("aged time should exist");
        std::fs::File::options()
            .write(true)
            .open(stale.as_std_path())
            .expect("stale temp should open")
            .set_modified(aged)
            .expect("stale temp should age");
        let pending = list_pending_cancel_controls(&run_dir).expect("drain scan should succeed");
        assert!(pending.is_empty(), "temps must never list as requests");
        assert!(!stale.exists(), "an abandoned temp must be reaped");
        assert!(fresh.exists(), "a fresh temp must survive the reap");
    }

    #[test]
    fn stale_locked_temp_survives_reap() {
        // E01: a stale-by-age temp still held under an exclusive lock is a
        // live publisher under clock skew and must survive the reap. Drives
        // the PRODUCTION drain-inline path, never the test-only sweep.
        use std::time::{Duration, SystemTime};
        let root = temp_root("cancel-reap-locked");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("control dir should be created");
        let locked = dir.join(".cancel-locked.tmp");
        std::fs::write(locked.as_std_path(), b"{}").expect("locked temp should be written");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(locked.as_std_path())
            .expect("locked temp should open");
        file.lock().expect("temp should lock");
        let aged = SystemTime::now()
            .checked_sub(Duration::from_secs(3600))
            .expect("aged time should exist");
        file.set_modified(aged).expect("locked temp should age");
        let pending = list_pending_cancel_controls(&run_dir).expect("drain scan should succeed");
        assert!(pending.is_empty(), "temps must never list as requests");
        assert!(
            locked.exists(),
            "a lock-held temp must survive even when stale"
        );
    }

    #[test]
    fn parallel_cancel_publish_loses_no_acknowledged_request() {
        // E01: concurrent publishers each receive a distinct operation id
        // and every acknowledged request survives in the mailbox.
        let root = temp_root("cancel-parallel");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        let ids: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let run_dir = run_dir.clone();
            let ids = std::sync::Arc::clone(&ids);
            handles.push(std::thread::spawn(move || {
                let id = request_remote_cancel(
                    &run_dir,
                    "run-1",
                    "tester",
                    engine::JournalLimits::default(),
                )
                .expect("publish");
                ids.lock().expect("ids lock").push(id);
            }));
        }
        for handle in handles {
            handle.join().expect("publisher should finish");
        }
        let ids = ids.lock().expect("ids lock");
        assert_eq!(ids.len(), 8, "every publisher must be acknowledged");
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 8, "operation ids must be distinct");
        let pending = list_pending_cancel_controls(&run_dir).expect("scan should succeed");
        let mut listed: Vec<String> = pending.iter().map(|(id, _)| id.clone()).collect();
        listed.sort();
        assert_eq!(listed, sorted, "no acknowledged request may vanish");
    }

    #[test]
    fn publish_never_overwrites_an_existing_control() {
        // E01: a planted file at the destination name fails the publish
        // and keeps its bytes; the staging temp is reclaimed. Same test
        // passes on both platforms with cfg-appropriate publish calls but
        // identical no-overwrite semantics: Linux uses
        // `renameat2(RENAME_NOREPLACE)` on the pinned fd, non-Linux uses the
        // portable `link(2)` + `unlink(temp)` dance (both fail with EEXIST
        // and never overwrite) (E01).
        let root = temp_root("cancel-no-overwrite");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("control dir should be created");
        let temporary = dir.join(".cancel-plant.tmp");
        std::fs::write(temporary.as_std_path(), b"staged").expect("temp should stage");
        let planted = dir.join("cancel-plant.json");
        std::fs::write(planted.as_std_path(), b"original").expect("plant should write");
        // The stage lock is borrowed across publication; the test holds an
        // ordinary open handle for the same role.
        let stage = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(temporary.as_std_path())
            .expect("staging temp should open");
        #[cfg(target_os = "linux")]
        let error = {
            let dir_fd = open_control_dir_pinned(&dir).expect("control dir should pin");
            publish_cancel_control(&dir_fd, &temporary, &planted, &stage)
                .expect_err("publish over an existing control must fail")
        };
        #[cfg(not(target_os = "linux"))]
        let error = publish_cancel_control(&dir, &temporary, &planted, &stage)
            .expect_err("publish over an existing control must fail");
        assert!(
            matches!(error, ServiceError::Io(ref io) if io.kind() == std::io::ErrorKind::AlreadyExists),
            "publication must report a typed collision for identity validation: {error}"
        );
        assert_eq!(
            std::fs::read(planted.as_std_path()).expect("plant should still read"),
            b"original",
            "the planted destination must be byte-identical"
        );
        assert!(
            !temporary.exists(),
            "the staging temp must be reclaimed on refusal"
        );
    }

    #[test]
    fn published_cancel_controls_are_complete_and_distinct() {
        // E01: every acknowledged publish is fully readable, listed once
        // per operation id, and removable independently.
        let root = temp_root("cancel-publish");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        let ids: Vec<String> = (0..4)
            .map(|_| {
                request_remote_cancel(
                    &run_dir,
                    "run-1",
                    "tester",
                    engine::JournalLimits::default(),
                )
                .expect("publish")
            })
            .collect();
        let pending = list_pending_cancel_controls(&run_dir).expect("scan should succeed");
        let listed: Vec<String> = pending.iter().map(|(id, _)| id.clone()).collect();
        let mut expected = ids.clone();
        expected.sort();
        assert_eq!(listed, expected, "all acknowledged requests must survive");
        for (id, value) in &pending {
            assert_eq!(value.get("op").and_then(Value::as_str), Some("cancel"));
            assert_eq!(value.get("run_id").and_then(Value::as_str), Some("run-1"));
            assert_eq!(
                value.get("operation_id").and_then(Value::as_str),
                Some(id.as_str())
            );
        }
        consume_cancel_control(&run_dir, &ids[0]);
        let pending = list_pending_cancel_controls(&run_dir).expect("scan should succeed");
        assert!(
            !pending.iter().any(|(id, _)| id == &ids[0]),
            "consumption removes exactly the consumed request"
        );
        assert_eq!(pending.len(), 3, "the other requests stay pending");
    }

    #[test]
    fn wipe_never_deletes_a_concurrently_published_cancel() {
        // E01: enumerated deletion only removes entries observed at scan
        // time, so a mailbox published concurrently with the wipe survives.
        let root = temp_root("wipe-race");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        std::fs::create_dir_all(run_dir.as_std_path()).expect("run dir should be created");
        std::fs::write(run_dir.join("partial.txt"), b"junk").expect("partial should be written");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("control dir should be created");
        // Simulate the race: wipe enumerates while a publish lands. The
        // implementation never uses whole-directory removal, so invoking
        // the wipe with a live mailbox must preserve it.
        let id = request_remote_cancel(
            &run_dir,
            "run-1",
            "tester",
            engine::JournalLimits::default(),
        )
        .expect("publish");
        wipe_incomplete_run_dir(&run_dir).expect("wipe should succeed");
        assert!(
            dir.join(format!("cancel-{id}.json")).exists(),
            "a concurrently published cancel must survive the wipe"
        );
        assert!(
            !run_dir.join("partial.txt").exists(),
            "partial artifacts must still be wiped"
        );
    }

    #[test]
    fn non_utf8_temp_respects_shared_reap_core() {
        // E01: a non-UTF8 temp name must route through the shared reap core
        // (symlink/age/lock checks), not be deleted as foreign damage. A
        // fresh non-UTF8 temp survives the production drain; an aged idle
        // one is reaped. Same test passes on both Unix flavors: macOS
        // (APFS) rejects non-UTF8 file names at creation, so a creation
        // failure documents the platform bound and still proves the drain
        // never honors temps (E01).
        let root = temp_root("cancel-nonutf8-temp");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("control dir should be created");
        // Build a non-UTF8 temp name that still matches the byte-exact
        // `.cancel-*.tmp` predicate on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt as _;
            use std::time::{Duration, SystemTime};
            let raw = b".cancel-\xff-nonutf8.tmp";
            let path =
                std::path::Path::new(dir.as_std_path()).join(std::ffi::OsStr::from_bytes(raw));
            if let Err(error) = std::fs::write(&path, b"{}") {
                // APFS (macOS) rejects non-UTF8 names with `InvalidInput`
                // (`Illegal byte sequence`): the byte-exact predicate is
                // still exercised (a temp that cannot be created cannot
                // be honored), so assert the drain stays empty and return
                // instead of failing the platform (E01).
                let is_platform_name_error = error.kind() == std::io::ErrorKind::InvalidInput
                    || error.to_string().contains("Illegal byte sequence");
                assert!(
                    is_platform_name_error,
                    "non-UTF8 creation must fail only with a platform name error: {error}"
                );
                let pending = list_pending_cancel_controls(&run_dir).expect("drain should succeed");
                assert!(pending.is_empty(), "temps must never list");
                return;
            }
            // Fresh: production drain must preserve it (age gate).
            let pending = list_pending_cancel_controls(&run_dir).expect("drain should succeed");
            assert!(pending.is_empty(), "temps must never list");
            assert!(
                path.exists(),
                "a fresh non-UTF8 temp must survive the drain"
            );
            // Age it: now the shared core may reap it once proven idle.
            let aged = SystemTime::now()
                .checked_sub(Duration::from_secs(3600))
                .expect("aged time should exist");
            std::fs::File::options()
                .write(true)
                .open(&path)
                .expect("temp should open")
                .set_modified(aged)
                .expect("temp should age");
            let pending = list_pending_cancel_controls(&run_dir).expect("drain should succeed");
            assert!(pending.is_empty(), "aged temps must never list");
            assert!(
                !path.exists(),
                "an aged idle non-UTF8 temp must be reaped via the shared core"
            );
        }
        #[cfg(not(unix))]
        {
            // Non-Unix cannot express non-UTF8 temp bytes through the
            // byte-exact predicate; the drain must still succeed without
            // honoring anything.
            let pending = list_pending_cancel_controls(&run_dir).expect("drain should succeed");
            assert!(pending.is_empty());
        }
    }

    #[test]
    fn control_dir_symlink_fails_scan_closed() {
        // E01: `read_dir` would follow a symlinked control dir while publish
        // pins O_NOFOLLOW, so the scan must lstat and fail closed.
        let root = temp_root("cancel-control-symlink");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(root.as_std_path()).expect("root should be created");
        let outside = root.join("outside");
        std::fs::create_dir_all(outside.as_std_path()).expect("outside should be created");
        std::fs::create_dir_all(dir.parent().unwrap().as_std_path())
            .expect("meta parent should be created");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.as_std_path(), dir.as_std_path())
                .expect("symlink should plant");
            let error = list_pending_cancel_controls(&run_dir)
                .expect_err("symlinked control dir must fail closed");
            assert!(
                error.to_string().contains("symlink"),
                "refusal must name the symlink: {error}"
            );
        }
    }

    #[test]
    fn wipe_refuses_symlinked_mailbox() {
        // E01: wipe must lstat (no-follow) the keep path and refuse a
        // symlinked mailbox instead of deleting through it.
        let root = temp_root("wipe-symlink-keep");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        std::fs::create_dir_all(run_dir.as_std_path()).expect("run dir should be created");
        std::fs::write(run_dir.join("partial.txt"), b"junk").expect("partial should be written");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.parent().unwrap().as_std_path())
            .expect("meta should be created");
        let outside = root.join("outside");
        std::fs::create_dir_all(outside.as_std_path()).expect("outside should be created");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.as_std_path(), dir.as_std_path())
                .expect("symlink should plant");
            let error =
                wipe_incomplete_run_dir(&run_dir).expect_err("symlinked keep must fail closed");
            assert!(
                error.to_string().contains("symlink"),
                "refusal must name the symlink: {error}"
            );
            assert!(
                run_dir.join("partial.txt").exists(),
                "refused wipe must leave partials untouched"
            );
        }
    }

    #[test]
    fn resurrect_duplicate_never_double_lists() {
        // E01 dir-sync warn-only rationale + duplicate-tolerance proof at
        // the mailbox layer: publication is `sync_all` + atomic rename/link
        // with warn-only dir sync, so a power loss may resurrect a consumed
        // control (removed link reappears). The drain stays
        // duplicate-tolerant by construction: controls carry a stable
        // `operation_id` and the journal re-check (`cancel_operations`
        // contains) converges resurrected duplicates without double
        // journaling. At this layer, re-listing the same operation id
        // twice yields the same id (dedup by file name), never two distinct
        // requests for one publication.
        let root = temp_root("cancel-resurrect");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run-1");
        let id = request_remote_cancel(
            &run_dir,
            "run-1",
            "tester",
            engine::JournalLimits::default(),
        )
        .expect("publish");
        let dir = control_dir(&run_dir);
        let published = dir.join(format!("cancel-{id}.json"));
        assert!(published.exists(), "publish must exist");
        // Simulate a power-loss resurrect: the consumed link reappears with
        // identical bytes (same operation id, same name).
        let bytes = std::fs::read(published.as_std_path()).expect("published should read");
        // Consume (removes the link), then resurrect by rewriting the same
        // bytes at the same name.
        assert!(
            consume_cancel_control(&run_dir, &id),
            "consume should remove"
        );
        assert!(!published.exists(), "consume must remove");
        std::fs::write(published.as_std_path(), &bytes).expect("resurrect should write");
        let pending =
            list_pending_cancel_controls(&run_dir).expect("resurrected scan should succeed");
        assert_eq!(pending.len(), 1, "resurrected control lists exactly once");
        assert_eq!(
            pending[0].0, id,
            "resurrect preserves the operation id for journal dedup"
        );
    }
}
