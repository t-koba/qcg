use crate::summaries::{read_events_from_meta, run_meta_dir, run_workspace_dir};
use crate::types::{RunRecord, ServiceError};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::ForkStatePatch;
use qcg_engine::{JournalError, JournalLimits, JournalWriter, RunState};
use qcg_policy::is_safe_relative_path;
use qcg_steps::deterministic_registry;
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

pub(crate) fn write_run_event(
    record: &RunRecord,
    kind: &str,
    payload: Value,
) -> Result<(), ServiceError> {
    let run_id = record
        .run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory must have a run id".into()))?;
    // Atomic single append under the cross-process journal lock. The writer
    // re-folds the latest journal so seq assignment never duplicates values
    // assigned concurrently by the running engine (A01).
    JournalWriter::append_single_event(
        &run_meta_dir(&record.run_dir).join("journal.jsonl"),
        run_id,
        kind,
        payload,
        JournalLimits::from(&record.contract.manifest.runtime),
        Some(record.events.clone()),
    )
    .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    Ok(())
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
    let run_id = record
        .run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory must have a run id".into()))?;
    JournalWriter::append_single_event_if(
        &run_meta_dir(&record.run_dir).join("journal.jsonl"),
        run_id,
        kind,
        payload,
        JournalLimits::from(&record.contract.manifest.runtime),
        Some(record.events.clone()),
        check,
    )
    .map_err(|error| match error {
        JournalError::PreconditionFailed(detail) => ServiceError::PreconditionFailed(detail),
        error => ServiceError::Invalid(error.to_string()),
    })?;
    Ok(())
}

/// Atomic multi-append variant of [`write_run_event_if`] for logically
/// joint settlements (for example denial bookkeeping plus its terminal
/// event). No other writer interleaves between the batched events.
pub(crate) fn write_run_events(
    record: &RunRecord,
    events: Vec<(&str, Value)>,
) -> Result<(), ServiceError> {
    let run_id = record
        .run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory must have a run id".into()))?;
    JournalWriter::append_events_if(
        &run_meta_dir(&record.run_dir).join("journal.jsonl"),
        run_id,
        events,
        JournalLimits::from(&record.contract.manifest.runtime),
        Some(record.events.clone()),
        |_| Ok(()),
    )
    .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    Ok(())
}

pub(crate) fn direct_run_id(workspace: &Utf8Path) -> String {
    let digest = hex::encode(Sha256::digest(workspace.as_str().as_bytes()));
    format!("direct-{digest}")
}

pub(crate) fn lock_runs_directory(runs_dir: &Utf8Path) -> Result<File, ServiceError> {
    std::fs::create_dir_all(runs_dir)?;
    let lock_path = runs_dir.join(".service.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(ServiceError::Invalid(format!(
                "runs directory `{runs_dir}` is already owned by another qcg service"
            )));
        }
        Err(std::fs::TryLockError::Error(error)) => return Err(ServiceError::Io(error)),
    }
    Ok(file)
}

/// Shared counterpart of [`lock_runs_directory`]: any number of
/// SharedFilesystem peers hold a shared lock together, while an Exclusive
/// service is refused until every shared peer exits. This makes the two
/// store modes mutually exclusive on the same runs directory (E12c).
pub(crate) fn lock_runs_directory_shared(runs_dir: &Utf8Path) -> Result<File, ServiceError> {
    std::fs::create_dir_all(runs_dir)?;
    let lock_path = runs_dir.join(".service.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    match file.try_lock_shared() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(ServiceError::Invalid(format!(
            "runs directory `{runs_dir}` is exclusively owned by another qcg service"
        ))),
        Err(std::fs::TryLockError::Error(error)) => Err(ServiceError::Io(error)),
    }
}

/// Serializes admission (fresh prepare, incomplete-journal wipe, or
/// adoption decision) for one run directory across processes. Two
/// admissions for the same reserved run id must not race: the loser would
/// otherwise wipe a live prepare or append a second `run_queued` event
/// (E03).
///
/// The lock lives next to the run directory, never inside it: admission may
/// wipe the run directory, and unlinking a locked file would let a later
/// admission lock a fresh inode while this one still holds the old (E03).
/// The name is a digest because run ids may contain separators.
pub(crate) struct RunAdmissionLock(File);

impl Drop for RunAdmissionLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

pub(crate) fn try_lock_run_admission(
    run_dir: &Utf8Path,
) -> Result<Option<RunAdmissionLock>, ServiceError> {
    // Lock files are intentionally never unlinked: removing a locked file
    // would let a later admission lock a fresh inode while the holder still
    // owns the old one. Growth is bounded: one small file per distinct run
    // id, and run directories (with their lock files) are bounded by
    // max_tracked_runs plus GC retention (E03).
    let runs_dir = run_dir
        .parent()
        .ok_or_else(|| ServiceError::Invalid(format!("run directory `{run_dir}` has no parent")))?;
    std::fs::create_dir_all(runs_dir)?;
    let run_id = run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid(format!("run directory `{run_dir}` has no name")))?;
    let digest = hex::encode(Sha256::digest(run_id.as_bytes()));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(runs_dir.join(format!(".admission-{digest}.lock")))?;
    match file.try_lock() {
        Ok(()) => Ok(Some(RunAdmissionLock(file))),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(ServiceError::Io(error)),
    }
}

pub(crate) fn try_lock_run_execution(run_dir: &Utf8Path) -> Result<Option<File>, ServiceError> {
    let metadata = run_meta_dir(run_dir);
    std::fs::create_dir_all(&metadata)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(metadata.join("execution.lock"))?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(ServiceError::Io(error)),
    }
}

/// Records which service process currently owns run execution in the
/// execution-lease file itself, so shared-store peers can observe owner
/// hand-offs without a separate channel (E12). The lease (flock) stays the
/// authority: the claim is an advisory convergence hint read by
/// `refresh_shared_runs`, which adopts it into memory on owner change. The
/// claim is bounded and validated on read; malformed content reads as no
/// claim, never as a foreign owner.
pub(crate) fn claim_run_execution_owner(lock: &File, owner_id: &str) -> Result<(), ServiceError> {
    use std::io::Seek as _;
    let mut lock = lock;
    lock.rewind().map_err(ServiceError::Io)?;
    lock.set_len(0).map_err(ServiceError::Io)?;
    lock.write_all(owner_id.as_bytes())
        .map_err(ServiceError::Io)?;
    lock.sync_all().map_err(ServiceError::Io)?;
    Ok(())
}

/// Reads the advisory execution-owner claim left by the current lease
/// holder, if any. Returns `None` when the file is missing, unreadable,
/// empty, or malformed: an unscannable claim is no evidence about
/// ownership, so callers keep their current owner instead of guessing
/// (E12). Never follows a symlink (E01).
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

pub(crate) fn try_lock_store_maintenance(
    runs_dir: &Utf8Path,
) -> Result<Option<File>, ServiceError> {
    std::fs::create_dir_all(runs_dir)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(runs_dir.join(".maintenance.lock"))?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(ServiceError::Io(error)),
    }
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

pub(crate) fn prepare_checkpoint_fork(
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
        // Fork copies the completed prefix only: terminal outcomes and the
        // pending prompt at the checkpoint are not carried over. The fork
        // resumes execution from the checkpoint with its own answers and
        // state patch; inheriting the source's pending question or terminal
        // state would fork the journal instead of continuing it (E06).
        .filter(|event| {
            !matches!(
                event.get("t").and_then(Value::as_str),
                Some(
                    "run_finished"
                        | "run_error"
                        | "run_canceled"
                        | "run_interrupted"
                        | "run_waiting"
                        | "confirm_request"
                )
            )
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

    prepare_api_run_directory(target_dir)?;
    let target_meta = run_meta_dir(target_dir);
    let target_workspace = run_workspace_dir(target_dir);
    std::fs::create_dir_all(&target_workspace)?;
    std::fs::create_dir_all(target_meta.join("checkpoint-blobs"))?;

    let mut latest_files = BTreeMap::<Utf8PathBuf, String>::new();
    // Every distinct revision any successful step pinned must have its blob
    // in the fork: resume verifies historical pins against those blobs, so
    // copying only the latest revision would make a valid fork unresumable
    // (E06).
    // Blob digests name store paths: validate hex shape before any stat so
    // a malformed journal pin cannot probe arbitrary paths (E06).
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
        if event.get("t").and_then(Value::as_str) != Some("step_finished") {
            continue;
        }
        // Both success and failure pins must survive: historical revisions
        // include failed-step-only updates, and resume verifies every
        // historical blob. Collecting only `files` would orphan
        // `failed_files` blobs and make a valid fork unresumable (E06).
        let mut pins: Vec<&Value> = Vec::new();
        if let Some(files) = event.get("files").and_then(Value::as_array) {
            pins.extend(files.iter());
        }
        if let Some(failed) = event.get("failed_files").and_then(Value::as_array) {
            pins.extend(failed.iter());
        }
        if pins.is_empty() {
            continue;
        }
        // Only a successful step projects the workspace; a failed or
        // suspended step still pins revisions that resume must verify
        // (E06), matching the engine's historical/latest split.
        let successful = matches!(
            event.get("status").and_then(Value::as_str),
            Some("success" | "repaired" | "routed" | "answered_on_fail" | "regenerated")
        );
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
            if successful {
                latest_files.insert(path, digest.to_string());
            }
        }
    }
    // Fresh target workspace, but every nested parent is still validated
    // level by level: a pre-planted symlink inside the fresh tree must not
    // redirect blob copies outside (strongest isolation, same invariant as
    // FsGateway for file inputs).
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
        // Consult the verified set BEFORE any copy or hash: a repeated
        // digest across paths reuses the already-verified blob instead of
        // recopying and rehashing the source (E06).
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
        verify_source_digest_nofollow(&source_blob, &digest, &path)?;
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
            ".{}.qcg-part-{}",
            destination.file_name().unwrap_or("blob"),
            uuid::Uuid::now_v7().as_simple()
        ));
        copy_source_handle_to_tmp(&source_blob, &tmp, &digest, &path)?;
        if qcg_fs::hash_file_sha256(&tmp, None).map(|(hex, _)| hex)? != digest {
            let _ = std::fs::remove_file(&tmp);
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{path}` changed while it was copied"
            )));
        }
        std::fs::rename(&tmp, &destination)?;
        if let Some(parent) = destination.parent() {
            sync_dir_entry(parent).map_err(ServiceError::Io)?;
        }
        if qcg_fs::hash_file_sha256(&destination, None).map(|(hex, _)| hex)? != digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{path}` changed while it was committed"
            )));
        }
        verified_blobs.insert(digest.clone());
        let target_blob = target_meta.join("checkpoint-blobs").join(&digest);
        std::fs::copy(&destination, &target_blob)?;
        sync_dir_entry(&target_meta.join("checkpoint-blobs")).map_err(ServiceError::Io)?;
        if qcg_fs::hash_file_sha256(&target_blob, None).map(|(hex, _)| hex)? != digest {
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
            verify_source_digest_nofollow(&target_blob, digest, path)?;
            continue;
        }
        let source_blob = source_meta.join("checkpoint-blobs").join(digest);
        if !regular_file_exists(&source_blob)? {
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` is missing; refusing the fork"
            )));
        }
        verify_source_digest_nofollow(&source_blob, digest, path)?;
        // Stage through a temp plus rename, never directly at the final
        // path: a crash mid-copy must leave no partial blob that later
        // verifies as fixed corruption (E03/E06).
        let blob_tmp = target_blob.with_file_name(format!(
            ".{digest}.qcg-part-{}",
            uuid::Uuid::now_v7().as_simple()
        ));
        copy_source_handle_to_tmp(&source_blob, &blob_tmp, digest, path)?;
        if qcg_fs::hash_file_sha256(&blob_tmp, None).map(|(hex, _)| hex)? != *digest {
            let _ = std::fs::remove_file(&blob_tmp);
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` failed integrity verification after the copy"
            )));
        }
        std::fs::rename(&blob_tmp, &target_blob).map_err(ServiceError::Io)?;
        sync_dir_entry(&target_meta.join("checkpoint-blobs")).map_err(ServiceError::Io)?;
        if qcg_fs::hash_file_sha256(&target_blob, None).map(|(hex, _)| hex)? != *digest {
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
            qcg_engine::read_journal_values(&source_audit_path, JournalLimits::default())
                .map(|scan| scan.events)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?;
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
            Value::String(qcg_api::trace_id_for_run(target_id)),
        );
        object.insert(
            "span_id".into(),
            Value::String(qcg_api::span_id_for_seq(next_seq)),
        );
        if object.contains_key("parent_span_id") {
            let scope = object
                .get("node")
                .and_then(Value::as_str)
                .map(|node| format!("step:{node}"))
                .unwrap_or_else(|| "run".to_string());
            object.insert(
                "parent_span_id".into(),
                Value::String(qcg_api::span_id_for_scope(target_id, &scope)),
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
        "trace_id": qcg_api::trace_id_for_run(target_id),
        "span_id": qcg_api::span_id_for_seq(next_seq),
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
            "trace_id": qcg_api::trace_id_for_run(target_id),
            "span_id": qcg_api::span_id_for_seq(next_seq),
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
    let state = RunState::fold_journal(&journal_path)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    state
        .persist_atomic(&target_meta.join("state.json"))
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
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

/// Opens a checkpoint source without following symlinks and validates the
/// open handle itself is a regular file. Path-based `symlink_metadata`
/// checks alone TOCTOU through a symlink swapped between the check and the
/// open/hash/copy, so the O_NOFOLLOW handle is authoritative on Unix (E06).
#[cfg(unix)]
fn open_regular_file_nofollow(path: &Utf8Path) -> Result<std::fs::File, ServiceError> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ServiceError::Invalid(format!("checkpoint path `{path}` is missing"))
            } else {
                ServiceError::Io(error)
            }
        })?;
    let metadata = file.metadata().map_err(ServiceError::Io)?;
    if !metadata.is_file() {
        return Err(ServiceError::Invalid(format!(
            "checkpoint path `{path}` is not a regular file"
        )));
    }
    Ok(file)
}

/// Non-Unix open: no O_NOFOLLOW, so the pre-open symlink probe plus the
/// handle type check is the whole defense. Symlinked sources are refused
/// before the handle is used (E06). Residual boundary: a swap between the
/// probe and the open remains by platform necessity; deployments with
/// untrusted concurrent writers must use Unix (E06).
#[cfg(not(unix))]
fn open_regular_file_nofollow(path: &Utf8Path) -> Result<std::fs::File, ServiceError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(ServiceError::Invalid(format!(
                "checkpoint path `{path}` is a symbolic link"
            )));
        }
        Ok(_) => {}
        Err(error) => {
            if error.kind() == std::io::ErrorKind::NotFound {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint path `{path}` is missing"
                )));
            }
            return Err(ServiceError::Io(error));
        }
    }
    let file = std::fs::File::open(path).map_err(ServiceError::Io)?;
    let metadata = file.metadata().map_err(ServiceError::Io)?;
    if !metadata.is_file() {
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
fn verify_source_digest_nofollow(
    source: &Utf8Path,
    expected_digest: &str,
    path_label: &Utf8Path,
) -> Result<(), ServiceError> {
    let mut handle = open_regular_file_nofollow(source)?;
    let actual = hash_opened_file(&mut handle)?;
    if actual != expected_digest {
        return Err(ServiceError::Invalid(format!(
            "checkpoint blob `{expected_digest}` for `{path_label}` failed integrity verification"
        )));
    }
    Ok(())
}

/// Copies verified source bytes from the O_NOFOLLOW handle to a fresh temp
/// path. Reading from the handle (not a second path open) keeps the copy
/// bound to the verified bytes (E06). Staging uses `sync_all` (content plus
/// metadata), the same durability bar as cancel temps and the fork journal:
/// checkpoint blobs are executable inputs, so a power loss must not leave a
/// staged temp whose content never reached the disk.
fn copy_source_handle_to_tmp(
    source: &Utf8Path,
    tmp: &Utf8Path,
    expected_digest: &str,
    path_label: &Utf8Path,
) -> Result<(), ServiceError> {
    use std::io::Seek as _;
    let mut handle = open_regular_file_nofollow(source)?;
    let actual = hash_opened_file(&mut handle)?;
    if actual != expected_digest {
        return Err(ServiceError::Invalid(format!(
            "checkpoint blob `{expected_digest}` for `{path_label}` failed integrity verification"
        )));
    }
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

/// Reuses an already-verified blob for a repeated digest without touching
/// the source again: the target blob store is the verified copy, so the
/// second path projects from it and only the fresh workspace file is
/// hashed (E06). Staging uses `sync_all` like `copy_source_handle_to_tmp`:
/// workspace inputs share the same durability bar as cancel temps (E06).
fn copy_verified_blob_to_workspace(
    verified_blob: &Utf8Path,
    destination: &Utf8Path,
    path_label: &Utf8Path,
    expected_digest: &str,
) -> Result<(), ServiceError> {
    use std::io::Seek as _;
    let mut handle = open_regular_file_nofollow(verified_blob)?;
    let actual = hash_opened_file(&mut handle)?;
    if actual != expected_digest {
        return Err(ServiceError::Invalid(format!(
            "checkpoint blob `{expected_digest}` for `{path_label}` failed integrity verification"
        )));
    }
    handle.rewind().map_err(ServiceError::Io)?;
    let tmp = destination.with_file_name(format!(
        ".{}.qcg-part-{}",
        destination.file_name().unwrap_or("blob"),
        uuid::Uuid::now_v7().as_simple()
    ));
    let mut tmp_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .map_err(ServiceError::Io)?;
    std::io::copy(&mut handle, &mut tmp_file).map_err(ServiceError::Io)?;
    tmp_file.sync_all().map_err(ServiceError::Io)?;
    drop(tmp_file);
    if qcg_fs::hash_file_sha256(&tmp, None).map(|(hex, _)| hex)? != expected_digest {
        let _ = std::fs::remove_file(&tmp);
        return Err(ServiceError::Invalid(format!(
            "checkpoint file `{path_label}` changed while it was copied"
        )));
    }
    std::fs::rename(&tmp, destination)?;
    if let Some(parent) = destination.parent() {
        sync_dir_entry(parent).map_err(ServiceError::Io)?;
    }
    if qcg_fs::hash_file_sha256(destination, None).map(|(hex, _)| hex)? != expected_digest {
        return Err(ServiceError::Invalid(format!(
            "checkpoint file `{path_label}` changed while it was committed"
        )));
    }
    Ok(())
}

pub(crate) fn journal_is_empty(path: &Utf8Path) -> std::io::Result<bool> {
    Ok(std::fs::metadata(path)?.len() == 0)
}

/// Decides whether a reserved run id binds to an existing run directory
/// left by a crashed idempotency owner. Adoption requires the journal to
/// fold to the same run id AND to hold that run's own `run_queued` event:
/// only a completed admission is resumed. A partial attempt (crash
/// mid-prepare, before `run_queued`) is wiped for a deterministic redo.
/// A fork's copied checkpoint journal carries the source's `run_queued`
/// (rewritten to the fork id) before its own admission event, so a
/// `run_queued` only counts when sequenced after every `run_forked` event:
/// otherwise a partial fork would be mistaken for a completed admission.
/// Identity chain: directory name = reserved run id = pending claim run id,
/// and the claim only issues one run id per key and digest, so wiping here
/// can never destroy foreign data. Anything else fails closed.
/// Removes an incomplete run directory while preserving its cancel
/// mailbox. A cancel that landed before any journal existed (racing a
/// fresh start, or a retry after a crash between preparation and
/// admission) is still a live request: wiping it with the partial
/// directory would lose the cancellation, so `meta/control` survives and
/// drains into the fresh run (E03).
/// Sidecar disposition: only `meta/control` is kept. `meta/idempotency.json`
/// and `checkpoint-blobs` are removed because an incomplete directory has
/// no completed admission and no completed execution, so no valid orphan
/// pointer or pinned blob can exist yet; anything present is partial
/// damage from the crashed attempt (E03).
fn wipe_incomplete_run_dir(run_dir: &Utf8Path) -> Result<(), ServiceError> {
    use crate::summaries::run_meta_dir;
    let keep = run_meta_dir(run_dir).join("control");
    // Pin the keep path without following symlinks (E01): `read_dir`
    // follows a symlinked control directory while publish pins O_NOFOLLOW,
    // so the wipe must lstat the keep path and refuse a symlinked mailbox
    // instead of deleting through it. `exists()` would follow the link and
    // misread a swapped mailbox as absent.
    match std::fs::symlink_metadata(&keep) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(ServiceError::Invalid(format!(
                "cancel control directory `{keep}` is a symlink; refusing wipe"
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ServiceError::Io(error)),
    }
    // Never use a whole-directory remove here: a cancel published between
    // the existence probe and the removal would already have been
    // acknowledged, and deleting it would lose a successful request (E01).
    // Enumerated deletion only removes entries observed at scan time, so a
    // concurrently published mailbox survives and drains later.
    // Callers hold the per-run admission lock and re-validate the journal
    // immediately before calling: no concurrent admission can complete a
    // journal while the wipe runs, so the wipe never deletes a live
    // admission (E03).
    // Deletes every entry under `dir` except `keep` itself; directories
    // containing `keep` are entered instead of removed, so the mailbox
    // survives while every partial artifact is removed. `symlink_metadata`
    // never reports a link as a dir, so links are removed as links and
    // never followed (E03).
    fn wipe_except(dir: &Utf8Path, keep: &Utf8Path) -> Result<(), ServiceError> {
        // Every scan uses `symlink_metadata` (never `metadata`/`exists`):
        // symlinks are removed as links and never followed, matching the
        // publish O_NOFOLLOW pin (E01).
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
                // Re-lstat the keep path without following: a symlink
                // swapped in after the outer check must fail closed instead
                // of being preserved as a foreign mailbox (E01).
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
            // Inspection failures fail the wipe instead of guessing
            // file-vs-dir: removing a directory as a file (or vice versa)
            // would either fail confusingly or miss content (E03). A
            // vanished entry races a concurrent cleanup; skip it.
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
    // When no mailbox exists the wipe must leave no trace for a
    // deterministic redo: remove newly-emptied directories, ignoring
    // races where a concurrent publisher repopulated them (E01/E03).
    // `remove_dir` only removes empty directories, so a concurrently
    // published mailbox survives. The probe uses `symlink_metadata`
    // (no-follow): `exists()` would follow a swapped link and misread a
    // symlinked mailbox as absent (E01).
    let keep_missing = match std::fs::symlink_metadata(&keep) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Ok(_) => false,
        Err(error) => return Err(ServiceError::Io(error)),
    };
    if keep_missing {
        // Best-effort: a concurrent publisher repopulating the directory
        // or a mid-wipe failure only leaves an empty directory behind for
        // the next admission to adopt-or-wipe again. Removal failures warn
        // instead of failing the wipe (E01/E03).
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

/// Adoption plus the single journal snapshot the admission reuses for its
/// seed, queue instant, and state fold, so start and fork admissions read
/// the journal once instead of scanning per field (E03). The fold is done
/// here, so seed derivation must not fold the same events again.
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

pub(crate) fn try_adopt_run_dir_with_snapshot(
    run_dir: &Utf8Path,
    run_id: &str,
) -> Result<AdoptSnapshot, ServiceError> {
    use crate::summaries::{read_events_from_meta, run_meta_dir};
    // Single attempt (E03): every caller holds the per-run admission lock,
    // which serializes prepare/wipe/adopt for one run id, so no concurrent
    // admission can complete a journal while this probe runs. A former
    // bounded-retry loop was removed as dead (it never looped: every path
    // returned on the first iteration); a single pass is the whole proof.
    let journal_path = run_meta_dir(run_dir).join("journal.jsonl");
    // All probes use `symlink_metadata` (no-follow): a symlinked
    // journal would otherwise redirect the fold outside the run
    // directory while publish pins O_NOFOLLOW (E01). Symlinks fail
    // closed here.
    // One metadata stat serves the whole empty-journal probe (E03):
    // every caller holds the per-run admission lock, so no concurrent
    // admission can complete this journal while the probe runs.
    match std::fs::symlink_metadata(&journal_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // No journal with a possibly dirty workspace from a crash
            // between directory preparation and admission: wipe
            // everything but the cancel mailbox (E03). Existence uses
            // `symlink_metadata` so a symlinked run dir never reads as
            // present-and-followed (E01).
            let run_exists = match std::fs::symlink_metadata(run_dir) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(ServiceError::Io(error)),
            };
            if run_exists {
                wipe_incomplete_run_dir(run_dir)?;
            }
            return Ok(AdoptSnapshot::empty());
        }
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(ServiceError::Invalid(format!(
                "run journal `{journal_path}` is a symlink; refusing adoption"
            )));
        }
        Ok(metadata) if metadata.len() == 0 => {
            // An empty journal with workspace remnants is the same partial
            // state: wipe the directory but not the mailbox (E03).
            wipe_incomplete_run_dir(run_dir)?;
            return Ok(AdoptSnapshot::empty());
        }
        Ok(_) => {}
        Err(error) => return Err(ServiceError::Io(error)),
    }
    // One journal read serves both the state fold and the admission-event
    // scan below: adoption never pays a scan per check (E03).
    let events = read_events_from_meta(&run_meta_dir(run_dir))?;
    let state =
        RunState::fold_values(&events).map_err(|error| ServiceError::Invalid(error.to_string()))?;
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
    // A fork checkpoint copy carries the source's `run_queued` (with the
    // fork id stamped on) ahead of a `run_forked` marker: only a
    // `run_queued` sequenced after every `run_forked` event is the fork's
    // own admission. A journal without any `run_forked` accepts any
    // matching `run_queued`.
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
        // A partial fork or a journal without this run's own admission
        // is wiped directly: no second re-read occurs here (E03). The
        // redundant re-read was removed because every caller holds the
        // per-run admission lock, which serializes prepare/wipe/adopt
        // for one run id: no concurrent admission can complete a journal
        // for this directory while this probe runs, so a re-read cannot
        // observe a newly completed admission that this probe missed.
        // Execution appends (non-admission writers) never create
        // `run_queued`, so they cannot flip this verdict either (E03).
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
    match std::fs::symlink_metadata(&joined) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{relative}` is a symlink"
            )));
        }
        Ok(_) => {}
        // A missing leaf is a fresh output path and stays admissible; any
        // other probe failure (permissions, I/O) fails closed instead of
        // treating an unscannable leaf as safe (E06).
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ServiceError::Io(error)),
    }
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

/// Opens the cancel control directory and pins it with
/// `O_DIRECTORY | O_NOFOLLOW` (Unix) so every later staging operation is
/// bound to the same directory inode: a symlink swapped in after
/// `create_dir_all` cannot redirect temp creation or publication (E01). The
/// handle must stay alive across staging and publication; temps are created
/// with `openat` relative to it and the publish rename runs between two
/// names in the same directory fd. Opening with `O_DIRECTORY | O_NOFOLLOW`
/// already refuses non-directories and symlinks atomically: no
/// check-then-use window remains.
#[cfg(unix)]
fn open_control_dir_pinned(dir: &Utf8Path) -> Result<File, ServiceError> {
    std::fs::create_dir_all(dir)?;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(dir)
        .map_err(ServiceError::Io)
}

/// Creates one cancel staging temp relative to a pinned control-directory
/// fd (openat semantics): no staging pathname is resolved, so a directory
/// swap after the pin cannot redirect the create (E01). The temp is
/// owner-only (0600) from creation, so no umask window exposes it. `EEXIST`
/// surfaces as `AlreadyExists` for the collision retry below.
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

/// Removes one cancel staging temp relative to a pinned control-directory
/// fd, so cleanup cannot stray after a directory swap (E01). Returns the
/// unlink result so callers warn on failure like the non-Unix path: silent
/// cleanup would hide a leaked temp that later blocks a fresh publish (E01).
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

pub(crate) fn request_remote_cancel(
    run_dir: &Utf8Path,
    run_id: &str,
    requester: &str,
) -> Result<String, ServiceError> {
    let dir = control_dir(run_dir);
    // Regression guards for the three cancel-mailbox windows (E01), all
    // closed in the current pin structure:
    // - control-dir check-then-temp-open: closed by fd-pinning below.
    //   `open_control_dir_pinned` (O_DIRECTORY|O_NOFOLLOW) anchors temp
    //   creation (`openat`) and publication (`renameat2`/`link`), so no
    //   staging pathname is resolved after the pin and a directory swap
    //   cannot redirect the create or publish.
    // - unsynced temp utime: closed by the synced mtime refresh
    //   below (`set_modified` + `sync_all` before the write, and the write
    //   itself ends with `sync_all`). A crash between refresh and publish
    //   cannot leave a fresh-looking temp whose content never reached disk.
    // - lock-release-before-rename: closed by holding the stage
    //   lock across publication (the `&file` borrow in the publish call).
    //   The reaper only touches stale AND unlocked temps, so releasing
    //   before the rename would open a fresh/stale-flip window; holding
    //   through the rename pins the published inode only briefly.
    // Pin the control directory before any staging path is used (Unix): the
    // fd below anchors temp creation (openat) and publication (renameat2),
    // closing the create_dir_all/symlink_metadata-check/temp-open TOCTOU
    // (E01).
    #[cfg(unix)]
    let control_fd = open_control_dir_pinned(&dir)?;
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(&dir)?;
        // The control directory itself must not be a symlink:
        // `create_dir_all` follows links, so verify after creation and
        // refuse to publish through a swapped path (E01).
        match std::fs::symlink_metadata(&dir) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(ServiceError::Invalid(format!(
                    "cancel control directory `{dir}` is a symlink; refusing publish"
                )));
            }
            Ok(_) => {}
            Err(error) => return Err(ServiceError::Io(error)),
        }
        // Non-Unix has no O_DIRECTORY|O_NOFOLLOW: the open below plus an
        // immediate re-metadata check is the whole defense, and a residual
        // pathname window remains between creation and publication.
        // Deployments that allow untrusted concurrent writers to the control
        // directory are unsupported on non-Unix and must use Unix (E01).
    }
    // Single sweep lives on the drain path (`list_pending_cancel_controls`);
    // sweeping here as well would double-scan every publish (E01).
    // A leftover temp with a fresh uuid is impossible without a planted
    // name collision, so on collision retry with a fresh id instead of
    // overwriting foreign bytes (E01).
    let mut attempts = 0;
    let (operation_id, temporary, path, mut file) = loop {
        if attempts >= 3 {
            return Err(ServiceError::Invalid(
                "cancel staging collided repeatedly; refusing to overwrite".into(),
            ));
        }
        attempts += 1;
        let operation_id = uuid::Uuid::now_v7().to_string();
        let temporary = dir.join(format!(".cancel-{operation_id}.tmp"));
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
                // Re-validate immediately: without O_NOFOLLOW a symlink
                // planted in the create/open window would have been followed.
                // Fail closed on any link; a residual pathname race remains
                // (documented above).
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
                // `temporary` is reused from above: recomputing the join
                // would risk divergence, so move the same value (E01).
                break (operation_id, temporary, path, file);
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
    // Stage the complete record in a name the scanner ignores, sync it,
    // then publish by rename. A reader can only observe a complete cancel
    // request or no request at all, never a torn one that a scanner would
    // misread as corruption and delete (E01). The temp is exclusively
    // locked for the whole stage so a reaper can tell a live publisher
    // from an abandoned temp without trusting mtimes alone (E01).
    let cleanup_unpublished = |temporary: &Utf8Path| {
        // Warn on both platforms: a leaked staging temp blocks no future
        // publish (fresh UUIDs) but hides a partially written request that
        // the reaper must still handle, so silence on either path would
        // diverge cleanup visibility (E01).
        #[cfg(unix)]
        {
            if let Some(name) = temporary.file_name()
                && let Err(cleanup) = remove_cancel_temp_at(&control_fd, name)
            {
                // A missing temp races a concurrent reaper and is not a
                // leak; any other failure warns like the non-Unix path.
                if cleanup.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %temporary, %cleanup, "failed to clean unpublished cancel temp");
                }
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
    // Refresh the temp mtime while holding the exclusive lock so a reaper
    // cannot mistake a live publisher for a stale temp even under clock
    // skew: the age gate only applies to unlocked temps (E01). A refresh
    // failure means the premise is gone, so fail the publish instead of
    // continuing with a temp that looks abandoned. The refreshed mtime is
    // synced before publication: a crash between refresh and publish must
    // not leave a fresh-looking temp whose content never reached the disk,
    // and the write below syncs content plus metadata together (E01).
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
    // The stage lock stays held across publication below (the `&file` borrow
    // in the publish call): the reaper only touches stale AND unlocked
    // temps, and this temp is seconds old, so holding the lock through the
    // rename only pins the published inode briefly until this function
    // returns. Releasing before the rename instead would open a
    // fresh/stale-flip window where the age gate could reap a temp that is
    // about to be published (E01).
    // Publish with no-overwrite semantics: the operation id is a fresh
    // UUIDv7, so an existing destination means a planted file or a UUID
    // collision. Overwriting it would destroy a previous request (E01).
    #[cfg(target_os = "linux")]
    publish_cancel_control(&control_fd, &temporary, &path, &file)?;
    #[cfg(not(target_os = "linux"))]
    publish_cancel_control(&dir, &temporary, &path, &file)?;
    // The rename is the publication: a later dir-sync failure only affects
    // power-loss durability (outside the Q2 process-crash boundary; the
    // staged payload itself was already synced with sync_all before the
    // rename), so it warns instead of reporting Err. Reporting Err here
    // would make the caller retry with a fresh operation id and journal a
    // duplicate cancel for one published request. Callers of this function
    // (remote-cancel admission) cannot turn a post-publish sync failure
    // into a refusal without risking exactly that duplication, hence
    // warn-only by necessity, not by optimism (E01).
    // Q2 boundary: if power loss drops the directory entry, the next boot
    // observes the request as absent and a client retry re-publishes under
    // a fresh operation id; duplicate journaling is prevented by the
    // cancel-drain idempotence, not by this sync. Process-crash (SIGKILL)
    // durability is unaffected because the payload sync precedes publish.
    if let Err(error) = sync_dir_entry(&dir) {
        tracing::warn!(path = %path, %error, "cancel control published but directory sync failed");
    }
    Ok(operation_id)
}

/// Atomically publishes a staged cancel temp under no-overwrite
/// semantics. The staged payload was already synced with `sync_all` before
/// this rename, and the caller syncs the directory entry after: both the
/// payload and the publication use `sync_all` durability (E01). The
/// publisher's stage lock (`_stage_lock`) is borrowed across the rename so
/// the reaper cannot observe an unlocked-but-unpublished temp (E01).
/// On Linux this is a single `renameat2(RENAME_NOREPLACE)` relative to the
/// pinned control-directory fd; on other platforms it is a portable
/// `link(2)` + `unlink(temp)` dance with identical no-overwrite semantics
/// (E01): `link` fails with `EEXIST` when the destination exists and never
/// overwrites, so no probe-then-rename plant window remains.
/// Failures remove the temp; an existing destination is a refusal, never
/// an overwrite (E01).
#[cfg(target_os = "linux")]
fn publish_cancel_control(
    dir_fd: &File,
    temporary: &Utf8Path,
    path: &Utf8Path,
    _stage_lock: &File,
) -> Result<(), ServiceError> {
    {
        // Both names are dir-relative: an absolute tmp would bypass the
        // dir_fd and reintroduce a pathname race. Fail closed on missing
        // file names instead of defaulting.
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
                return Err(ServiceError::Invalid(
                    "cancel control already exists; refusing to overwrite".into(),
                ));
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
        // `_dir` is unused on this path (the link below is path-based but
        // atomic); keep the parameter so both platforms share one helper
        // signature.
        // Portable NOREPLACE via link(2): creating a second hard link at
        // the destination fails atomically when it exists, so publication
        // never overwrites a planted file or a colliding UUID. The temp is
        // then unlinked, leaving the published control as the sole link.
        // Same-filesystem is guaranteed (same control directory), so
        // cross-device fallback is unreachable; any such error fails
        // closed instead of overwriting via rename (E01).
        // Refuse a symlinked staging temp before linking: without
        // O_NOFOLLOW a link planted in the create/open window would have
        // been followed at open time. Fail closed on any link (E01).
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
        // Refuse a symlinked destination probe before the link so the
        // error names the plant; the link itself remains authoritative
        // (it fails with EEXIST on any existing destination including a
        // symlink, never following it to overwrite the target) (E01).
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                if let Err(cleanup) = std::fs::remove_file(temporary) {
                    tracing::warn!(path = %temporary, %cleanup, "failed to clean cancel staging after overwrite refusal");
                }
                return Err(ServiceError::Invalid(
                    "cancel control already exists; refusing to overwrite".into(),
                ));
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
                // Publication is the link; unlink the staging name. A
                // failure here leaves two links to the same published
                // bytes: the destination is already durable, so warn and
                // reclaim best-effort without reporting failure (which
                // would invite a duplicate publish) (E01).
                if let Err(cleanup) = std::fs::remove_file(temporary) {
                    tracing::warn!(path = %temporary, %cleanup, "published cancel control but failed to unlink staging temp");
                }
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Err(cleanup) = std::fs::remove_file(temporary) {
                    tracing::warn!(path = %temporary, %cleanup, "failed to clean cancel staging after overwrite refusal");
                }
                Err(ServiceError::Invalid(
                    "cancel control already exists; refusing to overwrite".into(),
                ))
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

/// Flush a directory entry so a published rename survives a crash.
/// Unix opens the directory itself (`O_DIRECTORY | O_NOFOLLOW`) so a swapped
/// path cannot redirect the sync. Cancel payloads and idempotency claim,
/// Ready, and orphan stages all use `sync_all` (content plus metadata such
/// as the freshly refreshed mtime must persist together), followed by a
/// directory sync with the same crash visibility. Windows cannot open a
/// directory and NTFS
/// journals the rename itself; the non-Unix implementation is therefore a
/// no-op success, so identical renames have weaker power-loss durability on
/// non-Unix by platform necessity (Q2). Non-Unix mailbox reads stay
/// best-effort: concurrent symlink swaps are refused by the pre/post
/// `symlink_metadata` probes, but a pathname window remains; deployments
/// that allow untrusted concurrent writers to the control directory are
/// unsupported and must use Unix (E01).
/// Shared with the idempotency store so the platform branch
/// lives in exactly one place (E01).
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

/// Best-effort reaping of abandoned publish temps. A temp is removed only
/// when it is both older than [`CANCEL_TMP_TTL`] and proven not live: the
/// age gate reaps crashed publishers, while the lock probe protects a live
/// publisher from clock skew or admin time steps that would otherwise make
/// its fresh temp look stale (E01).
/// The mtime probe uses `symlink_metadata` so a symlinked temp never
/// follows outside the control directory; symlinked temps are removed as
/// links only and never opened (E01).
/// Shared predicate with the drain-path inline reap below so the two never
/// diverge (E01).
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
    // Single directory scan serves both the stale-temp reap and the pending
    // list: a second read_dir per drain is redundant I/O (E01). Temps are
    // reaped inline; controls are collected below.
    // The control directory itself is lstat-checked (no-follow) before the
    // scan: `read_dir` would follow a symlinked control dir while publish
    // pins O_NOFOLLOW, so a swapped mailbox fails the scan instead of
    // redirecting reads outside (E01).
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
    let mut pending = Vec::new();
    // A control is only honored for the run directory that holds it: the
    // expected run id is the directory name, resolved once. An
    // undeterminable directory fails the scan instead of matching every
    // control against an empty fallback (E01).
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
        // Never follow a symlink here: a planted link would redirect the
        // read outside the control directory (E01). A vanished file races
        // with a concurrent drain; skip it instead of failing the whole
        // mailbox, while any other inspection failure fails the scan so a
        // partially observed mailbox is never mistaken for clean (E01).
        // Every scan uses `symlink_metadata` (no-follow), matching the
        // publish O_NOFOLLOW pin: symlinks are never followed (E01).
        let meta = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "cancel control unreadable; failing scan");
                return Err(ServiceError::Io(error));
            }
        };
        // Temp check runs before the UTF-8 gate through the ONE shared core:
        // on Unix the byte-exact predicate classifies non-UTF8 temps exactly
        // like UTF-8 ones, so a live publisher's non-UTF8 temp keeps its
        // age/lock protection instead of being deleted as foreign damage
        // (E01). All removals below converge through the shared cores
        // (`reap_cancel_temp_entry` for temps, `reap_foreign_control_entry`
        // for foreign shapes): no direct `remove_file` bypasses the
        // symlink/age/lock discipline (E01).
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
                    tracing::warn!(path = %path.display(), %error, "cancel control unreadable; failing scan");
                    return Err(ServiceError::Io(error));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A control consumed by a racing drain between the metadata
                // probe and the open is gone: skip it instead of failing the
                // whole mailbox (E01).
                continue;
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "cancel control unreadable; failing scan");
                return Err(ServiceError::Io(error));
            }
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
                    Err(error) => {
                        tracing::warn!(path = %path.display(), %error, "cancel control unreadable; failing scan");
                        return Err(ServiceError::Io(error));
                    }
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

/// Removes a drained cancel control. Returns true when the link is gone
/// (removed or already absent). A removal failure returns false so the
/// caller can surface a phantom control instead of silently assuming the
/// mailbox is clean (E01/E05). The file stays for the next drain, which
/// re-checks under the journal lock and converges without duplicating.
/// A directory-sync failure after a successful removal still returns true:
/// the link is gone for the process-crash boundary and only power-loss
/// durability is affected (Q2).
pub(crate) fn consume_cancel_control(run_dir: &Utf8Path, operation_id: &str) -> bool {
    let dir = control_dir(run_dir);
    let path = dir.join(format!("cancel-{operation_id}.json"));
    if let Err(error) = std::fs::remove_file(&path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %path, %error, "failed to consume cancel control; it stays for the next drain");
            return false;
        }
        return true;
    }
    if let Err(error) = sync_dir_entry(&dir) {
        tracing::warn!(path = %path, %error, "consumed cancel control but directory sync failed");
    }
    true
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

pub(crate) fn lock_direct_run(metadata_dir: &Utf8Path) -> Result<File, ServiceError> {
    std::fs::create_dir_all(metadata_dir)?;
    let lock_path = metadata_dir.join(".run.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            return Err(ServiceError::Invalid(format!(
                "output metadata `{metadata_dir}` is already active in another qcg run"
            )));
        }
        Err(std::fs::TryLockError::Error(error)) => return Err(ServiceError::Io(error)),
    }
    Ok(file)
}

pub fn direct_run_meta_dir(workspace: &Utf8Path) -> Utf8PathBuf {
    let run_id = direct_run_id(workspace);
    workspace
        .parent()
        .unwrap_or_else(|| Utf8Path::new("."))
        .join(".qcg/runs")
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
        .join(".qcg/runs/.service.lock");
    let lock_file = match OpenOptions::new().read(true).write(true).open(&lock_path) {
        Ok(lock_file) => lock_file,
        Err(_) => return,
    };
    match lock_file.try_lock() {
        Ok(_) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            tracing::warn!(
                workspace = %workspace,
                "runs directory is owned by a running qcg server; direct execution bypasses its max_active_runs limit"
            );
        }
        Err(std::fs::TryLockError::Error(_)) => {}
    }
}

pub(crate) fn app_registry(runtime: Arc<qcg_llm::LlmRuntime>) -> qcg_engine::StepRegistry {
    let mut registry = qcg_steps::deterministic_registry_with_mcp(Arc::new(runtime.mcp.clone()));
    qcg_llm_steps::register_llm_steps(&mut registry, runtime);
    registry
}

/// Loads the LLM runtime for step registry assembly, falling back to
/// built-in capabilities when no providers registry is found. This is the
/// single choke point for registry assembly; binaries resolve the registry
/// through here instead of composing runtimes directly.
pub fn load_llm_runtime(
    providers_path: Option<&Utf8Path>,
) -> Result<Arc<qcg_llm::LlmRuntime>, ServiceError> {
    match qcg_llm::LlmRouter::load_optional(providers_path) {
        Ok(Some(router)) => Ok(Arc::new(router.into_runtime())),
        // No registry was found: built-in capabilities stay available
        // while other ids receive setup guidance during validation.
        Ok(None) => Ok(Arc::new(qcg_llm::LlmRuntime::builtins())),
        Err(error) => Err(ServiceError::Invalid(error.to_string())),
    }
}

/// Assembles the full application step registry, resolving the LLM runtime
/// from an explicit providers path or the standard search locations.
pub fn app_registry_with_providers(
    providers_path: Option<&Utf8Path>,
) -> Result<qcg_engine::StepRegistry, ServiceError> {
    Ok(app_registry(load_llm_runtime(providers_path)?))
}

pub fn built_in_step_param_schemas() -> BTreeMap<String, Value> {
    let mut registry = deterministic_registry();
    qcg_llm_steps::register_fake_llm_steps(&mut registry);
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
            std::env::temp_dir().join(format!("qcg-run-dirs-{name}-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8");
        std::fs::create_dir_all(root.as_std_path()).expect("temp root should be created");
        root
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
        let legit = request_remote_cancel(&run_dir, "run-1", "tester").expect("publish");
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
    fn stale_cancel_temps_reap_through_the_drain_inline_path() {
        // E01: the drain-path inline reclamation (not only the standalone
        // sweep) must reap an abandoned temp: production never runs the
        // standalone sweep.
        use std::time::{Duration, SystemTime};
        let root = temp_root("cancel-drain-reap");
        let _temp_guard = TempGuard(root.clone());
        let run_dir = root.join("run");
        let dir = control_dir(&run_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("control dir should be created");
        let stale = dir.join(".cancel-drain-stale.tmp");
        std::fs::write(stale.as_std_path(), b"{\"op\":\"cancel\"")
            .expect("stale temp should be written");
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
        assert!(
            pending.is_empty(),
            "a temp must never be listed as a pending request"
        );
        assert!(
            !stale.exists(),
            "the drain inline path must reap an abandoned temp"
        );
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
                let id = request_remote_cancel(&run_dir, "run-1", "tester").expect("publish");
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
            error.to_string().contains("refusing to overwrite"),
            "refusal must say so: {error}"
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
            .map(|_| request_remote_cancel(&run_dir, "run-1", "tester").expect("publish"))
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
        let id = request_remote_cancel(&run_dir, "run-1", "tester").expect("publish");
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
        let id = request_remote_cancel(&run_dir, "run-1", "tester").expect("publish");
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
