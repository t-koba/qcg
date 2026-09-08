use crate::summaries::{read_events_from_meta, run_meta_dir, run_workspace_dir};
use crate::types::{RunRecord, ServiceError};
use camino::{Utf8Path, Utf8PathBuf};
use fs2::FileExt as _;
use qcg_api::ForkStatePatch;
use qcg_engine::{JournalError, JournalLimits, JournalWriter, RunState};
use qcg_policy::is_safe_relative_path;
use qcg_steps::deterministic_registry;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
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
    .map_err(|error| ServiceError::Invalid(error.to_string()))?;
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

fn is_lock_contention(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }

    #[cfg(windows)]
    {
        // LockFileEx reports sharing and lock violations without mapping them to WouldBlock.
        matches!(error.raw_os_error(), Some(32 | 33))
    }

    #[cfg(not(windows))]
    {
        false
    }
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
    file.try_lock_exclusive().map_err(|error| {
        if is_lock_contention(&error) {
            ServiceError::Invalid(format!(
                "runs directory `{runs_dir}` is already owned by another qcg service"
            ))
        } else {
            ServiceError::Io(error)
        }
    })?;
    Ok(file)
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
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(file)),
        Err(error) if is_lock_contention(&error) => Ok(None),
        Err(error) => Err(ServiceError::Io(error)),
    }
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
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(file)),
        Err(error) if is_lock_contention(&error) => Ok(None),
        Err(error) => Err(ServiceError::Io(error)),
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
    for event in &selected {
        if event.get("t").and_then(Value::as_str) != Some("step_finished") {
            continue;
        }
        let Some(files) = event.get("files").and_then(Value::as_array) else {
            continue;
        };
        for file in files {
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
            latest_files.insert(path, digest.to_string());
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
    for (path, digest) in latest_files {
        let source_blob = source_meta.join("checkpoint-blobs").join(&digest);
        let source = if source_blob.is_file() {
            source_blob
        } else {
            let current = run_workspace_dir(source_dir).join(&path);
            if !current.is_file() {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint blob `{digest}` for `{path}` is unavailable; the source run predates checkpoint snapshots"
                )));
            }
            if qcg_policy::hash_file_sha256(&current, None)
                .map(|(hex, _)| hex)
                .map_err(|_| {
                    ServiceError::Invalid(format!(
                        "checkpoint blob `{digest}` for `{path}` is unavailable; the source run predates checkpoint snapshots"
                    ))
                })? != digest
            {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint blob `{digest}` for `{path}` is unavailable and the current workspace contains a different revision"
                )));
            }
            current
        };
        if qcg_policy::hash_file_sha256(&source, None).map(|(hex, _)| hex)? != digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` failed integrity verification"
            )));
        }
        safe_join_under_root(&target_workspace, &canonical_target_root, &path)?;
        let destination = target_workspace.join(path.as_str());
        if let Some(parent) = destination.parent() {
            ensure_dir_under_root(&target_workspace, &canonical_target_root, parent)?;
        }
        // Atomic copy via temp + rename with post-copy verification.
        let tmp = destination.with_file_name(format!(
            ".{}.fork-part-{}",
            destination.file_name().unwrap_or("blob"),
            uuid::Uuid::now_v7().as_simple()
        ));
        std::fs::copy(&source, &tmp)?;
        if qcg_policy::hash_file_sha256(&tmp, None).map(|(hex, _)| hex)? != digest {
            let _ = std::fs::remove_file(&tmp);
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{path}` changed while it was copied"
            )));
        }
        std::fs::rename(&tmp, &destination)?;
        if qcg_policy::hash_file_sha256(&destination, None).map(|(hex, _)| hex)? != digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{path}` changed while it was committed"
            )));
        }
        std::fs::copy(
            &destination,
            target_meta.join("checkpoint-blobs").join(&digest),
        )?;
    }

    let journal_path = target_meta.join("journal.jsonl");
    let mut journal = OpenOptions::new().append(true).open(&journal_path)?;
    let mut next_seq = 0_u64;
    for mut event in selected {
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
        serde_json::to_writer(&mut journal, &event)?;
        journal.write_all(b"\n")?;
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
    let state = RunState::fold_journal(&journal_path)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    state
        .persist_atomic(&target_meta.join("state.json"))
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
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
pub(crate) fn try_adopt_run_dir(run_dir: &Utf8Path, run_id: &str) -> Result<bool, ServiceError> {
    use crate::summaries::{fold_run_state, read_events_from_meta, run_meta_dir};
    let journal_path = run_meta_dir(run_dir).join("journal.jsonl");
    if !journal_path.exists() {
        return Ok(false);
    }
    if journal_is_empty(&journal_path)? {
        std::fs::remove_file(&journal_path)?;
        return Ok(false);
    }
    let state = fold_run_state(run_dir)?;
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
    let events = read_events_from_meta(&run_meta_dir(run_dir))?;
    // A fork checkpoint copy carries the source's `run_queued` (with the
    // fork id stamped on) ahead of a `run_forked` marker: only a
    // `run_queued` sequenced after every `run_forked` event is the fork's
    // own admission. Journals without any `run_forked` keep the legacy
    // rule (any matching `run_queued` admits).
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
        std::fs::remove_dir_all(run_dir)?;
        return Ok(false);
    }
    Ok(true)
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
    if let Ok(metadata) = std::fs::symlink_metadata(&joined)
        && metadata.file_type().is_symlink()
    {
        return Err(ServiceError::Invalid(format!(
            "checkpoint file `{relative}` is a symlink"
        )));
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

pub(crate) fn request_remote_cancel(
    run_dir: &Utf8Path,
    run_id: &str,
    requester: &str,
) -> Result<String, ServiceError> {
    let dir = control_dir(run_dir);
    std::fs::create_dir_all(&dir)?;
    let operation_id = uuid::Uuid::now_v7().to_string();
    let payload = serde_json::json!({
        "op": "cancel",
        "operation_id": operation_id,
        "run_id": run_id,
        "requester": requester,
        "ts": chrono::Utc::now().to_rfc3339(),
    });
    let bytes = serde_json::to_vec(&payload)?;
    let path = dir.join(format!("cancel-{operation_id}.json"));
    // create_new makes duplicate operation ids impossible; distinct
    // requests get distinct files and the owner dedupes by operation_id.
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(mut file) => {
            use std::io::Write as _;
            file.write_all(&bytes)?;
            file.sync_data()?;
            Ok(operation_id)
        }
        Err(error) => Err(ServiceError::Io(error)),
    }
}

/// Well-formed pending cancel controls as `(operation_id, descriptor)`.
/// Malformed or unparseable files are reaped with a warning instead of
/// leaking forever; they can never journal. Scan and read failures are
/// errors, never an empty mailbox: callers must not mistake a failed scan
/// for "no cancel requested" (A02).
pub(crate) fn list_pending_cancel_controls(
    run_dir: &Utf8Path,
) -> Result<Vec<(String, Value)>, ServiceError> {
    let dir = control_dir(run_dir);
    let mut pending = Vec::new();
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
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("cancel-") || !name.ends_with(".json") {
            continue;
        }
        // Read errors fail the scan: the file is retained and the next
        // drain retries. Only proven-unparseable content is reaped, since
        // it can never journal. Reads are bounded: our own controls are a
        // few hundred bytes, so anything larger is foreign damage, not a
        // cancel request.
        const MAX_CONTROL_FILE_BYTES: u64 = 64 * 1024;
        let mut bytes = Vec::new();
        match std::fs::File::open(&path) {
            Ok(file) => {
                use std::io::Read as _;
                if let Err(error) = file
                    .take(MAX_CONTROL_FILE_BYTES.saturating_add(1))
                    .read_to_end(&mut bytes)
                {
                    tracing::warn!(path = %path.display(), %error, "cancel control unreadable; failing scan");
                    return Err(ServiceError::Io(error));
                }
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "cancel control unreadable; failing scan");
                return Err(ServiceError::Io(error));
            }
        }
        if bytes.len() as u64 > MAX_CONTROL_FILE_BYTES {
            tracing::warn!(path = %path.display(), "removing oversize cancel control");
            let _ = std::fs::remove_file(&path);
            continue;
        }
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "removing unparseable cancel control");
                let _ = std::fs::remove_file(&path);
                continue;
            }
        };
        match value.get("operation_id").and_then(Value::as_str) {
            Some(operation_id) if !operation_id.is_empty() => {
                pending.push((operation_id.to_string(), value));
            }
            _ => {
                tracing::warn!(path = %path.display(), "removing cancel control without operation id");
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    pending.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(pending)
}

pub(crate) fn consume_cancel_control(run_dir: &Utf8Path, operation_id: &str) {
    let path = control_dir(run_dir).join(format!("cancel-{operation_id}.json"));
    let _ = std::fs::remove_file(&path);
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
    file.try_lock_exclusive().map_err(|error| {
        if is_lock_contention(&error) {
            ServiceError::Invalid(format!(
                "output metadata `{metadata_dir}` is already active in another qcg run"
            ))
        } else {
            ServiceError::Io(error)
        }
    })?;
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
    match lock_file.try_lock_exclusive() {
        Ok(_) => {}
        Err(error) if is_lock_contention(&error) => {
            tracing::warn!(
                workspace = %workspace,
                "runs directory is owned by a running qcg server; direct execution bypasses its max_active_runs limit"
            );
        }
        Err(_) => {}
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
