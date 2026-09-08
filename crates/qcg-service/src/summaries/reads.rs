use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::RunEvent;
use qcg_engine::{JournalLimits, read_journal_values};
use serde_json::Value;
use std::collections::BTreeMap;

use super::summary::run_meta_dir;

/// Durably accepted HITL responses: answers by question id and confirmations
/// by confirmation id.
pub(crate) type PersistedHitlMaps = (BTreeMap<String, Value>, BTreeMap<String, bool>);

pub fn resolve_run_dir(runs_dir: &Utf8Path, id: &str) -> Result<Utf8PathBuf, ServiceError> {
    if id.contains('/') || id.contains('\\') || id == "." || id == ".." {
        return Err(ServiceError::Invalid(format!(
            "run id `{id}` is not allowed"
        )));
    }
    let run_dir = runs_dir.join(id);
    if !run_meta_dir(&run_dir).join("journal.jsonl").exists() {
        return Err(ServiceError::Invalid(format!(
            "run `{id}` was not found under `{runs_dir}`"
        )));
    }
    Ok(run_dir)
}

pub fn read_journal_events(run_dir: &Utf8Path) -> Result<Vec<Value>, ServiceError> {
    read_events_from_meta(&run_meta_dir(run_dir))
}

/// Drop trailing `run_canceled` bookkeeping events written by an engine task
/// that was preempted before terminal settlement. Returns whether anything
/// was removed. Never touches a journal whose tail is anything else, so a
/// genuine user cancellation is preserved.
///
/// Holds the cross-process journal lock and inspects only the bounded tail.
/// Integrity comes from the journal lock, which every writer takes: no
/// append can interleave with the probe or the cut. Callers additionally
/// hold (or have awaited the release of) the run execution lease as
/// settlement authority, proving no owner is mid-flight while history is
/// rewritten. Full-file reads never happen; the cut offset resolves from a
/// backward bounded scan.
pub(crate) fn truncate_trailing_canceled_events(run_dir: &Utf8Path) -> Result<bool, ServiceError> {
    let path = run_meta_dir(run_dir).join("journal.jsonl");
    // Serialize with all journal appends; without this a live engine writer
    // could interleave and lose events (A01).
    let lock_path = path.with_file_name(".journal.lock");
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    {
        use fs2::FileExt as _;
        lock_file.lock_exclusive().map_err(ServiceError::Io)?;
    }
    // The open handle itself holds the lock; dropping it at function end
    // releases, so no explicit unlock dance is needed.
    let _lock_held = lock_file;
    // Bounded tail probe first: read only the final 64 KiB to decide whether
    // a full rewrite is needed (C05).
    let needs_rewrite = {
        let metadata = std::fs::metadata(&path)?;
        let len = metadata.len();
        if len == 0 {
            return Ok(false);
        }
        const PROBE: u64 = 64 * 1024;
        let start = len.saturating_sub(PROBE);
        let mut file = std::fs::File::open(&path)?;
        use std::io::{Read as _, Seek as _};
        file.seek(std::io::SeekFrom::Start(start))?;
        let mut buf = Vec::with_capacity(PROBE.min(len) as usize);
        file.take(PROBE).read_to_end(&mut buf)?;
        let mut slice = buf.as_slice();
        if start > 0 {
            match slice.iter().position(|b| *b == b'\n') {
                Some(pos) => slice = &slice[pos + 1..],
                None => slice = &[],
            }
        }
        let mut lines: Vec<&[u8]> = slice.split(|b| *b == b'\n').collect();
        // Drop the torn tail fragment (no trailing newline in file).
        if !buf.last().is_some_and(|b| *b == b'\n') {
            lines.pop();
        }
        let mut found = false;
        for line in lines.iter().rev() {
            if line.is_empty() {
                continue;
            }
            // Unparseable lines count as not-canceled: the probe must never
            // cut past unknown bytes, and the fold fails closed on them.
            let canceled = serde_json::from_slice::<Value>(line)
                .map(|event| event.get("t").and_then(Value::as_str) == Some("run_canceled"))
                .unwrap_or(false);
            if !canceled {
                break;
            }
            found = true;
        }
        // Ambiguous truncation (no newline in probe) falls through to a full
        // scan below; otherwise the probe decides.
        if slice.is_empty() && start > 0 {
            true
        } else {
            found
        }
    };
    if !needs_rewrite {
        return Ok(false);
    }
    // Rewrite by truncation offset, never by loading the whole journal:
    // walk backward to the end of the last non-canceled line and cut there.
    // Memory stays bounded by the scan window regardless of journal size.
    let removed = truncate_to_last_non_canceled(&path)?;
    if removed {
        // Re-fold to refresh state.json after truncation; failures fail
        // closed instead of leaving state.json ahead of the journal.
        let state = qcg_engine::RunState::fold_journal(&path)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        state
            .persist_atomic(&path.with_file_name("state.json"))
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    }
    Ok(removed)
}

/// Finds the cut offset just past the last newline-terminated line that is
/// not a `run_canceled` event, scanning backward in bounded windows. The
/// caller must hold the journal lock with no live writer; a torn tail
/// fragment is repaired first so every scanned line is complete.
fn truncate_to_last_non_canceled(path: &Utf8Path) -> Result<bool, ServiceError> {
    qcg_engine::repair_truncated_tail_locked(path)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    let len = std::fs::metadata(path)?.len();
    if len == 0 {
        return Ok(false);
    }
    const WINDOW: u64 = 64 * 1024;
    let mut cut = len;
    let mut offset = len;
    let mut first_window = true;
    loop {
        let start = offset.saturating_sub(WINDOW);
        let mut file = std::fs::File::open(path)?;
        use std::io::{Read as _, Seek as _};
        file.seek(std::io::SeekFrom::Start(start))?;
        let mut buf = Vec::with_capacity((offset - start) as usize);
        file.take(offset - start).read_to_end(&mut buf)?;
        let mut lines: Vec<(u64, &[u8])> = Vec::new();
        let mut line_start = 0_usize;
        for (index, byte) in buf.iter().enumerate() {
            if *byte == b'\n' {
                lines.push((start + line_start as u64, &buf[line_start..index]));
                line_start = index + 1;
            }
        }
        // After repair every line is newline-terminated; a trailing partial
        // line here means concurrent modification, which the lease forbids.
        if first_window && line_start < buf.len() {
            return Err(ServiceError::Invalid(
                "journal tail changed during locked truncation".into(),
            ));
        }
        first_window = false;
        for (line_offset, line) in lines.iter().rev() {
            if line.is_empty() {
                continue;
            }
            let canceled = serde_json::from_slice::<Value>(line)
                .map(|event| event.get("t").and_then(Value::as_str) == Some("run_canceled"))
                .unwrap_or(false);
            if !canceled {
                cut = line_offset + line.len() as u64 + 1;
                break;
            }
        }
        if cut != len {
            break;
        }
        // Every complete line in this window was canceled; continue before
        // it unless the whole file was scanned.
        if start == 0 {
            // A valid journal always starts with run_queued; an all-canceled
            // file is corruption, and truncating to zero would destroy it.
            return Err(ServiceError::Invalid(
                "journal holds only run_canceled events; refusing truncation".into(),
            ));
        }
        // Skip the first (possibly partial) line of the next window by
        // resuming before the earliest complete line start in this window.
        offset = lines
            .iter()
            .find(|(_, line)| !line.is_empty())
            .map(|(line_offset, _)| *line_offset)
            .unwrap_or(start);
        if offset == 0 {
            return Err(ServiceError::Invalid(
                "journal holds only run_canceled events; refusing truncation".into(),
            ));
        }
    }
    if cut == len {
        return Ok(false);
    }
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.set_len(cut)?;
    file.sync_data()?;
    Ok(true)
}

/// Latest scheduling identity from the trailing `run_queued` event:
/// priority and fork parent. Journals written before these fields existed
/// read as zero priority with no parent.
/// Priority and parent from already-read journal values. Pure scan over
/// memory: infallible by construction, so no Result to swallow.
pub(crate) fn read_queued_identity_from_values(events: &[Value]) -> (i32, Option<String>) {
    let mut priority = 0;
    let mut parent = None;
    for event in events {
        if event.get("t").and_then(Value::as_str) != Some("run_queued") {
            continue;
        }
        if let Some(value) = event.get("priority").and_then(Value::as_i64) {
            priority = value.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        }
        parent = event
            .get("parent_run_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(parent);
    }
    (priority, parent)
}

/// Durably accepted HITL responses folded from the journal.
///
/// `run_queued` may carry pre-provided `answers` / `confirmations` for
/// unattended runs, while later `user_answered` / `user_confirmed` events
/// record interactive acceptance. Later events win on the same key so a
/// restart resumes with the same values the API already acknowledged.
pub(crate) fn read_persisted_hitl(run_dir: &Utf8Path) -> Result<PersistedHitlMaps, ServiceError> {
    read_persisted_hitl_from_values(&read_journal_events(run_dir)?)
}

pub(crate) fn read_persisted_hitl_from_values(
    events: &[Value],
) -> Result<PersistedHitlMaps, ServiceError> {
    let mut answers: BTreeMap<String, Value> = BTreeMap::new();
    let mut confirmations: BTreeMap<String, bool> = BTreeMap::new();
    for event in events {
        match event.get("t").and_then(Value::as_str) {
            Some("run_queued") => {
                if let Some(map) = event.get("answers").and_then(Value::as_object) {
                    for (key, value) in map {
                        answers.insert(key.clone(), value.clone());
                    }
                }
                if let Some(map) = event.get("confirmations").and_then(Value::as_object) {
                    for (key, value) in map {
                        if let Some(approved) = value.as_bool() {
                            confirmations.insert(key.clone(), approved);
                        }
                    }
                }
            }
            Some("user_answered") => {
                if let (Some(id), Some(values)) = (
                    event.get("question_id").and_then(Value::as_str),
                    event.get("values").cloned(),
                ) {
                    answers.insert(id.to_string(), values);
                }
            }
            Some("user_confirmed") => {
                if let (Some(id), Some(approved)) = (
                    event.get("confirmation_id").and_then(Value::as_str),
                    event.get("approved").and_then(Value::as_bool),
                ) {
                    confirmations.insert(id.to_string(), approved);
                }
            }
            _ => {}
        }
    }
    Ok((answers, confirmations))
}

/// Whether the shared journal holds a peer cancel request for this run.
/// Control mailbox files count as pending requests so an owner running an
/// engine task observes peer cancellation even before the journal event
/// is converted by the owner (A02). Sticky journal events remain visible
/// after conversion for non-owning peers.
pub(crate) fn has_remote_cancel_request(run_dir: &Utf8Path) -> Result<bool, ServiceError> {
    for event in read_journal_events(run_dir)? {
        if event.get("t").and_then(Value::as_str) == Some("user_cancel_requested") {
            return Ok(true);
        }
    }
    if crate::run_dirs::has_pending_cancel_control(run_dir) {
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn read_events_from_meta(meta_dir: &Utf8Path) -> Result<Vec<Value>, ServiceError> {
    read_journal_values(&meta_dir.join("journal.jsonl"), JournalLimits::default())
        .map(|scan| scan.events)
        .map_err(|error| ServiceError::Invalid(error.to_string()))
}

pub fn read_run_events(run_dir: &Utf8Path) -> Result<Vec<RunEvent>, ServiceError> {
    read_journal_events(run_dir)?
        .into_iter()
        .map(|event| RunEvent::from_flat(&event).map_err(ServiceError::Invalid))
        .collect()
}
