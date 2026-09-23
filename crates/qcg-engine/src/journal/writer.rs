use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use qcg_api::RunEvent;
use serde::Serialize;
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::broadcast;

use super::read::{journal_metrics, read_journal_values};
use super::serialize::{append_serialized_json_line, serialize_bounded};
use super::types::{JournalError, JournalLimits, JournalWriter};

/// Terminal journal events use the single canonical
/// `qcg_api::is_terminal_event_kind` predicate shared with the service live
/// tail, SSE wrapper, and shared poller, so sync, metrics, shutdown markers,
/// and streams can never disagree about what "finished" means (Q2/E12).
/// There is no local duplicate predicate: every call below invokes the
/// canonical one directly.
/// Resynchronizes memory state from durable truth in a single pass: when
/// the persisted state.json already agrees with the durable tail seq it is
/// adopted with one small read (every append persists it atomically after
/// writing, so agreement means it reflects exactly the journal prefix
/// through that seq); otherwise the journal is folded once. Failures fail
/// closed instead of guessing seq values.
fn resync_durable_state(
    journal_path: &Utf8Path,
    state_path: &Utf8Path,
    limits: JournalLimits,
    durable_seq: u64,
) -> Result<crate::RunState, JournalError> {
    if let Ok(bytes) = std::fs::read(state_path)
        && let Ok(persisted) = serde_json::from_slice::<crate::RunState>(&bytes)
        && persisted.last_seq == durable_seq
    {
        if persisted.schema_version != crate::RUN_STATE_SCHEMA_VERSION {
            return Err(JournalError::InvalidEvent(format!(
                "unsupported state schema_version {}; this qcg implements {}",
                persisted.schema_version,
                crate::RUN_STATE_SCHEMA_VERSION
            )));
        }
        return Ok(persisted);
    }
    crate::RunState::fold_journal_with_limits(journal_path, limits)
}

/// Sibling lock file serializing all journal writers for one run across
/// processes and threads. Every journal append holds this lock from the
/// latest-state read through seq assignment, file append, and state.json
/// update so concurrent writers cannot assign duplicate seq values.
pub fn journal_lock_path(journal_path: &Utf8Path) -> Utf8PathBuf {
    journal_path.with_file_name(".journal.lock")
}

/// Marker written when a run reaches a terminal event through a graceful
/// writer path. A truncated tail found WITH this marker present means
/// durable history was damaged after a clean shutdown (tamper) and refuses
/// repair; WITHOUT the marker the truncation is a crash remnant and
/// repairs as before (Sensitive-8). The marker is removed on the next
/// successful non-terminal append, so a continued run never carries a stale
/// shutdown claim.
pub fn clean_shutdown_marker_path(journal_path: &Utf8Path) -> Utf8PathBuf {
    journal_path.with_file_name(".clean_shutdown")
}

/// Magic marker content: any regular file is not enough, since an
/// attacker (or stray tool) with directory write access could plant one.
/// Content is checked on read; absence still reads as no marker (Q2).
const CLEAN_SHUTDOWN_MAGIC: &[u8] = b"qcg-clean-shutdown-v1\n";

fn write_clean_shutdown_marker(journal_path: &Utf8Path) -> Result<(), JournalError> {
    std::fs::write(
        clean_shutdown_marker_path(journal_path),
        CLEAN_SHUTDOWN_MAGIC,
    )?;
    Ok(())
}

fn clear_clean_shutdown_marker(journal_path: &Utf8Path) -> Result<(), JournalError> {
    match std::fs::remove_file(clean_shutdown_marker_path(journal_path)) {
        Ok(()) => Ok(()),
        // Absence is the common case (runs that never terminated cleanly);
        // any other removal failure propagates so a stale marker can never
        // silently survive into a continued run.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(JournalError::Io(error)),
    }
}

/// Syncs the journal parent directory so the directory entry is durable
/// alongside the file bytes (Q2): same bar as `state.json` mapping persist
/// (file plus parent directory sync). Unix opens the directory and syncs;
/// non-Unix has no directory handle and relies on the file sync (best
/// effort, documented).
fn sync_parent_dir(journal_path: &Utf8Path) -> Result<(), JournalError> {
    // The path feeds only the Unix directory sync below.
    #[cfg(not(unix))]
    let _ = journal_path;
    #[cfg(unix)]
    {
        let Some(parent) = journal_path.parent() else {
            return Ok(());
        };
        std::fs::File::open(parent).and_then(|dir| dir.sync_all())?;
    }
    Ok(())
}

fn acquire_journal_lock(journal_path: &Utf8Path) -> Result<File, JournalError> {
    let lock_path = journal_lock_path(journal_path);
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    file.lock()?;
    Ok(file)
}

/// Latest durable seq without folding the whole journal: scan the tail
/// backwards for the last newline-terminated JSON line carrying `seq`.
/// Bounded to the final 64 KiB plus one over-long line so a huge journal
/// never forces a full read before the configured limits are checked.
/// `limits` bounds every fallback read: a fixed default would fail closed
/// on journals that are legal under the caller's configured limits (B09).
pub fn read_last_seq_from_tail(
    journal_path: &Utf8Path,
    limits: JournalLimits,
) -> Result<u64, JournalError> {
    Ok(
        read_last_seq_from_file_tail(journal_path, limits)?
            .max(state_json_seq_floor(journal_path)?),
    )
}

/// Tail seq of one JSONL file without the state.json floor. Used for the
/// audit stream, whose seq space is independent of `state.last_seq`.
pub fn read_last_seq_from_file_tail(
    journal_path: &Utf8Path,
    limits: JournalLimits,
) -> Result<u64, JournalError> {
    if !journal_path.exists() {
        return Ok(0);
    }
    let metadata = std::fs::metadata(journal_path)?;
    let len = metadata.len();
    if len == 0 {
        return Ok(0);
    }
    let tail_window = limits
        .scan_window_bytes
        .map(|value| value as u64)
        .unwrap_or(64 * 1024)
        .max(4 * 1024);
    const MAX_LINE: u64 = 1024 * 1024;
    let window = len.min(tail_window + MAX_LINE);
    let start = len.saturating_sub(window);
    let mut file = File::open(journal_path)?;
    use std::io::{Read as _, Seek as _};
    file.seek(std::io::SeekFrom::Start(start))?;
    let mut bytes = Vec::with_capacity(window as usize);
    file.take(window).read_to_end(&mut bytes)?;
    // Skip a possibly partial first line when the window is truncated.
    let mut slice = bytes.as_slice();
    if start > 0 {
        match slice.iter().position(|byte| *byte == b'\n') {
            Some(pos) => slice = &slice[pos + 1..],
            // The whole window is one partial line: resolve it below.
            None => slice = &[][..],
        }
    }
    let mut last_seq = 0_u64;
    for line in slice.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        // Ignore the torn tail: it has no trailing newline in the file.
        // The caller repairs it under the same lock before assigning seq.
        if let Ok(value) = serde_json::from_slice::<Value>(line)
            && let Some(seq) = value.get("seq").and_then(Value::as_u64)
        {
            last_seq = last_seq.max(seq);
        }
    }
    if start > 0 && last_seq == 0 {
        // The truncated window yielded no seq: either the final line
        // alone exceeds the window (B09) or the tail is corrupt. A stale
        // state.json must not stand in here: the persisted state may lag
        // a crashed peer's append, so resolve the final line with a
        // bounded backward scan, then with a full scan bounded by the
        // caller's limits when the bounded scan cannot resolve it. Limit
        // violations propagate: guessing a seq here would risk duplicates,
        // so resync fails closed instead.
        // Seq values grow with file order, so any complete line the
        // window scan above found would already be the global max; only
        // the found-nothing case needs this resolution.
        let huge = match read_huge_tail_seq(journal_path, len, limits) {
            Ok(seq) => seq,
            Err(first) => {
                let scan = read_journal_values(journal_path, limits).map_err(|second| {
                    // Both resolution paths failed: report both so the
                    // bounded-scan detail is not lost behind the full-scan
                    // failure.
                    JournalError::InvalidEvent(format!(
                        "journal tail seq resolution failed (bounded scan: {first}; full scan: {second})"
                    ))
                })?;
                // Explicit, not defaulted (Q2): an empty journal resolves to
                // 0, but a non-empty journal with no seq is corruption and
                // fails closed instead of silently restarting at 0.
                if scan.events.is_empty() {
                    0
                } else {
                    scan.events
                        .iter()
                        .filter_map(|event| event.get("seq").and_then(Value::as_u64))
                        .max()
                        .ok_or_else(|| {
                            JournalError::InvalidEvent(
                                "journal tail holds events without seq; refusing to guess".into(),
                            )
                        })?
                }
            }
        };
        last_seq = huge;
    }
    Ok(last_seq)
}

/// Seq of a final line larger than the tail window, found by scanning
/// backwards in bounded chunks. The scan never reads more than one event:
/// `max_event_bytes` caps it when configured (append enforces the same
/// cap, so a longer line is corruption or a narrowed limit and fails
/// closed); without a configured cap the scan stops at the file start.
/// Returns an error when no complete line exists or the line carries no
/// seq; the caller falls back to a bounded full scan.
fn read_huge_tail_seq(
    journal_path: &Utf8Path,
    len: u64,
    limits: JournalLimits,
) -> Result<u64, JournalError> {
    use std::io::{Read as _, Seek as _};
    const CHUNK: u64 = 64 * 1024;
    // No configured bound means the whole tail scans: the cap stays
    // `None` instead of a fake numeric ceiling.
    let cap = limits.max_event_bytes.map(|cap| cap as u64);
    let mut file = File::open(journal_path)?;
    // Locate the start of the last complete line by walking backwards;
    // then read that one line forward in a single bounded read.
    let mut cursor = len;
    let mut scanned = 0_u64;
    // End offset of the final complete line, fixed on the first
    // iteration: trailing newlines terminate lines instead of starting
    // an empty one. The caller repairs a torn (unterminated) tail before
    // resync, so anything else at the end is durable.
    let mut line_end: Option<u64> = None;
    loop {
        if cursor == 0 {
            // No newline in the whole file: it is a single line.
            return read_single_line_seq(&mut file, 0, line_end.unwrap_or(0), cap);
        }
        let take = cursor.min(CHUNK);
        file.seek(std::io::SeekFrom::Start(cursor - take))?;
        let mut chunk = vec![0_u8; take as usize];
        file.read_exact(&mut chunk)?;
        let mut effective = chunk.as_slice();
        if line_end.is_none() {
            let stripped = effective
                .iter()
                .rev()
                .take_while(|byte| **byte == b'\n')
                .count();
            line_end = Some(cursor - stripped as u64);
            effective = &effective[..effective.len() - stripped];
        }
        if let Some(pos) = effective.iter().rposition(|byte| *byte == b'\n') {
            // Scanning newest-first, the first newline found starts the
            // final complete line.
            let line_start = cursor - take + pos as u64 + 1;
            return read_single_line_seq(&mut file, line_start, line_end.unwrap_or(cursor), cap);
        }
        // No newline in this chunk: the line extends further back.
        // Fail as soon as the line provably exceeds the cap instead of
        // reading it to the file start. Counter overflow fails closed
        // instead of saturating past the cap check (E13).
        scanned = scanned.checked_add(take).ok_or_else(|| {
            JournalError::InvalidEvent("journal tail scan offset overflowed".into())
        })?;
        if cap.is_some_and(|cap| scanned > cap) {
            return Err(JournalError::InvalidEvent(
                "journal final line exceeds the event size limit".into(),
            ));
        }
        cursor -= take;
    }
}

/// Reads `[start, end)` as one journal line and returns its `seq`.
/// Length is enforced against `cap` before any allocation.
fn read_single_line_seq(
    file: &mut File,
    start: u64,
    end: u64,
    cap: Option<u64>,
) -> Result<u64, JournalError> {
    use std::io::{Read as _, Seek as _};
    // `end` always bounds `start` at every call site (file length or a
    // located line end); a violation fails closed instead of clamping to an
    // empty line (E13).
    let line_len = end.checked_sub(start).ok_or_else(|| {
        JournalError::InvalidEvent("journal tail offsets are inconsistent".into())
    })?;
    if line_len == 0 {
        return Err(JournalError::InvalidEvent(
            "journal tail holds no complete line".into(),
        ));
    }
    if cap.is_some_and(|cap| line_len > cap) {
        return Err(JournalError::InvalidEvent(
            "journal final line exceeds the event size limit".into(),
        ));
    }
    file.seek(std::io::SeekFrom::Start(start))?;
    let mut line = vec![0_u8; line_len as usize];
    file.read_exact(&mut line)?;
    let value: Value = serde_json::from_slice(&line)
        .map_err(|_| JournalError::InvalidEvent("journal final line is not JSON".into()))?;
    value
        .get("seq")
        .and_then(Value::as_u64)
        .ok_or_else(|| JournalError::InvalidEvent("journal final line carries no seq".into()))
}

/// state.json as a seq lower bound only, combined with the window scan
/// via max. The persisted state is written after its journal append, so it
/// is never newer than the journal: staleness only lowers it, which max
/// absorbs. Conversion failures propagate (Q2): an unreadable or corrupt
/// state file fails closed instead of silently equating with a zero bound.
/// Absence (no state file yet) is the only case that resolves to 0.
fn state_json_seq_floor(journal_path: &Utf8Path) -> Result<u64, JournalError> {
    let state_path = journal_path.with_file_name("state.json");
    match std::fs::read(&state_path) {
        Ok(bytes) => {
            let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
                JournalError::InvalidEvent(format!(
                    "state.json is corrupt and cannot bound the journal tail: {error}"
                ))
            })?;
            value
                .get("last_seq")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    JournalError::InvalidEvent(
                        "state.json has no usable last_seq; refusing to guess the tail seq".into(),
                    )
                })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(JournalError::Io(error)),
    }
}

impl JournalWriter {
    pub fn create(
        path: &Utf8Path,
        run_id: impl Into<String>,
        mirror_stdout: bool,
        event_sender: Option<broadcast::Sender<RunEvent>>,
    ) -> Result<Self, JournalError> {
        Self::create_with_limits(
            path,
            run_id,
            mirror_stdout,
            event_sender,
            JournalLimits::default(),
        )
    }

    pub fn create_with_limits(
        path: &Utf8Path,
        run_id: impl Into<String>,
        mirror_stdout: bool,
        event_sender: Option<broadcast::Sender<RunEvent>>,
        limits: JournalLimits,
    ) -> Result<Self, JournalError> {
        Self::create_with_policies(
            path,
            run_id,
            mirror_stdout,
            event_sender,
            limits,
            qcg_policy::AuditPolicy::default(),
            qcg_policy::AuditLimits::default(),
        )
    }

    /// Opens the durable journal plus the sibling observation stream under
    /// one lock. The durable fold never reads the observation stream, so an
    /// audit policy can never change resume/replay semantics.
    pub fn create_with_policies(
        path: &Utf8Path,
        run_id: impl Into<String>,
        mirror_stdout: bool,
        event_sender: Option<broadcast::Sender<RunEvent>>,
        limits: JournalLimits,
        audit_policy: qcg_policy::AuditPolicy,
        audit_limits: qcg_policy::AuditLimits,
    ) -> Result<Self, JournalError> {
        validate_limits(limits)?;
        if let Err(resource) = audit_limits.validate() {
            return Err(JournalError::InvalidLimit { resource });
        }
        let run_id = run_id.into();
        if run_id.trim().is_empty() {
            return Err(JournalError::InvalidEvent(
                "journal run_id must be non-empty".into(),
            ));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let audit_path = audit_path_for(path);
        // Repair and initial fold hold the cross-process journal lock so a
        // live writer can never race with torn-tail truncation.
        let _journal_guard = acquire_journal_lock(path)?;
        repair_truncated_tail_locked(path, limits)?;
        repair_truncated_tail_locked(&audit_path, limits)?;
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let state_path = path.with_file_name("state.json");
        // Single scan feeds both stats and the fold; a second full read
        // would double I/O on every open.
        let scan = read_journal_values(path, limits)?;
        let mut state = crate::RunState::default();
        for event in &scan.events {
            qcg_api::RunEvent::from_flat(event).map_err(|message| {
                JournalError::InvalidEvent(format!("invalid journal event: {message}"))
            })?;
            state.apply(event)?;
        }
        if state.schema_version != crate::RUN_STATE_SCHEMA_VERSION {
            return Err(JournalError::InvalidEvent(format!(
                "unsupported state schema_version {}; this qcg implements {}",
                state.schema_version,
                crate::RUN_STATE_SCHEMA_VERSION
            )));
        }
        if let Some(existing) = state.run_id.as_deref()
            && existing != run_id
        {
            return Err(JournalError::InvalidEvent(format!(
                "journal run_id mismatch: expected `{run_id}`, found `{existing}`"
            )));
        }
        // Seed identity at creation: budget reservations and other
        // run-keyed maps must never observe an ownerless state, even before
        // the first event folds. The audit seq is recovered from the
        // observation stream alone: durable and audit seqs share one space,
        // but only audit records advance `audit_seq`.
        state.run_id = Some(run_id.clone());
        let mut audit_seed_failed = false;
        let audit_stats = match read_journal_values(&audit_path, audit_read_limits(audit_limits)) {
            Ok(scan) => Some(scan.stats),
            // A stream that already breaches the configured audit bounds
            // degrades instead of failing the run: observation records are
            // policy data. The durable `audit_degraded` record is emitted by
            // the first observation append (or never, when none is written).
            Err(_) => {
                audit_seed_failed = true;
                None
            }
        };
        let audit_tail =
            match read_last_seq_from_file_tail(&audit_path, audit_read_limits(audit_limits)) {
                Ok(tail) => tail,
                Err(_) => {
                    audit_seed_failed = true;
                    0
                }
            };
        state.audit_seq = audit_tail;
        state.persist_atomic_with_limits(&state_path, limits.max_state_bytes)?;
        Ok(Self {
            run_id,
            journal_path: path.to_owned(),
            file: Arc::new(Mutex::new(file)),
            state: Arc::new(Mutex::new(state)),
            state_path,
            mirror_stdout,
            event_sender,
            limits,
            stats: Arc::new(Mutex::new(scan.stats)),
            // Unset until the first successful append: creation itself
            // repairs, so a create-time length is not a valid floor yet.
            floor_len: Arc::new(Mutex::new(None)),
            audit_path,
            audit_file: Arc::new(Mutex::new(None)),
            audit_policy,
            audit_limits,
            audit_stats: Arc::new(Mutex::new(audit_stats)),
            audit_degraded: Arc::new(Mutex::new(audit_seed_failed)),
        })
    }

    /// Appends one record. Durable kinds take the durability path; observation
    /// kinds are classified by [`qcg_policy::event_class`] and persisted
    /// according to the resolved audit policy.
    pub fn event(&self, kind: &str, payload: impl Serialize) -> Result<(), JournalError> {
        if qcg_policy::event_class(kind) == qcg_policy::EventClass::Observation {
            return self.observation_event(kind, payload);
        }
        self.durable_event(kind, payload)
    }

    /// Persists one observation record, or skips it per policy. Never fails
    /// the run: a write failure or limit breach degrades audit persistence
    /// and records one durable `audit_degraded` marker.
    fn observation_event(&self, kind: &str, payload: impl Serialize) -> Result<(), JournalError> {
        match self.audit_policy.mode_for(kind) {
            qcg_policy::AuditMode::Off => return Ok(()),
            qcg_policy::AuditMode::Full | qcg_policy::AuditMode::Digest => {}
        }
        if *self
            .audit_degraded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
            return Ok(());
        }
        let payload = serialize_bounded(&payload, self.limits.max_event_bytes, "event")?;
        let mut value = serde_json::from_slice::<Value>(&payload)?;
        if !value.is_object() {
            value = json!({ "value": value });
        }
        if self.audit_policy.mode_for(kind) == qcg_policy::AuditMode::Digest {
            value = audit_digest(&value)?;
        }
        let journal_guard = acquire_journal_lock(&self.journal_path)?;
        repair_truncated_tail_locked(&self.audit_path, self.limits)?;
        let mut outcome = Ok(());
        let event = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let seq = state
                .last_seq
                .max(state.audit_seq)
                .checked_add(1)
                .ok_or_else(|| JournalError::InvalidEvent("journal seq overflowed".into()))?;
            let event_run_id = self.run_id.clone();
            {
                let object = value.as_object_mut().ok_or(JournalError::InvalidPayload)?;
                object.insert("t".into(), Value::String(kind.into()));
                object.insert("ts".into(), Value::String(Utc::now().to_rfc3339()));
                object.insert("seq".into(), Value::Number(seq.into()));
                object.insert("run_id".into(), Value::String(event_run_id.clone()));
                object.insert(
                    "trace_id".into(),
                    Value::String(qcg_api::trace_id_for_run(&event_run_id)),
                );
                object.insert(
                    "span_id".into(),
                    Value::String(qcg_api::span_id_for_seq(seq)),
                );
                let parent_scope = object
                    .get("node")
                    .and_then(Value::as_str)
                    .map(|node| format!("step:{node}"))
                    .unwrap_or_else(|| "run".to_string());
                object.insert(
                    "parent_span_id".into(),
                    Value::String(qcg_api::span_id_for_scope(&event_run_id, &parent_scope)),
                );
            }
            let event = RunEvent::from_flat(&value).map_err(JournalError::InvalidEvent)?;
            let bytes =
                serialize_bounded(&value, self.audit_limits.max_event_bytes, "audit event")?;
            let mut audit_file = self
                .audit_file
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let open_error = match audit_file.as_mut() {
                Some(_) => None,
                None => match OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.audit_path)
                {
                    Ok(file) => {
                        *audit_file = Some(file);
                        None
                    }
                    Err(error) => Some(error),
                },
            };
            if let Some(error) = open_error {
                outcome = Err(JournalError::Io(error));
            } else {
                let mut audit_stats = self
                    .audit_stats
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let mut stats = audit_stats.take().unwrap_or_default();
                let limits = audit_append_limits(self.audit_limits);
                let file = audit_file.as_mut().ok_or(JournalError::InvalidPayload)?;
                match append_serialized_json_line(file, bytes, &mut stats, limits) {
                    Ok(()) => {
                        state.audit_seq = seq;
                        *audit_stats = Some(stats);
                    }
                    Err(error) => outcome = Err(error),
                }
            }
            event
        };
        // Release the journal lock before any degradation record: the
        // durable path takes it again, and a lock held on one descriptor
        // blocks a second acquisition even in the same thread.
        drop(journal_guard);
        if outcome.is_ok() {
            // No state.json persist per observation record: the audit seq is
            // recoverable from the observation tail at writer open, so
            // persisting here would add an atomic write to every delta
            // without adding a durability guarantee. The next durable record
            // persists the counter along with the folded state.
        } else {
            // Degrade once, explicitly: record the reason durably and stop
            // persisting observation records. The run itself proceeds.
            let first = {
                let mut degraded = self
                    .audit_degraded
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let first = !*degraded;
                *degraded = true;
                first
            };
            if first {
                let reason = match &outcome {
                    Err(JournalError::LimitExceeded { .. })
                    | Err(JournalError::EventCountExceeded { .. }) => "limit",
                    _ => "write_failure",
                };
                self.durable_event(
                    "audit_degraded",
                    json!({ "reason": reason, "record": kind }),
                )?;
            }
        }
        if self.mirror_stdout {
            let line = serde_json::to_string(&event)?;
            tracing::info!("{line}");
        }
        if let Some(sender) = &self.event_sender {
            let _ = sender.send(event);
        }
        Ok(())
    }

    fn durable_event(&self, kind: &str, payload: impl Serialize) -> Result<(), JournalError> {
        let payload = serialize_bounded(&payload, self.limits.max_event_bytes, "event")?;
        let mut value = serde_json::from_slice::<Value>(&payload)?;
        if !value.is_object() {
            value = json!({ "value": value });
        }
        let object = value.as_object_mut().ok_or(JournalError::InvalidPayload)?;
        // Every terminal event carries the accumulated cost metrics so wasted
        // spend on failed or canceled runs stays queryable afterwards.
        // Metrics must never break the event write itself.
        if qcg_api::is_terminal_event_kind(kind) {
            match journal_metrics(
                &self
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .budget,
            ) {
                Ok(metrics) => {
                    object.insert("metrics".into(), serde_json::to_value(metrics)?);
                }
                Err(error) => {
                    tracing::warn!("terminal event without metrics: {error}");
                }
            }
        }
        // Cross-process serialization: hold the journal lock while
        // resynchronizing seq, appending, and persisting state.json.
        let _journal_guard = acquire_journal_lock(&self.journal_path)?;
        // Repair a torn tail under the same lock before assigning seq so a
        // crash fragment can never fuse with the next append into an
        // InvalidLine. The file handle uses O_APPEND, so truncation remains
        // correct for subsequent writes.
        repair_truncated_tail_locked(&self.journal_path, self.limits)?;
        let (line, event) = {
            let mut file = self.file.lock().unwrap_or_else(PoisonError::into_inner);
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let mut stats = self.stats.lock().unwrap_or_else(PoisonError::into_inner);
            let mut floor = self
                .floor_len
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            // Byte-offset checkpoint (B09): every byte this writer ever
            // observed was a complete line or peer-torn residue the repair
            // above just stripped. A post-repair file shorter than the
            // previous checkpoint means something truncated durable history
            // outside the journal lock; assigning seq on top would reuse
            // seq values, so fail closed instead.
            let repaired_len = std::fs::metadata(&self.journal_path)
                .map(|metadata| metadata.len())
                .map_err(JournalError::Io)?;
            if let Some(min_len) = *floor
                && repaired_len < min_len
            {
                return Err(JournalError::InvalidEvent(format!(
                    "journal truncated outside the journal lock ({} bytes, checkpoint was {min_len}); refusing to assign seq",
                    repaired_len,
                )));
            }
            // A peer writer may have appended while this writer was busy.
            // Resynchronize from durable state so seq stays unique and
            // monotonic even across processes. Any resync failure is
            // fail-closed: continuing with a stale memory seq would risk
            // duplicates and last-writer-wins state.
            let durable_seq = read_last_seq_from_tail(&self.journal_path, self.limits)?;
            if durable_seq > state.last_seq {
                *state = resync_durable_state(
                    &self.journal_path,
                    &self.state_path,
                    self.limits,
                    durable_seq,
                )?;
                // Rebuild stats from durable truth: limits below must
                // enforce against what is on disk, not what this writer
                // last wrote itself. Otherwise a peer's appends let this
                // writer exceed configured event/byte caps (B09).
                // Unrepresentable counters fail closed instead of
                // saturating to a wrong bound (E13).
                let bytes = std::fs::metadata(&self.journal_path)
                    .map(|metadata| metadata.len())
                    .map_err(JournalError::Io)?;
                stats.bytes = usize::try_from(bytes).map_err(|_| {
                    JournalError::InvalidEvent("journal byte count is not representable".into())
                })?;
                stats.events = usize::try_from(state.last_seq).map_err(|_| {
                    JournalError::InvalidEvent("journal event count is not representable".into())
                })?;
            }
            // Durable and observation records share one seq space. After a
            // resync (or refold) the in-memory audit counter may lag the
            // observation stream; recover it from disk so a later audit
            // append cannot reuse a seq. A stream that cannot be resolved
            // degrades audit persistence instead of failing the run.
            match read_last_seq_from_file_tail(
                &self.audit_path,
                audit_read_limits(self.audit_limits),
            ) {
                Ok(audit_tail) => state.audit_seq = state.audit_seq.max(audit_tail),
                Err(_) => {
                    *self
                        .audit_degraded
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner) = true;
                }
            }
            // Seq overflow fails closed: wrapping would duplicate seq values
            // and fork journal state (E13).
            let seq = state
                .last_seq
                .max(state.audit_seq)
                .max(durable_seq)
                .checked_add(1)
                .ok_or_else(|| JournalError::InvalidEvent("journal seq overflowed".into()))?;
            if let Some(payload_run_id) = object.get("run_id").and_then(Value::as_str)
                && payload_run_id != self.run_id
            {
                return Err(JournalError::InvalidEvent(format!(
                    "journal event run_id mismatch: expected `{}`, found `{payload_run_id}`",
                    self.run_id
                )));
            }
            let event_run_id = self.run_id.clone();
            object.insert("t".into(), Value::String(kind.into()));
            object.insert("ts".into(), Value::String(Utc::now().to_rfc3339()));
            object.insert("seq".into(), Value::Number(seq.into()));
            object.insert("run_id".into(), Value::String(event_run_id.clone()));
            object.insert(
                "trace_id".into(),
                Value::String(qcg_api::trace_id_for_run(&event_run_id)),
            );
            object.insert(
                "span_id".into(),
                Value::String(qcg_api::span_id_for_seq(seq)),
            );
            let parent_scope = object
                .get("node")
                .and_then(Value::as_str)
                .map(|node| format!("step:{node}"))
                .unwrap_or_else(|| "run".to_string());
            if !matches!(kind, "run_queued" | "run_started") {
                object.insert(
                    "parent_span_id".into(),
                    Value::String(qcg_api::span_id_for_scope(&event_run_id, &parent_scope)),
                );
            }
            let event = RunEvent::from_flat(&value).map_err(JournalError::InvalidEvent)?;
            let mut next_state = state.clone();
            next_state.apply(&value)?;
            let state_bytes = serialize_bounded(&next_state, self.limits.max_state_bytes, "state")?;
            let bytes = serialize_bounded(&value, self.limits.max_event_bytes, "event")?;
            let line = String::from_utf8(bytes.clone()).map_err(|error| {
                JournalError::InvalidEvent(format!("journal event is not UTF-8: {error}"))
            })?;
            append_serialized_json_line(&mut *file, bytes, &mut stats, self.limits)?;
            // Durability ordering (Q2): terminal events seal the run and
            // operation mappings (`operation_started`/`operation_finished`)
            // precede any external send, so both fsync the journal file
            // before the atomic state persist below. The state persist
            // (file plus parent directory sync) follows, so the mapping is
            // durable in both the journal and state.json before the guard
            // returns and any gateway is touched. The journal parent
            // directory is also synced after the file sync (Q2): same
            // durability bar as mapping persist, so the directory entry is
            // durable alongside the bytes.
            if qcg_api::is_terminal_event_kind(kind)
                || matches!(kind, "operation_started" | "operation_finished")
            {
                file.sync_data()?;
                sync_parent_dir(&self.journal_path)?;
            }
            *state = next_state;
            crate::RunState::persist_serialized_atomic(&self.state_path, &state_bytes)?;
            // Advance the truncation checkpoint only after the append and
            // its state persist both succeeded: this length is the new
            // floor every future resync must meet or exceed. The length is
            // read once and reused here: a second metadata call could
            // observe a peer's concurrent append and record a floor this
            // writer never verified (single I/O pass). When the length
            // cannot be observed, the previous floor stands: both are valid
            // lower bounds, and inventing a length here could only mask a
            // future truncation.
            let appended_len = std::fs::metadata(&self.journal_path)
                .map(|metadata| metadata.len())
                .map_err(JournalError::Io)?;
            *floor = Some(appended_len);
            // Clean-shutdown marker (Sensitive-8): a terminal event seals
            // the journal, so the marker is written; any other event means
            // the run continues, so a stale marker is cleared before the
            // call reports success. A failed terminal marker is a durability
            // hole and fails the operation (Q2): warn-only would leave a
            // sealed run without its shutdown claim, so both write and
            // clear failures propagate.
            if qcg_api::is_terminal_event_kind(kind) {
                write_clean_shutdown_marker(&self.journal_path)?;
            } else {
                clear_clean_shutdown_marker(&self.journal_path)?;
            }
            (line, event)
        };
        if self.mirror_stdout {
            // The mirrored line is the already-redacted journal line
            // (secret values are digests or placeholders by construction),
            // shown on the operator's own terminal for direct runs only.
            // Library output routes through tracing, never stdout directly
            // (Q2): the CLI subscriber renders it to the terminal.
            tracing::info!("{line}");
        }
        if let Some(sender) = &self.event_sender {
            // A send failure only means no receiver is listening; the event
            // is already durable above, so it is intentionally ignored here
            // and documented as best-effort broadcast (E13).
            let _ = sender.send(event);
        }
        Ok(())
    }

    /// Single durable append without a long-lived writer. Holds the
    /// cross-process journal lock from torn-tail repair through fresh fold,
    /// seq assignment, append, and state.json persist so service-side control
    /// events never duplicate seq values assigned by a running engine.
    pub fn append_single_event(
        journal_path: &Utf8Path,
        run_id: &str,
        kind: &str,
        payload: Value,
        limits: JournalLimits,
        event_sender: Option<broadcast::Sender<RunEvent>>,
    ) -> Result<RunEvent, JournalError> {
        Self::append_single_event_if(
            journal_path,
            run_id,
            kind,
            payload,
            limits,
            event_sender,
            |_| Ok(()),
        )
    }

    /// Check-and-append under the journal lock: folds the latest durable
    /// state, runs `check` against it, and appends only when the check
    /// passes. Racing peers serialize here, so exactly one conflicting
    /// acceptance wins; the loser observes `PreconditionFailed`.
    pub fn append_single_event_if(
        journal_path: &Utf8Path,
        run_id: &str,
        kind: &str,
        payload: Value,
        limits: JournalLimits,
        event_sender: Option<broadcast::Sender<RunEvent>>,
        check: impl FnOnce(&crate::RunState) -> Result<(), JournalError>,
    ) -> Result<RunEvent, JournalError> {
        let mut events = Self::append_events_if(
            journal_path,
            run_id,
            vec![(kind, payload)],
            limits,
            event_sender,
            check,
        )?;
        events
            .pop()
            .ok_or_else(|| JournalError::InvalidEvent("journal batch produced no events".into()))
    }

    /// Atomic multi-append under a single journal-lock hold: one repair, one
    /// fold, one check, consecutive seq values, one state persist. No other
    /// writer can interleave between the batched events. (A crash mid-batch
    /// still leaves a prefix durable; batches group logically joint
    /// settlements, not transactions.)
    pub fn append_events_if(
        journal_path: &Utf8Path,
        run_id: &str,
        events: Vec<(&str, Value)>,
        limits: JournalLimits,
        event_sender: Option<broadcast::Sender<RunEvent>>,
        check: impl FnOnce(&crate::RunState) -> Result<(), JournalError>,
    ) -> Result<Vec<RunEvent>, JournalError> {
        validate_limits(limits)?;
        if run_id.trim().is_empty() {
            return Err(JournalError::InvalidEvent(
                "journal run_id must be non-empty".into(),
            ));
        }
        if events.is_empty() {
            return Err(JournalError::InvalidEvent(
                "journal batch must hold at least one event".into(),
            ));
        }
        if let Some(parent) = journal_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _journal_guard = acquire_journal_lock(journal_path)?;
        repair_truncated_tail_locked(journal_path, limits)?;
        // Normalize every payload before folding so validation failures
        // reject the whole batch before anything appends.
        let mut normalized = Vec::with_capacity(events.len());
        for (kind, mut value) in events {
            if !value.is_object() {
                value = json!({ "value": value });
            }
            {
                let object = value.as_object_mut().ok_or(JournalError::InvalidPayload)?;
                if let Some(payload_run_id) = object.get("run_id").and_then(Value::as_str)
                    && payload_run_id != run_id
                {
                    return Err(JournalError::InvalidEvent(format!(
                        "journal event run_id mismatch: expected `{run_id}`, found `{payload_run_id}`"
                    )));
                }
            }
            normalized.push((kind, value));
        }
        let mut state = crate::RunState::fold_journal_with_limits(journal_path, limits)?;
        if let Some(existing) = state.run_id.as_deref()
            && existing != run_id
        {
            return Err(JournalError::InvalidEvent(format!(
                "journal run_id mismatch: expected `{run_id}`, found `{existing}`"
            )));
        }
        // Control mutations persist state.json; preserve the observation
        // seq recovered from the sibling stream so a later audit append
        // cannot reuse a seq. A stream that cannot be resolved keeps the
        // folded value: audit persistence degrades on its own path.
        if let Ok(audit_tail) = read_last_seq_from_file_tail(
            &audit_path_for(journal_path),
            JournalLimits {
                max_event_bytes: limits.max_event_bytes,
                ..JournalLimits::default()
            },
        ) {
            state.audit_seq = state.audit_seq.max(audit_tail);
        }
        check(&state)?;
        // The batch continues the run unless it seals it: clear a stale
        // clean-shutdown marker before appending so a continued run never
        // carries a shutdown claim into a later torn tail (Sensitive-8).
        // Clearing before the first append keeps marker failures from
        // masking append outcomes.
        let batch_seals_run = normalized
            .iter()
            .any(|(kind, _)| qcg_api::is_terminal_event_kind(kind));
        if !batch_seals_run {
            clear_clean_shutdown_marker(journal_path)?;
        }
        // Stats without a second full scan: strict seq monotonicity plus
        // refold-on-truncate keeps event count exactly at last_seq, and the
        // file length is the byte total.
        let file_len = std::fs::metadata(journal_path)?.len();
        let mut stats = super::types::JournalStats {
            bytes: usize::try_from(file_len).map_err(|_| {
                JournalError::InvalidEvent("journal byte count is not representable".into())
            })?,
            events: usize::try_from(state.last_seq).map_err(|_| {
                JournalError::InvalidEvent("journal event count is not representable".into())
            })?,
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(journal_path)?;
        let event_run_id = run_id.to_string();
        let mut appended = Vec::with_capacity(normalized.len());
        let mut needs_sync = false;
        for (kind, mut value) in normalized {
            // Reject duplicate or out-of-order service writes instead of
            // silently keeping last-writer-wins state. The observation
            // stream shares this seq space, so the next seq clears both
            // counters. Seq overflow fails closed instead of wrapping into
            // duplicates (E13).
            let seq = state
                .last_seq
                .max(state.audit_seq)
                .checked_add(1)
                .ok_or_else(|| JournalError::InvalidEvent("journal seq overflowed".into()))?;
            {
                let object = value.as_object_mut().ok_or(JournalError::InvalidPayload)?;
                object.insert("t".into(), Value::String(kind.into()));
                object.insert("ts".into(), Value::String(Utc::now().to_rfc3339()));
                object.insert("seq".into(), Value::Number(seq.into()));
                object.insert("run_id".into(), Value::String(event_run_id.clone()));
                object.insert(
                    "trace_id".into(),
                    Value::String(qcg_api::trace_id_for_run(&event_run_id)),
                );
                object.insert(
                    "span_id".into(),
                    Value::String(qcg_api::span_id_for_seq(seq)),
                );
            }
            let event = RunEvent::from_flat(&value).map_err(JournalError::InvalidEvent)?;
            state.apply(&value)?;
            let bytes = serialize_bounded(&value, limits.max_event_bytes, "event")?;
            append_serialized_json_line(&mut file, bytes, &mut stats, limits)?;
            if qcg_api::is_terminal_event_kind(kind)
                || matches!(kind, "operation_started" | "operation_finished")
            {
                needs_sync = true;
            }
            appended.push(event);
        }
        if needs_sync {
            file.sync_data()?;
            sync_parent_dir(journal_path)?;
        }
        let state_bytes = serialize_bounded(&state, limits.max_state_bytes, "state")?;
        let state_path = journal_path.with_file_name("state.json");
        crate::RunState::persist_serialized_atomic(&state_path, &state_bytes)?;
        // A sealing batch writes the clean-shutdown marker after the state
        // persist (Sensitive-8). A marker failure is a durability hole and
        // fails the batch (Q2): warn-only would leave a sealed run without
        // its shutdown claim.
        if batch_seals_run {
            write_clean_shutdown_marker(journal_path)?;
        }
        if let Some(sender) = &event_sender {
            for event in &appended {
                // Broadcast only: the batch is durable, so a missing
                // receiver is intentionally ignored here and documented as
                // best-effort (E13).
                let _ = sender.send(event.clone());
            }
        }
        Ok(appended)
    }

    pub fn clone_for_parallel(&self) -> Result<Self, JournalError> {
        Ok(Self {
            run_id: self.run_id.clone(),
            journal_path: self.journal_path.clone(),
            file: Arc::clone(&self.file),
            state: Arc::clone(&self.state),
            state_path: self.state_path.clone(),
            mirror_stdout: self.mirror_stdout,
            event_sender: self.event_sender.clone(),
            limits: self.limits,
            stats: Arc::clone(&self.stats),
            floor_len: Arc::clone(&self.floor_len),
            audit_path: self.audit_path.clone(),
            audit_file: Arc::clone(&self.audit_file),
            audit_policy: self.audit_policy.clone(),
            audit_limits: self.audit_limits,
            audit_stats: Arc::clone(&self.audit_stats),
            audit_degraded: Arc::clone(&self.audit_degraded),
        })
    }

    pub fn state(&self) -> crate::RunState {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Observation stream path for one run: a sibling of the durable journal.
pub fn audit_path_for(journal_path: &Utf8Path) -> Utf8PathBuf {
    journal_path.with_file_name("audit.jsonl")
}

/// Digest projection for `AuditMode::Digest`: every string in the
/// observation payload is replaced by its content digest. Types and schema
/// shape are preserved, so the record still parses as its typed event while
/// the content itself is not retained.
fn audit_digest(value: &Value) -> Result<Value, JournalError> {
    use sha2::Digest as _;
    fn digest_strings(value: &mut Value) {
        match value {
            Value::String(text) => {
                let digest = hex::encode(sha2::Sha256::digest(text.as_bytes()));
                *text = format!("sha256:{digest}");
            }
            Value::Array(items) => {
                for item in items {
                    digest_strings(item);
                }
            }
            Value::Object(map) => {
                for item in map.values_mut() {
                    digest_strings(item);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
    let mut digested = value.clone();
    digest_strings(&mut digested);
    Ok(digested)
}

/// Limits used when scanning the observation stream (seed, resync, merge).
pub(crate) fn audit_read_limits(limits: qcg_policy::AuditLimits) -> JournalLimits {
    JournalLimits {
        max_event_bytes: limits.max_event_bytes,
        max_total_bytes: limits.max_total_bytes,
        max_event_count: limits.max_event_count,
        max_state_bytes: None,
        scan_window_bytes: None,
    }
}

/// Limits enforced while appending observation records.
pub(crate) fn audit_append_limits(limits: qcg_policy::AuditLimits) -> JournalLimits {
    audit_read_limits(limits)
}

pub(crate) fn validate_limits(limits: JournalLimits) -> Result<(), JournalError> {
    for (resource, value) in [
        ("max_event_bytes", limits.max_event_bytes),
        ("max_total_bytes", limits.max_total_bytes),
        ("max_event_count", limits.max_event_count),
        ("max_state_bytes", limits.max_state_bytes),
        ("scan_window_bytes", limits.scan_window_bytes),
    ] {
        if value == Some(0) {
            return Err(JournalError::InvalidLimit { resource });
        }
    }
    Ok(())
}

/// Repair a torn trailing line before appending. Callers must hold the
/// cross-process journal lock: repairing while a peer writer is appending
/// would mistake a live partial write for crash residue and truncate it.
///
/// The reader ignores an incomplete final line without a newline, but
/// appending after it would fuse two JSON objects into one invalid line.
/// Only newline-terminated lines are durable: a trailing fragment without
/// a newline is either committed (valid complete JSON gains its newline)
/// or truncated to the last newline boundary. Middle-line corruption is
/// never silently dropped and still surfaces as `InvalidLine` on read.
/// Bounded torn-tail repair for maintenance paths that already hold the
/// journal lock and the run execution lease with no live writer.
pub fn repair_truncated_tail_locked(
    path: &Utf8Path,
    limits: JournalLimits,
) -> Result<(), JournalError> {
    if !path.exists() {
        return Ok(());
    }
    // Bounded tail inspection: only the configured scan window plus one
    // line is read before the configured limits are enforced, so a huge
    // journal never forces a full read here (C05).
    let scan_window = limits
        .scan_window_bytes
        .map(|value| value as u64)
        .unwrap_or(1024 * 1024)
        .max(4 * 1024);
    let metadata = std::fs::metadata(path)?;
    let len = metadata.len();
    if len == 0 {
        return Ok(());
    }
    if len <= scan_window {
        let bytes = std::fs::read(path)?;
        if bytes.is_empty() || bytes.last() == Some(&b'\n') {
            return Ok(());
        }
        return repair_tail_bytes(path, &bytes, 0);
    }
    let start = len.saturating_sub(scan_window);
    let mut file = File::open(path)?;
    use std::io::{Read as _, Seek as _};
    file.seek(std::io::SeekFrom::Start(start))?;
    let mut bytes = Vec::with_capacity(scan_window as usize);
    file.take(scan_window).read_to_end(&mut bytes)?;
    if bytes.last() == Some(&b'\n') {
        return Ok(());
    }
    // When truncated, the last newline in the window bounds the torn tail.
    // Without any newline the tail may extend before the window; backscan
    // in bounded windows instead of loading the whole journal (C05).
    match bytes.iter().rposition(|byte| *byte == b'\n') {
        Some(pos) => {
            let tail = bytes[pos + 1..].to_vec();
            let tail_start = start + pos as u64 + 1;
            repair_tail_at(path, &tail, tail_start)
        }
        None => repair_tail_backscan(path, start, bytes, scan_window),
    }
}

/// Extends a newline-free tail scan towards the file start in bounded
/// windows. Memory stays flat: one window buffer plus a capped tail. A tail
/// that outgrows the cap is corruption too large to repair blindly and
/// fails closed instead of loading an unbounded journal into memory.
fn repair_tail_backscan(
    path: &Utf8Path,
    mut scan_end: u64,
    mut tail: Vec<u8>,
    scan_window: u64,
) -> Result<(), JournalError> {
    // The backscan bound scales with the configured window so repair memory
    // stays flat and proportional to one explicit contract bound.
    let max_backscan = scan_window.saturating_mul(16);
    use std::io::{Read as _, Seek as _};
    loop {
        let scan_start = scan_end.saturating_sub(scan_window);
        let mut file = File::open(path)?;
        file.seek(std::io::SeekFrom::Start(scan_start))?;
        let mut chunk = vec![0u8; (scan_end - scan_start) as usize];
        file.read_exact(&mut chunk)?;
        if let Some(pos) = chunk.iter().rposition(|byte| *byte == b'\n') {
            let mut full_tail = chunk[pos + 1..].to_vec();
            full_tail.extend_from_slice(&tail);
            let tail_start = scan_start + pos as u64 + 1;
            return repair_tail_at(path, &full_tail, tail_start);
        }
        if tail.len() + chunk.len() > max_backscan as usize {
            return Err(JournalError::InvalidEvent(format!(
                "journal tail exceeds {max_backscan} bytes without a line boundary; refusing unbounded repair"
            )));
        }
        chunk.extend_from_slice(&tail);
        tail = chunk;
        if scan_start == 0 {
            return repair_tail_bytes(path, &tail, 0);
        }
        scan_end = scan_start;
    }
}

fn repair_tail_bytes(path: &Utf8Path, bytes: &[u8], base: u64) -> Result<(), JournalError> {
    let last_newline = bytes.iter().rposition(|byte| *byte == b'\n');
    let tail_start = last_newline.map(|pos| pos + 1).unwrap_or(0);
    repair_tail_at(path, &bytes[tail_start..], base + tail_start as u64)
}

fn repair_tail_at(path: &Utf8Path, tail: &[u8], tail_start: u64) -> Result<(), JournalError> {
    // Tamper gate (Sensitive-8): a torn tail on a journal that shut down
    // cleanly is damage to durable history, not a crash remnant. The marker
    // must read as a non-symlink regular file with the exact magic content
    // below; absence reads as no marker, while any other inspection failure
    // propagates so a possibly tampered journal never repairs on an
    // unreadable marker (fail closed). Coupling note (Q2): the marker
    // write itself fails the operation on failure (see `JournalWriter::event`
    // and `append_events_if`), so a missing marker at repair time means the
    // terminal event never reported success — either no terminal event ran
    // or its failure already surfaced. The truncated tail still repairs
    // conservatively (prior events survive, the fragment is dropped), never
    // by inventing events.
    let clean_shutdown = match std::fs::read(clean_shutdown_marker_path(path)) {
        Ok(bytes) => bytes == CLEAN_SHUTDOWN_MAGIC,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(JournalError::Io(error)),
    };
    // A non-symlink check stays: a link at the marker path is damage, never
    // a shutdown claim, even with magic content.
    if clean_shutdown
        && std::fs::symlink_metadata(clean_shutdown_marker_path(path))
            .is_ok_and(|meta| meta.file_type().is_symlink())
    {
        return Err(JournalError::InvalidEvent(
            "clean-shutdown marker is a symbolic link; refusing repair as possible tampering"
                .into(),
        ));
    }
    if clean_shutdown {
        return Err(JournalError::InvalidEvent(
            "journal tail is truncated but a clean-shutdown marker is present; refusing repair as possible tampering".into(),
        ));
    }
    if tail.iter().all(u8::is_ascii_whitespace) {
        // Trailing whitespace without a newline carries no event; truncate
        // it so the next append starts on a clean line.
        truncate_to(path, tail_start)?;
        return Ok(());
    }
    match serde_json::from_slice::<serde_json::Value>(tail) {
        Ok(_) => {
            // Complete JSON without its newline: commit it so the event is
            // preserved with the same semantics as a clean shutdown.
            let file = OpenOptions::new().append(true).open(path)?;
            use std::io::Write as _;
            let mut file = file;
            file.write_all(b"\n")?;
            file.sync_data()?;
            Ok(())
        }
        Err(_) => {
            // Torn JSON, partial UTF-8, or over-long fragment: drop only the
            // trailing fragment so prior newline-terminated events survive
            // and the next append does not fuse lines.
            truncate_to(path, tail_start)?;
            Ok(())
        }
    }
}

fn truncate_to(path: &Utf8Path, len: u64) -> Result<(), JournalError> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_data()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{JournalWriter, clean_shutdown_marker_path};
    use crate::JournalLimits;
    use serde_json::json;

    fn marker_fail_dir(case: &str) -> (std::path::PathBuf, camino::Utf8PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "qcg-journal-marker-fail-{case}-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&dir).expect("test dir should be created");
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temporary path must be UTF-8");
        (dir, path)
    }

    #[test]
    fn failed_terminal_marker_fails_the_event_operation() {
        // Q2: a failed terminal marker is a durability hole, so the event
        // operation fails instead of warn-only. A directory planted at the
        // marker path makes the marker write fail on any platform with real
        // filesystem semantics (no mocks).
        let (dir, path) = marker_fail_dir("event");
        let journal = JournalWriter::create(&path, "marker-fail-run", false, None)
            .expect("journal should open");
        journal
            .event(
                "run_started",
                json!({
                    "generator": "marker-fail@1.0.0",
                    "generator_path": "marker-fail",
                    "contract_sha256": "abc",
                    "inputs": {},
                    "resource_hashes": [],
                    "qcg": "0.1.0",
                    "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
                }),
            )
            .expect("run_started should append");
        std::fs::create_dir(clean_shutdown_marker_path(&path))
            .expect("marker-path directory should be created");
        let error = journal
            .event("run_finished", json!({ "status": "success" }))
            .expect_err("a failed terminal marker must fail the operation");
        assert!(
            matches!(error, crate::JournalError::Io(_)),
            "the marker write failure must propagate as an error: {error}"
        );
        std::fs::remove_dir(clean_shutdown_marker_path(&path))
            .expect("marker-path directory should be removable");
        std::fs::remove_dir_all(&dir).expect("test dir should be removed");
    }

    fn audit_test_journal(
        case: &str,
        policy: qcg_policy::AuditPolicy,
        limits: qcg_policy::AuditLimits,
    ) -> (std::path::PathBuf, camino::Utf8PathBuf, JournalWriter) {
        let (dir, path) = marker_fail_dir(case);
        let journal = JournalWriter::create_with_policies(
            &path,
            format!("audit-{case}"),
            false,
            None,
            JournalLimits::default(),
            policy,
            limits,
        )
        .expect("journal should open");
        (dir, path, journal)
    }

    #[test]
    fn audit_policy_off_keeps_observation_records_out_of_both_streams() {
        let (dir, path, journal) = audit_test_journal(
            "off",
            qcg_policy::AuditPolicy {
                default_mode: qcg_policy::AuditMode::Off,
                classes: std::collections::BTreeMap::new(),
            },
            qcg_policy::AuditLimits::default(),
        );
        journal
            .event(
                "llm_delta",
                json!({ "provider": "p", "model": "m", "index": 0, "text": "observation" }),
            )
            .expect("filtered observation record must not fail the run");
        journal
            .event(
                "run_finished",
                json!({ "status": "success", "metrics": {} }),
            )
            .expect("durable record must append");
        let durable = std::fs::read_to_string(&path).expect("journal should read");
        assert!(!durable.contains("llm_delta"));
        assert!(durable.contains("run_finished"));
        assert!(
            !super::audit_path_for(&path).exists(),
            "a policy that persists no observation record must not create the stream"
        );
        std::fs::remove_dir_all(&dir).expect("test dir should be removed");
    }

    #[test]
    fn audit_digest_replaces_the_payload_with_its_digest() {
        let (dir, path, journal) = audit_test_journal(
            "digest",
            qcg_policy::AuditPolicy {
                default_mode: qcg_policy::AuditMode::Digest,
                classes: std::collections::BTreeMap::new(),
            },
            qcg_policy::AuditLimits::default(),
        );
        journal
            .event(
                "llm_delta",
                json!({ "provider": "p", "model": "m", "index": 0, "text": "super-secret" }),
            )
            .expect("digest record should append");
        journal
            .event(
                "run_finished",
                json!({ "status": "success", "metrics": {} }),
            )
            .expect("durable record must append");
        let audit = std::fs::read_to_string(super::audit_path_for(&path))
            .expect("audit stream should exist");
        assert!(audit.contains("llm_delta"));
        assert!(
            !audit.contains("super-secret"),
            "digest must not retain content"
        );
        assert!(audit.contains("sha256"));
        let durable = std::fs::read_to_string(&path).expect("journal should read");
        assert!(!durable.contains("llm_delta"));
        std::fs::remove_dir_all(&dir).expect("test dir should be removed");
    }

    #[test]
    fn audit_limit_breach_degrades_without_failing_the_run() {
        let (dir, path, journal) = audit_test_journal(
            "degrade",
            qcg_policy::AuditPolicy::default(),
            qcg_policy::AuditLimits {
                max_event_bytes: None,
                max_total_bytes: None,
                max_event_count: Some(1),
            },
        );
        journal
            .event(
                "llm_delta",
                json!({ "provider": "p", "model": "m", "index": 0, "text": "first" }),
            )
            .expect("first observation record should append");
        journal
            .event(
                "llm_delta",
                json!({ "provider": "p", "model": "m", "index": 0, "text": "second" }),
            )
            .expect("a breach must degrade, never fail");
        journal
            .event(
                "run_finished",
                json!({ "status": "success", "metrics": {} }),
            )
            .expect("the run must still settle");
        let durable = std::fs::read_to_string(&path).expect("journal should read");
        assert!(durable.contains("audit_degraded"));
        assert!(!durable.contains("second"));
        let audit = std::fs::read_to_string(super::audit_path_for(&path))
            .expect("audit stream should exist");
        assert!(audit.contains("first"));
        assert!(!audit.contains("second"));
        std::fs::remove_dir_all(&dir).expect("test dir should be removed");
    }

    #[test]
    fn failed_terminal_marker_fails_the_batch_operation() {
        // Q2: the batch append path carries the same bar: a sealing batch
        // whose marker write fails reports the failure instead of sealing
        // silently without its shutdown claim.
        let (dir, path) = marker_fail_dir("batch");
        JournalWriter::create(&path, "marker-fail-batch", false, None)
            .expect("journal should open");
        JournalWriter::append_single_event(
            &path,
            "marker-fail-batch",
            "run_started",
            json!({
                "generator": "marker-fail@1.0.0",
                "generator_path": "marker-fail",
                "contract_sha256": "abc",
                "inputs": {},
                "resource_hashes": [],
                "qcg": "0.1.0",
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
            }),
            JournalLimits::default(),
            None,
        )
        .expect("run_started should append");
        std::fs::create_dir(clean_shutdown_marker_path(&path))
            .expect("marker-path directory should be created");
        let error = JournalWriter::append_single_event(
            &path,
            "marker-fail-batch",
            "run_finished",
            // The batch path attaches no automatic metrics: the payload
            // carries the (all-default) metrics object explicitly so the
            // failure below proves the marker hole, not a shape error.
            json!({ "status": "success", "metrics": {} }),
            JournalLimits::default(),
            None,
        )
        .expect_err("a failed batch terminal marker must fail the operation");
        assert!(
            matches!(error, crate::JournalError::Io(_)),
            "the batch marker write failure must propagate as an error: {error}"
        );
        std::fs::remove_dir(clean_shutdown_marker_path(&path))
            .expect("marker-path directory should be removable");
        std::fs::remove_dir_all(&dir).expect("test dir should be removed");
    }
}
