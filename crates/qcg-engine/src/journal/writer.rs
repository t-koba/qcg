use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use fs2::FileExt as _;
use qcg_api::RunEvent;
use serde::Serialize;
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::broadcast;

use super::read::{journal_metrics, read_journal_values};
use super::serialize::{append_serialized_json_line, serialize_bounded};
use super::types::{JournalError, JournalLimits, JournalWriter};

/// Sibling lock file serializing all journal writers for one run across
/// processes and threads. Every journal append holds this lock from the
/// latest-state read through seq assignment, file append, and state.json
/// update so concurrent writers cannot assign duplicate seq values.
pub fn journal_lock_path(journal_path: &Utf8Path) -> Utf8PathBuf {
    journal_path.with_file_name(".journal.lock")
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
    file.lock_exclusive()?;
    Ok(file)
}

/// Latest durable seq without folding the whole journal: scan the tail
/// backwards for the last newline-terminated JSON line carrying `seq`.
/// Bounded to the final 64 KiB plus one over-long line so a huge journal
/// never forces a full read before the configured limits are checked.
pub fn read_last_seq_from_tail(journal_path: &Utf8Path) -> Result<u64, JournalError> {
    if !journal_path.exists() {
        return Ok(0);
    }
    let metadata = std::fs::metadata(journal_path)?;
    let len = metadata.len();
    if len == 0 {
        return Ok(0);
    }
    const TAIL_WINDOW: u64 = 64 * 1024;
    const MAX_LINE: u64 = 1024 * 1024;
    let window = len.min(TAIL_WINDOW + MAX_LINE);
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
            None => return Ok(0),
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
    // When the tail window was truncated mid-history the max above may miss
    // older seq values, so fall back to state.json as a lower bound. An
    // unreadable state file is logged, never silently equated with a zero
    // bound: the window scan above remains the authoritative floor.
    let state_path = journal_path.with_file_name("state.json");
    match std::fs::read(&state_path) {
        Ok(bytes) => {
            match serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|state| state.get("last_seq").and_then(Value::as_u64))
            {
                Some(seq) => {
                    last_seq = last_seq.max(seq);
                }
                None => {
                    tracing::warn!(
                        state_path = %state_path,
                        "state.json has no usable last_seq; seq floor comes from the journal window only"
                    );
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(
                state_path = %state_path,
                %error,
                "state.json unreadable; seq floor comes from the journal window only"
            );
        }
    }
    Ok(last_seq)
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
        validate_limits(limits)?;
        let run_id = run_id.into();
        if run_id.trim().is_empty() {
            return Err(JournalError::InvalidEvent(
                "journal run_id must be non-empty".into(),
            ));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Repair and initial fold hold the cross-process journal lock so a
        // live writer can never race with torn-tail truncation.
        let _journal_guard = acquire_journal_lock(path)?;
        repair_truncated_tail_locked(path)?;
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
        if let Some(existing) = state.run_id.as_deref()
            && existing != run_id
        {
            return Err(JournalError::InvalidEvent(format!(
                "journal run_id mismatch: expected `{run_id}`, found `{existing}`"
            )));
        }
        // Seed identity at creation: budget reservations and other
        // run-keyed maps must never observe an ownerless state, even before
        // the first event folds.
        state.run_id = Some(run_id.clone());
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
        })
    }

    pub fn event(&self, kind: &str, payload: impl Serialize) -> Result<(), JournalError> {
        let payload = serialize_bounded(&payload, self.limits.max_event_bytes, "event")?;
        let mut value = serde_json::from_slice::<Value>(&payload)?;
        if !value.is_object() {
            value = json!({ "value": value });
        }
        let object = value.as_object_mut().ok_or(JournalError::InvalidPayload)?;
        // Every terminal event carries the accumulated cost metrics so wasted
        // spend on failed or canceled runs stays queryable afterwards.
        // Metrics must never break the event write itself.
        if matches!(
            kind,
            "run_finished" | "run_error" | "run_canceled" | "run_interrupted"
        ) {
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
        repair_truncated_tail_locked(&self.journal_path)?;
        let (line, event) = {
            let mut file = self.file.lock().unwrap_or_else(PoisonError::into_inner);
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let mut stats = self.stats.lock().unwrap_or_else(PoisonError::into_inner);
            // A peer writer may have appended while this writer was busy.
            // Resynchronize from durable state so seq stays unique and
            // monotonic even across processes. Any resync failure is
            // fail-closed: continuing with a stale memory seq would risk
            // duplicates and last-writer-wins state.
            let durable_seq = read_last_seq_from_tail(&self.journal_path)?;
            if durable_seq > state.last_seq {
                *state =
                    crate::RunState::fold_journal_with_limits(&self.journal_path, self.limits)?;
            }
            let seq = state.last_seq.max(durable_seq).saturating_add(1);
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
            if matches!(kind, "run_finished" | "run_error" | "run_canceled") {
                file.sync_data()?;
            }
            *state = next_state;
            crate::RunState::persist_serialized_atomic(&self.state_path, &state_bytes)?;
            (line, event)
        };
        if self.mirror_stdout {
            println!("{line}");
        }
        if let Some(sender) = &self.event_sender {
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
        repair_truncated_tail_locked(journal_path)?;
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
        check(&state)?;
        // Stats without a second full scan: strict seq monotonicity plus
        // refold-on-truncate keeps event count exactly at last_seq, and the
        // file length is the byte total.
        let file_len = std::fs::metadata(journal_path)?.len();
        let mut stats = super::types::JournalStats {
            bytes: usize::try_from(file_len).unwrap_or(usize::MAX),
            events: usize::try_from(state.last_seq).unwrap_or(usize::MAX),
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
            // silently keeping last-writer-wins state.
            let seq = state.last_seq.saturating_add(1);
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
            if matches!(
                kind,
                "run_finished" | "run_error" | "run_canceled" | "run_interrupted"
            ) {
                needs_sync = true;
            }
            appended.push(event);
        }
        if needs_sync {
            file.sync_data()?;
        }
        let state_bytes = serialize_bounded(&state, limits.max_state_bytes, "state")?;
        let state_path = journal_path.with_file_name("state.json");
        crate::RunState::persist_serialized_atomic(&state_path, &state_bytes)?;
        if let Some(sender) = &event_sender {
            for event in &appended {
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
        })
    }

    pub fn state(&self) -> crate::RunState {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

pub(crate) fn validate_limits(limits: JournalLimits) -> Result<(), JournalError> {
    for (resource, value) in [
        ("max_event_bytes", limits.max_event_bytes),
        ("max_total_bytes", limits.max_total_bytes),
        ("max_event_count", limits.max_event_count),
        ("max_state_bytes", limits.max_state_bytes),
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
pub fn repair_truncated_tail_locked(path: &Utf8Path) -> Result<(), JournalError> {
    if !path.exists() {
        return Ok(());
    }
    // Bounded tail inspection: only the final 1 MiB plus one line is read
    // before the configured limits are enforced, so a huge journal never
    // forces a full read here (C05).
    const SCAN_WINDOW: u64 = 1024 * 1024;
    let metadata = std::fs::metadata(path)?;
    let len = metadata.len();
    if len == 0 {
        return Ok(());
    }
    if len <= SCAN_WINDOW {
        let bytes = std::fs::read(path)?;
        if bytes.is_empty() || bytes.last() == Some(&b'\n') {
            return Ok(());
        }
        return repair_tail_bytes(path, &bytes, 0);
    }
    let start = len.saturating_sub(SCAN_WINDOW);
    let mut file = File::open(path)?;
    use std::io::{Read as _, Seek as _};
    file.seek(std::io::SeekFrom::Start(start))?;
    let mut bytes = Vec::with_capacity(SCAN_WINDOW as usize);
    file.take(SCAN_WINDOW).read_to_end(&mut bytes)?;
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
        None => repair_tail_backscan(path, start, bytes),
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
) -> Result<(), JournalError> {
    // Bounded tail inspection: only the final 1 MiB plus one line is read
    // before the configured limits are enforced, so a huge journal never
    // forces a full read here (C05).
    const SCAN_WINDOW: u64 = 1024 * 1024;
    const MAX_BACKSCAN: u64 = 16 * 1024 * 1024;
    use std::io::{Read as _, Seek as _};
    loop {
        let scan_start = scan_end.saturating_sub(SCAN_WINDOW);
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
        if tail.len() + chunk.len() > MAX_BACKSCAN as usize {
            return Err(JournalError::InvalidEvent(format!(
                "journal tail exceeds {MAX_BACKSCAN} bytes without a line boundary; refusing unbounded repair"
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
