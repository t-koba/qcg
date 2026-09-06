use camino::Utf8Path;
use chrono::Utc;
use qcg_api::RunEvent;
use serde::Serialize;
use serde_json::{Value, json};
use std::fs::OpenOptions;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::broadcast;

use super::read::{journal_metrics, read_journal_values};
use super::serialize::{append_serialized_json_line, serialize_bounded};
use super::types::{JournalError, JournalLimits, JournalWriter};

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
        repair_truncated_tail(path)?;
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let state_path = path.with_file_name("state.json");
        let scan = read_journal_values(path, limits)?;
        let state = crate::RunState::fold_journal_with_limits(path, limits)?;
        if let Some(existing) = state.run_id.as_deref()
            && existing != run_id
        {
            return Err(JournalError::InvalidEvent(format!(
                "journal run_id mismatch: expected `{run_id}`, found `{existing}`"
            )));
        }
        state.persist_atomic_with_limits(&state_path, limits.max_state_bytes)?;
        Ok(Self {
            run_id,
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
        let (line, event) = {
            let mut file = self.file.lock().unwrap_or_else(PoisonError::into_inner);
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let mut stats = self.stats.lock().unwrap_or_else(PoisonError::into_inner);
            let seq = state.last_seq.saturating_add(1);
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

    pub fn clone_for_parallel(&self) -> Result<Self, JournalError> {
        Ok(Self {
            run_id: self.run_id.clone(),
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

/// Repair a torn trailing line before appending.
///
/// The reader ignores an incomplete final line without a newline, but
/// appending after it would fuse two JSON objects into one invalid line.
/// Only newline-terminated lines are durable: a trailing fragment without
/// a newline is either committed (valid complete JSON gains its newline)
/// or truncated to the last newline boundary. Middle-line corruption is
/// never silently dropped and still surfaces as `InvalidLine` on read.
fn repair_truncated_tail(path: &Utf8Path) -> Result<(), JournalError> {
    if !path.exists() {
        return Ok(());
    }
    let bytes = std::fs::read(path)?;
    if bytes.is_empty() || bytes.last() == Some(&b'\n') {
        return Ok(());
    }
    let last_newline = bytes.iter().rposition(|byte| *byte == b'\n');
    let tail_start = last_newline.map(|pos| pos + 1).unwrap_or(0);
    let tail = &bytes[tail_start..];
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

fn truncate_to(path: &Utf8Path, len: usize) -> Result<(), JournalError> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len as u64)?;
    file.sync_data()?;
    Ok(())
}
