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
