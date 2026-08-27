use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use qcg_types::RunEvent;
use serde::Serialize;
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;
use tokio::sync::broadcast;

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid journal JSON at line {line}: {source}")]
    InvalidLine {
        line: usize,
        source: serde_json::Error,
    },
    #[error("journal event payload must serialize to an object")]
    InvalidPayload,
    #[error("invalid run event: {0}")]
    InvalidEvent(String),
}

pub struct JournalWriter {
    file: Arc<Mutex<File>>,
    state: Arc<Mutex<crate::RunState>>,
    state_path: Utf8PathBuf,
    mirror_stdout: bool,
    event_sender: Option<broadcast::Sender<RunEvent>>,
    metrics: Arc<Mutex<JournalMetrics>>,
    started_at: Instant,
}

#[derive(Debug, Default, Clone, Serialize)]
struct JournalMetrics {
    steps_total: u64,
    steps_succeeded: u64,
    steps_failed: u64,
    steps_skipped: u64,
    repair_attempts: u64,
    regenerate_attempts: u64,
    llm_calls: u64,
    tokens_input: u64,
    tokens_output: u64,
    steps_executed: u64,
    tokens_total: u64,
    cost_microusd: u64,
    duration_ms: u64,
}

impl JournalWriter {
    pub fn create(
        path: &Utf8Path,
        mirror_stdout: bool,
        event_sender: Option<broadcast::Sender<RunEvent>>,
    ) -> Result<Self, JournalError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let state_path = path.with_file_name("state.json");
        let state = crate::RunState::fold_journal(path)?;
        state.persist_atomic(&state_path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            state: Arc::new(Mutex::new(state)),
            state_path,
            mirror_stdout,
            event_sender,
            metrics: Arc::new(Mutex::new(JournalMetrics::default())),
            started_at: Instant::now(),
        })
    }

    pub fn event(&self, kind: &str, payload: impl Serialize) -> Result<(), JournalError> {
        let mut value = serde_json::to_value(payload)?;
        if !value.is_object() {
            value = json!({ "value": value });
        }
        let object = value.as_object_mut().ok_or(JournalError::InvalidPayload)?;
        if kind == "run_finished" {
            let mut metrics = self.metrics.lock().unwrap_or_else(PoisonError::into_inner);
            metrics.steps_executed = metrics.steps_total;
            metrics.tokens_total = metrics.tokens_input.saturating_add(metrics.tokens_output);
            metrics.duration_ms = self
                .started_at
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
            object.insert("metrics".into(), serde_json::to_value(metrics.clone())?);
        } else {
            self.record_metrics(kind, object);
        }
        let (line, event) = {
            let mut file = self.file.lock().unwrap_or_else(PoisonError::into_inner);
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let seq = state.last_seq.saturating_add(1);
            object.insert("t".into(), Value::String(kind.into()));
            object.insert("ts".into(), Value::String(Utc::now().to_rfc3339()));
            object.insert("seq".into(), Value::Number(seq.into()));
            let line = serde_json::to_string(object)?;
            let mut bytes = line.as_bytes().to_vec();
            bytes.push(b'\n');
            file.write_all(&bytes)?;
            if matches!(kind, "run_finished" | "run_error" | "run_canceled") {
                file.sync_data()?;
            }
            state.apply(&value)?;
            state.persist_atomic(&self.state_path)?;
            let event = RunEvent::from_flat(&value, state.run_id.as_deref().unwrap_or("unscoped"))
                .map_err(JournalError::InvalidEvent)?;
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

    fn record_metrics(&self, kind: &str, object: &serde_json::Map<String, Value>) {
        let mut metrics = self.metrics.lock().unwrap_or_else(PoisonError::into_inner);
        match kind {
            "step_finished" => {
                metrics.steps_total = metrics.steps_total.saturating_add(1);
                match object.get("status").and_then(Value::as_str) {
                    Some(
                        "success" | "repaired" | "routed" | "answered_on_fail" | "regenerated",
                    ) => {
                        metrics.steps_succeeded = metrics.steps_succeeded.saturating_add(1);
                    }
                    Some("needs_user" | "needs_confirm") => {}
                    _ => {
                        metrics.steps_failed = metrics.steps_failed.saturating_add(1);
                    }
                }
            }
            "step_skipped" => {
                metrics.steps_skipped = metrics.steps_skipped.saturating_add(1);
            }
            "repair_attempt_started" => {
                metrics.repair_attempts = metrics.repair_attempts.saturating_add(1);
            }
            "regenerate_attempt_started" => {
                metrics.regenerate_attempts = metrics.regenerate_attempts.saturating_add(1);
            }
            "llm_call" => {
                metrics.llm_calls = metrics.llm_calls.saturating_add(1);
                if let Some(tokens) = object.get("tokens").and_then(Value::as_object) {
                    metrics.tokens_input = metrics.tokens_input.saturating_add(
                        tokens
                            .get("input")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                    );
                    metrics.tokens_output = metrics.tokens_output.saturating_add(
                        tokens
                            .get("output")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                    );
                }
                metrics.cost_microusd = metrics.cost_microusd.saturating_add(
                    object
                        .get("cost_microusd")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                );
            }
            _ => {}
        }
    }

    pub fn clone_for_parallel(&self) -> Result<Self, JournalError> {
        Ok(Self {
            file: Arc::clone(&self.file),
            state: Arc::clone(&self.state),
            state_path: self.state_path.clone(),
            mirror_stdout: self.mirror_stdout,
            event_sender: self.event_sender.clone(),
            metrics: Arc::clone(&self.metrics),
            started_at: self.started_at,
        })
    }

    pub fn state(&self) -> crate::RunState {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_journal_ignores_only_a_truncated_final_record() {
        let dir =
            std::env::temp_dir().join(format!("qcg-journal-truncated-tail-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("test directory should be created");
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temporary path must be UTF-8");
        std::fs::write(
            &path,
            concat!(
                "{\"t\":\"run_started\",\"run_id\":\"tail-test\",\"contract_sha256\":\"abc\",\"seq\":1}\n",
                "{\"t\":\"step_started\",\"seq\":"
            ),
        )
        .expect("test journal should be written");
        let state = crate::RunState::fold_journal(&path)
            .expect("a truncated final record should be ignored");
        assert_eq!(state.run_id.as_deref(), Some("tail-test"));
        assert_eq!(state.contract_sha256.as_deref(), Some("abc"));
        assert_eq!(state.last_seq, 1);
    }

    #[test]
    fn run_finished_includes_accumulated_metrics() {
        let dir = std::env::temp_dir().join(format!("qcg-journal-metrics-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let journal = JournalWriter::create(&path, false, None).unwrap();
        journal
            .event("step_finished", json!({ "node": "a", "status": "success" }))
            .unwrap();
        journal
            .event(
                "step_finished",
                json!({ "node": "b", "status": "check_failed" }),
            )
            .unwrap();
        journal
            .event(
                "step_skipped",
                json!({
                    "node": "c",
                    "reason": qcg_types::FailureDetail::new(
                        qcg_types::FailureCode::DependencyUnsatisfied,
                        "dependency failed",
                    ),
                }),
            )
            .unwrap();
        journal
            .event(
                "repair_attempt_started",
                json!({ "node": "b", "repair": "r", "recheck": "c", "attempt": 1, "max_attempts": 2 }),
            )
            .unwrap();
        journal
            .event(
                "llm_call",
                json!({
                    "node": "r",
                    "provider": "fake",
                    "tokens": { "input": 7, "output": 3 },
                    "cost_microusd": 25,
                }),
            )
            .unwrap();
        journal
            .event("run_finished", json!({ "status": "failed" }))
            .unwrap();

        let source = std::fs::read_to_string(&path).unwrap();
        let last = source.lines().last().unwrap();
        let event: Value = serde_json::from_str(last).unwrap();
        assert_eq!(event["t"], "run_finished");
        assert_eq!(event["metrics"]["steps_total"], 2);
        assert_eq!(event["metrics"]["steps_executed"], 2);
        assert_eq!(event["metrics"]["steps_succeeded"], 1);
        assert_eq!(event["metrics"]["steps_failed"], 1);
        assert_eq!(event["metrics"]["steps_skipped"], 1);
        assert_eq!(event["metrics"]["repair_attempts"], 1);
        assert_eq!(event["metrics"]["llm_calls"], 1);
        assert_eq!(event["metrics"]["tokens_input"], 7);
        assert_eq!(event["metrics"]["tokens_output"], 3);
        assert_eq!(event["metrics"]["tokens_total"], 10);
        assert_eq!(event["metrics"]["cost_microusd"], 25);
        assert!(event["metrics"]["duration_ms"].as_u64().is_some());
    }

    #[test]
    fn poisoned_mutexes_do_not_leave_journal_unusable() {
        let dir = std::env::temp_dir().join(format!("qcg-journal-poison-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let journal = JournalWriter::create(&path, false, None).unwrap();

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = journal.metrics.lock().unwrap();
            panic!("poison metrics mutex for recovery test");
        }));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = journal.file.lock().unwrap();
            panic!("poison file mutex for recovery test");
        }));

        journal
            .event("run_error", json!({ "error": "recovered" }))
            .expect("poisoned journal locks should be recoverable");
    }

    #[test]
    fn budget_state_accumulates_across_journal_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-journal-budget-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&dir).expect("test directory should be created");
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temporary path must be UTF-8");
        {
            let journal =
                JournalWriter::create(&path, false, None).expect("first journal round should open");
            journal
                .event(
                    "run_started",
                    json!({
                        "run_id": "budget-run",
                        "generator": "budget@1.0.0",
                        "generator_path": "budget",
                        "contract_sha256": "abc",
                        "inputs": {},
                        "resource_hashes": [],
                        "qcg": "0.1.0",
                        "schema_version": 1,
                    }),
                )
                .expect("run should start");
            journal
                .event(
                    "llm_call",
                    json!({
                        "node": "first",
                        "provider": "fake",
                        "tokens": { "input": 7, "output": 3 },
                        "cost_microusd": 25,
                    }),
                )
                .expect("first LLM call should be recorded");
        }

        let journal =
            JournalWriter::create(&path, false, None).expect("second journal round should open");
        journal
            .event("run_resumed", json!({ "run_id": "budget-run" }))
            .expect("run should resume");
        journal
            .event(
                "llm_call",
                json!({
                    "node": "second",
                    "provider": "fake",
                    "tokens": { "input": 11, "output": 5 },
                    "cost_microusd": 75,
                }),
            )
            .expect("second LLM call should be recorded");
        let state = journal.state();
        assert_eq!(state.budget.tokens_input, 18);
        assert_eq!(state.budget.tokens_output, 8);
        assert_eq!(state.budget.cost_microusd, 100);
    }
}
