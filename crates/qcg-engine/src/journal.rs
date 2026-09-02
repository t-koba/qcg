use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use qcg_contract::RuntimeLimits;
use qcg_types::RunEvent;
use serde::Serialize;
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read as _, Write};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::broadcast;

const DEFAULT_JOURNAL_EVENT_LIMIT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_JOURNAL_TOTAL_LIMIT_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_JOURNAL_EVENT_COUNT_LIMIT: usize = 100_000;
const DEFAULT_STATE_LIMIT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalLimits {
    pub max_event_bytes: usize,
    pub max_total_bytes: usize,
    pub max_event_count: usize,
    pub max_state_bytes: usize,
}

impl Default for JournalLimits {
    fn default() -> Self {
        Self {
            max_event_bytes: DEFAULT_JOURNAL_EVENT_LIMIT_BYTES,
            max_total_bytes: DEFAULT_JOURNAL_TOTAL_LIMIT_BYTES,
            max_event_count: DEFAULT_JOURNAL_EVENT_COUNT_LIMIT,
            max_state_bytes: DEFAULT_STATE_LIMIT_BYTES,
        }
    }
}

impl From<&RuntimeLimits> for JournalLimits {
    fn from(runtime: &RuntimeLimits) -> Self {
        Self {
            max_event_bytes: runtime.journal_event_limit_bytes,
            max_total_bytes: runtime.journal_total_limit_bytes,
            max_event_count: runtime.journal_event_count_limit,
            max_state_bytes: runtime.state_limit_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JournalStats {
    pub bytes: usize,
    pub events: usize,
}

#[derive(Debug)]
pub struct JournalScan {
    pub events: Vec<Value>,
    pub stats: JournalStats,
}

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
    #[error("journal {resource} exceeds {limit} bytes (attempted {actual})")]
    LimitExceeded {
        resource: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("journal event count exceeds {limit} (attempted {actual})")]
    EventCountExceeded { actual: usize, limit: usize },
    #[error("invalid journal limit: {resource} must be greater than zero")]
    InvalidLimit { resource: &'static str },
}

pub struct JournalWriter {
    run_id: String,
    file: Arc<Mutex<File>>,
    state: Arc<Mutex<crate::RunState>>,
    state_path: Utf8PathBuf,
    mirror_stdout: bool,
    event_sender: Option<broadcast::Sender<RunEvent>>,
    limits: JournalLimits,
    stats: Arc<Mutex<JournalStats>>,
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
        if kind == "run_finished" {
            let metrics = journal_metrics(
                &self
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .budget,
            )?;
            object.insert("metrics".into(), serde_json::to_value(metrics)?);
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
                Value::String(qcg_types::trace_id_for_run(&event_run_id)),
            );
            object.insert(
                "span_id".into(),
                Value::String(qcg_types::span_id_for_seq(seq)),
            );
            let parent_scope = object
                .get("node")
                .and_then(Value::as_str)
                .map(|node| format!("step:{node}"))
                .unwrap_or_else(|| "run".to_string());
            if !matches!(kind, "run_queued" | "run_started") {
                object.insert(
                    "parent_span_id".into(),
                    Value::String(qcg_types::span_id_for_scope(&event_run_id, &parent_scope)),
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

fn validate_limits(limits: JournalLimits) -> Result<(), JournalError> {
    for (resource, value) in [
        ("max_event_bytes", limits.max_event_bytes),
        ("max_total_bytes", limits.max_total_bytes),
        ("max_event_count", limits.max_event_count),
        ("max_state_bytes", limits.max_state_bytes),
    ] {
        if value == 0 {
            return Err(JournalError::InvalidLimit { resource });
        }
    }
    Ok(())
}

pub fn serialize_bounded<T: Serialize>(
    value: &T,
    limit: usize,
    resource: &'static str,
) -> Result<Vec<u8>, JournalError> {
    if limit == 0 {
        return Err(JournalError::InvalidLimit { resource });
    }
    let mut writer = BoundedBytesWriter::new(limit);
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.bytes),
        Err(_error) if writer.exceeded => Err(JournalError::LimitExceeded {
            resource,
            actual: limit.saturating_add(1),
            limit,
        }),
        Err(error) => Err(JournalError::Json(error)),
    }
}

struct BoundedBytesWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedBytesWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(64 * 1024)),
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedBytesWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(next) = self.bytes.len().checked_add(bytes.len()) else {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "bounded JSON serialization overflowed",
            ));
        };
        if next > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "bounded JSON serialization exceeded limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn append_serialized_json_line<W: Write>(
    writer: &mut W,
    mut bytes: Vec<u8>,
    stats: &mut JournalStats,
    limits: JournalLimits,
) -> Result<(), JournalError> {
    validate_limits(limits)?;
    if stats.events >= limits.max_event_count {
        return Err(JournalError::EventCountExceeded {
            actual: stats.events.saturating_add(1),
            limit: limits.max_event_count,
        });
    }
    if bytes.len() > limits.max_event_bytes {
        return Err(JournalError::LimitExceeded {
            resource: "event",
            actual: bytes.len(),
            limit: limits.max_event_bytes,
        });
    }
    let line_bytes = bytes
        .len()
        .checked_add(1)
        .ok_or(JournalError::LimitExceeded {
            resource: "total journal",
            actual: usize::MAX,
            limit: limits.max_total_bytes,
        })?;
    let total = stats
        .bytes
        .checked_add(line_bytes)
        .ok_or(JournalError::LimitExceeded {
            resource: "total journal",
            actual: usize::MAX,
            limit: limits.max_total_bytes,
        })?;
    if total > limits.max_total_bytes {
        return Err(JournalError::LimitExceeded {
            resource: "total journal",
            actual: total,
            limit: limits.max_total_bytes,
        });
    }
    bytes.push(b'\n');
    writer.write_all(&bytes)?;
    stats.bytes = total;
    stats.events = stats.events.saturating_add(1);
    Ok(())
}

pub fn read_journal_values(
    path: &Utf8Path,
    limits: JournalLimits,
) -> Result<JournalScan, JournalError> {
    read_journal_values_through(path, None, limits)
}

pub fn read_journal_values_through(
    path: &Utf8Path,
    through_seq: Option<u64>,
    limits: JournalLimits,
) -> Result<JournalScan, JournalError> {
    validate_limits(limits)?;
    if !path.exists() {
        return Ok(JournalScan {
            events: Vec::new(),
            stats: JournalStats::default(),
        });
    }
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut line_number = 0_usize;
    let mut stats = JournalStats::default();
    let mut events = Vec::new();
    let line_read_limit =
        limits
            .max_event_bytes
            .checked_add(2)
            .ok_or(JournalError::LimitExceeded {
                resource: "event",
                actual: usize::MAX,
                limit: limits.max_event_bytes,
            })?;
    loop {
        line.clear();
        let read = (&mut reader)
            .take(
                u64::try_from(line_read_limit).map_err(|_| JournalError::LimitExceeded {
                    resource: "event",
                    actual: usize::MAX,
                    limit: limits.max_event_bytes,
                })?,
            )
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        line_number = line_number.saturating_add(1);
        stats.bytes = stats
            .bytes
            .checked_add(read)
            .ok_or(JournalError::LimitExceeded {
                resource: "total journal",
                actual: usize::MAX,
                limit: limits.max_total_bytes,
            })?;
        if stats.bytes > limits.max_total_bytes {
            return Err(JournalError::LimitExceeded {
                resource: "total journal",
                actual: stats.bytes,
                limit: limits.max_total_bytes,
            });
        }
        let has_newline = line.last() == Some(&b'\n');
        if !has_newline && read == line_read_limit {
            return Err(JournalError::LimitExceeded {
                resource: "event",
                actual: read,
                limit: limits.max_event_bytes,
            });
        }
        let body_len = line.len().saturating_sub(usize::from(has_newline));
        if body_len > limits.max_event_bytes {
            return Err(JournalError::LimitExceeded {
                resource: "event",
                actual: body_len,
                limit: limits.max_event_bytes,
            });
        }
        let body = &line[..body_len];
        if body.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if stats.events >= limits.max_event_count {
            return Err(JournalError::EventCountExceeded {
                actual: stats.events.saturating_add(1),
                limit: limits.max_event_count,
            });
        }
        let event = match serde_json::from_slice::<Value>(body) {
            Ok(event) => event,
            Err(source) if !has_newline && source.is_eof() && reader.fill_buf()?.is_empty() => {
                break;
            }
            Err(source) => {
                return Err(JournalError::InvalidLine {
                    line: line_number,
                    source,
                });
            }
        };
        if through_seq.is_some_and(|limit| {
            event
                .get("seq")
                .and_then(Value::as_u64)
                .is_some_and(|seq| seq > limit)
        }) {
            break;
        }
        stats.events = stats.events.saturating_add(1);
        events.push(event);
    }
    Ok(JournalScan { events, stats })
}

fn journal_metrics(budget: &crate::BudgetState) -> Result<JournalMetrics, JournalError> {
    let steps_executed = u64::try_from(budget.steps_executed).map_err(|_| {
        JournalError::InvalidEvent("executed step count exceeds journal metric range".into())
    })?;
    let started_at = budget.started_at.as_deref().ok_or_else(|| {
        JournalError::InvalidEvent("run start time is required before finishing a run".into())
    })?;
    let started_at = chrono::DateTime::parse_from_rfc3339(started_at)
        .map_err(|error| JournalError::InvalidEvent(format!("invalid run start time: {error}")))?;
    let duration_ms = Utc::now()
        .signed_duration_since(started_at)
        .num_milliseconds();
    let duration_ms = u64::try_from(duration_ms).map_err(|_| {
        JournalError::InvalidEvent("run start time is later than its finish time".into())
    })?;
    Ok(JournalMetrics {
        steps_total: steps_executed,
        steps_succeeded: budget.steps_succeeded,
        steps_failed: budget.steps_failed,
        steps_skipped: budget.steps_skipped,
        repair_attempts: budget.repair_attempts,
        regenerate_attempts: budget.regenerate_attempts,
        llm_calls: budget.llm_calls,
        tokens_input: budget.tokens_input,
        tokens_output: budget.tokens_output,
        steps_executed,
        tokens_total: budget.tokens_input.saturating_add(budget.tokens_output),
        cost_microusd: budget.cost_microusd,
        duration_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_limits(
        max_event_bytes: usize,
        max_total_bytes: usize,
        max_event_count: usize,
        max_state_bytes: usize,
    ) -> JournalLimits {
        JournalLimits {
            max_event_bytes,
            max_total_bytes,
            max_event_count,
            max_state_bytes,
        }
    }

    #[test]
    fn append_accepts_an_event_at_the_byte_limit() {
        let limits = test_limits(5, 32, 4, 64);
        let mut output = Vec::new();
        let mut stats = JournalStats::default();

        append_serialized_json_line(&mut output, b"12345".to_vec(), &mut stats, limits)
            .expect("an event exactly at the byte limit should be accepted");
        assert_eq!(output, b"12345\n");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 6,
                events: 1
            }
        );

        let error =
            append_serialized_json_line(&mut output, b"123456".to_vec(), &mut stats, limits)
                .expect_err("an event over the byte limit should be rejected");
        assert!(matches!(
            error,
            JournalError::LimitExceeded {
                resource: "event",
                actual: 6,
                limit: 5,
            }
        ));
        assert_eq!(output, b"12345\n");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 6,
                events: 1
            }
        );
    }

    #[test]
    fn append_accepts_total_bytes_at_the_limit() {
        let limits = test_limits(5, 12, 4, 64);
        let mut output = Vec::new();
        let mut stats = JournalStats::default();

        append_serialized_json_line(&mut output, b"12345".to_vec(), &mut stats, limits)
            .expect("the first event should fit within the total byte limit");
        append_serialized_json_line(&mut output, b"12345".to_vec(), &mut stats, limits)
            .expect("the total bytes exactly at the limit should be accepted");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 12,
                events: 2
            }
        );

        let error = append_serialized_json_line(&mut output, b"x".to_vec(), &mut stats, limits)
            .expect_err("an event exceeding the total byte limit should be rejected");
        assert!(matches!(
            error,
            JournalError::LimitExceeded {
                resource: "total journal",
                actual: 14,
                limit: 12,
            }
        ));
        assert_eq!(output, b"12345\n12345\n");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 12,
                events: 2
            }
        );
    }

    #[test]
    fn append_rejects_the_event_after_the_count_limit() {
        let limits = test_limits(8, 32, 2, 64);
        let mut output = Vec::new();
        let mut stats = JournalStats::default();

        append_serialized_json_line(&mut output, b"a".to_vec(), &mut stats, limits)
            .expect("the first event should fit within the count limit");
        append_serialized_json_line(&mut output, b"b".to_vec(), &mut stats, limits)
            .expect("the second event should fit within the count limit");

        let error = append_serialized_json_line(&mut output, b"c".to_vec(), &mut stats, limits)
            .expect_err("the event after the count limit should be rejected");
        assert!(matches!(
            error,
            JournalError::EventCountExceeded {
                actual: 3,
                limit: 2,
            }
        ));
        assert_eq!(output, b"a\nb\n");
        assert_eq!(
            stats,
            JournalStats {
                bytes: 4,
                events: 2
            }
        );
    }

    #[test]
    fn journal_event_rejects_state_at_the_byte_limit() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-journal-state-limit-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&dir).expect("test directory should be created");
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temporary path must be UTF-8");
        let empty_state_bytes = serde_json::to_vec(&crate::RunState::default())
            .expect("the default run state should serialize");
        let limits = test_limits(64 * 1024, 128 * 1024, 8, empty_state_bytes.len());
        let journal =
            JournalWriter::create_with_limits(&path, "state-limit-run", false, None, limits)
                .expect("the empty state should fit exactly at the state limit");

        let error = journal
            .event(
                "run_started",
                json!({
                    "generator": "state-limit@1.0.0",
                    "generator_path": "state-limit",
                    "contract_sha256": "abc",
                    "inputs": {},
                    "resource_hashes": [],
                    "qcg": "0.1.0",
                    "schema_version": 1,
                }),
            )
            .expect_err("state growth over the limit should reject the event");
        assert!(matches!(
            error,
            JournalError::LimitExceeded {
                resource: "state",
                limit,
                ..
            } if limit == empty_state_bytes.len()
        ));
        let state = journal.state();
        assert_eq!(state.run_id, None);
        assert_eq!(state.last_seq, 0);
        assert!(state.nodes.is_empty());
        assert!(state.checkpoints.is_empty());
        assert!(state.pending.is_none());
        assert!(state.terminal.is_none());
        assert_eq!(
            std::fs::read(&path).expect("journal should be readable"),
            b""
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_rejects_a_large_newline_free_record_while_reading() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-journal-newline-free-limit-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&dir).expect("test directory should be created");
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl"))
            .expect("temporary path must be UTF-8");
        let limits = test_limits(8, 64, 4, 64);
        std::fs::write(&path, vec![b'x'; limits.max_event_bytes + 2])
            .expect("the oversized newline-free record should be written");

        let error = read_journal_values(&path, limits)
            .expect_err("an oversized newline-free record should be rejected");
        assert!(matches!(
            error,
            JournalError::LimitExceeded {
                resource: "event",
                actual: 10,
                limit: 8,
            }
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

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
                "{\"t\":\"run_started\",\"run_id\":\"tail-test\",\"trace_id\":\"trace\",\"span_id\":\"span\",\"ts\":\"2026-01-01T00:00:00Z\",\"generator\":\"test@1\",\"generator_path\":\"test\",\"contract_sha256\":\"abc\",\"inputs\":{},\"resource_hashes\":[],\"qcg\":\"0.1.0\",\"schema_version\":1,\"seq\":1}\n",
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
        let journal = JournalWriter::create(&path, "metrics-run", false, None).unwrap();
        journal
            .event(
                "run_started",
                json!({
                    "generator": "metrics@1.0.0",
                    "generator_path": "metrics",
                    "contract_sha256": "abc",
                    "inputs": {},
                    "resource_hashes": [],
                    "qcg": "0.1.0",
                    "schema_version": 1,
                }),
            )
            .unwrap();
        journal
            .event(
                "step_started",
                json!({ "node": "a", "type": "test", "attempt": 1 }),
            )
            .unwrap();
        journal
            .event("step_finished", json!({ "node": "a", "status": "success" }))
            .unwrap();
        journal
            .event(
                "step_started",
                json!({ "node": "b", "type": "test", "attempt": 1 }),
            )
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
                    "model": "fake",
                    "max_tokens": 128,
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
        let journal = JournalWriter::create(&path, "poison-run", false, None).unwrap();

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = journal.state.lock().unwrap();
            panic!("poison state mutex for recovery test");
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
            let journal = JournalWriter::create(&path, "budget-run", false, None)
                .expect("first journal round should open");
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
                    "step_started",
                    json!({ "node": "first", "type": "test", "attempt": 1 }),
                )
                .expect("first step should start");
            journal
                .event(
                    "step_finished",
                    json!({ "node": "first", "status": "success" }),
                )
                .expect("first step should finish");
            journal
                .event(
                    "llm_call",
                    json!({
                        "node": "first",
                        "provider": "fake",
                        "model": "fake",
                        "max_tokens": 128,
                        "tokens": { "input": 7, "output": 3 },
                        "cost_microusd": 25,
                    }),
                )
                .expect("first LLM call should be recorded");
        }

        let journal = JournalWriter::create(&path, "budget-run", false, None)
            .expect("second journal round should open");
        journal
            .event("run_resumed", json!({ "run_id": "budget-run" }))
            .expect("run should resume");
        journal
            .event(
                "step_started",
                json!({ "node": "second", "type": "test", "attempt": 1 }),
            )
            .expect("second step should start");
        journal
            .event(
                "step_finished",
                json!({ "node": "second", "status": "success" }),
            )
            .expect("second step should finish");
        journal
            .event(
                "llm_call",
                json!({
                    "node": "second",
                    "provider": "fake",
                    "model": "fake",
                    "max_tokens": 128,
                    "tokens": { "input": 11, "output": 5 },
                    "cost_microusd": 75,
                }),
            )
            .expect("second LLM call should be recorded");
        journal
            .event("run_finished", json!({ "status": "success" }))
            .expect("run should finish");
        let state = journal.state();
        assert_eq!(state.budget.steps_executed, 2);
        assert_eq!(state.budget.steps_succeeded, 2);
        assert_eq!(state.budget.llm_calls, 2);
        assert_eq!(state.budget.tokens_input, 18);
        assert_eq!(state.budget.tokens_output, 8);
        assert_eq!(state.budget.cost_microusd, 100);
        let source = std::fs::read_to_string(&path).expect("journal should be readable");
        let finished: Value = serde_json::from_str(source.lines().last().expect("finished event"))
            .expect("finished event should be JSON");
        assert_eq!(finished["metrics"]["steps_succeeded"], 2);
        assert_eq!(finished["metrics"]["llm_calls"], 2);
        assert_eq!(finished["metrics"]["tokens_total"], 26);
    }

    #[test]
    fn agent_checkpoint_is_durable_and_cleared_by_step_completion() {
        let dir = std::env::temp_dir().join(format!("qcg-agent-checkpoint-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let journal = JournalWriter::create(&path, "checkpoint-run", false, None).unwrap();
        journal
            .event(
                "agent_checkpoint",
                json!({
                    "node": "agent",
                    "turn": 1,
                    "phase": "turn_completed",
                    "checkpoint": {"next_turn": 2}
                }),
            )
            .unwrap();
        assert!(
            journal
                .state()
                .checkpoints
                .contains_key(&qcg_types::NodePath::root("agent"))
        );
        journal
            .event(
                "step_finished",
                json!({"node":"agent","status":"success","files":[]}),
            )
            .unwrap();
        assert!(journal.state().checkpoints.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }
}
