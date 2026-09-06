use camino::Utf8PathBuf;
use qcg_api::RunEvent;
use qcg_contract::RuntimeLimits;
use serde::Serialize;
use serde_json::Value;
use std::fs::File;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JournalLimits {
    pub max_event_bytes: Option<usize>,
    pub max_total_bytes: Option<usize>,
    pub max_event_count: Option<usize>,
    pub max_state_bytes: Option<usize>,
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
    pub(crate) run_id: String,
    pub(crate) file: Arc<Mutex<File>>,
    pub(crate) state: Arc<Mutex<crate::RunState>>,
    pub(crate) state_path: Utf8PathBuf,
    pub(crate) mirror_stdout: bool,
    pub(crate) event_sender: Option<broadcast::Sender<RunEvent>>,
    pub(crate) limits: JournalLimits,
    pub(crate) stats: Arc<Mutex<JournalStats>>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct JournalMetrics {
    pub(crate) steps_total: u64,
    pub(crate) steps_succeeded: u64,
    pub(crate) steps_failed: u64,
    pub(crate) steps_skipped: u64,
    pub(crate) repair_attempts: u64,
    pub(crate) regenerate_attempts: u64,
    pub(crate) llm_calls: u64,
    pub(crate) tokens_input: u64,
    pub(crate) tokens_output: u64,
    pub(crate) tokens_cached_input: u64,
    pub(crate) steps_executed: u64,
    pub(crate) tokens_total: u64,
    pub(crate) cost_microusd: u64,
    pub(crate) duration_ms: u64,
}
