use crate::{JournalLimits, read_journal_values_through, serialize_bounded};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::ValueBag;
use qcg_types::{ConfirmSpec, FailureCode, FailureDetail, FormSpec, NodePath};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
#[cfg(unix)]
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;

pub const RUN_STATE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TerminalState {
    Succeeded,
    Failed,
    Canceled,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Interaction {
    Question { question: FormSpec },
    Confirmation { confirm: ConfirmSpec },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FilePin {
    pub path: Utf8PathBuf,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum NodeOutcome {
    Success {
        #[serde(default)]
        output: Option<Value>,
        #[serde(default)]
        files: Vec<FilePin>,
    },
    Skipped {
        reason: FailureDetail,
    },
    Failed {
        reason: FailureDetail,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BudgetState {
    #[serde(default)]
    pub steps_executed: usize,
    #[serde(default)]
    pub steps_succeeded: u64,
    #[serde(default)]
    pub steps_failed: u64,
    #[serde(default)]
    pub steps_skipped: u64,
    #[serde(default)]
    pub repair_attempts: u64,
    #[serde(default)]
    pub regenerate_attempts: u64,
    #[serde(default)]
    pub llm_calls: u64,
    #[serde(default)]
    pub tokens_input: u64,
    #[serde(default)]
    pub tokens_output: u64,
    #[serde(default)]
    pub cost_microusd: u64,
    #[serde(default)]
    pub started_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub schema_version: u32,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub contract_sha256: Option<String>,
    #[serde(default)]
    pub last_seq: u64,
    /// The canonical inputs recorded by `run_started`; retained for replay.
    #[serde(default)]
    pub inputs: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    pub vars: ValueBag,
    #[serde(default)]
    pub nodes: BTreeMap<NodePath, NodeOutcome>,
    #[serde(default)]
    pub checkpoints: BTreeMap<NodePath, Value>,
    #[serde(default)]
    pub budget: BudgetState,
    #[serde(default)]
    pub resource_pins: BTreeMap<String, String>,
    #[serde(default)]
    pub pending: Option<Interaction>,
    #[serde(default)]
    pub terminal: Option<TerminalState>,
    #[serde(default)]
    pub execution_started: bool,
}

impl Default for RunState {
    fn default() -> Self {
        Self {
            schema_version: RUN_STATE_SCHEMA_VERSION,
            run_id: None,
            contract_sha256: None,
            last_seq: 0,
            inputs: None,
            vars: ValueBag::default(),
            nodes: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            budget: BudgetState::default(),
            resource_pins: BTreeMap::new(),
            pending: None,
            terminal: None,
            execution_started: false,
        }
    }
}

impl RunState {
    pub fn fold_journal(path: &Utf8Path) -> Result<Self, crate::JournalError> {
        Self::fold_journal_through(path, None)
    }

    pub fn fold_journal_with_limits(
        path: &Utf8Path,
        limits: JournalLimits,
    ) -> Result<Self, crate::JournalError> {
        Self::fold_journal_through_with_limits(path, None, limits)
    }

    pub fn fold_journal_through(
        path: &Utf8Path,
        through_seq: Option<u64>,
    ) -> Result<Self, crate::JournalError> {
        Self::fold_journal_through_with_limits(path, through_seq, JournalLimits::default())
    }

    pub fn fold_journal_through_with_limits(
        path: &Utf8Path,
        through_seq: Option<u64>,
        limits: JournalLimits,
    ) -> Result<Self, crate::JournalError> {
        let mut state = Self::default();
        for event in read_journal_values_through(path, through_seq, limits)?.events {
            let parsed = qcg_types::RunEvent::from_flat(&event).map_err(|message| {
                crate::JournalError::InvalidEvent(format!("invalid journal event: {message}"))
            })?;
            if through_seq.is_some_and(|limit| parsed.seq > limit) {
                break;
            }
            state.apply(&event)?;
        }
        Ok(state)
    }

    pub fn apply(&mut self, event: &Value) -> Result<(), crate::JournalError> {
        if let Some(seq) = event.get("seq").and_then(Value::as_u64) {
            self.last_seq = self.last_seq.max(seq);
        }
        let kind = event
            .get("t")
            .and_then(Value::as_str)
            .ok_or_else(|| crate::JournalError::InvalidEvent("event kind is required".into()))?;
        match kind {
            "run_queued" | "run_started" => {
                self.run_id = event
                    .get("run_id")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                self.contract_sha256 = event
                    .get("contract_sha256")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                if let Some(inputs) = event.get("inputs").and_then(Value::as_object) {
                    let inputs = inputs
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<BTreeMap<_, _>>();
                    self.inputs = Some(inputs.clone());
                    self.vars = ValueBag::with_inputs(inputs);
                }
                self.terminal = None;
                if self.budget.started_at.is_none() {
                    self.budget.started_at =
                        event.get("ts").and_then(Value::as_str).map(str::to_string);
                }
                if kind == "run_started" {
                    self.execution_started = true;
                }
            }
            "run_resumed" => {
                self.terminal = None;
            }
            "step_started" => {
                self.budget.steps_executed = self.budget.steps_executed.saturating_add(1);
            }
            "llm_call" => {
                self.budget.llm_calls = self.budget.llm_calls.saturating_add(1);
                if let Some(tokens) = event.get("tokens").and_then(Value::as_object) {
                    self.budget.tokens_input = self.budget.tokens_input.saturating_add(
                        tokens
                            .get("input")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                    );
                    self.budget.tokens_output = self.budget.tokens_output.saturating_add(
                        tokens
                            .get("output")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                    );
                }
                self.budget.cost_microusd = self.budget.cost_microusd.saturating_add(
                    event
                        .get("cost_microusd")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                );
            }
            "agent_checkpoint" => {
                if let (Some(path), Some(checkpoint)) = (
                    event.get("node").and_then(Value::as_str),
                    event.get("checkpoint").cloned(),
                ) {
                    self.checkpoints.insert(NodePath::root(path), checkpoint);
                }
            }
            "state_patched" => {
                if let Some(values) = event.get("inputs").and_then(Value::as_object) {
                    let values = values
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<BTreeMap<_, _>>();
                    if let Some(inputs) = self.inputs.as_mut() {
                        inputs.extend(values.clone());
                    }
                    self.vars.patch_inputs(values);
                }
                if let Some(values) = event.get("step_outputs").and_then(Value::as_object) {
                    self.vars.patch_step_outputs(
                        values
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect(),
                    );
                }
                if let Some(values) = event.get("step_statuses").and_then(Value::as_object) {
                    let values = values
                        .iter()
                        .map(|(key, value)| {
                            value.as_str().map(|value| (key.clone(), value.to_string()))
                        })
                        .collect::<Option<BTreeMap<_, _>>>()
                        .ok_or_else(|| {
                            crate::JournalError::InvalidEvent(
                                "state patch statuses must be strings".into(),
                            )
                        })?;
                    self.vars.patch_step_statuses(values);
                }
            }
            "step_finished" => self.apply_step_finished(event)?,
            "step_skipped" => {
                self.budget.steps_skipped = self.budget.steps_skipped.saturating_add(1);
                if let Some(path) = event.get("node").and_then(Value::as_str) {
                    let reason =
                        failure_detail(event, FailureCode::ExecutionFailed, "step was skipped")?;
                    self.nodes.insert(
                        NodePath::root(path),
                        NodeOutcome::Skipped {
                            reason: reason.clone(),
                        },
                    );
                    self.vars.set_step_status(path, "skipped");
                }
            }
            "repair_attempt_started" => {
                self.budget.repair_attempts = self.budget.repair_attempts.saturating_add(1);
            }
            "regenerate_attempt_started" => {
                self.budget.regenerate_attempts = self.budget.regenerate_attempts.saturating_add(1);
            }
            "resource" => {
                if let (Some(name), Some(digest)) = (
                    event.get("name").and_then(Value::as_str),
                    event.get("sha256").and_then(Value::as_str),
                ) {
                    self.resource_pins
                        .insert(name.to_string(), digest.to_string());
                }
            }
            "confirm_request" => {
                if let Some(value) = event.get("confirm").cloned() {
                    let confirm = serde_json::from_value(value).map_err(|error| {
                        crate::JournalError::InvalidEvent(format!(
                            "invalid confirmation payload: {error}"
                        ))
                    })?;
                    self.pending = Some(Interaction::Confirmation { confirm });
                }
            }
            "run_waiting" => {
                if let Some(value) = event.get("question").cloned() {
                    let question = serde_json::from_value(value).map_err(|error| {
                        crate::JournalError::InvalidEvent(format!(
                            "invalid question payload: {error}"
                        ))
                    })?;
                    self.pending = Some(Interaction::Question { question });
                }
            }
            "run_finished" => {
                self.pending = None;
                self.terminal = Some(
                    if event.get("status").and_then(Value::as_str) == Some("success") {
                        TerminalState::Succeeded
                    } else {
                        TerminalState::Failed
                    },
                );
            }
            "run_error" => {
                self.pending = None;
                self.terminal = Some(TerminalState::Failed);
            }
            "run_canceled" => {
                self.pending = None;
                self.terminal = Some(TerminalState::Canceled);
            }
            "run_interrupted" => {
                self.pending = None;
                self.terminal = Some(TerminalState::Interrupted);
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_step_finished(&mut self, event: &Value) -> Result<(), crate::JournalError> {
        let path = event.get("node").and_then(Value::as_str).ok_or_else(|| {
            crate::JournalError::InvalidEvent("step_finished node is required".into())
        })?;
        let status = event.get("status").and_then(Value::as_str).ok_or_else(|| {
            crate::JournalError::InvalidEvent("step_finished status is required".into())
        })?;
        if matches!(
            status,
            "success" | "repaired" | "routed" | "answered_on_fail" | "regenerated"
        ) {
            self.budget.steps_succeeded = self.budget.steps_succeeded.saturating_add(1);
            let output = event
                .get("output")
                .filter(|value| !value.is_null())
                .cloned();
            let output_name = event
                .get("output_name")
                .and_then(Value::as_str)
                .unwrap_or(path);
            if let Some(output) = output.clone() {
                self.vars.set_step_output(output_name, output);
            }
            self.vars.set_step_status(output_name, "success");
            let files = event
                .get("files")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| {
                    crate::JournalError::InvalidEvent(format!(
                        "invalid step output file pins: {error}"
                    ))
                })?
                .unwrap_or_default();
            self.nodes
                .insert(NodePath::root(path), NodeOutcome::Success { output, files });
            self.checkpoints.remove(&NodePath::root(path));
            self.pending = None;
        } else if !matches!(status, "needs_user" | "needs_confirm") {
            self.budget.steps_failed = self.budget.steps_failed.saturating_add(1);
            let reason = failure_detail(event, FailureCode::ExecutionFailed, status)?;
            self.vars.set_step_status(path, "failed");
            self.nodes
                .insert(NodePath::root(path), NodeOutcome::Failed { reason });
            self.checkpoints.remove(&NodePath::root(path));
        }
        Ok(())
    }

    pub fn persist_atomic(&self, path: &Utf8Path) -> Result<(), crate::JournalError> {
        self.persist_atomic_with_limits(path, JournalLimits::default().max_state_bytes)
    }

    pub fn persist_atomic_with_limits(
        &self,
        path: &Utf8Path,
        max_bytes: usize,
    ) -> Result<(), crate::JournalError> {
        let bytes = serialize_bounded(self, max_bytes, "state")?;
        Self::persist_serialized_atomic(path, &bytes)
    }

    pub(crate) fn persist_serialized_atomic(
        path: &Utf8Path,
        bytes: &[u8],
    ) -> Result<(), crate::JournalError> {
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "state path has no parent")
        })?;
        std::fs::create_dir_all(parent)?;
        let tmp = path.with_extension("json.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_data()?;
        std::fs::rename(&tmp, path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_data()?;
        Ok(())
    }
}

fn failure_detail(
    event: &Value,
    default_code: FailureCode,
    default_message: &str,
) -> Result<FailureDetail, crate::JournalError> {
    let Some(reason) = event.get("reason") else {
        return Ok(FailureDetail::new(default_code, default_message));
    };
    serde_json::from_value(reason.clone()).map_err(|error| {
        crate::JournalError::InvalidEvent(format!("invalid structured failure detail: {error}"))
    })
}
