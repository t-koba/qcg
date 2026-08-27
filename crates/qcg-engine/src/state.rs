use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::ValueBag;
use qcg_types::{ConfirmSpec, FailureCode, FailureDetail, FormSpec, NodePath};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};

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
    pub budget: BudgetState,
    #[serde(default)]
    pub resource_pins: BTreeMap<String, String>,
    #[serde(default)]
    pub pending: Option<Interaction>,
    #[serde(default)]
    pub terminal: Option<TerminalState>,
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
            budget: BudgetState::default(),
            resource_pins: BTreeMap::new(),
            pending: None,
            terminal: None,
        }
    }
}

impl RunState {
    pub fn fold_journal(path: &Utf8Path) -> Result<Self, crate::JournalError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let file = File::open(path)?;
        let mut state = Self::default();
        let mut lines = BufReader::new(file).lines().peekable();
        let mut line_number = 0_usize;
        while let Some(line) = lines.next() {
            line_number += 1;
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(&line) {
                Ok(event) => state.apply(&event)?,
                Err(_) if lines.peek().is_none() => break,
                Err(source) => {
                    return Err(crate::JournalError::InvalidLine {
                        line: line_number,
                        source,
                    });
                }
            }
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
            "run_started" => {
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
            }
            "run_resumed" => {
                self.terminal = None;
            }
            "step_started" => {
                self.budget.steps_executed = self.budget.steps_executed.saturating_add(1);
            }
            "llm_call" => {
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
            "step_finished" => self.apply_step_finished(event)?,
            "step_skipped" => {
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
        let Some(path) = event.get("node").and_then(Value::as_str) else {
            return Ok(());
        };
        let status = event
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("failed");
        if matches!(
            status,
            "success" | "repaired" | "routed" | "answered_on_fail" | "regenerated"
        ) {
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
            self.pending = None;
        } else if !matches!(status, "needs_user" | "needs_confirm") {
            let reason = failure_detail(event, FailureCode::ExecutionFailed, status)?;
            self.vars.set_step_status(path, "failed");
            self.nodes
                .insert(NodePath::root(path), NodeOutcome::Failed { reason });
        }
        Ok(())
    }

    pub fn persist_atomic(&self, path: &Utf8Path) -> Result<(), crate::JournalError> {
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "state path has no parent")
        })?;
        std::fs::create_dir_all(parent)?;
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(&bytes)?;
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
