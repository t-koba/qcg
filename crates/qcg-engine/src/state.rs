use crate::{JournalLimits, read_journal_values_through, serialize_bounded};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::{ConfirmSpec, FormSpec};
use qcg_contract::ValueBag;
use qcg_types::{FailureCode, FailureDetail, NodePath};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
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
    pub tokens_cached_input: u64,
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
    /// Owning generator id derived from the journal identity event, so
    /// disk-only snapshots never parse it out of the run id string.
    /// Required on read: persisted state without an owner is corrupt and
    /// must fail instead of silently degrading to ownerless.
    pub generator_id: Option<String>,
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
    /// Journal seq of the event that installed the current `pending` prompt.
    /// Answer and confirm acceptance must match this generation, not just
    /// the prompt id, so a stale observation cannot accept a prompt that a
    /// peer already regenerated (A02).
    #[serde(default)]
    pub pending_seq: Option<u64>,
    #[serde(default)]
    pub terminal: Option<TerminalState>,
    #[serde(default)]
    pub execution_started: bool,
    /// External side-effect operations by operation_id. Identity binds
    /// run, node, and invocation ONLY: the content digest lives on the
    /// record as a compared attribute, never in the key, so distinct
    /// invocations never share an id even for identical content, while
    /// the same invocation reuses its id across resends and retries
    /// (B07, C04). Statuses: `Started` (remote may have executed, result unknown),
    /// `Succeeded` (the operation completed; a result is cached only when
    /// the output checks passed, and an uncached success refuses automatic
    /// replay instead of re-executing), `FailedClean` (proven nothing
    /// applied, retryable), `FailedIndeterminate` (unknown effects,
    /// refused unless the node opts into at-least-once repeat) (B08, D03).
    /// Replaces the former `operations` status map without migration:
    /// persisted snapshots drop the old map (no `deny_unknown_fields`)
    /// and running state rebuilds from the journal fold, which is the
    /// durable truth.
    #[serde(default)]
    pub operation_records: BTreeMap<String, OperationRecord>,
    /// Guard generations by operation_id: counts `operation_started` events
    /// so each guard journals a truthful attempt number derived from durable
    /// state instead of a caller-supplied constant.
    #[serde(default)]
    pub operation_attempts: BTreeMap<String, u32>,
    /// Durably accepted HITL answers by question id. Later `user_answered`
    /// events win; consulted under the journal lock so exactly one of two
    /// racing answers is accepted per question.
    #[serde(default)]
    pub answers: BTreeMap<String, Value>,
    /// Durably accepted HITL decisions by confirmation id.
    #[serde(default)]
    pub confirmations: BTreeMap<String, bool>,
    /// Whether any `user_cancel_requested` was journaled. Consulted under
    /// the journal lock so a concurrent answer cannot revive a canceled run.
    #[serde(default)]
    pub cancel_requested: bool,
    /// Journaled cancel operation ids for mailbox deduplication.
    #[serde(default)]
    pub cancel_operations: BTreeSet<String>,
    /// Journaled MCP continuation descriptors by reserved pending key.
    #[serde(default)]
    pub mcp_pending: BTreeMap<String, Value>,
    /// Continuation resumptions by pending key to last resumed question id.
    /// A restart that finds its own pending key already resumed for the same
    /// question proves a crash mid-remote-call with an indeterminate remote
    /// outcome, and must fail instead of blindly resuming.
    #[serde(default)]
    pub mcp_resumed: BTreeMap<String, String>,
}

impl Default for RunState {
    fn default() -> Self {
        Self {
            schema_version: RUN_STATE_SCHEMA_VERSION,
            run_id: None,
            contract_sha256: None,
            generator_id: None,
            last_seq: 0,
            inputs: None,
            vars: ValueBag::default(),
            nodes: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            budget: BudgetState::default(),
            resource_pins: BTreeMap::new(),
            pending: None,
            pending_seq: None,
            terminal: None,
            execution_started: false,
            operation_records: BTreeMap::new(),
            operation_attempts: BTreeMap::new(),
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            cancel_requested: false,
            cancel_operations: BTreeSet::new(),
            mcp_pending: BTreeMap::new(),
            mcp_resumed: BTreeMap::new(),
        }
    }
}

/// Outcome class of a finished external operation. `Clean` failures prove
/// nothing was applied (remote-declared errors, validation) and stay
/// retryable; `Indeterminate` covers timeouts, disconnects, and kills where
/// effects are unknowable (B08).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Started,
    Succeeded,
    FailedClean,
    FailedIndeterminate,
}

/// Durable record of one external operation invocation: the content digest
/// it was admitted with, its latest status, and a bounded cached result
/// for same-invocation resends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationRecord {
    pub digest: String,
    pub status: OperationStatus,
    /// Cached success result for resends, or `None` when the result was
    /// too large to cache or never recorded: resends then route to
    /// explicit manual recovery instead of silent re-execution.
    #[serde(default)]
    pub result: Option<Value>,
    /// Invocation this record was admitted with. Current writers always
    /// set it; pre-upgrade records predate it and read as empty. A
    /// current-scheme id whose record names a different invocation is
    /// treated as a lookup miss gone wrong and refused, never executed.
    #[serde(default)]
    pub invocation: String,
}

/// Results larger than this are not cached for resends: replays route to
/// explicit manual recovery instead of bloating the journal or returning
/// truncated results.
pub const OPERATION_RESULT_MAX_BYTES: usize = 64 * 1024;

/// Returns `Some(value)` when the value serializes within the resend-cache
/// bound, else `None` (caller routes to manual recovery on resend).
pub fn cacheable_operation_result(value: &Value) -> Option<Value> {
    let bytes = serde_json::to_vec(value).ok()?;
    (bytes.len() <= OPERATION_RESULT_MAX_BYTES).then(|| value.clone())
}

/// Stable operation id for an external side effect (ID scheme v3, C04).
/// The logical key is run, node, and invocation ONLY: the content digest
/// lives on the record as a compared attribute, never in the key, so a
/// same-invocation content change reaches the changed-content Refuse
/// check instead of silently starting a fresh operation. The invocation
/// binds by its full SHA-256 hex digest: truncated fragments collide
/// across distinct calls (C04). The invocation that produced the id is
/// verified against the stored record on lookup.
pub fn operation_id_for(run_id: &str, node_id: &str, invocation_id: &str) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{run_id}:{node_id}:{}",
        hex::encode(Sha256::digest(invocation_id.as_bytes()))
    )
}

/// ID scheme v2 (B07 era): run, node, 16-hex content digest, and an
/// 8-hex invocation fragment. Superseded by [`operation_id_for`];
/// recognized on lookup so in-flight v2 records keep their protection
/// across the upgrade instead of silently restarting (C03).
pub fn operation_id_for_v2(
    run_id: &str,
    node_id: &str,
    operation_digest: &str,
    invocation_id: &str,
) -> String {
    use sha2::{Digest, Sha256};
    let fragment = hex::encode(Sha256::digest(invocation_id.as_bytes()));
    format!(
        "{run_id}:{node_id}:{}:{}",
        &operation_digest[..16.min(operation_digest.len())],
        &fragment[..8.min(fragment.len())],
    )
}

/// ID scheme v1 (pre-B07): run, node, and 16-hex content digest with no
/// invocation dimension. Same lookup recognition as v2 (C03): an old
/// unfinished side effect refuses automatic replay instead of silently
/// re-executing under a new id.
pub fn legacy_operation_id_for(run_id: &str, node_id: &str, operation_digest: &str) -> String {
    format!(
        "{run_id}:{node_id}:{}",
        &operation_digest[..16.min(operation_digest.len())]
    )
}

/// Content-derived invocation identity for single-shot step executions:
/// one execution per node per content, stable across restarts.
pub fn content_invocation_id(operation_digest: &str) -> String {
    format!("content:{operation_digest}")
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
            let parsed = qcg_api::RunEvent::from_flat(&event).map_err(|message| {
                crate::JournalError::InvalidEvent(format!("invalid journal event: {message}"))
            })?;
            if through_seq.is_some_and(|limit| parsed.seq > limit) {
                break;
            }
            state.apply(&event)?;
        }
        Ok(state)
    }

    /// Folds already-read journal values with the same validation as
    /// [`Self::fold_journal`]: every event is type-checked before it
    /// affects state, so readers that already hold the values never
    /// re-read the file to fold.
    pub fn fold_values(events: &[Value]) -> Result<Self, crate::JournalError> {
        let mut state = Self::default();
        for event in events {
            qcg_api::RunEvent::from_flat(event).map_err(|message| {
                crate::JournalError::InvalidEvent(format!("invalid journal event: {message}"))
            })?;
            state.apply(event)?;
        }
        Ok(state)
    }

    pub fn apply(&mut self, event: &Value) -> Result<(), crate::JournalError> {
        if let Some(seq) = event.get("seq").and_then(Value::as_u64) {
            // Strict monotonicity: duplicate or rewound seq values indicate
            // concurrent writers or journal corruption and must fail closed
            // instead of silently keeping last-writer-wins state (A01).
            if seq == 0 {
                return Err(crate::JournalError::InvalidEvent(
                    "journal event seq must be greater than zero".into(),
                ));
            }
            if seq <= self.last_seq {
                return Err(crate::JournalError::InvalidEvent(format!(
                    "journal event seq {seq} is not greater than last_seq {}",
                    self.last_seq
                )));
            }
            self.last_seq = seq;
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
                if self.generator_id.is_none() {
                    self.generator_id =
                        event
                            .get("generator")
                            .and_then(Value::as_str)
                            .map(|generator| {
                                generator
                                    .split_once('@')
                                    .map(|(id, _)| id.to_string())
                                    .unwrap_or_else(|| generator.to_string())
                            });
                }
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
                // Pre-provided answers and confirmations ride on run_queued
                // for unattended runs; later events win on the same key.
                if kind == "run_queued" {
                    if let Some(map) = event.get("answers").and_then(Value::as_object) {
                        for (key, value) in map {
                            self.answers.insert(key.clone(), value.clone());
                        }
                    }
                    if let Some(map) = event.get("confirmations").and_then(Value::as_object) {
                        for (key, value) in map {
                            if let Some(approved) = value.as_bool() {
                                self.confirmations.insert(key.clone(), approved);
                            }
                        }
                    }
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
                    self.budget.tokens_cached_input =
                        self.budget.tokens_cached_input.saturating_add(
                            tokens
                                .get("cached_input")
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
                    self.pending_seq = event.get("seq").and_then(Value::as_u64);
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
                    self.pending_seq = event.get("seq").and_then(Value::as_u64);
                }
            }
            "user_answered" | "user_confirmed" => {
                // Durability record for an accepted HITL response. The engine
                // consumes the persisted answers map on resume, so the
                // pending prompt is cleared here and rehydrate restores the
                // values from the same events. Later events win on the same
                // key, matching read_persisted_hitl.
                if kind == "user_answered" {
                    if let (Some(id), Some(values)) = (
                        event.get("question_id").and_then(Value::as_str),
                        event.get("values").cloned(),
                    ) {
                        self.answers.insert(id.to_string(), values);
                    }
                } else if let (Some(id), Some(approved)) = (
                    event.get("confirmation_id").and_then(Value::as_str),
                    event.get("approved").and_then(Value::as_bool),
                ) {
                    self.confirmations.insert(id.to_string(), approved);
                }
                self.pending = None;
                self.pending_seq = None;
                self.terminal = None;
            }
            "user_cancel_requested" => {
                self.cancel_requested = true;
                // Only operation-bound cancels join the mailbox dedup set.
                // Pre-mailbox events carry no operation id and are observed
                // through cancel_requested instead.
                if let Some(operation_id) = event.get("operation_id").and_then(Value::as_str) {
                    self.cancel_operations.insert(operation_id.to_string());
                }
            }
            "mcp_input_pending" => {
                // Same filtered shape as the service-side pending reader so
                // both paths observe identical descriptors.
                if let Some(key) = event.get("pending_key").and_then(Value::as_str) {
                    let mut pending = serde_json::Map::new();
                    for field in [
                        "node",
                        "question_id",
                        "server",
                        "tool",
                        "alias",
                        "call_id",
                        "arguments",
                        "request_state",
                        "input_requests",
                    ] {
                        if let Some(value) = event.get(field).cloned() {
                            pending.insert(field.to_string(), value);
                        }
                    }
                    self.mcp_pending
                        .insert(key.to_string(), Value::Object(pending));
                }
            }
            "mcp_continuation_consumed" => {
                if let Some(key) = event.get("pending_key").and_then(Value::as_str) {
                    self.mcp_pending.remove(key);
                    self.mcp_resumed.remove(key);
                    self.operation_records.insert(
                        format!("mcp:{key}"),
                        OperationRecord {
                            digest: String::new(),
                            status: OperationStatus::Succeeded,
                            result: None,
                            // MCP continuation records key outside the
                            // operation-id schemes and carry no invocation.
                            invocation: String::new(),
                        },
                    );
                }
            }
            "mcp_continuation_resumed" => {
                if let (Some(key), Some(question_id)) = (
                    event.get("pending_key").and_then(Value::as_str),
                    event.get("question_id").and_then(Value::as_str),
                ) {
                    self.mcp_resumed
                        .insert(key.to_string(), question_id.to_string());
                }
            }
            "run_finished" => {
                self.pending = None;
                self.pending_seq = None;
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
                self.pending_seq = None;
                self.terminal = Some(TerminalState::Failed);
            }
            "run_canceled" => {
                self.pending = None;
                self.pending_seq = None;
                self.terminal = Some(TerminalState::Canceled);
            }
            "run_interrupted" => {
                self.pending = None;
                self.pending_seq = None;
                self.terminal = Some(TerminalState::Interrupted);
            }
            "operation_started" => {
                if let Some(id) = event.get("operation_id").and_then(Value::as_str) {
                    // A present-but-non-string field is journal corruption:
                    // defaulting it to empty could skip the invocation
                    // mismatch check, so fail closed instead.
                    let digest = match event.get("operation_digest") {
                        None => String::new(),
                        Some(value) => value.as_str().map(str::to_string).ok_or_else(|| {
                            crate::JournalError::InvalidEvent(
                                "operation_started operation_digest must be a string".into(),
                            )
                        })?,
                    };
                    let invocation = match event.get("invocation_id") {
                        // Pre-invocation journals legitimately omit it.
                        None => String::new(),
                        Some(value) => value.as_str().map(str::to_string).ok_or_else(|| {
                            crate::JournalError::InvalidEvent(
                                "operation_started invocation_id must be a string".into(),
                            )
                        })?,
                    };
                    let record =
                        self.operation_records
                            .entry(id.to_string())
                            .or_insert_with(|| OperationRecord {
                                digest: digest.clone(),
                                status: OperationStatus::Started,
                                result: None,
                                invocation: invocation.clone(),
                            });
                    // A restarted attempt reuses its record; only the status
                    // motion matters, never a digest rewrite.
                    record.status = OperationStatus::Started;
                    let attempts = self.operation_attempts.entry(id.to_string()).or_default();
                    *attempts = attempts.saturating_add(1);
                }
            }
            "operation_finished" => {
                if let Some(id) = event.get("operation_id").and_then(Value::as_str) {
                    // One-way migration for pre-split journals, not a
                    // compatibility shim: legacy writers recorded only
                    // `success`/`error` without distinguishing clean from
                    // indeterminate failures, and no current writer emits
                    // those forms. Success without a cached result and
                    // every legacy error fail toward explicit handling:
                    // unknown effects must never read as safely retryable
                    // (B08). Journals written by this version always carry
                    // the split statuses and never take this path.
                    let (status, result) = match event.get("status").and_then(Value::as_str) {
                        Some("success") => {
                            (OperationStatus::Succeeded, event.get("result").cloned())
                        }
                        Some("clean") => (OperationStatus::FailedClean, None),
                        Some("indeterminate") => (OperationStatus::FailedIndeterminate, None),
                        _ => (OperationStatus::FailedIndeterminate, None),
                    };
                    let record =
                        self.operation_records
                            .entry(id.to_string())
                            .or_insert_with(|| OperationRecord {
                                digest: String::new(),
                                status,
                                result: None,
                                invocation: String::new(),
                            });
                    // The started record carries the authoritative digest;
                    // finished events repeat it, but an absent one must not
                    // blank the check that refuses changed-content resends.
                    if let Some(digest) = event.get("operation_digest").and_then(Value::as_str) {
                        record.digest = digest.to_string();
                    }
                    record.status = status;
                    record.result = result;
                }
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
            self.pending_seq = None;
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
        max_bytes: Option<usize>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn operation_event(seq: u64, kind: &str, id: &str, status: Option<&str>) -> Value {
        let mut event = json!({
            "t": kind,
            "seq": seq,
            "operation_id": id,
        });
        if let Some(status) = status {
            event["status"] = json!(status);
        }
        event
    }

    #[test]
    fn operation_attempts_count_guards_per_id() {
        let mut state = RunState::default();
        state
            .apply(&operation_event(1, "operation_started", "op-a", None))
            .expect("first guard should fold");
        state
            .apply(&operation_event(2, "operation_started", "op-b", None))
            .expect("other id should fold");
        state
            .apply(&operation_event(
                3,
                "operation_finished",
                "op-a",
                Some("error"),
            ))
            .expect("finish should fold");
        state
            .apply(&operation_event(4, "operation_started", "op-a", None))
            .expect("retry guard should fold");
        assert_eq!(state.operation_attempts.get("op-a"), Some(&2));
        assert_eq!(state.operation_attempts.get("op-b"), Some(&1));
        assert!(matches!(
            state
                .operation_records
                .get("op-a")
                .map(|record| record.status),
            Some(OperationStatus::Started)
        ));
    }

    #[test]
    fn legacy_finished_error_folds_indeterminate() {
        // Writers that only recorded `error` predate the clean vs
        // indeterminate split: unknown effects must never read as safely
        // retryable.
        let mut state = RunState::default();
        state
            .apply(&operation_event(1, "operation_started", "op-a", None))
            .expect("guard should fold");
        state
            .apply(&operation_event(
                2,
                "operation_finished",
                "op-a",
                Some("error"),
            ))
            .expect("finish should fold");
        assert!(matches!(
            state
                .operation_records
                .get("op-a")
                .map(|record| record.status),
            Some(OperationStatus::FailedIndeterminate)
        ));
    }

    #[test]
    fn pending_seq_tracks_prompt_generation() {
        let mut state = RunState::default();
        assert_eq!(state.pending_seq, None);
        state
            .apply(&json!({
                "t": "run_waiting",
                "seq": 1,
                "question": {"id": "q1", "title": "first", "fields": []},
            }))
            .expect("waiting should fold");
        assert_eq!(state.pending_seq, Some(1));
        // A regenerated prompt advances the generation even for the same id.
        state
            .apply(&json!({
                "t": "run_waiting",
                "seq": 2,
                "question": {"id": "q1", "title": "second", "fields": []},
            }))
            .expect("regenerated waiting should fold");
        assert_eq!(state.pending_seq, Some(2));
        state
            .apply(&json!({
                "t": "user_answered",
                "seq": 3,
                "question_id": "q1",
                "values": {"answer": "yes"},
            }))
            .expect("answer should fold");
        assert_eq!(state.pending_seq, None);
        state
            .apply(&json!({
                "t": "confirm_request",
                "seq": 4,
                "confirm": {"id": "c1", "title": "go", "kind": "k", "target": "t", "dry_run": false},
            }))
            .expect("confirm request should fold");
        assert_eq!(state.pending_seq, Some(4));
        state
            .apply(&json!({"t": "run_canceled", "seq": 5}))
            .expect("cancel should fold");
        assert_eq!(state.pending_seq, None);
    }
}
