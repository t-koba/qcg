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

/// Requires a field to be PRESENT on read while still accepting an explicit
/// null for `Option` fields. Serde implicitly defaults missing `Option`
/// fields to `None`, so removing `#[serde(default)]` alone cannot make them
/// required (E07): every `Option` field in the persisted state uses this so
/// a partial or old-format record fails closed instead of silently
/// degrading.
pub(crate) fn required_presence<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

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
        #[serde(deserialize_with = "required_presence")]
        output: Option<Value>,
        files: Vec<FilePin>,
    },
    Skipped {
        reason: FailureDetail,
    },
    Failed {
        reason: FailureDetail,
    },
}

// No `#[serde(default)]` compat shims below (E07): every field is required
// on read so a partial or old-format record fails closed instead of
// silently degrading to zero values. Writers always persist complete
// records, so a missing field is corruption, not a version to tolerate.

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BudgetState {
    pub steps_executed: usize,
    /// Durable budget consumption (F13): sum of `budget_charged` deltas.
    /// Live execution and journal recovery share this counter so retry and
    /// foreach charge rules mean the same after a restart. `steps_executed`
    /// stays as the observational event count (step_started events).
    #[serde(default)]
    pub budget_charged: usize,
    /// Whether any `budget_charged` event folded (F13 migration): new
    /// journals seed the live tracker from `budget_charged`; journals that
    /// predate the event fall back to `steps_executed`.
    #[serde(default)]
    pub has_budget_charges: bool,
    pub steps_succeeded: u64,
    pub steps_failed: u64,
    pub steps_skipped: u64,
    pub repair_attempts: u64,
    pub regenerate_attempts: u64,
    pub llm_calls: u64,
    pub tokens_input: u64,
    pub tokens_output: u64,
    pub tokens_cached_input: u64,
    pub cost_microusd: u64,
    #[serde(deserialize_with = "required_presence")]
    pub started_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub schema_version: u32,
    #[serde(deserialize_with = "required_presence")]
    pub run_id: Option<String>,
    #[serde(deserialize_with = "required_presence")]
    pub contract_sha256: Option<String>,
    /// Owning generator id derived from the journal identity event, so
    /// disk-only snapshots never parse it out of the run id string.
    /// Required on read: persisted state without an owner is corrupt and
    /// must fail instead of silently degrading to ownerless.
    #[serde(deserialize_with = "required_presence")]
    pub generator_id: Option<String>,
    pub last_seq: u64,
    /// Run metadata admitted with the run; metadata only.
    pub labels: BTreeMap<String, String>,
    /// Last assigned audit-record seq. Durable and audit records share one
    /// monotonic seq space so a merged reader observes a single order;
    /// `last_seq` still tracks durable records only, so resume folds remain
    /// independent of audit policy. Persisted alongside `last_seq`.
    pub audit_seq: u64,
    /// The canonical inputs recorded by `run_started`; retained for replay.
    #[serde(deserialize_with = "required_presence")]
    pub inputs: Option<BTreeMap<String, Value>>,
    pub vars: ValueBag,
    pub nodes: BTreeMap<NodePath, NodeOutcome>,
    pub checkpoints: BTreeMap<NodePath, Value>,
    pub budget: BudgetState,
    pub resource_pins: BTreeMap<String, String>,
    /// Latest pinned revision per workspace path. Values always reflect the
    /// journal-latest write (sequential fold overwrites); iteration order is
    /// the map's key order, not journal order. Resume verifies the workspace
    /// against this revision and every historical revision against its
    /// immutable blob, so a later step overwriting an earlier pin is
    /// legitimate while a rollback or tamper is rejected (E06).
    pub latest_file_pins: BTreeMap<String, String>,
    /// Finished executions per node in journal order. Single-shot side
    /// effects derive their invocation identity from this count, so retries
    /// and crash resumes keep the current execution's approval while a
    /// repair or regenerate is a new invocation that re-confirms under
    /// `invocation` scope (Q1).
    pub node_executions: BTreeMap<String, u64>,
    /// Every file revision any step finished with. Values accumulate in
    /// journal order (later writes add revisions, never remove); iteration
    /// order is the map's key order. Resume verifies each revision against
    /// its immutable blob, so a same-node overwrite whose intermediate
    /// revision is no longer the node's final outcome is still checked
    /// instead of silently trusted (E06).
    pub historical_file_pins: BTreeMap<String, BTreeSet<String>>,
    #[serde(deserialize_with = "required_presence")]
    pub pending: Option<Interaction>,
    /// Journal seq of the event that installed the current `pending` prompt.
    /// Answer and confirm acceptance must match this generation, not just
    /// the prompt id, so a stale observation cannot accept a prompt that a
    /// peer already regenerated (A02).
    #[serde(deserialize_with = "required_presence")]
    pub pending_seq: Option<u64>,
    #[serde(deserialize_with = "required_presence")]
    pub terminal: Option<TerminalState>,
    pub execution_started: bool,
    /// External side-effect operations by operation_id. Identity binds
    /// run, node, and invocation ONLY: the content digest lives on the
    /// record as a compared attribute, never in the key, so distinct
    /// invocations never share an id even for identical content, while
    /// the same invocation reuses its id across resends and retries.
    /// Statuses: `Started` (remote may have executed, result unknown),
    /// `Succeeded` (the operation completed; a small result is cached
    /// inline while a large result spills to a sidecar blob named by
    /// `result_ref`, and an uncached success refuses automatic replay
    /// instead of re-executing), `FailedClean` (proven nothing applied,
    /// retryable), `FailedIndeterminate` (unknown effects, refused unless
    /// the node opts into at-least-once repeat).
    pub operation_records: BTreeMap<String, OperationRecord>,
    /// Guard generations by operation_id: counts `operation_started` events
    /// so each guard journals a truthful attempt number derived from durable
    /// state instead of a caller-supplied constant.
    pub operation_attempts: BTreeMap<String, u32>,
    /// Durably accepted HITL answers by question id. Later `user_answered`
    /// events win; consulted under the journal lock so exactly one of two
    /// racing answers is accepted per question.
    pub answers: BTreeMap<String, Value>,
    /// Durably accepted HITL decisions by confirmation id.
    pub confirmations: BTreeMap<String, bool>,
    /// Whether any `user_cancel_requested` was journaled. Consulted under
    /// the journal lock so a concurrent answer cannot revive a canceled run.
    pub cancel_requested: bool,
    /// Journaled cancel operation ids for mailbox deduplication.
    pub cancel_operations: BTreeSet<String>,
    /// Journaled MCP continuation descriptors by reserved pending key.
    pub mcp_pending: BTreeMap<String, Value>,
    /// Continuation resumptions by pending key to last resumed question id.
    /// A restart that finds its own pending key already resumed for the same
    /// question proves a crash mid-remote-call with an indeterminate remote
    /// outcome, and must fail instead of blindly resuming.
    pub mcp_resumed: BTreeMap<String, String>,
    /// Hooks settled with `on_error = warn` (F07-02): a Warn-ended hook is
    /// processed, not pending. Resume skips it instead of re-executing so a
    /// HITL resume after a warned `run_started` hook has no undeclared
    /// duplicate execution. Successful hooks replay via node outcomes.
    #[serde(default)]
    pub hooks_settled: BTreeSet<String>,
}

impl Default for RunState {
    fn default() -> Self {
        Self {
            schema_version: RUN_STATE_SCHEMA_VERSION,
            run_id: None,
            contract_sha256: None,
            generator_id: None,
            last_seq: 0,
            labels: BTreeMap::new(),
            audit_seq: 0,
            inputs: None,
            vars: ValueBag::default(),
            nodes: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            budget: BudgetState::default(),
            resource_pins: BTreeMap::new(),
            latest_file_pins: BTreeMap::new(),
            node_executions: BTreeMap::new(),
            historical_file_pins: BTreeMap::new(),
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
            hooks_settled: BTreeSet::new(),
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
    /// Cached success result for resends, or `None` when the result spilled
    /// to a sidecar blob (see `result_ref`) or was never recorded: resends
    /// then route to explicit manual recovery instead of silent re-execution.
    #[serde(deserialize_with = "required_presence")]
    pub result: Option<Value>,
    /// Sidecar blob name holding a large success result that exceeded the
    /// inline cache bound. The name is the content hash of the spilled
    /// bytes, verified on load; `None` means the result (if any) rides
    /// inline in `result` (E07).
    #[serde(deserialize_with = "required_presence")]
    pub result_ref: Option<String>,
    /// Invocation this record was admitted with. A record whose id maps to
    /// a different invocation is treated as corruption and refused, never
    /// executed.
    pub invocation: String,
}

/// Results larger than this are not cached inline for resends: they spill
/// to a sidecar blob under the run meta dir, and replays load the blob
/// instead of routing to manual recovery or returning truncated results.
pub const OPERATION_RESULT_MAX_BYTES: usize = 64 * 1024;

/// Returns `Some(value)` when the value serializes within the inline
/// resend-cache bound, else `None` (the caller spills to a sidecar blob).
/// Serialization failures propagate with context instead of silently
/// reading as "too large": an unserializable result must fail closed (E07).
pub fn cacheable_operation_result(value: &Value) -> Result<Option<Value>, serde_json::Error> {
    let bytes = serde_json::to_vec(value)?;
    Ok((bytes.len() <= OPERATION_RESULT_MAX_BYTES).then(|| value.clone()))
}

/// Stable operation id for an external side effect. The logical key is run,
/// node, and invocation ONLY: the content digest lives on the record as a
/// compared attribute, never in the key, so a same-invocation content change
/// reaches the changed-content Refuse check instead of silently starting a
/// fresh operation. The invocation binds by its full SHA-256 hex digest so
/// distinct calls can never share an id. The invocation that produced the id
/// is verified against the stored record on lookup.
pub fn operation_id_for(run_id: &str, node_id: &str, invocation_id: &str) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{run_id}:{node_id}:{}",
        hex::encode(Sha256::digest(invocation_id.as_bytes()))
    )
}

impl RunState {
    /// Full journal fold without caller limits (recovery, fork, and resume
    /// verification paths that must observe complete durable truth).
    /// Kept only with this documented necessity and always bounded by the
    /// journal's own configured limits via `JournalLimits::default`
    /// (which the writer enforces on append); callers with explicit caps
    /// use `fold_journal_with_limits` instead.
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
        // Journal format gate: the identity record declares the schema
        // version. An unknown version is refused instead of being folded
        // under current assumptions (fail closed, no compatibility branch).
        if matches!(kind, "run_queued" | "run_started")
            && let Some(version) = event.get("schema_version").and_then(Value::as_u64)
            && version != u64::from(qcg_api::JOURNAL_SCHEMA_VERSION)
        {
            return Err(crate::JournalError::InvalidEvent(format!(
                "unsupported journal schema_version {version}; this qcg implements {}",
                qcg_api::JOURNAL_SCHEMA_VERSION
            )));
        }
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
                // Queue-inclusive elapsed start (E11): the first queued or
                // started event stamps the run-wide budget clock, so queue
                // time consumes the budget. Later events never move it, and
                // HITL suspension does not pause it either: the wall clock
                // here only feeds durable records, while enforcement uses
                // the monotonic deadline built from this stamp.
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
                if let Some(labels) = event.get("labels").and_then(Value::as_object) {
                    self.labels = labels
                        .iter()
                        .filter_map(|(key, value)| {
                            value.as_str().map(|value| (key.clone(), value.to_string()))
                        })
                        .collect();
                }
            }
            "run_resumed" => {
                self.terminal = None;
            }
            "step_started" => {
                // Budget counters overflow fail closed: saturating would
                // silently undercount durable stats, and wrapping would fork
                // budget enforcement (E13).
                self.budget.steps_executed =
                    self.budget.steps_executed.checked_add(1).ok_or_else(|| {
                        crate::JournalError::InvalidEvent("executed step count overflowed".into())
                    })?;
            }
            "budget_charged" => {
                // Durable budget delta (F13): live and recovery share this
                // counter. Observational `steps_executed` stays separate.
                // A present-but-mistyped amount fails closed instead of
                // silently charging a default: the journal is the budget.
                let amount = match event.get("amount") {
                    None => 1_usize,
                    Some(value) => value
                        .as_u64()
                        .and_then(|n| usize::try_from(n).ok())
                        .ok_or_else(|| {
                            crate::JournalError::InvalidEvent(
                                "budget_charged amount must be a non-negative integer".into(),
                            )
                        })?,
                };
                self.budget.budget_charged = self
                    .budget
                    .budget_charged
                    .checked_add(amount)
                    .ok_or_else(|| {
                        crate::JournalError::InvalidEvent("budget charge count overflowed".into())
                    })?;
                self.budget.has_budget_charges = true;
            }
            "llm_call" => {
                self.budget.llm_calls = self.budget.llm_calls.checked_add(1).ok_or_else(|| {
                    crate::JournalError::InvalidEvent("LLM call count overflowed".into())
                })?;
                if let Some(tokens) = event.get("tokens").and_then(Value::as_object) {
                    // No wire-compat default (Q1): every `llm_call` journaled
                    // by the current version carries all token counters and
                    // `cost_microusd`. An absent counter is a corrupt or
                    // foreign-old event and fails the fold instead of
                    // defaulting to zero; present-but-wrongly-typed counters
                    // fail the same way. Old test journals were converted to
                    // the full form alongside this removal.
                    let counter = |name: &str| -> Result<u64, crate::JournalError> {
                        tokens
                            .get(name)
                            .ok_or_else(|| {
                                crate::JournalError::InvalidEvent(format!(
                                    "llm_call event is missing token count `{name}`"
                                ))
                            })?
                            .as_u64()
                            .ok_or_else(|| {
                                crate::JournalError::InvalidEvent(format!(
                                    "invalid token count `{name}`"
                                ))
                            })
                    };
                    self.budget.tokens_input = self
                        .budget
                        .tokens_input
                        .checked_add(counter("input")?)
                        .ok_or_else(|| {
                            crate::JournalError::InvalidEvent("input token count overflowed".into())
                        })?;
                    self.budget.tokens_output = self
                        .budget
                        .tokens_output
                        .checked_add(counter("output")?)
                        .ok_or_else(|| {
                            crate::JournalError::InvalidEvent(
                                "output token count overflowed".into(),
                            )
                        })?;
                    self.budget.tokens_cached_input = self
                        .budget
                        .tokens_cached_input
                        .checked_add(counter("cached_input")?)
                        .ok_or_else(|| {
                            crate::JournalError::InvalidEvent(
                                "cached input token count overflowed".into(),
                            )
                        })?;
                }
                self.budget.cost_microusd = self
                    .budget
                    .cost_microusd
                    .checked_add(
                        event
                            .get("cost_microusd")
                            .ok_or_else(|| {
                                crate::JournalError::InvalidEvent(
                                    "llm_call event is missing `cost_microusd`".into(),
                                )
                            })?
                            .as_u64()
                            .ok_or_else(|| {
                                crate::JournalError::InvalidEvent("invalid cost counter".into())
                            })?,
                    )
                    .ok_or_else(|| {
                        crate::JournalError::InvalidEvent("cost counter overflowed".into())
                    })?;
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
                self.budget.steps_skipped =
                    self.budget.steps_skipped.checked_add(1).ok_or_else(|| {
                        crate::JournalError::InvalidEvent("skipped step count overflowed".into())
                    })?;
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
                self.budget.repair_attempts =
                    self.budget.repair_attempts.checked_add(1).ok_or_else(|| {
                        crate::JournalError::InvalidEvent("repair attempt count overflowed".into())
                    })?;
            }
            "regenerate_attempt_started" => {
                self.budget.regenerate_attempts = self
                    .budget
                    .regenerate_attempts
                    .checked_add(1)
                    .ok_or_else(|| {
                        crate::JournalError::InvalidEvent(
                            "regenerate attempt count overflowed".into(),
                        )
                    })?;
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
            "hook_failed" => {
                // Warn-policy hook settlement (F07-02): the hook ran and was
                // accepted as a warning, so resume must not re-execute it.
                // Fail-policy hooks return Err immediately and never settle.
                if event.get("policy").and_then(Value::as_str) == Some("warn")
                    && let Some(hook) = event.get("hook").and_then(Value::as_str)
                {
                    self.hooks_settled.insert(hook.to_string());
                }
            }
            "hook_skipped" => {
                // Budget-skipped hooks are settled too: the budget cannot
                // grow on resume, so re-running would skip identically while
                // risking an undeclared duplicate side effect.
                if let Some(hook) = event.get("hook").and_then(Value::as_str) {
                    self.hooks_settled.insert(hook.to_string());
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
                            result_ref: None,
                            // MCP continuation records key outside the
                            // operation-id namespace and carry no invocation.
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
                    // Both fields are required journal identity: a missing
                    // digest or invocation cannot key guard decisions, and
                    // no pre-split journal is honored anymore.
                    let digest = event
                        .get("operation_digest")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            crate::JournalError::InvalidEvent(
                                "operation_started operation_digest is required".into(),
                            )
                        })?
                        .to_string();
                    let invocation = event
                        .get("invocation_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            crate::JournalError::InvalidEvent(
                                "operation_started invocation_id is required".into(),
                            )
                        })?
                        .to_string();
                    let record =
                        self.operation_records
                            .entry(id.to_string())
                            .or_insert_with(|| OperationRecord {
                                digest: digest.clone(),
                                status: OperationStatus::Started,
                                result: None,
                                result_ref: None,
                                invocation: invocation.clone(),
                            });
                    // A restarted attempt reuses its record; only the status
                    // motion matters, never a digest rewrite. Generation
                    // overflow fails closed instead of reusing generations.
                    record.status = OperationStatus::Started;
                    let attempts = self.operation_attempts.entry(id.to_string()).or_default();
                    *attempts = attempts.checked_add(1).ok_or_else(|| {
                        crate::JournalError::InvalidEvent(
                            "operation attempt generation overflowed".into(),
                        )
                    })?;
                }
            }
            "operation_finished" => {
                if let Some(id) = event.get("operation_id").and_then(Value::as_str) {
                    // Current writers emit the split statuses; an unknown
                    // status fails toward explicit handling because unknown
                    // effects must never read as safely retryable.
                    let (status, result, result_ref) = match event
                        .get("status")
                        .and_then(Value::as_str)
                    {
                        Some("success") => (
                            OperationStatus::Succeeded,
                            event.get("result").cloned(),
                            event
                                .get("result_ref")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        ),
                        Some("clean") => (OperationStatus::FailedClean, None, None),
                        Some("indeterminate") => (OperationStatus::FailedIndeterminate, None, None),
                        _ => (OperationStatus::FailedIndeterminate, None, None),
                    };
                    let record =
                        self.operation_records
                            .entry(id.to_string())
                            .or_insert_with(|| OperationRecord {
                                digest: String::new(),
                                status,
                                result: None,
                                result_ref: None,
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
                    // A spilled large result is referenced, never inlined:
                    // the dir-backed guard loads and integrity-checks the
                    // sidecar on resend (E07).
                    record.result_ref = result_ref;
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
        // Every finished revision is retained for immutable-blob
        // verification on resume, independent of which outcome later
        // overwrites the node (E06). Both `files` and `failed_files`
        // are retained: a failed revision's blob must still verify when
        // named, otherwise it orphans and escapes tamper detection (E06).
        // Absent pin lists default to empty as a FOREIGN-journal boundary
        // (E06): journals written before `failed_files` existed omit the key
        // entirely. Only absence defaults; present-but-invalid pins fail the
        // fold above. Journals written by the current version always carry
        // both keys, so this default never masks a current-version write.
        let files: Vec<crate::FilePin> = event
            .get("files")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| {
                crate::JournalError::InvalidEvent(format!("invalid step output file pins: {error}"))
            })?
            .unwrap_or_default();
        let failed_files: Vec<crate::FilePin> = event
            .get("failed_files")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| {
                crate::JournalError::InvalidEvent(format!("invalid step failed file pins: {error}"))
            })?
            .unwrap_or_default();
        for pin in files.iter().chain(failed_files.iter()) {
            self.historical_file_pins
                .entry(pin.path.to_string())
                .or_default()
                .insert(pin.sha256.clone());
        }
        if matches!(
            status,
            "success" | "repaired" | "routed" | "answered_on_fail" | "regenerated"
        ) {
            self.budget.steps_succeeded =
                self.budget.steps_succeeded.checked_add(1).ok_or_else(|| {
                    crate::JournalError::InvalidEvent("succeeded step count overflowed".into())
                })?;
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
            for pin in &files {
                self.latest_file_pins
                    .insert(pin.path.to_string(), pin.sha256.clone());
            }
            self.nodes
                .insert(NodePath::root(path), NodeOutcome::Success { output, files });
            self.checkpoints.remove(&NodePath::root(path));
            self.pending = None;
            self.pending_seq = None;
            self.record_node_execution(path)?;
        } else if !matches!(status, "needs_user" | "needs_confirm") {
            self.budget.steps_failed =
                self.budget.steps_failed.checked_add(1).ok_or_else(|| {
                    crate::JournalError::InvalidEvent("failed step count overflowed".into())
                })?;
            let reason = failure_detail(event, FailureCode::ExecutionFailed, status)?;
            self.vars.set_step_status(path, "failed");
            self.nodes
                .insert(NodePath::root(path), NodeOutcome::Failed { reason });
            self.checkpoints.remove(&NodePath::root(path));
            self.record_node_execution(path)?;
        }
        Ok(())
    }

    /// Counts one finished node execution. Interactions that suspend the
    /// same execution (`needs_user`, `needs_confirm`) do not count, so the
    /// invocation identity survives confirmation and answers (Q1).
    fn record_node_execution(&mut self, path: &str) -> Result<(), crate::JournalError> {
        // Execution-generation overflow fails closed: reusing a generation
        // would alias distinct invocations under one approval scope.
        let executions = self.node_executions.entry(path.to_string()).or_default();
        *executions = executions.checked_add(1).ok_or_else(|| {
            crate::JournalError::InvalidEvent("node execution generation overflowed".into())
        })?;
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
            "operation_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "invocation_id": "invocation",
        });
        if let Some(status) = status {
            event["status"] = json!(status);
        }
        event
    }

    #[test]
    fn latest_pin_follows_journal_order_not_name_order() {
        // E06: `z_first` writes v1 and `a_second` overwrites with v2 for the
        // same path. Dictionary order would put `a_second` first, but the
        // journal order (z then a) must win so resume projects v2.
        let mut state = RunState::default();
        for (seq, node, digest) in [(1, "z_first", "v1"), (2, "a_second", "v2")] {
            state
                .apply(&json!({
                    "t": "step_finished",
                    "seq": seq,
                    "node": node,
                    "status": "success",
                    "files": [{"path": "out.txt", "sha256": digest}],
                }))
                .expect("step finish should fold");
        }
        assert_eq!(
            state.latest_file_pins.get("out.txt").map(String::as_str),
            Some("v2"),
            "the later journal entry must win regardless of node name order"
        );
        assert!(
            state
                .historical_file_pins
                .get("out.txt")
                .is_some_and(|revisions| revisions.contains("v1") && revisions.contains("v2")),
            "both revisions must remain verifiable as blobs"
        );
    }

    #[test]
    fn failed_files_are_retained_as_historical_blobs() {
        // E06: failed revisions must verify as blobs instead of orphaning.
        let mut state = RunState::default();
        state
            .apply(&serde_json::json!({
                "t": "step_finished",
                "seq": 1u64,
                "node": "n",
                "status": "failed",
                "reason": {"code": "execution_failed", "message": "x"},
                "failed_files": [{"path": "out.txt", "sha256": "deadbeef"}],
            }))
            .expect("failed event must fold");
        assert!(
            state
                .historical_file_pins
                .get("out.txt")
                .is_some_and(|s| s.contains("deadbeef")),
            "failed_files must join historical pins"
        );
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
    fn node_executions_count_only_finished_executions() {
        // Q1: retries and crash resumes share one invocation; only a
        // finished execution (success or failure) advances it, so a repair
        // or regenerate re-confirms under `invocation` scope.
        let mut state = RunState::default();
        let step = |seq: u64, status: &str| {
            json!({
                "t": "step_finished",
                "seq": seq,
                "node": "build",
                "status": status,
            })
        };
        state
            .apply(&step(1, "needs_confirm"))
            .expect("suspension should fold");
        assert_eq!(
            state.node_executions.get("build").copied(),
            None,
            "a suspended interaction is still the same invocation"
        );
        state
            .apply(&step(2, "success"))
            .expect("success should fold");
        assert_eq!(state.node_executions.get("build").copied(), Some(1));
        state
            .apply(&step(3, "failed"))
            .expect("failure should fold");
        assert_eq!(state.node_executions.get("build").copied(), Some(2));
        state
            .apply(&step(4, "needs_user"))
            .expect("question should fold");
        assert_eq!(
            state.node_executions.get("build").copied(),
            Some(2),
            "answering a question does not start a new invocation"
        );
    }

    #[test]
    fn unknown_operation_status_folds_indeterminate() {
        // An unknown finish status must never read as safely retryable:
        // unknown effects fold to indeterminate.
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
                "confirm": {"id": "c1", "title": "go", "kind": "k", "target": "t", "dry_run": false, "operation_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "scope": "invocation"},
            }))
            .expect("confirm request should fold");
        assert_eq!(state.pending_seq, Some(4));
        state
            .apply(&json!({"t": "run_canceled", "seq": 5}))
            .expect("cancel should fold");
        assert_eq!(state.pending_seq, None);
    }

    #[test]
    fn persisted_state_requires_every_field() {
        // E07: no `#[serde(default)]` compat shims. A persisted state with
        // a missing field is corruption from an old or partial writer and
        // fails closed instead of silently degrading to zero values.
        let probe: Result<RunState, _> = serde_json::from_value(json!({}));
        assert!(
            probe.is_err(),
            "an empty object must fail closed, got: {:?}",
            probe.map(|state| state.last_seq)
        );
        let full = serde_json::to_value(RunState::default()).expect("default should serialize");
        serde_json::from_value::<RunState>(full.clone()).expect("complete state should parse");
        for field in [
            "schema_version",
            "run_id",
            "contract_sha256",
            "generator_id",
            "last_seq",
            "audit_seq",
            "labels",
            "inputs",
            "vars",
            "nodes",
            "checkpoints",
            "budget",
            "resource_pins",
            "latest_file_pins",
            "node_executions",
            "historical_file_pins",
            "pending",
            "pending_seq",
            "terminal",
            "execution_started",
            "operation_records",
            "operation_attempts",
            "answers",
            "confirmations",
            "cancel_requested",
            "cancel_operations",
            "mcp_pending",
            "mcp_resumed",
        ] {
            let mut partial = full.clone();
            let removed = partial
                .as_object_mut()
                .expect("state should be an object")
                .remove(field);
            assert!(
                removed.is_some(),
                "field `{field}` must exist to be removed"
            );
            if serde_json::from_value::<RunState>(partial).is_ok() {
                panic!("a state without `{field}` must fail closed");
            }
        }
        // Budget counters are required too.
        let mut no_budget = full.clone();
        no_budget
            .as_object_mut()
            .expect("state should be an object")
            .get_mut("budget")
            .expect("budget should exist")
            .as_object_mut()
            .expect("budget should be an object")
            .remove("steps_executed");
        assert!(
            serde_json::from_value::<RunState>(no_budget).is_err(),
            "a budget without `steps_executed` must fail closed"
        );
    }

    #[test]
    fn operation_finished_folds_a_spilled_result_reference() {
        // E07: a large success result folds its `result_ref` sidecar name
        // instead of inline bytes; the dir-backed guard resolves it later.
        let mut state = RunState::default();
        state
            .apply(&json!({
                "t": "operation_started",
                "seq": 1,
                "operation_id": "op-big",
                "operation_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "invocation_id": "call-1",
            }))
            .expect("guard should fold");
        state
            .apply(&json!({
                "t": "operation_finished",
                "seq": 2,
                "operation_id": "op-big",
                "status": "success",
                "result_ref": "abc123.json",
            }))
            .expect("spilled finish should fold");
        let record = state
            .operation_records
            .get("op-big")
            .expect("record should exist");
        assert!(matches!(record.status, OperationStatus::Succeeded));
        assert_eq!(record.result, None);
        assert_eq!(record.result_ref.as_deref(), Some("abc123.json"));
    }

    #[test]
    fn budget_charged_amount_must_typecheck() {
        // Explicit amounts fold; a present-but-mistyped amount fails the
        // fold instead of silently charging a default: the journal is the
        // budget.
        let mut state = RunState::default();
        state
            .apply(&json!({"t": "budget_charged", "seq": 1, "node": "a", "amount": 2}))
            .expect("explicit amount should fold");
        state
            .apply(&json!({"t": "budget_charged", "seq": 2, "node": "a"}))
            .expect("absent amount defaults to one charge");
        assert_eq!(state.budget.budget_charged, 3);
        assert!(state.budget.has_budget_charges);
        let error = state
            .apply(&json!({"t": "budget_charged", "seq": 3, "node": "a", "amount": "many"}))
            .expect_err("mistyped amount must fail closed");
        assert!(
            error.to_string().contains("non-negative integer"),
            "{error}"
        );
        let error = state
            .apply(&json!({"t": "budget_charged", "seq": 4, "node": "a", "amount": -1}))
            .expect_err("negative amount must fail closed");
        assert!(
            error.to_string().contains("non-negative integer"),
            "{error}"
        );
    }
}
