use crate::{JournalLimits, read_journal_values_through, serialize_bounded};
use api::{ConfirmSpec, FormSpec};
use camino::{Utf8Path, Utf8PathBuf};
use contract::ValueBag;
use model::{FailureCode, FailureDetail, NodePath};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const RUN_STATE_SCHEMA_VERSION: u32 = 11;

fn is_known_journal_event_kind(kind: &str) -> bool {
    api::is_known_run_event_kind(kind) || api::INTERNAL_RUN_EVENT_KINDS.contains(&kind)
}

/// Requires Option presence (E07): explicit null is accepted; absence fails closed.
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

impl Interaction {
    fn response_identity(&self) -> (&str, &str, &str) {
        match self {
            Self::Question { question } => ("user_answered", "question_id", &question.id),
            Self::Confirmation { confirm } => ("user_confirmed", "confirmation_id", &confirm.id),
        }
    }
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

// Missing persisted fields are corruption (E07); Option fields distinguish null from absence.

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BudgetState {
    pub elapsed_budget_mode: policy::ElapsedBudgetMode,
    #[serde(deserialize_with = "required_presence")]
    pub elapsed: Option<api::ElapsedAccountedEventData>,
    pub steps_executed: usize,
    /// Durable charge deltas shared by live/recovered retry and foreach accounting.
    pub budget_charged: usize,
    /// Total usage captured by an explicit versioned run-entry baseline.
    #[serde(default)]
    pub step_budget_seed: Option<usize>,
    /// Cumulative charge count at the same baseline; only later deltas are added.
    #[serde(default)]
    pub step_budget_charged_at_seed: Option<usize>,
    /// An old resume/fork boundary may conceal uncharged legacy work until a new baseline.
    #[serde(default)]
    pub legacy_budget_resume_pending: bool,
    /// Legacy retries/foreach executions before a baseline have no exact charge proof.
    #[serde(default)]
    pub legacy_step_budget_unknown: bool,
    /// True when at least one `budget_charged` delta has been recorded.
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
    /// Input tokens used to create cache entries, included in total input.
    #[serde(default)]
    pub tokens_cache_creation_input: u64,
    pub cost_microusd: u64,
    /// At least one hosted search has an unknown/unpriced charge or unsettled liability.
    #[serde(default)]
    pub unresolved_search_cost: bool,
    /// A provider billed a response whose actual charge cannot be represented.
    #[serde(default)]
    pub unresolved_llm_cost: bool,
    #[serde(deserialize_with = "required_presence")]
    pub started_at: Option<String>,
}

impl BudgetState {
    pub(crate) fn step_budget_consumed(&self) -> Result<usize, &'static str> {
        if self.legacy_step_budget_unknown {
            return Err("legacy step-budget history has no exact charge proof");
        }
        let (seed, deltas) = match (self.step_budget_seed, self.step_budget_charged_at_seed) {
            (Some(seed), Some(charged_at_seed)) => (
                seed,
                self.budget_charged
                    .checked_sub(charged_at_seed)
                    .ok_or("budget charge count precedes its step-budget baseline")?,
            ),
            (None, None) if self.has_budget_charges => (0, self.budget_charged),
            (None, None) => (0, self.steps_executed),
            _ => return Err("step-budget baseline is incomplete"),
        };
        seed.checked_add(deltas)
            .ok_or("step-budget consumption overflowed")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    #[serde(skip)]
    pub(crate) dependency_traces: crate::journal::traces::OutcomeTraces,
    pub schema_version: u32,
    #[serde(deserialize_with = "required_presence")]
    pub run_id: Option<String>,
    #[serde(deserialize_with = "required_presence")]
    pub contract_sha256: Option<String>,
    /// Full package identity; absent in legacy/direct state. The service
    /// refuses recovery/fork without it rather than inferring a current pin.
    #[serde(default)]
    pub package_sha256: Option<String>,
    /// Original source path for identity events, not the runtime snapshot root.
    #[serde(default)]
    pub generator_path: Option<String>,
    /// Required owner from journal identity, never parsed from a run id.
    /// An ownerless persisted state is corrupt and fails closed.
    #[serde(deserialize_with = "required_presence")]
    pub generator_id: Option<String>,
    pub last_seq: u64,
    /// Run metadata admitted with the run; metadata only.
    pub labels: BTreeMap<String, String>,
    /// Last audit seq in the shared monotonic space; `last_seq` is durable-only,
    /// keeping resume independent of audit policy while merged readers see one order.
    pub audit_seq: u64,
    /// The canonical inputs recorded by `run_started`; retained for replay.
    #[serde(deserialize_with = "required_presence")]
    pub inputs: Option<BTreeMap<String, Value>>,
    pub vars: ValueBag,
    pub nodes: BTreeMap<NodePath, NodeOutcome>,
    pub checkpoints: BTreeMap<NodePath, Value>,
    /// Unreported join facts by stable owner: cause/seq survive starting or authorizing replay.
    pub(crate) unresolved_tasks: BTreeMap<String, Value>,
    /// Completed reports by foreach iteration; these grant no retry authority.
    pub(crate) foreach_reports: BTreeMap<String, Value>,
    /// Run-wide expected absences, recorded by durable agent file changes.
    pub removed_files: BTreeSet<String>,
    pub budget: BudgetState,
    pub resource_pins: BTreeMap<String, String>,
    /// Journal-latest revision per path; resume checks workspace and immutable history,
    /// allowing later writes but rejecting rollback/tamper (E06).
    pub latest_file_pins: BTreeMap<String, String>,
    /// Finished executions by node: invocation-scoped approvals survive suspension (Q1).
    pub node_executions: BTreeMap<String, u64>,
    /// All revisions for immutable-blob verification (E06); map/set order is not journal order.
    pub historical_file_pins: BTreeMap<String, BTreeSet<String>>,
    #[serde(deserialize_with = "required_presence")]
    pub pending: Option<Interaction>,
    /// Installing seq: answers must match the prompt generation, not only its id (A02).
    #[serde(deserialize_with = "required_presence")]
    pub pending_seq: Option<u64>,
    /// Reported suspensions by owner, independent of the one-at-a-time `pending`.
    pub(crate) pending_interactions: BTreeMap<NodePath, Interaction>,
    /// Installing journal seq by owner; projection never creates a new generation.
    pub(crate) interaction_seqs: BTreeMap<NodePath, u64>,
    #[serde(deserialize_with = "required_presence")]
    pub terminal: Option<TerminalState>,
    pub execution_started: bool,
    /// Journal timestamp of the terminal event, or `None` while the run is
    /// live. Projections report the settled time from here instead of
    /// re-deriving it by scanning lifecycle events.
    #[serde(deserialize_with = "required_presence")]
    pub finished_at: Option<String>,
    /// Admission identity from the last `run_queued`: scheduling priority,
    /// the parent run this one forked, and its durable FIFO key. A
    /// restart reads these from the fold instead of rescanning the journal.
    pub priority: i32,
    pub parent_run_id: Option<String>,
    pub queued_at: Option<String>,
    /// External operations keyed by run/node/invocation, not digest; digest is compared.
    /// Retries reuse identity, but equal content in another invocation does not share it.
    /// `Started` means remote execution may be unknown. `Succeeded` caches small results
    /// inline and large ones by `result_ref`; uncached success refuses replay. `FailedClean`
    /// is retryable; indeterminate failure is refused unless the node opts into repetition.
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
    pub hooks_settled: BTreeSet<String>,
    /// Container effects whose `effect_intent` has no `effect_closed` yet,
    /// keyed by unique instance name with the journaled backend. Each is
    /// reclaimed by name before the run's journal may be deleted.
    #[serde(default)]
    pub effects: BTreeMap<String, Value>,
}

impl Default for RunState {
    fn default() -> Self {
        Self {
            dependency_traces: Default::default(),
            schema_version: RUN_STATE_SCHEMA_VERSION,
            run_id: None,
            contract_sha256: None,
            package_sha256: None,
            generator_path: None,
            generator_id: None,
            last_seq: 0,
            labels: BTreeMap::new(),
            audit_seq: 0,
            inputs: None,
            vars: ValueBag::default(),
            nodes: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            removed_files: BTreeSet::new(),
            budget: BudgetState::default(),
            resource_pins: BTreeMap::new(),
            latest_file_pins: BTreeMap::new(),
            node_executions: BTreeMap::new(),
            historical_file_pins: BTreeMap::new(),
            pending: None,
            pending_seq: None,
            pending_interactions: BTreeMap::new(),
            interaction_seqs: BTreeMap::new(),
            unresolved_tasks: BTreeMap::new(),
            foreach_reports: BTreeMap::new(),
            terminal: None,
            execution_started: false,
            finished_at: None,
            priority: 0,
            parent_run_id: None,
            queued_at: None,
            operation_records: BTreeMap::new(),
            operation_attempts: BTreeMap::new(),
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            cancel_requested: false,
            cancel_operations: BTreeSet::new(),
            mcp_pending: BTreeMap::new(),
            mcp_resumed: BTreeMap::new(),
            hooks_settled: BTreeSet::new(),
            effects: BTreeMap::new(),
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
    /// Effective `on_indeterminate` retry policy journaled with the
    /// `operation_started` event. Absent in pre-policy journals and
    /// deserializes to `Fail` closed.
    #[serde(default)]
    pub on_indeterminate: contract::RetryOnIndeterminate,
}

/// Results larger than this are not cached inline for resends: they spill
/// to a sidecar blob under the run meta dir, and replays load the blob
/// instead of routing to manual recovery or returning truncated results.
pub const OPERATION_RESULT_MAX_BYTES: usize = 64 * 1024;

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
    /// Deletion and retention-GC gate: retains every `Started` operation
    /// regardless of its durable `on_indeterminate` policy. Neither
    /// deletion nor GC is cleanup; they must not erase evidence while the
    /// external outcome is unknown.
    pub fn ensure_no_inflight_operations(&self) -> Result<(), String> {
        if let Some(name) = self.effects.keys().next() {
            return Err(format!(
                "container effect `{name}` is not yet confirmed removed; recovery reclaims it by name before the run may be deleted"
            ));
        }
        if let Some((id, _)) = self
            .operation_records
            .iter()
            .find(|(_, record)| record.status == OperationStatus::Started)
        {
            return Err(format!(
                "unresolved external operation `{id}` remains Started; preserve the run journal and reconcile the external outcome before recovery or deletion; qcg has not proved that the effect stopped"
            ));
        }
        Ok(())
    }

    /// Re-entry and replay gate: refuses a `Started` operation only when
    /// its durable `on_indeterminate` policy is fail-closed (the default
    /// for pre-policy journals). A `Started` record with `repeat` is
    /// allowed through so the existing operation guard can re-execute it
    /// with the acknowledged double-apply risk.
    pub fn ensure_no_fail_closed_inflight_operations(&self) -> Result<(), String> {
        if let Some((id, _)) = self.operation_records.iter().find(|(_, record)| {
            record.status == OperationStatus::Started
                && record.on_indeterminate == contract::RetryOnIndeterminate::Fail
        }) {
            return Err(format!(
                "unresolved external operation `{id}` remains Started; preserve the run journal and reconcile the external outcome before recovery or deletion; qcg has not proved that the effect stopped"
            ));
        }
        Ok(())
    }

    /// An active episode may emit a prompt before its cleanup has finished.
    /// Publish/accept that interaction only once accounting is durably closed.
    /// An open crash anchor must be recovered under the execution lease, not
    /// parked indefinitely as a paused run by a service restart.
    pub fn settled_pending(&self) -> Option<&Interaction> {
        if self.budget.elapsed_budget_mode == policy::ElapsedBudgetMode::ActiveExecution
            && self
                .budget
                .elapsed
                .as_ref()
                .is_none_or(|ledger| ledger.active_started_at.is_some())
        {
            None
        } else {
            self.pending.as_ref()
        }
    }

    /// Full bounded fold for recovery, fork, and resume; capped callers use `fold_journal_with_limits`.
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
        let events = read_journal_values_through(path, through_seq, limits)?.events;
        for event in &events {
            let parsed = api::RunEvent::from_flat(event).map_err(|message| {
                crate::JournalError::InvalidEvent(format!("invalid journal event: {message}"))
            })?;
            if through_seq.is_some_and(|limit| parsed.seq > limit) {
                break;
            }
            state.apply(event)?;
        }
        crate::journal::traces::TraceJournal::restore(&mut state, &events, path, limits);
        Ok(state)
    }

    /// Folds already-read values with the same typed validation as [`Self::fold_journal`].
    pub fn fold_values(events: &[Value]) -> Result<Self, crate::JournalError> {
        let mut state = Self::default();
        for event in events {
            api::RunEvent::from_flat(event).map_err(|message| {
                crate::JournalError::InvalidEvent(format!("invalid journal event: {message}"))
            })?;
            state.apply(event)?;
        }
        Ok(state)
    }

    fn apply_step_budget_baseline(&mut self, event: &Value) -> Result<(), crate::JournalError> {
        let Some(value) = event.get("step_budget_baseline") else {
            return Ok(());
        };
        if value.is_null() {
            return Err(crate::JournalError::InvalidEvent(
                "step-budget baseline cannot be null when present".into(),
            ));
        }
        let invalid = |message: &str| crate::JournalError::InvalidEvent(message.to_string());
        let version = value
            .get("version")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("step-budget baseline version must be an integer"))?;
        if version != u64::from(api::STEP_BUDGET_BASELINE_VERSION) {
            return Err(invalid("unsupported step-budget baseline version"));
        }
        let used = value
            .get("used")
            .and_then(Value::as_u64)
            .and_then(|used| usize::try_from(used).ok())
            .ok_or_else(|| invalid("step-budget baseline usage is not representable"))?;
        let charged = value
            .get("charged")
            .and_then(Value::as_u64)
            .and_then(|charged| usize::try_from(charged).ok())
            .ok_or_else(|| invalid("step-budget baseline charge count is not representable"))?;
        if self.budget.legacy_step_budget_unknown {
            return Err(invalid(
                "cannot establish a step-budget baseline for ambiguous legacy history",
            ));
        }
        if self.budget.step_budget_seed.is_none()
            && self.budget.has_budget_charges
            && !self.budget.legacy_budget_resume_pending
        {
            return Err(invalid(
                "cannot establish a baseline for unanchored step-budget charges",
            ));
        }
        if charged != self.budget.budget_charged {
            return Err(invalid(
                "step-budget baseline charge count does not match the journal",
            ));
        }
        let expected = self.budget.step_budget_consumed().map_err(invalid)?;
        if used != expected {
            return Err(invalid(
                "step-budget baseline usage does not match folded history",
            ));
        }
        self.budget.step_budget_seed = Some(used);
        self.budget.step_budget_charged_at_seed = Some(charged);
        self.budget.legacy_budget_resume_pending = false;
        Ok(())
    }

    fn fold_llm_usage(
        &mut self,
        event: &Value,
        require_usage: bool,
    ) -> Result<(), crate::JournalError> {
        self.budget.llm_calls =
            self.budget.llm_calls.checked_add(1).ok_or_else(|| {
                crate::JournalError::InvalidEvent("LLM call count overflowed".into())
            })?;
        let event_kind = if require_usage {
            "llm_cost_unresolved"
        } else {
            "llm_call"
        };
        let Some(tokens) = event.get("tokens").and_then(Value::as_object) else {
            if require_usage {
                return Err(crate::JournalError::InvalidEvent(
                    "unresolved LLM cost event requires token usage".into(),
                ));
            }
            return Ok(());
        };
        let counter = |name: &str| -> Result<u64, crate::JournalError> {
            tokens
                .get(name)
                .ok_or_else(|| {
                    crate::JournalError::InvalidEvent(format!(
                        "{event_kind} event is missing token count `{name}`"
                    ))
                })?
                .as_u64()
                .ok_or_else(|| {
                    crate::JournalError::InvalidEvent(format!(
                        "invalid token count `{name}` in {event_kind}"
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
                crate::JournalError::InvalidEvent("output token count overflowed".into())
            })?;
        self.budget.tokens_cached_input = self
            .budget
            .tokens_cached_input
            .checked_add(counter("cached_input")?)
            .ok_or_else(|| {
                crate::JournalError::InvalidEvent("cached input token count overflowed".into())
            })?;
        self.budget.tokens_cache_creation_input = self
            .budget
            .tokens_cache_creation_input
            .checked_add(if tokens.contains_key("cache_creation_input") {
                counter("cache_creation_input")?
            } else {
                0
            })
            .ok_or_else(|| {
                crate::JournalError::InvalidEvent(
                    "cache creation input token count overflowed".into(),
                )
            })?;
        Ok(())
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
        // Only admission records rebind identity; forks must name their source.
        // Every other event must match folded identity or the journal is foreign or spliced.
        if !matches!(kind, "run_queued" | "run_started" | "run_forked")
            && let Some(expected) = self.run_id.as_deref()
            && let Some(actual) = event.get("run_id").and_then(Value::as_str)
            && actual != expected
        {
            return Err(crate::JournalError::InvalidEvent(format!(
                "run event belongs to `{actual}`, expected `{expected}`"
            )));
        }
        if !is_known_journal_event_kind(kind) {
            return Err(crate::JournalError::InvalidEvent(format!(
                "unknown run event kind `{kind}`"
            )));
        }
        // Final runs accept only explicit lifecycle restarts. `run_interrupted` is
        // nonterminal and HITL resume appends after it; any other post-final event is invalid,
        // except `effect_closed`: a late cleanup or recovery may still confirm an instance gone.
        if self.terminal.as_ref().is_some_and(|terminal| {
            matches!(
                terminal,
                TerminalState::Succeeded | TerminalState::Failed | TerminalState::Canceled
            )
        }) && !matches!(
            kind,
            "run_queued" | "run_started" | "run_resumed" | "effect_closed"
        ) {
            return Err(crate::JournalError::InvalidEvent(format!(
                "final run cannot accept `{kind}`"
            )));
        }
        // Identity records declare the schema; schema 1 alone omits wall-clock mode.
        if matches!(kind, "run_queued" | "run_started")
            && let Some(version) = event.get("schema_version").and_then(Value::as_u64)
            && !matches!(version, 1 | 2)
            && version != u64::from(api::JOURNAL_SCHEMA_VERSION)
        {
            return Err(crate::JournalError::InvalidEvent(format!(
                "unsupported journal schema_version {version}; this runtime implements {}",
                api::JOURNAL_SCHEMA_VERSION
            )));
        }
        if matches!(kind, "llm_call" | "llm_route_failed") {
            self.budget.unresolved_search_cost |= event
                .get("web_search")
                .filter(|search| search.is_object())
                .is_some_and(|search| {
                    search
                        .get("cost_microusd")
                        .and_then(Value::as_u64)
                        .is_none()
                });
        }
        match kind {
            "run_queued" | "run_started" => {
                // Only schema 1 may omit the mode: its historical meaning is WallClock.
                let version = event
                    .get("schema_version")
                    .and_then(Value::as_u64)
                    .unwrap_or(1);
                let mode = match event.get("elapsed_budget_mode") {
                    Some(value) => {
                        serde_json::from_value::<policy::ElapsedBudgetMode>(value.clone())?
                    }
                    None if version == 1 => policy::ElapsedBudgetMode::WallClock,
                    None => {
                        return Err(crate::JournalError::InvalidEvent(
                            "missing elapsed_budget_mode".into(),
                        ));
                    }
                };
                if version == 1 && mode != policy::ElapsedBudgetMode::WallClock {
                    return Err(crate::JournalError::InvalidEvent(
                        "schema 1 has only wall-clock accounting".into(),
                    ));
                }
                if self.budget.started_at.is_some() && self.budget.elapsed_budget_mode != mode {
                    return Err(crate::JournalError::InvalidEvent(
                        "elapsed budget mode changed within lineage".into(),
                    ));
                }
                // The first identity is also the atomic zero/closed ledger. Never
                // reconstruct a missing ledger once identity/execution exists:
                // a requeue or fork must preserve the inherited consumption.
                if mode == policy::ElapsedBudgetMode::ActiveExecution
                    && self.budget.elapsed.is_none()
                {
                    if self.contract_sha256.is_some()
                        || self.budget.started_at.is_some()
                        || self.execution_started
                    {
                        return Err(crate::JournalError::InvalidEvent(
                            "missing active ledger after identity".into(),
                        ));
                    }
                    self.budget.elapsed = Some(api::ElapsedAccountedEventData {
                        active_nanoseconds: 0,
                        active_started_at: None,
                    });
                }
                self.budget.elapsed_budget_mode = mode;
                // A different run id in the same journal is only a fork, and
                // only when the admission names the run it forked from.
                if let (Some(previous), Some(next)) = (
                    self.run_id.as_deref(),
                    event.get("run_id").and_then(Value::as_str),
                ) && previous != next
                    && event.get("parent_run_id").and_then(Value::as_str) != Some(previous)
                {
                    return Err(crate::JournalError::InvalidEvent(format!(
                        "admission for `{next}` does not name `{previous}` as its parent"
                    )));
                }
                self.run_id = event
                    .get("run_id")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let package = event.get("package_sha256").and_then(Value::as_str);
                if self.package_sha256.is_some() && self.package_sha256.as_deref() != package {
                    return Err(crate::JournalError::InvalidEvent(
                        "generator package identity changed within lineage".into(),
                    ));
                }
                self.package_sha256 = package.map(str::to_string);
                self.generator_path = event
                    .get("generator_path")
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
                // Wall-clock elapsed start (E11): the first queued or started
                // event stamps this clock. WallClock mode charges queue and
                // HITL wait time; ActiveExecution mode instead enforces from
                // its durable episode ledger. Later identity events never
                // move this stamp, and WallClock enforcement uses a monotonic
                // deadline seeded from it.
                if self.budget.started_at.is_none() {
                    self.budget.started_at =
                        event.get("ts").and_then(Value::as_str).map(str::to_string);
                }
                // Pre-provided answers and confirmations ride on run_queued
                // for unattended runs; later events win on the same key.
                if kind == "run_queued" {
                    if let Some(priority) = event.get("priority").and_then(Value::as_i64) {
                        self.priority =
                            i32::try_from(priority.clamp(i64::from(i32::MIN), i64::from(i32::MAX)))
                                .unwrap_or_default();
                    }
                    self.parent_run_id = event
                        .get("parent_run_id")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    if event.get("step_budget_baseline").is_some() {
                        return Err(crate::JournalError::InvalidEvent(
                            "run_queued cannot establish a step-budget baseline".into(),
                        ));
                    }
                    if self.parent_run_id.is_some() && self.budget.step_budget_seed.is_none() {
                        if self.budget.legacy_budget_resume_pending
                            && self.budget.has_budget_charges
                        {
                            self.budget.legacy_step_budget_unknown = true;
                        }
                        self.budget.legacy_budget_resume_pending = true;
                    }
                    // Requeue order survives restarts. Preemption may
                    // carry a preserved `queued_at` key; otherwise the actual
                    // event timestamp becomes the new key (C04).
                    self.queued_at = event
                        .get("queued_at")
                        .and_then(Value::as_str)
                        .or_else(|| event.get("ts").and_then(Value::as_str))
                        .map(str::to_string);
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
                    self.apply_step_budget_baseline(event)?;
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
                let has_baseline = event.get("step_budget_baseline").is_some();
                if self.budget.step_budget_seed.is_none()
                    && self.budget.has_budget_charges
                    && !self.budget.legacy_budget_resume_pending
                {
                    self.budget.legacy_step_budget_unknown = true;
                }
                if has_baseline {
                    self.apply_step_budget_baseline(event)?;
                } else if self.budget.step_budget_seed.is_none() {
                    self.budget.legacy_budget_resume_pending = true;
                }
            }
            "step_retry" | "foreach_iteration" => {
                if self.budget.step_budget_seed.is_none() {
                    self.budget.legacy_step_budget_unknown = true;
                }
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
            "elapsed_accounted" => {
                if self.budget.elapsed_budget_mode != policy::ElapsedBudgetMode::ActiveExecution
                    || self.run_id.is_none()
                {
                    return Err(crate::JournalError::InvalidEvent(
                        "active ledger without active identity".into(),
                    ));
                }
                let next: api::ElapsedAccountedEventData = serde_json::from_value(
                    serde_json::json!({
                        "active_nanoseconds": event.get("active_nanoseconds").ok_or_else(|| crate::JournalError::InvalidEvent("missing active total".into()))?,
                        "active_started_at": event.get("active_started_at").ok_or_else(|| crate::JournalError::InvalidEvent("missing active anchor".into()))?,
                    }),
                )?;
                if let Some(anchor) = &next.active_started_at {
                    chrono::DateTime::parse_from_rfc3339(anchor).map_err(|_| {
                        crate::JournalError::InvalidEvent("invalid active anchor".into())
                    })?;
                }
                let legal = match &self.budget.elapsed {
                    None => false,
                    Some(previous) => {
                        match (&previous.active_started_at, &next.active_started_at) {
                            (None, Some(_)) => {
                                next.active_nanoseconds == previous.active_nanoseconds
                            }
                            (Some(_), None) => {
                                next.active_nanoseconds >= previous.active_nanoseconds
                            }
                            _ => false,
                        }
                    }
                };
                if !legal {
                    return Err(crate::JournalError::InvalidEvent(
                        "illegal active ledger transition".into(),
                    ));
                }
                self.budget.elapsed = Some(next);
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
                if amount > 0
                    && self.budget.legacy_budget_resume_pending
                    && self.budget.step_budget_seed.is_none()
                {
                    self.budget.legacy_step_budget_unknown = true;
                }
                self.budget.budget_charged = self
                    .budget
                    .budget_charged
                    .checked_add(amount)
                    .ok_or_else(|| {
                        crate::JournalError::InvalidEvent("budget charge count overflowed".into())
                    })?;
                self.budget.has_budget_charges = self.budget.budget_charged > 0;
            }
            "llm_call" => {
                self.fold_llm_usage(event, false)?;
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
            "llm_cost_unresolved" => {
                for field in ["node", "provider", "model"] {
                    if event
                        .get(field)
                        .and_then(Value::as_str)
                        .is_none_or(str::is_empty)
                    {
                        return Err(crate::JournalError::InvalidEvent(format!(
                            "unresolved LLM cost event requires `{field}`"
                        )));
                    }
                }
                if event.get("liability").and_then(Value::as_str)
                    != Some("actual_cost_unrepresentable")
                {
                    return Err(crate::JournalError::InvalidEvent(
                        "unresolved LLM cost event has an invalid liability marker".into(),
                    ));
                }
                self.fold_llm_usage(event, true)?;
                self.budget.unresolved_llm_cost = true;
            }
            "agent_file_changes" => {
                let parsed =
                    api::RunEvent::from_flat(event).map_err(crate::JournalError::InvalidEvent)?;
                parsed.path.ok_or_else(|| {
                    crate::JournalError::InvalidEvent("agent file changes require a node".into())
                })?;
                let api::RunEventData::AgentFileChanges(changes) = parsed.data else {
                    return Err(crate::JournalError::InvalidEvent(
                        "invalid agent file changes".into(),
                    ));
                };
                if changes
                    .removed_files
                    .iter()
                    .chain(&changes.written_files)
                    .any(|path| !policy::is_safe_relative_path(path))
                {
                    return Err(crate::JournalError::InvalidEvent(
                        "unsafe agent file-change path".into(),
                    ));
                }
                for path in changes.removed_files {
                    self.latest_file_pins.remove(&path);
                    self.removed_files.insert(path);
                }
                for path in changes.written_files {
                    self.removed_files.remove(&path);
                }
            }
            "agent_checkpoint" => {
                if let (Some(path), Some(checkpoint)) = (
                    event.get("node").and_then(Value::as_str),
                    event.get("checkpoint").cloned(),
                ) {
                    // Checkpoints carry continuation data, not workspace truth.
                    // Only file-change events and new pins supersede removals.
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
            "parallel_task_joined" | "foreach_sibling_drained" | "foreach_iteration_settled" => {
                let class = event["classification"].as_str().unwrap_or("");
                let unreported = !matches!(
                    class,
                    "completed_success"
                        | "completed_terminal"
                        | "interaction_pending"
                        | "task_error"
                        | "agent_continuation"
                );
                if !unreported && kind != "parallel_task_joined" {
                    let owner = event["iteration"].as_str().ok_or_else(|| {
                        crate::JournalError::InvalidEvent("foreach iteration is required".into())
                    })?;
                    let mut fact = event.clone();
                    for key in ["t", "ts", "run_id", "trace_id", "span_id", "parent_span_id"] {
                        fact.as_object_mut().expect("journal object").remove(key);
                    }
                    self.foreach_reports.insert(owner.to_string(), fact);
                    let resumable = matches!(event["outcome"].as_str(), Some("needs_user") if event["question_id"].is_string()) // legacy terminal without reusable `result` refuses replay
                        || matches!(event["outcome"].as_str(), Some("needs_confirm") if event["confirm_id"].is_string());
                    if event.get("result").is_none()
                        && (class == "task_error" || class == "completed_terminal" && !resumable)
                    {
                        self.unresolved_tasks
                            .insert(owner.to_string(), event.clone());
                    }
                }
                if unreported {
                    let owner = if kind != "parallel_task_joined" {
                        &event["iteration"]
                    } else {
                        &event["node"]
                    };
                    let owner = owner
                        .as_str()
                        .filter(|_| event["node"].is_string())
                        .ok_or_else(|| {
                            crate::JournalError::InvalidEvent(
                                "unreported task owner is required".into(),
                            )
                        })?;
                    self.unresolved_tasks
                        .insert(owner.to_string(), event.clone());
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
                    self.retire_interaction(path);
                    let serialized_reason = failure_value(&reason)?;
                    self.vars
                        .set_step_outcome(path, "skipped", Some(serialized_reason));
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
            "confirm_request" | "run_waiting" => {
                // A confirmation's pins fold like a suspended step's.
                if kind == "confirm_request"
                    && event.get("files").is_some()
                    && event.get("node").is_some()
                {
                    let mut suspended_step = event.clone();
                    suspended_step["status"] = Value::String("needs_confirm".into());
                    self.apply_step_finished(&suspended_step)?;
                }
                self.install_interaction(event, kind == "confirm_request")?;
            }
            "user_answered" | "user_confirmed" => {
                self.queued_at = event
                    .get("queued_at")
                    .and_then(Value::as_str)
                    .or_else(|| event.get("ts").and_then(Value::as_str))
                    .map(str::to_string);
                // Accepted responses remove only their own typed interaction.
                let id_key = if kind == "user_answered" {
                    "question_id"
                } else {
                    "confirmation_id"
                };
                let id = event.get(id_key).and_then(Value::as_str).ok_or_else(|| {
                    crate::JournalError::InvalidEvent(format!("{kind} {id_key} is required"))
                })?;
                if kind == "user_answered" {
                    let values = event.get("values").cloned().ok_or_else(|| {
                        crate::JournalError::InvalidEvent(
                            "user_answered values are required".into(),
                        )
                    })?;
                    self.answers.insert(id.to_string(), values);
                } else {
                    let approved =
                        event
                            .get("approved")
                            .and_then(Value::as_bool)
                            .ok_or_else(|| {
                                crate::JournalError::InvalidEvent(
                                    "user_confirmed approved must be boolean".into(),
                                )
                            })?;
                    self.confirmations.insert(id.to_string(), approved);
                }
                let matches = |interaction: &Interaction| {
                    let (expected, key, id) = interaction.response_identity();
                    kind == expected && event.get(key).and_then(Value::as_str) == Some(id)
                };
                self.pending_interactions
                    .retain(|_, interaction| !matches(interaction));
                self.interaction_seqs
                    .retain(|owner, _| self.pending_interactions.contains_key(owner));
                if self.pending.as_ref().is_some_and(matches) {
                    self.project_interaction();
                }
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
                if let Some(hook) = event.get("hook").and_then(Value::as_str) {
                    // A check-failed hook already has a structured outcome
                    // from its `step_finished` event. Execution errors only
                    // have `hook_failed`, so project that completed failure
                    // here without replacing a more specific existing reason.
                    let status_path = format!("steps.{hook}.status");
                    if self.vars.get_path(&status_path).is_none() {
                        let message = event
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or("hook execution failed");
                        let reason = FailureDetail::new(FailureCode::ExecutionFailed, message);
                        let serialized_reason = failure_value(&reason)?;
                        self.vars
                            .set_step_outcome(hook, "failed", Some(serialized_reason));
                    }
                    // Warn-policy hook settlement (F07-02): the hook ran and
                    // was accepted as a warning, so resume must not re-execute.
                    // Fail-policy hooks return Err immediately and never settle.
                    if event.get("policy").and_then(Value::as_str) == Some("warn") {
                        self.hooks_settled.insert(hook.to_string());
                    }
                }
            }
            "hook_skipped" => {
                // Budget-skipped hooks are settled too: the budget cannot
                // grow on resume, so re-running would skip identically while
                // risking an undeclared duplicate side effect.
                if let Some(hook) = event.get("hook").and_then(Value::as_str) {
                    let message = event
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("hook skipped because the run budget was exhausted");
                    let reason = FailureDetail::new(FailureCode::BudgetExceeded, message);
                    let serialized_reason = failure_value(&reason)?;
                    self.vars
                        .set_step_outcome(hook, "skipped", Some(serialized_reason));
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
                            on_indeterminate: contract::RetryOnIndeterminate::Fail,
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
            "run_finished" | "run_error" | "run_canceled" | "run_interrupted" => {
                self.finished_at = event.get("ts").and_then(Value::as_str).map(str::to_string);
                self.pending = None;
                self.pending_seq = None;
                self.terminal = Some(match (kind, event.get("status").and_then(Value::as_str)) {
                    ("run_finished", Some("success")) => TerminalState::Succeeded,
                    ("run_canceled", _) => TerminalState::Canceled,
                    ("run_interrupted", _) => TerminalState::Interrupted,
                    _ => TerminalState::Failed,
                });
            }
            "run_forked" => {
                // The fork marker rebinds the copied journal to the fork's
                // identity; everything after it must carry that run id.
                self.run_id = event
                    .get("run_id")
                    .and_then(Value::as_str)
                    .map(str::to_string);
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
                    // Durable retry policy for the re-entry gate: current
                    // writers journal `on_indeterminate` (`fail`/`repeat`);
                    // absent or unknown values default to fail-closed.
                    let on_indeterminate =
                        match event.get("on_indeterminate").and_then(Value::as_str) {
                            Some("repeat") => contract::RetryOnIndeterminate::Repeat,
                            _ => contract::RetryOnIndeterminate::Fail,
                        };
                    let record =
                        self.operation_records
                            .entry(id.to_string())
                            .or_insert_with(|| OperationRecord {
                                digest: digest.clone(),
                                status: OperationStatus::Started,
                                result: None,
                                result_ref: None,
                                invocation: invocation.clone(),
                                on_indeterminate,
                            });
                    // A restarted attempt reuses its record; only the status
                    // motion matters, never a digest rewrite. Generation
                    // overflow fails closed instead of reusing generations.
                    record.status = OperationStatus::Started;
                    record.on_indeterminate = on_indeterminate;
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
                                on_indeterminate: contract::RetryOnIndeterminate::Fail,
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
            "effect_intent" => {
                if let (Some(name), Some(backend)) = (
                    event.get("effect").and_then(Value::as_str),
                    event.get("backend"),
                ) {
                    self.effects.insert(name.to_string(), backend.clone());
                }
            }
            "effect_closed" => {
                if let Some(name) = event.get("effect").and_then(Value::as_str) {
                    self.effects.remove(name);
                }
            }
            kind if api::INTERNAL_RUN_EVENT_KINDS.contains(&kind) => {
                // Internal diagnostics have no folded state unless an earlier arm consumes them.
            }
            _ => {}
        }
        if let Some(owner) = crate::journal::traces::trace_owner(event) {
            self.dependency_traces.remove(&owner);
        }
        Ok(())
    }

    /// Terminal reports retire only their owner's prompt, never a sibling's projection.
    fn retire_interaction(&mut self, path: &str) {
        if let Some(retired) = self.pending_interactions.remove(&NodePath::root(path))
            && self
                .pending
                .as_ref()
                .is_some_and(|pending| pending.response_identity() == retired.response_identity())
        {
            self.project_interaction();
        }
        self.interaction_seqs.remove(&NodePath::root(path));
    }

    /// Requests and suspended reports install generations; observation only projects them.
    fn install_interaction(
        &mut self,
        event: &Value,
        confirmation: bool,
    ) -> Result<(), crate::JournalError> {
        let (key, kind) = if confirmation {
            ("confirm", "confirmation")
        } else {
            ("question", "question")
        };
        let Some(payload) = event.get(key) else {
            return Ok(());
        };
        let interaction: Interaction = serde_json::from_value(
            serde_json::json!({"kind": kind, (key): payload}),
        )
        .map_err(|error| {
            crate::JournalError::InvalidEvent(format!("invalid saved interaction: {error}"))
        })?;
        let owner = event
            .get("node")
            .and_then(Value::as_str)
            .map(NodePath::root)
            .or_else(|| {
                self.pending_interactions
                    .iter()
                    .find(|(_, saved)| saved.response_identity() == interaction.response_identity())
                    .map(|(owner, _)| owner.clone())
            });
        let seq = event.get("seq").and_then(Value::as_u64);
        if let Some(owner) = owner {
            self.interaction_seqs.remove(&owner);
            if let Some(seq) = seq {
                self.interaction_seqs.insert(owner.clone(), seq);
            }
            self.pending_interactions.insert(owner, interaction);
            self.project_interaction();
        } else {
            // Ownerless legacy requests still carry only their recorded generation.
            self.pending = Some(interaction);
            self.pending_seq = seq;
        }
        Ok(())
    }

    fn project_interaction(&mut self) {
        let next = self.pending_interactions.first_key_value();
        self.pending = next.map(|(_, prompt)| prompt.clone());
        self.pending_seq = next.and_then(|(owner, _)| self.interaction_seqs.get(owner).copied());
    }

    fn apply_step_finished(&mut self, event: &Value) -> Result<(), crate::JournalError> {
        let path = event.get("node").and_then(Value::as_str).ok_or_else(|| {
            crate::JournalError::InvalidEvent("step_finished node is required".into())
        })?;
        let status = event.get("status").and_then(Value::as_str).ok_or_else(|| {
            crate::JournalError::InvalidEvent("step_finished status is required".into())
        })?;
        // Retain every revision for resume verification; absent lists default empty.
        let pins = |key: &str| -> Result<Vec<crate::FilePin>, crate::JournalError> {
            event
                .get(key)
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| {
                    crate::JournalError::InvalidEvent(format!("invalid {key} pins: {error}"))
                })
                .map(Option::unwrap_or_default)
        };
        let files = pins("files")?;
        let failed_files = pins("failed_files")?;
        for pin in files.iter().chain(failed_files.iter()) {
            self.historical_file_pins
                .entry(pin.path.to_string())
                .or_default()
                .insert(pin.sha256.clone());
        }
        let succeeded = matches!(
            status,
            "success" | "repaired" | "routed" | "answered_on_fail" | "regenerated"
        );
        let suspended = matches!(status, "needs_user" | "needs_confirm");
        // A foreach suspension/failure does not settle its aborted iterations.
        // Only completion of the parent, or a later report by the exact owner, does.
        self.unresolved_tasks.retain(|owner, fact| {
            owner != path && !(succeeded && fact["node"].as_str() == Some(path))
        });
        if succeeded || suspended {
            self.apply_workspace_file_pins(&files);
        }
        if suspended {
            self.install_interaction(event, status == "needs_confirm")?;
        }
        if succeeded {
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
            self.vars.set_step_outcome(path, "success", None);
            self.nodes
                .insert(NodePath::root(path), NodeOutcome::Success { output, files });
        } else if !suspended {
            self.budget.steps_failed =
                self.budget.steps_failed.checked_add(1).ok_or_else(|| {
                    crate::JournalError::InvalidEvent("failed step count overflowed".into())
                })?;
            let reason = failure_detail(event, FailureCode::ExecutionFailed, status)?;
            let serialized_reason = failure_value(&reason)?;
            self.vars
                .set_step_outcome(path, "failed", Some(serialized_reason));
            self.nodes
                .insert(NodePath::root(path), NodeOutcome::Failed { reason });
        }
        if !suspended {
            self.checkpoints.remove(&NodePath::root(path));
            self.retire_interaction(path);
            self.record_node_execution(path)?;
        }
        Ok(())
    }

    /// Makes the attempt's revisions the workspace's latest ones.
    fn apply_workspace_file_pins(&mut self, files: &[crate::FilePin]) {
        for pin in files {
            self.latest_file_pins
                .insert(pin.path.to_string(), pin.sha256.clone());
            self.removed_files.remove(pin.path.as_str());
        }
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
        // The shared sidecar publisher: owner-only staging, refusal of a
        // planted symlink at the destination, atomic publish, and a parent
        // directory sync. The previous hand-rolled `state.json.tmp` sequence
        // followed symlinks and left the temp name behind on failure.
        files::write_sidecar_atomic(path, bytes)?;
        Ok(())
    }
}

fn failure_value(reason: &FailureDetail) -> Result<Value, crate::JournalError> {
    serde_json::to_value(reason).map_err(|error| {
        crate::JournalError::InvalidEvent(format!("invalid structured failure detail: {error}"))
    })
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

    /// State-free diagnostics; fold tests ensure API internal-event registration.
    const STATE_FREE_JOURNAL_EVENT_KINDS: &[&str] = &[
        "audit_degraded",
        "elapsed_exceeded",
        "foreach_sibling_ignored",
        "hook_replayed",
        "mcp_destination_decision",
        "operation_repeated",
        "parallel_replay_authorized",
        "step_interrupted",
        "step_timeout",
    ];

    #[test]
    fn internal_journal_kinds_pass_the_fold_gate() {
        for kind in STATE_FREE_JOURNAL_EVENT_KINDS {
            assert!(api::INTERNAL_RUN_EVENT_KINDS.contains(kind), "{kind}");
            let mut state = RunState::default();
            let before = serde_json::to_value(&state).unwrap();
            state
                .apply(&json!({"t": kind}))
                .expect("known state-free kind");
            assert_eq!(serde_json::to_value(state).unwrap(), before, "{kind}");
        }
    }

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
    fn package_identity_cannot_change_or_disappear_after_admission() {
        let identity = |seq, kind, package| {
            json!({
                "seq": seq, "t": kind, "run_id": "run", "generator": "test@1",
                "generator_path": "installed/test", "contract_sha256": "manifest",
                "package_sha256": package, "inputs": {}, "schema_version": 2,
                "elapsed_budget_mode": "wall_clock",
            })
        };
        let mut state = RunState::default();
        state
            .apply(&identity(1, "run_queued", Some("package-a")))
            .unwrap();
        state
            .apply(&identity(2, "run_started", Some("package-a")))
            .unwrap();
        assert_eq!(state.package_sha256.as_deref(), Some("package-a"));
        assert_eq!(state.generator_path.as_deref(), Some("installed/test"));
        for package in [Some("package-b"), None] {
            assert!(
                state
                    .clone()
                    .apply(&identity(3, "run_queued", package))
                    .is_err()
            );
        }
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
    fn suspended_step_files_are_historical_and_latest_workspace_pins() {
        for (seq, status, digest) in [
            (1, "needs_user", "question-revision"),
            (2, "needs_confirm", "confirm-revision"),
        ] {
            let mut state = RunState::default();
            state
                .apply(&json!({
                    "t": "step_finished",
                    "seq": seq,
                    "node": "suspended",
                    "status": status,
                    "files": [{"path": "before-prompt.txt", "sha256": digest}],
                }))
                .expect("suspended step should fold");
            assert_eq!(
                state
                    .latest_file_pins
                    .get("before-prompt.txt")
                    .map(String::as_str),
                Some(digest),
                "suspended attempt must become the workspace's latest revision"
            );
            assert!(state.historical_file_pins["before-prompt.txt"].contains(digest));
            assert_eq!(state.budget.steps_succeeded, 0);
            assert_eq!(state.budget.steps_failed, 0);
            assert!(!state.node_executions.contains_key("suspended"));
        }
    }

    #[test]
    fn confirmation_file_pins_fold_into_workspace_and_history() {
        let mut state = RunState::default();
        state
            .apply(&json!({
                "t": "confirm_request",
                "seq": 7,
                "node": "publish",
                "confirm": {
                    "id": "c1",
                    "title": "publish",
                    "kind": "publish",
                    "target": "artifact",
                    "dry_run": false,
                    "operation_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "scope": "invocation"
                },
                "files": [{"path": "artifact.txt", "sha256": "before-confirmation"}]
            }))
            .expect("confirmation and file pins should fold");
        assert_eq!(state.pending_seq, Some(7));
        assert_eq!(
            state
                .latest_file_pins
                .get("artifact.txt")
                .map(String::as_str),
            Some("before-confirmation")
        );
        assert!(state.historical_file_pins["artifact.txt"].contains("before-confirmation"));
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
    fn agent_file_changes_fold_removals_and_later_writes_before_resume() {
        let mut state = RunState::default();
        let change = |seq, removed_files: Vec<&str>, written_files: Vec<&str>| {
            json!({
                "t": "agent_file_changes", "seq": seq, "ts": "2026-07-05T00:00:00Z",
                "run_id": "run-1", "trace_id": "trace-1", "span_id": format!("span-{seq}"),
                "node": "agent", "removed_files": removed_files, "written_files": written_files
            })
        };
        state
            .apply(&json!({
                "t": "agent_checkpoint", "seq": 1, "node": "agent",
                "checkpoint": {"next_turn": 2, "removed_files": []}
            }))
            .expect("checkpoint should fold");
        state
            .apply(&change(2, vec!["out/note.txt"], vec![]))
            .expect("removal should fold before another checkpoint");
        assert_eq!(state.removed_files, BTreeSet::from(["out/note.txt".into()]));
        assert_eq!(
            state.checkpoints[&NodePath::root("agent")]["removed_files"],
            json!([])
        );
        state
            .apply(&change(3, vec![], vec!["out/note.txt"]))
            .expect("later write should supersede removal");
        assert!(state.removed_files.is_empty());
        assert_eq!(
            state.checkpoints[&NodePath::root("agent")]["removed_files"],
            json!([])
        );
    }

    #[test]
    fn agent_file_changes_reject_unsafe_paths() {
        let error = RunState::default()
            .apply(&json!({
                "t": "agent_file_changes", "seq": 1, "ts": "2026-07-05T00:00:00Z",
                "run_id": "run-1", "trace_id": "trace-1", "span_id": "span-1", "node": "agent",
                "removed_files": ["../outside"], "written_files": []
            }))
            .expect_err("unsafe file-change paths must fail closed");
        assert!(error.to_string().contains("unsafe agent file-change path"));
    }

    #[test]
    fn run_wide_removals_retract_pins_and_only_writes_supersede_them() {
        let mut state = RunState::default();
        state
            .apply(&json!({
                "t": "step_finished", "seq": 1, "node": "producer", "status": "success",
                "files": [{"path": "out/note.txt", "sha256": "abc"}], "failed_files": []
            }))
            .unwrap();
        state
            .apply(&json!({
                "t": "agent_file_changes", "seq": 2, "ts": "2026-07-05T00:00:00Z",
                "run_id": "run-1", "trace_id": "trace-1", "span_id": "span-2", "node": "agent",
                "removed_files": ["out/note.txt"], "written_files": []
            }))
            .unwrap();
        assert_eq!(state.removed_files, BTreeSet::from(["out/note.txt".into()]));
        assert!(!state.latest_file_pins.contains_key("out/note.txt"));
        assert!(state.historical_file_pins["out/note.txt"].contains("abc"));
        // A stale checkpoint from any node cannot undo workspace facts.
        for (seq, node, removed) in [
            (3, "agent", json!([])),
            (4, "other", json!(["out/stale.txt"])),
        ] {
            state
                .apply(&json!({
                    "t": "agent_checkpoint", "seq": seq, "node": node,
                    "checkpoint": {"next_turn": 2, "removed_files": removed}
                }))
                .unwrap();
        }
        assert_eq!(state.removed_files, BTreeSet::from(["out/note.txt".into()]));
        state
            .apply(&json!({
                "t": "step_finished", "seq": 5, "node": "another_writer", "status": "success",
                "files": [{"path": "out/note.txt", "sha256": "def"}], "failed_files": []
            }))
            .unwrap();
        assert!(state.removed_files.is_empty());
        assert_eq!(state.latest_file_pins["out/note.txt"], "def");
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
            "removed_files",
            "budget",
            "resource_pins",
            "latest_file_pins",
            "node_executions",
            "historical_file_pins",
            "pending",
            "pending_seq",
            "pending_interactions",
            "interaction_seqs",
            "unresolved_tasks",
            "foreach_reports",
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
        let mut zero = RunState::default();
        zero.apply(&json!({"t": "budget_charged", "seq": 1, "node": "a", "amount": 0}))
            .expect("a zero delta is a no-op");
        assert_eq!(zero.budget.budget_charged, 0);
        assert!(!zero.budget.has_budget_charges);
    }

    fn step_budget_boundary(seq: u64, kind: &str, run_id: &str, used: u64, charged: u64) -> Value {
        let mut event = json!({
            "t": kind,
            "seq": seq,
            "run_id": run_id,
            "step_budget_baseline": {
                "version": api::STEP_BUDGET_BASELINE_VERSION,
                "used": used,
                "charged": charged
            }
        });
        if kind == "run_started" {
            event["schema_version"] = json!(api::JOURNAL_SCHEMA_VERSION);
            event["elapsed_budget_mode"] = json!(policy::ElapsedBudgetMode::default());
        }
        event
    }

    #[test]
    fn legacy_resume_markers_do_not_freeze_usage_and_new_baselines_track_deltas() {
        let mut state = RunState::default();
        state
            .apply(&json!({"t":"step_started", "seq":1, "node":"legacy-a"}))
            .unwrap();
        state
            .apply(&json!({"t":"run_resumed", "seq":2, "run_id":"run"}))
            .unwrap();
        state
            .apply(&json!({"t":"step_started", "seq":3, "node":"legacy-b"}))
            .unwrap();
        state
            .apply(&json!({"t":"run_resumed", "seq":4, "run_id":"run"}))
            .unwrap();
        assert_eq!(state.budget.step_budget_seed, None);
        assert_eq!(state.budget.step_budget_consumed(), Ok(2));

        // The current engine records an explicit boundary before new work.
        state
            .apply(&step_budget_boundary(5, "run_resumed", "run", 2, 0))
            .unwrap();
        assert_eq!(state.budget.step_budget_seed, Some(2));
        assert_eq!(state.budget.step_budget_charged_at_seed, Some(0));
        assert!(!state.budget.legacy_budget_resume_pending);
        assert_eq!(state.budget.step_budget_consumed(), Ok(2));

        state
            .apply(&json!({"t":"step_started", "seq":6, "node":"new-a"}))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":7, "node":"new-a", "amount":1}))
            .unwrap();
        assert_eq!(state.budget.step_budget_consumed(), Ok(3));

        // A later restart re-anchors the cumulative total and charge position.
        state
            .apply(&step_budget_boundary(8, "run_resumed", "run", 3, 1))
            .unwrap();
        assert_eq!(state.budget.step_budget_seed, Some(3));
        assert_eq!(state.budget.step_budget_charged_at_seed, Some(1));
        state
            .apply(&json!({"t":"step_started", "seq":9, "node":"new-b"}))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":10, "node":"new-b", "amount":1}))
            .unwrap();
        assert_eq!(state.budget.step_budget_consumed(), Ok(4));
    }

    #[test]
    fn unseeded_legacy_resume_followed_by_new_charges_fails_closed() {
        let mut state = RunState::default();
        state
            .apply(&json!({"t":"step_started", "seq":1, "node":"legacy-a"}))
            .unwrap();
        state
            .apply(&json!({"t":"run_resumed", "seq":2, "run_id":"run"}))
            .unwrap();
        state
            .apply(&json!({"t":"step_started", "seq":3, "node":"legacy-b"}))
            .unwrap();
        state
            .apply(&json!({"t":"run_resumed", "seq":4, "run_id":"run"}))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":5, "node":"new", "amount":1}))
            .unwrap();
        assert!(state.budget.legacy_step_budget_unknown);
        assert!(state.budget.step_budget_consumed().is_err());
    }

    #[test]
    fn fork_inherits_legacy_usage_at_its_new_run_started_boundary() {
        let mut state = RunState {
            run_id: Some("source".into()),
            ..RunState::default()
        };
        state.budget.steps_executed = 4;
        state
            .apply(&json!({
                "t":"run_queued", "seq":1, "run_id":"child", "parent_run_id":"source",
                "schema_version":1
            }))
            .expect("fork admission should fold");
        assert_eq!(state.budget.step_budget_seed, None);
        assert!(state.budget.legacy_budget_resume_pending);
        assert_eq!(state.budget.step_budget_consumed(), Ok(4));
        state
            .apply(&step_budget_boundary(2, "run_started", "child", 4, 0))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":3, "node":"new", "amount":1}))
            .unwrap();
        assert_eq!(state.budget.step_budget_consumed(), Ok(5));
    }

    #[test]
    fn legacy_fork_queue_markers_do_not_freeze_step_usage() {
        let mut state = RunState {
            run_id: Some("source".into()),
            ..RunState::default()
        };
        state.budget.steps_executed = 1;
        state
            .apply(&json!({
                "t":"run_queued", "seq":1, "run_id":"child", "parent_run_id":"source",
                "schema_version":1
            }))
            .unwrap();
        state
            .apply(&json!({"t":"step_started", "seq":2, "node":"legacy-after-queue"}))
            .unwrap();
        state
            .apply(&json!({
                "t":"run_queued", "seq":3, "run_id":"child", "parent_run_id":"source",
                "schema_version":1
            }))
            .unwrap();
        assert_eq!(state.budget.step_budget_seed, None);
        assert_eq!(state.budget.step_budget_consumed(), Ok(2));
        state
            .apply(&step_budget_boundary(4, "run_started", "child", 2, 0))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":5, "node":"new", "amount":1}))
            .unwrap();
        assert_eq!(state.budget.step_budget_consumed(), Ok(3));
    }

    #[test]
    fn fork_with_charge_deltas_without_a_baseline_fails_closed() {
        let mut state = RunState {
            run_id: Some("source".into()),
            ..RunState::default()
        };
        state
            .apply(&json!({"t":"step_started", "seq":1, "node":"prior-1"}))
            .unwrap();
        state
            .apply(&json!({"t":"step_started", "seq":2, "node":"prior-2"}))
            .unwrap();
        state
            .apply(&json!({"t":"run_resumed", "seq":3, "run_id":"source"}))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":4, "node":"prior", "amount":2}))
            .unwrap();
        state
            .apply(&json!({
                "t":"run_queued", "seq":5, "run_id":"child", "parent_run_id":"source",
                "schema_version":1
            }))
            .expect("fork admission should fold");
        assert!(state.budget.legacy_step_budget_unknown);
        assert!(state.budget.step_budget_consumed().is_err());
    }

    #[test]
    fn fork_with_a_complete_charge_ledger_can_establish_a_baseline() {
        let mut state = RunState::default();
        state
            .apply(&json!({
                "t":"run_started", "seq":1, "run_id":"source", "schema_version":1
            }))
            .unwrap();
        state
            .apply(&json!({"t":"step_started", "seq":2, "node":"prior-1"}))
            .unwrap();
        state
            .apply(&json!({"t":"step_started", "seq":3, "node":"prior-2"}))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":4, "node":"prior", "amount":2}))
            .unwrap();
        state
            .apply(&json!({
                "t":"run_queued", "seq":5, "run_id":"child", "parent_run_id":"source",
                "schema_version":1
            }))
            .expect("fork admission should fold");
        assert!(!state.budget.legacy_step_budget_unknown);
        assert!(state.budget.legacy_budget_resume_pending);
        assert_eq!(state.budget.step_budget_consumed(), Ok(2));
        state
            .apply(&step_budget_boundary(6, "run_resumed", "child", 2, 2))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":7, "node":"new", "amount":1}))
            .unwrap();
        assert_eq!(state.budget.step_budget_consumed(), Ok(3));
    }

    #[test]
    fn fork_with_a_durable_baseline_preserves_charged_usage() {
        let mut state = RunState::default();
        state
            .apply(&step_budget_boundary(1, "run_started", "source", 0, 0))
            .unwrap();
        state
            .apply(&json!({"t":"step_started", "seq":2, "node":"prior-1"}))
            .unwrap();
        state
            .apply(&json!({"t":"step_started", "seq":3, "node":"prior-2"}))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":4, "node":"prior", "amount":2}))
            .unwrap();
        state
            .apply(&json!({
                "t":"run_queued", "seq":5, "run_id":"child", "parent_run_id":"source",
                "schema_version":1
            }))
            .expect("fork admission should fold");
        assert_eq!(state.budget.step_budget_consumed(), Ok(2));
        state
            .apply(&step_budget_boundary(6, "run_started", "child", 2, 2))
            .unwrap();
        state
            .apply(&json!({"t":"budget_charged", "seq":7, "node":"new", "amount":1}))
            .unwrap();
        assert_eq!(state.budget.step_budget_consumed(), Ok(3));
    }

    #[test]
    fn legacy_retry_and_foreach_without_baselines_refuse_new_baselines() {
        for event in [
            json!({"t":"step_retry", "seq":2, "node":"legacy", "attempt":1}),
            json!({"t":"foreach_iteration", "seq":2, "node":"legacy_each", "index":0}),
        ] {
            let mut state = RunState::default();
            state
                .apply(&json!({"t":"step_started", "seq":1, "node":"legacy"}))
                .unwrap();
            state.apply(&event).unwrap();
            let boundary = step_budget_boundary(3, "run_resumed", "run", 1, 0);
            let error = state
                .apply(&boundary)
                .expect_err("unknown legacy work cannot be baselined");
            assert!(error.to_string().contains("ambiguous legacy history"));
            assert!(state.budget.legacy_step_budget_unknown);
            assert!(state.budget.step_budget_seed.is_none());
            assert!(state.budget.step_budget_consumed().is_err());
        }
    }

    #[test]
    fn step_budget_baseline_requires_matching_usage_and_charge_count() {
        let mut state = RunState::default();
        state
            .apply(&json!({"t":"step_started", "seq":1, "node":"legacy"}))
            .unwrap();
        let mismatch = step_budget_boundary(2, "run_resumed", "run", 2, 0);
        assert!(state.apply(&mismatch).is_err());
        assert_eq!(state.budget.step_budget_seed, None);

        let mut charged = RunState::default();
        charged
            .apply(&step_budget_boundary(1, "run_started", "run", 0, 0))
            .unwrap();
        charged
            .apply(&json!({"t":"budget_charged", "seq":2, "node":"new", "amount":1}))
            .unwrap();
        let wrong_charge_count = step_budget_boundary(3, "run_resumed", "run", 1, 0);
        assert!(charged.apply(&wrong_charge_count).is_err());
        assert_eq!(charged.budget.step_budget_seed, Some(0));

        let mut future = RunState::default();
        let mut unsupported = step_budget_boundary(1, "run_started", "run", 0, 0);
        unsupported["step_budget_baseline"]["version"] = json!(2);
        assert!(future.apply(&unsupported).is_err());
        let mut null_state = RunState::default();
        let null = json!({
            "t":"run_resumed", "seq":1, "run_id":"run", "step_budget_baseline":null
        });
        assert!(null_state.apply(&null).is_err());
    }

    #[test]
    fn step_budget_seed_addition_overflow_fails_closed() {
        let mut state = RunState::default();
        state.budget.step_budget_seed = Some(usize::MAX);
        state.budget.step_budget_charged_at_seed = Some(0);
        state.budget.budget_charged = 1;
        state.budget.has_budget_charges = true;
        assert!(state.budget.step_budget_consumed().is_err());
    }

    #[test]
    fn started_policy_separates_reentry_from_retention() {
        // Legacy Started without a policy field folds fail-closed and
        // blocks both gates; `repeat` passes re-entry but still blocks
        // deletion/GC; finished indeterminate blocks neither.
        let mut legacy = RunState::default();
        legacy
            .apply(&operation_event(1, "operation_started", "op-legacy", None))
            .expect("legacy start should fold");
        assert_eq!(
            legacy.operation_records["op-legacy"].on_indeterminate,
            contract::RetryOnIndeterminate::Fail
        );
        assert!(legacy.ensure_no_fail_closed_inflight_operations().is_err());
        assert!(legacy.ensure_no_inflight_operations().is_err());

        let mut repeated = RunState::default();
        repeated
            .apply(&json!({
                "t": "operation_started",
                "seq": 1,
                "operation_id": "op-repeat",
                "operation_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "invocation_id": "invocation",
                "on_indeterminate": "repeat",
            }))
            .expect("repeat start should fold");
        assert_eq!(
            repeated.operation_records["op-repeat"].on_indeterminate,
            contract::RetryOnIndeterminate::Repeat
        );
        assert!(repeated.ensure_no_fail_closed_inflight_operations().is_ok());
        assert!(repeated.ensure_no_inflight_operations().is_err());

        let mut unknown = RunState::default();
        unknown
            .apply(&json!({
                "t": "operation_started",
                "seq": 1,
                "operation_id": "op-unknown",
                "operation_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "invocation_id": "invocation",
                "on_indeterminate": "sometimes",
            }))
            .expect("unknown policy should fold");
        assert_eq!(
            unknown.operation_records["op-unknown"].on_indeterminate,
            contract::RetryOnIndeterminate::Fail
        );
        assert!(unknown.ensure_no_fail_closed_inflight_operations().is_err());

        let mut finished = RunState::default();
        finished
            .apply(&json!({
                "t": "operation_started",
                "seq": 1,
                "operation_id": "op-done",
                "operation_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "invocation_id": "invocation",
                "on_indeterminate": "fail",
            }))
            .expect("start should fold");
        finished
            .apply(&operation_event(
                2,
                "operation_finished",
                "op-done",
                Some("indeterminate"),
            ))
            .expect("indeterminate finish should fold");
        assert!(finished.ensure_no_fail_closed_inflight_operations().is_ok());
        assert!(finished.ensure_no_inflight_operations().is_ok());

        // A restarted Started attempt updates the durable policy.
        let mut updated = RunState::default();
        updated
            .apply(&operation_event(1, "operation_started", "op-flip", None))
            .expect("legacy start should fold");
        assert!(updated.ensure_no_fail_closed_inflight_operations().is_err());
        updated
            .apply(&json!({
                "t": "operation_started",
                "seq": 2,
                "operation_id": "op-flip",
                "operation_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "invocation_id": "invocation",
                "on_indeterminate": "repeat",
            }))
            .expect("repeat restart should fold");
        assert_eq!(
            updated.operation_records["op-flip"].on_indeterminate,
            contract::RetryOnIndeterminate::Repeat
        );
        assert!(updated.ensure_no_fail_closed_inflight_operations().is_ok());

        // Diagnostics stay nonsecret and per-run.
        let detail = legacy
            .ensure_no_fail_closed_inflight_operations()
            .unwrap_err();
        assert!(detail.contains("op-legacy") && detail.contains("remains Started"));
        assert!(!detail.contains("invocation-payload-secret"));

        // Operation records round-trip the policy through state.json.
        let bytes = serde_json::to_vec(&repeated).expect("state should serialize");
        let reparsed: RunState = serde_json::from_slice(&bytes).expect("state should parse");
        assert_eq!(
            reparsed.operation_records["op-repeat"].on_indeterminate,
            contract::RetryOnIndeterminate::Repeat
        );
        let legacy_bytes = serde_json::to_vec(&legacy).expect("state should serialize");
        assert!(String::from_utf8_lossy(&legacy_bytes).contains("fail"));
    }
}
