use qcg_contract::{AssetSpec, GeneratorMeta, InputField, InputSpec, Permissions};
use qcg_types::{OutputManifest, RunMetrics};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GeneratorSummary {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GeneratorDetail {
    pub generator: GeneratorMeta,
    pub inputs: InputSpec,
    pub assets: AssetSpec,
    pub permissions: Permissions,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct StartRun {
    pub generator_id: String,
    pub inputs: BTreeMap<String, Value>,
    /// Pre-provisioned answers keyed by question id, consumed without interaction.
    /// Same semantics as eval suite answers.
    #[serde(default)]
    pub answers: BTreeMap<String, Value>,
    /// Pre-provisioned confirmation decisions keyed by confirmation id.
    #[serde(default)]
    pub confirmations: BTreeMap<String, bool>,
    /// Scheduling priority, higher runs first. Defaults to 0.
    #[serde(default)]
    pub priority: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForkStatePatch {
    #[serde(default)]
    pub inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub step_outputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub step_statuses: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForkRun {
    pub at_seq: u64,
    #[serde(default)]
    pub state_patch: ForkStatePatch,
    /// Pre-provisioned answers keyed by question id, consumed without interaction.
    #[serde(default)]
    pub answers: BTreeMap<String, Value>,
    /// Pre-provisioned confirmation decisions keyed by confirmation id.
    #[serde(default)]
    pub confirmations: BTreeMap<String, bool>,
    /// Scheduling priority, higher runs first. Defaults to 0.
    #[serde(default)]
    pub priority: Option<i32>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    Waiting,
    Confirming,
    /// A cancel request was accepted into the mailbox but no terminal
    /// outcome is journaled yet. This is acceptance, not settlement: only
    /// a journaled terminal state may report `Canceled` (A02).
    CancelRequested,
    Succeeded,
    Failed,
    Canceled,
    Interrupted,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Canceled | Self::Interrupted
        )
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Confirming => "confirming",
            Self::CancelRequested => "cancel_requested",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Interrupted => "interrupted",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FormSpec {
    pub id: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub title_i18n: BTreeMap<String, String>,
    pub fields: Vec<InputField>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConfirmSpec {
    pub id: String,
    pub title: String,
    pub kind: String,
    pub target: String,
    pub dry_run: bool,
    #[serde(default)]
    pub details: Option<Value>,
    /// Canonical digest of target + details binding this approval to the
    /// exact operation content (A06). Approvals never transfer across
    /// different digests even for the same node and kind.
    #[serde(default)]
    pub operation_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunSnapshot {
    pub run_id: String,
    pub state: RunStatus,
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub contract_sha256: Option<String>,
    /// Owning generator id so clients never parse it from run_id (C03).
    /// Required: a snapshot without an owner is corrupt, never something
    /// to guess from the run id.
    pub generator_id: String,
    pub artifacts: Option<OutputManifest>,
    pub question: Option<FormSpec>,
    pub confirm: Option<ConfirmSpec>,
    /// RFC 3339 timestamp of the last transition to `Queued`, if any.
    #[serde(default)]
    pub queued_at: Option<String>,
    /// 1-based position among queued runs, present only while `Queued`.
    #[serde(default)]
    pub queue_position: Option<usize>,
    /// Scheduling priority, higher runs first.
    #[serde(default)]
    pub priority: i32,
    /// Parent run id for runs created by fork.
    #[serde(default)]
    pub parent_run_id: Option<String>,
    /// Cost totals: exact terminal metrics when finished, otherwise a live
    /// synthesis from the folded budget. Absent only before any activity.
    #[serde(default)]
    pub metrics: Option<RunMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunListItem {
    pub run_id: String,
    pub state: RunStatus,
    pub generator_id: String,
    pub started_at: String,
    #[serde(default)]
    pub seq: u64,
}

/// Per-run cost metrics with a USD estimate for display.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunCostMetrics {
    pub run_id: String,
    pub state: RunStatus,
    pub metrics: RunMetrics,
    /// `cost_microusd / 1_000_000`, for display only.
    pub cost_usd: f64,
    /// False when any billed call lacked contract pricing, so the total may
    /// understate the true spend.
    pub priced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunListResponse {
    pub items: Vec<RunListItem>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// Run history sort order. Ascending is the default and preserves the
/// existing cursor contract; descending serves newest-first views so
/// recent runs never fall out of a capped fetch window (B12).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunListOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct RunListQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub state: Option<RunStatus>,
    pub generator_id: Option<String>,
    pub since: Option<String>,
    pub order: Option<RunListOrder>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AnswerPayload {
    pub values: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConfirmDecision {
    pub decision: ConfirmationDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct McpServerSummary {
    pub id: String,
    pub transport: String,
    pub auth: String,
    pub authorized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct McpServerList {
    pub items: Vec<McpServerSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct McpAuthorizationStart {
    pub authorization_url: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmationDecision {
    Approve,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProblemFieldError {
    pub field: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProblemDetails {
    #[serde(rename = "type")]
    pub problem_type: String,
    pub title: String,
    pub status: u16,
    pub detail: String,
    pub instance: String,
    pub code: String,
    #[serde(default)]
    pub errors: Vec<ProblemFieldError>,
}
