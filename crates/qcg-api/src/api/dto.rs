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
    /// Free-form run metadata (for example `owner`). Metadata only: it is
    /// stored and returned with the run, and never confers authorization.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Requests a higher observation-audit level for this run. The
    /// deployment floor and the generator policy can only be raised.
    #[serde(default)]
    pub audit_level: Option<qcg_policy::AuditLevel>,
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
    /// different digests even for the same node and kind. Required:
    /// a confirmation without a digest is corrupt, never an
    /// invocation-scoped default (Q1-5 fail-closed).
    pub operation_digest: String,
    /// How far this approval reaches, recorded from the manifest
    /// permission: `invocation` approves one call, `content` reuses the
    /// approval for identical content until the run ends (Q1). Required:
    /// the scope must be stated explicitly, never defaulted silently.
    /// Wire form: `invocation` confirmations carry 4-part ids
    /// (`node:kind:digest:invocation-hash`), `content` ones 3-part ids
    /// (`node:kind:digest`).
    pub scope: qcg_contract::SideEffectScope,
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
    /// Run metadata admitted with the run; never authorization.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
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
    // New-optional field default (C-3): `errors` was always optional on the
    // wire (an empty list means no field errors). This is the declared
    // schema default for a newly optional field, not a rescue for a removed
    // required field: writers always emit the key, readers accept its
    // absence as `[]`.
    #[serde(default)]
    pub errors: Vec<ProblemFieldError>,
}

/// Selectable LLM provider/model catalog. Metadata only: the catalog never
/// carries credentials and never grants run permissions.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LlmCatalogResponse {
    /// RFC 3339 timestamp of the last successful external catalog fetch.
    #[serde(default)]
    pub fetched_at: Option<String>,
    /// True while external metadata is missing or stale.
    #[serde(default)]
    pub stale: bool,
    #[serde(default)]
    pub sources: Vec<LlmCatalogSource>,
    pub providers: Vec<LlmCatalogProvider>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LlmCatalogSource {
    pub kind: String,
    pub location: String,
    #[serde(default)]
    pub fetched_at: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LlmCatalogProvider {
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
    /// False when the provider needs a credential that is not configured.
    pub available: bool,
    /// Whether live `/v1/models` discovery is enabled for this provider.
    pub discovery: bool,
    #[serde(default)]
    pub error: Option<String>,
    pub models: Vec<LlmCatalogModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LlmCatalogModel {
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
    pub enabled: bool,
    /// `built-in`, `explicit`, `external`, or `discovery` (combined sources
    /// are joined with ` + `).
    pub source: String,
    #[serde(default)]
    pub reasoning_effort: Vec<String>,
    #[serde(default)]
    pub input_cost_per_million_usd: Option<f64>,
    #[serde(default)]
    pub output_cost_per_million_usd: Option<f64>,
    #[serde(default)]
    pub context_tokens: Option<u64>,
    #[serde(default)]
    pub max_output_tokens: Option<u64>,
    pub capabilities: LlmCatalogCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LlmCatalogCapabilities {
    pub tool_use: bool,
    pub json_schema: bool,
    pub structured_output_with_tools: bool,
    pub seed: bool,
    pub image_input: bool,
    pub audio_input: bool,
    pub file_input: bool,
    pub streaming: bool,
    pub temperature: bool,
    pub top_p: bool,
    pub stop_sequences: bool,
    pub tool_choice: bool,
    pub parallel_tool_calls: bool,
    pub verbosity: bool,
}
