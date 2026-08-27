use super::{ConfirmSpec, FailureDetail, FormSpec, NodePath, OutputArtifact, RunMetrics};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

pub const RUN_EVENT_DATA_SCHEMAS: &[(&str, &str)] = &[
    ("run_started", "RunStartedEventData"),
    ("run_resumed", "EmptyEventData"),
    ("graph_resolved", "GraphResolvedEventData"),
    ("resource", "ResourceEventData"),
    ("step_started", "StepStartedEventData"),
    ("step_finished", "StepFinishedEventData"),
    ("step_replayed", "StepReplayedEventData"),
    ("step_skipped", "ReasonEventData"),
    ("foreach_iteration", "ForeachIterationEventData"),
    ("foreach_budget_exhausted", "ForeachBudgetEventData"),
    ("repair_attempt_started", "RepairAttemptStartedEventData"),
    ("repair_attempt_finished", "AttemptFinishedEventData"),
    (
        "regenerate_attempt_started",
        "RegenerateAttemptStartedEventData",
    ),
    ("regenerate_attempt_finished", "AttemptFinishedEventData"),
    ("llm_call", "LlmCallEventData"),
    ("llm_validation_failed", "LlmValidationFailedEventData"),
    ("tool_call", "ToolCallEventData"),
    ("tool_backend_resolved", "ToolBackendResolvedEventData"),
    ("user_interaction", "UserInteractionEventData"),
    ("out_of_contract", "OutOfContractEventData"),
    ("confirm_request", "ConfirmRequestEventData"),
    ("side_effect", "SideEffectEventData"),
    ("dry_run", "DryRunEventData"),
    ("artifact", "OutputArtifact"),
    ("run_waiting", "RunWaitingEventData"),
    ("run_error", "RunErrorEventData"),
    ("run_canceled", "ReasonEventData"),
    ("run_interrupted", "ReasonEventData"),
    ("run_finished", "RunFinishedEventData"),
    ("lagged", "LaggedEventData"),
];

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum RunEventData {
    RunStarted(RunStartedEventData),
    RunResumed(EmptyEventData),
    GraphResolved(GraphResolvedEventData),
    Resource(ResourceEventData),
    StepStarted(StepStartedEventData),
    StepFinished(StepFinishedEventData),
    StepReplayed(StepReplayedEventData),
    StepSkipped(ReasonEventData),
    ForeachIteration(ForeachIterationEventData),
    ForeachBudgetExhausted(ForeachBudgetEventData),
    RepairAttemptStarted(RepairAttemptStartedEventData),
    RepairAttemptFinished(AttemptFinishedEventData),
    RegenerateAttemptStarted(RegenerateAttemptStartedEventData),
    RegenerateAttemptFinished(AttemptFinishedEventData),
    LlmCall(LlmCallEventData),
    LlmValidationFailed(LlmValidationFailedEventData),
    ToolCall(ToolCallEventData),
    ToolBackendResolved(ToolBackendResolvedEventData),
    UserInteraction(UserInteractionEventData),
    OutOfContract(OutOfContractEventData),
    ConfirmRequest(ConfirmRequestEventData),
    SideEffect(SideEffectEventData),
    DryRun(DryRunEventData),
    Artifact(OutputArtifact),
    RunWaiting(RunWaitingEventData),
    RunError(RunErrorEventData),
    RunCanceled(ReasonEventData),
    RunInterrupted(ReasonEventData),
    RunFinished(RunFinishedEventData),
    Lagged(LaggedEventData),
    Unknown(Value),
}

impl RunEventData {
    pub fn parse(kind: &str, data: Value) -> Result<Self, String> {
        macro_rules! decode {
            ($variant:ident, $type:ty) => {
                serde_json::from_value::<$type>(data)
                    .map(Self::$variant)
                    .map_err(|error| format!("invalid `{kind}` event data: {error}"))
            };
        }

        match kind {
            "run_started" => decode!(RunStarted, RunStartedEventData),
            "run_resumed" => decode!(RunResumed, EmptyEventData),
            "graph_resolved" => decode!(GraphResolved, GraphResolvedEventData),
            "resource" => decode!(Resource, ResourceEventData),
            "step_started" => decode!(StepStarted, StepStartedEventData),
            "step_finished" => decode!(StepFinished, StepFinishedEventData),
            "step_replayed" => decode!(StepReplayed, StepReplayedEventData),
            "step_skipped" => decode!(StepSkipped, ReasonEventData),
            "foreach_iteration" => decode!(ForeachIteration, ForeachIterationEventData),
            "foreach_budget_exhausted" => {
                decode!(ForeachBudgetExhausted, ForeachBudgetEventData)
            }
            "repair_attempt_started" => {
                decode!(RepairAttemptStarted, RepairAttemptStartedEventData)
            }
            "repair_attempt_finished" => {
                decode!(RepairAttemptFinished, AttemptFinishedEventData)
            }
            "regenerate_attempt_started" => {
                decode!(RegenerateAttemptStarted, RegenerateAttemptStartedEventData)
            }
            "regenerate_attempt_finished" => {
                decode!(RegenerateAttemptFinished, AttemptFinishedEventData)
            }
            "llm_call" => decode!(LlmCall, LlmCallEventData),
            "llm_validation_failed" => {
                decode!(LlmValidationFailed, LlmValidationFailedEventData)
            }
            "tool_call" => decode!(ToolCall, ToolCallEventData),
            "tool_backend_resolved" => {
                decode!(ToolBackendResolved, ToolBackendResolvedEventData)
            }
            "user_interaction" => decode!(UserInteraction, UserInteractionEventData),
            "out_of_contract" => decode!(OutOfContract, OutOfContractEventData),
            "confirm_request" => decode!(ConfirmRequest, ConfirmRequestEventData),
            "side_effect" => decode!(SideEffect, SideEffectEventData),
            "dry_run" => decode!(DryRun, DryRunEventData),
            "artifact" => decode!(Artifact, OutputArtifact),
            "run_waiting" => decode!(RunWaiting, RunWaitingEventData),
            "run_error" => decode!(RunError, RunErrorEventData),
            "run_canceled" => decode!(RunCanceled, ReasonEventData),
            "run_interrupted" => decode!(RunInterrupted, ReasonEventData),
            "run_finished" => decode!(RunFinished, RunFinishedEventData),
            "lagged" => decode!(Lagged, LaggedEventData),
            _ => Ok(Self::Unknown(data)),
        }
    }

    pub fn run_started(&self) -> Option<&RunStartedEventData> {
        match self {
            Self::RunStarted(data) => Some(data),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmptyEventData {}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunStartedEventData {
    pub generator: String,
    pub generator_path: String,
    pub contract_sha256: String,
    pub inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub resource_hashes: Vec<ResourceEventData>,
    pub qcg: String,
    pub schema_version: u32,
    #[serde(default)]
    pub retain_days: Option<u64>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphResolvedEventData {
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceEventData {
    pub name: String,
    #[serde(rename = "type")]
    pub resource_type: String,
    pub source: ResourceSource,
    #[serde(default)]
    pub snapshot: Option<String>,
    pub sha256: String,
    pub bytes: usize,
    #[serde(default)]
    pub files: Vec<ResourceFileEventData>,
    pub cache: ResourceEventCacheStatus,
    #[serde(default)]
    pub pin_sha256: Option<String>,
    pub trust: String,
    pub llm_visible: bool,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceSource {
    Path { path: String },
    Url { url: String, final_url: String },
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceFileEventData {
    pub path: String,
    pub sha256: String,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceEventCacheStatus {
    NotApplicable,
    Local,
    Hit,
    Miss,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StepStartedEventData {
    #[serde(rename = "type")]
    pub step_type: String,
    pub attempt: u32,
    #[serde(default)]
    pub parallel: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Success,
    Repaired,
    Routed,
    RepairExhausted,
    AnsweredOnFail,
    Regenerated,
    RegenerateExhausted,
    NeedsUser,
    NeedsConfirm,
    CheckFailed,
    Failed,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StepFinishedEventData {
    pub status: StepStatus,
    #[serde(default)]
    pub files: Vec<Value>,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub output_name: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub findings: Vec<Value>,
    #[serde(default)]
    pub reason: Option<FailureDetail>,
    #[serde(default)]
    pub answer: Option<Value>,
    #[serde(default)]
    pub question: Option<FormSpec>,
    #[serde(default)]
    pub parallel: bool,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StepReplayedEventData {
    pub status: StepStatus,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReasonEventData {
    pub reason: FailureDetail,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForeachIterationEventData {
    pub index: usize,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForeachBudgetEventData {
    pub requested_iterations: usize,
    pub executed_iterations: usize,
    pub max_iterations: usize,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RepairAttemptStartedEventData {
    pub repair: String,
    pub recheck: String,
    pub attempt: u32,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegenerateAttemptStartedEventData {
    pub attempt: u32,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Success,
    Repaired,
    RepairFailed,
    RecheckFailed,
    CheckFailed,
    NeedsUser,
    NeedsConfirm,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AttemptFinishedEventData {
    pub attempt: u32,
    pub status: AttemptStatus,
    #[serde(default)]
    pub reason: Option<FailureDetail>,
    #[serde(default)]
    pub findings: Vec<Value>,
    #[serde(default)]
    pub question: Option<FormSpec>,
    #[serde(default)]
    pub confirm: Option<ConfirmSpec>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TokenUsageEventData {
    pub input: u64,
    pub output: u64,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LlmCallEventData {
    pub provider: String,
    #[serde(default)]
    pub seed: Option<u64>,
    pub tokens: TokenUsageEventData,
    pub cost_microusd: u64,
    #[serde(default)]
    pub attempt: usize,
    #[serde(default)]
    pub turn: Option<usize>,
    #[serde(default)]
    pub tokens_total: Option<u64>,
    #[serde(default)]
    pub max_tokens_total: Option<u64>,
    #[serde(default)]
    pub repair: Option<bool>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LlmValidationFailedEventData {
    pub attempt: usize,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolCallEventData {
    pub tool: String,
    pub id: String,
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolBackendResolvedEventData {
    pub tool: String,
    pub backend: String,
    pub argv: Vec<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserInteractionEventData {
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutOfContractEventData {
    pub policy: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConfirmRequestEventData {
    pub confirm: ConfirmSpec,
    #[serde(default)]
    pub parallel: bool,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SideEffectDecision {
    Denied,
    Allowed,
    ApprovedByUser,
    DeniedByUser,
    ConfirmationRequired,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SideEffectEventData {
    pub kind: String,
    pub target: String,
    pub decision: SideEffectDecision,
    #[serde(default)]
    pub policy: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DryRunEventData {
    pub kind: String,
    pub target: String,
    #[serde(default)]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunWaitingEventData {
    pub question_id: String,
    pub question: FormSpec,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunErrorEventData {
    pub error: String,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunCompletionStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunFinishedEventData {
    pub status: RunCompletionStatus,
    #[serde(default)]
    pub reason: Option<FailureDetail>,
    #[serde(default)]
    pub failures: Vec<RunNodeFailureEventData>,
    pub metrics: RunMetrics,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunNodeFailureEventData {
    pub path: NodePath,
    pub failure: FailureDetail,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LaggedEventData {
    pub action: LaggedAction,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LaggedAction {
    ResyncSnapshot,
}
