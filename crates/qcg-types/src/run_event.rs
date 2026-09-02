use super::{ConfirmSpec, FailureDetail, FormSpec, NodePath, OutputArtifact, RunMetrics};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

pub const RUN_EVENT_DATA_SCHEMAS: &[(&str, &str)] = &[
    ("run_queued", "RunStartedEventData"),
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
    ("llm_delta", "LlmDeltaEventData"),
    ("agent_checkpoint", "AgentCheckpointEventData"),
    ("agent_delegated", "AgentDelegatedEventData"),
    ("agent_completed", "AgentCompletedEventData"),
    ("agent_failed", "AgentFailedEventData"),
    ("agent_handoff", "AgentHandoffEventData"),
    ("context_compacted", "ContextCompactedEventData"),
    ("llm_validation_failed", "LlmValidationFailedEventData"),
    ("llm_route_failed", "LlmRouteFailedEventData"),
    ("tool_call", "ToolCallEventData"),
    ("guardrail_evaluated", "GuardrailEvaluatedEventData"),
    ("guardrail_error", "GuardrailErrorEventData"),
    ("guardrail_tripwire", "GuardrailTripwireEventData"),
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
    RunQueued(RunStartedEventData),
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
    LlmDelta(LlmDeltaEventData),
    AgentCheckpoint(AgentCheckpointEventData),
    AgentDelegated(AgentDelegatedEventData),
    AgentCompleted(AgentCompletedEventData),
    AgentFailed(AgentFailedEventData),
    AgentHandoff(AgentHandoffEventData),
    ContextCompacted(ContextCompactedEventData),
    LlmValidationFailed(LlmValidationFailedEventData),
    LlmRouteFailed(LlmRouteFailedEventData),
    ToolCall(ToolCallEventData),
    GuardrailEvaluated(GuardrailEvaluatedEventData),
    GuardrailError(GuardrailErrorEventData),
    GuardrailTripwire(GuardrailTripwireEventData),
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
            "run_queued" => decode!(RunQueued, RunStartedEventData),
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
            "llm_delta" => decode!(LlmDelta, LlmDeltaEventData),
            "agent_checkpoint" => decode!(AgentCheckpoint, AgentCheckpointEventData),
            "agent_delegated" => decode!(AgentDelegated, AgentDelegatedEventData),
            "agent_completed" => decode!(AgentCompleted, AgentCompletedEventData),
            "agent_failed" => decode!(AgentFailed, AgentFailedEventData),
            "agent_handoff" => decode!(AgentHandoff, AgentHandoffEventData),
            "context_compacted" => decode!(ContextCompacted, ContextCompactedEventData),
            "llm_validation_failed" => {
                decode!(LlmValidationFailed, LlmValidationFailedEventData)
            }
            "llm_route_failed" => decode!(LlmRouteFailed, LlmRouteFailedEventData),
            "tool_call" => decode!(ToolCall, ToolCallEventData),
            "guardrail_evaluated" => decode!(GuardrailEvaluated, GuardrailEvaluatedEventData),
            "guardrail_error" => decode!(GuardrailError, GuardrailErrorEventData),
            "guardrail_tripwire" => decode!(GuardrailTripwire, GuardrailTripwireEventData),
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
            Self::RunQueued(data) | Self::RunStarted(data) => Some(data),
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
    Command { command: Vec<String> },
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
    pub failed_output: Option<Value>,
    #[serde(default)]
    pub output_name: Option<String>,
    #[serde(default)]
    pub failed_files: Vec<Value>,
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
    /// Reasoning tokens included in `output`.
    #[serde(default)]
    pub reasoning: u64,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LlmCallEventData {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub reasoning_effort: Option<crate::ReasoningEffort>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    pub max_tokens: u32,
    #[serde(default)]
    pub stop_sequences: Vec<String>,
    #[serde(default)]
    pub structured_output: crate::StructuredOutputMode,
    #[serde(default)]
    pub tool_choice: Option<crate::ToolChoice>,
    #[serde(default)]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub verbosity: Option<crate::ResponseVerbosity>,
    #[serde(default)]
    pub stream: bool,
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
pub struct LlmDeltaEventData {
    pub provider: String,
    pub model: String,
    pub index: usize,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentCheckpointEventData {
    pub turn: usize,
    pub phase: String,
    pub checkpoint: Value,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentDelegatedEventData {
    pub agent: String,
    pub tool_call_id: String,
    pub tools: Vec<String>,
    pub max_calls: usize,
    pub max_iterations: usize,
    pub max_tokens_total: u64,
    pub max_tool_calls_total: usize,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentCompletedEventData {
    pub agent: String,
    pub tool_call_id: String,
    pub turn: usize,
    pub tokens_total: u64,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentFailedEventData {
    pub agent: String,
    pub tool_call_id: String,
    pub code: AgentFailureCode,
    pub action: AgentFailureAction,
    pub message: String,
}

#[derive(
    Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentFailureCode {
    ToolFailed,
    GuardrailRejected,
    TokenBudgetExceeded,
    ToolCallBudgetExceeded,
    IterationBudgetExceeded,
    RunBudgetExceeded,
    ValidationFailed,
    ProviderFailed,
    Cancelled,
}

impl AgentFailureCode {
    pub fn is_recoverable(self) -> bool {
        !matches!(self, Self::RunBudgetExceeded | Self::Cancelled)
    }

    pub fn policy_code(self) -> Option<RecoverableAgentFailureCode> {
        match self {
            Self::ToolFailed => Some(RecoverableAgentFailureCode::ToolFailed),
            Self::GuardrailRejected => Some(RecoverableAgentFailureCode::GuardrailRejected),
            Self::TokenBudgetExceeded => Some(RecoverableAgentFailureCode::TokenBudgetExceeded),
            Self::ToolCallBudgetExceeded => {
                Some(RecoverableAgentFailureCode::ToolCallBudgetExceeded)
            }
            Self::IterationBudgetExceeded => {
                Some(RecoverableAgentFailureCode::IterationBudgetExceeded)
            }
            Self::ValidationFailed => Some(RecoverableAgentFailureCode::ValidationFailed),
            Self::ProviderFailed => Some(RecoverableAgentFailureCode::ProviderFailed),
            Self::RunBudgetExceeded | Self::Cancelled => None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(rename_all = "snake_case")]
pub enum RecoverableAgentFailureCode {
    ToolFailed,
    GuardrailRejected,
    TokenBudgetExceeded,
    ToolCallBudgetExceeded,
    IterationBudgetExceeded,
    ValidationFailed,
    ProviderFailed,
}

#[derive(Debug, Clone, Copy, Default, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentFailureAction {
    Fail,
    #[default]
    ReturnError,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentHandoffEventData {
    pub agent: String,
    pub tool_call_id: String,
}

/// The policy used when a prompt or an agent transcript exceeded its context limit.
#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextCompactionPolicy {
    Error,
    TruncateHead,
    TruncateTail,
}

/// The two payload shapes emitted by context management are deliberately kept
/// as separate closed objects so prompt compaction cannot silently acquire
/// transcript-only fields.
#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ContextCompactedEventData {
    Prompt(ContextCompactedPromptEventData),
    RequestOrTranscript(ContextCompactedRequestEventData),
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContextCompactedPromptEventData {
    pub policy: ContextCompactionPolicy,
    pub original_bytes: usize,
    pub final_bytes: usize,
    pub limit_bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextCompactionScope {
    Request,
    AgentTranscript,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContextCompactedRequestEventData {
    pub scope: ContextCompactionScope,
    pub policy: ContextCompactionPolicy,
    pub original_bytes: usize,
    pub final_bytes: usize,
    pub limit_bytes: usize,
    pub compacted_tool_results: usize,
    pub compacted_messages: usize,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LlmValidationFailedEventData {
    pub attempt: usize,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LlmRouteFailedEventData {
    pub provider: String,
    pub model: String,
    pub attempt: usize,
    pub kind: LlmRouteFailureKind,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LlmRouteFailureKind {
    HttpStatus(u16),
    TimedOut,
    Network,
    EmptyResponse,
    InvalidResponse,
    PartialStream,
    Canceled,
    CircuitOpen,
    Other,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolCallEventData {
    #[serde(default)]
    pub server: Option<String>,
    pub tool: String,
    pub id: String,
    pub status: ToolCallStatus,
    pub phase: ToolCallPhase,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub error: Option<ToolCallError>,
    pub duration_ms: u64,
    pub arguments: Value,
    pub result: Value,
    pub sources: Vec<ToolCallSource>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    Succeeded,
    Failed,
    NeedsUser,
    NeedsConfirmation,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallPhase {
    InputValidation,
    InputGuardrail,
    Execution,
    OutputGuardrail,
    Completed,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolCallError {
    pub code: ToolCallErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallErrorCode {
    InvalidArguments,
    GuardrailRejected,
    ExecutionFailed,
    OutputRejected,
    Cancelled,
    BudgetExceeded,
    ToolReportedError,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolCallSource {
    pub url: String,
    #[serde(default)]
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GuardrailViolationEventData {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailStage {
    Input,
    Output,
    ToolInput,
    ToolOutput,
}

#[derive(Debug, Clone, Copy, Default, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailErrorPolicy {
    #[default]
    Fail,
    Block,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailErrorKind {
    InvalidConfiguration,
    Evaluation,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GuardrailEvaluatedEventData {
    pub guardrail: String,
    pub kind: String,
    pub stage: GuardrailStage,
    #[serde(default)]
    pub tool: Option<String>,
    pub passed: bool,
    pub tripwire: bool,
    #[serde(default)]
    pub violation: Option<GuardrailViolationEventData>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GuardrailErrorEventData {
    pub guardrail: String,
    pub kind: String,
    pub stage: GuardrailStage,
    #[serde(default)]
    pub tool: Option<String>,
    pub error_kind: GuardrailErrorKind,
    pub code: String,
    pub message: String,
    pub policy: GuardrailErrorPolicy,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GuardrailTripwireEventData {
    pub guardrail: String,
    pub kind: String,
    pub stage: GuardrailStage,
    #[serde(default)]
    pub tool: Option<String>,
    pub violation: GuardrailViolationEventData,
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
