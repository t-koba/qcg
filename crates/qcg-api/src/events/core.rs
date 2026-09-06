use qcg_types::OutputArtifact;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

use super::attempts::AttemptFinishedEventData;
use super::completion::{LaggedEventData, RunErrorEventData, RunFinishedEventData};
use super::guardrails::{
    GuardrailErrorEventData, GuardrailEvaluatedEventData, GuardrailTripwireEventData,
};
use super::interaction::{
    ConfirmRequestEventData, DryRunEventData, OutOfContractEventData, RunWaitingEventData,
    SideEffectEventData, ToolBackendResolvedEventData, UserInteractionEventData,
};
use super::llm::{
    AgentCheckpointEventData, AgentCompletedEventData, AgentDelegatedEventData,
    AgentFailedEventData, AgentHandoffEventData, ContextCompactedEventData, LlmCallEventData,
    LlmDeltaEventData, LlmRouteFailedEventData, LlmValidationFailedEventData,
};
use super::resources::ResourceEventData;
use super::run::{GraphResolvedEventData, RunStartedEventData};
use super::steps::{
    ForeachBudgetEventData, ForeachIterationEventData, ReasonEventData,
    RegenerateAttemptStartedEventData, RepairAttemptStartedEventData, StepFinishedEventData,
    StepReplayedEventData, StepRetryEventData, StepStartedEventData,
};
use super::tools::ToolCallEventData;

pub const RUN_EVENT_DATA_SCHEMAS: &[(&str, &str)] = &[
    ("run_queued", "RunStartedEventData"),
    ("run_started", "RunStartedEventData"),
    ("run_resumed", "EmptyEventData"),
    ("graph_resolved", "GraphResolvedEventData"),
    ("resource", "ResourceEventData"),
    ("step_started", "StepStartedEventData"),
    ("step_retry", "StepRetryEventData"),
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
    StepRetry(StepRetryEventData),
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
            "step_retry" => decode!(StepRetry, StepRetryEventData),
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
