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

macro_rules! run_event_data_registry {
    (
        terminal: [$($terminal:literal),* $(,)?];
        $($kind:literal => $variant:ident : $data:ty),* $(,)?
    ) => {
        /// Terminal run-event kinds shared by the service live tail, the SSE
        /// wrapper, and the shared poller, so the three layers can never
        /// disagree on what ends a stream (E12).
        pub const TERMINAL_EVENT_KINDS: &[&str] = &[$($terminal),*];

        #[derive(Debug, Clone, Serialize, JsonSchema)]
        #[serde(untagged)]
        pub enum RunEventData {
            $($variant($data),)*
            Unknown(Value),
        }

        impl RunEventData {
            pub fn parse(kind: &str, data: Value) -> Result<Self, String> {
                macro_rules! decode {
                    ($data_variant:ident, $payload:ty) => {
                        serde_json::from_value::<$payload>(data)
                            .map(Self::$data_variant)
                            .map_err(|error| format!("invalid `{kind}` event data: {error}"))
                    };
                }

                match kind {
                    $($kind => decode!($variant, $data),)*
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

        pub fn is_known_run_event_kind(kind: &str) -> bool {
            matches!(kind, $($kind)|*)
        }

        /// The closed set of kinds this build emits, with the schema each one
        /// publishes. Single source for the OpenAPI document, the generated
        /// event reference, and the client type generator.
        pub fn run_event_data_schemas() -> Vec<(&'static str, String)> {
            vec![$(($kind, <$data as JsonSchema>::schema_name().into_owned())),*]
        }
    };
}

run_event_data_registry! {
    terminal: ["run_finished", "run_error", "run_canceled", "run_interrupted"];
    "run_queued" => RunQueued: RunStartedEventData,
    "run_started" => RunStarted: RunStartedEventData,
    "run_resumed" => RunResumed: EmptyEventData,
    "graph_resolved" => GraphResolved: GraphResolvedEventData,
    "resource" => Resource: ResourceEventData,
    "step_started" => StepStarted: StepStartedEventData,
    "step_retry" => StepRetry: StepRetryEventData,
    "step_finished" => StepFinished: StepFinishedEventData,
    "step_replayed" => StepReplayed: StepReplayedEventData,
    "step_skipped" => StepSkipped: ReasonEventData,
    "foreach_iteration" => ForeachIteration: ForeachIterationEventData,
    "foreach_budget_exhausted" => ForeachBudgetExhausted: ForeachBudgetEventData,
    "repair_attempt_started" => RepairAttemptStarted: RepairAttemptStartedEventData,
    "repair_attempt_finished" => RepairAttemptFinished: AttemptFinishedEventData,
    "regenerate_attempt_started" => RegenerateAttemptStarted: RegenerateAttemptStartedEventData,
    "regenerate_attempt_finished" => RegenerateAttemptFinished: AttemptFinishedEventData,
    "llm_call" => LlmCall: LlmCallEventData,
    "llm_delta" => LlmDelta: LlmDeltaEventData,
    "agent_checkpoint" => AgentCheckpoint: AgentCheckpointEventData,
    "agent_delegated" => AgentDelegated: AgentDelegatedEventData,
    "agent_completed" => AgentCompleted: AgentCompletedEventData,
    "agent_failed" => AgentFailed: AgentFailedEventData,
    "agent_handoff" => AgentHandoff: AgentHandoffEventData,
    "context_compacted" => ContextCompacted: ContextCompactedEventData,
    "llm_validation_failed" => LlmValidationFailed: LlmValidationFailedEventData,
    "llm_route_failed" => LlmRouteFailed: LlmRouteFailedEventData,
    "tool_call" => ToolCall: ToolCallEventData,
    "guardrail_evaluated" => GuardrailEvaluated: GuardrailEvaluatedEventData,
    "guardrail_error" => GuardrailError: GuardrailErrorEventData,
    "guardrail_tripwire" => GuardrailTripwire: GuardrailTripwireEventData,
    "tool_backend_resolved" => ToolBackendResolved: ToolBackendResolvedEventData,
    "user_interaction" => UserInteraction: UserInteractionEventData,
    "out_of_contract" => OutOfContract: OutOfContractEventData,
    "confirm_request" => ConfirmRequest: ConfirmRequestEventData,
    "side_effect" => SideEffect: SideEffectEventData,
    "dry_run" => DryRun: DryRunEventData,
    "artifact" => Artifact: OutputArtifact,
    "run_waiting" => RunWaiting: RunWaitingEventData,
    "run_error" => RunError: RunErrorEventData,
    "run_canceled" => RunCanceled: ReasonEventData,
    "run_interrupted" => RunInterrupted: ReasonEventData,
    "run_finished" => RunFinished: RunFinishedEventData,
    "lagged" => Lagged: LaggedEventData,
}

#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EmptyEventData {}
