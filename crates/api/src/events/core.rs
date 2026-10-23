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
    AgentFailedEventData, AgentFileChangesEventData, AgentHandoffEventData,
    ContextCompactedEventData, LlmCallEventData, LlmDeltaEventData, LlmRouteFailedEventData,
    LlmValidationFailedEventData,
};
use super::resources::ResourceEventData;
use super::run::{GraphResolvedEventData, RunStartedEventData};
use super::steps::{
    ForeachBudgetEventData, ForeachIterationEventData, ReasonEventData,
    RegenerateAttemptStartedEventData, RepairAttemptStartedEventData, StepFinishedEventData,
    StepReplayedEventData, StepRetryEventData, StepStartedEventData,
};
use super::tools::ToolCallEventData;
use model::OutputArtifact;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
macro_rules! run_event_data_registry {
    (
        terminal: [$($terminal:literal),* $(,)?];
        $($kind:literal => $variant:ident : $data:ty),* $(,)?
    ) => {
        /// Terminal kinds shared by the service live tail and pollers (E12).
        pub const TERMINAL_EVENT_KINDS: &[&str] = &[$($terminal),*];
        #[derive(Debug, Clone, Serialize, JsonSchema)]
        #[serde(untagged)]
        pub enum RunEventData {
            $($variant($data),)*
            Unknown(Value),
        }
        impl RunEventData {
            pub fn parse(kind: &str, data: Value) -> Result<Self, String> {
                // Explicit schema-1 migration: historical run identity events
                // omitted the mode and mean wall-clock accounting. Current
                // schema-2 records must carry the required field.
                let mut data = data;
                if matches!(kind, "run_queued" | "run_started")
                    && data.get("schema_version").and_then(Value::as_u64) == Some(1)
                    && data.get("elapsed_budget_mode").is_none()
                    && let Some(object) = data.as_object_mut()
                {
                    object.insert(
                        "elapsed_budget_mode".into(),
                        Value::String("wall_clock".into()),
                    );
                }
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
        /// The single source of event schemas for OpenAPI, docs, and clients.
        pub fn run_event_data_schemas() -> Vec<(&'static str, String)> {
            vec![$(($kind, <$data as JsonSchema>::schema_name().into_owned())),*]
        }
    };
}
run_event_data_registry! {
    terminal: ["run_finished", "run_error", "run_canceled", "run_interrupted"];
    "elapsed_accounted" => ElapsedAccounted: super::run::ElapsedAccountedEventData,
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
    "agent_file_changes" => AgentFileChanges: AgentFileChangesEventData,
    "agent_delegated" => AgentDelegated: AgentDelegatedEventData,
    "agent_completed" => AgentCompleted: AgentCompletedEventData,
    "agent_failed" => AgentFailed: AgentFailedEventData,
    "agent_handoff" => AgentHandoff: AgentHandoffEventData,
    "context_compacted" => ContextCompacted: ContextCompactedEventData,
    "llm_validation_failed" => LlmValidationFailed: LlmValidationFailedEventData,
    "llm_route_failed" => LlmRouteFailed: LlmRouteFailedEventData,
    "tool_call" => ToolCall: ToolCallEventData,
    "mcp_cleanup_settled" => McpCleanupSettled: ToolCallEventData,
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
