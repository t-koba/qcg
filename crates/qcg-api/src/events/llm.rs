use qcg_contract::{AgentFailureAction, AgentFailureCode};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

use super::attempts::TokenUsageEventData;

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
    pub reasoning_effort: Option<qcg_types::ReasoningEffort>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    pub max_tokens: u32,
    #[serde(default)]
    pub stop_sequences: Vec<String>,
    #[serde(default)]
    pub structured_output: qcg_types::StructuredOutputMode,
    #[serde(default)]
    pub tool_choice: Option<qcg_types::ToolChoice>,
    #[serde(default)]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub verbosity: Option<qcg_types::ResponseVerbosity>,
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
    /// Zero-based index into the declared route list (0 is the primary route).
    #[serde(default)]
    pub fallback_index: usize,
    /// Number of declared routes available for this invocation.
    #[serde(default)]
    pub routes_total: usize,
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
