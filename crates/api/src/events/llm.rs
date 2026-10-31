use contract::{AgentFailureAction, AgentFailureCode};
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
    pub reasoning_effort: Option<model::ReasoningEffort>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    pub max_tokens: u32,
    #[serde(default)]
    pub stop_sequences: Vec<String>,
    #[serde(default)]
    pub structured_output: model::StructuredOutputMode,
    #[serde(default)]
    pub tool_choice: Option<model::ToolChoice>,
    #[serde(default)]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub verbosity: Option<model::ResponseVerbosity>,
    #[serde(default)]
    pub stream: bool,
    /// Normalized terminal reason; absent in older journals and decision calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    pub tokens: TokenUsageEventData,
    /// Estimated priced portion of the call; excludes explicitly unmetered components.
    pub cost_microusd: u64,
    /// Failed provider attempts, including retries preceding a successful call.
    #[serde(default)]
    pub retry_decisions: Vec<model::RetryDecision>,
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
    /// Requested cache write policy when non-default (`forbid` only).
    /// Absent means the default `allow`: the request left provider caching
    /// alone. Present only to make the TTL cost basis inspectable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_policy: Option<contract::CacheWritePolicy>,
    /// Requested cache retention tier when non-default (`extended` only).
    /// Absent means the provider default lifetime. Present only to make the
    /// TTL cost basis inspectable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_retention: Option<contract::CacheRetention>,
    /// Hosted-search details; null when the request did not enable the tool.
    #[serde(default)]
    pub web_search: Option<WebSearchUsage>,
}

/// What provider-hosted search did for one response and how much was metered.
#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WebSearchUsage {
    /// Provider-billable units under the configured mapping; null is unknown, never zero.
    #[serde(default)]
    pub calls: Option<u64>,
    /// Hosted search charge in microusd; null means unmetered, never free.
    #[serde(default)]
    pub cost_microusd: Option<u64>,
    #[serde(default)]
    pub events: Vec<model::ProviderSearchEvent>,
    #[serde(default)]
    pub citations: Vec<model::UrlCitation>,
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
pub struct AgentFileChangesEventData {
    pub removed_files: Vec<String>,
    pub written_files: Vec<String>,
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
pub struct AgentResourceExhaustion {
    pub resource: String,
    pub used: u64,
    pub allowed: u64,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentFailedEventData {
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    pub code: AgentFailureCode,
    pub action: AgentFailureAction,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_exhaustion: Option<AgentResourceExhaustion>,
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
    #[serde(default)]
    pub retry_decisions: Vec<model::RetryDecision>,
    /// Whether this invocation advanced to the next declared route.
    #[serde(default)]
    pub fallback_taken: bool,
    /// Zero-based index into the declared route list (0 is the primary route).
    #[serde(default)]
    pub fallback_index: usize,
    /// Number of declared routes available for this invocation.
    #[serde(default)]
    pub routes_total: usize,
}

pub type LlmRouteFailureKind = model::LlmErrorKind;
