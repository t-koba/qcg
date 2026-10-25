use model::{
    ProviderWebSearch, ProviderWebSearchOutcome, ReasoningEffort, ResponseVerbosity,
    StructuredOutputMode, ToolChoice,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Capabilities {
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
    pub prompt_cache: bool,
    /// The provider's own hosted web search is available on this model.
    /// It is a separate mechanism from the local `web.search` tool.
    pub web_search: bool,
    pub reasoning_effort: Vec<ReasoningEffort>,
}

/// Per-request prompt-cache directive resolved from `[llm].cache`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptCache {
    /// Attach the provider's cache instructions for this request.
    Auto,
    /// Send no cache instructions (omit qcg hints only; provider-side
    /// caching is unchanged).
    #[default]
    Off,
}

/// Per-request prompt-cache write directive resolved from
/// `[llm].cache_write_policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheWritePolicy {
    /// Leave provider caching alone.
    #[default]
    Allow,
    /// Require qcg/provider caching to be disabled for the request.
    Forbid,
}

fn is_default_cache_write_policy(policy: &CacheWritePolicy) -> bool {
    *policy == CacheWritePolicy::Allow
}

/// Per-request prompt-cache retention tier resolved from
/// `[llm].cache_retention`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheRetention {
    /// The route's explicitly declared extended-retention mechanism.
    Extended,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub provider: String,
    pub model: String,
    pub system: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ToolSpec>,
    pub response_schema: Option<Value>,
    #[serde(default)]
    pub structured_output: StructuredOutputMode,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub max_tokens: u32,
    pub stop_sequences: Vec<String>,
    pub seed: Option<u64>,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub tool_choice: Option<ToolChoice>,
    pub parallel_tool_calls: Option<bool>,
    pub verbosity: Option<ResponseVerbosity>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub prompt_cache: PromptCache,
    #[serde(default, skip_serializing_if = "is_default_cache_write_policy")]
    pub cache_write: CacheWritePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_retention: Option<CacheRetention>,
    /// The hosted web search this request opts into. Absent by default:
    /// selecting a model never enables the provider's search.
    #[serde(default)]
    pub provider_web_search: Option<ProviderWebSearch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<ChatContentPart>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ChatToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_state: Option<Vec<Value>>,
}

impl ChatMessage {
    pub fn text(role: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: content.into(),
            parts: vec![],
            tool_calls: vec![],
            tool_call_id: None,
            provider_state: None,
        }
    }

    pub fn assistant_tool_calls(content: impl Into<String>, tool_calls: Vec<ChatToolCall>) -> Self {
        let mut message = Self::text("assistant", content);
        message.tool_calls = tool_calls;
        message
    }

    pub fn tool_result(id: impl Into<String>, content: impl Into<String>) -> Self {
        let mut message = Self::text("tool", content);
        message.tool_call_id = Some(id.into());
        message
    }

    pub fn provider_state(items: Vec<Value>) -> Self {
        let mut message = Self::text("provider", String::new());
        message.provider_state = Some(items);
        message
    }

    pub fn with_parts(role: impl Into<String>, parts: Vec<ChatContentPart>) -> Self {
        let mut message = Self::text(role, String::new());
        message.parts = parts;
        message
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChatContentPart {
    Text {
        text: String,
    },
    InputImage {
        media_type: String,
        data: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    InputAudio {
        media_type: String,
        data: String,
    },
    InputFile {
        media_type: String,
        data: String,
        filename: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageDetail {
    Auto,
    Low,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatToolCall {
    pub id: String,
    pub name: String,
    pub args: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatResponse {
    pub content: Vec<ChatContent>,
    pub usage: TokenUsage,
    pub stop: StopReason,
    pub provider_state: Option<Value>,
    /// What the provider's hosted search did in this response. Absent when
    /// the request did not enable the hosted search.
    #[serde(default)]
    pub web_search: Option<ProviderWebSearchOutcome>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChatStreamEvent {
    TextDelta { text: String },
    Completed { response: ChatResponse },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatContent {
    Text(String),
    ToolCall {
        id: String,
        name: String,
        args: Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TokenUsage {
    /// Total input, including the disjoint cache read and creation subsets.
    pub input: u64,
    pub output: u64,
    /// Reasoning tokens included in `output`, when reported by the provider.
    pub reasoning: u64,
    /// Input tokens served from provider cache, included in `input`.
    pub cached_input: u64,
    /// Input tokens used to create provider cache entries, included in `input`.
    #[serde(default)]
    pub cache_creation_input: u64,
}

impl TokenUsage {
    /// Validate provider counters before metering disjoint input categories.
    pub fn ordinary_input(&self) -> Result<u64, LlmError> {
        if self.reasoning > self.output {
            return Err(LlmError::invalid_response("reasoning usage exceeds output"));
        }
        self.cached_input
            .checked_add(self.cache_creation_input)
            .and_then(|cached| self.input.checked_sub(cached))
            .ok_or_else(|| {
                LlmError::invalid_response("cache usage exceeds total input or overflowed")
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    ModelContextWindowExceeded,
    Refusal,
}

pub use model::LlmErrorKind;

#[derive(Debug, Default, thiserror::Error)]
#[error("{message}")]
pub struct LlmError {
    pub message: String,
    pub kind: LlmErrorKind,
    pub facts: model::TransportFacts,
    pub decisions: Vec<model::RetryDecision>,
    /// Sanitized provider `error.code` for this failure, when a bounded
    /// inspection of the error response observed one. Credential-free and
    /// inert unless a provider retry rule explicitly matches it.
    pub provider_code: Option<String>,
    /// Bounded `Retry-After` delay observed on this failure, in seconds.
    /// Capped at 60; `None` preserves current default backoff.
    pub retry_after_secs: Option<u64>,
}

impl LlmError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::Other,
            ..Default::default()
        }
    }

    pub fn is_retryable(&self) -> bool {
        is_retryable_llm_error(self)
    }

    pub(crate) fn http_status(status: u16, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::HttpStatus(status),
            facts: model::TransportFacts {
                phase: model::TransportPhase::Response,
                dispatch: model::RequestDispatch::Sent,
            },
            ..Default::default()
        }
    }

    pub(crate) fn invalid_response(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::InvalidResponse,
            facts: model::TransportFacts {
                phase: model::TransportPhase::Response,
                dispatch: model::RequestDispatch::Sent,
            },
            ..Default::default()
        }
    }

    pub(crate) fn empty_response(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::EmptyResponse,
            facts: model::TransportFacts {
                phase: model::TransportPhase::Response,
                dispatch: model::RequestDispatch::Sent,
            },
            ..Default::default()
        }
    }
}

pub(crate) fn is_retryable_llm_error(error: &LlmError) -> bool {
    match error.kind {
        LlmErrorKind::TimedOut => true,
        // Routers sometimes return 200 with an empty body while the upstream
        // pool is saturated; retrying these transient empties is required for
        // shared-pool providers.
        LlmErrorKind::EmptyResponse => true,
        LlmErrorKind::CircuitOpen => true,
        LlmErrorKind::HttpStatus(status) => status == 429 || (500..=599).contains(&status),
        _ => false,
    }
}
