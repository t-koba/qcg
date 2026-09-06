use qcg_types::{ReasoningEffort, ResponseVerbosity, StructuredOutputMode, ToolChoice};
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
    pub reasoning_effort: Vec<ReasoningEffort>,
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
        Self {
            role: "assistant".into(),
            content: content.into(),
            parts: vec![],
            tool_calls,
            tool_call_id: None,
            provider_state: None,
        }
    }

    pub fn tool_result(id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: "tool".into(),
            content: content.into(),
            parts: vec![],
            tool_calls: vec![],
            tool_call_id: Some(id.into()),
            provider_state: None,
        }
    }

    pub fn provider_state(items: Vec<Value>) -> Self {
        Self {
            role: "provider".into(),
            content: String::new(),
            parts: vec![],
            tool_calls: vec![],
            tool_call_id: None,
            provider_state: Some(items),
        }
    }

    pub fn with_parts(role: impl Into<String>, parts: Vec<ChatContentPart>) -> Self {
        Self {
            role: role.into(),
            content: String::new(),
            parts,
            tool_calls: vec![],
            tool_call_id: None,
            provider_state: None,
        }
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
    pub input: u64,
    pub output: u64,
    /// Reasoning tokens included in `output`, when reported by the provider.
    pub reasoning: u64,
    /// Input tokens served from provider cache, included in `input`.
    /// Billed at the input rate; cache discounts are not modeled.
    pub cached_input: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    Refusal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmErrorKind {
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

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct LlmError {
    pub message: String,
    pub kind: LlmErrorKind,
}

impl LlmError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::Other,
        }
    }

    pub fn is_retryable(&self) -> bool {
        is_retryable_llm_error(self)
    }

    pub(crate) fn http_status(status: u16, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::HttpStatus(status),
        }
    }

    pub(crate) fn invalid_response(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::InvalidResponse,
        }
    }

    pub(crate) fn empty_response(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::EmptyResponse,
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
