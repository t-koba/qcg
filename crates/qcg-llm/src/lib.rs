use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use futures_util::StreamExt;
use qcg_types::{
    ReasoningEffort, ResponseVerbosity, StructuredOutputMode, ToolChoice, ToolChoiceMode,
    credential_like_name,
};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sse_stream::SseStream;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
#[cfg(unix)]
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::{Duration, sleep, timeout};

mod search;

pub use search::{SearchMethod, SearchProfile, SearchProviderSpec, SearchRuntime};

const DEFAULT_RESPONSE_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROVIDER_TIMEOUT_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_PROVIDER_RESPONSE_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_PROVIDER_CONCURRENCY: usize = 1024;
const MAX_PROVIDER_REQUESTS_PER_MINUTE: usize = 1_000_000;
const MAX_PROVIDER_CIRCUIT_FAILURES: usize = 10_000;
const MAX_CREDENTIAL_FILE_BYTES: u64 = 64 * 1024;
const MAX_TOOL_SCHEMA_BYTES: usize = 256 * 1024;
const MAX_TOOL_SCHEMA_DEPTH: usize = 64;
const MAX_TOOL_SCHEMA_NODES: usize = 8_192;
const MAX_PROVIDERS_FILE_BYTES: usize = 4 * 1024 * 1024;
const MAX_PROVIDER_ENTRIES_PER_KIND: usize = 256;
const MAX_PROVIDER_ENTRIES_TOTAL: usize = 512;

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

    fn http_status(status: u16, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::HttpStatus(status),
        }
    }

    fn invalid_response(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::InvalidResponse,
        }
    }

    fn empty_response(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LlmErrorKind::EmptyResponse,
        }
    }
}

fn is_retryable_llm_error(error: &LlmError) -> bool {
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

#[async_trait]
pub trait LlmProvider: Send + Sync {
    fn id(&self) -> &str;
    fn capabilities(&self) -> Capabilities;
    fn capabilities_for(&self, provider: &str) -> Option<Capabilities> {
        (provider == self.id()).then(|| self.capabilities())
    }
    fn configuration_error_for(&self, _provider: &str) -> Option<String> {
        None
    }
    /// Names of environment variables that may contain provider credentials.
    ///
    /// Implementations must never return the credential values themselves.
    fn credential_env_names(&self) -> Vec<String> {
        Vec::new()
    }
    /// Upper bound for a single completion call, including model thinking
    /// time. Defaults to 120 seconds; providers backed by reasoning models
    /// override this through their registry row (`timeout_seconds`).
    fn timeout_seconds(&self) -> u64 {
        120
    }
    async fn complete(&self, req: ChatRequest) -> Result<ChatResponse, LlmError>;

    async fn stream(
        &self,
        req: ChatRequest,
        events: mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        let response = self.complete(req).await?;
        for content in &response.content {
            if let ChatContent::Text(text) = content {
                events
                    .send(ChatStreamEvent::TextDelta { text: text.clone() })
                    .await
                    .map_err(|_| LlmError::new("LLM stream receiver closed"))?;
            }
        }
        events
            .send(ChatStreamEvent::Completed { response })
            .await
            .map_err(|_| LlmError::new("LLM stream receiver closed"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSelection {
    pub provider: String,
    pub model: String,
}

pub struct LlmRuntime {
    pub provider: Arc<dyn LlmProvider>,
    pub default_model: Option<ModelSelection>,
    pub search: SearchRuntime,
    pub mcp: qcg_mcp::McpRuntime,
    /// Whether a providers registry file was resolved for this process. When
    /// false only the built-in `fake` provider is registered, and
    /// validation errors for other ids carry registry-setup guidance.
    pub registry_present: bool,
}

impl LlmRuntime {
    /// Runtime backed by the built-in `fake` provider and anonymous public MCP
    /// profiles. Used when no providers registry was named or found so local
    /// test contracts and public MCP-backed generators remain self-contained.
    pub fn builtins() -> Self {
        let router = LlmRouter::parse_text("").expect("an empty registry is valid");
        Self {
            provider: Arc::new(router),
            default_model: None,
            search: SearchRuntime::unavailable(),
            mcp: qcg_mcp::McpRuntime::public_defaults(),
            registry_present: false,
        }
    }
}

impl std::fmt::Debug for LlmRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmRuntime")
            .field("provider_id", &self.provider.id())
            .field("default_model", &self.default_model)
            .field("search", &self.search)
            .field("mcp", &self.mcp)
            .field("registry_present", &self.registry_present)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiFlavor {
    ChatCompletions,
    Responses,
    AnthropicMessages,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatTokenLimitField {
    MaxTokens,
    MaxCompletionTokens,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSpec {
    pub id: String,
    pub api: ApiFlavor,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub base_url_env: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub api_key_file_env: Option<String>,
    #[serde(default)]
    pub auth_header: Option<String>,
    #[serde(default)]
    pub capabilities: Capabilities,
    #[serde(default)]
    pub path_template: Option<String>,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub chat_token_limit_field: Option<ChatTokenLimitField>,
    #[serde(default)]
    pub response_body_limit_bytes: Option<usize>,
    #[serde(default)]
    pub max_concurrency: Option<usize>,
    #[serde(default)]
    pub requests_per_minute: Option<usize>,
    #[serde(default)]
    pub circuit_breaker_failures: Option<usize>,
    #[serde(default)]
    pub circuit_breaker_cooldown_seconds: Option<u64>,
}

impl ProviderSpec {
    fn validate(&self) -> Result<(), String> {
        if !is_provider_id(&self.id) {
            return Err(format!(
                "provider id `{}` must contain only lowercase ASCII letters, digits, `.`, `_`, or `-`",
                self.id
            ));
        }
        if self.id == "fake" {
            return Err("provider id `fake` is reserved for the built-in provider".into());
        }
        if self.base_url.is_none() && self.base_url_env.is_none() {
            return Err(format!(
                "provider `{}` must declare `base_url` or `base_url_env`",
                self.id
            ));
        }
        for (field, value) in [
            ("api_key_env", self.api_key_env.as_deref()),
            ("api_key_file_env", self.api_key_file_env.as_deref()),
        ] {
            if value.is_some_and(str::is_empty) {
                return Err(format!(
                    "provider `{}` declares an empty `{field}`",
                    self.id
                ));
            }
        }
        if self.api_key_env.is_some() && self.api_key_file_env.is_some() {
            return Err(format!(
                "provider `{}` must declare at most one of api_key_env or api_key_file_env",
                self.id
            ));
        }
        let has_credential = self.api_key_env.is_some() || self.api_key_file_env.is_some();
        let credential_source_env = self
            .api_key_env
            .as_deref()
            .or(self.api_key_file_env.as_deref());
        if self.auth_header.is_some() && !has_credential {
            return Err(format!(
                "provider `{}` may not set `auth_header` without a credential source",
                self.id
            ));
        }
        if let Some(header) = self.auth_header.as_deref()
            && reqwest::header::HeaderName::from_bytes(header.as_bytes()).is_err()
        {
            return Err(format!(
                "provider `{}` has an invalid auth_header `{header}`",
                self.id
            ));
        }
        if let Some(path) = self.path_template.as_deref() {
            if (path.contains('{') && !path.contains("{model}"))
                || path
                    .replace("{model}", "")
                    .chars()
                    .any(|character| matches!(character, '{' | '}'))
            {
                return Err(format!(
                    "provider `{}` path_template contains an unknown placeholder",
                    self.id
                ));
            }
            if path
                .trim_matches('/')
                .split('/')
                .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
            {
                return Err(format!(
                    "provider `{}` path_template must contain safe non-empty path segments",
                    self.id
                ));
            }
        }
        if self.query.keys().any(String::is_empty) {
            return Err(format!(
                "provider `{}` query parameter names must not be empty",
                self.id
            ));
        }
        for (field, name) in [
            ("base_url_env", self.base_url_env.as_deref()),
            ("api_key_env", self.api_key_env.as_deref()),
            ("api_key_file_env", self.api_key_file_env.as_deref()),
        ] {
            if let Some(name) = name
                && !is_env_name(name)
            {
                return Err(format!(
                    "provider `{}` has an invalid `{field}` environment variable name `{name}`",
                    self.id
                ));
            }
        }
        if self.timeout_seconds == Some(0) {
            return Err(format!(
                "provider `{}` timeout_seconds must be greater than zero",
                self.id
            ));
        }
        if self
            .timeout_seconds
            .is_some_and(|value| value > MAX_PROVIDER_TIMEOUT_SECONDS)
        {
            return Err(format!(
                "provider `{}` timeout_seconds must not exceed {MAX_PROVIDER_TIMEOUT_SECONDS}",
                self.id
            ));
        }
        if self.response_body_limit_bytes == Some(0) {
            return Err(format!(
                "provider `{}` response_body_limit_bytes must be greater than zero",
                self.id
            ));
        }
        if self
            .response_body_limit_bytes
            .is_some_and(|value| value > MAX_PROVIDER_RESPONSE_BODY_BYTES)
        {
            return Err(format!(
                "provider `{}` response_body_limit_bytes must not exceed {MAX_PROVIDER_RESPONSE_BODY_BYTES}",
                self.id
            ));
        }
        for (field, value) in [
            ("max_concurrency", self.max_concurrency),
            ("requests_per_minute", self.requests_per_minute),
            ("circuit_breaker_failures", self.circuit_breaker_failures),
        ] {
            if value == Some(0) {
                return Err(format!(
                    "provider `{}` {field} must be greater than zero",
                    self.id
                ));
            }
        }
        for (field, value, maximum) in [
            (
                "max_concurrency",
                self.max_concurrency,
                MAX_PROVIDER_CONCURRENCY,
            ),
            (
                "requests_per_minute",
                self.requests_per_minute,
                MAX_PROVIDER_REQUESTS_PER_MINUTE,
            ),
            (
                "circuit_breaker_failures",
                self.circuit_breaker_failures,
                MAX_PROVIDER_CIRCUIT_FAILURES,
            ),
        ] {
            if value.is_some_and(|value| value > maximum) {
                return Err(format!(
                    "provider `{}` {field} must not exceed {maximum}",
                    self.id
                ));
            }
        }
        if self.circuit_breaker_cooldown_seconds == Some(0) {
            return Err(format!(
                "provider `{}` circuit_breaker_cooldown_seconds must be greater than zero",
                self.id
            ));
        }
        if self
            .circuit_breaker_cooldown_seconds
            .is_some_and(|value| value > MAX_PROVIDER_TIMEOUT_SECONDS)
        {
            return Err(format!(
                "provider `{}` circuit_breaker_cooldown_seconds must not exceed {MAX_PROVIDER_TIMEOUT_SECONDS}",
                self.id
            ));
        }
        if self.capabilities.seed && self.api != ApiFlavor::ChatCompletions {
            return Err(format!(
                "provider `{}` may only advertise `seed` for chat_completions",
                self.id
            ));
        }
        if !self.capabilities.reasoning_effort.is_empty()
            && self.api == ApiFlavor::AnthropicMessages
        {
            return Err(format!(
                "provider `{}` may not advertise OpenAI reasoning_effort for anthropic_messages",
                self.id
            ));
        }
        if self.capabilities.structured_output_with_tools
            && self.api == ApiFlavor::AnthropicMessages
        {
            return Err(format!(
                "provider `{}` may not advertise `structured_output_with_tools` for anthropic_messages because qcg_response must be selected exclusively",
                self.id
            ));
        }
        if self.capabilities.stop_sequences && self.api == ApiFlavor::Responses {
            return Err(format!(
                "provider `{}` may not advertise `stop_sequences` for the Responses API",
                self.id
            ));
        }
        if self.capabilities.verbosity && self.api != ApiFlavor::Responses {
            return Err(format!(
                "provider `{}` may only advertise `verbosity` for the Responses API",
                self.id
            ));
        }
        if (self.capabilities.tool_choice || self.capabilities.parallel_tool_calls)
            && !self.capabilities.tool_use
        {
            return Err(format!(
                "provider `{}` may not advertise tool selection controls without `tool_use`",
                self.id
            ));
        }
        for (index, effort) in self.capabilities.reasoning_effort.iter().enumerate() {
            if self.capabilities.reasoning_effort[index + 1..].contains(effort) {
                return Err(format!(
                    "provider `{}` advertises duplicate reasoning_effort `{effort}`",
                    self.id
                ));
            }
        }
        if self.chat_token_limit_field.is_some() && self.api != ApiFlavor::ChatCompletions {
            return Err(format!(
                "provider `{}` may only set `chat_token_limit_field` for chat_completions",
                self.id
            ));
        }
        if self.api == ApiFlavor::ChatCompletions
            && !self.capabilities.reasoning_effort.is_empty()
            && self.chat_token_limit_field != Some(ChatTokenLimitField::MaxCompletionTokens)
        {
            return Err(format!(
                "provider `{}` must set `chat_token_limit_field = \"max_completion_tokens\"` when reasoning_effort is enabled",
                self.id
            ));
        }
        if let Some(base_url) = self.base_url.as_deref() {
            if let Some(name) = credential_placeholder(base_url, credential_source_env) {
                return Err(format!(
                    "provider `{}` base_url must not interpolate credential environment variable `{name}`",
                    self.id
                ));
            }
            let validation_url = normalize_url_placeholders(base_url);
            validate_base_url(&validation_url, has_credential).map_err(|error| {
                format!("provider `{}` has an invalid `base_url`: {error}", self.id)
            })?;
        }
        validate_query_parameters(&self.query, credential_source_env)
            .map_err(|error| format!("provider `{}` has an invalid `query`: {error}", self.id))?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultSection {
    #[serde(default)]
    pub model: Option<ModelSelection>,
    #[serde(default)]
    pub search: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvidersFile {
    #[serde(default)]
    pub default: Option<DefaultSection>,
    #[serde(default)]
    pub provider: Vec<ProviderSpec>,
    #[serde(default)]
    pub search_provider: Vec<SearchProviderSpec>,
    #[serde(default)]
    pub mcp_server: Vec<qcg_mcp::McpServerSpec>,
}

impl ProvidersFile {
    pub fn parse(text: &str) -> Result<Self, String> {
        if text.len() > MAX_PROVIDERS_FILE_BYTES {
            return Err(format!(
                "providers registry exceeds {MAX_PROVIDERS_FILE_BYTES} bytes"
            ));
        }
        let file: ProvidersFile =
            toml::from_str(text).map_err(|error| format!("invalid providers registry: {error}"))?;
        file.validate()?;
        Ok(file)
    }

    fn validate(&self) -> Result<(), String> {
        for (kind, count) in [
            ("provider", self.provider.len()),
            ("search_provider", self.search_provider.len()),
            ("mcp_server", self.mcp_server.len()),
        ] {
            if count > MAX_PROVIDER_ENTRIES_PER_KIND {
                return Err(format!(
                    "providers registry contains more than {MAX_PROVIDER_ENTRIES_PER_KIND} {kind} entries"
                ));
            }
        }
        let total = self
            .provider
            .len()
            .checked_add(self.search_provider.len())
            .and_then(|count| count.checked_add(self.mcp_server.len()))
            .ok_or_else(|| "providers registry entry count overflowed".to_string())?;
        if total > MAX_PROVIDER_ENTRIES_TOTAL {
            return Err(format!(
                "providers registry contains more than {MAX_PROVIDER_ENTRIES_TOTAL} total entries"
            ));
        }
        let mut seen = BTreeMap::new();
        for spec in &self.provider {
            spec.validate()?;
            if seen.insert(spec.id.as_str(), ()).is_some() {
                return Err(format!("duplicate provider id `{}`", spec.id));
            }
        }
        let mut search_seen = BTreeMap::new();
        for spec in &self.search_provider {
            spec.validate()?;
            if search_seen.insert(spec.id.as_str(), ()).is_some() {
                return Err(format!("duplicate search provider id `{}`", spec.id));
            }
        }
        let mut mcp_seen = BTreeMap::new();
        for spec in &self.mcp_server {
            spec.validate()?;
            if mcp_seen.insert(spec.id.as_str(), ()).is_some() {
                return Err(format!("duplicate MCP server id `{}`", spec.id));
            }
        }
        if let Some(default) = &self.default
            && let Some(model) = &default.model
        {
            if !is_provider_id(&model.provider) || model.model.trim().is_empty() {
                return Err(
                    "[default].model provider and model must contain valid non-empty identifiers"
                        .into(),
                );
            }
            if model.provider != "fake" && !seen.contains_key(model.provider.as_str()) {
                return Err(format!(
                    "[default].model references unregistered provider `{}`",
                    model.provider
                ));
            }
        }
        if let Some(default) = &self.default
            && let Some(search) = default.search.as_deref()
        {
            if !is_provider_id(search) {
                return Err("[default].search must be a valid non-empty provider id".into());
            }
            if !search_seen.contains_key(search) {
                return Err(format!(
                    "[default].search references unregistered search provider `{search}`"
                ));
            }
        }
        Ok(())
    }
}

pub struct FakeLlmProvider;

#[async_trait]
impl LlmProvider for FakeLlmProvider {
    fn id(&self) -> &str {
        "fake"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            tool_use: true,
            json_schema: true,
            structured_output_with_tools: true,
            seed: true,
            image_input: true,
            audio_input: true,
            file_input: true,
            streaming: true,
            temperature: true,
            top_p: true,
            stop_sequences: true,
            tool_choice: true,
            parallel_tool_calls: true,
            verbosity: true,
            reasoning_effort: vec![
                ReasoningEffort::None,
                ReasoningEffort::Minimal,
                ReasoningEffort::Low,
                ReasoningEffort::Medium,
                ReasoningEffort::High,
                ReasoningEffort::Xhigh,
                ReasoningEffort::Max,
            ],
        }
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
        validate_chat_request(&req, ApiFlavor::ChatCompletions)?;
        let prompt = req
            .messages
            .last()
            .map(|message| message.content.as_str())
            .unwrap_or_default();
        if req.messages.last().map(|message| message.role.as_str()) == Some("tool") {
            let first_prompt = req
                .messages
                .iter()
                .find(|message| message.role == "user")
                .map(|message| message.content.as_str())
                .unwrap_or_default();
            if let Some(sequence) = marker_line(first_prompt, "FAKE_TOOL_SEQUENCE:")
                && let Ok(values) = serde_json::from_str::<Vec<Value>>(&sequence)
            {
                let completed_tools = req
                    .messages
                    .iter()
                    .filter(|message| message.role == "tool")
                    .count();
                if let Some(value) = values.get(completed_tools) {
                    let name = value
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let args = value.get("args").cloned().unwrap_or_else(|| json!({}));
                    return Ok(ChatResponse {
                        content: vec![ChatContent::ToolCall {
                            id: format!("fake-tool-{}", completed_tools + 1),
                            name,
                            args,
                        }],
                        usage: TokenUsage {
                            input: first_prompt.len() as u64,
                            output: 0,
                            reasoning: 0,
                        },
                        stop: StopReason::ToolUse,
                        provider_state: None,
                    });
                }
            }
            let text = marker_block(first_prompt, "FAKE_AGENT_FINAL:")
                .unwrap_or_else(|| "agent finished".into());
            return Ok(ChatResponse {
                content: vec![ChatContent::Text(text)],
                usage: TokenUsage {
                    input: first_prompt.len() as u64,
                    output: 0,
                    reasoning: 0,
                },
                stop: StopReason::EndTurn,
                provider_state: None,
            });
        }
        if let Some(tool) = marker_line(prompt, "FAKE_TOOL:")
            && let Ok(value) = serde_json::from_str::<Value>(&tool)
        {
            let name = value
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let args = value.get("args").cloned().unwrap_or_else(|| json!({}));
            return Ok(ChatResponse {
                content: vec![ChatContent::ToolCall {
                    id: "fake-tool-1".into(),
                    name,
                    args,
                }],
                usage: TokenUsage {
                    input: prompt.len() as u64,
                    output: 0,
                    reasoning: 0,
                },
                stop: StopReason::ToolUse,
                provider_state: None,
            });
        }
        if let Some(sequence) = marker_line(prompt, "FAKE_TOOL_SEQUENCE:")
            && let Ok(values) = serde_json::from_str::<Vec<Value>>(&sequence)
            && let Some(value) = values.first()
        {
            let name = value
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let args = value.get("args").cloned().unwrap_or_else(|| json!({}));
            return Ok(ChatResponse {
                content: vec![ChatContent::ToolCall {
                    id: "fake-tool-1".into(),
                    name,
                    args,
                }],
                usage: TokenUsage {
                    input: prompt.len() as u64,
                    output: 0,
                    reasoning: 0,
                },
                stop: StopReason::ToolUse,
                provider_state: None,
            });
        }
        let text = if let Some(sequence) = marker_line(prompt, "FAKE_JSON_SEQUENCE:")
            && let Ok(values) = serde_json::from_str::<Vec<Value>>(&sequence)
        {
            let attempt = marker_line(prompt, "QCG_RETRY_ATTEMPT:")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            let value = values
                .get(attempt)
                .or_else(|| values.last())
                .cloned()
                .unwrap_or_else(|| json!({}));
            match value {
                Value::String(text) => text,
                other => other.to_string(),
            }
        } else if let Some(choice) = marker_line(prompt, "FAKE_CHOICE:") {
            choice
        } else if let Some(json) = marker_line(prompt, "FAKE_JSON:") {
            json
        } else if let Some(text) = marker_block(prompt, "FAKE_TEXT:") {
            text
        } else if let Some(options) = marker_line(prompt, "QCG_OPTIONS:")
            && let Ok(values) = serde_json::from_str::<Vec<String>>(&options)
        {
            values.first().cloned().unwrap_or_default()
        } else if let Some(schema) = req.response_schema.as_ref() {
            schema
                .get("default")
                .cloned()
                .unwrap_or_else(|| json!({}))
                .to_string()
        } else {
            prompt.to_string()
        };
        Ok(ChatResponse {
            content: vec![ChatContent::Text(text)],
            usage: TokenUsage {
                input: prompt.len() as u64,
                output: 0,
                reasoning: 0,
            },
            stop: StopReason::EndTurn,
            provider_state: None,
        })
    }
}

fn is_env_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().enumerate().all(|(index, character)| {
            character.is_ascii_uppercase()
                || character == '_'
                || (index > 0 && character.is_ascii_digit())
        })
}

fn is_provider_id(id: &str) -> bool {
    !id.is_empty()
        && id.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '_' | '-')
        })
}

fn interpolate_env(value: &str) -> Result<String, String> {
    let mut result = String::new();
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        let Some(close_offset) = rest[start..].find('}') else {
            return Err("unclosed `{` environment placeholder".into());
        };
        let end = start + close_offset;
        let name = &rest[start + 1..end];
        if !is_env_name(name) {
            return Err(format!("invalid environment placeholder `{{{name}}}`"));
        }
        match std::env::var(name) {
            Ok(value) => {
                result.push_str(&rest[..start]);
                result.push_str(&value);
            }
            Err(_) => {
                return Err(format!("set `{name}` before running the generator"));
            }
        }
        rest = &rest[end + 1..];
    }
    result.push_str(rest);
    Ok(result)
}

fn normalize_url_placeholders(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        normalized.push_str(&rest[..start]);
        let Some(close_offset) = rest[start..].find('}') else {
            normalized.push_str(&rest[start..]);
            return normalized;
        };
        normalized.push_str("placeholder");
        rest = &rest[start + close_offset + 1..];
    }
    normalized.push_str(rest);
    normalized
}

fn credential_placeholder(value: &str, credential_env: Option<&str>) -> Option<String> {
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        let close_offset = rest[start..].find('}')?;
        let name = &rest[start + 1..start + close_offset];
        if credential_env.is_some_and(|credential_env| name == credential_env)
            || credential_like_name(name)
        {
            return Some(name.to_owned());
        }
        rest = &rest[start + close_offset + 1..];
    }
    None
}

fn validate_query_parameters(
    query: &BTreeMap<String, String>,
    credential_env: Option<&str>,
) -> Result<(), String> {
    for (key, value) in query {
        if credential_env.is_some_and(|credential_env| key == credential_env)
            || credential_like_name(key)
        {
            return Err(format!(
                "query parameter `{key}` must not carry credentials"
            ));
        }
        if let Some(name) = credential_placeholder(value, credential_env) {
            return Err(format!(
                "query parameter `{key}` must not interpolate credential environment variable `{name}`"
            ));
        }
    }
    Ok(())
}

fn validate_base_url(raw: &str, requires_credential: bool) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|_| "base_url is not a valid URL".to_owned())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("base_url must use the http or https scheme".into());
    }
    if url.host_str().is_none() {
        return Err("base_url must include a host".into());
    }
    if has_url_userinfo(&url) {
        return Err("base_url must not include userinfo".into());
    }
    if url.query().is_some() {
        return Err("base_url must not include a query".into());
    }
    if url.fragment().is_some() {
        return Err("base_url must not include a fragment".into());
    }
    if requires_credential && url.scheme() == "http" && !is_loopback_host(&url) {
        return Err("credentialed http base_url is only permitted for loopback hosts".into());
    }
    Ok(url)
}

fn read_credential_file(path: &Path) -> Result<String, String> {
    if !path.is_absolute() {
        return Err("path must be absolute".into());
    }
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect `{}`: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("path must be a regular non-symlink file".into());
    }
    if metadata.len() > MAX_CREDENTIAL_FILE_BYTES {
        return Err(format!("file exceeds {MAX_CREDENTIAL_FILE_BYTES} bytes"));
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("file permissions must not grant group or other access".into());
        }
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        options
            .open(path)
            .map_err(|error| format!("cannot open safely: {error}"))?
    };
    #[cfg(not(unix))]
    let file = File::open(path).map_err(|error| format!("cannot open: {error}"))?;

    read_open_credential_file(file)
}

fn read_open_credential_file(file: File) -> Result<String, String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("cannot inspect after opening: {error}"))?;
    if !metadata.is_file() || metadata.len() > MAX_CREDENTIAL_FILE_BYTES {
        return Err(format!(
            "file must be regular and no larger than {MAX_CREDENTIAL_FILE_BYTES} bytes"
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_CREDENTIAL_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read: {error}"))?;
    if bytes.len() as u64 > MAX_CREDENTIAL_FILE_BYTES {
        return Err(format!("file exceeds {MAX_CREDENTIAL_FILE_BYTES} bytes"));
    }
    let value = String::from_utf8(bytes).map_err(|_| "file is not valid UTF-8".to_owned())?;
    let value = value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(&value)
        .to_owned();
    if value.is_empty() {
        return Err("file is empty".into());
    }
    Ok(value)
}

fn has_url_userinfo(url: &Url) -> bool {
    if !url.username().is_empty() || url.password().is_some() {
        return true;
    }

    // `Url::username` cannot distinguish a missing username from an empty
    // userinfo component (`https://@example.test`). Inspect only the
    // authority portion so an `@` in the path does not count as userinfo.
    url.as_str()
        .split_once("://")
        .and_then(|(_, rest)| rest.split(['/', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'))
}

fn is_loopback_host(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    host.trim_end_matches('.').eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

pub struct HttpProvider {
    id: String,
    api: ApiFlavor,
    base_url: Option<Url>,
    auth_header: Option<String>,
    credential_env: Option<String>,
    credential_file_env: Option<String>,
    capabilities: Capabilities,
    path_template: Option<String>,
    query: BTreeMap<String, String>,
    timeout_seconds: u64,
    chat_token_limit_field: ChatTokenLimitField,
    response_body_limit_bytes: usize,
    config_errors: Vec<String>,
    client: Option<Client>,
    concurrency: Option<Arc<Semaphore>>,
    requests_per_minute: Option<usize>,
    rate_window: Arc<Mutex<VecDeque<Instant>>>,
    circuit: Arc<Mutex<CircuitState>>,
    circuit_breaker_failures: usize,
    circuit_breaker_cooldown: Duration,
}

#[derive(Default)]
struct CircuitState {
    consecutive_failures: usize,
    open_until: Option<Instant>,
}

impl HttpProvider {
    fn from_spec(spec: ProviderSpec) -> Self {
        let mut config_errors = Vec::new();
        let has_credential = spec.api_key_env.is_some() || spec.api_key_file_env.is_some();
        let credential_source_env = spec
            .api_key_env
            .as_deref()
            .or(spec.api_key_file_env.as_deref());
        let raw_base_url = match spec.base_url_env.as_deref() {
            Some(name) => match std::env::var(name) {
                Ok(value) => Some(value),
                Err(std::env::VarError::NotPresent) => spec.base_url.clone(),
                Err(std::env::VarError::NotUnicode(_)) => {
                    config_errors.push(format!("environment variable `{name}` is not valid UTF-8"));
                    None
                }
            },
            None => spec.base_url.clone(),
        };
        let base_url = match raw_base_url {
            Some(raw) => match credential_placeholder(&raw, credential_source_env) {
                Some(name) => {
                    config_errors.push(format!(
                        "provider `{}` base_url must not interpolate credential environment variable `{name}`",
                        spec.id
                    ));
                    None
                }
                None => match interpolate_env(&raw) {
                    Ok(resolved) => match validate_base_url(&resolved, has_credential) {
                        Ok(url) => Some(url),
                        Err(error) => {
                            config_errors.push(format!(
                                "provider `{}` has an invalid base_url: {error}",
                                spec.id
                            ));
                            None
                        }
                    },
                    Err(error) => {
                        config_errors.push(format!(
                            "provider `{}` has an invalid base_url: {error}",
                            spec.id
                        ));
                        None
                    }
                },
            },
            None => {
                let source = spec.base_url_env.as_deref().unwrap_or("base_url");
                config_errors.push(format!("set `{source}` before running the generator"));
                None
            }
        };
        let mut query = BTreeMap::new();
        let query_is_safe = match validate_query_parameters(&spec.query, credential_source_env) {
            Ok(()) => true,
            Err(error) => {
                config_errors.push(format!(
                    "provider `{}` has an invalid query: {error}",
                    spec.id
                ));
                false
            }
        };
        for (key, value) in spec.query {
            if !query_is_safe {
                continue;
            }
            match interpolate_env(&value) {
                Ok(resolved) => {
                    query.insert(key, resolved);
                }
                Err(error) => config_errors.push(error),
            }
        }
        let client = match Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
        {
            Ok(client) => Some(client),
            Err(error) => {
                config_errors.push(format!(
                    "provider `{}` could not initialize its HTTP client: {error}",
                    spec.id
                ));
                None
            }
        };
        Self {
            id: spec.id,
            api: spec.api,
            base_url,
            auth_header: spec.auth_header,
            credential_env: spec.api_key_env,
            credential_file_env: spec.api_key_file_env,
            capabilities: spec.capabilities,
            path_template: spec.path_template,
            query,
            timeout_seconds: spec.timeout_seconds.unwrap_or(120),
            chat_token_limit_field: spec
                .chat_token_limit_field
                .unwrap_or(ChatTokenLimitField::MaxTokens),
            response_body_limit_bytes: spec
                .response_body_limit_bytes
                .unwrap_or(DEFAULT_RESPONSE_BODY_LIMIT_BYTES),
            config_errors,
            client,
            concurrency: spec
                .max_concurrency
                .map(|limit| Arc::new(Semaphore::new(limit))),
            requests_per_minute: spec.requests_per_minute,
            rate_window: Arc::new(Mutex::new(VecDeque::new())),
            circuit: Arc::new(Mutex::new(CircuitState::default())),
            circuit_breaker_failures: spec.circuit_breaker_failures.unwrap_or(5),
            circuit_breaker_cooldown: Duration::from_secs(
                spec.circuit_breaker_cooldown_seconds.unwrap_or(30),
            ),
        }
    }

    fn default_path(api: ApiFlavor) -> &'static str {
        match api {
            ApiFlavor::ChatCompletions => "chat/completions",
            ApiFlavor::Responses => "responses",
            ApiFlavor::AnthropicMessages => "messages",
        }
    }

    fn endpoint_for(&self, model: &str) -> Result<Url, String> {
        let mut url = self
            .base_url
            .clone()
            .ok_or_else(|| "provider base_url is not configured".to_owned())?;
        let path = self
            .path_template
            .as_deref()
            .unwrap_or(Self::default_path(self.api))
            .to_owned();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| "provider base_url cannot be used as a base URL".to_owned())?;
            segments.pop_if_empty();
            for segment in path
                .trim_matches('/')
                .split('/')
                .filter(|segment| !segment.is_empty())
            {
                segments.push(&segment.replace("{model}", model));
            }
        }
        if !self.query.is_empty() {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in &self.query {
                pairs.append_pair(key, value);
            }
        }
        Ok(url)
    }

    fn credential_for_request(&self) -> Result<Option<String>, LlmError> {
        match (
            self.credential_env.as_deref(),
            self.credential_file_env.as_deref(),
        ) {
            (Some(name), None) => match std::env::var(name) {
                Ok(value) if !value.is_empty() => Ok(Some(value)),
                Ok(_) | Err(_) => Err(LlmError::new(format!(
                    "set `{name}` before running the generator"
                ))),
            },
            (None, Some(name)) => {
                let path = std::env::var(name).map_err(|_| {
                    LlmError::new(format!("set `{name}` before running the generator"))
                })?;
                read_credential_file(Path::new(&path))
                    .map(Some)
                    .map_err(|error| {
                        LlmError::new(format!("credential file from `{name}` is invalid: {error}"))
                    })
            }
            (None, None) => Ok(None),
            (Some(_), Some(_)) => Err(LlmError::new(
                "provider declares multiple credential sources",
            )),
        }
    }

    fn credential_configuration_error(&self) -> Option<String> {
        self.credential_for_request()
            .err()
            .map(|error| error.message)
    }

    async fn acquire_request_slot(&self) -> Result<Option<SemaphorePermit<'_>>, LlmError> {
        {
            let mut circuit = self.circuit.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(until) = circuit.open_until {
                if until > Instant::now() {
                    return Err(LlmError {
                        message: format!("{} provider circuit breaker is open", self.id),
                        kind: LlmErrorKind::CircuitOpen,
                    });
                }
                circuit.open_until = None;
                circuit.consecutive_failures = 0;
            }
        }
        if let Some(limit) = self.requests_per_minute {
            loop {
                let delay = {
                    let mut window = self
                        .rate_window
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    let now = Instant::now();
                    while window.front().is_some_and(|started| {
                        now.duration_since(*started) >= Duration::from_secs(60)
                    }) {
                        window.pop_front();
                    }
                    if window.len() < limit {
                        window.push_back(now);
                        None
                    } else {
                        window.front().map(|started| {
                            Duration::from_secs(60).saturating_sub(now.duration_since(*started))
                        })
                    }
                };
                match delay {
                    Some(delay) => sleep(delay).await,
                    None => break,
                }
            }
        }
        match &self.concurrency {
            Some(semaphore) => semaphore
                .acquire()
                .await
                .map(Some)
                .map_err(|_| LlmError::new("provider concurrency limiter closed")),
            None => Ok(None),
        }
    }

    fn record_request_result<T>(&self, result: &Result<T, LlmError>) {
        let mut circuit = self.circuit.lock().unwrap_or_else(PoisonError::into_inner);
        match result {
            Ok(_) => {
                circuit.consecutive_failures = 0;
                circuit.open_until = None;
            }
            Err(error) if error.is_retryable() && error.kind != LlmErrorKind::CircuitOpen => {
                circuit.consecutive_failures = circuit.consecutive_failures.saturating_add(1);
                if circuit.consecutive_failures >= self.circuit_breaker_failures {
                    circuit.open_until = Some(Instant::now() + self.circuit_breaker_cooldown);
                }
            }
            Err(_) => {}
        }
    }

    async fn send(&self, payload: Value, model: &str) -> Result<ChatResponse, LlmError> {
        if let Some(error) = self.configuration_error_for(&self.id) {
            return Err(LlmError::new(error));
        }
        let credential = self.credential_for_request()?;
        let client = self.client.as_ref().ok_or_else(|| {
            LlmError::new(format!("{} provider HTTP client is unavailable", self.id))
        })?;
        let endpoint = self.endpoint_for(model).map_err(|error| {
            LlmError::new(format!(
                "provider `{}` has an invalid endpoint: {error}",
                self.id
            ))
        })?;
        let mut builder = client.post(endpoint).json(&payload);
        if let Some(key) = credential.as_deref() {
            builder = match &self.auth_header {
                Some(header) => builder.header(header.as_str(), key),
                None => builder.bearer_auth(key),
            };
        }
        if self.api == ApiFlavor::AnthropicMessages {
            builder = builder.header("anthropic-version", "2023-06-01");
        }
        let mut response = builder.send().await.map_err(llm_http_error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(LlmError::http_status(
                status.as_u16(),
                format!("{} provider returned HTTP {}", self.id, status.as_u16()),
            ));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(llm_http_error)? {
            let next_len = body.len().checked_add(chunk.len()).ok_or_else(|| {
                LlmError::invalid_response(format!(
                    "{} provider response exceeded the configured body limit",
                    self.id
                ))
            })?;
            if next_len > self.response_body_limit_bytes {
                return Err(LlmError::invalid_response(format!(
                    "{} provider response exceeded response_body_limit_bytes ({})",
                    self.id, self.response_body_limit_bytes
                )));
            }
            body.extend_from_slice(&chunk);
        }
        let body = String::from_utf8(body).map_err(|_| {
            LlmError::invalid_response(format!(
                "{} provider returned a response body that was not UTF-8",
                self.id
            ))
        })?;
        if body.trim().is_empty() {
            return Err(LlmError::empty_response(format!(
                "{} provider returned an empty response body",
                self.id
            )));
        }
        if credential
            .as_deref()
            .is_some_and(|key| !key.is_empty() && body.contains(key))
        {
            return Err(LlmError::new(format!(
                "{} provider response contained its configured credential",
                self.id
            )));
        }
        let value: Value = serde_json::from_str(&body).map_err(|error| {
            LlmError::invalid_response(format!(
                "{} provider returned invalid JSON: {error}",
                self.id
            ))
        })?;
        if credential
            .as_deref()
            .is_some_and(|key| !key.is_empty() && json_contains_string_fragment(&value, key))
        {
            return Err(LlmError::new(format!(
                "{} provider response contained its configured credential",
                self.id
            )));
        }
        match self.api {
            ApiFlavor::ChatCompletions => parse_chat_completions_response(value),
            ApiFlavor::Responses => parse_responses_response(value),
            ApiFlavor::AnthropicMessages => parse_anthropic_response(value),
        }
    }

    async fn send_stream(
        &self,
        payload: Value,
        model: &str,
        events: mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        if let Some(error) = self.configuration_error_for(&self.id) {
            return Err(LlmError::new(error));
        }
        let credential = self.credential_for_request()?;
        let client = self.client.as_ref().ok_or_else(|| {
            LlmError::new(format!("{} provider HTTP client is unavailable", self.id))
        })?;
        let endpoint = self.endpoint_for(model).map_err(|error| {
            LlmError::new(format!(
                "provider `{}` has an invalid endpoint: {error}",
                self.id
            ))
        })?;
        let mut builder = client.post(endpoint).json(&payload);
        if let Some(key) = credential.as_deref() {
            builder = match &self.auth_header {
                Some(header) => builder.header(header.as_str(), key),
                None => builder.bearer_auth(key),
            };
        }
        if self.api == ApiFlavor::AnthropicMessages {
            builder = builder.header("anthropic-version", "2023-06-01");
        }
        let response = builder.send().await.map_err(llm_http_error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(LlmError::http_status(
                status.as_u16(),
                format!("{} provider returned HTTP {}", self.id, status.as_u16()),
            ));
        }
        let mut stream = SseStream::from_bytes_stream(response.bytes_stream());
        let mut total_bytes = 0_usize;
        let mut accumulator = HttpStreamAccumulator::new(self.api);
        while let Some(event) = stream.next().await {
            let event = event.map_err(|_| {
                LlmError::invalid_response(format!("{} provider returned invalid SSE", self.id))
            })?;
            let Some(data) = event.data else {
                continue;
            };
            if data.trim() == "[DONE]" {
                break;
            }
            total_bytes = total_bytes.checked_add(data.len()).ok_or_else(|| {
                LlmError::invalid_response(format!(
                    "{} provider stream exceeded the configured body limit",
                    self.id
                ))
            })?;
            if total_bytes > self.response_body_limit_bytes {
                return Err(LlmError::invalid_response(format!(
                    "{} provider stream exceeded response_body_limit_bytes ({})",
                    self.id, self.response_body_limit_bytes
                )));
            }
            if credential
                .as_deref()
                .is_some_and(|key| !key.is_empty() && data.contains(key))
            {
                return Err(LlmError::new(format!(
                    "{} provider stream contained its configured credential",
                    self.id
                )));
            }
            let value: Value = serde_json::from_str(&data).map_err(|_| {
                LlmError::invalid_response(format!(
                    "{} provider returned invalid JSON in its SSE stream",
                    self.id
                ))
            })?;
            if value
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| {
                    kind == "error" || kind.ends_with(".failed") || kind.ends_with(".error")
                })
            {
                return Err(LlmError::new(format!(
                    "{} provider stream reported an error",
                    self.id
                )));
            }
            if let Some(response) = accumulator.ingest(value, &events).await? {
                events
                    .send(ChatStreamEvent::Completed { response })
                    .await
                    .map_err(|_| LlmError::new("LLM stream receiver closed"))?;
                return Ok(());
            }
        }
        let response = accumulator.finish()?;
        events
            .send(ChatStreamEvent::Completed { response })
            .await
            .map_err(|_| LlmError::new("LLM stream receiver closed"))
    }
}

enum HttpStreamAccumulator {
    Chat(ChatCompletionAccumulator),
    Responses,
    Anthropic(AnthropicAccumulator),
}

impl HttpStreamAccumulator {
    fn new(api: ApiFlavor) -> Self {
        match api {
            ApiFlavor::ChatCompletions => Self::Chat(ChatCompletionAccumulator::default()),
            ApiFlavor::Responses => Self::Responses,
            ApiFlavor::AnthropicMessages => Self::Anthropic(AnthropicAccumulator::default()),
        }
    }

    async fn ingest(
        &mut self,
        value: Value,
        events: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<Option<ChatResponse>, LlmError> {
        match self {
            Self::Chat(accumulator) => {
                accumulator.ingest(&value, events).await?;
                Ok(None)
            }
            Self::Responses => ingest_responses_stream(value, events).await,
            Self::Anthropic(accumulator) => {
                accumulator.ingest(&value, events).await?;
                Ok(None)
            }
        }
    }

    fn finish(self) -> Result<ChatResponse, LlmError> {
        match self {
            Self::Chat(accumulator) => accumulator.finish(),
            Self::Responses => Err(LlmError::invalid_response(
                "Responses API stream ended without response.completed",
            )),
            Self::Anthropic(accumulator) => accumulator.finish(),
        }
    }
}

#[derive(Default)]
struct PartialToolCall {
    id: String,
    name: String,
    arguments: String,
}

#[derive(Default)]
struct ChatCompletionAccumulator {
    text: String,
    tool_calls: BTreeMap<usize, PartialToolCall>,
    usage: Option<TokenUsage>,
    stop: Option<StopReason>,
}

impl ChatCompletionAccumulator {
    async fn ingest(
        &mut self,
        value: &Value,
        events: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        if let Some(usage) = value.get("usage").filter(|usage| !usage.is_null()) {
            self.usage = Some(TokenUsage {
                input: usage
                    .get("prompt_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                output: usage
                    .get("completion_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                reasoning: usage
                    .pointer("/completion_tokens_details/reasoning_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
            });
        }
        let Some(choice) = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return Ok(());
        };
        let delta = choice.get("delta").unwrap_or(&Value::Null);
        for text in [delta.get("content"), delta.get("refusal")]
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            self.text.push_str(text);
            events
                .send(ChatStreamEvent::TextDelta {
                    text: text.to_string(),
                })
                .await
                .map_err(|_| LlmError::new("LLM stream receiver closed"))?;
        }
        for call in delta
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let index = call
                .get("index")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize;
            let partial = self.tool_calls.entry(index).or_default();
            if let Some(id) = call.get("id").and_then(Value::as_str) {
                partial.id.push_str(id);
            }
            if let Some(function) = call.get("function") {
                if let Some(name) = function.get("name").and_then(Value::as_str) {
                    partial.name.push_str(name);
                }
                if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                    partial.arguments.push_str(arguments);
                }
            }
        }
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            self.stop = Some(parse_openai_stop_reason(reason)?);
        }
        Ok(())
    }

    fn finish(self) -> Result<ChatResponse, LlmError> {
        let mut content = Vec::new();
        if !self.text.is_empty() {
            content.push(ChatContent::Text(self.text));
        }
        for (_, call) in self.tool_calls {
            if call.id.is_empty() || call.name.is_empty() {
                return Err(LlmError::invalid_response(
                    "OpenAI-compatible stream returned an incomplete tool call",
                ));
            }
            let args = serde_json::from_str(&call.arguments).map_err(|_| {
                LlmError::invalid_response(
                    "OpenAI-compatible stream returned invalid tool arguments",
                )
            })?;
            content.push(ChatContent::ToolCall {
                id: call.id,
                name: call.name,
                args,
            });
        }
        if content.is_empty() {
            return Err(LlmError::invalid_response(
                "OpenAI-compatible stream did not include text or tool calls",
            ));
        }
        Ok(ChatResponse {
            content,
            usage: self.usage.ok_or_else(|| {
                LlmError::invalid_response("OpenAI-compatible stream did not include usage")
            })?,
            stop: self.stop.ok_or_else(|| {
                LlmError::invalid_response("OpenAI-compatible stream did not include finish_reason")
            })?,
            provider_state: None,
        })
    }
}

fn parse_openai_stop_reason(reason: &str) -> Result<StopReason, LlmError> {
    match reason {
        "stop" => Ok(StopReason::EndTurn),
        "tool_calls" => Ok(StopReason::ToolUse),
        "length" => Ok(StopReason::MaxTokens),
        "content_filter" => Ok(StopReason::Refusal),
        _ => Err(LlmError::invalid_response(
            "OpenAI-compatible stream returned an unknown finish_reason",
        )),
    }
}

async fn ingest_responses_stream(
    value: Value,
    events: &mpsc::Sender<ChatStreamEvent>,
) -> Result<Option<ChatResponse>, LlmError> {
    match value.get("type").and_then(Value::as_str) {
        Some("response.output_text.delta") | Some("response.refusal.delta") => {
            if let Some(text) = value.get("delta").and_then(Value::as_str)
                && !text.is_empty()
            {
                events
                    .send(ChatStreamEvent::TextDelta {
                        text: text.to_string(),
                    })
                    .await
                    .map_err(|_| LlmError::new("LLM stream receiver closed"))?;
            }
            Ok(None)
        }
        Some("response.completed") => value
            .get("response")
            .cloned()
            .ok_or_else(|| {
                LlmError::invalid_response("response.completed did not include response")
            })
            .and_then(parse_responses_response)
            .map(Some),
        _ => Ok(None),
    }
}

#[derive(Default)]
struct AnthropicAccumulator {
    text: String,
    tool_calls: BTreeMap<usize, PartialToolCall>,
    input_tokens: u64,
    output_tokens: Option<u64>,
    stop: Option<StopReason>,
}

impl AnthropicAccumulator {
    async fn ingest(
        &mut self,
        value: &Value,
        events: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        match value.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                self.input_tokens = value
                    .pointer("/message/usage/input_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
            }
            Some("content_block_start") => {
                let index = value
                    .get("index")
                    .and_then(Value::as_u64)
                    .unwrap_or_default() as usize;
                let block = value.get("content_block").unwrap_or(&Value::Null);
                if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                    let partial = self.tool_calls.entry(index).or_default();
                    partial.id = block
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    partial.name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                }
            }
            Some("content_block_delta") => {
                let delta = value.get("delta").unwrap_or(&Value::Null);
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") | Some("refusal_delta") => {
                        if let Some(text) = delta.get("text").and_then(Value::as_str) {
                            self.text.push_str(text);
                            events
                                .send(ChatStreamEvent::TextDelta {
                                    text: text.to_string(),
                                })
                                .await
                                .map_err(|_| LlmError::new("LLM stream receiver closed"))?;
                        }
                    }
                    Some("input_json_delta") => {
                        let index = value
                            .get("index")
                            .and_then(Value::as_u64)
                            .unwrap_or_default() as usize;
                        if let Some(json) = delta.get("partial_json").and_then(Value::as_str) {
                            self.tool_calls
                                .entry(index)
                                .or_default()
                                .arguments
                                .push_str(json);
                        }
                    }
                    _ => {}
                }
            }
            Some("message_delta") => {
                self.output_tokens = value
                    .pointer("/usage/output_tokens")
                    .and_then(Value::as_u64);
                self.stop = value
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                    .map(parse_anthropic_stop_reason)
                    .transpose()?;
            }
            _ => {}
        }
        Ok(())
    }

    fn finish(self) -> Result<ChatResponse, LlmError> {
        let mut content = Vec::new();
        let mut structured_response = false;
        if !self.text.is_empty() {
            content.push(ChatContent::Text(self.text));
        }
        for (_, call) in self.tool_calls {
            if call.id.is_empty() || call.name.is_empty() {
                return Err(LlmError::invalid_response(
                    "Anthropic stream returned an incomplete tool call",
                ));
            }
            let args: Value = serde_json::from_str(&call.arguments).map_err(|_| {
                LlmError::invalid_response("Anthropic stream returned invalid tool arguments")
            })?;
            if call.name == "qcg_response" {
                structured_response = true;
                content.push(ChatContent::Text(args.to_string()));
            } else {
                content.push(ChatContent::ToolCall {
                    id: call.id,
                    name: call.name,
                    args,
                });
            }
        }
        if content.is_empty() {
            return Err(LlmError::invalid_response(
                "Anthropic stream did not include text or tool calls",
            ));
        }
        if structured_response
            && content
                .iter()
                .any(|item| matches!(item, ChatContent::ToolCall { .. }))
        {
            return Err(LlmError::invalid_response(
                "Anthropic stream mixed qcg_response with external tool calls",
            ));
        }
        let stop = self.stop.ok_or_else(|| {
            LlmError::invalid_response("Anthropic stream did not include stop_reason")
        })?;
        let stop = if structured_response {
            if stop != StopReason::ToolUse {
                return Err(LlmError::invalid_response(
                    "Anthropic stream returned qcg_response without tool_use stop_reason",
                ));
            }
            StopReason::EndTurn
        } else {
            stop
        };
        Ok(ChatResponse {
            content,
            usage: TokenUsage {
                input: self.input_tokens,
                output: self.output_tokens.ok_or_else(|| {
                    LlmError::invalid_response("Anthropic stream did not include output usage")
                })?,
                reasoning: 0,
            },
            stop,
            provider_state: None,
        })
    }
}

fn parse_anthropic_stop_reason(reason: &str) -> Result<StopReason, LlmError> {
    match reason {
        "end_turn" | "stop_sequence" => Ok(StopReason::EndTurn),
        "tool_use" => Ok(StopReason::ToolUse),
        "max_tokens" => Ok(StopReason::MaxTokens),
        "refusal" => Ok(StopReason::Refusal),
        _ => Err(LlmError::invalid_response(
            "Anthropic stream returned an unknown stop_reason",
        )),
    }
}

fn json_contains_string_fragment(value: &Value, fragment: &str) -> bool {
    match value {
        Value::String(value) => value.contains(fragment),
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string_fragment(value, fragment)),
        Value::Object(values) => values.iter().any(|(key, value)| {
            key.contains(fragment) || json_contains_string_fragment(value, fragment)
        }),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

#[async_trait]
impl LlmProvider for HttpProvider {
    fn id(&self) -> &str {
        &self.id
    }

    fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }

    fn capabilities(&self) -> Capabilities {
        self.capabilities.clone()
    }

    fn configuration_error_for(&self, provider: &str) -> Option<String> {
        if provider != self.id() {
            return None;
        }
        let mut errors = self.config_errors.clone();
        if let Some(error) = self.credential_configuration_error() {
            errors.push(error);
        }
        (!errors.is_empty()).then(|| errors.join("; "))
    }

    fn credential_env_names(&self) -> Vec<String> {
        self.credential_env
            .iter()
            .chain(self.credential_file_env.iter())
            .cloned()
            .collect()
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
        validate_chat_request(&req, self.api)?;
        validate_multimodal_capabilities(&req, &self.capabilities)?;
        validate_structured_output_capabilities(&req, self.api, &self.capabilities)?;
        if req.seed.is_some() && !self.capabilities.seed {
            return Err(LlmError::new(format!(
                "{} provider does not support seed",
                self.id
            )));
        }
        if req
            .reasoning_effort
            .is_some_and(|effort| !self.capabilities.reasoning_effort.contains(&effort))
        {
            return Err(LlmError::new(format!(
                "{} provider does not support reasoning_effort `{}`",
                self.id,
                req.reasoning_effort.expect("checked reasoning_effort")
            )));
        }
        for (configured, supported, name) in [
            (
                req.temperature.is_some(),
                self.capabilities.temperature,
                "temperature",
            ),
            (req.top_p.is_some(), self.capabilities.top_p, "top_p"),
            (
                !req.stop_sequences.is_empty(),
                self.capabilities.stop_sequences,
                "stop_sequences",
            ),
            (
                req.tool_choice.is_some(),
                self.capabilities.tool_choice,
                "tool_choice",
            ),
            (
                req.parallel_tool_calls.is_some(),
                self.capabilities.parallel_tool_calls,
                "parallel_tool_calls",
            ),
            (
                req.verbosity.is_some(),
                self.capabilities.verbosity,
                "verbosity",
            ),
        ] {
            if configured && !supported {
                return Err(LlmError::new(format!(
                    "{} provider does not support {name}",
                    self.id
                )));
            }
        }
        let payload = match self.api {
            ApiFlavor::ChatCompletions => chat_completions_payload(
                &req,
                req.seed.is_some() && self.capabilities.seed,
                self.chat_token_limit_field,
            ),
            ApiFlavor::Responses => responses_payload(&req),
            ApiFlavor::AnthropicMessages => anthropic_payload(&req),
        };
        let _slot = self.acquire_request_slot().await?;
        let result = self.send(payload, &req.model).await;
        self.record_request_result(&result);
        result
    }

    async fn stream(
        &self,
        req: ChatRequest,
        events: mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        validate_chat_request(&req, self.api)?;
        validate_multimodal_capabilities(&req, &self.capabilities)?;
        validate_structured_output_capabilities(&req, self.api, &self.capabilities)?;
        if !self.capabilities.streaming {
            return Err(LlmError::new(format!(
                "{} provider does not support streaming",
                self.id
            )));
        }
        let mut payload = match self.api {
            ApiFlavor::ChatCompletions => chat_completions_payload(
                &req,
                req.seed.is_some() && self.capabilities.seed,
                self.chat_token_limit_field,
            ),
            ApiFlavor::Responses => responses_payload(&req),
            ApiFlavor::AnthropicMessages => anthropic_payload(&req),
        };
        payload["stream"] = Value::Bool(true);
        if self.api == ApiFlavor::ChatCompletions {
            payload["stream_options"] = json!({ "include_usage": true });
        }
        let _slot = self.acquire_request_slot().await?;
        let result = self.send_stream(payload, &req.model, events).await;
        self.record_request_result(&result);
        result
    }
}

fn validate_chat_request(req: &ChatRequest, api: ApiFlavor) -> Result<(), LlmError> {
    if req.max_tokens == 0 {
        return Err(LlmError::new("max_tokens must be greater than zero"));
    }
    if req
        .temperature
        .is_some_and(|value| !value.is_finite() || !(0.0..=2.0).contains(&value))
    {
        return Err(LlmError::new(
            "temperature must be finite and between 0 and 2",
        ));
    }
    if req
        .top_p
        .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
    {
        return Err(LlmError::new("top_p must be finite and between 0 and 1"));
    }
    if req.temperature.is_some() && req.top_p.is_some() {
        return Err(LlmError::new(
            "temperature and top_p are mutually exclusive",
        ));
    }
    if req.reasoning_effort.is_some() && (req.temperature.is_some() || req.top_p.is_some()) {
        return Err(LlmError::new(
            "temperature and top_p must be omitted when reasoning_effort is set",
        ));
    }
    if req.reasoning_effort.is_some() && req.seed.is_some() {
        return Err(LlmError::new(
            "seed must be omitted when reasoning_effort is set",
        ));
    }
    if req.stop_sequences.len() > 8
        || req
            .stop_sequences
            .iter()
            .any(|sequence| sequence.is_empty() || sequence.len() > 1_024)
    {
        return Err(LlmError::new(
            "stop_sequences must contain at most 8 non-empty strings of at most 1024 bytes",
        ));
    }
    if (req.tool_choice.is_some() || req.parallel_tool_calls.is_some()) && req.tools.is_empty() {
        return Err(LlmError::new(
            "tool_choice and parallel_tool_calls require at least one tool",
        ));
    }
    if let Some(ToolChoice::Tool { tool }) = &req.tool_choice
        && (tool.trim().is_empty() || !req.tools.iter().any(|candidate| candidate.name == *tool))
    {
        return Err(LlmError::new(
            "tool_choice.tool must name one of the request tools",
        ));
    }
    if api == ApiFlavor::AnthropicMessages
        && req.response_schema.is_some()
        && req.structured_output != StructuredOutputMode::Prompt
        && req
            .tool_choice
            .as_ref()
            .is_some_and(|choice| !matches!(choice, ToolChoice::Mode(ToolChoiceMode::Auto)))
    {
        return Err(LlmError::new(
            "Anthropic native structured output owns tool_choice",
        ));
    }
    if let Some(schema) = &req.response_schema {
        jsonschema::validator_for(schema)
            .map_err(|error| LlmError::new(format!("response_schema is invalid: {error}")))?;
        match req.structured_output {
            StructuredOutputMode::NativeStrict if !strict_schema_syntax_compatible(schema) => {
                return Err(LlmError::new(
                    "native_strict response_schema contains unsupported keywords or is not fully closed",
                ));
            }
            StructuredOutputMode::NativeCompatible if !native_schema_syntax_compatible(schema) => {
                return Err(LlmError::new(
                    "native_compatible response_schema contains unsupported keywords",
                ));
            }
            _ => {}
        }
    }
    let mut tool_names = std::collections::BTreeSet::new();
    for tool in &req.tools {
        if tool.name.trim().is_empty() {
            return Err(LlmError::new("tool name must not be empty"));
        }
        if tool.name == "qcg_response" {
            return Err(LlmError::new(
                "tool name `qcg_response` is reserved for structured output",
            ));
        }
        if !tool_names.insert(tool.name.as_str()) {
            return Err(LlmError::new(format!(
                "duplicate tool name `{}`",
                tool.name
            )));
        }
        validate_tool_input_schema(&tool.name, &tool.input_schema)?;
    }
    for message in &req.messages {
        if message.provider_state.is_some() && api != ApiFlavor::Responses {
            return Err(LlmError::new(
                "provider state messages are only valid for the Responses API",
            ));
        }
        if message.role == "tool" && message.tool_call_id.as_deref().is_none_or(str::is_empty) {
            return Err(LlmError::new("tool result is missing its tool call id"));
        }
        if !message.parts.is_empty() && message.role != "user" {
            return Err(LlmError::new(
                "multimodal content parts are only valid on user messages",
            ));
        }
        for part in &message.parts {
            let (media_type, data) = match part {
                ChatContentPart::Text { text } => {
                    if text.is_empty() {
                        return Err(LlmError::new("text content parts must not be empty"));
                    }
                    continue;
                }
                ChatContentPart::InputImage {
                    media_type, data, ..
                }
                | ChatContentPart::InputAudio { media_type, data }
                | ChatContentPart::InputFile {
                    media_type, data, ..
                } => (media_type, data),
            };
            if !valid_media_type(media_type) || data.is_empty() {
                return Err(LlmError::new(
                    "multimodal content requires a valid MIME type and non-empty base64 data",
                ));
            }
            if api == ApiFlavor::AnthropicMessages
                && matches!(part, ChatContentPart::InputAudio { .. })
            {
                return Err(LlmError::new(
                    "Anthropic Messages does not support audio input content parts",
                ));
            }
        }
        if !message.tool_calls.is_empty() {
            if message.role != "assistant" {
                return Err(LlmError::new(
                    "tool calls must be carried by an assistant message",
                ));
            }
            if message
                .tool_calls
                .iter()
                .any(|call| call.id.is_empty() || call.name.is_empty())
            {
                return Err(LlmError::new(
                    "assistant tool calls must include non-empty ids and names",
                ));
            }
        }
    }
    Ok(())
}

fn validate_tool_input_schema(name: &str, schema: &Value) -> Result<(), LlmError> {
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err(LlmError::new(format!(
            "tool `{name}` input_schema root must have type `object`"
        )));
    }
    let size = serde_json::to_vec(schema)
        .map_err(|error| LlmError::new(format!("tool `{name}` input_schema is invalid: {error}")))?
        .len();
    if size > MAX_TOOL_SCHEMA_BYTES {
        return Err(LlmError::new(format!(
            "tool `{name}` input_schema exceeds {MAX_TOOL_SCHEMA_BYTES} bytes"
        )));
    }
    let mut stack = vec![(schema, 1_usize)];
    let mut nodes = 0_usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_TOOL_SCHEMA_NODES || depth > MAX_TOOL_SCHEMA_DEPTH {
            return Err(LlmError::new(format!(
                "tool `{name}` input_schema exceeds complexity limits"
            )));
        }
        match value {
            Value::Object(object) => {
                if object.iter().any(|(key, value)| {
                    matches!(key.as_str(), "$ref" | "$dynamicRef" | "$recursiveRef")
                        && value
                            .as_str()
                            .is_some_and(|reference| !reference.starts_with('#'))
                }) {
                    return Err(LlmError::new(format!(
                        "tool `{name}` input_schema contains an external reference"
                    )));
                }
                stack.extend(object.values().map(|value| (value, depth + 1)));
            }
            Value::Array(array) => {
                stack.extend(array.iter().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    jsonschema::validator_for(schema).map_err(|error| {
        LlmError::new(format!("tool `{name}` input_schema is invalid: {error}"))
    })?;
    Ok(())
}

fn validate_structured_output_capabilities(
    req: &ChatRequest,
    api: ApiFlavor,
    capabilities: &Capabilities,
) -> Result<(), LlmError> {
    if req.response_schema.is_none() {
        return Ok(());
    }
    let uses_native = native_response_schema(req).is_some();
    if uses_native && !capabilities.json_schema {
        return Err(LlmError::new(
            "provider does not support native structured output",
        ));
    }
    if req.tools.is_empty() {
        return Ok(());
    }
    if uses_native && !capabilities.structured_output_with_tools {
        return Err(LlmError::new(
            "provider does not support native structured output with external tools",
        ));
    }
    if uses_native && api == ApiFlavor::AnthropicMessages {
        return Err(LlmError::new(
            "Anthropic Messages cannot force qcg_response while external tools are available",
        ));
    }
    Ok(())
}

fn validate_multimodal_capabilities(
    req: &ChatRequest,
    capabilities: &Capabilities,
) -> Result<(), LlmError> {
    for part in req.messages.iter().flat_map(|message| &message.parts) {
        let supported = match part {
            ChatContentPart::Text { .. } => true,
            ChatContentPart::InputImage { .. } => capabilities.image_input,
            ChatContentPart::InputAudio { .. } => capabilities.audio_input,
            ChatContentPart::InputFile { .. } => capabilities.file_input,
        };
        if !supported {
            return Err(LlmError::new(format!(
                "provider `{}` does not advertise support for `{}`",
                req.provider,
                content_part_capability(part)
            )));
        }
    }
    Ok(())
}

fn content_part_capability(part: &ChatContentPart) -> &'static str {
    match part {
        ChatContentPart::Text { .. } => "text_input",
        ChatContentPart::InputImage { .. } => "image_input",
        ChatContentPart::InputAudio { .. } => "audio_input",
        ChatContentPart::InputFile { .. } => "file_input",
    }
}

fn valid_media_type(media_type: &str) -> bool {
    let Some((kind, subtype)) = media_type.split_once('/') else {
        return false;
    };
    !kind.is_empty()
        && !subtype.is_empty()
        && media_type
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'+' | b'.' | b'-'))
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("providers registry file was not found; looked at:\n{paths}")]
    NotFound { paths: String },
    #[error("failed to read providers registry `{path}`: {source}")]
    Read {
        path: Utf8PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse providers registry `{path}`: {message}")]
    Parse { path: Utf8PathBuf, message: String },
    #[error("{message}")]
    Invalid { message: String },
}

impl RegistryError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, RegistryError::NotFound { .. })
    }
}

pub struct LlmRouter {
    providers: BTreeMap<String, Arc<dyn LlmProvider>>,
    default_model: Option<ModelSelection>,
    search: SearchRuntime,
    mcp: qcg_mcp::McpRuntime,
}

impl std::fmt::Debug for LlmRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmRouter")
            .field("provider_ids", &self.provider_ids())
            .field("default_model", &self.default_model)
            .field("search", &self.search)
            .field("mcp", &self.mcp)
            .finish()
    }
}

impl LlmRouter {
    pub fn load(explicit: Option<&Utf8Path>) -> Result<Self, RegistryError> {
        if let Some(path) = explicit {
            // An explicit registry is authoritative: never fall back.
            if !path.is_file() {
                return Err(RegistryError::NotFound {
                    paths: format!("- {path}"),
                });
            }
            return Self::from_file(path);
        }
        let candidates = candidate_paths(true);
        for candidate in &candidates {
            if candidate.is_file() {
                return Self::from_file(candidate);
            }
        }
        Err(RegistryError::NotFound {
            paths: candidates
                .iter()
                .map(|path| format!("- {path}"))
                .collect::<Vec<_>>()
                .join("\n"),
        })
    }

    /// Loads the registry like [`LlmRouter::load`] but reports a missing file
    /// as `Ok(None)` when neither the caller nor the `QCG_PROVIDERS`
    /// environment variable named it explicitly. Callers use this to keep
    /// LLM-free workflows running and to surface guided configuration errors
    /// only when an LLM node is actually reached.
    pub fn load_optional(explicit: Option<&Utf8Path>) -> Result<Option<Self>, RegistryError> {
        if let Some(path) = explicit {
            return Self::load(Some(path)).map(Some);
        }
        if let Some(env_path) = std::env::var_os("QCG_PROVIDERS") {
            // The environment override is authoritative as well.
            let path =
                Utf8PathBuf::from_path_buf(std::path::PathBuf::from(&env_path)).map_err(|_| {
                    RegistryError::Invalid {
                        message: format!("`QCG_PROVIDERS` is not valid UTF-8: {env_path:?}"),
                    }
                })?;
            return Self::load(Some(path.as_path())).map(Some);
        }
        for candidate in candidate_paths(false) {
            if candidate.is_file() {
                return Self::from_file(&candidate).map(Some);
            }
        }
        Ok(None)
    }

    pub fn from_file(path: &Utf8Path) -> Result<Self, RegistryError> {
        let mut file = File::open(path).map_err(|source| RegistryError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let metadata = file.metadata().map_err(|source| RegistryError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(RegistryError::Invalid {
                message: format!("providers registry `{path}` is not a regular file"),
            });
        }
        if metadata.len() > MAX_PROVIDERS_FILE_BYTES as u64 {
            return Err(RegistryError::Invalid {
                message: format!(
                    "providers registry `{path}` exceeds {MAX_PROVIDERS_FILE_BYTES} bytes"
                ),
            });
        }
        let mut bytes = Vec::new();
        file.by_ref()
            .take(MAX_PROVIDERS_FILE_BYTES.saturating_add(1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|source| RegistryError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        if bytes.len() > MAX_PROVIDERS_FILE_BYTES {
            return Err(RegistryError::Invalid {
                message: format!(
                    "providers registry `{path}` exceeds {MAX_PROVIDERS_FILE_BYTES} bytes"
                ),
            });
        }
        let text = String::from_utf8(bytes).map_err(|error| RegistryError::Invalid {
            message: format!("providers registry `{path}` is not valid UTF-8: {error}"),
        })?;
        Self::from_str_at(&text, path)
    }

    pub fn parse_text(text: &str) -> Result<Self, RegistryError> {
        Self::from_str_at(text, Utf8Path::new("<memory>"))
    }

    fn from_str_at(text: &str, path: &Utf8Path) -> Result<Self, RegistryError> {
        let file = ProvidersFile::parse(text).map_err(|message| RegistryError::Parse {
            path: path.to_path_buf(),
            message,
        })?;
        let default_model = file
            .default
            .as_ref()
            .and_then(|default| default.model.clone());
        let default_search = file
            .default
            .as_ref()
            .and_then(|default| default.search.clone());
        let mcp = qcg_mcp::McpRuntime::from_specs_with_public_defaults(file.mcp_server).map_err(
            |message| RegistryError::Parse {
                path: path.to_path_buf(),
                message,
            },
        )?;
        let mut router = Self {
            providers: BTreeMap::new(),
            default_model,
            search: SearchRuntime::from_specs(default_search, file.search_provider),
            mcp,
        };
        router.register(Arc::new(FakeLlmProvider));
        for spec in file.provider {
            router.register(Arc::new(HttpProvider::from_spec(spec)));
        }
        Ok(router)
    }

    pub fn register(&mut self, provider: Arc<dyn LlmProvider>) {
        self.providers.insert(provider.id().to_string(), provider);
    }

    pub fn provider_ids(&self) -> Vec<&str> {
        self.providers.keys().map(String::as_str).collect()
    }

    pub fn default_model(&self) -> Option<&ModelSelection> {
        self.default_model.as_ref()
    }

    pub fn search_runtime(&self) -> &SearchRuntime {
        &self.search
    }

    pub fn mcp_runtime(&self) -> &qcg_mcp::McpRuntime {
        &self.mcp
    }

    pub fn into_runtime(self) -> LlmRuntime {
        LlmRuntime {
            default_model: self.default_model.clone(),
            search: self.search.clone(),
            mcp: self.mcp.clone(),
            provider: Arc::new(self),
            registry_present: true,
        }
    }
}

#[async_trait]
impl LlmProvider for LlmRouter {
    fn id(&self) -> &str {
        "router"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            tool_use: true,
            json_schema: true,
            structured_output_with_tools: true,
            seed: true,
            image_input: true,
            audio_input: true,
            file_input: true,
            streaming: true,
            temperature: true,
            top_p: true,
            stop_sequences: true,
            tool_choice: true,
            parallel_tool_calls: true,
            verbosity: true,
            reasoning_effort: vec![
                ReasoningEffort::None,
                ReasoningEffort::Minimal,
                ReasoningEffort::Low,
                ReasoningEffort::Medium,
                ReasoningEffort::High,
                ReasoningEffort::Xhigh,
                ReasoningEffort::Max,
            ],
        }
    }

    fn capabilities_for(&self, provider: &str) -> Option<Capabilities> {
        self.providers
            .get(provider)
            .map(|provider| provider.capabilities())
    }

    fn configuration_error_for(&self, provider: &str) -> Option<String> {
        self.providers
            .get(provider)
            .and_then(|entry| entry.configuration_error_for(provider))
    }

    fn credential_env_names(&self) -> Vec<String> {
        self.providers
            .values()
            .flat_map(|provider| provider.credential_env_names())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
        let provider = self.providers.get(&req.provider).ok_or_else(|| {
            LlmError::new(format!("LLM provider `{}` is not registered", req.provider))
        })?;
        complete_with_retry(provider.as_ref(), req).await
    }

    async fn stream(
        &self,
        req: ChatRequest,
        events: mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        let provider = self.providers.get(&req.provider).ok_or_else(|| {
            LlmError::new(format!("LLM provider `{}` is not registered", req.provider))
        })?;
        timeout(
            Duration::from_secs(provider.timeout_seconds()),
            provider.stream(req, events),
        )
        .await
        .map_err(|_| LlmError {
            message: format!(
                "LLM provider `{}` stream timed out after {} seconds",
                provider.id(),
                provider.timeout_seconds()
            ),
            kind: LlmErrorKind::TimedOut,
        })?
    }
}

fn candidate_paths(include_env: bool) -> Vec<Utf8PathBuf> {
    let mut candidates = Vec::new();
    if include_env
        && let Some(path) = std::env::var_os("QCG_PROVIDERS")
        && let Some(path) = Utf8PathBuf::from_path_buf(std::path::PathBuf::from(path)).ok()
    {
        candidates.push(path);
    }
    candidates.push(Utf8PathBuf::from("providers.toml"));
    if let Ok(exe) = std::env::current_exe()
        && let Some(bin_dir) = exe.parent()
        && let Some(prefix) = bin_dir.parent()
        && let Some(prefix) = Utf8PathBuf::from_path_buf(prefix.to_path_buf()).ok()
    {
        candidates.push(prefix.join("share/qcg/providers.toml"));
    }
    candidates
}

async fn complete_with_retry(
    provider: &dyn LlmProvider,
    req: ChatRequest,
) -> Result<ChatResponse, LlmError> {
    let timeout_seconds = provider.timeout_seconds();
    const MAX_ATTEMPTS: usize = 3;
    let mut last_error = None;
    for attempt in 1..=MAX_ATTEMPTS {
        let result = timeout(
            Duration::from_secs(timeout_seconds),
            provider.complete(req.clone()),
        )
        .await
        .map_err(|_| LlmError {
            message: format!(
                "LLM provider `{}` timed out after {timeout_seconds} seconds",
                provider.id()
            ),
            kind: LlmErrorKind::TimedOut,
        })
        .and_then(|result| result);
        match result {
            Ok(response) => return Ok(response),
            Err(error)
                if attempt < MAX_ATTEMPTS
                    && is_retryable_llm_error(&error)
                    && error.kind != LlmErrorKind::CircuitOpen =>
            {
                let slow_down = matches!(
                    error.kind,
                    LlmErrorKind::HttpStatus(429) | LlmErrorKind::EmptyResponse
                );
                last_error = Some(error);
                let mut delay = retry_backoff(attempt);
                // Shared upstream pools publish "retry shortly" limits that a
                // sub-second exponential cannot clear; give rate-limited and
                // empty-body responses at least a few seconds per attempt.
                if slow_down {
                    delay = delay.max(Duration::from_secs(5 * attempt as u64));
                }
                sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        LlmError::new(format!(
            "LLM provider `{}` failed without an error",
            provider.id()
        ))
    }))
}

fn retry_backoff(attempt: usize) -> Duration {
    let exponent = attempt.saturating_sub(1).min(8) as u32;
    Duration::from_millis(200 * 2_u64.pow(exponent))
}

fn chat_completions_payload(
    req: &ChatRequest,
    send_seed: bool,
    token_limit_field: ChatTokenLimitField,
) -> Value {
    let mut payload = json!({
        "model": req.model,
        "messages": openai_messages(req),
    });
    if let Some(effort) = req.reasoning_effort {
        payload["reasoning_effort"] = json!(effort);
    }
    let token_limit_key = match token_limit_field {
        ChatTokenLimitField::MaxTokens => "max_tokens",
        ChatTokenLimitField::MaxCompletionTokens => "max_completion_tokens",
    };
    payload[token_limit_key] = json!(req.max_tokens);
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = req.top_p {
        payload["top_p"] = json!(top_p);
    }
    if !req.stop_sequences.is_empty() {
        payload["stop"] = json!(req.stop_sequences);
    }
    if send_seed && let Some(seed) = req.seed {
        payload["seed"] = json!(seed);
    }
    if let Some((schema, strict)) = native_response_schema(req) {
        payload["response_format"] = json!({
            "type": "json_schema",
            "json_schema": {
                "name": "qcg_response",
                "strict": strict,
                "schema": schema
            }
        });
    }
    if !req.tools.is_empty() {
        payload["tools"] = Value::Array(req.tools.iter().map(openai_tool).collect());
        payload["tool_choice"] = openai_chat_tool_choice(req.tool_choice.as_ref());
        if let Some(parallel) = req.parallel_tool_calls {
            payload["parallel_tool_calls"] = json!(parallel);
        }
    }
    payload
}

fn responses_payload(req: &ChatRequest) -> Value {
    let mut payload = json!({
        "model": req.model,
        "input": responses_input(req),
        "max_output_tokens": req.max_tokens,
        "store": false,
    });
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = req.top_p {
        payload["top_p"] = json!(top_p);
    }
    if let Some(effort) = req.reasoning_effort {
        payload["reasoning"] = json!({ "effort": effort });
        if !req.tools.is_empty() {
            payload["include"] = json!(["reasoning.encrypted_content"]);
        }
    }
    if let Some(verbosity) = req.verbosity {
        payload["text"]["verbosity"] = json!(verbosity);
    }
    if let Some((schema, strict)) = native_response_schema(req) {
        payload["text"] = json!({
            "format": {
                "type": "json_schema",
                "name": "qcg_response",
                "strict": strict,
                "schema": schema
            }
        });
    }
    if !req.tools.is_empty() {
        payload["tools"] = Value::Array(req.tools.iter().map(responses_tool).collect());
        payload["tool_choice"] = responses_tool_choice(req.tool_choice.as_ref());
        if let Some(parallel) = req.parallel_tool_calls {
            payload["parallel_tool_calls"] = json!(parallel);
        }
    }
    payload
}

fn anthropic_payload(req: &ChatRequest) -> Value {
    let mut tools: Vec<Value> = req.tools.iter().map(anthropic_tool).collect();
    if let Some((schema, _)) = native_response_schema(req) {
        tools.push(json!({
            "name": "qcg_response",
            "description": "Return the final structured qcg response.",
            "input_schema": schema,
        }));
    }
    let mut payload = json!({
        "model": req.model,
        "max_tokens": req.max_tokens,
        "messages": anthropic_messages(req),
    });
    if let Some(system) = &req.system {
        payload["system"] = Value::String(system.clone());
    }
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = req.top_p {
        payload["top_p"] = json!(top_p);
    }
    if !req.stop_sequences.is_empty() {
        payload["stop_sequences"] = json!(req.stop_sequences);
    }
    if !tools.is_empty() {
        payload["tools"] = Value::Array(tools);
        payload["tool_choice"] = match req.tool_choice.as_ref().unwrap_or(&ToolChoice::auto()) {
            ToolChoice::Mode(ToolChoiceMode::None) => json!({ "type": "none" }),
            ToolChoice::Mode(ToolChoiceMode::Auto) => json!({ "type": "auto" }),
            ToolChoice::Mode(ToolChoiceMode::Required) => json!({ "type": "any" }),
            ToolChoice::Tool { tool } => json!({ "type": "tool", "name": tool }),
        };
        if req.parallel_tool_calls == Some(false) {
            payload["tool_choice"]["disable_parallel_tool_use"] = json!(true);
        }
    }
    if payload["tools"]
        .as_array()
        .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "qcg_response"))
    {
        payload["tool_choice"] = json!({ "type": "tool", "name": "qcg_response" });
    }
    payload
}

fn openai_chat_tool_choice(choice: Option<&ToolChoice>) -> Value {
    match choice.unwrap_or(&ToolChoice::auto()) {
        ToolChoice::Mode(mode) => json!(mode),
        ToolChoice::Tool { tool } => json!({
            "type": "function",
            "function": { "name": tool }
        }),
    }
}

fn responses_tool_choice(choice: Option<&ToolChoice>) -> Value {
    match choice.unwrap_or(&ToolChoice::auto()) {
        ToolChoice::Mode(mode) => json!(mode),
        ToolChoice::Tool { tool } => json!({ "type": "function", "name": tool }),
    }
}

fn native_response_schema(req: &ChatRequest) -> Option<(&Value, bool)> {
    let schema = req.response_schema.as_ref()?;
    match req.structured_output {
        StructuredOutputMode::Prompt => None,
        StructuredOutputMode::NativeStrict => Some((schema, true)),
        StructuredOutputMode::NativeCompatible => Some((schema, false)),
        StructuredOutputMode::Auto if native_schema_syntax_compatible(schema) => {
            Some((schema, strict_schema_syntax_compatible(schema)))
        }
        StructuredOutputMode::Auto => None,
    }
}

pub fn native_schema_compatible(schema: &Value) -> bool {
    native_schema_syntax_compatible(schema) && jsonschema::validator_for(schema).is_ok()
}

fn native_schema_syntax_compatible(schema: &Value) -> bool {
    let Value::Object(root) = schema else {
        return false;
    };
    if root.get("type").and_then(Value::as_str) != Some("object") || root.contains_key("anyOf") {
        return false;
    }
    let mut limits = NativeSchemaLimits::default();
    native_schema_node_compatible(schema, 1, &mut limits)
}

#[derive(Default)]
struct NativeSchemaLimits {
    properties: usize,
    string_chars: usize,
    enum_values: usize,
}

fn native_schema_node_compatible(
    schema: &Value,
    depth: usize,
    limits: &mut NativeSchemaLimits,
) -> bool {
    let Value::Object(object) = schema else {
        return false;
    };
    if depth > 10 {
        return false;
    }
    const SUPPORTED: &[&str] = &[
        "$defs",
        "$ref",
        "additionalProperties",
        "anyOf",
        "const",
        "description",
        "enum",
        "exclusiveMaximum",
        "exclusiveMinimum",
        "format",
        "items",
        "maximum",
        "maxItems",
        "minimum",
        "minItems",
        "multipleOf",
        "pattern",
        "properties",
        "required",
        "title",
        "type",
    ];
    if object.keys().any(|key| !SUPPORTED.contains(&key.as_str())) {
        return false;
    }
    if object.get("type").is_some_and(|value| match value {
        Value::String(kind) => !matches!(
            kind.as_str(),
            "object" | "array" | "string" | "number" | "integer" | "boolean" | "null"
        ),
        Value::Array(kinds) => {
            kinds.len() != 2
                || !kinds.iter().any(|kind| kind == "null")
                || kinds.iter().any(|kind| {
                    !kind.as_str().is_some_and(|kind| {
                        matches!(
                            kind,
                            "object"
                                | "array"
                                | "string"
                                | "number"
                                | "integer"
                                | "boolean"
                                | "null"
                        )
                    })
                })
        }
        _ => true,
    }) {
        return false;
    }
    let allows_type = |expected: &str| match object.get("type") {
        Some(Value::String(kind)) => kind == expected,
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind == expected),
        _ => false,
    };
    if (["properties", "required", "additionalProperties"]
        .iter()
        .any(|keyword| object.contains_key(*keyword))
        && !allows_type("object"))
        || (["items", "minItems", "maxItems"]
            .iter()
            .any(|keyword| object.contains_key(*keyword))
            && !allows_type("array"))
        || (["pattern", "format"]
            .iter()
            .any(|keyword| object.contains_key(*keyword))
            && !allows_type("string"))
        || ([
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
        ]
        .iter()
        .any(|keyword| object.contains_key(*keyword))
            && !allows_type("number")
            && !allows_type("integer"))
    {
        return false;
    }
    if object
        .get("properties")
        .is_some_and(|value| !value.is_object())
        || object.get("$defs").is_some_and(|value| !value.is_object())
        || object.get("required").is_some_and(|value| {
            !value
                .as_array()
                .is_some_and(|items| items.iter().all(Value::is_string))
        })
        || object
            .get("enum")
            .is_some_and(|value| !value.as_array().is_some_and(|items| !items.is_empty()))
        || object
            .get("anyOf")
            .is_some_and(|value| !value.as_array().is_some_and(|items| !items.is_empty()))
    {
        return false;
    }
    if object.get("$ref").is_some_and(|value| {
        !value
            .as_str()
            .is_some_and(|reference| reference.starts_with('#'))
    }) {
        return false;
    }
    if object.get("format").is_some_and(|value| {
        !value.as_str().is_some_and(|format| {
            matches!(
                format,
                "date-time"
                    | "time"
                    | "date"
                    | "duration"
                    | "email"
                    | "hostname"
                    | "ipv4"
                    | "ipv6"
                    | "uuid"
            )
        })
    }) {
        return false;
    }
    if let Some(properties) = object.get("properties").and_then(Value::as_object) {
        limits.properties = limits.properties.saturating_add(properties.len());
        limits.string_chars = limits.string_chars.saturating_add(
            properties
                .keys()
                .map(|name| name.chars().count())
                .sum::<usize>(),
        );
        if limits.properties > 5_000
            || !properties
                .values()
                .all(|schema| native_schema_node_compatible(schema, depth + 1, limits))
        {
            return false;
        }
    }
    if let Some(definitions) = object.get("$defs").and_then(Value::as_object) {
        limits.string_chars = limits.string_chars.saturating_add(
            definitions
                .keys()
                .map(|name| name.chars().count())
                .sum::<usize>(),
        );
        if !definitions
            .values()
            .all(|schema| native_schema_node_compatible(schema, depth + 1, limits))
        {
            return false;
        }
    }
    if let Some(values) = object.get("enum").and_then(Value::as_array) {
        limits.enum_values = limits.enum_values.saturating_add(values.len());
        let enum_string_chars = values
            .iter()
            .filter_map(Value::as_str)
            .map(|value| value.chars().count())
            .sum::<usize>();
        limits.string_chars = limits.string_chars.saturating_add(enum_string_chars);
        if limits.enum_values > 1_000 || (values.len() > 250 && enum_string_chars > 15_000) {
            return false;
        }
    }
    if let Some(value) = object.get("const").and_then(Value::as_str) {
        limits.string_chars = limits.string_chars.saturating_add(value.chars().count());
    }
    if limits.string_chars > 120_000 {
        return false;
    }
    if let Some(nested) = object.get("items")
        && !nested.is_boolean()
        && !native_schema_node_compatible(nested, depth + 1, limits)
    {
        return false;
    }
    if let Some(additional) = object.get("additionalProperties")
        && !additional.is_boolean()
    {
        return false;
    }
    if let Some(required) = object.get("required").and_then(Value::as_array)
        && let Some(properties) = object.get("properties").and_then(Value::as_object)
        && required
            .iter()
            .filter_map(Value::as_str)
            .any(|name| !properties.contains_key(name))
    {
        return false;
    }
    object
        .get("anyOf")
        .and_then(Value::as_array)
        .is_none_or(|schemas| {
            schemas
                .iter()
                .all(|schema| native_schema_node_compatible(schema, depth + 1, limits))
        })
}

pub fn strict_schema_compatible(schema: &Value) -> bool {
    if !native_schema_compatible(schema) {
        return false;
    }
    strict_schema_node_compatible(schema)
}

fn strict_schema_syntax_compatible(schema: &Value) -> bool {
    native_schema_syntax_compatible(schema) && strict_schema_node_compatible(schema)
}

fn strict_schema_node_compatible(schema: &Value) -> bool {
    let object = schema
        .as_object()
        .expect("native-compatible schema is an object");
    let object_is_closed = !schema_allows_object(object)
        || (object.get("additionalProperties").and_then(Value::as_bool) == Some(false)
            && object
                .get("properties")
                .and_then(Value::as_object)
                .is_none_or(|properties| {
                    let required = object
                        .get("required")
                        .and_then(Value::as_array)
                        .map(|required| {
                            required
                                .iter()
                                .filter_map(Value::as_str)
                                .collect::<std::collections::BTreeSet<_>>()
                        })
                        .unwrap_or_default();
                    properties
                        .keys()
                        .all(|name| required.contains(name.as_str()))
                }));
    object_is_closed
        && object
            .get("properties")
            .and_then(Value::as_object)
            .is_none_or(|properties| properties.values().all(strict_schema_node_compatible))
        && object
            .get("$defs")
            .and_then(Value::as_object)
            .is_none_or(|definitions| definitions.values().all(strict_schema_node_compatible))
        && ["items", "additionalProperties"].iter().all(|keyword| {
            object
                .get(*keyword)
                .is_none_or(|nested| nested.is_boolean() || strict_schema_node_compatible(nested))
        })
        && object
            .get("anyOf")
            .and_then(Value::as_array)
            .is_none_or(|schemas| schemas.iter().all(strict_schema_node_compatible))
}

fn schema_allows_object(schema: &serde_json::Map<String, Value>) -> bool {
    match schema.get("type") {
        Some(Value::String(kind)) => kind == "object",
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind == "object"),
        _ => schema.contains_key("properties"),
    }
}

fn openai_messages(req: &ChatRequest) -> Vec<Value> {
    let mut messages = Vec::new();
    if let Some(system) = &req.system {
        messages.push(json!({ "role": "system", "content": system }));
    }
    messages.extend(req.messages.iter().map(|message| {
        if !message.tool_calls.is_empty() {
            let tool_calls: Vec<Value> = message
                .tool_calls
                .iter()
                .map(|call| {
                    json!({
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.name,
                            "arguments": call.args.to_string(),
                        }
                    })
                })
                .collect();
            json!({
                "role": "assistant",
                "content": if message.content.is_empty() { Value::Null } else { json!(message.content) },
                "tool_calls": tool_calls,
            })
        } else if message.role == "tool" {
            json!({
                "role": "tool",
                "tool_call_id": message.tool_call_id,
                "content": message.content,
            })
        } else {
            json!({ "role": message.role, "content": openai_message_content(message) })
        }
    }));
    messages
}

fn openai_message_content(message: &ChatMessage) -> Value {
    if message.parts.is_empty() {
        return Value::String(message.content.clone());
    }
    let mut parts = Vec::new();
    if !message.content.is_empty() {
        parts.push(json!({ "type": "text", "text": message.content }));
    }
    parts.extend(message.parts.iter().map(|part| match part {
        ChatContentPart::Text { text } => json!({ "type": "text", "text": text }),
        ChatContentPart::InputImage {
            media_type,
            data,
            detail,
        } => json!({
            "type": "image_url",
            "image_url": {
                "url": data_url(media_type, data),
                "detail": detail.unwrap_or(ImageDetail::Auto),
            }
        }),
        ChatContentPart::InputAudio { media_type, data } => json!({
            "type": "input_audio",
            "input_audio": {
                "data": data,
                "format": media_subtype(media_type),
            }
        }),
        ChatContentPart::InputFile {
            media_type,
            data,
            filename,
        } => json!({
            "type": "file",
            "file": {
                "filename": filename,
                "file_data": data_url(media_type, data),
            }
        }),
    }));
    Value::Array(parts)
}

fn openai_tool(tool: &ToolSpec) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.input_schema
        }
    })
}

fn responses_input(req: &ChatRequest) -> Vec<Value> {
    let mut messages = Vec::new();
    if let Some(system) = &req.system {
        messages.push(json!({ "role": "system", "content": system }));
    }
    for message in &req.messages {
        if let Some(state) = &message.provider_state {
            messages.extend(state.iter().cloned());
        } else if !message.tool_calls.is_empty() {
            messages.extend(message.tool_calls.iter().map(|call| {
                json!({
                    "type": "function_call",
                    "call_id": call.id,
                    "name": call.name,
                    "arguments": call.args.to_string(),
                })
            }));
        } else if message.role == "tool" {
            messages.push(json!({
                "type": "function_call_output",
                "call_id": message.tool_call_id,
                "output": message.content,
            }));
        } else {
            messages.push(json!({
                "role": message.role,
                "content": responses_message_content(message),
            }));
        }
    }
    messages
}

fn responses_message_content(message: &ChatMessage) -> Value {
    if message.parts.is_empty() {
        return Value::String(message.content.clone());
    }
    let mut parts = Vec::new();
    if !message.content.is_empty() {
        parts.push(json!({ "type": "input_text", "text": message.content }));
    }
    parts.extend(message.parts.iter().map(|part| match part {
        ChatContentPart::Text { text } => json!({ "type": "input_text", "text": text }),
        ChatContentPart::InputImage {
            media_type,
            data,
            detail,
        } => json!({
            "type": "input_image",
            "image_url": data_url(media_type, data),
            "detail": detail.unwrap_or(ImageDetail::Auto),
        }),
        ChatContentPart::InputAudio { media_type, data } => json!({
            "type": "input_audio",
            "input_audio": {
                "data": data,
                "format": media_subtype(media_type),
            }
        }),
        ChatContentPart::InputFile {
            media_type,
            data,
            filename,
        } => json!({
            "type": "input_file",
            "file_data": data_url(media_type, data),
            "filename": filename,
        }),
    }));
    Value::Array(parts)
}

fn responses_tool(tool: &ToolSpec) -> Value {
    json!({
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": tool.input_schema,
    })
}

fn anthropic_messages(req: &ChatRequest) -> Vec<Value> {
    req.messages
        .iter()
        .filter_map(|message| match message.role.as_str() {
            "system" => None,
            "tool" => Some(json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": message.tool_call_id,
                    "content": message.content,
                }]
            })),
            "assistant" if !message.tool_calls.is_empty() => {
                let mut content = Vec::new();
                if !message.content.is_empty() {
                    content.push(json!({ "type": "text", "text": message.content }));
                }
                content.extend(message.tool_calls.iter().map(|call| {
                    json!({
                        "type": "tool_use",
                        "id": call.id,
                        "name": call.name,
                        "input": call.args,
                    })
                }));
                Some(json!({ "role": "assistant", "content": content }))
            }
            role => Some(json!({
                "role": role,
                "content": anthropic_message_content(message),
            })),
        })
        .collect()
}

fn anthropic_message_content(message: &ChatMessage) -> Value {
    if message.parts.is_empty() {
        return Value::String(message.content.clone());
    }
    let mut parts = Vec::new();
    if !message.content.is_empty() {
        parts.push(json!({ "type": "text", "text": message.content }));
    }
    parts.extend(message.parts.iter().map(|part| match part {
        ChatContentPart::Text { text } => json!({ "type": "text", "text": text }),
        ChatContentPart::InputImage {
            media_type, data, ..
        } => json!({
            "type": "image",
            "source": { "type": "base64", "media_type": media_type, "data": data }
        }),
        ChatContentPart::InputFile {
            media_type,
            data,
            filename,
        } => json!({
            "type": "document",
            "title": filename,
            "source": { "type": "base64", "media_type": media_type, "data": data }
        }),
        ChatContentPart::InputAudio { .. } => json!({
            "type": "text",
            "text": "[QCG_UNSUPPORTED_AUDIO_INPUT]"
        }),
    }));
    Value::Array(parts)
}

fn data_url(media_type: &str, data: &str) -> String {
    format!("data:{media_type};base64,{data}")
}

fn media_subtype(media_type: &str) -> &str {
    media_type
        .split_once('/')
        .map(|(_, subtype)| subtype)
        .unwrap_or(media_type)
}

fn anthropic_tool(tool: &ToolSpec) -> Value {
    json!({
        "name": tool.name,
        "description": tool.description,
        "input_schema": tool.input_schema,
    })
}

fn parse_chat_completions_response(value: Value) -> Result<ChatResponse, LlmError> {
    let message = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .ok_or_else(|| {
            LlmError::invalid_response(
                "OpenAI-compatible response did not include choices[0].message",
            )
        })?;
    let mut content = Vec::new();
    if let Some(text) = message.get("content").and_then(Value::as_str)
        && !text.is_empty()
    {
        content.push(ChatContent::Text(text.to_string()));
    }
    let refused = message
        .get("refusal")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty());
    if let Some(text) = refused {
        content.push(ChatContent::Text(text.to_string()));
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for call in tool_calls {
            content.push(parse_openai_tool_call(call)?);
        }
    }
    if content.is_empty() {
        return Err(LlmError::invalid_response(
            "OpenAI-compatible response did not include text or tool calls",
        ));
    }
    let usage = TokenUsage {
        input: required_usage(&value, "/usage/prompt_tokens")?,
        output: required_usage(&value, "/usage/completion_tokens")?,
        reasoning: value
            .pointer("/usage/completion_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    };
    let finish_reason = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(Value::as_str);
    let stop = match (finish_reason, refused.is_some()) {
        (_, true) | (Some("content_filter"), _) => StopReason::Refusal,
        (Some("length"), _) => StopReason::MaxTokens,
        (Some("tool_calls"), _) => StopReason::ToolUse,
        (Some("stop"), _) => StopReason::EndTurn,
        (other, _) => {
            return Err(LlmError::invalid_response(format!(
                "OpenAI-compatible response returned unknown finish_reason `{}`",
                other.unwrap_or("missing")
            )));
        }
    };
    Ok(ChatResponse {
        content,
        usage,
        stop,
        provider_state: None,
    })
}

fn parse_openai_tool_call(call: &Value) -> Result<ChatContent, LlmError> {
    let id = call
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| {
            LlmError::invalid_response("OpenAI-compatible tool call did not include id")
        })?
        .to_string();
    let function = call.get("function").ok_or_else(|| {
        LlmError::invalid_response("OpenAI-compatible tool call did not include function")
    })?;
    let name = function
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            LlmError::invalid_response("OpenAI-compatible tool call did not include function.name")
        })?
        .to_string();
    let args_text = function
        .get("arguments")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            LlmError::invalid_response(
                "OpenAI-compatible tool call did not include string function.arguments",
            )
        })?;
    let args = serde_json::from_str(args_text).map_err(|error| {
        LlmError::invalid_response(format!(
            "OpenAI-compatible tool call arguments were invalid JSON: {error}"
        ))
    })?;
    Ok(ChatContent::ToolCall { id, name, args })
}

fn parse_responses_response(value: Value) -> Result<ChatResponse, LlmError> {
    let mut content = Vec::new();
    let output_items = value.get("output").and_then(Value::as_array);
    let has_message_items = output_items.is_some_and(|items| {
        items
            .iter()
            .any(|item| item.get("type").and_then(Value::as_str) == Some("message"))
    });
    if !has_message_items
        && let Some(text) = value.get("output_text").and_then(Value::as_str)
        && !text.is_empty()
    {
        content.push(ChatContent::Text(text.to_string()));
    }
    let mut refused = false;
    for item in output_items.into_iter().flatten() {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                for block in item
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    match block.get("type").and_then(Value::as_str) {
                        Some("output_text") | Some("text") => {
                            if let Some(text) = block
                                .get("text")
                                .or_else(|| block.get("content"))
                                .and_then(Value::as_str)
                                && !text.is_empty()
                            {
                                content.push(ChatContent::Text(text.to_string()));
                            }
                        }
                        Some("refusal") => {
                            if let Some(text) = block.get("refusal").and_then(Value::as_str) {
                                content.push(ChatContent::Text(text.to_string()));
                                refused = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Some("function_call") => {
                let id = item
                    .get("call_id")
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        LlmError::invalid_response(
                            "Responses API function_call did not include call_id",
                        )
                    })?
                    .to_string();
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        LlmError::invalid_response(
                            "Responses API function_call did not include name",
                        )
                    })?
                    .to_string();
                let args_text = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        LlmError::invalid_response(
                            "Responses API function_call did not include string arguments",
                        )
                    })?;
                let args = serde_json::from_str(args_text).map_err(|error| {
                    LlmError::invalid_response(format!(
                        "Responses API function_call arguments were invalid JSON: {error}"
                    ))
                })?;
                content.push(ChatContent::ToolCall { id, name, args });
            }
            _ => {}
        }
    }
    if content.is_empty() {
        return Err(LlmError::invalid_response(
            "Responses API response did not include text or tool calls",
        ));
    }
    let usage = TokenUsage {
        input: required_usage(&value, "/usage/input_tokens")?,
        output: required_usage(&value, "/usage/output_tokens")?,
        reasoning: value
            .pointer("/usage/output_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    };
    let stop = match value.get("status").and_then(Value::as_str) {
        Some("incomplete") => match value
            .pointer("/incomplete_details/reason")
            .and_then(Value::as_str)
        {
            Some("max_output_tokens") => StopReason::MaxTokens,
            Some("content_filter") => StopReason::Refusal,
            other => {
                return Err(LlmError::invalid_response(format!(
                    "Responses API returned unknown incomplete reason `{}`",
                    other.unwrap_or("missing")
                )));
            }
        },
        Some("completed") if refused => StopReason::Refusal,
        Some("completed")
            if content
                .iter()
                .any(|item| matches!(item, ChatContent::ToolCall { .. })) =>
        {
            StopReason::ToolUse
        }
        Some("completed") => StopReason::EndTurn,
        other => {
            return Err(LlmError::invalid_response(format!(
                "Responses API returned unsupported status `{}`",
                other.unwrap_or("missing")
            )));
        }
    };
    let provider_state = matches!(stop, StopReason::ToolUse)
        .then(|| value.get("output").cloned())
        .flatten();
    Ok(ChatResponse {
        content,
        usage,
        stop,
        provider_state,
    })
}

fn parse_anthropic_response(value: Value) -> Result<ChatResponse, LlmError> {
    let blocks = value
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            LlmError::invalid_response("Anthropic response did not include content array")
        })?;
    let mut content = Vec::new();
    let mut structured_response = false;
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = block.get("text").and_then(Value::as_str)
                    && !text.is_empty()
                {
                    content.push(ChatContent::Text(text.to_string()));
                }
            }
            Some("tool_use") => {
                let id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        LlmError::invalid_response("Anthropic tool_use did not include id")
                    })?
                    .to_string();
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        LlmError::invalid_response("Anthropic tool_use did not include name")
                    })?
                    .to_string();
                let args = block.get("input").cloned().unwrap_or_else(|| json!({}));
                if name == "qcg_response" {
                    structured_response = true;
                    content.push(ChatContent::Text(args.to_string()));
                } else {
                    content.push(ChatContent::ToolCall { id, name, args });
                }
            }
            _ => {}
        }
    }
    if content.is_empty() {
        return Err(LlmError::invalid_response(
            "Anthropic response did not include text or tool calls",
        ));
    }
    let usage = TokenUsage {
        input: required_usage(&value, "/usage/input_tokens")?,
        output: required_usage(&value, "/usage/output_tokens")?,
        reasoning: 0,
    };
    let stop = match value.get("stop_reason").and_then(Value::as_str) {
        Some("tool_use") => StopReason::ToolUse,
        Some("max_tokens") => StopReason::MaxTokens,
        Some("refusal") => StopReason::Refusal,
        Some("end_turn") | Some("stop_sequence") => StopReason::EndTurn,
        other => {
            return Err(LlmError::invalid_response(format!(
                "Anthropic response returned unsupported stop_reason `{}`",
                other.unwrap_or("missing")
            )));
        }
    };
    if structured_response
        && content
            .iter()
            .any(|item| matches!(item, ChatContent::ToolCall { .. }))
    {
        return Err(LlmError::invalid_response(
            "Anthropic response mixed qcg_response with external tool calls",
        ));
    }
    let stop = if structured_response {
        if stop != StopReason::ToolUse {
            return Err(LlmError::invalid_response(
                "Anthropic response returned qcg_response without tool_use stop_reason",
            ));
        }
        StopReason::EndTurn
    } else {
        stop
    };
    Ok(ChatResponse {
        content,
        usage,
        stop,
        provider_state: None,
    })
}

fn required_usage(value: &Value, pointer: &str) -> Result<u64, LlmError> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            LlmError::invalid_response(format!(
                "LLM provider response did not include integer `{pointer}`"
            ))
        })
}

fn llm_http_error(error: reqwest::Error) -> LlmError {
    if error.is_timeout() {
        return LlmError {
            message: "LLM provider request timed out".into(),
            kind: LlmErrorKind::TimedOut,
        };
    }
    if error.is_decode() {
        return LlmError {
            message: "LLM provider response could not be decoded".into(),
            kind: LlmErrorKind::InvalidResponse,
        };
    }
    LlmError {
        message: "LLM provider request failed".into(),
        kind: LlmErrorKind::Network,
    }
}

fn marker_line(prompt: &str, marker: &str) -> Option<String> {
    prompt
        .lines()
        .find_map(|line| line.trim().strip_prefix(marker).map(str::trim))
        .map(ToOwned::to_owned)
}

fn marker_block(prompt: &str, marker: &str) -> Option<String> {
    let mut lines = prompt.lines();
    while let Some(line) = lines.next() {
        if let Some(first) = line.trim().strip_prefix(marker).map(str::trim) {
            let mut value = first.to_owned();
            let rest = lines.collect::<Vec<_>>().join("\n");
            if !rest.trim().is_empty() {
                if !value.is_empty() {
                    value.push('\n');
                }
                value.push_str(&rest);
            }
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::JoinHandle;

    fn sample_request() -> ChatRequest {
        ChatRequest {
            provider: "openai".into(),
            model: "gpt-test".into(),
            system: Some("system".into()),
            messages: vec![ChatMessage::text("user", "hello")],
            tools: vec![],
            response_schema: None,
            structured_output: StructuredOutputMode::Auto,
            temperature: Some(0.5),
            top_p: None,
            max_tokens: 128,
            stop_sequences: vec![],
            seed: Some(42),
            reasoning_effort: None,
            tool_choice: None,
            parallel_tool_calls: None,
            verbosity: None,
            stream: false,
        }
    }

    fn spawn_http_response(status: u16, body: String, headers: String) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test request should arrive");
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
        });
        (format!("http://{address}"), handle)
    }

    #[test]
    fn parses_chat_completions_text_response() {
        let response = parse_chat_completions_response(json!({
            "choices": [{
                "message": { "content": "hello" },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
        }))
        .expect("response should parse");

        assert_eq!(response.usage.input, 3);
        assert_eq!(response.usage.output, 2);
        assert!(matches!(response.stop, StopReason::EndTurn));
        assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "hello"));
    }

    #[test]
    fn parses_chat_completions_tool_call_response() {
        let response = parse_chat_completions_response(json!({
            "choices": [{
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "write_draft",
                            "arguments": "{\"path\":\"drafts/result.txt\",\"content\":\"ok\"}"
                        }
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": { "prompt_tokens": 10, "completion_tokens": 4 }
        }))
        .expect("response should parse");

        assert!(matches!(response.stop, StopReason::ToolUse));
        match &response.content[0] {
            ChatContent::ToolCall { id, name, args } => {
                assert_eq!(id, "call_1");
                assert_eq!(name, "write_draft");
                assert_eq!(args["path"], "drafts/result.txt");
            }
            other => panic!("expected tool call, got {other:?}"),
        }
    }

    #[test]
    fn formats_openai_tools() {
        let tool = openai_tool(&ToolSpec {
            name: "fetch".into(),
            description: "fetch something".into(),
            input_schema: json!({ "type": "object" }),
        });
        assert_eq!(tool["type"], "function");
        assert_eq!(tool["function"]["name"], "fetch");
        assert_eq!(tool["function"]["parameters"]["type"], "object");
    }

    #[test]
    fn tool_continuations_keep_provider_call_ids() {
        let call = ChatToolCall {
            id: "call_1".into(),
            name: "fetch".into(),
            args: json!({ "key": "value" }),
        };
        let mut request = sample_request();
        request.messages = vec![
            ChatMessage::assistant_tool_calls("", vec![call.clone()]),
            ChatMessage::tool_result("call_1", "{\"ok\":true}"),
        ];

        let chat = openai_messages(&request);
        assert_eq!(chat[1]["tool_calls"][0]["id"], "call_1");
        assert_eq!(chat[2]["tool_call_id"], "call_1");

        let responses = responses_input(&request);
        assert_eq!(responses[1]["call_id"], "call_1");
        assert_eq!(responses[2]["type"], "function_call_output");
        assert_eq!(responses[2]["call_id"], "call_1");

        let anthropic = anthropic_messages(&request);
        assert_eq!(anthropic[0]["content"][0]["id"], "call_1");
        assert_eq!(anthropic[1]["content"][0]["tool_use_id"], "call_1");
    }

    #[test]
    fn responses_continuation_preserves_reasoning_state() {
        let mut request = sample_request();
        request.system = None;
        request.messages = vec![
            ChatMessage::text("user", "hello"),
            ChatMessage::provider_state(vec![
                json!({ "type": "reasoning", "encrypted_content": "opaque" }),
                json!({ "type": "function_call", "call_id": "call_1", "name": "fetch", "arguments": "{}" }),
            ]),
            ChatMessage::tool_result("call_1", "done"),
        ];

        let input = responses_input(&request);

        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[1]["type"], "reasoning");
        assert_eq!(input[2]["type"], "function_call");
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(input[3]["call_id"], "call_1");
    }

    #[test]
    fn chat_completions_payload_gates_seed_by_capability() {
        let request = sample_request();

        let payload = chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens);
        assert_eq!(payload["seed"], 42);

        let payload = chat_completions_payload(&request, false, ChatTokenLimitField::MaxTokens);
        assert!(payload.get("seed").is_none());
    }

    #[test]
    fn chat_completions_payload_omits_null_fields_and_includes_schema_and_tools() {
        let mut request = sample_request();
        request.temperature = None;
        request.response_schema = Some(json!({ "type": "object" }));
        request.tools = vec![ToolSpec {
            name: "fetch".into(),
            description: "fetch".into(),
            input_schema: json!({ "type": "object" }),
        }];

        let payload = chat_completions_payload(&request, false, ChatTokenLimitField::MaxTokens);

        assert!(payload.get("temperature").is_none());
        assert_eq!(payload["max_tokens"], 128);
        assert_eq!(payload["response_format"]["type"], "json_schema");
        assert_eq!(payload["response_format"]["json_schema"]["strict"], false);
        assert_eq!(payload["tool_choice"], "auto");
        assert_eq!(payload["messages"][0]["role"], "system");
    }

    #[test]
    fn structured_output_mode_selects_strict_compatible_or_prompt_transport() {
        let mut request = sample_request();
        request.response_schema = Some(json!({
            "type": "object",
            "additionalProperties": false,
            "properties": { "answer": { "type": "string" } },
            "required": ["answer"]
        }));
        let auto =
            chat_completions_payload(&request, false, ChatTokenLimitField::MaxCompletionTokens);
        assert_eq!(auto["response_format"]["json_schema"]["strict"], true);

        request.structured_output = StructuredOutputMode::NativeCompatible;
        let compatible = responses_payload(&request);
        assert_eq!(compatible["text"]["format"]["strict"], false);

        request.structured_output = StructuredOutputMode::Prompt;
        let prompt =
            chat_completions_payload(&request, false, ChatTokenLimitField::MaxCompletionTokens);
        assert!(prompt.get("response_format").is_none());
        let anthropic = anthropic_payload(&request);
        assert!(anthropic.get("tool_choice").is_none());
    }

    #[test]
    fn provider_boundary_rejects_native_schema_without_capability() {
        let mut request = sample_request();
        request.response_schema = Some(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }));
        request.structured_output = StructuredOutputMode::Auto;

        let error = validate_structured_output_capabilities(
            &request,
            ApiFlavor::ChatCompletions,
            &Capabilities::default(),
        )
        .expect_err("native transport must require the provider capability");
        assert!(error.to_string().contains("native structured output"));

        request.structured_output = StructuredOutputMode::Prompt;
        validate_structured_output_capabilities(
            &request,
            ApiFlavor::ChatCompletions,
            &Capabilities::default(),
        )
        .expect("prompt mode must remain available without native support");
    }

    #[test]
    fn provider_boundary_rejects_reserved_and_invalid_tool_schemas() {
        let mut request = sample_request();
        request.tools = vec![ToolSpec {
            name: "qcg_response".into(),
            description: "reserved".into(),
            input_schema: json!({ "type": "object" }),
        }];
        let error = validate_chat_request(&request, ApiFlavor::ChatCompletions)
            .expect_err("reserved tool name must fail");
        assert!(error.to_string().contains("reserved"));

        request.tools[0].name = "broken".into();
        request.tools[0].input_schema = json!({ "type": "object", "required": "value" });
        let error = validate_chat_request(&request, ApiFlavor::ChatCompletions)
            .expect_err("invalid tool schema must fail");
        assert!(error.to_string().contains("input_schema is invalid"));

        request.tools[0].input_schema = json!({
            "type": "object",
            "properties": { "value": { "$dynamicRef": "https://example.invalid/schema.json" } }
        });
        let error = validate_chat_request(&request, ApiFlavor::ChatCompletions)
            .expect_err("external tool schema reference must fail");
        assert!(error.to_string().contains("external reference"));
    }

    #[test]
    fn native_schema_compatibility_rejects_unsupported_keywords_and_external_refs() {
        let supported = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "answer": { "type": "string", "pattern": "^[a-z]+$" },
                "score": { "type": "number", "minimum": 0, "maximum": 1 },
                "tags": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 3,
                    "items": { "type": "string" }
                }
            },
            "required": ["answer", "score", "tags"]
        });
        assert!(native_schema_compatible(&supported));
        assert!(strict_schema_compatible(&supported));

        for schema in [
            json!({ "type": "array", "items": { "type": "string" } }),
            json!({ "type": "object", "properties": { "answer": { "type": "string", "minLength": 1 } } }),
            json!({ "type": "object", "properties": { "answer": { "$ref": "https://example.invalid/schema.json" } } }),
            json!({ "type": "object", "properties": { "answer": { "$ref": "#/$defs/missing" } } }),
            json!({ "type": "object", "properties": { "answer": { "type": "string", "minimum": 1 } } }),
            json!({ "type": "object", "properties": { "answer": { "properties": {} } } }),
            json!({ "type": "object", "properties": [] }),
            json!({ "type": "object", "properties": {}, "required": "answer" }),
            json!({ "type": "object", "anyOf": [{ "type": "object" }] }),
        ] {
            assert!(!native_schema_compatible(&schema), "{schema}");
            assert!(!strict_schema_compatible(&schema), "{schema}");
        }
    }

    #[tokio::test]
    async fn fake_provider_schema_response_uses_an_explicit_default() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["summary"],
            "properties": {
                "summary": { "type": "string", "minLength": 1 }
            },
            "default": { "summary": "bounded specialist result" }
        });
        let mut request = sample_request();
        request.response_schema = Some(schema);
        request.structured_output = StructuredOutputMode::Prompt;
        let response = FakeLlmProvider
            .complete(request)
            .await
            .expect("fake provider should complete");
        assert!(matches!(
            &response.content[0],
            ChatContent::Text(text) if text == r#"{"summary":"bounded specialist result"}"#
        ));
    }

    #[test]
    fn multimodal_parts_map_to_provider_native_payloads() {
        let mut request = sample_request();
        request.messages = vec![ChatMessage::with_parts(
            "user",
            vec![
                ChatContentPart::Text {
                    text: "inspect".into(),
                },
                ChatContentPart::InputImage {
                    media_type: "image/png".into(),
                    data: "aGVsbG8=".into(),
                    detail: Some(ImageDetail::High),
                },
                ChatContentPart::InputFile {
                    media_type: "application/pdf".into(),
                    data: "cGRm".into(),
                    filename: "input.pdf".into(),
                },
            ],
        )];
        let chat = chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens);
        assert_eq!(chat["messages"][1]["content"][1]["type"], "image_url");
        assert_eq!(
            chat["messages"][1]["content"][1]["image_url"]["url"],
            "data:image/png;base64,aGVsbG8="
        );
        let responses = responses_payload(&request);
        assert_eq!(responses["input"][1]["content"][2]["type"], "input_file");
        assert_eq!(responses["input"][1]["content"][2]["filename"], "input.pdf");
        let anthropic = anthropic_payload(&request);
        assert_eq!(anthropic["messages"][0]["content"][1]["type"], "image");
        assert_eq!(
            anthropic["messages"][0]["content"][2]["source"]["media_type"],
            "application/pdf"
        );
    }

    #[test]
    fn provider_payloads_map_explicit_invocation_policy() {
        let mut request = sample_request();
        request.temperature = None;
        request.top_p = Some(0.25);
        request.stop_sequences = vec!["END".into()];
        request.tools = vec![ToolSpec {
            name: "lookup".into(),
            description: "Look up a value".into(),
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
        }];
        request.tool_choice = Some(ToolChoice::required());
        request.parallel_tool_calls = Some(false);
        request.verbosity = Some(ResponseVerbosity::High);

        validate_chat_request(&request, ApiFlavor::ChatCompletions)
            .expect("portable request policy should validate");
        let chat = chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens);
        assert_eq!(chat["top_p"], 0.25);
        assert_eq!(chat["stop"], json!(["END"]));
        assert_eq!(chat["tool_choice"], "required");
        assert_eq!(chat["parallel_tool_calls"], false);

        let responses = responses_payload(&request);
        assert_eq!(responses["top_p"], 0.25);
        assert!(responses.get("stop").is_none());
        assert_eq!(responses["tool_choice"], "required");
        assert_eq!(responses["parallel_tool_calls"], false);
        assert_eq!(responses["text"]["verbosity"], "high");

        let anthropic = anthropic_payload(&request);
        assert_eq!(anthropic["top_p"], 0.25);
        assert_eq!(anthropic["stop_sequences"], json!(["END"]));
        assert_eq!(anthropic["tool_choice"]["type"], "any");
        assert_eq!(anthropic["tool_choice"]["disable_parallel_tool_use"], true);

        request.tool_choice = Some(ToolChoice::Tool {
            tool: "lookup".into(),
        });
        assert_eq!(
            chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens)["tool_choice"]
                ["function"]["name"],
            "lookup"
        );
        assert_eq!(responses_payload(&request)["tool_choice"]["name"], "lookup");
        assert_eq!(anthropic_payload(&request)["tool_choice"]["name"], "lookup");
    }

    #[test]
    fn invocation_policy_rejects_conflicts_and_tool_controls_without_tools() {
        let mut request = sample_request();
        request.top_p = Some(0.5);
        assert!(
            validate_chat_request(&request, ApiFlavor::ChatCompletions)
                .unwrap_err()
                .to_string()
                .contains("mutually exclusive")
        );

        request.top_p = None;
        request.tool_choice = Some(ToolChoice::required());
        assert!(
            validate_chat_request(&request, ApiFlavor::ChatCompletions)
                .unwrap_err()
                .to_string()
                .contains("require at least one tool")
        );
    }

    #[tokio::test]
    async fn chat_stream_accumulates_deltas_tool_calls_and_usage() {
        let (events, mut receiver) = mpsc::channel(4);
        let mut accumulator = ChatCompletionAccumulator::default();
        accumulator
            .ingest(
                &json!({"choices":[{"delta":{"content":"hel"},"finish_reason":null}]}),
                &events,
            )
            .await
            .unwrap();
        accumulator
            .ingest(
                &json!({
                    "choices":[{"delta":{"content":"lo"},"finish_reason":"stop"}],
                    "usage":{"prompt_tokens":2,"completion_tokens":1}
                }),
                &events,
            )
            .await
            .unwrap();
        let response = accumulator.finish().unwrap();
        assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "hello"));
        assert_eq!(response.usage.input, 2);
        assert!(
            matches!(receiver.try_recv(), Ok(ChatStreamEvent::TextDelta { text }) if text == "hel")
        );
        assert!(
            matches!(receiver.try_recv(), Ok(ChatStreamEvent::TextDelta { text }) if text == "lo")
        );
    }

    #[tokio::test]
    async fn circuit_breaker_opens_after_declared_failures() {
        let mut spec = spec_with_base_url("guarded", "http://127.0.0.1:1/v1/");
        spec.circuit_breaker_failures = Some(2);
        let provider = HttpProvider::from_spec(spec);
        for _ in 0..2 {
            provider.record_request_result::<()>(&Err(LlmError {
                message: "unavailable".into(),
                kind: LlmErrorKind::HttpStatus(503),
            }));
        }
        let error = provider
            .acquire_request_slot()
            .await
            .expect_err("open circuit must reject without issuing a request");
        assert_eq!(error.kind, LlmErrorKind::CircuitOpen);
    }

    #[test]
    fn chat_completions_payload_maps_reasoning_effort_and_completion_limit() {
        let mut request = sample_request();
        request.temperature = None;
        request.seed = None;
        request.reasoning_effort = Some(ReasoningEffort::High);

        let payload =
            chat_completions_payload(&request, false, ChatTokenLimitField::MaxCompletionTokens);

        assert_eq!(payload["reasoning_effort"], "high");
        assert_eq!(payload["max_completion_tokens"], 128);
        assert!(payload.get("max_tokens").is_none());
    }

    #[test]
    fn responses_payload_maps_reasoning_effort() {
        let mut request = sample_request();
        request.temperature = None;
        request.seed = None;
        request.reasoning_effort = Some(ReasoningEffort::Max);

        let payload = responses_payload(&request);

        assert_eq!(payload["reasoning"]["effort"], "max");
        assert_eq!(payload["max_output_tokens"], 128);
        assert_eq!(payload["store"], false);
    }

    #[test]
    fn parses_responses_text_response() {
        let response = parse_responses_response(json!({
            "status": "completed",
            "output": [{
                "type": "message",
                "content": [{ "type": "output_text", "text": "hello" }]
            }],
            "usage": { "input_tokens": 7, "output_tokens": 2 }
        }))
        .expect("response should parse");

        assert_eq!(response.usage.input, 7);
        assert_eq!(response.usage.output, 2);
        assert!(matches!(response.stop, StopReason::EndTurn));
        assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "hello"));
    }

    #[test]
    fn parses_responses_function_call() {
        let response = parse_responses_response(json!({
            "status": "completed",
            "output": [{
                "type": "function_call",
                "call_id": "call_1",
                "name": "write_draft",
                "arguments": "{\"path\":\"drafts/result.txt\",\"content\":\"ok\"}"
            }],
            "usage": { "input_tokens": 9, "output_tokens": 3 }
        }))
        .expect("response should parse");

        assert!(matches!(response.stop, StopReason::ToolUse));
        assert!(response.provider_state.is_some());
        match &response.content[0] {
            ChatContent::ToolCall { id, name, args } => {
                assert_eq!(id, "call_1");
                assert_eq!(name, "write_draft");
                assert_eq!(args["content"], "ok");
            }
            other => panic!("expected tool call, got {other:?}"),
        }
    }

    #[test]
    fn response_parsers_report_reasoning_tokens_as_output_detail() {
        let chat = parse_chat_completions_response(json!({
            "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
            "usage": {
                "prompt_tokens": 3,
                "completion_tokens": 11,
                "completion_tokens_details": { "reasoning_tokens": 7 }
            }
        }))
        .expect("chat response should parse");
        assert_eq!(chat.usage.output, 11);
        assert_eq!(chat.usage.reasoning, 7);

        let responses = parse_responses_response(json!({
            "status": "completed",
            "output": [{
                "type": "message",
                "content": [{ "type": "output_text", "text": "ok" }]
            }],
            "usage": {
                "input_tokens": 3,
                "output_tokens": 11,
                "output_tokens_details": { "reasoning_tokens": 7 }
            }
        }))
        .expect("Responses API response should parse");
        assert_eq!(responses.usage.output, 11);
        assert_eq!(responses.usage.reasoning, 7);
    }

    #[test]
    fn anthropic_payload_forces_qcg_response_tool_choice() {
        let mut request = sample_request();
        request.response_schema = Some(json!({ "type": "object" }));

        let payload = anthropic_payload(&request);

        assert_eq!(payload["tool_choice"]["name"], "qcg_response");
        assert_eq!(payload["system"], "system");
        assert!(
            payload["tools"]
                .as_array()
                .expect("tools")
                .iter()
                .any(|tool| tool["name"] == "qcg_response")
        );
    }

    #[test]
    fn parses_anthropic_text_response() {
        let response = parse_anthropic_response(json!({
            "content": [{ "type": "text", "text": "hello" }],
            "stop_reason": "end_turn",
            "usage": { "input_tokens": 5, "output_tokens": 2 }
        }))
        .expect("response should parse");

        assert_eq!(response.usage.input, 5);
        assert_eq!(response.usage.output, 2);
        assert!(matches!(response.stop, StopReason::EndTurn));
        assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "hello"));
    }

    #[test]
    fn parses_anthropic_tool_use_response() {
        let response = parse_anthropic_response(json!({
            "content": [{
                "type": "tool_use",
                "id": "toolu_1",
                "name": "write_file",
                "input": { "path": "out.txt", "content": "ok" }
            }],
            "stop_reason": "tool_use",
            "usage": { "input_tokens": 8, "output_tokens": 3 }
        }))
        .expect("response should parse");

        assert!(matches!(response.stop, StopReason::ToolUse));
        match &response.content[0] {
            ChatContent::ToolCall { id, name, args } => {
                assert_eq!(id, "toolu_1");
                assert_eq!(name, "write_file");
                assert_eq!(args["path"], "out.txt");
            }
            other => panic!("expected tool call, got {other:?}"),
        }
    }

    #[test]
    fn parses_anthropic_schema_tool_as_text() {
        let response = parse_anthropic_response(json!({
            "content": [{
                "type": "tool_use",
                "id": "toolu_schema",
                "name": "qcg_response",
                "input": { "title": "structured" }
            }],
            "stop_reason": "tool_use",
            "usage": { "input_tokens": 8, "output_tokens": 3 }
        }))
        .expect("response should parse");

        assert!(
            matches!(&response.content[0], ChatContent::Text(text) if text == "{\"title\":\"structured\"}")
        );
        assert_eq!(response.stop, StopReason::EndTurn);
    }

    #[tokio::test]
    async fn streams_anthropic_schema_tool_as_a_completed_structured_response() {
        let (events, _receiver) = mpsc::channel(4);
        let mut accumulator = AnthropicAccumulator::default();
        for event in [
            json!({
                "type": "message_start",
                "message": { "usage": { "input_tokens": 8 } }
            }),
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {
                    "type": "tool_use",
                    "id": "toolu_schema",
                    "name": "qcg_response"
                }
            }),
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": "{\"title\":\"structured\"}"
                }
            }),
            json!({
                "type": "message_delta",
                "delta": { "stop_reason": "tool_use" },
                "usage": { "output_tokens": 3 }
            }),
        ] {
            accumulator
                .ingest(&event, &events)
                .await
                .expect("stream event should parse");
        }
        let response = accumulator.finish().expect("stream should finish");
        assert_eq!(response.stop, StopReason::EndTurn);
        assert!(
            matches!(&response.content[0], ChatContent::Text(text) if text == "{\"title\":\"structured\"}")
        );
    }

    #[test]
    fn interpolates_known_environment_placeholders() {
        // SAFETY: single-threaded test binary section; unique variable name.
        unsafe { std::env::set_var("QCG_LLM_INTERP_TEST", "resolved") };
        let resolved = interpolate_env("https://host/{QCG_LLM_INTERP_TEST}/v1")
            .expect("placeholder should resolve");
        assert_eq!(resolved, "https://host/resolved/v1");
    }

    #[test]
    fn interpolation_reports_missing_variables() {
        let error = interpolate_env("https://host/{QCG_LLM_MISSING_VARIABLE_XYZ}/v1").unwrap_err();
        assert!(error.contains("QCG_LLM_MISSING_VARIABLE_XYZ"), "{error}");
    }

    #[test]
    fn interpolation_rejects_invalid_placeholder_names() {
        let error = interpolate_env("https://host/{not-an-env}/v1").unwrap_err();
        assert!(error.contains("invalid environment placeholder"), "{error}");
    }

    fn spec_with_base_url(id: &str, base_url: &str) -> ProviderSpec {
        let capabilities = Capabilities {
            temperature: true,
            ..Capabilities::default()
        };
        ProviderSpec {
            id: id.into(),
            api: ApiFlavor::ChatCompletions,
            base_url: Some(base_url.into()),
            base_url_env: None,
            api_key_env: None,
            api_key_file_env: None,
            auth_header: None,
            capabilities,
            path_template: None,
            query: BTreeMap::new(),
            timeout_seconds: None,
            chat_token_limit_field: None,
            response_body_limit_bytes: None,
            max_concurrency: None,
            requests_per_minute: None,
            circuit_breaker_failures: None,
            circuit_breaker_cooldown_seconds: None,
        }
    }

    #[test]
    fn endpoint_uses_flavor_default_path() {
        let provider = HttpProvider::from_spec(spec_with_base_url("x", "http://host/v1/"));
        assert_eq!(
            provider
                .endpoint_for("m")
                .expect("endpoint should be valid")
                .as_str(),
            "http://host/v1/chat/completions"
        );
    }

    #[test]
    fn endpoint_supports_path_template_and_query_interpolation() {
        let mut spec = spec_with_base_url("azure", "https://resource.example");
        spec.path_template = Some("openai/deployments/{model}/chat/completions".into());
        spec.query
            .insert("api-version".into(), "2024-10-21&unexpected=true".into());
        // SAFETY: single-threaded test binary section; unique variable name.
        unsafe { std::env::remove_var("QCG_LLM_AZURE_VERSION_TEST") };

        let provider = HttpProvider::from_spec(spec);

        assert_eq!(
            provider
                .endpoint_for("deploy-1")
                .expect("endpoint should be valid")
                .as_str(),
            "https://resource.example/openai/deployments/deploy-1/chat/completions?api-version=2024-10-21%26unexpected%3Dtrue"
        );
    }

    #[test]
    fn endpoint_encodes_model_as_a_path_segment() {
        let mut spec = spec_with_base_url("x", "https://resource.example/v1");
        spec.path_template = Some("deployments/{model}/chat".into());
        let provider = HttpProvider::from_spec(spec);

        assert_eq!(
            provider
                .endpoint_for("deployment/with?delimiters")
                .expect("endpoint should be valid")
                .as_str(),
            "https://resource.example/v1/deployments/deployment%2Fwith%3Fdelimiters/chat"
        );
    }

    #[test]
    fn query_rejects_credential_environment_placeholders() {
        for value in [
            "{QCG_PROVIDER_API_KEY_XYZ}",
            "{QCG_PROVIDER_KEY_XYZ}",
            "prefix-{QCG_PROVIDER_TOKEN_XYZ}",
            "{QCG_PROVIDER_SECRET_XYZ}",
            "{QCG_PROVIDER_PASSWORD_XYZ}",
            "{QCG_PROVIDER_AUTH_XYZ}",
            "{QCG_PROVIDER_CREDENTIAL_XYZ}",
        ] {
            let mut spec = spec_with_base_url("query-secret", "https://example.test/v1");
            spec.query.insert("version".into(), value.into());
            let error = ProvidersFile {
                default: None,
                provider: vec![spec],
                search_provider: vec![],
                mcp_server: vec![],
            }
            .validate()
            .expect_err("credential query interpolation must be rejected");
            assert!(error.contains("must not interpolate credential"), "{error}");
        }

        let mut spec = spec_with_base_url("query-secret", "https://example.test/v1");
        spec.query.insert("api-key".into(), "literal".into());
        let error = spec
            .validate()
            .expect_err("credential-like query names must be rejected");
        assert!(error.contains("must not carry credentials"), "{error}");
    }

    #[test]
    fn query_rejects_the_configured_credential_env_without_name_heuristics() {
        let mut spec = spec_with_base_url("query-secret", "https://example.test/v1");
        spec.api_key_env = Some("QCG_LLM_AUTH_XYZ".into());
        spec.query
            .insert("version".into(), "{QCG_LLM_AUTH_XYZ}".into());

        let error = spec
            .validate()
            .expect_err("the configured credential must not be interpolated");
        assert!(error.contains("QCG_LLM_AUTH_XYZ"), "{error}");
    }

    #[test]
    fn query_allows_non_credential_environment_placeholders() {
        let mut spec = spec_with_base_url("query-version", "https://example.test/v1");
        spec.query.insert(
            "api-version".into(),
            "{QCG_PROVIDER_API_VERSION_XYZ}".into(),
        );
        assert!(
            spec.validate().is_ok(),
            "version placeholders are not credentials"
        );
    }

    #[test]
    fn credential_like_environment_names_use_token_boundaries() {
        for name in [
            "QCG_PROVIDER_APIKEY_XYZ",
            "QCG_PROVIDER_AUTH_XYZ",
            "QCG_PROVIDER_KEY_XYZ",
            "QCG_PROVIDER_PASSWORD_XYZ",
            "QCG_PROVIDER_TOKEN_XYZ",
        ] {
            assert!(
                credential_like_name(name),
                "{name} should be credential-like"
            );
        }
        for name in [
            "QCG_PROVIDER_API_VERSION_XYZ",
            "QCG_PROVIDER_AUTHORITY_XYZ",
            "QCG_PROVIDER_KEYBOARD_XYZ",
            "QCG_PROVIDER_PASSWORDLESS_XYZ",
            "QCG_PROVIDER_TOKENIZER_XYZ",
        ] {
            assert!(
                !credential_like_name(name),
                "{name} should not be classified as a credential"
            );
        }
    }

    #[test]
    fn base_url_rejects_credential_environment_placeholders() {
        let mut spec = spec_with_base_url(
            "base-secret",
            "https://example.test/v1/{QCG_PROVIDER_API_KEY_XYZ}",
        );
        spec.api_key_env = None;
        let error = spec
            .validate()
            .expect_err("base URL must not interpolate credentials");
        assert!(error.contains("must not interpolate credential"), "{error}");
    }

    #[test]
    fn base_url_rejects_credential_like_placeholders() {
        for name in [
            "QCG_PROVIDER_AUTH_XYZ",
            "QCG_PROVIDER_KEY_XYZ",
            "QCG_PROVIDER_PASSWORD_XYZ",
        ] {
            let mut spec = spec_with_base_url(
                "base-secret",
                &format!("https://example.test/v1/{{{name}}}"),
            );
            spec.api_key_env = None;
            let error = spec
                .validate()
                .expect_err("base URL must not interpolate credentials");
            assert!(error.contains("must not interpolate credential"), "{error}");
        }
    }

    #[test]
    fn base_url_rejects_the_configured_credential_env_without_name_heuristics() {
        let mut spec =
            spec_with_base_url("base-secret", "https://example.test/v1/{QCG_LLM_AUTH_XYZ}");
        spec.api_key_env = Some("QCG_LLM_AUTH_XYZ".into());

        let error = spec
            .validate()
            .expect_err("the configured credential must not be interpolated");
        assert!(error.contains("QCG_LLM_AUTH_XYZ"), "{error}");
    }

    #[test]
    fn base_url_rejects_userinfo_query_and_fragment() {
        for (base_url, expected) in [
            ("https://user:password@example.test/v1", "userinfo"),
            ("https://example.test/v1?api-version=1", "query"),
            ("https://example.test/v1#fragment", "fragment"),
        ] {
            let mut spec = spec_with_base_url("unsafe", base_url);
            spec.api_key_env = None;
            let provider = HttpProvider::from_spec(spec);
            let error = provider
                .configuration_error_for("unsafe")
                .expect("unsafe base URL should be rejected");
            assert!(error.contains(expected), "{error}");
            assert!(!error.contains("password"), "{error}");
        }
    }

    #[test]
    fn credentialed_http_requires_https_except_for_loopback() {
        let mut remote = spec_with_base_url("remote", "http://example.test/v1");
        remote.api_key_env = Some("QCG_LLM_HTTP_REMOTE_KEY_XYZ".into());
        let provider = HttpProvider::from_spec(remote);
        let error = provider
            .configuration_error_for("remote")
            .expect("remote credentialed HTTP should be rejected");
        assert!(error.contains("loopback"), "{error}");

        // SAFETY: single-threaded test section; unique variable name.
        unsafe { std::env::set_var("QCG_LLM_HTTP_LOOPBACK_KEY_XYZ", "loopback-secret") };
        let mut loopback = spec_with_base_url("loopback", "http://127.0.0.7:8080/v1");
        loopback.api_key_env = Some("QCG_LLM_HTTP_LOOPBACK_KEY_XYZ".into());
        let provider = HttpProvider::from_spec(loopback);
        assert!(
            provider.configuration_error_for("loopback").is_none(),
            "loopback HTTP should be allowed"
        );
        // SAFETY: see above; restore the unique test variable.
        unsafe { std::env::remove_var("QCG_LLM_HTTP_LOOPBACK_KEY_XYZ") };
    }

    #[test]
    fn credential_env_names_expose_names_without_values() {
        // SAFETY: single-threaded test section; unique variable name.
        unsafe { std::env::set_var("QCG_LLM_ENV_NAME_ONLY_XYZ", "do-not-expose") };
        let mut spec = spec_with_base_url("keyed", "https://example.test/v1");
        spec.api_key_env = Some("QCG_LLM_ENV_NAME_ONLY_XYZ".into());
        let provider = HttpProvider::from_spec(spec);
        assert_eq!(
            provider.credential_env_names(),
            vec!["QCG_LLM_ENV_NAME_ONLY_XYZ".to_owned()]
        );
        // SAFETY: see above; restore the unique test variable.
        unsafe { std::env::remove_var("QCG_LLM_ENV_NAME_ONLY_XYZ") };
    }

    #[tokio::test]
    #[ignore = "requires loopback socket permissions"]
    async fn redirects_are_not_followed() {
        let (base_url, server) = spawn_http_response(
            302,
            "upstream redirect body must stay private".into(),
            "Location: http://127.0.0.1:1/redirected\r\n".into(),
        );
        let provider = HttpProvider::from_spec(spec_with_base_url("redirect", &base_url));
        let mut request = sample_request();
        request.seed = None;

        let error = provider
            .complete(request)
            .await
            .expect_err("redirect responses must not be followed");
        assert_eq!(error.kind, LlmErrorKind::HttpStatus(302));
        assert!(!error.message.contains("upstream redirect body"));
        server.join().expect("test server should stop");
    }

    #[tokio::test]
    #[ignore = "requires loopback socket permissions"]
    async fn non_success_body_is_not_returned_in_error() {
        let (base_url, server) =
            spawn_http_response(401, "sensitive upstream error details".into(), "".into());
        let provider = HttpProvider::from_spec(spec_with_base_url("status", &base_url));
        let mut request = sample_request();
        request.seed = None;

        let error = provider
            .complete(request)
            .await
            .expect_err("non-success responses must fail");
        assert_eq!(error.kind, LlmErrorKind::HttpStatus(401));
        assert!(!error.message.contains("sensitive upstream error details"));
        server.join().expect("test server should stop");
    }

    #[tokio::test]
    #[ignore = "requires loopback socket permissions"]
    async fn reflected_credential_is_never_returned() {
        let key = "qcg<reflected-credential-unique";
        // SAFETY: single-threaded test section; unique variable name.
        unsafe { std::env::set_var("QCG_LLM_REFLECTION_KEY_XYZ", key) };
        let body =
            r#"{"choices":[{"message":{"content":"qcg\u003creflected-credential-unique"}}]}"#
                .to_string();
        let (base_url, server) = spawn_http_response(200, body, "".into());
        let mut spec = spec_with_base_url("reflection", &base_url);
        spec.api_key_env = Some("QCG_LLM_REFLECTION_KEY_XYZ".into());
        let provider = HttpProvider::from_spec(spec);
        let mut request = sample_request();
        request.seed = None;

        let error = provider
            .complete(request)
            .await
            .expect_err("a reflected credential must fail closed");
        assert!(!error.message.contains(key));
        assert!(error.message.contains("configured credential"));
        // SAFETY: see above; restore the unique test variable.
        unsafe { std::env::remove_var("QCG_LLM_REFLECTION_KEY_XYZ") };
        server.join().expect("test server should stop");
    }

    #[tokio::test]
    #[ignore = "requires loopback socket permissions"]
    async fn oversized_response_body_is_rejected_before_json_parsing() {
        let body = r#"{"oversized":"sensitive upstream body"}"#.to_string();
        let (base_url, server) = spawn_http_response(200, body, "".into());
        let mut spec = spec_with_base_url("bounded", &base_url);
        spec.response_body_limit_bytes = Some(8);
        let provider = HttpProvider::from_spec(spec);
        let mut request = sample_request();
        request.seed = None;

        let error = provider
            .complete(request)
            .await
            .expect_err("an oversized response must fail closed");
        assert_eq!(error.kind, LlmErrorKind::InvalidResponse);
        assert!(error.message.contains("response_body_limit_bytes"));
        assert!(!error.message.contains("sensitive upstream body"));
        server.join().expect("test server should stop");
    }

    #[test]
    fn decoded_json_reflection_scan_catches_escaped_credentials() {
        let value: Value = serde_json::from_str(
            r#"{"choices":[{"message":{"content":"qcg\u003creflected-credential"}}]}"#,
        )
        .expect("response should be valid JSON");
        assert!(json_contains_string_fragment(
            &value,
            "qcg<reflected-credential"
        ));
    }

    #[tokio::test]
    async fn reqwest_errors_do_not_expose_endpoint_url() {
        let provider = HttpProvider::from_spec(spec_with_base_url(
            "network",
            "http://127.0.0.1:9/qcg-llm-network-test",
        ));
        let mut request = sample_request();
        request.seed = None;

        let error = provider
            .complete(request)
            .await
            .expect_err("closed endpoint should fail");
        assert_eq!(error.kind, LlmErrorKind::Network);
        assert!(!error.message.contains("127.0.0.1"));
        assert!(!error.message.contains("qcg-llm-network-test"));
    }

    #[test]
    fn missing_api_key_is_reported_as_configuration_error() {
        let mut spec = spec_with_base_url("keyed", "https://example.invalid/v1");
        spec.api_key_env = Some("QCG_LLM_MISSING_KEY_XYZ".into());

        let provider = HttpProvider::from_spec(spec);

        let error = provider
            .configuration_error_for("keyed")
            .expect("missing key should be reported");
        assert!(error.contains("QCG_LLM_MISSING_KEY_XYZ"), "{error}");
        assert!(provider.configuration_error_for("other").is_none());
    }

    #[test]
    fn unresolved_base_url_placeholder_is_a_configuration_error() {
        let mut spec = spec_with_base_url(
            "cloudy",
            "https://api.example/accounts/{QCG_LLM_MISSING_ACCOUNT_XYZ}/ai/v1",
        );

        // Simulate an env override that is absent so the literal placeholder path runs.
        spec.base_url_env = Some("QCG_LLM_MISSING_OVERRIDE_XYZ".into());

        let provider = HttpProvider::from_spec(spec);

        let error = provider
            .configuration_error_for("cloudy")
            .expect("unresolved placeholder should be reported");
        assert!(error.contains("QCG_LLM_MISSING_ACCOUNT_XYZ"), "{error}");
    }

    #[test]
    fn providers_file_rejects_unknown_keys_and_duplicates() {
        let parsed = ProvidersFile::parse(
            r#"
[[provider]]
id = "a"
api = "chat_completions"
base_url = "https://example.invalid"

[[provider]]
id = "a"
api = "responses"
base_url = "https://example.invalid"
"#,
        );
        assert!(parsed.is_err());
        assert!(parsed.unwrap_err().contains("duplicate"));

        let parsed = ProvidersFile::parse(
            r#"
[[provider]]
id = "a"
api = "chat_completions"
base_url = "https://example.invalid"
unknown_field = true
"#,
        );
        assert!(parsed.is_err());

        let parsed = ProvidersFile::parse(
            r#"
[[provider]]
id = "a"
api = "chat_completions"
"#,
        );
        assert!(parsed.is_err());
        assert!(parsed.unwrap_err().contains("base_url"));
    }

    #[test]
    fn providers_registry_rejects_excessive_file_size_and_entry_count() {
        let mut entries = String::new();
        for index in 0..=MAX_PROVIDER_ENTRIES_PER_KIND {
            entries.push_str(&format!(
                "[[provider]]\nid = \"provider-{index}\"\napi = \"chat_completions\"\nbase_url = \"https://example.invalid\"\n"
            ));
        }
        let error = ProvidersFile::parse(&entries)
            .expect_err("provider entry count beyond the bound must fail");
        assert!(error.contains("more than"), "{error}");

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should follow the Unix epoch")
            .as_nanos();
        let path = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-providers-limit-{unique}.toml")),
        )
        .expect("temporary path must be UTF-8");
        std::fs::write(&path, vec![b' '; MAX_PROVIDERS_FILE_BYTES + 1])
            .expect("oversized registry fixture should be written");
        let error = LlmRouter::from_file(&path)
            .expect_err("oversized provider registry file must fail before parsing");
        assert!(error.to_string().contains("exceeds"), "{error}");
        std::fs::remove_file(path).expect("temporary registry should be removed");
    }

    #[test]
    fn providers_file_rejects_invalid_capability_and_transport_combinations() {
        for (source, expected) in [
            (
                r#"
[[provider]]
id = "typo"
api = "chat_completions"
base_url = "https://example.invalid"
capabilities = { reasonng_effort = ["high"] }
"#,
                "reasonng_effort",
            ),
            (
                r#"
[[provider]]
id = "anthropic-reasoning"
api = "anthropic_messages"
base_url = "https://example.invalid"
capabilities = { reasoning_effort = ["high"] }
"#,
                "anthropic_messages",
            ),
            (
                r#"
[[provider]]
id = "anthropic-combined"
api = "anthropic_messages"
base_url = "https://example.invalid"
capabilities = { tool_use = true, json_schema = true, structured_output_with_tools = true }
"#,
                "structured_output_with_tools",
            ),
            (
                r#"
[[provider]]
id = "chat-reasoning"
api = "chat_completions"
base_url = "https://example.invalid"
capabilities = { reasoning_effort = ["high"] }
"#,
                "max_completion_tokens",
            ),
            (
                r#"
[[provider]]
id = "responses-seed"
api = "responses"
base_url = "https://example.invalid"
capabilities = { seed = true }
"#,
                "seed",
            ),
            (
                r#"
[[provider]]
id = "chat-verbosity"
api = "chat_completions"
base_url = "https://example.invalid"
capabilities = { verbosity = true }
"#,
                "Responses API",
            ),
        ] {
            let error = ProvidersFile::parse(source)
                .expect_err("invalid provider combinations must fail validation");
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn providers_file_rejects_invalid_defaults_reserved_ids_and_zero_limits() {
        for (source, expected) in [
            (
                r#"
[default]
model = { provider = "missing", model = "model" }
"#,
                "unregistered provider",
            ),
            (
                r#"
[[provider]]
id = "fake"
api = "chat_completions"
base_url = "https://example.invalid"
"#,
                "reserved",
            ),
            (
                r#"
[[provider]]
id = "invalid id"
api = "chat_completions"
base_url = "https://example.invalid"
"#,
                "lowercase ASCII",
            ),
            (
                r#"
[[provider]]
id = "bounded"
api = "chat_completions"
base_url = "https://example.invalid"
response_body_limit_bytes = 0
"#,
                "response_body_limit_bytes",
            ),
            (
                r#"
[[provider]]
id = "bounded"
api = "chat_completions"
base_url = "https://example.invalid"
timeout_seconds = 604801
"#,
                "timeout_seconds",
            ),
            (
                r#"
[[provider]]
id = "bounded"
api = "chat_completions"
base_url = "https://example.invalid"
response_body_limit_bytes = 67108865
"#,
                "response_body_limit_bytes",
            ),
            (
                r#"
[[provider]]
id = "bounded"
api = "chat_completions"
base_url = "https://example.invalid"
max_concurrency = 1025
"#,
                "max_concurrency",
            ),
        ] {
            let error = ProvidersFile::parse(source)
                .expect_err("invalid provider registry invariants must fail");
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn provider_reasoning_effort_levels_are_explicit() {
        let parsed = ProvidersFile::parse(
            r#"
[[provider]]
id = "reasoning"
api = "responses"
base_url = "https://example.invalid"
capabilities = { reasoning_effort = ["none", "high", "max"] }
"#,
        )
        .expect("reasoning provider should parse");
        assert_eq!(
            parsed.provider[0].capabilities.reasoning_effort,
            vec![
                ReasoningEffort::None,
                ReasoningEffort::High,
                ReasoningEffort::Max
            ]
        );
    }

    #[test]
    fn router_reports_configuration_errors_from_specs() {
        let router = LlmRouter::parse_text(
            r#"
[[provider]]
id = "keyed"
api = "chat_completions"
base_url = "https://example.invalid/v1"
api_key_env = "QCG_LLM_MISSING_KEY_XYZ"
"#,
        )
        .expect("router should build");

        let error = router
            .configuration_error_for("keyed")
            .expect("configuration error should surface");
        assert!(error.contains("QCG_LLM_MISSING_KEY_XYZ"), "{error}");
        assert!(router.configuration_error_for("fake").is_none());
        assert!(router.capabilities_for("keyed").is_some());
        assert!(router.capabilities_for("nope").is_none());
    }

    #[test]
    fn workspace_registry_enables_local_endpoints_by_default() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = Utf8PathBuf::from(manifest_dir).join("../../providers.toml");
        if !path.is_file() {
            panic!("workspace providers.toml must exist at {}", path);
        }
        let router = LlmRouter::from_file(&path).expect("providers.toml should be valid");
        for id in ["fake", "ollama", "lmstudio", "openai_compat"] {
            assert!(
                router.capabilities_for(id).is_some(),
                "{id} should stay active by default"
            );
        }
        for id in [
            "openai",
            "anthropic",
            "openrouter",
            "groq",
            "azure-openai",
            "opencode-zen",
        ] {
            assert!(
                router.capabilities_for(id).is_none(),
                "{id} must ship disabled until it is uncommented"
            );
        }
        assert!(router.default_model().is_none());
        assert_eq!(router.search_runtime().default_provider(), None);
        assert_eq!(
            router.search_runtime().provider_ids(),
            vec![
                "brave",
                "exa",
                "firecrawl",
                "parallel-advanced",
                "parallel-fast",
                "serpapi",
                "serper",
                "tavily",
                "tinyfish-api",
            ]
        );
        assert_eq!(
            router.mcp_runtime().server_ids(),
            vec!["exa-public", "parallel-public", "tinyfish"]
        );
        assert!(
            router
                .search_runtime()
                .credential_env_names()
                .contains(&"TINYFISH_API_KEY".to_string())
        );
    }

    /// Strips `# ` from commented `[[provider]]` blocks so the shipped
    /// catalog can be validated as real TOML. Header prose stays outside
    /// blocks and is never uncommented.
    fn uncomment_provider_blocks(text: &str) -> String {
        let mut out = String::new();
        let mut in_block = false;
        for line in text.lines() {
            if line.starts_with("# [[provider]]") {
                in_block = true;
            }
            if in_block {
                if line.is_empty() {
                    in_block = false;
                    out.push('\n');
                    continue;
                }
                match line.strip_prefix("# ") {
                    Some(rest) => out.push_str(rest),
                    None => out.push_str(line),
                }
                out.push('\n');
                continue;
            }
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    #[test]
    fn workspace_commented_rows_enable_into_valid_toml() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let path = Utf8PathBuf::from(manifest_dir).join("../../providers.toml");
        let text = std::fs::read_to_string(&path).expect("providers.toml should be readable");
        let enabled = uncomment_provider_blocks(&text);
        let router = LlmRouter::parse_text(&enabled)
            .expect("uncommenting every catalog row must yield a valid registry");
        for id in [
            "anthropic",
            "openai",
            "openai_responses",
            "ollama",
            "lmstudio",
            "openai_compat",
            "openrouter",
            "gemini",
            "sakura",
            "cloudflare",
            "opencode-go",
            "opencode-zen",
            "opencode-go-responses",
            "opencode-zen-responses",
            "groq",
            "deepseek",
            "mistral",
            "xai",
            "together",
            "fireworks",
            "azure-openai",
            "fake",
        ] {
            assert!(
                router.capabilities_for(id).is_some(),
                "{id} should register once its row is enabled"
            );
        }
        for id in [
            "anthropic",
            "openai",
            "groq",
            "deepseek",
            "mistral",
            "xai",
            "together",
            "fireworks",
            "azure-openai",
        ] {
            assert!(
                text.contains(&format!("# id = \"{id}\"")),
                "{id} must remain present as an enable-able template"
            );
        }
    }

    #[test]
    fn load_prefers_explicit_path_and_lists_candidates_when_missing() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-llm-load-test-{}-{}",
            std::process::id(),
            uuid_like_suffix()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let explicit = dir.join("explicit.toml");
        std::fs::write(
            &explicit,
            r#"
[default]
model = { provider = "fake", model = "fake" }

[[provider]]
id = "local"
api = "chat_completions"
base_url = "http://127.0.0.1:9/v1"
"#,
        )
        .expect("registry should be written");

        let router = LlmRouter::load(Some(
            Utf8PathBuf::from_path_buf(explicit.clone())
                .unwrap()
                .as_path(),
        ))
        .expect("explicit registry should load");
        assert!(router.capabilities_for("local").is_some());
        assert_eq!(
            router.default_model(),
            Some(&ModelSelection {
                provider: "fake".into(),
                model: "fake".into(),
            })
        );

        let missing = dir.join("missing.toml");
        let error = LlmRouter::load(Some(Utf8PathBuf::from_path_buf(missing).unwrap().as_path()))
            .expect_err("missing explicit registry should fail");
        assert!(error.to_string().contains("looked at"), "{error}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_optional_treats_explicit_paths_as_authoritative() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-llm-load-optional-explicit-{}-{}",
            std::process::id(),
            uuid_like_suffix()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");

        let explicit = dir.join("present.toml");
        std::fs::write(&explicit, REGISTRY_FIXTURE).expect("registry should be written");
        let router = LlmRouter::load_optional(Some(
            Utf8PathBuf::from_path_buf(explicit.clone())
                .unwrap()
                .as_path(),
        ))
        .expect("explicit registry should load")
        .expect("explicit registry should resolve");
        assert!(router.capabilities_for("local").is_some());

        let missing = dir.join("missing.toml");
        let error =
            LlmRouter::load_optional(Some(Utf8PathBuf::from_path_buf(missing).unwrap().as_path()))
                .expect_err("missing explicit registry must stay a hard error");
        assert!(error.is_not_found());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_optional_honors_the_environment_override() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-llm-load-optional-env-{}-{}",
            std::process::id(),
            uuid_like_suffix()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let present = dir.join("present.toml");
        std::fs::write(&present, REGISTRY_FIXTURE).expect("registry should be written");
        let missing = dir.join("missing.toml");

        // SAFETY: single-threaded test binary section; restored below.
        unsafe { std::env::set_var("QCG_PROVIDERS", &present) };
        let router = LlmRouter::load_optional(None)
            .expect("configured env registry should load")
            .expect("env override should resolve");
        assert!(router.capabilities_for("local").is_some());

        unsafe { std::env::set_var("QCG_PROVIDERS", &missing) };
        let error = LlmRouter::load_optional(None)
            .expect_err("missing env registry must stay a hard error");
        assert!(error.is_not_found());

        // SAFETY: see above; the variable is removed to restore state.
        unsafe { std::env::remove_var("QCG_PROVIDERS") };

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn uuid_like_suffix() -> u128 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    }

    const REGISTRY_FIXTURE: &str = r#"
[[provider]]
id = "local"
api = "chat_completions"
base_url = "http://127.0.0.1:9/v1"
"#;

    struct RetryProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl LlmProvider for RetryProvider {
        fn id(&self) -> &str {
            "retry"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }

        async fn complete(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Err(LlmError {
                    message: "rate limited".into(),
                    kind: LlmErrorKind::HttpStatus(429),
                });
            }
            Ok(ChatResponse {
                content: vec![ChatContent::Text("ok".into())],
                usage: TokenUsage {
                    input: 1,
                    output: 1,
                    reasoning: 0,
                },
                stop: StopReason::EndTurn,
                provider_state: None,
            })
        }
    }

    #[tokio::test]
    async fn router_retries_retryable_provider_error() {
        let provider = Arc::new(RetryProvider {
            calls: AtomicUsize::new(0),
        });
        let observed = Arc::clone(&provider);
        let mut router = LlmRouter {
            providers: BTreeMap::new(),
            default_model: None,
            search: SearchRuntime::unavailable(),
            mcp: qcg_mcp::McpRuntime::unavailable(),
        };
        router.register(provider);
        let response = router
            .complete(ChatRequest {
                provider: "retry".into(),
                model: "test".into(),
                system: None,
                messages: vec![],
                tools: vec![],
                response_schema: None,
                structured_output: StructuredOutputMode::Auto,
                temperature: None,
                top_p: None,
                max_tokens: 1,
                stop_sequences: vec![],
                seed: None,
                reasoning_effort: None,
                tool_choice: None,
                parallel_tool_calls: None,
                verbosity: None,
                stream: false,
            })
            .await
            .expect("router should retry once and succeed");
        assert_eq!(observed.calls.load(Ordering::SeqCst), 2);
        assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "ok"));
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        assert_eq!(retry_backoff(1), Duration::from_millis(200));
        assert_eq!(retry_backoff(2), Duration::from_millis(400));
        assert_eq!(retry_backoff(3), Duration::from_millis(800));
        assert_eq!(retry_backoff(100), Duration::from_millis(51_200));
    }

    #[test]
    fn retryability_is_structural_not_string_based() {
        assert!(!is_retryable_llm_error(&LlmError {
            message: "model gpt-500 failed".into(),
            kind: LlmErrorKind::Other,
        }));
        assert!(is_retryable_llm_error(&LlmError {
            message: "server error".into(),
            kind: LlmErrorKind::HttpStatus(503),
        }));
        assert!(is_retryable_llm_error(&LlmError {
            message: "slow".into(),
            kind: LlmErrorKind::TimedOut,
        }));
        assert!(!is_retryable_llm_error(&LlmError {
            message: "bad request".into(),
            kind: LlmErrorKind::HttpStatus(400),
        }));
        assert!(!is_retryable_llm_error(&LlmError {
            message: "refused".into(),
            kind: LlmErrorKind::Network,
        }));
        assert!(is_retryable_llm_error(&LlmError {
            message: "empty body".into(),
            kind: LlmErrorKind::EmptyResponse,
        }));
        assert!(!is_retryable_llm_error(&LlmError {
            message: "malformed response".into(),
            kind: LlmErrorKind::InvalidResponse,
        }));
    }
}
