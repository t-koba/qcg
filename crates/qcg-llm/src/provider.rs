use crate::search::{SearchProviderSpec, SearchRuntime};
use async_trait::async_trait;
use qcg_types::ReasoningEffort;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;

use crate::config::{
    credential_placeholder, is_env_name, is_provider_id, normalize_url_placeholders,
    validate_base_url, validate_query_parameters,
};
use crate::parse::{marker_block, marker_line};
use crate::router::LlmRouter;
use crate::types::{
    Capabilities, ChatContent, ChatRequest, ChatResponse, ChatStreamEvent, LlmError, StopReason,
    TokenUsage,
};
use crate::validate::validate_chat_request;

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
    /// Retry attempts per completion call, including the first.
    fn retry_attempts(&self) -> usize {
        3
    }
    /// Base backoff between retries; waits grow exponentially from here.
    fn retry_base_backoff(&self) -> Duration {
        Duration::from_millis(200)
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
    /// Retry attempts per completion call, including the first (default 3).
    #[serde(default)]
    pub retry_attempts: Option<usize>,
    /// Base backoff between retries in milliseconds (default 200, exponential).
    #[serde(default)]
    pub retry_base_backoff_ms: Option<u64>,
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
    pub(crate) fn validate(&self) -> Result<(), String> {
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
        match self.retry_attempts {
            Some(attempts) if !(1..=10).contains(&attempts) => {
                return Err(format!(
                    "provider `{}` retry_attempts must be between 1 and 10",
                    self.id
                ));
            }
            _ => {}
        }
        if self
            .retry_base_backoff_ms
            .is_some_and(|backoff| backoff > 60_000)
        {
            return Err(format!(
                "provider `{}` retry_base_backoff_ms must not exceed 60000",
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
        if self.response_body_limit_bytes == Some(0) {
            return Err(format!(
                "provider `{}` response_body_limit_bytes must be greater than zero",
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
        if self.circuit_breaker_cooldown_seconds == Some(0) {
            return Err(format!(
                "provider `{}` circuit_breaker_cooldown_seconds must be greater than zero",
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
        let file: ProvidersFile =
            toml::from_str(text).map_err(|error| format!("invalid providers registry: {error}"))?;
        file.validate()?;
        Ok(file)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
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
                            cached_input: 0,
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
                    cached_input: 0,
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
                    cached_input: 0,
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
                    cached_input: 0,
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
                cached_input: 0,
            },
            stop: StopReason::EndTurn,
            provider_state: None,
        })
    }
}
