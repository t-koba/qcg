use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::{Duration, sleep, timeout};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Capabilities {
    pub tool_use: bool,
    pub json_schema: bool,
    pub streaming: bool,
    pub seed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub provider: String,
    pub model: String,
    pub system: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ToolSpec>,
    pub response_schema: Option<Value>,
    pub temperature: Option<f32>,
    pub max_tokens: u32,
    pub seed: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    Refusal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmErrorKind {
    HttpStatus(u16),
    TimedOut,
    Network,
    InvalidResponse,
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
}

fn is_retryable_llm_error(error: &LlmError) -> bool {
    match error.kind {
        LlmErrorKind::TimedOut => true,
        // Routers sometimes return 200 with an empty body while the upstream
        // pool is saturated; retrying these transient empties is required for
        // shared-pool providers.
        LlmErrorKind::InvalidResponse => true,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelSelection {
    pub provider: String,
    pub model: String,
}

pub struct LlmRuntime {
    pub provider: Arc<dyn LlmProvider>,
    pub default_model: Option<ModelSelection>,
    /// Whether a providers registry file was resolved for this process. When
    /// false only the built-in `fake` provider is registered, and
    /// validation errors for other ids carry registry-setup guidance.
    pub registry_present: bool,
}

impl LlmRuntime {
    /// Runtime backed solely by the built-in `fake` provider. Used when no
    /// providers registry was named or found so that `fake`-only contracts
    /// keep working while other ids receive setup guidance.
    pub fn fake_only() -> Self {
        let router = LlmRouter::parse_text("").expect("an empty registry is valid");
        Self {
            provider: Arc::new(router),
            default_model: None,
            registry_present: false,
        }
    }
}

impl std::fmt::Debug for LlmRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmRuntime")
            .field("provider_id", &self.provider.id())
            .field("default_model", &self.default_model)
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
    pub auth_header: Option<String>,
    #[serde(default)]
    pub capabilities: Capabilities,
    #[serde(default)]
    pub path_template: Option<String>,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
}

impl ProviderSpec {
    fn validate(&self) -> Result<(), String> {
        if self.id.trim().is_empty() {
            return Err("[[provider]].id must not be empty".into());
        }
        if self.base_url.is_none() && self.base_url_env.is_none() {
            return Err(format!(
                "provider `{}` must declare `base_url` or `base_url_env`",
                self.id
            ));
        }
        if self
            .api_key_env
            .as_ref()
            .is_some_and(|env| env.trim().is_empty())
        {
            return Err(format!(
                "provider `{}` declares an empty `api_key_env`",
                self.id
            ));
        }
        if let Some(base_url) = self.base_url.as_deref() {
            if let Some(name) = credential_placeholder(base_url, self.api_key_env.as_deref()) {
                return Err(format!(
                    "provider `{}` base_url must not interpolate credential environment variable `{name}`",
                    self.id
                ));
            }
            let validation_url = normalize_url_placeholders(base_url);
            validate_base_url(&validation_url, self.api_key_env.is_some()).map_err(|error| {
                format!("provider `{}` has an invalid `base_url`: {error}", self.id)
            })?;
        }
        validate_query_parameters(&self.query, self.api_key_env.as_deref())
            .map_err(|error| format!("provider `{}` has an invalid `query`: {error}", self.id))?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultSection {
    pub model: ModelSelection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvidersFile {
    #[serde(default)]
    pub default: Option<DefaultSection>,
    #[serde(default)]
    pub provider: Vec<ProviderSpec>,
}

impl ProvidersFile {
    pub fn parse(text: &str) -> Result<Self, String> {
        let file: ProvidersFile =
            toml::from_str(text).map_err(|error| format!("invalid providers registry: {error}"))?;
        file.validate()?;
        Ok(file)
    }

    fn validate(&self) -> Result<(), String> {
        let mut seen = BTreeMap::new();
        for spec in &self.provider {
            spec.validate()?;
            if seen.insert(spec.id.as_str(), ()).is_some() {
                return Err(format!("duplicate provider id `{}`", spec.id));
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
            streaming: false,
            seed: true,
        }
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
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
                        },
                        stop: StopReason::ToolUse,
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
                },
                stop: StopReason::EndTurn,
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
                },
                stop: StopReason::ToolUse,
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
                },
                stop: StopReason::ToolUse,
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
        } else if req.response_schema.is_some() {
            "{}".into()
        } else {
            prompt.to_string()
        };
        Ok(ChatResponse {
            content: vec![ChatContent::Text(text)],
            usage: TokenUsage {
                input: prompt.len() as u64,
                output: 0,
            },
            stop: StopReason::EndTurn,
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

fn credential_like_env_name(name: &str) -> bool {
    name.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_uppercase())
        .any(|token| {
            [
                "APIKEY",
                "APITOKEN",
                "AUTH",
                "AUTHORIZATION",
                "BEARER",
                "CREDENTIAL",
                "CREDENTIALS",
                "KEY",
                "PASSWORD",
                "PASSWD",
                "SECRET",
                "TOKEN",
            ]
            .iter()
            .any(|marker| {
                token == *marker
                    || token.strip_prefix(marker).is_some_and(|suffix| {
                        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit())
                    })
            })
        })
}

fn credential_placeholder(value: &str, credential_env: Option<&str>) -> Option<String> {
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        let close_offset = rest[start..].find('}')?;
        let name = &rest[start + 1..start + close_offset];
        if credential_env.is_some_and(|credential_env| name == credential_env)
            || credential_like_env_name(name)
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
            || credential_like_env_name(key)
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
    capabilities: Capabilities,
    path_template: Option<String>,
    query: BTreeMap<String, String>,
    timeout_seconds: u64,
    config_errors: Vec<String>,
}

impl HttpProvider {
    pub fn from_spec(spec: ProviderSpec) -> Self {
        let mut config_errors = Vec::new();
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
            Some(raw) => match credential_placeholder(&raw, spec.api_key_env.as_deref()) {
                Some(name) => {
                    config_errors.push(format!(
                        "provider `{}` base_url must not interpolate credential environment variable `{name}`",
                        spec.id
                    ));
                    None
                }
                None => match interpolate_env(&raw) {
                    Ok(resolved) => {
                        match validate_base_url(&resolved, spec.api_key_env.is_some()) {
                            Ok(url) => Some(url),
                            Err(error) => {
                                config_errors.push(format!(
                                    "provider `{}` has an invalid base_url: {error}",
                                    spec.id
                                ));
                                None
                            }
                        }
                    }
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
        let query_is_safe =
            match validate_query_parameters(&spec.query, spec.api_key_env.as_deref()) {
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
        Self {
            id: spec.id,
            api: spec.api,
            base_url,
            auth_header: spec.auth_header,
            credential_env: spec.api_key_env,
            capabilities: spec.capabilities,
            path_template: spec.path_template,
            query,
            timeout_seconds: spec.timeout_seconds.unwrap_or(120),
            config_errors,
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
        let Some(name) = self.credential_env.as_deref() else {
            return Ok(None);
        };
        match std::env::var(name) {
            Ok(value) if !value.is_empty() => Ok(Some(value)),
            Ok(_) | Err(_) => Err(LlmError::new(format!(
                "set `{name}` before running the generator"
            ))),
        }
    }

    fn credential_configuration_error(&self) -> Option<String> {
        let name = self.credential_env.as_deref()?;
        match std::env::var(name) {
            Ok(value) if !value.is_empty() => None,
            Ok(_) | Err(_) => Some(format!("set `{name}` before running the generator")),
        }
    }

    async fn send(&self, payload: Value, model: &str) -> Result<ChatResponse, LlmError> {
        if let Some(error) = self.configuration_error_for(&self.id) {
            return Err(LlmError::new(error));
        }
        let credential = self.credential_for_request()?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(llm_http_error)?;
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
        let body = response.text().await.map_err(llm_http_error)?;
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
        self.credential_env.iter().cloned().collect()
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
        let payload = match self.api {
            ApiFlavor::ChatCompletions => {
                chat_completions_payload(&req, req.seed.is_some() && self.capabilities.seed)
            }
            ApiFlavor::Responses => responses_payload(&req),
            ApiFlavor::AnthropicMessages => anthropic_payload(&req),
        };
        self.send(payload, &req.model).await
    }
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
}

impl std::fmt::Debug for LlmRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmRouter")
            .field("provider_ids", &self.provider_ids())
            .field("default_model", &self.default_model)
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
        let text = std::fs::read_to_string(path).map_err(|source| RegistryError::Read {
            path: path.to_path_buf(),
            source,
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
        let mut router = Self {
            providers: BTreeMap::new(),
            default_model: file.default.map(|default| default.model),
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

    pub fn into_runtime(self) -> LlmRuntime {
        LlmRuntime {
            default_model: self.default_model.clone(),
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
            streaming: false,
            seed: true,
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
            Err(error) if attempt < MAX_ATTEMPTS && is_retryable_llm_error(&error) => {
                let slow_down = matches!(
                    error.kind,
                    LlmErrorKind::HttpStatus(429) | LlmErrorKind::InvalidResponse
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

fn chat_completions_payload(req: &ChatRequest, send_seed: bool) -> Value {
    let mut payload = json!({
        "model": req.model,
        "messages": openai_messages(req),
        "max_tokens": req.max_tokens,
    });
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    if send_seed && let Some(seed) = req.seed {
        payload["seed"] = json!(seed);
    }
    if let Some(schema) = &req.response_schema {
        payload["response_format"] = json!({
            "type": "json_schema",
            "json_schema": {
                "name": "qcg_response",
                "strict": true,
                "schema": schema
            }
        });
    }
    if !req.tools.is_empty() {
        payload["tools"] = Value::Array(req.tools.iter().map(openai_tool).collect());
        payload["tool_choice"] = Value::String("auto".into());
    }
    payload
}

fn responses_payload(req: &ChatRequest) -> Value {
    let mut payload = json!({
        "model": req.model,
        "input": responses_input(req),
        "max_output_tokens": req.max_tokens,
    });
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(schema) = &req.response_schema {
        payload["text"] = json!({
            "format": {
                "type": "json_schema",
                "name": "qcg_response",
                "strict": true,
                "schema": schema
            }
        });
    }
    if !req.tools.is_empty() {
        payload["tools"] = Value::Array(req.tools.iter().map(responses_tool).collect());
        payload["tool_choice"] = Value::String("auto".into());
    }
    payload
}

fn anthropic_payload(req: &ChatRequest) -> Value {
    let mut tools: Vec<Value> = req.tools.iter().map(anthropic_tool).collect();
    if let Some(schema) = &req.response_schema {
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
    if !tools.is_empty() {
        payload["tools"] = Value::Array(tools);
    }
    if payload["tools"]
        .as_array()
        .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "qcg_response"))
    {
        payload["tool_choice"] = json!({ "type": "tool", "name": "qcg_response" });
    }
    payload
}

fn openai_messages(req: &ChatRequest) -> Vec<Value> {
    let mut messages = Vec::new();
    if let Some(system) = &req.system {
        messages.push(json!({ "role": "system", "content": system }));
    }
    messages.extend(
        req.messages
            .iter()
            .map(|message| json!({ "role": message.role, "content": message.content })),
    );
    messages
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
    messages.extend(req.messages.iter().map(|message| {
        let role = if message.role == "tool" {
            "user"
        } else {
            message.role.as_str()
        };
        json!({ "role": role, "content": message.content })
    }));
    messages
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
                    "tool_use_id": "qcg-tool-result",
                    "content": message.content,
                }]
            })),
            role => Some(json!({
                "role": role,
                "content": message.content,
            })),
        })
        .collect()
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
        input: value
            .pointer("/usage/prompt_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output: value
            .pointer("/usage/completion_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    };
    let stop = match value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(Value::as_str)
    {
        Some("length") => StopReason::MaxTokens,
        Some("tool_calls") => StopReason::ToolUse,
        Some("content_filter") => StopReason::Refusal,
        _ => StopReason::EndTurn,
    };
    Ok(ChatResponse {
        content,
        usage,
        stop,
    })
}

fn parse_openai_tool_call(call: &Value) -> Result<ChatContent, LlmError> {
    let id = call
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
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
        .unwrap_or("{}");
    let args = serde_json::from_str(args_text).map_err(|error| {
        LlmError::invalid_response(format!(
            "OpenAI-compatible tool call arguments were invalid JSON: {error}"
        ))
    })?;
    Ok(ChatContent::ToolCall { id, name, args })
}

fn parse_responses_response(value: Value) -> Result<ChatResponse, LlmError> {
    let mut content = Vec::new();
    if let Some(text) = value.get("output_text").and_then(Value::as_str)
        && !text.is_empty()
    {
        content.push(ChatContent::Text(text.to_string()));
    }
    for item in value
        .get("output")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
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
                    .unwrap_or_default()
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
                    .unwrap_or("{}");
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
        input: value
            .pointer("/usage/input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output: value
            .pointer("/usage/output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    };
    let stop = match value.get("status").and_then(Value::as_str) {
        Some("incomplete") => StopReason::MaxTokens,
        _ if content
            .iter()
            .any(|item| matches!(item, ChatContent::ToolCall { .. })) =>
        {
            StopReason::ToolUse
        }
        _ => StopReason::EndTurn,
    };
    Ok(ChatResponse {
        content,
        usage,
        stop,
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
                    .unwrap_or_default()
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
        input: value
            .pointer("/usage/input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output: value
            .pointer("/usage/output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    };
    let stop = match value.get("stop_reason").and_then(Value::as_str) {
        Some("tool_use") => StopReason::ToolUse,
        Some("max_tokens") => StopReason::MaxTokens,
        Some("refusal") => StopReason::Refusal,
        _ => StopReason::EndTurn,
    };
    Ok(ChatResponse {
        content,
        usage,
        stop,
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
            messages: vec![ChatMessage {
                role: "user".into(),
                content: "hello".into(),
            }],
            tools: vec![],
            response_schema: None,
            temperature: Some(0.5),
            max_tokens: 128,
            seed: Some(42),
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
    fn chat_completions_payload_gates_seed_by_capability() {
        let request = sample_request();

        let payload = chat_completions_payload(&request, true);
        assert_eq!(payload["seed"], 42);

        let payload = chat_completions_payload(&request, false);
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

        let payload = chat_completions_payload(&request, false);

        assert!(payload.get("temperature").is_none());
        assert_eq!(payload["max_tokens"], 128);
        assert_eq!(payload["response_format"]["type"], "json_schema");
        assert_eq!(payload["tool_choice"], "auto");
        assert_eq!(payload["messages"][0]["role"], "system");
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
        ProviderSpec {
            id: id.into(),
            api: ApiFlavor::ChatCompletions,
            base_url: Some(base_url.into()),
            base_url_env: None,
            api_key_env: None,
            auth_header: None,
            capabilities: Capabilities::default(),
            path_template: None,
            query: BTreeMap::new(),
            timeout_seconds: None,
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
                credential_like_env_name(name),
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
                !credential_like_env_name(name),
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

        let error = provider
            .complete(sample_request())
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

        let error = provider
            .complete(sample_request())
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

        let error = provider
            .complete(sample_request())
            .await
            .expect_err("a reflected credential must fail closed");
        assert!(!error.message.contains(key));
        assert!(error.message.contains("configured credential"));
        // SAFETY: see above; restore the unique test variable.
        unsafe { std::env::remove_var("QCG_LLM_REFLECTION_KEY_XYZ") };
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

        let error = provider
            .complete(sample_request())
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
                },
                stop: StopReason::EndTurn,
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
                temperature: None,
                max_tokens: 1,
                seed: None,
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
            kind: LlmErrorKind::InvalidResponse,
        }));
    }
}
