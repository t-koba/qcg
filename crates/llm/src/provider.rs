use crate::search::{SearchProviderSpec, SearchRuntime};
use async_trait::async_trait;
use model::ReasoningEffort;
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
    Capabilities, ChatContent, ChatRequest, ChatResponse, ChatStreamEvent, LlmError, PromptCache,
    StopReason, TokenUsage,
};
use crate::validate::validate_chat_request;

/// Default floor between rate-limited (429) and empty-response retries, in
/// milliseconds. Provider rows may lower it to zero or raise it up to the
/// mechanistic ceiling of 60000.
pub const DEFAULT_RETRY_RATE_LIMIT_FLOOR_MS: u64 = 5000;
/// Default cap on the exponential retry-backoff exponent. Provider rows may
/// set any cap within the mechanistic range 1..=16.
pub const DEFAULT_RETRY_BACKOFF_EXPONENT_CAP: u32 = 8;

#[async_trait]
pub trait LlmProvider: Send + Sync {
    fn id(&self) -> &str;
    fn capabilities(&self) -> Capabilities;
    fn capabilities_for(&self, provider: &str) -> Option<Capabilities> {
        (provider == self.id()).then(|| self.capabilities())
    }
    /// Effective capabilities for one model of a provider. Registry rows may
    /// declare per-model capabilities and effort lists; the default keeps the
    /// provider-level value, which preserves generic rows that enumerate no
    /// models.
    fn model_capabilities_for(&self, provider: &str, _model: &str) -> Option<Capabilities> {
        self.capabilities_for(provider)
    }
    /// Unit prices declared by the model catalog, when any. Contract-level
    /// prices still win; this only fills the gap for operator-selected models.
    fn model_pricing_for(&self, _provider: &str, _model: &str) -> Option<ModelPricing> {
        None
    }
    /// Declared extended-retention mechanism for billing the requested tier.
    /// This is route/model metadata, never inferred from usage or model names.
    fn prompt_cache_retention_for(
        &self,
        _provider: &str,
        _model: &str,
    ) -> Option<PromptCacheRetention> {
        None
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
    /// Provider-owned failure decisions. The router enforces attempt and
    /// stream-emission guards; policy never grants route authorization.
    fn retry_policy(&self) -> crate::RetryPolicy {
        crate::RetryPolicy::default()
    }

    /// Like `complete`, but reports failed-attempt decisions even if a later
    /// retry succeeds. Leaf providers need not override this; the router owns
    /// retry loops and invokes the observer synchronously before each action.
    async fn complete_observed(
        &self,
        req: ChatRequest,
        _observer: crate::RetryObserver,
    ) -> Result<ChatResponse, LlmError> {
        self.complete(req).await
    }

    async fn decide_observed(
        &self,
        req: crate::DecisionRequest,
        _observer: crate::RetryObserver,
    ) -> Result<crate::DecisionResponse, LlmError> {
        self.decide(req).await
    }

    async fn stream_observed(
        &self,
        req: ChatRequest,
        events: mpsc::Sender<ChatStreamEvent>,
        _observer: crate::RetryObserver,
    ) -> Result<(), LlmError> {
        self.stream(req, events).await
    }

    /// Retry attempts per completion call, including the first.
    fn retry_attempts(&self) -> usize {
        3
    }
    /// Base backoff between retries; waits grow exponentially from here.
    fn retry_base_backoff(&self) -> Duration {
        Duration::from_millis(200)
    }
    /// Retry attempts for a stream that failed before delivering any event.
    /// Zero (the default) never retries a stream: once a delta is emitted a
    /// retry could mix model output, and even before the first delta the
    /// safe default is to surface the provider error.
    fn stream_retry_attempts(&self) -> usize {
        0
    }
    /// Minimum wait per attempt for rate-limited (429) and empty-response
    /// retries, multiplied by the attempt number. Zero disables the floor.
    fn retry_rate_limit_floor(&self) -> Duration {
        Duration::from_millis(DEFAULT_RETRY_RATE_LIMIT_FLOOR_MS)
    }
    /// Largest exponent applied to the exponential retry backoff.
    fn retry_backoff_exponent_cap(&self) -> u32 {
        DEFAULT_RETRY_BACKOFF_EXPONENT_CAP
    }
    fn supports_decisions_for(&self, _provider: &str) -> bool {
        false
    }

    async fn decide(
        &self,
        _req: crate::DecisionRequest,
    ) -> Result<crate::DecisionResponse, LlmError> {
        Err(LlmError::new("provider does not support typed decisions"))
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
    pub mcp: mcp::McpRuntime,
    /// Registry-derived model catalog shared by validation, the HTTP API, and
    /// the CLI. External sources and discovery are metadata only.
    pub catalog: Arc<crate::catalog::CatalogService>,
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
            mcp: mcp::McpRuntime::public_defaults(),
            catalog: Arc::new(crate::catalog::CatalogService::empty()),
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
            .field("catalog", &self.catalog)
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
    SystemOne,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatTokenLimitField {
    MaxTokens,
    MaxCompletionTokens,
}

/// Request-body mechanism carrying prompt-cache hints. Declared per provider
/// row like `chat_token_limit_field` because API flavors differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptCacheField {
    /// Anthropic Messages explicit `cache_control` block on the stable
    /// system prefix. Safe default for routes without automatic caching.
    CacheControl,
    /// Anthropic robust combination: explicit system marker plus top-level
    /// automatic caching for the growing conversation tail. Opt in only on
    /// routes that accept the top-level field; others keep `cache_control`.
    CacheControlAuto,
    /// OpenAI-compatible `prompt_cache_key` request field.
    PromptCacheKey,
}

/// Explicitly declared extended-retention mechanism for one route.
/// Model-aware: a `[[provider.models]]` entry may declare or override the
/// row value for that model only; never infer from the model-name string.
/// A provider-level declaration applies to every model in that row only as
/// an operator claim (the operator asserts every model on the row honors
/// the mechanism).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptCacheRetention {
    /// Older OpenAI/Azure model mechanism: `prompt_cache_retention: "24h"`
    /// (provider maximum-retention policy, not a deletion deadline).
    /// Valid for `chat_completions` and `responses`.
    #[serde(rename = "retention_24h")]
    Retention24h,
    /// OpenAI GPT-5.6+ Responses mechanism:
    /// `prompt_cache_options: {"ttl": "12h"}` (minimum lifetime, not a
    /// deletion deadline). Responses only.
    #[serde(rename = "ttl_12h")]
    Ttl12h,
    /// Anthropic Messages mechanism: add `cache_control: {"ttl": "1h"}`
    /// to every cache marker the row emits (system and top-level auto
    /// marker). Anthropic Messages only.
    #[serde(rename = "ttl_1h")]
    Ttl1h,
}

impl PromptCacheRetention {
    pub(crate) fn supports_api(self, api: ApiFlavor) -> bool {
        match self {
            Self::Retention24h => matches!(api, ApiFlavor::ChatCompletions | ApiFlavor::Responses),
            Self::Ttl12h => matches!(api, ApiFlavor::Responses),
            Self::Ttl1h => matches!(api, ApiFlavor::AnthropicMessages),
        }
    }
}

/// Decision request wire format. Mechanism only: how the logical
/// `{model, state, questions}` decision maps onto the HTTP JSON body.
/// Direct TypeSafe/OpenRouter/Vercel gateways send state and questions at the
/// top level; Cloudflare Workers AI wraps them under `input`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionInputWrapper {
    /// `{"model","state","questions"}` (or without `model` when
    /// `decision_model_in_body = false`). Default.
    #[default]
    None,
    /// `{"model","input":{"state","questions"}}` (or `{"input":{...}}` when
    /// the model travels in the path). Cloudflare `POST .../ai/run` style.
    Input,
}

/// Decision response wire format. Mechanism only: where the typed
/// `{model, answers, usage}` object lives in the HTTP JSON body.
/// Cloudflare's classic `POST .../ai/run/{model}` style nests it under
/// `result` alongside `success`/`errors`; direct endpoints return it at the
/// top level. `auto` (the default) accepts both without policy input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionResponseWrapper {
    /// Try `result` when the top level does not parse, else top level.
    /// Default: accepts both direct and Cloudflare envelopes.
    #[default]
    Auto,
    /// Top level only. Rejects Cloudflare `result` envelopes.
    Direct,
    /// Require the `result` field. Rejects direct responses.
    Result,
}

fn default_decision_model_in_body() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelsDiscovery {
    /// OpenAI-compatible model listing at `{base_url}/models`.
    Openai,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPricing {
    pub input_cost_per_million_usd: Option<f64>,
    /// Cache creation input USD/1M; absent falls back to ordinary input.
    pub cache_creation_input_cost_per_million_usd: Option<f64>,
    /// Cache read input USD/1M; absent falls back to ordinary input.
    pub cache_read_input_cost_per_million_usd: Option<f64>,
    pub output_cost_per_million_usd: Option<f64>,
    /// Context tiers: the rates that apply to a whole request once its
    /// prompt input reaches a tier's threshold.
    #[serde(default)]
    pub cost_tiers: Vec<policy::CostTier>,
}

impl ModelPricing {
    pub fn is_empty(&self) -> bool {
        self.input_cost_per_million_usd.is_none()
            && self.output_cost_per_million_usd.is_none()
            && self.cache_read_input_cost_per_million_usd.is_none()
            && self.cache_creation_input_cost_per_million_usd.is_none()
            && self.cost_tiers.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSpec {
    pub id: String,
    pub label: Option<String>,
    /// Full capability override for this model. When omitted the provider
    /// capabilities apply.
    pub capabilities: Option<Capabilities>,
    /// Shorthand that overrides only the provider's `reasoning_effort` list.
    pub reasoning_effort: Option<Vec<ReasoningEffort>>,
    pub input_cost_per_million_usd: Option<f64>,
    /// Cache creation input USD/1M; absent falls back to ordinary input.
    pub cache_creation_input_cost_per_million_usd: Option<f64>,
    /// Cache read input USD/1M; absent falls back to ordinary input.
    pub cache_read_input_cost_per_million_usd: Option<f64>,
    pub output_cost_per_million_usd: Option<f64>,
    /// Context tiers: the rates that apply to a whole request once its
    /// prompt input reaches a tier's threshold.
    #[serde(default)]
    pub cost_tiers: Vec<policy::CostTier>,
    /// Extended-retention mechanism for this model only. Overrides the
    /// provider row when set; `None` inherits the row value.
    pub prompt_cache_retention: Option<PromptCacheRetention>,
    /// When true, this model honors a write-disable via
    /// `prompt_cache_options: {"mode": "explicit"}` with no qcg explicit
    /// breakpoints (so no cache read/write). Overrides the provider row.
    pub prompt_cache_explicit_mode: Option<bool>,
    pub context_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    /// Disabled models stay usable by pinned contracts but are hidden from
    /// the selectable catalog. `None` means enabled.
    pub enabled: Option<bool>,
}

impl ModelSpec {
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    pub fn effective_capabilities(&self, provider: &Capabilities) -> Capabilities {
        let mut effective = self
            .capabilities
            .clone()
            .unwrap_or_else(|| provider.clone());
        if let Some(reasoning_effort) = &self.reasoning_effort {
            effective.reasoning_effort = reasoning_effort.clone();
        }
        effective
    }

    pub(crate) fn rates(&self) -> policy::CostRates {
        policy::CostRates {
            input: self.input_cost_per_million_usd,
            cache_read: self.cache_read_input_cost_per_million_usd,
            cache_creation: self.cache_creation_input_cost_per_million_usd,
            output: self.output_cost_per_million_usd,
        }
    }

    pub fn pricing(&self) -> Option<ModelPricing> {
        let pricing = ModelPricing {
            input_cost_per_million_usd: self.input_cost_per_million_usd,
            cache_creation_input_cost_per_million_usd: self
                .cache_creation_input_cost_per_million_usd,
            cache_read_input_cost_per_million_usd: self.cache_read_input_cost_per_million_usd,
            output_cost_per_million_usd: self.output_cost_per_million_usd,
            cost_tiers: self.cost_tiers.clone(),
        };
        (!pricing.is_empty()).then_some(pricing)
    }

    pub(crate) fn validate(&self, provider: &str, api: ApiFlavor) -> Result<(), String> {
        let invalid =
            |message: &str| format!("provider `{provider}` model `{}` {message}", self.id);
        if self.id.trim().is_empty() || self.id.chars().any(char::is_control) {
            return Err(format!(
                "provider `{provider}` has a model with an empty or invalid id"
            ));
        }
        if let Some(capabilities) = &self.capabilities {
            validate_capabilities_for_api(provider, api, capabilities)
                .map_err(|error| format!("model `{}`: {error}", self.id))?;
        }
        if let Some(field) = self.rates().invalid_rate() {
            return Err(invalid(&format!("{field} must be finite and non-negative")));
        }
        policy::validate_cost_tiers(&self.cost_tiers)
            .map_err(|error| invalid(&error.to_string()))?;
        if let Some(retention) = self.prompt_cache_retention {
            if !retention.supports_api(api) {
                return Err(invalid(
                    "has a `prompt_cache_retention` that is not valid for its api",
                ));
            }
            if let Some(capabilities) = &self.capabilities
                && !capabilities.prompt_cache
            {
                return Err(invalid(
                    "may only set `prompt_cache_retention` when prompt caching is enabled",
                ));
            }
        }
        if self.prompt_cache_explicit_mode == Some(true)
            && !matches!(api, ApiFlavor::ChatCompletions | ApiFlavor::Responses)
        {
            return Err(invalid(
                "may only set `prompt_cache_explicit_mode` for chat_completions or responses",
            ));
        }
        for (field, value) in [
            ("context_tokens", self.context_tokens),
            ("max_output_tokens", self.max_output_tokens),
        ] {
            if value == Some(0) {
                return Err(invalid(&format!("{field} must be greater than zero")));
            }
        }
        if let Some(efforts) = &self.reasoning_effort {
            for (index, effort) in efforts.iter().enumerate() {
                if efforts[index + 1..].contains(effort) {
                    return Err(invalid(&format!(
                        "advertises duplicate reasoning_effort `{effort}`"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSpec {
    pub id: String,
    pub api: ApiFlavor,
    pub base_url: Option<String>,
    pub base_url_env: Option<String>,
    pub api_key_env: Option<String>,
    pub api_key_file_env: Option<String>,
    pub auth_header: Option<String>,
    #[serde(default)]
    pub capabilities: Capabilities,
    #[serde(default)]
    pub models: Vec<ModelSpec>,
    pub models_discovery: Option<ModelsDiscovery>,
    /// External catalog provider key (for example a models.dev provider id).
    pub catalog_id: Option<String>,
    pub path_template: Option<String>,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    pub timeout_seconds: Option<u64>,
    /// Retry attempts per completion call, including the first (default 3).
    pub retry_attempts: Option<usize>,
    #[serde(default)]
    pub retry_policy: crate::RetryPolicy,
    /// Base backoff between retries in milliseconds (default 200, exponential).
    pub retry_base_backoff_ms: Option<u64>,
    /// Retry attempts for a stream that failed before delivering any event.
    pub stream_retry_attempts: Option<u32>,
    /// Minimum wait per rate-limited (429) or empty-response retry attempt in
    /// milliseconds (default 5000, multiplied by the attempt number). `0`
    /// disables the floor.
    pub retry_rate_limit_floor_ms: Option<u64>,
    /// Cap on the exponential retry-backoff exponent (default 8).
    pub retry_backoff_exponent_cap: Option<u32>,
    pub chat_token_limit_field: Option<ChatTokenLimitField>,
    /// How `[llm].cache = "auto"` is expressed for this row. Required when
    /// the `prompt_cache` capability is enabled and invalid otherwise.
    pub prompt_cache_field: Option<PromptCacheField>,
    /// Extended-retention mechanism for the `extended` tier on this row.
    /// Model entries may override per model. A provider-level value is an
    /// operator claim that every model on the row honors the mechanism.
    pub prompt_cache_retention: Option<PromptCacheRetention>,
    /// When true, models on this row honor `cache_write_policy = "forbid"`
    /// via `prompt_cache_options: {"mode": "explicit"}` with no qcg
    /// explicit breakpoints (so no cache read/write). Valid only for
    /// `chat_completions` and `responses`. Model entries may override.
    #[serde(default)]
    pub prompt_cache_explicit_mode: bool,
    /// Decision wire-format mechanism for `api = "system_one"` rows.
    /// `decision_input_wrapper` selects the request envelope (`none` for
    /// direct TypeSafe shape, `input` for Cloudflare `{"input":{...}}`).
    /// `decision_model_in_body` selects whether `model` travels in the JSON
    /// body (direct and Cloudflare `POST .../ai/run` styles) or only in
    /// `path_template` via `{model}` (Cloudflare
    /// `POST .../ai/run/{model}` style). `decision_response_wrapper`
    /// selects where the typed answer lives (`auto` accepts both direct
    /// and Cloudflare `result` envelopes). Rejected for other APIs so chat
    /// rows cannot declare decision transport.
    #[serde(default)]
    pub decision_input_wrapper: DecisionInputWrapper,
    #[serde(default = "default_decision_model_in_body")]
    pub decision_model_in_body: bool,
    #[serde(default)]
    pub decision_response_wrapper: DecisionResponseWrapper,
    pub response_body_limit_bytes: Option<usize>,
    pub max_concurrency: Option<usize>,
    pub requests_per_minute: Option<usize>,
    pub circuit_breaker_failures: Option<usize>,
    pub circuit_breaker_cooldown_seconds: Option<u64>,
}

const MAX_STREAM_RETRY_ATTEMPTS: u32 = 3;

impl ProviderSpec {
    pub(crate) fn validate(&self) -> Result<(), String> {
        let invalid = |message: &str| format!("provider `{}` {message}", self.id);
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
            return Err(invalid("must declare `base_url` or `base_url_env`"));
        }
        for (field, value) in [
            ("api_key_env", self.api_key_env.as_deref()),
            ("api_key_file_env", self.api_key_file_env.as_deref()),
        ] {
            if value.is_some_and(str::is_empty) {
                return Err(invalid(&format!("declares an empty `{field}`")));
            }
        }
        if self.api_key_env.is_some() && self.api_key_file_env.is_some() {
            return Err(invalid(
                "must declare at most one of api_key_env or api_key_file_env",
            ));
        }
        let has_credential = self.api_key_env.is_some() || self.api_key_file_env.is_some();
        let credential_source_env = self
            .api_key_env
            .as_deref()
            .or(self.api_key_file_env.as_deref());
        if self.auth_header.is_some() && !has_credential {
            return Err(invalid(
                "may not set `auth_header` without a credential source",
            ));
        }
        if let Some(header) = self.auth_header.as_deref()
            && reqwest::header::HeaderName::from_bytes(header.as_bytes()).is_err()
        {
            return Err(invalid(&format!("has an invalid auth_header `{header}`")));
        }
        if let Some(path) = self.path_template.as_deref() {
            if (path.contains('{') && !path.contains("{model}"))
                || path
                    .replace("{model}", "")
                    .chars()
                    .any(|character| matches!(character, '{' | '}'))
            {
                return Err(invalid("path_template contains an unknown placeholder"));
            }
            if path
                .trim_matches('/')
                .split('/')
                .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
            {
                return Err(invalid(
                    "path_template must contain safe non-empty path segments",
                ));
            }
        }
        if self.query.keys().any(String::is_empty) {
            return Err(invalid("query parameter names must not be empty"));
        }
        self.retry_policy
            .validate()
            .map_err(|message| invalid(&message))?;
        if let Some(attempts) = self.retry_attempts
            && !(1..=10).contains(&attempts)
        {
            return Err(invalid("retry_attempts must be between 1 and 10"));
        }
        if self
            .retry_base_backoff_ms
            .is_some_and(|backoff| backoff > 60_000)
        {
            return Err(invalid("retry_base_backoff_ms must not exceed 60000"));
        }
        if let Some(attempts) = self.stream_retry_attempts
            && attempts > MAX_STREAM_RETRY_ATTEMPTS
        {
            return Err(invalid(&format!(
                "stream_retry_attempts must be between 0 and {MAX_STREAM_RETRY_ATTEMPTS}"
            )));
        }
        if self
            .retry_rate_limit_floor_ms
            .is_some_and(|floor| floor > 60_000)
        {
            return Err(invalid(
                "retry_rate_limit_floor_ms must be between 0 and 60000",
            ));
        }
        if let Some(cap) = self.retry_backoff_exponent_cap
            && !(1..=16).contains(&cap)
        {
            return Err(invalid(
                "retry_backoff_exponent_cap must be between 1 and 16",
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
                return Err(invalid(&format!(
                    "has an invalid `{field}` environment variable name `{name}`"
                )));
            }
        }
        if self.timeout_seconds == Some(0) {
            return Err(invalid("timeout_seconds must be greater than zero"));
        }
        if self.response_body_limit_bytes == Some(0) {
            return Err(invalid(
                "response_body_limit_bytes must be greater than zero",
            ));
        }
        for (field, value) in [
            ("max_concurrency", self.max_concurrency),
            ("requests_per_minute", self.requests_per_minute),
            ("circuit_breaker_failures", self.circuit_breaker_failures),
        ] {
            if value == Some(0) {
                return Err(invalid(&format!("{field} must be greater than zero")));
            }
        }
        if self.circuit_breaker_cooldown_seconds == Some(0) {
            return Err(invalid(
                "circuit_breaker_cooldown_seconds must be greater than zero",
            ));
        }
        validate_capabilities_for_api(&self.id, self.api, &self.capabilities)?;
        if self.chat_token_limit_field.is_some() && self.api != ApiFlavor::ChatCompletions {
            return Err(invalid(
                "may only set `chat_token_limit_field` for chat_completions",
            ));
        }
        let decision_transport_is_default = self.decision_input_wrapper
            == DecisionInputWrapper::None
            && self.decision_model_in_body
            && self.decision_response_wrapper == DecisionResponseWrapper::Auto;
        if self.api != ApiFlavor::SystemOne && !decision_transport_is_default {
            return Err(invalid(
                "may only set `decision_input_wrapper`, `decision_model_in_body`, or `decision_response_wrapper` for system_one",
            ));
        }
        if self.api == ApiFlavor::SystemOne && !self.decision_model_in_body {
            let carries_model = self
                .path_template
                .as_deref()
                .is_some_and(|template| template.contains("{model}"));
            if !carries_model {
                return Err(invalid(
                    "with `decision_model_in_body = false` must set `path_template` containing `{model}`",
                ));
            }
        }
        match (self.capabilities.prompt_cache, self.prompt_cache_field) {
            (true, None) => {
                return Err(invalid(
                    "must set `prompt_cache_field` when `capabilities.prompt_cache` is enabled",
                ));
            }
            (false, Some(_)) => {
                return Err(invalid(
                    "may only set `prompt_cache_field` when `capabilities.prompt_cache` is enabled",
                ));
            }
            (true, Some(field)) => {
                let supported = matches!(
                    (self.api, field),
                    (
                        ApiFlavor::ChatCompletions | ApiFlavor::Responses,
                        PromptCacheField::PromptCacheKey
                    ) | (
                        ApiFlavor::AnthropicMessages,
                        PromptCacheField::CacheControl | PromptCacheField::CacheControlAuto
                    )
                );
                if !supported {
                    return Err(invalid(
                        "has a `prompt_cache_field` that is not valid for its api",
                    ));
                }
            }
            (false, None) => {}
        }
        if let Some(retention) = self.prompt_cache_retention
            && !retention.supports_api(self.api)
        {
            return Err(invalid(
                "declares `prompt_cache_retention` that is not valid for its api",
            ));
        }
        if self.prompt_cache_retention.is_some() && !self.capabilities.prompt_cache {
            return Err(invalid(
                "may only set `prompt_cache_retention` when `capabilities.prompt_cache` is enabled",
            ));
        }
        if self.prompt_cache_explicit_mode
            && !matches!(self.api, ApiFlavor::ChatCompletions | ApiFlavor::Responses)
        {
            return Err(invalid(
                "may only set `prompt_cache_explicit_mode` for chat_completions or responses",
            ));
        }
        if self.api == ApiFlavor::ChatCompletions
            && !self.capabilities.reasoning_effort.is_empty()
            && self.chat_token_limit_field != Some(ChatTokenLimitField::MaxCompletionTokens)
        {
            return Err(invalid(
                "must set `chat_token_limit_field = \"max_completion_tokens\"` when reasoning_effort is enabled",
            ));
        }
        let mut model_ids = BTreeMap::new();
        for model in &self.models {
            model.validate(&self.id, self.api)?;
            if model.prompt_cache_retention.is_some()
                && !model
                    .effective_capabilities(&self.capabilities)
                    .prompt_cache
            {
                return Err(format!(
                    "provider `{}` model `{}` may only set `prompt_cache_retention` when prompt caching is enabled",
                    self.id, model.id
                ));
            }
            if model_ids.insert(model.id.as_str(), ()).is_some() {
                return Err(format!(
                    "provider `{}` declares duplicate model id `{}`",
                    self.id, model.id
                ));
            }
        }
        if self.api == ApiFlavor::ChatCompletions
            && self.chat_token_limit_field != Some(ChatTokenLimitField::MaxCompletionTokens)
            && self.models.iter().any(|model| {
                !model
                    .effective_capabilities(&self.capabilities)
                    .reasoning_effort
                    .is_empty()
            })
        {
            return Err(invalid(
                "must set `chat_token_limit_field = \"max_completion_tokens\"` when a declared model advertises reasoning_effort",
            ));
        }
        if let Some(catalog_id) = self.catalog_id.as_deref()
            && (catalog_id.trim().is_empty() || catalog_id.chars().any(char::is_control))
        {
            return Err(invalid("catalog_id must be a non-empty identifier"));
        }
        if let Some(base_url) = self.base_url.as_deref() {
            if let Some(name) = credential_placeholder(base_url, credential_source_env) {
                return Err(invalid(&format!(
                    "base_url must not interpolate credential environment variable `{name}`"
                )));
            }
            let validation_url = normalize_url_placeholders(base_url);
            validate_base_url(&validation_url, has_credential)
                .map_err(|error| invalid(&format!("has an invalid `base_url`: {error}")))?;
        }
        validate_query_parameters(&self.query, credential_source_env)
            .map_err(|error| invalid(&format!("has an invalid `query`: {error}")))?;
        Ok(())
    }

    pub fn model(&self, id: &str) -> Option<&ModelSpec> {
        self.models.iter().find(|model| model.id == id)
    }

    /// Effective capabilities for one model, falling back to the provider row
    /// when the model is not declared (generic OpenAI-compatible rows keep
    /// working with arbitrary model strings).
    pub fn capabilities_for_model(&self, id: &str) -> Capabilities {
        match self.model(id) {
            Some(model) => model.effective_capabilities(&self.capabilities),
            None => self.capabilities.clone(),
        }
    }

    pub fn pricing_for_model(&self, id: &str) -> Option<ModelPricing> {
        self.model(id).and_then(ModelSpec::pricing)
    }

    /// Effective extended-retention mechanism for one model: the model
    /// entry overrides the row when set, otherwise the row value applies
    /// (an operator claim for every model on the row).
    pub fn retention_for_model(&self, id: &str) -> Option<PromptCacheRetention> {
        match self.model(id) {
            Some(model) => model.prompt_cache_retention.or(self.prompt_cache_retention),
            None => self.prompt_cache_retention,
        }
    }

    /// Effective write-disable mechanism for one model: the model entry
    /// overrides the row when set, otherwise the row value applies.
    pub fn explicit_mode_for_model(&self, id: &str) -> bool {
        self.model(id)
            .and_then(|model| model.prompt_cache_explicit_mode)
            .unwrap_or(self.prompt_cache_explicit_mode)
    }
}

/// API-flavor constraints shared by provider rows and per-model capability
/// overrides. Keeping one implementation means a model override cannot
/// advertise a combination the provider row itself would reject.
fn validate_capabilities_for_api(
    provider: &str,
    api: ApiFlavor,
    capabilities: &Capabilities,
) -> Result<(), String> {
    let invalid = |message: &str| format!("provider `{provider}` {message}");
    if api == ApiFlavor::SystemOne
        && serde_json::to_value(capabilities).map_err(|error| error.to_string())?
            != serde_json::to_value(Capabilities::default()).map_err(|error| error.to_string())?
    {
        return Err("system_one may not advertise chat capabilities".into());
    }
    if capabilities.seed && api != ApiFlavor::ChatCompletions {
        return Err(invalid("may only advertise `seed` for chat_completions"));
    }
    if !capabilities.reasoning_effort.is_empty() && api == ApiFlavor::AnthropicMessages {
        return Err(invalid(
            "may not advertise OpenAI reasoning_effort for anthropic_messages",
        ));
    }
    if capabilities.structured_output_with_tools && api == ApiFlavor::AnthropicMessages {
        return Err(invalid(
            "may not advertise `structured_output_with_tools` for anthropic_messages because response must be selected exclusively",
        ));
    }
    if capabilities.stop_sequences && api == ApiFlavor::Responses {
        return Err(invalid(
            "may not advertise `stop_sequences` for the Responses API",
        ));
    }
    if capabilities.verbosity && api != ApiFlavor::Responses {
        return Err(invalid(
            "may only advertise `verbosity` for the Responses API",
        ));
    }
    if capabilities.web_search && api != ApiFlavor::Responses {
        return Err(invalid(
            "may only advertise `web_search` for the Responses API",
        ));
    }
    if (capabilities.tool_choice || capabilities.parallel_tool_calls) && !capabilities.tool_use {
        return Err(invalid(
            "may not advertise tool selection controls without `tool_use`",
        ));
    }
    for (index, effort) in capabilities.reasoning_effort.iter().enumerate() {
        if capabilities.reasoning_effort[index + 1..].contains(effort) {
            return Err(format!(
                "provider `{provider}` advertises duplicate reasoning_effort `{effort}`"
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultSection {
    pub model: Option<ModelSelection>,
    pub search: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvidersFile {
    pub default: Option<DefaultSection>,
    #[serde(default)]
    pub provider: Vec<ProviderSpec>,
    #[serde(default)]
    pub search_provider: Vec<SearchProviderSpec>,
    #[serde(default)]
    pub mcp_server: Vec<mcp::McpServerSpec>,
    pub catalog: Option<crate::catalog::CatalogConfig>,
}

impl ProvidersFile {
    pub fn parse(text: &str) -> Result<Self, String> {
        let file: ProvidersFile =
            toml::from_str(text).map_err(|error| format!("invalid providers registry: {error}"))?;
        file.validate()?;
        Ok(file)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if let Some(catalog) = &self.catalog {
            catalog.validate()?;
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
            prompt_cache: false,
            // The hosted search exists only on the Responses endpoint, and
            // neither of these rows is a Responses endpoint.
            web_search: false,
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
        if req.prompt_cache == PromptCache::Auto {
            return Err(LlmError::new(
                "fake provider does not support prompt caching",
            ));
        }
        let prompt = req
            .messages
            .last()
            .map(|message| message.content.as_str())
            .unwrap_or_default();
        // A delegated specialist is scripted by its instructions, which
        // reach the model as the system prompt rather than as a message,
        // so the markers are looked up in both places.
        let system = req.system.as_deref().unwrap_or_default();
        if req.messages.last().map(|message| message.role.as_str()) == Some("tool") {
            let first_prompt = req
                .messages
                .iter()
                .find(|message| message.role == "user")
                .map(|message| message.content.as_str())
                .unwrap_or_default();
            if let Some(sequence) = marker_line(first_prompt, "FAKE_TOOL_SEQUENCE:")
                .or_else(|| marker_line(system, "FAKE_TOOL_SEQUENCE:"))
                && let Ok(values) = serde_json::from_str::<Vec<Value>>(&sequence)
            {
                let completed_tools = req
                    .messages
                    .iter()
                    .filter(|message| message.role == "tool")
                    .count();
                if let Some(value) = values.get(completed_tools) {
                    let id = value
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("fake-tool-{}", completed_tools + 1));
                    return Ok(Self::response(first_prompt, Self::tool(value, id)));
                }
            }
            let text = marker_block(first_prompt, "FAKE_AGENT_FINAL:")
                .or_else(|| marker_block(system, "FAKE_AGENT_FINAL:"))
                .unwrap_or_else(|| "agent finished".into());
            return Ok(Self::response(first_prompt, ChatContent::Text(text)));
        }
        if let Some(tool) =
            marker_line(prompt, "FAKE_TOOL:").or_else(|| marker_line(system, "FAKE_TOOL:"))
            && let Ok(value) = serde_json::from_str::<Value>(&tool)
        {
            let id = "fake-tool-1".to_string();
            return Ok(Self::response(prompt, Self::tool(&value, id)));
        }
        if let Some(sequence) = marker_line(prompt, "FAKE_TOOL_SEQUENCE:")
            .or_else(|| marker_line(system, "FAKE_TOOL_SEQUENCE:"))
            && let Ok(values) = serde_json::from_str::<Vec<Value>>(&sequence)
            && let Some(value) = values.first()
        {
            let id = value
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| "fake-tool-1".to_string());
            return Ok(Self::response(prompt, Self::tool(value, id)));
        }
        let text = if let Some(sequence) = marker_line(prompt, "FAKE_JSON_SEQUENCE:")
            && let Ok(values) = serde_json::from_str::<Vec<Value>>(&sequence)
        {
            let attempt = marker_line(prompt, "RETRY_ATTEMPT:")
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
        } else if let Some(options) = marker_line(prompt, "OPTIONS:")
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
        Ok(Self::response(prompt, ChatContent::Text(text)))
    }
}

impl FakeLlmProvider {
    fn tool(value: &Value, id: String) -> ChatContent {
        ChatContent::ToolCall {
            id,
            name: value
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .into(),
            args: value.get("args").cloned().unwrap_or_else(|| json!({})),
        }
    }

    fn response(prompt: &str, content: ChatContent) -> ChatResponse {
        let stop = if matches!(content, ChatContent::ToolCall { .. }) {
            StopReason::ToolUse
        } else {
            StopReason::EndTurn
        };
        ChatResponse {
            content: vec![content],
            usage: TokenUsage {
                input: prompt.len() as u64,
                ..Default::default()
            },
            stop,
            provider_state: None,
            web_search: None,
        }
    }
}
