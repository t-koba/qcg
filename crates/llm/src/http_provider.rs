use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{Value, json};
use sse_stream::SseStream;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::{Duration, sleep};
use url::Url;

use crate::config::{
    BodyReadError, credential_placeholder, interpolate_env, read_body_bounded,
    read_credential_file, validate_base_url, validate_query_parameters,
};
use crate::parse::{
    llm_http_error, llm_response_error, parse_anthropic_response, parse_chat_completions_response,
    parse_responses_response,
};
use crate::payload::{anthropic_payload, chat_completions_payload, responses_payload};
use crate::provider::{
    ApiFlavor, ChatTokenLimitField, DEFAULT_RETRY_BACKOFF_EXPONENT_CAP,
    DEFAULT_RETRY_RATE_LIMIT_FLOOR_MS, LlmProvider, ModelPricing, PromptCacheField,
    PromptCacheRetention, ProviderSpec,
};
use crate::retry::MAX_RETRY_AFTER_SECS;
use crate::stream::{HttpStreamAccumulator, json_contains_string_fragment};
use crate::types::{
    CacheWritePolicy, Capabilities, ChatRequest, ChatResponse, ChatStreamEvent, LlmError,
    LlmErrorKind, PromptCache,
};
use crate::validate::{
    validate_chat_request, validate_multimodal_capabilities,
    validate_structured_output_capabilities,
};

const DEFAULT_RESPONSE_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// Bounded inspection budget for non-success error bodies. The structured
/// `error.code` is recognized only within this prefix and within the
/// configured `response_body_limit_bytes`; larger bodies yield no code.
const ERROR_CODE_INSPECT_LIMIT_BYTES: usize = 8 * 1024;

/// Returns the sanitized provider `error.code` from a parsed error body, or
/// `None` when the supported `{"error":{"code":"..."}}` shape is absent or
/// the code is malformed. Never retains the raw body.
pub(crate) fn extract_provider_error_code(value: &serde_json::Value) -> Option<String> {
    let raw = value.get("error")?.get("code")?.as_str()?;
    crate::retry::sanitize_provider_error_code(raw)
}

/// Parses a bounded `Retry-After` delay from response headers. Supports
/// delta-seconds and HTTP-date (IMF-fixdate) forms; values are capped at
/// 60 seconds. Returns `None` for missing, invalid, past, or non-UTF-8
/// headers so current backoff is preserved.
pub(crate) fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let raw = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(secs.min(MAX_RETRY_AFTER_SECS));
    }
    // HTTP-date form: delay until the advertised time, bounded and never
    // negative. Past dates yield no delay so current behavior is preserved.
    // IMF-fixdate (`... GMT`) is accepted via RFC 2822 and via an explicit
    // GMT fallback so a different valid spelling cannot bypass the bound.
    let timestamp = chrono::DateTime::parse_from_rfc2822(raw)
        .map(|dt| dt.timestamp())
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(raw, "%a, %d %b %Y %H:%M:%S GMT")
                .map(|naive| naive.and_utc().timestamp())
        })
        .ok()?;
    let now = chrono::Utc::now().timestamp();
    let delta = timestamp.saturating_sub(now);
    if delta <= 0 {
        return None;
    }
    Some((delta as u64).min(MAX_RETRY_AFTER_SECS))
}

/// Bounded read of a non-success response body for `error.code` inspection.
/// Returns `None` when the body exceeds `limit`, fails to stream, or is not
/// UTF-8, so the caller preserves current behavior without a code.
async fn read_error_body_bounded(response: reqwest::Response, limit: usize) -> Option<String> {
    String::from_utf8(read_body_bounded(response, limit).await.ok()?).ok()
}

#[derive(Debug, thiserror::Error)]
enum SseBodyError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("response body limit exceeded")]
    LimitExceeded,
}

pub struct HttpProvider {
    spec: ProviderSpec,
    base_url: Option<Url>,
    query: BTreeMap<String, String>,
    chat_token_limit_field: ChatTokenLimitField,
    response_body_limit_bytes: usize,
    config_errors: Vec<String>,
    client: Option<Client>,
    concurrency: Option<Arc<Semaphore>>,
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
    pub(crate) fn from_spec(spec: ProviderSpec) -> Self {
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
        let base_url = raw_base_url.ok_or_else(|| {
            let source = spec.base_url_env.as_deref().unwrap_or("base_url");
            format!("set `{source}` before running the generator")
        }).and_then(|raw| {
            if let Some(name) = credential_placeholder(&raw, credential_source_env) {
                return Err(format!(
                    "provider `{}` base_url must not interpolate credential environment variable `{name}`",
                    spec.id
                ));
            }
            interpolate_env(&raw)
                .and_then(|resolved| validate_base_url(&resolved, has_credential))
                .map_err(|error| format!("provider `{}` has an invalid base_url: {error}", spec.id))
        }).map_err(|error| config_errors.push(error)).ok();
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
        for (key, value) in &spec.query {
            if !query_is_safe {
                continue;
            }
            match interpolate_env(value) {
                Ok(resolved) => {
                    query.insert(key.clone(), resolved);
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
            base_url,
            query,
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
            rate_window: Arc::new(Mutex::new(VecDeque::new())),
            circuit: Arc::new(Mutex::new(CircuitState::default())),
            circuit_breaker_failures: spec.circuit_breaker_failures.unwrap_or(5),
            circuit_breaker_cooldown: Duration::from_secs(
                spec.circuit_breaker_cooldown_seconds.unwrap_or(30),
            ),
            spec,
        }
    }

    pub fn capabilities_for_model(&self, model: &str) -> Capabilities {
        self.spec.capabilities_for_model(model)
    }

    pub fn pricing_for_model(&self, model: &str) -> Option<ModelPricing> {
        self.spec.pricing_for_model(model)
    }

    /// Resolves the cache mechanism for one request. An `auto` request against
    /// a provider that does not advertise support fails explicitly instead of
    /// silently dropping the declared policy.
    fn prompt_cache_field_for(
        &self,
        req: &ChatRequest,
        capabilities: &Capabilities,
    ) -> Result<Option<PromptCacheField>, LlmError> {
        // Off and write-disable never emit auto hints; the disable mechanism
        // is validated separately.
        if req.prompt_cache != PromptCache::Auto || req.cache_write == CacheWritePolicy::Forbid {
            return Ok(None);
        }
        if !capabilities.prompt_cache {
            return Err(LlmError::new(
                self.message("does not support prompt caching"),
            ));
        }
        self.spec.prompt_cache_field.map(Some).ok_or_else(|| {
            LlmError::new(self.message("enables prompt caching without a `prompt_cache_field`"))
        })
    }

    /// Fail-closed validation for the advanced cache controls. Runs before
    /// any network dispatch, per route, so every fallback route is
    /// revalidated against its own declared mechanisms.
    pub(crate) fn check_cache_policy(
        &self,
        req: &ChatRequest,
        capabilities: &Capabilities,
    ) -> Result<(Option<PromptCacheRetention>, bool), LlmError> {
        let invalid = |message: &str| {
            LlmError::new(format!(
                "provider `{}` model `{}` {message}",
                self.spec.id, req.model
            ))
        };
        if self.spec.api == ApiFlavor::SystemOne {
            if req.prompt_cache == PromptCache::Auto
                || req.cache_write == CacheWritePolicy::Forbid
                || req.cache_retention.is_some()
            {
                return Err(invalid(
                    "cannot use advanced cache policies on a system_one route: remove cache, cache_write_policy and cache_retention",
                ));
            }
            return Ok((None, false));
        }
        if req.cache_retention.is_some() {
            if req.prompt_cache != PromptCache::Auto {
                return Err(invalid("cache_retention requires cache = \"auto\""));
            }
            if req.cache_write == CacheWritePolicy::Forbid {
                return Err(invalid(
                    "cache_retention requires cache_write_policy = \"allow\"",
                ));
            }
            let mechanism = self.spec.retention_for_model(&req.model).ok_or_else(|| invalid(
                "does not declare an extended-retention mechanism: add `prompt_cache_retention` for this model or row"
            ))?;
            if !mechanism.supports_api(self.spec.api) {
                return Err(invalid(
                    "declares an extended-retention mechanism that is not valid for its api",
                ));
            }
            if !capabilities.prompt_cache {
                return Err(invalid(
                    "cannot use cache_retention without prompt caching support",
                ));
            }
            return Ok((Some(mechanism), false));
        }
        let explicit_disable = req.cache_write == CacheWritePolicy::Forbid
            && matches!(
                self.spec.api,
                ApiFlavor::ChatCompletions | ApiFlavor::Responses
            );
        if req.cache_write == CacheWritePolicy::Forbid
            && self.spec.api == ApiFlavor::AnthropicMessages
        {
            return Err(invalid(
                "cannot guarantee no cache writes: Anthropic may cache tool/thinking continuations and server-tool results without qcg markers",
            ));
        }
        if explicit_disable && !self.spec.explicit_mode_for_model(&req.model) {
            return Err(invalid(
                "cannot disable caching: declare `prompt_cache_explicit_mode = true` for this model or row",
            ));
        }
        Ok((None, explicit_disable))
    }

    /// Refuses a hosted web search the endpoint or the model cannot honor.
    /// The check runs before the network call, and the message names the
    /// provider, the model and what to change.
    fn check_provider_web_search(
        &self,
        req: &ChatRequest,
        capabilities: &Capabilities,
    ) -> Result<(), LlmError> {
        if req.provider_web_search.is_none() {
            return Ok(());
        }
        if self.spec.api != ApiFlavor::Responses {
            return Err(LlmError::new(format!(
                "provider `{}` model `{}` cannot use provider web search: the hosted search tool exists only on the Responses API; use a Responses endpoint or remove `provider_web_search`",
                self.spec.id, req.model
            )));
        }
        if !capabilities.web_search {
            return Err(LlmError::new(format!(
                "provider `{}` model `{}` does not support provider web search: select a model whose registry row advertises the `web_search` capability or remove `provider_web_search`",
                self.spec.id, req.model
            )));
        }
        Ok(())
    }

    fn request_payload(
        &self,
        req: &ChatRequest,
        capabilities: &Capabilities,
    ) -> Result<Value, LlmError> {
        self.check_provider_web_search(req, capabilities)?;
        let (cache_retention, cache_explicit_disable) =
            self.check_cache_policy(req, capabilities)?;
        let prompt_cache = self.prompt_cache_field_for(req, capabilities)?;
        Ok(match self.spec.api {
            ApiFlavor::ChatCompletions => chat_completions_payload(
                req,
                req.seed.is_some() && capabilities.seed,
                self.chat_token_limit_field,
                prompt_cache,
                cache_retention,
                cache_explicit_disable,
            ),
            ApiFlavor::Responses => {
                responses_payload(req, prompt_cache, cache_retention, cache_explicit_disable)
            }
            ApiFlavor::AnthropicMessages => anthropic_payload(req, prompt_cache, cache_retention),
            ApiFlavor::SystemOne => {
                return Err(LlmError::new("system_one requires typed decisions"));
            }
        })
    }

    fn default_path(api: ApiFlavor) -> &'static str {
        match api {
            ApiFlavor::ChatCompletions => "chat/completions",
            ApiFlavor::Responses => "responses",
            ApiFlavor::AnthropicMessages => "messages",
            ApiFlavor::SystemOne => "systemone",
        }
    }

    pub(crate) fn endpoint_for(&self, model: &str) -> Result<Url, String> {
        let mut url = self
            .base_url
            .clone()
            .ok_or_else(|| "provider base_url is not configured".to_owned())?;
        let path = self
            .spec
            .path_template
            .as_deref()
            .unwrap_or(Self::default_path(self.spec.api))
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
                if segment.contains("{model}") {
                    // `{model}` expands as path segments so namespaced models
                    // such as `typesafe/jev` reach
                    // `.../ai/run/typesafe/jev` instead of a single
                    // percent-encoded `typesafe%2Fjev` segment. Each piece is
                    // still pushed as a segment, so `?`, `#`, and `%` stay
                    // encoded and cannot escape into query or fragment.
                    // Empty, `.`, and `..` pieces are rejected fail-closed so
                    // a model name can never traverse the path.
                    if model.trim().is_empty() || model.chars().any(char::is_control) {
                        return Err(
                            "provider model contains an empty or invalid path segment".to_owned()
                        );
                    }
                    let replaced = segment.replace("{model}", model);
                    for piece in replaced.split('/') {
                        if piece.is_empty() || matches!(piece, "." | "..") {
                            return Err("provider model contains an empty or unsafe path segment"
                                .to_owned());
                        }
                        segments.push(piece);
                    }
                } else {
                    segments.push(segment);
                }
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
            self.spec.api_key_env.as_deref(),
            self.spec.api_key_file_env.as_deref(),
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

    pub(crate) async fn acquire_request_slot(
        &self,
    ) -> Result<Option<SemaphorePermit<'_>>, LlmError> {
        {
            let mut circuit = self.circuit.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(until) = circuit.open_until {
                if until > Instant::now() {
                    return Err(LlmError {
                        message: self.message("circuit breaker is open"),
                        kind: LlmErrorKind::CircuitOpen,
                        ..Default::default()
                    });
                }
                circuit.open_until = None;
                circuit.consecutive_failures = 0;
            }
        }
        if let Some(limit) = self.spec.requests_per_minute {
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

    pub(crate) fn record_request_result<T>(&self, result: &Result<T, LlmError>) {
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

    async fn with_request_slot<T>(
        &self,
        request: impl std::future::Future<Output = Result<T, LlmError>>,
    ) -> Result<T, LlmError> {
        let _slot = self.acquire_request_slot().await?;
        let result = request.await;
        self.record_request_result(&result);
        result
    }

    async fn send(
        &self,
        payload: Value,
        model: &str,
        provider_web_search: bool,
    ) -> Result<ChatResponse, LlmError> {
        let value = self.send_json(payload, model).await?;
        match self.spec.api {
            ApiFlavor::ChatCompletions => parse_chat_completions_response(value),
            ApiFlavor::Responses => parse_responses_response(value, provider_web_search),
            ApiFlavor::AnthropicMessages => parse_anthropic_response(value),
            ApiFlavor::SystemOne => Err(LlmError::new("system_one requires typed decisions")),
        }
    }

    fn message(&self, detail: impl std::fmt::Display) -> String {
        format!("{} provider {detail}", self.spec.id)
    }

    async fn send_request(
        &self,
        payload: Value,
        model: &str,
    ) -> Result<(reqwest::Response, Option<String>), LlmError> {
        if let Some(error) = self.configuration_error_for(&self.spec.id) {
            return Err(LlmError::new(error));
        }
        let credential = self.credential_for_request()?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| LlmError::new(self.message("HTTP client is unavailable")))?;
        let endpoint = self.endpoint_for(model).map_err(|error| {
            LlmError::new(format!(
                "provider `{}` has an invalid endpoint: {error}",
                self.spec.id
            ))
        })?;
        let mut builder = client.post(endpoint).json(&payload);
        if let Some(key) = credential.as_deref() {
            builder = match &self.spec.auth_header {
                Some(header) => builder.header(header.as_str(), key),
                None => builder.bearer_auth(key),
            };
        }
        if self.spec.api == ApiFlavor::AnthropicMessages {
            builder = builder.header("anthropic-version", "2023-06-01");
        }
        let response = builder.send().await.map_err(llm_http_error)?;
        let status = response.status();
        if !status.is_success() {
            let status_u16 = status.as_u16();
            // Bounded, credential-free inspection of the error response.
            // Headers are read before the body is consumed; the body is
            // inspected only within the configured body bound (capped at the
            // error-code budget) and only the sanitized `error.code` string
            // and bounded `Retry-After` delay are retained. Raw bodies and
            // credentials are never stored or surfaced.
            let retry_after_secs = parse_retry_after(response.headers());
            let inspect_limit = self
                .response_body_limit_bytes
                .min(ERROR_CODE_INSPECT_LIMIT_BYTES);
            let mut error = LlmError::http_status(
                status_u16,
                format!("{} provider returned HTTP {}", self.spec.id, status_u16),
            );
            // Malformed, non-UTF-8, oversized, or unrecognized bodies yield no
            // code. Screening always finishes before any code is retained.
            if let Some(text) = read_error_body_bounded(response, inspect_limit).await
                && let Ok(value) = self.screened_json(&text, credential.as_deref(), "response")?
                && let Some(code) = extract_provider_error_code(&value)
            {
                let leaks_credential = credential
                    .as_deref()
                    .is_some_and(|key| !key.is_empty() && code == key);
                if !leaks_credential {
                    error.provider_code = Some(code);
                }
            }
            error.retry_after_secs = retry_after_secs;
            return Err(error);
        }
        Ok((response, credential))
    }

    // Screening is independent of JSON validity: error bodies may be malformed,
    // while success/SSE callers retain their distinct parse-error diagnostics.
    fn screened_json(
        &self,
        text: &str,
        credential: Option<&str>,
        context: &str,
    ) -> Result<Result<Value, serde_json::Error>, LlmError> {
        let key = credential.filter(|key| !key.is_empty());
        let reflected = || {
            LlmError::new(self.message(format_args!(
                "{context} contained its configured credential"
            )))
        };
        if key.is_some_and(|key| text.contains(key)) {
            return Err(reflected());
        }
        let parsed = serde_json::from_str(text);
        if let Ok(value) = &parsed
            && key.is_some_and(|key| json_contains_string_fragment(value, key))
        {
            return Err(reflected());
        }
        Ok(parsed)
    }

    fn prepare_request(&self, req: &ChatRequest) -> Result<Value, LlmError> {
        validate_chat_request(req, self.spec.api)?;
        let capabilities = self.capabilities_for_model(&req.model);
        validate_multimodal_capabilities(req, &capabilities)?;
        validate_structured_output_capabilities(req, self.spec.api, &capabilities)?;
        let payload = self.request_payload(req, &capabilities)?;
        crate::validate::validate_request_capabilities(req, &capabilities)?;
        Ok(payload)
    }

    async fn send_json(&self, payload: Value, model: &str) -> Result<Value, LlmError> {
        let (response, credential) = self.send_request(payload, model).await?;
        let body = read_body_bounded(response, self.response_body_limit_bytes)
            .await
            .map_err(|error| match error {
                BodyReadError::Http(error) => llm_response_error(error),
                BodyReadError::Overflow => LlmError::invalid_response(
                    self.message("response exceeded the configured body limit"),
                ),
                BodyReadError::Limit => LlmError::invalid_response(format!(
                    "{} provider response exceeded response_body_limit_bytes ({})",
                    self.spec.id, self.response_body_limit_bytes
                )),
            })?;
        let body = String::from_utf8(body).map_err(|_| {
            LlmError::invalid_response(self.message("returned a response body that was not UTF-8"))
        })?;
        if body.trim().is_empty() {
            return Err(LlmError::empty_response(
                self.message("returned an empty response body"),
            ));
        }
        self.screened_json(&body, credential.as_deref(), "response")?
            .map_err(|error| {
                LlmError::invalid_response(
                    self.message(format_args!("returned invalid JSON: {error}")),
                )
            })
    }

    async fn send_stream(
        &self,
        payload: Value,
        model: &str,
        provider_web_search: bool,
        events: mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        let (response, credential) = self.send_request(payload, model).await?;
        let body = response.bytes_stream().scan(
            Some(self.response_body_limit_bytes),
            |remaining, chunk| {
                let item = remaining.take().map(|budget| match chunk {
                    Ok(chunk) if chunk.len() <= budget => {
                        // Count raw bytes before parsing, including SSE framing.
                        *remaining = Some(budget - chunk.len());
                        Ok(chunk)
                    }
                    Ok(_) => {
                        // Reject the crossing chunk in full before parsing.
                        Err(SseBodyError::LimitExceeded)
                    }
                    Err(error) => Err(SseBodyError::Http(error)),
                });
                std::future::ready(item)
            },
        );
        let mut stream = SseStream::from_bytes_stream(body);
        let mut accumulator = HttpStreamAccumulator::new(self.spec.api, provider_web_search)?;
        // Pre-publication gate for the provider credential (B04): text
        // deltas pass through it, and only prefixes proven free of the
        // credential are released downstream. A metadata-mixed window can
        // hide a split secret while the published text stream reveals it,
        // so detection runs on the emitted text itself, per channel.
        let mut credential_gate = crate::text_gate::SensitiveTextGate::new(credential.as_deref());
        // Decouples ingest from publication without changing ingest.
        // Ingest emits with non-blocking sends and fails closed on fan-out
        // overflow, so this capacity is a memory bound, never a liveness
        // assumption: the loop drains the gate after every ingest, and no
        // single ingest can block waiting for that drain.
        let (gate_tx, mut gate_rx) = mpsc::channel::<ChatStreamEvent>(64);
        let response = loop {
            let Some(event) = stream.next().await else {
                break accumulator.finish()?;
            };
            let event = event.map_err(|error| {
                if let sse_stream::Error::Body(error) = error
                    && let Ok(error) = error.downcast::<SseBodyError>()
                {
                    return match *error {
                        SseBodyError::Http(error) => llm_response_error(error),
                        SseBodyError::LimitExceeded => LlmError::invalid_response(format!(
                            "{} provider stream exceeded response_body_limit_bytes ({})",
                            self.spec.id, self.response_body_limit_bytes
                        )),
                    };
                }
                LlmError::invalid_response(self.message("returned invalid SSE"))
            })?;
            let Some(data) = event.data else {
                continue;
            };
            if data.trim() == "[DONE]" {
                break accumulator.finish()?;
            }
            let value = self
                .screened_json(&data, credential.as_deref(), "stream")?
                .map_err(|_| {
                    LlmError::invalid_response(
                        self.message("returned invalid JSON in its SSE stream"),
                    )
                })?;
            if value
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| {
                    kind == "error" || kind.ends_with(".failed") || kind.ends_with(".error")
                })
            {
                return Err(LlmError::new(self.message("stream reported an error")));
            }
            if let Some(response) = accumulator.ingest(value, &gate_tx).await? {
                self.forward_gated_text(&mut gate_rx, &mut credential_gate, &events)
                    .await?;
                break response;
            }
            self.forward_gated_text(&mut gate_rx, &mut credential_gate, &events)
                .await?;
        };
        if credential.as_deref().is_some_and(|key| {
            !key.is_empty()
                && json_contains_string_fragment(
                    &serde_json::to_value(&response).unwrap_or(Value::Null),
                    key,
                )
        }) {
            return Err(LlmError::new(
                self.message("stream contained its configured credential"),
            ));
        }
        self.flush_gate_tail(&mut credential_gate, &events).await?;
        events
            .send(ChatStreamEvent::Completed { response })
            .await
            .map_err(|_| LlmError::new("LLM stream receiver closed"))
    }

    /// Forwards ingest-emitted deltas through the credential gate, releasing
    /// only cleared prefixes downstream. Non-text events pass through
    /// untouched.
    async fn forward_gated_text(
        &self,
        gate_rx: &mut mpsc::Receiver<ChatStreamEvent>,
        gate: &mut crate::text_gate::SensitiveTextGate,
        events: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        while let Ok(event) = gate_rx.try_recv() {
            let event = match event {
                ChatStreamEvent::TextDelta { text } => {
                    let cleared = gate.push(&text).map_err(|()| {
                        LlmError::new(self.message("stream contained its configured credential"))
                    })?;
                    if cleared.is_empty() {
                        continue;
                    }
                    ChatStreamEvent::TextDelta { text: cleared }
                }
                other => other,
            };
            events
                .send(event)
                .await
                .map_err(|_| LlmError::new("LLM stream receiver closed"))?;
        }
        Ok(())
    }

    /// Releases the gate's withheld suffix after the stream ends. A hit here
    /// aborts before Completed, so no published prefix ever completes the
    /// credential.
    async fn flush_gate_tail(
        &self,
        gate: &mut crate::text_gate::SensitiveTextGate,
        events: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        // `finish` consumes; rebuild an empty gate in its place.
        let gate = std::mem::replace(gate, crate::text_gate::SensitiveTextGate::new(None));
        let tail = gate.finish().map_err(|()| {
            LlmError::new(self.message("stream contained its configured credential"))
        })?;
        if !tail.is_empty() {
            events
                .send(ChatStreamEvent::TextDelta { text: tail })
                .await
                .map_err(|_| LlmError::new("LLM stream receiver closed"))?;
        }
        Ok(())
    }
}

#[async_trait]
impl LlmProvider for HttpProvider {
    fn id(&self) -> &str {
        &self.spec.id
    }

    fn timeout_seconds(&self) -> u64 {
        self.spec.timeout_seconds.unwrap_or(120)
    }

    fn retry_policy(&self) -> crate::RetryPolicy {
        self.spec.retry_policy.clone()
    }

    fn retry_attempts(&self) -> usize {
        self.spec.retry_attempts.unwrap_or(3)
    }

    fn retry_base_backoff(&self) -> Duration {
        Duration::from_millis(self.spec.retry_base_backoff_ms.unwrap_or(200))
    }

    fn stream_retry_attempts(&self) -> usize {
        self.spec.stream_retry_attempts.unwrap_or(0) as usize
    }

    fn retry_rate_limit_floor(&self) -> Duration {
        Duration::from_millis(
            self.spec
                .retry_rate_limit_floor_ms
                .unwrap_or(DEFAULT_RETRY_RATE_LIMIT_FLOOR_MS),
        )
    }

    fn retry_backoff_exponent_cap(&self) -> u32 {
        self.spec
            .retry_backoff_exponent_cap
            .unwrap_or(DEFAULT_RETRY_BACKOFF_EXPONENT_CAP)
    }

    fn capabilities(&self) -> Capabilities {
        self.spec.capabilities.clone()
    }

    fn model_capabilities_for(&self, provider: &str, model: &str) -> Option<Capabilities> {
        (provider == self.spec.id).then(|| self.capabilities_for_model(model))
    }

    fn model_pricing_for(&self, provider: &str, model: &str) -> Option<ModelPricing> {
        (provider == self.spec.id)
            .then(|| self.pricing_for_model(model))
            .flatten()
    }

    fn prompt_cache_retention_for(
        &self,
        provider: &str,
        model: &str,
    ) -> Option<PromptCacheRetention> {
        (provider == self.spec.id)
            .then(|| self.spec.retention_for_model(model))
            .flatten()
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
        self.spec
            .api_key_env
            .iter()
            .chain(self.spec.api_key_file_env.iter())
            .cloned()
            .collect()
    }

    fn supports_decisions_for(&self, provider: &str) -> bool {
        provider == self.spec.id && self.spec.api == ApiFlavor::SystemOne
    }

    async fn decide(
        &self,
        req: crate::DecisionRequest,
    ) -> Result<crate::DecisionResponse, LlmError> {
        if !self.supports_decisions_for(&req.provider) {
            return Err(LlmError::new("provider does not support typed decisions"));
        }
        req.validate()?;
        self.with_request_slot(async {
            let payload = req.payload_for(
                self.spec.decision_model_in_body,
                self.spec.decision_input_wrapper,
            );
            let value = self.send_json(payload, &req.model).await?;
            let response =
                crate::parse_decision_response(value, self.spec.decision_response_wrapper)?;
            response.validate(&req)?;
            Ok(response)
        })
        .await
    }

    async fn complete(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
        let payload = self.prepare_request(&req)?;
        self.with_request_slot(self.send(payload, &req.model, req.provider_web_search.is_some()))
            .await
    }

    async fn stream(
        &self,
        req: ChatRequest,
        events: mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        let mut payload = self.prepare_request(&req)?;
        payload["stream"] = Value::Bool(true);
        if self.spec.api == ApiFlavor::ChatCompletions {
            payload["stream_options"] = json!({ "include_usage": true });
        }
        self.with_request_slot(self.send_stream(
            payload,
            &req.model,
            req.provider_web_search.is_some(),
            events,
        ))
        .await
    }
}

#[cfg(test)]
mod loopback_stream_tests {
    use super::*;
    use crate::types::ChatMessage;
    use model::StructuredOutputMode;
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    const CANARY: &str = "canary-credential-12345";

    /// Serves one canned SSE stream over loopback: real bytes, real HTTP
    /// framing, real chunk splits. No mocks.
    fn spawn_sse_stream(chunks: Vec<String>) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let handle = std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = [0_u8; 8192];
            let _ = stream.read(&mut request);
            let mut response =
                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n"
                    .to_vec();
            for chunk in chunks {
                response.extend_from_slice(format!("data: {chunk}\n\n").as_bytes());
            }
            let _ = stream.write_all(&response);
        });
        (format!("http://{address}"), handle)
    }

    /// Writes raw SSE body parts without adding data framing. The response is
    /// close-delimited, but the socket stays open until the client has returned
    /// (or timed out), so the body limit cannot rely on EOF. Parts need not
    /// correspond to TCP packets or reqwest chunks.
    fn spawn_raw_sse_stream(
        body_parts: Vec<String>,
    ) -> (
        String,
        std::sync::mpsc::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        listener
            .set_nonblocking(true)
            .expect("test listener should be nonblocking");
        let (release, released) = std::sync::mpsc::channel();
        let server = tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline
                            || !matches!(
                                released.try_recv(),
                                Err(std::sync::mpsc::TryRecvError::Empty)
                            )
                        {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("test listener failed: {error}"),
                }
            };
            // Some Unix platforms inherit O_NONBLOCK from the listener on
            // accepted sockets; request reads below rely on blocking I/O with
            // the configured timeout instead.
            socket
                .set_nonblocking(false)
                .expect("test socket should be blocking");
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("test socket should set read timeout");
            socket
                .set_write_timeout(Some(Duration::from_secs(2)))
                .expect("test socket should set write timeout");
            let mut request = Vec::new();
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let mut buffer = [0_u8; 1024];
                let count = socket
                    .read(&mut buffer)
                    .expect("test request should arrive");
                assert!(count > 0, "test request ended before its headers");
                request.extend_from_slice(&buffer[..count]);
                assert!(request.len() <= 8192, "test request headers are too large");
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n",
                )
                .expect("test response headers should write");
            for part in body_parts {
                if Instant::now() >= deadline || socket.write_all(part.as_bytes()).is_err() {
                    // A client that already refused the body may close early.
                    break;
                }
            }
            let _ = released.recv_timeout(Duration::from_secs(10));
        });
        (format!("http://{address}"), release, server)
    }

    async fn run_raw_stream(
        body_parts: Vec<String>,
        limit: usize,
    ) -> (Result<(), LlmError>, String, bool) {
        let (base_url, release, server) = spawn_raw_sse_stream(body_parts);
        let spec: ProviderSpec = serde_json::from_value(json!({
            "id": "loopback",
            "api": "chat_completions",
            "base_url": base_url,
            "capabilities": {"streaming": true},
            "response_body_limit_bytes": limit,
        }))
        .expect("test provider spec should parse");
        let provider = HttpProvider::from_spec(spec);
        let (events, mut receiver) = mpsc::channel(16);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            provider.stream(sample_stream_request(), events),
        )
        .await;
        // Release even on timeout, before any assertions can panic. Joining is
        // separately bounded, and the server also bounds every blocking wait.
        let _ = release.send(());
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .expect("raw SSE server cleanup must not hang")
            .expect("raw SSE server must not panic");
        let result = result.expect("raw stream must return without waiting for EOF");
        let (published, completed) = collect_text(&mut receiver);
        (result, published, completed)
    }

    async fn assert_raw_stream_limit(body_parts: Vec<String>, limit: usize) {
        let (result, _, completed) = run_raw_stream(body_parts, limit).await;
        let error = result.expect_err("excess raw body must fail the stream");
        assert_eq!(error.kind, LlmErrorKind::InvalidResponse, "{error}");
        assert_eq!(
            error.message,
            format!("loopback provider stream exceeded response_body_limit_bytes ({limit})")
        );
        assert!(!completed, "a capped stream must not report completion");
    }

    #[tokio::test]
    async fn raw_stream_limit_rejects_unfinished_data_line_before_eof() {
        assert_raw_stream_limit(vec![format!("data: {}", "x".repeat(65))], 64).await;
    }

    #[tokio::test]
    async fn raw_stream_limit_counts_cumulative_data_lines_without_terminator() {
        // Each line is below the cap; there is never a blank event terminator.
        assert_raw_stream_limit(vec!["data: x\n".to_owned(); 12], 64).await;
    }

    #[tokio::test]
    async fn raw_stream_limit_counts_comments_before_eof() {
        assert_raw_stream_limit(vec![": heartbeat\n".to_owned(); 8], 64).await;
    }

    #[tokio::test]
    async fn raw_stream_limit_accepts_complete_stream_at_and_below_limit() {
        let body = format!(
            ": heartbeat\n\ndata: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            chat_chunk("hello, ", None),
            chat_chunk("world", Some("stop")),
        );
        for limit in [body.len(), body.len() + 1] {
            let (result, published, completed) = run_raw_stream(vec![body.clone()], limit).await;
            result.expect("complete stream within the raw body cap must succeed");
            assert_eq!(published, "hello, world");
            assert!(completed, "valid stream must report completion");
        }
    }

    fn chat_chunk(content: &str, finish_reason: Option<&str>) -> String {
        let mut chunk = json!({
            "id": "chatcmpl-canarydemo",
            "object": "chat.completion.chunk",
            "model": "canary-model",
            "choices": [{
                "delta": {"content": content, "role": "assistant"},
                "finish_reason": finish_reason,
            }],
        });
        if finish_reason.is_some() {
            chunk["usage"] = json!({"prompt_tokens": 1, "completion_tokens": 2});
        }
        chunk.to_string()
    }

    fn loopback_provider(base_url: String, cred_env: &str) -> HttpProvider {
        let spec: ProviderSpec = serde_json::from_value(json!({
            "id": "loopback",
            "api": "chat_completions",
            "base_url": base_url,
            "api_key_env": cred_env,
            "capabilities": {"streaming": true},
            "retry_attempts": 1,
            "timeout_seconds": 30,
        }))
        .expect("test provider spec should parse");
        HttpProvider::from_spec(spec)
    }

    fn sample_stream_request() -> ChatRequest {
        ChatRequest {
            provider: "loopback".into(),
            model: "canary-model".into(),
            system: None,
            messages: vec![ChatMessage::text("user", "hello")],
            tools: vec![],
            response_schema: None,
            structured_output: StructuredOutputMode::Auto,
            temperature: None,
            top_p: None,
            max_tokens: 128,
            stop_sequences: vec![],
            seed: None,
            reasoning_effort: None,
            tool_choice: None,
            parallel_tool_calls: None,
            verbosity: None,
            stream: true,
            prompt_cache: PromptCache::Off,
            cache_write: CacheWritePolicy::Allow,
            cache_retention: None,
            provider_web_search: None,
        }
    }

    fn collect_text(receiver: &mut mpsc::Receiver<ChatStreamEvent>) -> (String, bool) {
        let mut text = String::new();
        let mut completed = false;
        while let Ok(event) = receiver.try_recv() {
            match event {
                ChatStreamEvent::TextDelta { text: delta } => text.push_str(&delta),
                ChatStreamEvent::Completed { .. } => completed = true,
            }
        }
        (text, completed)
    }

    #[tokio::test]
    async fn split_credential_across_chunks_never_publishes() {
        if env_process::isolated() {
            return;
        }
        // B04: the audit counterexample shape — canary split across text
        // deltas with per-chunk metadata that defeats metadata-mixed
        // windows. The published stream must never complete the canary,
        // and the call must fail instead of completing.
        // SAFETY: environment mutation is serialized with ENV_LOCK; unique variable name.
        // The guard below lives until the end of the test, so the matching
        // remove_var runs under the same held guard (re-acquiring here
        // would self-deadlock: the right side evaluates before the old
        // guard drops).
        let _guard = crate::ENV_LOCK.lock().await;
        // SAFETY: lock held.
        unsafe {
            std::env::set_var("TEST_LOOPBACK_CRED_SPLIT", CANARY);
        }
        let (base_url, server) = spawn_sse_stream(vec![
            chat_chunk("canary-", None),
            chat_chunk("credential-12345", Some("stop")),
        ]);
        let provider = loopback_provider(base_url, "TEST_LOOPBACK_CRED_SPLIT");
        let (events, mut receiver) = mpsc::channel(16);
        let result = provider.stream(sample_stream_request(), events).await;
        let (published, completed) = collect_text(&mut receiver);
        let _ = server.join();
        // SAFETY: the start-of-test ENV_LOCK guard is still held.
        unsafe {
            std::env::remove_var("TEST_LOOPBACK_CRED_SPLIT");
        }
        let error = result.expect_err("split credential must fail the stream");
        assert!(
            error.to_string().contains("credential"),
            "unexpected error: {error}"
        );
        assert!(
            !published.contains(CANARY),
            "published prefixes must never complete the credential, got: {published}"
        );
        assert!(!completed, "a refused stream must not report completion");
    }

    #[tokio::test]
    async fn auto_prompt_cache_without_provider_support_is_explicit_error() {
        // The registry row does not advertise prompt caching, so the
        // declared `auto` policy must fail instead of being dropped.
        let spec: ProviderSpec = serde_json::from_value(json!({
            "id": "no-cache",
            "api": "chat_completions",
            "base_url": "http://127.0.0.1:9",
        }))
        .expect("test provider spec should parse");
        let provider = HttpProvider::from_spec(spec);
        let mut request = sample_stream_request();
        request.stream = false;
        request.prompt_cache = PromptCache::Auto;
        let error = provider
            .complete(request)
            .await
            .expect_err("unsupported prompt_cache must fail");
        assert!(
            error
                .to_string()
                .contains("does not support prompt caching"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn clean_stream_publishes_in_full() {
        if env_process::isolated() {
            return;
        }
        // The gate must not disturb honest streams: every byte publishes
        // and the stream completes.
        // SAFETY: environment mutation is serialized with ENV_LOCK; unique variable name.
        // The guard below lives until the end of the test (see above).
        let _guard = crate::ENV_LOCK.lock().await;
        // SAFETY: lock held.
        unsafe {
            std::env::set_var("TEST_LOOPBACK_CRED_CLEAN", CANARY);
        }
        let (base_url, server) = spawn_sse_stream(vec![
            chat_chunk("hello, ", None),
            chat_chunk("world", Some("stop")),
        ]);
        let provider = loopback_provider(base_url, "TEST_LOOPBACK_CRED_CLEAN");
        let (events, mut receiver) = mpsc::channel(16);
        provider
            .stream(sample_stream_request(), events)
            .await
            .expect("clean stream should succeed");
        let (published, completed) = collect_text(&mut receiver);
        let _ = server.join();
        // SAFETY: the start-of-test ENV_LOCK guard is still held.
        unsafe {
            std::env::remove_var("TEST_LOOPBACK_CRED_CLEAN");
        }
        assert_eq!(published, "hello, world");
        assert!(completed, "clean stream must report completion");
    }
}

#[cfg(test)]
mod stream_credential_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn split_and_escaped_credential_reflection_is_detected() {
        // A08: single-chunk raw matching misses split and escaped reflection.
        let key = "sk-secret-credential-12345";
        // Split across two decoded chunks: neither chunk alone contains the
        // key. Cross-chunk reassembly is the text gate's job now.
        let first = json!({"delta": "sk-secret-"});
        let second = json!({"delta": "credential-12345"});
        assert!(!json_contains_string_fragment(&first, key));
        assert!(!json_contains_string_fragment(&second, key));
        // JSON-escaped reflection decodes before inspection.
        let escaped = serde_json::from_str::<Value>(
            r#"{"text": "prefix \u0073k-secret-credential-12345 suffix"}"#,
        )
        .unwrap();
        assert!(json_contains_string_fragment(&escaped, key));
    }
}

#[cfg(test)]
mod env_process {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/test-support/environment.rs"
    ));
}
