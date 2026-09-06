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
    credential_placeholder, interpolate_env, read_credential_file, validate_base_url,
    validate_query_parameters,
};
use crate::parse::{
    llm_http_error, parse_anthropic_response, parse_chat_completions_response,
    parse_responses_response,
};
use crate::payload::{anthropic_payload, chat_completions_payload, responses_payload};
use crate::provider::{ApiFlavor, ChatTokenLimitField, LlmProvider, ProviderSpec};
use crate::stream::{HttpStreamAccumulator, json_contains_string_fragment};
use crate::types::{
    Capabilities, ChatRequest, ChatResponse, ChatStreamEvent, LlmError, LlmErrorKind,
};
use crate::validate::{
    validate_chat_request, validate_multimodal_capabilities,
    validate_structured_output_capabilities,
};

const DEFAULT_RESPONSE_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

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
    retry_attempts: usize,
    retry_base_backoff_ms: u64,
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
            retry_attempts: spec.retry_attempts.unwrap_or(3),
            retry_base_backoff_ms: spec.retry_base_backoff_ms.unwrap_or(200),
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

    pub(crate) fn endpoint_for(&self, model: &str) -> Result<Url, String> {
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

    pub(crate) async fn acquire_request_slot(
        &self,
    ) -> Result<Option<SemaphorePermit<'_>>, LlmError> {
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

#[async_trait]
impl LlmProvider for HttpProvider {
    fn id(&self) -> &str {
        &self.id
    }

    fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }

    fn retry_attempts(&self) -> usize {
        self.retry_attempts
    }

    fn retry_base_backoff(&self) -> Duration {
        Duration::from_millis(self.retry_base_backoff_ms)
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
