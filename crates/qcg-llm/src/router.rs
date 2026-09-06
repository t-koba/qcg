use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use qcg_types::ReasoningEffort;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep, timeout};

use crate::http_provider::HttpProvider;
use crate::provider::{FakeLlmProvider, LlmProvider, LlmRuntime, ModelSelection, ProvidersFile};
use crate::search::SearchRuntime;
use crate::types::{
    Capabilities, ChatRequest, ChatResponse, ChatStreamEvent, LlmError, LlmErrorKind,
    is_retryable_llm_error,
};

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
    pub(crate) providers: BTreeMap<String, Arc<dyn LlmProvider>>,
    pub(crate) default_model: Option<ModelSelection>,
    pub(crate) search: SearchRuntime,
    pub(crate) mcp: qcg_mcp::McpRuntime,
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
        let mut bytes = Vec::new();
        file.by_ref()
            .read_to_end(&mut bytes)
            .map_err(|source| RegistryError::Read {
                path: path.to_path_buf(),
                source,
            })?;
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
    let max_attempts = provider.retry_attempts().max(1);
    let mut last_error = None;
    for attempt in 1..=max_attempts {
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
                if attempt < max_attempts
                    && is_retryable_llm_error(&error)
                    && error.kind != LlmErrorKind::CircuitOpen =>
            {
                let slow_down = matches!(
                    error.kind,
                    LlmErrorKind::HttpStatus(429) | LlmErrorKind::EmptyResponse
                );
                last_error = Some(error);
                let mut delay = retry_backoff(attempt, provider.retry_base_backoff());
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

pub(crate) fn retry_backoff(attempt: usize, base: Duration) -> Duration {
    let exponent = attempt.saturating_sub(1).min(8) as u32;
    base.checked_mul(2_u32.pow(exponent))
        .unwrap_or(Duration::from_millis(51_200))
}
