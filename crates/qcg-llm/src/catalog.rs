//! Model catalog: explicit registry declarations, an optional external
//! catalog (models.dev), and per-provider model discovery.
//!
//! The catalog is metadata only. It never performs an LLM request and never
//! grants a run permission: it tells operators and clients which provider,
//! model, and effort combinations are selectable. Runs remain bound by the
//! contract's permissions and the registry's transport policy.

use crate::config::{
    credential_placeholder, interpolate_env, read_credential_file, validate_base_url,
};
use crate::provider::{
    FakeLlmProvider, LlmProvider, ModelPricing, ModelSpec, ModelsDiscovery, ProviderSpec,
};
use qcg_types::ReasoningEffort;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::Duration;

const DISCOVERY_BODY_LIMIT_BYTES: usize = 2 * 1024 * 1024;
const MODELS_DEV_BODY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CATALOG_FETCH_TIMEOUT_SECONDS: u64 = 30;

/// `[catalog]` section of the provider registry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    /// Cache path for fetched external catalogs. Defaults to
    /// `$QCG_HOME/cache/llm-catalog.json`.
    #[serde(default)]
    pub cache: Option<String>,
    #[serde(default = "default_refresh_seconds")]
    pub refresh_seconds: u64,
    #[serde(default)]
    pub source: Vec<CatalogSourceSpec>,
}

fn default_refresh_seconds() -> u64 {
    86_400
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            cache: None,
            refresh_seconds: default_refresh_seconds(),
            source: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogSourceKind {
    ModelsDev,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSourceSpec {
    pub kind: CatalogSourceKind,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
    /// Optional pinned SHA-256 of the fetched bytes.
    #[serde(default)]
    pub sha256: Option<String>,
}

impl CatalogSourceSpec {
    pub fn location(&self) -> &str {
        self.url
            .as_deref()
            .or(self.file.as_deref())
            .unwrap_or("<unset>")
    }

    pub fn kind_name(&self) -> &'static str {
        match self.kind {
            CatalogSourceKind::ModelsDev => "models_dev",
        }
    }
}

impl CatalogConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.refresh_seconds == 0 {
            return Err("[catalog].refresh_seconds must be greater than zero".into());
        }
        if let Some(cache) = self.cache.as_deref()
            && cache.trim().is_empty()
        {
            return Err("[catalog].cache must not be empty".into());
        }
        for source in &self.source {
            match (&source.url, &source.file) {
                (Some(_), Some(_)) => {
                    return Err("[catalog.source] must declare exactly one of url or file".into());
                }
                (None, None) => {
                    return Err("[catalog.source] must declare exactly one of url or file".into());
                }
                (Some(url), None) => {
                    let url = url.trim();
                    if !(url.starts_with("https://")
                        || url.starts_with("http://127.0.0.1")
                        || url.starts_with("http://localhost"))
                    {
                        return Err(format!(
                            "[catalog.source] url `{url}` must use HTTPS (loopback HTTP is allowed for tests)"
                        ));
                    }
                }
                (None, Some(file)) => {
                    if file.trim().is_empty() {
                        return Err("[catalog.source] file must not be empty".into());
                    }
                }
            }
            if let Some(sha256) = source.sha256.as_deref()
                && (sha256.len() != 64 || !sha256.chars().all(|c| c.is_ascii_hexdigit()))
            {
                return Err("[catalog.source] sha256 must be a 64-character hex digest".into());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExternalCatalog {
    #[serde(default)]
    fetched_at: Option<String>,
    /// Fingerprint of the source configuration this data was fetched under
    /// (F09-02). A cache entry from different sources is never treated as
    /// verified data for the current pins.
    #[serde(default)]
    fingerprint: Option<String>,
    /// External provider key -> model id -> spec.
    #[serde(default)]
    providers: BTreeMap<String, BTreeMap<String, ModelSpec>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CachedCatalog {
    #[serde(default)]
    fetched_at: Option<String>,
    #[serde(default)]
    fingerprint: Option<String>,
    #[serde(default)]
    providers: BTreeMap<String, BTreeMap<String, ModelSpec>>,
}

#[derive(Debug, Clone, Default)]
struct Snapshot {
    /// Provider id -> merged explicit + external models.
    models: BTreeMap<String, Vec<ModelSpec>>,
    external: ExternalCatalog,
    sources: Vec<CatalogSourceStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogSourceStatus {
    pub kind: String,
    pub location: String,
    pub fetched_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogView {
    pub fetched_at: Option<String>,
    pub stale: bool,
    pub sources: Vec<CatalogSourceStatus>,
    pub providers: Vec<CatalogProviderView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogProviderView {
    pub id: String,
    pub label: Option<String>,
    /// False when the provider needs a credential that is not configured.
    pub available: bool,
    pub discovery: bool,
    pub error: Option<String>,
    pub models: Vec<CatalogModelView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogModelView {
    pub id: String,
    pub label: Option<String>,
    pub enabled: bool,
    /// `explicit`, `external`, or `discovery`.
    pub source: String,
    pub capabilities: crate::types::Capabilities,
    pub reasoning_effort: Vec<ReasoningEffort>,
    pub input_cost_per_million_usd: Option<f64>,
    pub output_cost_per_million_usd: Option<f64>,
    pub context_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
}

/// Catalog runtime shared by the engine registry, the HTTP API, and the CLI.
/// Explicit declarations are always available; external sources and
/// discovery are best-effort metadata that never change run permissions.
pub struct CatalogService {
    specs: BTreeMap<String, ProviderSpec>,
    config: CatalogConfig,
    snapshot: RwLock<Snapshot>,
    client: reqwest::Client,
}

impl std::fmt::Debug for CatalogService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogService")
            .field("provider_ids", &self.specs.keys().collect::<Vec<_>>())
            .field("sources", &self.config.source.len())
            .finish()
    }
}

impl Default for CatalogService {
    fn default() -> Self {
        Self::empty()
    }
}

impl CatalogService {
    /// Catalog for `LlmRuntime::builtins` and unit tests: the synthetic
    /// `fake` provider only.
    pub fn empty() -> Self {
        Self::build(Vec::new(), None).expect("an empty catalog is valid")
    }

    pub fn new(specs: Vec<ProviderSpec>, config: Option<CatalogConfig>) -> Result<Self, String> {
        Self::build(specs, config)
    }

    fn build(specs: Vec<ProviderSpec>, config: Option<CatalogConfig>) -> Result<Self, String> {
        if let Some(config) = &config {
            config.validate()?;
        }
        let config = config.unwrap_or_default();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("model catalog HTTP client failed: {error}"))?;
        let mut by_id = BTreeMap::new();
        for spec in specs {
            by_id.insert(spec.id.clone(), spec);
        }
        let (external, cache_error) = load_cached_external(&config);
        let mut sources = Vec::new();
        if let Some(error) = cache_error {
            sources.push(CatalogSourceStatus {
                kind: "cache".to_string(),
                location: cache_path(&config)
                    .map(|path| path.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                fetched_at: None,
                error: Some(error),
            });
        }
        let snapshot = Snapshot {
            models: BTreeMap::new(),
            sources,
            external,
        };
        let service = Self {
            specs: by_id,
            config,
            snapshot: RwLock::new(snapshot),
            client,
        };
        service.rebuild_models();
        Ok(service)
    }

    pub fn provider(&self, id: &str) -> Option<&ProviderSpec> {
        self.specs.get(id)
    }

    /// Whether any external catalog source is configured.
    pub fn has_sources(&self) -> bool {
        !self.config.source.is_empty()
    }

    /// Whether the loaded external metadata is missing or older than the
    /// configured refresh interval (F09-01). Computed from `fetched_at` and
    /// the current time on every read so staleness re-arises after the TTL
    /// even after a successful refresh; no stored bool can go stale.
    pub fn is_stale(&self) -> bool {
        let snapshot = self
            .snapshot
            .read()
            .unwrap_or_else(|error| error.into_inner());
        snapshot_stale(
            snapshot.external.fetched_at.as_deref(),
            self.config.refresh_seconds,
        )
    }

    /// Interval between background staleness checks. Never below one minute
    /// so a misconfigured value cannot spin the resident task.
    pub fn refresh_interval(&self) -> Duration {
        Duration::from_secs(self.config.refresh_seconds.max(60))
    }

    /// Merged model spec for run-time capability and pricing resolution.
    pub fn model_spec(&self, provider: &str, model: &str) -> Option<ModelSpec> {
        let snapshot = self
            .snapshot
            .read()
            .unwrap_or_else(|error| error.into_inner());
        snapshot
            .models
            .get(provider)
            .and_then(|models| models.iter().find(|spec| spec.id == model))
            .cloned()
    }

    pub fn model_capabilities(
        &self,
        provider: &str,
        model: &str,
    ) -> Option<crate::types::Capabilities> {
        let spec = self.specs.get(provider)?;
        Some(match self.model_spec(provider, model) {
            Some(model) => model.effective_capabilities(&spec.capabilities),
            None => spec.capabilities.clone(),
        })
    }

    pub fn model_pricing(&self, provider: &str, model: &str) -> Option<ModelPricing> {
        self.model_spec(provider, model)
            .and_then(|model| model.pricing())
    }

    fn rebuild_models(&self) {
        let external = {
            let snapshot = self
                .snapshot
                .read()
                .unwrap_or_else(|error| error.into_inner());
            snapshot.external.clone()
        };
        let mut models = BTreeMap::new();
        for (provider_id, spec) in &self.specs {
            let catalog_id = spec.catalog_id.as_deref().unwrap_or(provider_id);
            let mut provider_models = spec.models.clone();
            if let Some(external_models) = external.providers.get(catalog_id) {
                for (model_id, external_model) in external_models {
                    match provider_models
                        .iter_mut()
                        .find(|model| &model.id == model_id)
                    {
                        Some(explicit) => {
                            *explicit = merge_model(external_model, explicit);
                        }
                        None => provider_models.push(external_model.clone()),
                    }
                }
            }
            models.insert(provider_id.clone(), provider_models);
        }
        let mut snapshot = self
            .snapshot
            .write()
            .unwrap_or_else(|error| error.into_inner());
        snapshot.models = models;
    }

    /// Refreshes every configured external source and persists the cache.
    /// Returned errors are also recorded per source in the catalog view.
    /// Cache persistence failures are reported (F09-03), never silently
    /// ignored.
    pub async fn refresh(&self) -> Result<(), String> {
        if self.config.source.is_empty() {
            return Ok(());
        }
        let mut merged = ExternalCatalog::default();
        let mut statuses = Vec::new();
        let mut first_error: Option<String> = None;
        for source in &self.config.source {
            match self.fetch_source(source).await {
                Ok(catalog) => {
                    for (provider, models) in catalog.providers {
                        let entry = merged.providers.entry(provider).or_default();
                        for (model, spec) in models {
                            entry.entry(model).or_insert(spec);
                        }
                    }
                    statuses.push(CatalogSourceStatus {
                        kind: source.kind_name().to_string(),
                        location: source.location().to_string(),
                        fetched_at: catalog.fetched_at,
                        error: None,
                    });
                }
                Err(error) => {
                    statuses.push(CatalogSourceStatus {
                        kind: source.kind_name().to_string(),
                        location: source.location().to_string(),
                        fetched_at: None,
                        error: Some(error.clone()),
                    });
                    first_error.get_or_insert(error);
                }
            }
        }
        merged.fetched_at = Some(chrono::Utc::now().to_rfc3339());
        merged.fingerprint = Some(config_fingerprint(&self.config));
        let mut cache_error: Option<String> = None;
        if first_error.is_none() {
            // Persist before publishing so a crash cannot publish data the
            // cache does not hold; a write failure is reported and keeps
            // the previous snapshot instead of advertising fresh data.
            match write_cache(&self.config, &merged) {
                Ok(()) => {}
                Err(error) => {
                    cache_error = Some(error.clone());
                    statuses.push(CatalogSourceStatus {
                        kind: "cache".to_string(),
                        location: cache_path(&self.config)
                            .map(|path| path.to_string_lossy().into_owned())
                            .unwrap_or_default(),
                        fetched_at: None,
                        error: Some(error),
                    });
                }
            }
        }
        {
            let mut snapshot = self
                .snapshot
                .write()
                .unwrap_or_else(|error| error.into_inner());
            // A failed source keeps the previous external data instead of
            // silently dropping metadata; the error stays visible.
            // Staleness recomputes from fetched_at on read (F09-01).
            if first_error.is_none() && cache_error.is_none() {
                snapshot.external = merged.clone();
            }
            snapshot.sources = statuses;
        }
        if first_error.is_none() && cache_error.is_none() {
            self.rebuild_models();
        }
        match first_error.or(cache_error) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn fetch_source(&self, source: &CatalogSourceSpec) -> Result<ExternalCatalog, String> {
        let bytes = match (&source.url, &source.file) {
            (Some(url), None) => self.fetch_url(url).await?,
            (None, Some(file)) => std::fs::read(expand_home(file))
                .map_err(|error| format!("catalog file `{file}` could not be read: {error}"))?,
            _ => return Err("[catalog.source] must declare exactly one of url or file".into()),
        };
        if let Some(expected) = source.sha256.as_deref() {
            use sha2::{Digest, Sha256};
            let actual = hex::encode(Sha256::digest(&bytes));
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(format!(
                    "catalog source `{}` sha256 mismatch: expected {expected}, got {actual}",
                    source.location()
                ));
            }
        }
        let text = String::from_utf8(bytes).map_err(|error| {
            format!(
                "catalog source `{}` is not UTF-8: {error}",
                source.location()
            )
        })?;
        let mut catalog = match source.kind {
            CatalogSourceKind::ModelsDev => map_models_dev(&text)?,
        };
        catalog.fetched_at = Some(chrono::Utc::now().to_rfc3339());
        Ok(catalog)
    }

    async fn fetch_url(&self, url: &str) -> Result<Vec<u8>, String> {
        let response = self
            .client
            .get(url)
            .timeout(Duration::from_secs(DEFAULT_CATALOG_FETCH_TIMEOUT_SECONDS))
            .send()
            .await
            .map_err(|error| format!("catalog fetch `{url}` failed: {error}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "catalog fetch `{url}` returned HTTP {}",
                response.status().as_u16()
            ));
        }
        read_bounded(response, MODELS_DEV_BODY_LIMIT_BYTES)
            .await
            .map_err(|error| format!("catalog fetch `{url}` failed: {error}"))
    }

    /// Builds the serializable catalog view. Discovery runs on demand and is
    /// merged over the explicit and cached metadata for selection only.
    /// Display and runtime share the same field-level overlay (F09-04):
    /// explicit declarations win per field, unspecified fields inherit the
    /// external entry, so shown capabilities/pricing equal execution-time
    /// resolution.
    pub async fn view(&self, refresh: bool) -> CatalogView {
        if refresh {
            let _ = self.refresh().await;
        }
        let (fetched_at, sources, external) = {
            let snapshot = self
                .snapshot
                .read()
                .unwrap_or_else(|error| error.into_inner());
            (
                snapshot.external.fetched_at.clone(),
                snapshot.sources.clone(),
                snapshot.external.clone(),
            )
        };
        let stale = snapshot_stale(fetched_at.as_deref(), self.config.refresh_seconds);
        let mut providers = vec![fake_provider_view()];
        for (provider_id, spec) in &self.specs {
            let catalog_id = spec.catalog_id.as_deref().unwrap_or(provider_id);
            let mut models: BTreeMap<String, CatalogModelView> = BTreeMap::new();
            // Same overlay as rebuild_models (F09-04): explicit wins per
            // field, external fills the gaps, so the view matches runtime.
            let external_models = external.providers.get(catalog_id);
            for model in &spec.models {
                let merged = match external_models.and_then(|models| models.get(&model.id)) {
                    Some(external_model) => merge_model(external_model, model),
                    None => model.clone(),
                };
                let has_external =
                    external_models.is_some_and(|models| models.contains_key(&model.id));
                models.insert(
                    model.id.clone(),
                    model_view(
                        &merged,
                        if has_external {
                            "explicit + external"
                        } else {
                            "explicit"
                        },
                        &spec.capabilities,
                    ),
                );
            }
            if let Some(external_models) = external_models {
                for (model_id, model) in external_models {
                    models
                        .entry(model_id.clone())
                        .or_insert_with(|| model_view(model, "external", &spec.capabilities));
                }
            }
            let mut provider_error = None;
            if spec.models_discovery.is_some() {
                match self.discover(spec).await {
                    Ok(discovered) => {
                        for id in discovered {
                            let view = match models.get_mut(&id) {
                                Some(existing) => {
                                    existing.source = format!("{} + discovery", existing.source);
                                    continue;
                                }
                                None => CatalogModelView {
                                    id: id.clone(),
                                    label: None,
                                    enabled: true,
                                    source: "discovery".to_string(),
                                    capabilities: spec.capabilities.clone(),
                                    reasoning_effort: spec.capabilities.reasoning_effort.clone(),
                                    input_cost_per_million_usd: None,
                                    output_cost_per_million_usd: None,
                                    context_tokens: None,
                                    max_output_tokens: None,
                                },
                            };
                            models.insert(id, view);
                        }
                    }
                    Err(error) => provider_error = Some(error),
                }
            }
            providers.push(CatalogProviderView {
                id: provider_id.clone(),
                label: None,
                available: provider_available(spec),
                discovery: spec.models_discovery.is_some(),
                error: provider_error,
                models: models.into_values().collect(),
            });
        }
        CatalogView {
            fetched_at,
            stale,
            sources,
            providers,
        }
    }

    /// `GET {base_url}/models` discovery for one provider. The endpoint is
    /// the operator-configured base URL, so no contract permission applies;
    /// credentials are attached exactly like inference requests.
    pub async fn discover(&self, spec: &ProviderSpec) -> Result<Vec<String>, String> {
        let discovery = spec
            .models_discovery
            .ok_or_else(|| format!("provider `{}` does not enable model discovery", spec.id))?;
        match discovery {
            ModelsDiscovery::Openai => self.discover_openai(spec).await,
        }
    }

    async fn discover_openai(&self, spec: &ProviderSpec) -> Result<Vec<String>, String> {
        let credential_source = spec
            .api_key_env
            .as_deref()
            .or(spec.api_key_file_env.as_deref());
        let has_credential = credential_source.is_some();
        let raw_base_url = match spec.base_url_env.as_deref() {
            Some(name) => match std::env::var(name) {
                Ok(value) => value,
                Err(std::env::VarError::NotPresent) => spec.base_url.clone().ok_or_else(|| {
                    format!("provider `{}` has no base URL for discovery", spec.id)
                })?,
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(format!(
                        "provider `{}` base URL environment variable `{name}` is not valid UTF-8",
                        spec.id
                    ));
                }
            },
            None => spec
                .base_url
                .clone()
                .ok_or_else(|| format!("provider `{}` has no base URL for discovery", spec.id))?,
        };
        if credential_placeholder(&raw_base_url, credential_source).is_some() {
            return Err(format!(
                "provider `{}` base URL must not interpolate a credential",
                spec.id
            ));
        }
        let base_url = interpolate_env(&raw_base_url)?;
        let mut url = validate_base_url(&base_url, has_credential)
            .map_err(|error| format!("provider `{}` base URL is invalid: {error}", spec.id))?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| format!("provider `{}` base URL is not usable", spec.id))?;
            segments.pop_if_empty();
            segments.push("models");
        }
        let mut request = self
            .client
            .get(url)
            .timeout(Duration::from_secs(DEFAULT_CATALOG_FETCH_TIMEOUT_SECONDS));
        // Active credentials for reflection screening (F03). Loaded here so
        // rotation takes effect on the next call; never logged or returned
        // in errors.
        let mut active_credentials: Vec<String> = Vec::new();
        if let Some(name) = &spec.api_key_env {
            let value = std::env::var(name).map_err(|_| {
                format!(
                    "set `{name}` before listing models for provider `{}`",
                    spec.id
                )
            })?;
            active_credentials.push(value.clone());
            request = attach_credential(request, spec.auth_header.as_deref(), &value)?;
        } else if let Some(name) = &spec.api_key_file_env {
            let path = std::env::var(name).map_err(|_| {
                format!(
                    "set `{name}` before listing models for provider `{}`",
                    spec.id
                )
            })?;
            let value = read_credential_file(Path::new(&path))?;
            active_credentials.push(value.clone());
            request = attach_credential(request, spec.auth_header.as_deref(), &value)?;
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("provider `{}` model discovery failed: {error}", spec.id))?;
        if !response.status().is_success() {
            return Err(format!(
                "provider `{}` model discovery returned HTTP {}",
                spec.id,
                response.status().as_u16()
            ));
        }
        let bytes = read_bounded(response, DISCOVERY_BODY_LIMIT_BYTES)
            .await
            .map_err(|error| format!("provider `{}` model discovery failed: {error}", spec.id))?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "provider `{}` model discovery returned invalid JSON: {error}",
                spec.id
            )
        })?;
        // Credential reflection screening (F03): a malicious or confused
        // provider must not be able to launder the request credential into
        // the public catalog view as a model id. Check raw bytes and all
        // decoded keys/values before anything reaches the view, records, or
        // error text. The error never echoes the secret.
        if let Some(reason) = credential_reflection(&bytes, &value, &active_credentials) {
            return Err(format!(
                "provider `{}` model discovery response failed credential screening: {reason}",
                spec.id
            ));
        }
        let entries = value
            .get("data")
            .or_else(|| value.get("models"))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                format!(
                    "provider `{}` model discovery response has no `data` array",
                    spec.id
                )
            })?;
        let mut ids = BTreeSet::new();
        for entry in entries {
            let Some(id) = entry
                .get("id")
                .or_else(|| entry.get("name"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            if id.trim().is_empty() || id.chars().any(char::is_control) {
                continue;
            }
            ids.insert(id.to_string());
        }
        Ok(ids.into_iter().collect())
    }
}

fn attach_credential(
    request: reqwest::RequestBuilder,
    auth_header: Option<&str>,
    value: &str,
) -> Result<reqwest::RequestBuilder, String> {
    match auth_header {
        Some(name) => {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| format!("invalid auth_header: {error}"))?;
            let value = reqwest::header::HeaderValue::from_str(value)
                .map_err(|error| format!("invalid credential: {error}"))?;
            Ok(request.header(name, value))
        }
        None => Ok(request.bearer_auth(value)),
    }
}

/// Screens a discovery response for credential reflection (F03). Checks the
/// raw response bytes (covers JSON-escaped reflections) and every decoded
/// key/value string before the ids reach the public view. Returns a generic
/// reason without echoing the secret. Short credentials (< 8 chars) are
/// still checked for exact id equality to avoid substring false positives
/// on tiny values while catching direct echo.
fn credential_reflection(
    raw: &[u8],
    decoded: &Value,
    credentials: &[String],
) -> Option<&'static str> {
    for credential in credentials {
        let credential = credential.trim();
        if credential.is_empty() {
            continue;
        }
        // Raw-byte check covers JSON-escaped reflections.
        if let Ok(text) = std::str::from_utf8(raw)
            && text.contains(credential)
        {
            return Some("response reflects the request credential");
        }
        if json_contains_credential(decoded, credential) {
            return Some("response reflects the request credential");
        }
    }
    None
}

fn json_contains_credential(value: &Value, credential: &str) -> bool {
    match value {
        Value::String(text) => {
            if credential.len() < 8 {
                text == credential
            } else {
                text.contains(credential)
            }
        }
        Value::Array(items) => items
            .iter()
            .any(|item| json_contains_credential(item, credential)),
        Value::Object(map) => map.iter().any(|(key, val)| {
            (credential.len() >= 8 && key.contains(credential))
                || json_contains_credential(val, credential)
        }),
        _ => false,
    }
}

async fn read_bounded(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    let mut response = response;
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(format!("response exceeded {limit} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn provider_available(spec: &ProviderSpec) -> bool {
    match (&spec.api_key_env, &spec.api_key_file_env) {
        (None, None) => true,
        (Some(name), None) => std::env::var_os(name).is_some(),
        (None, Some(name)) => std::env::var(name)
            .ok()
            .map(|path| Path::new(&path).is_file())
            .unwrap_or(false),
        (Some(_), Some(_)) => false,
    }
}

fn fake_provider_view() -> CatalogProviderView {
    CatalogProviderView {
        id: "fake".to_string(),
        label: Some("Built-in deterministic provider".to_string()),
        available: true,
        discovery: false,
        error: None,
        models: vec![CatalogModelView {
            id: "fake".to_string(),
            label: Some("fake".to_string()),
            enabled: true,
            source: "built-in".to_string(),
            capabilities: FakeLlmProvider.capabilities(),
            reasoning_effort: vec![
                ReasoningEffort::None,
                ReasoningEffort::Minimal,
                ReasoningEffort::Low,
                ReasoningEffort::Medium,
                ReasoningEffort::High,
                ReasoningEffort::Xhigh,
                ReasoningEffort::Max,
            ],
            input_cost_per_million_usd: None,
            output_cost_per_million_usd: None,
            context_tokens: None,
            max_output_tokens: None,
        }],
    }
}

fn model_view(
    model: &ModelSpec,
    source: &str,
    provider_capabilities: &crate::types::Capabilities,
) -> CatalogModelView {
    let effective = model.effective_capabilities(provider_capabilities);
    CatalogModelView {
        id: model.id.clone(),
        label: model.label.clone(),
        enabled: model.is_enabled(),
        source: source.to_string(),
        reasoning_effort: effective.reasoning_effort.clone(),
        capabilities: effective,
        input_cost_per_million_usd: model.input_cost_per_million_usd,
        output_cost_per_million_usd: model.output_cost_per_million_usd,
        context_tokens: model.context_tokens,
        max_output_tokens: model.max_output_tokens,
    }
}

/// Field-level overlay: explicit registry declarations win over external
/// metadata, and unspecified fields inherit from the external entry.
fn merge_model(base: &ModelSpec, over: &ModelSpec) -> ModelSpec {
    ModelSpec {
        id: over.id.clone(),
        label: over.label.clone().or_else(|| base.label.clone()),
        capabilities: over
            .capabilities
            .clone()
            .or_else(|| base.capabilities.clone()),
        reasoning_effort: over
            .reasoning_effort
            .clone()
            .or_else(|| base.reasoning_effort.clone()),
        input_cost_per_million_usd: over
            .input_cost_per_million_usd
            .or(base.input_cost_per_million_usd),
        output_cost_per_million_usd: over
            .output_cost_per_million_usd
            .or(base.output_cost_per_million_usd),
        context_tokens: over.context_tokens.or(base.context_tokens),
        max_output_tokens: over.max_output_tokens.or(base.max_output_tokens),
        enabled: over.enabled.or(base.enabled),
    }
}

fn snapshot_stale(fetched_at: Option<&str>, refresh_seconds: u64) -> bool {
    let Some(fetched_at) = fetched_at else {
        return true;
    };
    let Ok(fetched_at) = chrono::DateTime::parse_from_rfc3339(fetched_at) else {
        return true;
    };
    let age = chrono::Utc::now().signed_duration_since(fetched_at.with_timezone(&chrono::Utc));
    age.num_seconds() < 0 || age.num_seconds() as u64 > refresh_seconds
}

fn cache_path(config: &CatalogConfig) -> Option<PathBuf> {
    if let Some(path) = config.cache.as_deref() {
        return Some(PathBuf::from(expand_home(path)));
    }
    let home = std::env::var_os("QCG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".qcg")))?;
    Some(home.join("cache").join("llm-catalog.json"))
}

fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home)
            .join(rest)
            .to_string_lossy()
            .into_owned();
    }
    path.to_string()
}

fn load_cached_external(config: &CatalogConfig) -> (ExternalCatalog, Option<String>) {
    let Some(path) = cache_path(config) else {
        return (ExternalCatalog::default(), None);
    };
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return (ExternalCatalog::default(), None);
        }
        Err(error) => {
            return (
                ExternalCatalog::default(),
                Some(format!("catalog cache could not be read: {error}")),
            );
        }
    };
    match serde_json::from_slice::<CachedCatalog>(&bytes) {
        Ok(cached) => {
            // Fingerprint gate (F09-02): a cache written under different
            // sources is not verified data for the current pins.
            let expected = config_fingerprint(config);
            if cached.fingerprint.as_deref() != Some(expected.as_str()) {
                return (
                    ExternalCatalog::default(),
                    Some("catalog cache is from different sources; ignoring".to_string()),
                );
            }
            (
                ExternalCatalog {
                    fetched_at: cached.fetched_at,
                    fingerprint: cached.fingerprint,
                    providers: cached.providers,
                },
                None,
            )
        }
        Err(error) => (
            ExternalCatalog::default(),
            Some(format!("catalog cache is corrupt: {error}")),
        ),
    }
}

/// Fingerprint of the source configuration (F09-02): kind plus location
/// plus pin, so a URL/file/hash change invalidates the old cache.
fn config_fingerprint(config: &CatalogConfig) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update([config.source.len() as u8]);
    for source in &config.source {
        hasher.update(source.kind_name().as_bytes());
        hasher.update([0]);
        hasher.update(source.location().as_bytes());
        hasher.update([0]);
        hasher.update(source.sha256.as_deref().unwrap_or("").as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

fn write_cache(config: &CatalogConfig, catalog: &ExternalCatalog) -> Result<(), String> {
    let Some(path) = cache_path(config) else {
        return Ok(());
    };
    let Some(parent) = path.parent() else {
        return Err("catalog cache path has no parent directory".into());
    };
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("catalog cache directory could not be created: {error}"))?;
    let cached = CachedCatalog {
        fetched_at: catalog.fetched_at.clone(),
        fingerprint: catalog
            .fingerprint
            .clone()
            .or_else(|| Some(config_fingerprint(config))),
        providers: catalog.providers.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&cached)
        .map_err(|error| format!("catalog cache is not serializable: {error}"))?;
    // Unique temp name (F09-03): concurrent refreshes must not share one
    // fixed `.tmp` path and clobber each other's publish.
    let tmp = parent.join(format!(
        ".llm-catalog-{}-{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));
    std::fs::write(&tmp, &bytes)
        .map_err(|error| format!("catalog cache could not be written: {error}"))?;
    #[cfg(unix)]
    {
        if let Ok(file) = std::fs::File::open(&tmp) {
            let _ = file.sync_all();
        }
    }
    std::fs::rename(&tmp, &path)
        .map_err(|error| format!("catalog cache could not be published: {error}"))?;
    #[cfg(unix)]
    {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct ModelsDevModel {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    reasoning: bool,
    #[serde(default)]
    reasoning_options: Vec<ModelsDevReasoningOption>,
    #[serde(default)]
    tool_call: bool,
    #[serde(default)]
    structured_output: bool,
    #[serde(default)]
    temperature: bool,
    #[serde(default)]
    attachment: bool,
    #[serde(default)]
    modalities: Option<ModelsDevModalities>,
    #[serde(default)]
    limit: Option<ModelsDevLimit>,
    #[serde(default)]
    cost: Option<ModelsDevCost>,
    #[serde(default)]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ModelsDevReasoningOption {
    #[serde(rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    values: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelsDevModalities {
    #[serde(default)]
    input: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelsDevLimit {
    #[serde(default)]
    context: Option<u64>,
    #[serde(default)]
    output: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ModelsDevCost {
    #[serde(default)]
    input: Option<f64>,
    #[serde(default)]
    output: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct ModelsDevProvider {
    #[serde(default)]
    models: BTreeMap<String, ModelsDevModel>,
}

/// Maps a models.dev `api.json` document onto qcg model metadata. Unknown
/// fields are ignored by the model/dev shapes; malformed JSON fails closed
/// so a broken catalog never silently empties the selection list.
pub(crate) fn map_models_dev(text: &str) -> Result<ExternalCatalog, String> {
    let providers: BTreeMap<String, ModelsDevProvider> = serde_json::from_str(text)
        .map_err(|error| format!("models.dev catalog is not valid JSON: {error}"))?;
    let mut mapped = ExternalCatalog::default();
    for (provider_id, provider) in providers {
        let mut models = BTreeMap::new();
        for (model_id, model) in provider.models {
            let mut capabilities = crate::types::Capabilities {
                tool_use: model.tool_call,
                json_schema: model.structured_output,
                temperature: model.temperature,
                ..crate::types::Capabilities::default()
            };
            if let Some(modalities) = &model.modalities {
                capabilities.image_input = modalities.input.iter().any(|m| m == "image");
                capabilities.audio_input = modalities.input.iter().any(|m| m == "audio");
                capabilities.file_input =
                    modalities.input.iter().any(|m| m == "pdf" || m == "file");
            }
            if model.attachment {
                capabilities.image_input = true;
                capabilities.file_input = true;
            }
            let reasoning_effort = if model.reasoning {
                model
                    .reasoning_options
                    .iter()
                    .filter(|option| option.kind.as_deref() == Some("effort"))
                    .flat_map(|option| option.values.iter())
                    .filter_map(|value| ReasoningEffort::from_snake_case(value))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let enabled = match model.status.as_deref() {
                Some("deprecated") => Some(false),
                _ => None,
            };
            models.insert(
                model_id.clone(),
                ModelSpec {
                    id: model_id,
                    label: model.name,
                    capabilities: Some(capabilities),
                    reasoning_effort: Some(reasoning_effort),
                    input_cost_per_million_usd: model.cost.as_ref().and_then(|cost| cost.input),
                    output_cost_per_million_usd: model.cost.as_ref().and_then(|cost| cost.output),
                    context_tokens: model.limit.as_ref().and_then(|limit| limit.context),
                    max_output_tokens: model.limit.as_ref().and_then(|limit| limit.output),
                    enabled,
                },
            );
        }
        mapped.providers.insert(provider_id, models);
    }
    Ok(mapped)
}

/// Default cache location used by the service when `[catalog].cache` is
/// omitted. Exposed so the CLI and docs agree with the service.
pub fn default_cache_path() -> Option<PathBuf> {
    cache_path(&CatalogConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::ProvidersFile;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    const MODELS_DEV_FIXTURE: &str = r#"
{
  "openai": {
    "models": {
      "gpt-5.2": {
        "name": "GPT-5.2",
        "reasoning": true,
        "reasoning_options": [
          { "type": "effort", "values": ["none", "low", "xhigh"] }
        ],
        "tool_call": true,
        "structured_output": true,
        "temperature": true,
        "attachment": true,
        "modalities": { "input": ["text", "image"], "output": ["text"] },
        "limit": { "context": 400000, "output": 128000 },
        "cost": { "input": 1.75, "output": 14 },
        "status": "deprecated"
      },
      "plain": { "tool_call": false }
    }
  }
}
"#;

    #[test]
    fn models_dev_maps_reasoning_effort_capabilities_and_pricing() {
        let catalog = map_models_dev(MODELS_DEV_FIXTURE).expect("fixture must map");
        let models = catalog.providers.get("openai").expect("provider entry");
        let gpt = models.get("gpt-5.2").expect("model entry");
        assert_eq!(gpt.label.as_deref(), Some("GPT-5.2"));
        assert_eq!(
            gpt.reasoning_effort,
            Some(vec![
                ReasoningEffort::None,
                ReasoningEffort::Low,
                ReasoningEffort::Xhigh
            ])
        );
        let capabilities = gpt.capabilities.as_ref().expect("capabilities");
        assert!(capabilities.tool_use);
        assert!(capabilities.json_schema);
        assert!(capabilities.temperature);
        assert!(capabilities.image_input);
        assert!(capabilities.file_input);
        assert_eq!(gpt.input_cost_per_million_usd, Some(1.75));
        assert_eq!(gpt.output_cost_per_million_usd, Some(14.0));
        assert_eq!(gpt.context_tokens, Some(400_000));
        assert_eq!(gpt.max_output_tokens, Some(128_000));
        assert_eq!(gpt.enabled, Some(false), "deprecated models stay hidden");

        let plain = models.get("plain").expect("plain model");
        assert!(!plain.capabilities.as_ref().expect("caps").tool_use);
        assert_eq!(plain.reasoning_effort, Some(Vec::new()));
    }

    #[test]
    fn explicit_models_override_external_metadata_field_by_field() {
        let external = ModelSpec {
            id: "gpt-5".into(),
            label: Some("External label".into()),
            capabilities: None,
            reasoning_effort: Some(vec![ReasoningEffort::Low, ReasoningEffort::High]),
            input_cost_per_million_usd: Some(1.0),
            output_cost_per_million_usd: Some(2.0),
            context_tokens: Some(1000),
            max_output_tokens: Some(500),
            enabled: None,
        };
        let explicit = ModelSpec {
            id: "gpt-5".into(),
            label: Some("Explicit label".into()),
            capabilities: None,
            reasoning_effort: Some(vec![ReasoningEffort::Minimal]),
            input_cost_per_million_usd: None,
            output_cost_per_million_usd: None,
            context_tokens: None,
            max_output_tokens: None,
            enabled: Some(false),
        };
        let merged = merge_model(&external, &explicit);
        assert_eq!(merged.label.as_deref(), Some("Explicit label"));
        assert_eq!(
            merged.reasoning_effort,
            Some(vec![ReasoningEffort::Minimal]),
            "explicit effort list wins"
        );
        assert_eq!(merged.input_cost_per_million_usd, Some(1.0));
        assert_eq!(merged.context_tokens, Some(1000));
        assert_eq!(merged.enabled, Some(false));
    }

    #[test]
    fn cached_external_catalog_refines_capabilities_and_pricing() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-catalog-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).expect("cache dir");
        let cache = dir.join("catalog.json");
        // Fingerprint for a sourceless config (F09-02): sha256 of [0x00].
        // The cache is only trusted when it names the current sources.
        std::fs::write(
            &cache,
            format!(
                r#"{{
  "fetched_at": "{}",
  "fingerprint": "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
  "providers": {{
    "openai": {{
      "gpt-from-cache": {{
        "id": "gpt-from-cache",
        "reasoning_effort": ["low", "high"],
        "input_cost_per_million_usd": 3.0,
        "output_cost_per_million_usd": 12.0
      }}
    }}
  }}
}}"#,
                chrono::Utc::now().to_rfc3339()
            ),
        )
        .expect("cache write");
        let file = ProvidersFile::parse(&format!(
            r#"
[catalog]
cache = {:?}

[[provider]]
id = "openai"
api = "chat_completions"
base_url = "https://example.test/v1"
chat_token_limit_field = "max_completion_tokens"
capabilities = {{ tool_use = true, reasoning_effort = ["low"] }}
"#,
            cache.to_string_lossy()
        ))
        .expect("registry must parse");
        let service = CatalogService::new(file.provider, file.catalog).expect("catalog service");
        let capabilities = service
            .model_capabilities("openai", "gpt-from-cache")
            .expect("capabilities");
        assert!(capabilities.tool_use);
        assert_eq!(
            capabilities.reasoning_effort,
            vec![ReasoningEffort::Low, ReasoningEffort::High]
        );
        let pricing = service
            .model_pricing("openai", "gpt-from-cache")
            .expect("pricing");
        assert_eq!(pricing.input_cost_per_million_usd, Some(3.0));
        assert_eq!(pricing.output_cost_per_million_usd, Some(12.0));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn openai_discovery_lists_models_over_loopback() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("address");
        let handle = std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = [0_u8; 8192];
            let _ = stream.read(&mut request);
            let body = r#"{"data":[{"id":"alpha"},{"id":"beta"},{"id":"alpha"}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
        });
        let file = ProvidersFile::parse(&format!(
            r#"
[[provider]]
id = "local"
api = "chat_completions"
base_url = "http://{address}/v1"
models_discovery = "openai"
"#
        ))
        .expect("registry must parse");
        let service = CatalogService::new(file.provider, None).expect("catalog service");
        let spec = service.provider("local").expect("provider");
        let models = service.discover(spec).await.expect("discovery");
        assert_eq!(models, vec!["alpha".to_string(), "beta".to_string()]);
        let _ = handle.join();
    }

    #[tokio::test]
    async fn discovery_normal_rotation_and_limits_share_one_path() {
        // F03-03: normal listings, credential rotation, and the body limit
        // all flow through the same credentialed discovery path.
        use std::io::{Read, Write};
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("address");
        let handle = std::thread::spawn(move || {
            // Four requests: key-1 listing, rotated key-2 listing, an
            // over-limit body, and a reflection attack with key-2.
            let bodies = [
                r#"{"data":[{"id":"alpha"}]}"#.to_string(),
                r#"{"data":[{"id":"alpha"}]}"#.to_string(),
                "x".repeat(2 * 1024 * 1024 + 1),
                r#"{"data":[{"id":"QCG-E2E-ROTATED-KEY-2"}]}"#.to_string(),
            ];
            for body in bodies {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") && head.len() < 65536 {
                    let mut byte = [0u8; 1];
                    match stream.read(&mut byte) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => head.push(byte[0]),
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        let file = ProvidersFile::parse(&format!(
            r#"
[[provider]]
id = "local"
api = "chat_completions"
base_url = "http://{address}/v1"
api_key_env = "QCG_TEST_DISCOVER_E2E_KEY"
models_discovery = "openai"
"#
        ))
        .expect("registry must parse");
        let service = CatalogService::new(file.provider, None).expect("catalog service");
        let spec = service.provider("local").expect("provider");
        // SAFETY: unique env name used only by this test.
        unsafe {
            std::env::set_var("QCG_TEST_DISCOVER_E2E_KEY", "QCG-E2E-KEY-1");
        }
        let models = service.discover(spec).await.expect("listing should pass");
        assert_eq!(models, vec!["alpha".to_string()]);
        // Rotation uses the same path with the new credential.
        unsafe {
            std::env::set_var("QCG_TEST_DISCOVER_E2E_KEY", "QCG-E2E-ROTATED-KEY-2");
        }
        let models = service.discover(spec).await.expect("rotated listing");
        assert_eq!(models, vec!["alpha".to_string()]);
        // Over-limit bodies fail closed on the same path.
        let error = service
            .discover(spec)
            .await
            .expect_err("over-limit body must fail");
        assert!(error.contains("exceeded"), "{error}");
        assert!(
            !error.contains("QCG-E2E-ROTATED-KEY-2"),
            "limits must not echo the credential: {error}"
        );
        // A reflection of the current credential is rejected (F03-01/02).
        let error = service
            .discover(spec)
            .await
            .expect_err("reflection must fail");
        assert!(error.contains("credential screening"), "{error}");
        assert!(
            !error.contains("QCG-E2E-ROTATED-KEY-2"),
            "rejections must not echo the credential: {error}"
        );
        unsafe {
            std::env::remove_var("QCG_TEST_DISCOVER_E2E_KEY");
        }
        let _ = handle.join();
    }

    #[test]
    fn discovery_reflection_screening_rejects_credential_echo() {
        // F03-01/F03-02: data id, models name, and JSON-escaped reflections
        // are rejected before publication, without echoing the secret.
        let secret = "QCG-TEST-SECRET-abc123xyz".to_string();
        let raw = format!(r#"{{"data":[{{"id":"{secret}"}}]}}"#).into_bytes();
        let decoded: Value = serde_json::from_slice(&raw).expect("fixture JSON");
        let reason = credential_reflection(&raw, &decoded, std::slice::from_ref(&secret))
            .expect("direct echo must be rejected");
        assert!(reason.contains("credential"), "{reason}");

        let raw_models =
            format!(r#"{{"models":[{{"name":"prefix-{secret}-suffix"}}]}}"#).into_bytes();
        let decoded_models: Value = serde_json::from_slice(&raw_models).expect("fixture JSON");
        assert!(
            credential_reflection(&raw_models, &decoded_models, std::slice::from_ref(&secret))
                .is_some(),
            "models[].name reflection must be rejected"
        );

        // Normal ids pass, and rotation (different credential) passes.
        let benign = br#"{"data":[{"id":"gpt-5"},{"name":"o3"}]}"#;
        let benign_decoded: Value = serde_json::from_slice(benign).expect("fixture JSON");
        assert!(
            credential_reflection(benign, &benign_decoded, &[secret]).is_none(),
            "benign listing must pass"
        );
        assert!(
            credential_reflection(benign, &benign_decoded, &["rotated-key-999".to_string()])
                .is_none(),
            "rotated credential must use the same path"
        );
    }

    #[test]
    fn staleness_recomputes_from_time_not_a_stored_flag() {
        // F09-01: staleness derives from fetched_at + now on every read.
        assert!(snapshot_stale(None, 3600), "missing fetch is stale");
        let old = (chrono::Utc::now() - chrono::Duration::seconds(7200)).to_rfc3339();
        assert!(
            snapshot_stale(Some(old.as_str()), 3600),
            "TTL-exceeded fetch is stale"
        );
        let fresh = chrono::Utc::now().to_rfc3339();
        assert!(
            !snapshot_stale(Some(fresh.as_str()), 3600),
            "fresh fetch is not stale"
        );
        assert!(
            snapshot_stale(Some("not-a-date"), 3600),
            "unparseable is stale"
        );
    }

    #[test]
    fn cache_from_other_sources_is_ignored() {
        // F09-02: a cache fingerprint from different sources is never
        // trusted as verified data for the current pins.
        let config = CatalogConfig {
            cache: None,
            refresh_seconds: 86_400,
            source: vec![CatalogSourceSpec {
                kind: CatalogSourceKind::ModelsDev,
                url: Some("https://example.test/api.json".into()),
                file: None,
                sha256: None,
            }],
        };
        let expected = config_fingerprint(&config);
        let other = CatalogConfig::default();
        assert_ne!(
            config_fingerprint(&other),
            expected,
            "different sources must fingerprint differently"
        );
    }

    #[tokio::test]
    async fn view_matches_runtime_overlay_for_partial_explicit() {
        // F09-04: a partially explicit model shows the same merged
        // capabilities/pricing in the view as runtime resolution uses.
        use crate::provider::ProvidersFile;
        let file = ProvidersFile::parse(
            r#"
[[provider]]
id = "openai"
api = "chat_completions"
base_url = "https://example.test/v1"
chat_token_limit_field = "max_completion_tokens"
capabilities = { tool_use = true }

[[provider.models]]
id = "gpt-5"
label = "Explicit label"
"#,
        )
        .expect("registry must parse");
        let service = CatalogService::new(file.provider, None).expect("catalog service");
        // Inject an external entry for the same model with pricing the
        // explicit declaration leaves unset.
        {
            let mut snapshot = service
                .snapshot
                .write()
                .unwrap_or_else(|error| error.into_inner());
            let mut models = BTreeMap::new();
            models.insert(
                "gpt-5".to_string(),
                crate::provider::ModelSpec {
                    id: "gpt-5".into(),
                    label: Some("External label".into()),
                    capabilities: None,
                    reasoning_effort: Some(vec![ReasoningEffort::Low]),
                    input_cost_per_million_usd: Some(1.0),
                    output_cost_per_million_usd: Some(2.0),
                    context_tokens: Some(1000),
                    max_output_tokens: Some(500),
                    enabled: None,
                },
            );
            let mut providers = BTreeMap::new();
            providers.insert("openai".to_string(), models);
            snapshot.external = ExternalCatalog {
                fetched_at: Some(chrono::Utc::now().to_rfc3339()),
                fingerprint: None,
                providers,
            };
        }
        service.rebuild_models();
        let runtime_pricing = service
            .model_pricing("openai", "gpt-5")
            .expect("runtime pricing");
        assert_eq!(runtime_pricing.input_cost_per_million_usd, Some(1.0));
        let view = service.view(false).await;
        let provider = view
            .providers
            .iter()
            .find(|p| p.id == "openai")
            .expect("provider view");
        let model = provider
            .models
            .iter()
            .find(|m| m.id == "gpt-5")
            .expect("model view");
        assert_eq!(
            model.input_cost_per_million_usd, runtime_pricing.input_cost_per_million_usd,
            "view pricing must equal runtime pricing"
        );
        assert_eq!(model.label.as_deref(), Some("Explicit label"));
    }

    fn file_source_config(cache: &std::path::Path, source: &std::path::Path) -> CatalogConfig {
        CatalogConfig {
            cache: Some(cache.to_string_lossy().into_owned()),
            refresh_seconds: 1,
            source: vec![CatalogSourceSpec {
                kind: CatalogSourceKind::ModelsDev,
                url: None,
                file: Some(source.to_string_lossy().into_owned()),
                sha256: None,
            }],
        }
    }

    fn write_models_dev(path: &std::path::Path, model: &str) {
        std::fs::write(
            path,
            format!(r#"{{"openai": {{"models": {{"{model}": {{"tool_call": true}}}}}}}}"#),
        )
        .expect("fixture should write");
    }

    fn temp_catalog_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "qcg-catalog-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).expect("dir should create");
        dir
    }

    #[tokio::test]
    async fn staleness_returns_after_ttl_and_view_agrees() {
        // F09-01: a successful refresh clears staleness, the TTL expiry
        // brings it back, and the view reports the same value so the
        // resident loop refreshes again.
        let dir = temp_catalog_dir("stale");
        let source = dir.join("api.json");
        write_models_dev(&source, "gpt-stale");
        let config = file_source_config(&dir.join("cache.json"), &source);
        let service = CatalogService::new(Vec::new(), Some(config)).expect("service should build");
        assert!(service.is_stale(), "missing fetch starts stale");
        service.refresh().await.expect("refresh should succeed");
        assert!(!service.is_stale(), "fresh fetch is not stale");
        let view = service.view(false).await;
        assert!(!view.stale, "the view must agree while fresh");
        // Whole-second TTL comparison needs a full two seconds past the
        // one-second TTL.
        std::thread::sleep(std::time::Duration::from_millis(2200));
        assert!(service.is_stale(), "TTL expiry must restore staleness");
        assert!(
            service.view(false).await.stale,
            "the view must agree once stale"
        );
        service.refresh().await.expect("re-refresh should succeed");
        assert!(!service.is_stale(), "re-refresh clears staleness again");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn concurrent_refreshes_converge_on_a_complete_snapshot() {
        // F09-03: concurrent refreshes publish complete snapshots (no
        // fixed-temp clobbering) and both report success.
        let dir = temp_catalog_dir("concurrent");
        let source = dir.join("api.json");
        write_models_dev(&source, "gpt-race");
        let config = file_source_config(&dir.join("cache.json"), &source);
        let service = CatalogService::new(Vec::new(), Some(config)).expect("service should build");
        let (first, second) = tokio::join!(service.refresh(), service.refresh());
        first.expect("first refresh should succeed");
        second.expect("concurrent refresh should succeed");
        let snapshot = service
            .snapshot
            .read()
            .unwrap_or_else(|error| error.into_inner());
        assert!(
            snapshot
                .external
                .providers
                .get("openai")
                .is_some_and(|models| models.contains_key("gpt-race")),
            "the snapshot must be complete after concurrent refresh"
        );
        assert!(snapshot.external.fetched_at.is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cache_save_failures_are_reported_not_silent() {
        // F09-03: when the cache cannot persist, refresh fails loudly,
        // records the cache error, and keeps the previous snapshot instead
        // of advertising fresh data it did not store.
        let dir = temp_catalog_dir("save-fail");
        let source = dir.join("api.json");
        write_models_dev(&source, "gpt-lost");
        let blocker = dir.join("blocker");
        std::fs::write(&blocker, b"not a directory").expect("blocker should write");
        let config = file_source_config(&blocker.join("cache.json"), &source);
        let service = CatalogService::new(Vec::new(), Some(config)).expect("service should build");
        let error = service
            .refresh()
            .await
            .expect_err("an unwritable cache must fail refresh");
        assert!(error.contains("cache"), "{error}");
        assert!(
            service.is_stale(),
            "the snapshot must stay stale when persistence failed"
        );
        assert!(
            service
                .snapshot
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .sources
                .iter()
                .any(|status| status.kind == "cache" && status.error.is_some()),
            "the cache failure must stay visible per source"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
