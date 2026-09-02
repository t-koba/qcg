mod bounded_http;
mod bounded_stdio;

use async_trait::async_trait;
use bounded_http::BoundedHttpClient;
use bounded_stdio::BoundedChildTransport;
use keyring::Entry;
use qcg_types::credential_like_name;
use reqwest::redirect::Policy;
use rmcp::ClientHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, CancelTaskParams, ClientCapabilities,
    ClientInfo, GetTaskParams, Implementation, InputResponses, PaginatedRequestParams,
    ProtocolVersion, TASKS_EXTENSION_ID, TaskPayload, Tool,
};
use rmcp::service::{ClientLifecycleMode, ClientServiceExt as _, RoleClient, RunningService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::auth::{
    AuthClient, AuthError, AuthorizationManager, AuthorizationRequest, CredentialStore,
    InMemoryCredentialStore, OAuthHttpClient, OAuthHttpClientFuture, OAuthHttpRedirectPolicy,
    OAuthHttpRequest, OAuthState, StoredCredentials,
};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

const KEYRING_SERVICE: &str = "qcg.mcp.oauth";
const DEFAULT_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TIMEOUT_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const AUTHORIZATION_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_TOOL_LIST_PAGES: usize = 100;

#[derive(Debug, Clone, Copy)]
struct QcgMcpClient;

impl ClientHandler for QcgMcpClient {
    fn get_info(&self) -> ClientInfo {
        let mut capabilities = ClientCapabilities::default();
        capabilities.extensions = Some(BTreeMap::from([(
            TASKS_EXTENSION_ID.to_string(),
            Default::default(),
        )]));
        ClientInfo::new(
            capabilities,
            Implementation::new("qcg", env!("CARGO_PKG_VERSION")),
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum McpError {
    #[error("{0}")]
    Configuration(String),
    #[error("MCP server `{server}` requires OAuth authorization; connect it from the qcg web UI")]
    AuthorizationRequired { server: String },
    #[error("MCP authorization failed: {0}")]
    Authorization(String),
    #[error("MCP transport failed: {0}")]
    Transport(String),
    #[error("MCP tool `{tool}` returned an error")]
    ToolFailed { tool: String, result: Value },
    #[error("MCP operation timed out after {seconds} seconds")]
    TimedOut { seconds: u64 },
    #[error("MCP operation was canceled")]
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpInputRequired {
    pub input_requests: BTreeMap<String, Value>,
    pub request_state: Option<String>,
}

#[derive(Debug, Clone)]
pub enum McpCallOutcome {
    Complete(Value),
    InputRequired(McpInputRequired),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTransport {
    StreamableHttp,
    Stdio,
}

fn default_transport() -> McpTransport {
    McpTransport::StreamableHttp
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpLifecycle {
    Initialize,
    Discover,
}

fn default_lifecycle() -> McpLifecycle {
    McpLifecycle::Discover
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpAuth {
    None,
    Bearer,
    Header,
    Oauth,
}

fn default_auth() -> McpAuth {
    McpAuth::None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthCredentialStore {
    Keyring,
    Memory,
}

fn default_oauth_store() -> OAuthCredentialStore {
    OAuthCredentialStore::Keyring
}

fn default_timeout_seconds() -> u64 {
    DEFAULT_TIMEOUT_SECONDS
}

fn default_max_response_bytes() -> usize {
    DEFAULT_MAX_RESPONSE_BYTES
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerSpec {
    pub id: String,
    #[serde(default = "default_transport")]
    pub transport: McpTransport,
    #[serde(default = "default_lifecycle")]
    pub lifecycle: McpLifecycle,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub env_from: BTreeMap<String, String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "default_auth")]
    pub auth: McpAuth,
    #[serde(default)]
    pub credential_env: Option<String>,
    #[serde(default)]
    pub auth_header: Option<String>,
    #[serde(default)]
    pub auth_prefix: String,
    #[serde(default)]
    pub oauth_scopes: Vec<String>,
    #[serde(default)]
    pub oauth_client_id_env: Option<String>,
    #[serde(default)]
    pub oauth_client_secret_env: Option<String>,
    #[serde(default = "default_oauth_store")]
    pub oauth_store: OAuthCredentialStore,
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
}

impl std::fmt::Debug for McpServerSpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpServerSpec")
            .field("id", &self.id)
            .field("transport", &self.transport)
            .field("lifecycle", &self.lifecycle)
            .field("url", &self.url.as_ref().map(|_| "<configured>"))
            .field("command_bin", &self.command.first())
            .field("command_arg_count", &self.command.len().saturating_sub(1))
            .field("env", &self.env.keys().collect::<Vec<_>>())
            .field("env_from", &self.env_from)
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("auth", &self.auth)
            .field("credential_env", &self.credential_env)
            .field("auth_header", &self.auth_header)
            .field(
                "auth_prefix",
                &(!self.auth_prefix.is_empty()).then_some("<redacted>"),
            )
            .field("oauth_scopes", &self.oauth_scopes)
            .field("oauth_client_id_env", &self.oauth_client_id_env)
            .field("oauth_client_secret_env", &self.oauth_client_secret_env)
            .field("oauth_store", &self.oauth_store)
            .field("allowed_hosts", &self.allowed_hosts)
            .field("timeout_seconds", &self.timeout_seconds)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

impl McpServerSpec {
    pub fn validate(&self) -> Result<(), String> {
        if !valid_id(&self.id) {
            return Err(format!(
                "MCP server id `{}` must contain only lowercase ASCII letters, digits, `.`, `_`, or `-`",
                self.id
            ));
        }
        if self.timeout_seconds == 0 {
            return Err(format!(
                "MCP server `{}` timeout_seconds must be greater than zero",
                self.id
            ));
        }
        if self.timeout_seconds > MAX_TIMEOUT_SECONDS {
            return Err(format!(
                "MCP server `{}` timeout_seconds must not exceed {MAX_TIMEOUT_SECONDS}",
                self.id
            ));
        }
        if self.max_response_bytes == 0 {
            return Err(format!(
                "MCP server `{}` max_response_bytes must be greater than zero",
                self.id
            ));
        }
        if self.max_response_bytes > MAX_RESPONSE_BYTES {
            return Err(format!(
                "MCP server `{}` max_response_bytes must not exceed {MAX_RESPONSE_BYTES}",
                self.id
            ));
        }
        for (name, value) in &self.headers {
            let header = http::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| format!("MCP server `{}` has invalid header `{name}`", self.id))?;
            http::HeaderValue::from_str(value)
                .map_err(|_| format!("MCP server `{}` has invalid header `{name}`", self.id))?;
            if reserved_transport_header(header.as_str()) {
                return Err(format!(
                    "MCP server `{}` static headers must not override MCP transport header `{header}`",
                    self.id
                ));
            }
            if credential_like_name(header.as_str()) {
                return Err(format!(
                    "MCP server `{}` static headers must not contain credentials",
                    self.id
                ));
            }
        }
        for (target, source) in &self.env_from {
            if !valid_env_name(target) || !valid_env_name(source) {
                return Err(format!(
                    "MCP server `{}` env_from must map valid environment variable names",
                    self.id
                ));
            }
            if dangerous_process_env_name(target) {
                return Err(format!(
                    "MCP server `{}` env_from must not override process control variable `{target}`",
                    self.id
                ));
            }
        }
        if self.env.keys().any(|name| !valid_env_name(name)) {
            return Err(format!(
                "MCP server `{}` env contains an invalid environment variable name",
                self.id
            ));
        }
        if let Some(name) = self
            .env
            .keys()
            .find(|name| dangerous_process_env_name(name))
        {
            return Err(format!(
                "MCP server `{}` env must not override process control variable `{name}`",
                self.id
            ));
        }
        if let Some(name) = self.env.keys().find(|name| credential_like_name(name)) {
            return Err(format!(
                "MCP server `{}` must load sensitive environment variable `{name}` through env_from",
                self.id
            ));
        }
        match self.transport {
            McpTransport::StreamableHttp => {
                if !self.command.is_empty() || !self.env.is_empty() || !self.env_from.is_empty() {
                    return Err(format!(
                        "MCP server `{}` streamable_http transport must not declare command or environment fields",
                        self.id
                    ));
                }
                let raw = self.url.as_deref().ok_or_else(|| {
                    format!(
                        "MCP server `{}` streamable_http transport requires url",
                        self.id
                    )
                })?;
                let url = validate_remote_url(&self.id, raw)?;
                let endpoint_host = url.host_str().expect("validated URL has host");
                if !self.allowed_hosts.iter().any(|host| host == endpoint_host) {
                    return Err(format!(
                        "MCP server `{}` allowed_hosts must include endpoint host `{endpoint_host}`",
                        self.id
                    ));
                }
                for host in &self.allowed_hosts {
                    validate_host(host).map_err(|error| {
                        format!("MCP server `{}` has invalid allowed host: {error}", self.id)
                    })?;
                }
            }
            McpTransport::Stdio => {
                if self.url.is_some() || !self.headers.is_empty() || !self.allowed_hosts.is_empty()
                {
                    return Err(format!(
                        "MCP server `{}` stdio transport must not declare HTTP fields",
                        self.id
                    ));
                }
                if self
                    .command
                    .first()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    return Err(format!(
                        "MCP server `{}` stdio transport requires a non-empty command",
                        self.id
                    ));
                }
                if self.auth != McpAuth::None {
                    return Err(format!(
                        "MCP server `{}` stdio transport must use auth = \"none\"; pass credentials through env_from",
                        self.id
                    ));
                }
            }
        }
        match self.auth {
            McpAuth::None => {
                if self.credential_env.is_some()
                    || self.auth_header.is_some()
                    || !self.auth_prefix.is_empty()
                    || self.oauth_client_id_env.is_some()
                    || self.oauth_client_secret_env.is_some()
                    || !self.oauth_scopes.is_empty()
                {
                    return Err(format!(
                        "MCP server `{}` auth = \"none\" must not declare authentication fields",
                        self.id
                    ));
                }
            }
            McpAuth::Bearer => {
                self.validate_credential_env()?;
                if self.auth_header.is_some() || !self.auth_prefix.is_empty() {
                    return Err(format!(
                        "MCP server `{}` bearer auth uses the Authorization header implicitly",
                        self.id
                    ));
                }
                self.reject_oauth_fields()?;
            }
            McpAuth::Header => {
                self.validate_credential_env()?;
                let header = self.auth_header.as_deref().ok_or_else(|| {
                    format!("MCP server `{}` header auth requires auth_header", self.id)
                })?;
                let header = http::HeaderName::from_bytes(header.as_bytes())
                    .map_err(|_| format!("MCP server `{}` has invalid auth_header", self.id))?;
                if reserved_transport_header(header.as_str()) {
                    return Err(format!(
                        "MCP server `{}` auth_header must not override an MCP transport header",
                        self.id
                    ));
                }
                if self.auth_prefix.contains(['\r', '\n']) {
                    return Err(format!(
                        "MCP server `{}` auth_prefix must not contain line breaks",
                        self.id
                    ));
                }
                self.reject_oauth_fields()?;
            }
            McpAuth::Oauth => {
                if self.credential_env.is_some()
                    || self.auth_header.is_some()
                    || !self.auth_prefix.is_empty()
                {
                    return Err(format!(
                        "MCP server `{}` OAuth must not declare static credential fields",
                        self.id
                    ));
                }
                if let Some(name) = self.oauth_client_id_env.as_deref()
                    && !valid_env_name(name)
                {
                    return Err(format!(
                        "MCP server `{}` has invalid oauth_client_id_env",
                        self.id
                    ));
                }
                if let Some(name) = self.oauth_client_secret_env.as_deref()
                    && !valid_env_name(name)
                {
                    return Err(format!(
                        "MCP server `{}` has invalid oauth_client_secret_env",
                        self.id
                    ));
                }
                if self.oauth_client_secret_env.is_some() && self.oauth_client_id_env.is_none() {
                    return Err(format!(
                        "MCP server `{}` oauth_client_secret_env requires oauth_client_id_env",
                        self.id
                    ));
                }
                if self
                    .oauth_scopes
                    .iter()
                    .any(|scope| scope.trim().is_empty())
                {
                    return Err(format!(
                        "MCP server `{}` oauth_scopes must not contain empty values",
                        self.id
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_credential_env(&self) -> Result<(), String> {
        let name = self.credential_env.as_deref().ok_or_else(|| {
            format!(
                "MCP server `{}` authentication requires credential_env",
                self.id
            )
        })?;
        if !valid_env_name(name) {
            return Err(format!(
                "MCP server `{}` has invalid credential_env",
                self.id
            ));
        }
        Ok(())
    }

    fn reject_oauth_fields(&self) -> Result<(), String> {
        if self.oauth_client_id_env.is_some()
            || self.oauth_client_secret_env.is_some()
            || !self.oauth_scopes.is_empty()
        {
            return Err(format!(
                "MCP server `{}` non-OAuth auth must not declare OAuth fields",
                self.id
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct McpProfile {
    spec: Arc<McpServerSpec>,
    url: Option<Url>,
}

impl McpProfile {
    pub fn id(&self) -> &str {
        &self.spec.id
    }

    pub fn transport(&self) -> McpTransport {
        self.spec.transport
    }

    pub fn transport_name(&self) -> &'static str {
        match self.spec.transport {
            McpTransport::StreamableHttp => "streamable_http",
            McpTransport::Stdio => "stdio",
        }
    }

    pub fn auth_name(&self) -> &'static str {
        match self.spec.auth {
            McpAuth::None => "none",
            McpAuth::Bearer => "bearer",
            McpAuth::Header => "header",
            McpAuth::Oauth => "oauth",
        }
    }

    pub fn allowed_hosts(&self) -> &[String] {
        &self.spec.allowed_hosts
    }

    pub fn command(&self) -> &[String] {
        &self.spec.command
    }

    pub fn timeout_seconds(&self) -> u64 {
        self.spec.timeout_seconds
    }

    pub fn max_response_bytes(&self) -> usize {
        self.spec.max_response_bytes
    }

    pub fn credential_env_names(&self) -> Vec<String> {
        let mut names = self.spec.env_from.values().cloned().collect::<Vec<_>>();
        names.extend(self.spec.credential_env.iter().cloned());
        names.extend(self.spec.oauth_client_id_env.iter().cloned());
        names.extend(self.spec.oauth_client_secret_env.iter().cloned());
        names.sort();
        names.dedup();
        names
    }
}

#[derive(Clone)]
enum ProfileCredentialStore {
    Keyring(KeyringCredentialStore),
    Memory(InMemoryCredentialStore),
}

#[derive(Clone)]
enum CredentialGuard {
    None,
    Static(Vec<String>),
    OAuth(AuthClient<BoundedHttpClient>),
}

impl std::fmt::Debug for CredentialGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::None => "CredentialGuard::None",
            Self::Static(_) => "CredentialGuard::Static(<redacted>)",
            Self::OAuth(_) => "CredentialGuard::OAuth(<redacted>)",
        })
    }
}

impl CredentialGuard {
    async fn values(&self) -> Result<Vec<String>, McpError> {
        match self {
            Self::None => Ok(Vec::new()),
            Self::Static(values) => Ok(values.clone()),
            Self::OAuth(client) => client
                .get_access_token()
                .await
                .map(|value| vec![value])
                .map_err(auth_error),
        }
    }
}

#[async_trait]
impl CredentialStore for ProfileCredentialStore {
    async fn load(&self) -> Result<Option<StoredCredentials>, AuthError> {
        match self {
            Self::Keyring(store) => store.load().await,
            Self::Memory(store) => store.load().await,
        }
    }

    async fn save(&self, credentials: StoredCredentials) -> Result<(), AuthError> {
        match self {
            Self::Keyring(store) => store.save(credentials).await,
            Self::Memory(store) => store.save(credentials).await,
        }
    }

    async fn clear(&self) -> Result<(), AuthError> {
        match self {
            Self::Keyring(store) => store.clear().await,
            Self::Memory(store) => store.clear().await,
        }
    }
}

#[derive(Debug, Clone)]
struct KeyringCredentialStore {
    account: String,
}

impl KeyringCredentialStore {
    fn new(account: impl Into<String>) -> Self {
        Self {
            account: account.into(),
        }
    }
}

#[async_trait]
impl CredentialStore for KeyringCredentialStore {
    async fn load(&self) -> Result<Option<StoredCredentials>, AuthError> {
        let account = self.account.clone();
        tokio::task::spawn_blocking(move || {
            let entry = Entry::new(KEYRING_SERVICE, &account)
                .map_err(|error| AuthError::InternalError(error.to_string()))?;
            match entry.get_secret() {
                Ok(secret) => serde_json::from_slice(&secret)
                    .map(Some)
                    .map_err(|error| AuthError::InternalError(error.to_string())),
                Err(keyring::Error::NoEntry) => Ok(None),
                Err(error) => Err(AuthError::InternalError(error.to_string())),
            }
        })
        .await
        .map_err(|error| AuthError::InternalError(error.to_string()))?
    }

    async fn save(&self, credentials: StoredCredentials) -> Result<(), AuthError> {
        let account = self.account.clone();
        let secret = serde_json::to_vec(&credentials)
            .map_err(|error| AuthError::InternalError(error.to_string()))?;
        tokio::task::spawn_blocking(move || {
            Entry::new(KEYRING_SERVICE, &account)
                .and_then(|entry| entry.set_secret(&secret))
                .map_err(|error| AuthError::InternalError(error.to_string()))
        })
        .await
        .map_err(|error| AuthError::InternalError(error.to_string()))?
    }

    async fn clear(&self) -> Result<(), AuthError> {
        let account = self.account.clone();
        tokio::task::spawn_blocking(move || {
            let entry = Entry::new(KEYRING_SERVICE, &account)
                .map_err(|error| AuthError::InternalError(error.to_string()))?;
            match entry.delete_credential() {
                Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
                Err(error) => Err(AuthError::InternalError(error.to_string())),
            }
        })
        .await
        .map_err(|error| AuthError::InternalError(error.to_string()))?
    }
}

struct PendingAuthorization {
    server_id: String,
    state: OAuthState,
    expires_at: Instant,
}

#[derive(Debug)]
struct AllowedOAuthHttpClient {
    client: reqwest::Client,
    stop_client: reqwest::Client,
    allowed_hosts: BTreeSet<String>,
    max_response_bytes: usize,
}

impl AllowedOAuthHttpClient {
    fn new(profile: &McpProfile) -> Result<Self, McpError> {
        let allowed_hosts = profile
            .spec
            .allowed_hosts
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let redirect_hosts = allowed_hosts.clone();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(profile.spec.timeout_seconds))
            .redirect(Policy::custom(move |attempt| {
                if attempt.previous().len() >= 5 {
                    return attempt.stop();
                }
                let previous_host = attempt.previous().last().and_then(|url| url.host_str());
                match attempt.url().host_str() {
                    Some(host)
                        if redirect_hosts.contains(host)
                            && previous_host == Some(host)
                            && attempt
                                .previous()
                                .last()
                                .is_some_and(|url| url.scheme() == attempt.url().scheme()) =>
                    {
                        attempt.follow()
                    }
                    _ => attempt.stop(),
                }
            }))
            .build()
            .map_err(|error| McpError::Configuration(error.to_string()))?;
        let stop_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(profile.spec.timeout_seconds))
            .redirect(Policy::none())
            .build()
            .map_err(|error| McpError::Configuration(error.to_string()))?;
        Ok(Self {
            client,
            stop_client,
            allowed_hosts,
            max_response_bytes: profile.spec.max_response_bytes,
        })
    }
}

impl OAuthHttpClient for AllowedOAuthHttpClient {
    fn execute(&self, operation: OAuthHttpRequest) -> OAuthHttpClientFuture<'_> {
        Box::pin(async move {
            let OAuthHttpRequest {
                request,
                redirect_policy,
                timeout,
                ..
            } = operation;
            let url = Url::parse(&request.uri().to_string())?;
            validate_oauth_operation_url(&url)?;
            let host = url
                .host_str()
                .ok_or_else(|| "OAuth request URL omitted a host".to_string())?;
            if !self.allowed_hosts.contains(host) {
                return Err(format!("OAuth request host `{host}` is not allowed").into());
            }
            if !is_secure_remote_url(&url) {
                return Err("OAuth request URL must use HTTPS, or HTTP on loopback".into());
            }
            let mut request = reqwest::Request::try_from(request)?;
            if redirect_policy == OAuthHttpRedirectPolicy::Stop {
                if let Some(timeout) = timeout {
                    *request.timeout_mut() = Some(timeout);
                }
                let response = self.stop_client.execute(request).await?;
                validate_oauth_operation_url(response.url())?;
                return bounded_oauth_response(response, self.max_response_bytes).await;
            }
            if let Some(timeout) = timeout {
                *request.timeout_mut() = Some(timeout);
            }
            let response = self.client.execute(request).await?;
            validate_oauth_operation_url(response.url())?;
            if let Some(host) = response.url().host_str()
                && !self.allowed_hosts.contains(host)
            {
                return Err(format!("OAuth redirect host `{host}` is not allowed").into());
            }
            if !is_secure_remote_url(response.url()) {
                return Err("OAuth redirect URL must use HTTPS, or HTTP on loopback".into());
            }
            bounded_oauth_response(response, self.max_response_bytes).await
        })
    }
}

async fn bounded_oauth_response(
    response: reqwest::Response,
    max_response_bytes: usize,
) -> Result<oauth2::HttpResponse, rmcp::transport::auth::OAuthHttpClientError> {
    use futures_util::StreamExt as _;

    let mut builder = oauth2::http::Response::builder()
        .status(response.status())
        .version(response.version());
    for (name, value) in response.headers() {
        builder = builder.header(name, value);
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if chunk.len() > max_response_bytes.saturating_sub(body.len()) {
            return Err(format!("OAuth response exceeded {max_response_bytes} bytes").into());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(builder.body(body)?)
}

struct McpRuntimeInner {
    profiles: BTreeMap<String, McpProfile>,
    stores: BTreeMap<String, ProfileCredentialStore>,
    authorized_clients: Mutex<HashMap<String, AuthClient<BoundedHttpClient>>>,
    pending: Mutex<HashMap<String, PendingAuthorization>>,
    active_sessions: BTreeMap<String, Arc<AtomicUsize>>,
    lifecycle_gates: BTreeMap<String, Arc<Mutex<()>>>,
}

#[derive(Clone)]
pub struct McpRuntime {
    inner: Arc<McpRuntimeInner>,
}

impl std::fmt::Debug for McpRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpRuntime")
            .field("profiles", &self.inner.profiles.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl McpRuntime {
    pub fn unavailable() -> Self {
        Self::from_specs(Vec::new()).expect("empty MCP registry is valid")
    }

    /// Creates a registry containing qcg's anonymous, read-only public MCP
    /// endpoints. These profiles require no providers file or credential.
    pub fn public_defaults() -> Self {
        Self::from_specs(public_default_specs()).expect("built-in public MCP profiles are valid")
    }

    /// Adds the built-in public profiles and rejects attempts to override their
    /// reserved ids.
    pub fn from_specs_with_public_defaults(mut specs: Vec<McpServerSpec>) -> Result<Self, String> {
        let defaults = public_default_specs();
        if let Some(spec) = specs
            .iter()
            .find(|spec| defaults.iter().any(|default| default.id == spec.id))
        {
            return Err(format!(
                "MCP server id `{}` is reserved for a built-in public profile",
                spec.id
            ));
        }
        specs.extend(defaults);
        Self::from_specs(specs)
    }

    pub fn from_specs(specs: Vec<McpServerSpec>) -> Result<Self, String> {
        let mut profiles = BTreeMap::new();
        let mut stores = BTreeMap::new();
        let mut active_sessions = BTreeMap::new();
        let mut lifecycle_gates = BTreeMap::new();
        for spec in specs {
            spec.validate()?;
            let id = spec.id.clone();
            if profiles.contains_key(&id) {
                return Err(format!("duplicate MCP server id `{id}`"));
            }
            let url = spec
                .url
                .as_deref()
                .map(|raw| validate_remote_url(&id, raw))
                .transpose()?;
            let keyring_account = match spec.url.as_deref() {
                Some(url) => format!("{}@{url}", spec.id),
                None => spec.id.clone(),
            };
            let store = match spec.oauth_store {
                OAuthCredentialStore::Keyring => {
                    ProfileCredentialStore::Keyring(KeyringCredentialStore::new(keyring_account))
                }
                OAuthCredentialStore::Memory => {
                    ProfileCredentialStore::Memory(InMemoryCredentialStore::new())
                }
            };
            profiles.insert(
                id.clone(),
                McpProfile {
                    spec: Arc::new(spec),
                    url,
                },
            );
            stores.insert(id.clone(), store);
            active_sessions.insert(id.clone(), Arc::new(AtomicUsize::new(0)));
            lifecycle_gates.insert(id, Arc::new(Mutex::new(())));
        }
        Ok(Self {
            inner: Arc::new(McpRuntimeInner {
                profiles,
                stores,
                authorized_clients: Mutex::new(HashMap::new()),
                pending: Mutex::new(HashMap::new()),
                active_sessions,
                lifecycle_gates,
            }),
        })
    }

    pub fn resolve(&self, id: &str) -> Result<&McpProfile, McpError> {
        self.inner
            .profiles
            .get(id)
            .ok_or_else(|| McpError::Configuration(format!("MCP server `{id}` is not registered")))
    }

    pub fn server_ids(&self) -> Vec<&str> {
        self.inner.profiles.keys().map(String::as_str).collect()
    }

    pub fn credential_env_names(&self) -> Vec<String> {
        self.inner
            .profiles
            .values()
            .flat_map(McpProfile::credential_env_names)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub async fn is_authorized(&self, server_id: &str) -> Result<bool, McpError> {
        let profile = self.resolve(server_id)?;
        if profile.spec.auth != McpAuth::Oauth {
            return Ok(true);
        }
        if self
            .inner
            .authorized_clients
            .lock()
            .await
            .contains_key(server_id)
        {
            return Ok(true);
        }
        self.store(profile)
            .load()
            .await
            .map(|credentials| credentials.is_some())
            .map_err(auth_error)
    }

    pub async fn start_authorization(
        &self,
        server_id: &str,
        redirect_uri: &str,
    ) -> Result<String, McpError> {
        let profile = self.resolve(server_id)?.clone();
        if profile.spec.auth != McpAuth::Oauth {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` does not use OAuth"
            )));
        }
        let lifecycle_gate = self.lifecycle_gate(server_id);
        let _lifecycle = lifecycle_gate.lock().await;
        if self
            .inner
            .authorized_clients
            .lock()
            .await
            .contains_key(server_id)
        {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` is already authorized"
            )));
        }
        let mut state = self.authorization_manager(&profile).await?;
        if matches!(state, OAuthState::Authorized(_)) {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` is already authorized"
            )));
        }
        validate_redirect_uri(redirect_uri)?;
        let mut request = AuthorizationRequest::new(redirect_uri)
            .with_client_name("qcg")
            .with_scopes(profile.spec.oauth_scopes.clone());
        if let Some(name) = profile.spec.oauth_client_id_env.as_deref() {
            let client_id = required_env(name)?;
            request = request.with_preregistered_client(client_id);
            if let Some(secret_name) = profile.spec.oauth_client_secret_env.as_deref() {
                request = request.with_client_secret(required_env(secret_name)?);
            }
        }
        state
            .start_authorization(request)
            .await
            .map_err(auth_error)?;
        let authorization_url = state.get_authorization_url().await.map_err(auth_error)?;
        let authorization_url_parsed = Url::parse(&authorization_url)
            .map_err(|error| McpError::Authorization(error.to_string()))?;
        let authorization_host = authorization_url_parsed
            .host_str()
            .ok_or_else(|| McpError::Authorization("authorization URL omitted a host".into()))?;
        if !is_secure_remote_url(&authorization_url_parsed)
            || !authorization_url_parsed.username().is_empty()
            || authorization_url_parsed.password().is_some()
            || authorization_url_parsed.fragment().is_some()
            || !profile
                .spec
                .allowed_hosts
                .iter()
                .any(|host| host == authorization_host)
        {
            return Err(McpError::Authorization(format!(
                "authorization URL host `{authorization_host}` is not allowed"
            )));
        }
        let csrf = authorization_url_parsed
            .query_pairs()
            .find_map(|(name, value)| (name == "state").then(|| value.into_owned()))
            .ok_or_else(|| {
                McpError::Authorization("authorization URL did not contain state".into())
            })?;
        let mut pending = self.inner.pending.lock().await;
        pending.retain(|_, value| value.expires_at > Instant::now());
        pending.retain(|_, value| value.server_id != server_id);
        pending.insert(
            csrf,
            PendingAuthorization {
                server_id: server_id.to_string(),
                state,
                expires_at: Instant::now() + AUTHORIZATION_TTL,
            },
        );
        Ok(authorization_url)
    }

    pub async fn complete_authorization(&self, callback_url: &str) -> Result<String, McpError> {
        let callback =
            Url::parse(callback_url).map_err(|error| McpError::Authorization(error.to_string()))?;
        let csrf = callback
            .query_pairs()
            .find_map(|(name, value)| (name == "state").then(|| value.into_owned()))
            .ok_or_else(|| McpError::Authorization("OAuth callback omitted state".into()))?;
        let server_id = self
            .inner
            .pending
            .lock()
            .await
            .get(&csrf)
            .map(|authorization| authorization.server_id.clone())
            .ok_or_else(|| McpError::Authorization("OAuth state is unknown or expired".into()))?;
        let lifecycle_gate = self.lifecycle_gate(&server_id);
        let _lifecycle = lifecycle_gate.lock().await;
        let mut authorization = self
            .inner
            .pending
            .lock()
            .await
            .remove(&csrf)
            .ok_or_else(|| McpError::Authorization("OAuth state is unknown or expired".into()))?;
        if authorization.expires_at <= Instant::now() {
            return Err(McpError::Authorization("OAuth state expired".into()));
        }
        authorization
            .state
            .handle_callback_url(callback_url)
            .await
            .map_err(auth_error)?;
        let OAuthState::Authorized(manager) = authorization.state else {
            return Err(McpError::Authorization(
                "OAuth callback did not complete authorization".into(),
            ));
        };
        let profile = self.resolve(&authorization.server_id)?;
        self.inner.authorized_clients.lock().await.insert(
            authorization.server_id.clone(),
            AuthClient::new(mcp_http_client(profile)?, manager),
        );
        Ok(authorization.server_id)
    }

    pub async fn clear_authorization(&self, server_id: &str) -> Result<(), McpError> {
        let profile = self.resolve(server_id)?;
        if profile.spec.auth != McpAuth::Oauth {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` does not use OAuth"
            )));
        }
        let lifecycle_gate = self.lifecycle_gate(server_id);
        let _lifecycle = lifecycle_gate.lock().await;
        if self.active_sessions(server_id).load(Ordering::Acquire) != 0 {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` authorization cannot be cleared while sessions are active"
            )));
        }
        self.store(profile).clear().await.map_err(auth_error)?;
        self.inner.authorized_clients.lock().await.remove(server_id);
        self.inner
            .pending
            .lock()
            .await
            .retain(|_, pending| pending.server_id != server_id);
        Ok(())
    }

    pub async fn cancel_pending_authorization(&self, server_id: &str) -> Result<(), McpError> {
        let profile = self.resolve(server_id)?;
        if profile.spec.auth != McpAuth::Oauth {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` does not use OAuth"
            )));
        }
        let lifecycle_gate = self.lifecycle_gate(server_id);
        let _lifecycle = lifecycle_gate.lock().await;
        self.inner
            .pending
            .lock()
            .await
            .retain(|_, pending| pending.server_id != server_id);
        Ok(())
    }

    async fn authorization_manager(&self, profile: &McpProfile) -> Result<OAuthState, McpError> {
        let oauth_client = Arc::new(AllowedOAuthHttpClient::new(profile)?);
        let mut manager = AuthorizationManager::new_with_oauth_http_client(
            profile
                .url
                .as_ref()
                .expect("OAuth profile has a validated URL")
                .clone(),
            oauth_client,
        )
        .await
        .map_err(auth_error)?;
        manager.set_credential_store(self.store(profile));
        if manager.initialize_from_store().await.map_err(auth_error)? {
            Ok(OAuthState::Authorized(manager))
        } else {
            Ok(OAuthState::Unauthorized(manager))
        }
    }

    fn store(&self, profile: &McpProfile) -> ProfileCredentialStore {
        self.inner
            .stores
            .get(profile.id())
            .expect("profile credential store exists")
            .clone()
    }

    pub async fn connect(
        &self,
        server_id: &str,
        access: &McpAccess,
        cancellation: CancellationToken,
    ) -> Result<McpSession, McpError> {
        let profile = self.resolve(server_id)?.clone();
        access.validate(&profile)?;
        let lifecycle_gate = self.lifecycle_gate(server_id);
        let _lifecycle = tokio::select! {
            _ = cancellation.cancelled() => return Err(McpError::Canceled),
            lifecycle = lifecycle_gate.lock() => lifecycle,
        };
        let active_sessions = self.active_sessions(server_id);
        active_sessions.fetch_add(1, Ordering::AcqRel);
        drop(_lifecycle);
        let session_cancellation = cancellation.child_token();
        let cancellation_wait = session_cancellation.clone();
        let connection = async {
            match profile.spec.transport {
                McpTransport::StreamableHttp => {
                    self.connect_http(profile, session_cancellation).await
                }
                McpTransport::Stdio => {
                    self.connect_stdio(profile, access, session_cancellation)
                        .await
                }
            }
        };
        let session_result = tokio::select! {
            _ = cancellation_wait.cancelled() => Err(McpError::Canceled),
            result = connection => result,
        };
        let mut session = match session_result {
            Ok(session) => session,
            Err(error) => {
                active_sessions.fetch_sub(1, Ordering::AcqRel);
                return Err(error);
            }
        };
        session.active_sessions = Some(active_sessions);
        Ok(session)
    }

    fn active_sessions(&self, server_id: &str) -> Arc<AtomicUsize> {
        self.inner
            .active_sessions
            .get(server_id)
            .expect("profile active session counter exists")
            .clone()
    }

    fn lifecycle_gate(&self, server_id: &str) -> Arc<Mutex<()>> {
        self.inner
            .lifecycle_gates
            .get(server_id)
            .expect("profile lifecycle gate exists")
            .clone()
    }

    async fn connect_http(
        &self,
        profile: McpProfile,
        cancellation: CancellationToken,
    ) -> Result<McpSession, McpError> {
        let mut config = StreamableHttpClientTransportConfig::with_uri(
            profile
                .url
                .as_ref()
                .expect("HTTP profile has validated URL")
                .as_str(),
        )
        .max_sse_event_size(profile.spec.max_response_bytes);
        config.allow_stateless = true;
        let mut headers = HashMap::new();
        for (name, value) in &profile.spec.headers {
            headers.insert(
                http::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|error| McpError::Configuration(error.to_string()))?,
                http::HeaderValue::from_str(value)
                    .map_err(|error| McpError::Configuration(error.to_string()))?,
            );
        }
        let credential_guard = match profile.spec.auth {
            McpAuth::None => CredentialGuard::None,
            McpAuth::Bearer => {
                let credential = required_env(
                    profile
                        .spec
                        .credential_env
                        .as_deref()
                        .expect("validated credential env"),
                )?;
                config = config.auth_header(credential.clone());
                CredentialGuard::Static(vec![credential])
            }
            McpAuth::Header => {
                let credential = required_env(
                    profile
                        .spec
                        .credential_env
                        .as_deref()
                        .expect("validated credential env"),
                )?;
                headers.insert(
                    http::HeaderName::from_bytes(
                        profile
                            .spec
                            .auth_header
                            .as_deref()
                            .expect("validated auth header")
                            .as_bytes(),
                    )
                    .map_err(|error| McpError::Configuration(error.to_string()))?,
                    http::HeaderValue::from_str(&format!(
                        "{}{credential}",
                        profile.spec.auth_prefix
                    ))
                    .map_err(|error| McpError::Configuration(error.to_string()))?,
                );
                CredentialGuard::Static(vec![credential])
            }
            McpAuth::Oauth => {
                let auth_client = self.authorized_client(&profile).await?;
                config = config.custom_headers(headers);
                let credential_guard = CredentialGuard::OAuth(auth_client.clone());
                let transport = StreamableHttpClientTransport::with_client(auth_client, config);
                return McpSession::serve(profile, transport, cancellation, credential_guard).await;
            }
        };
        config = config.custom_headers(headers);
        let transport =
            StreamableHttpClientTransport::with_client(mcp_http_client(&profile)?, config);
        McpSession::serve(profile, transport, cancellation, credential_guard).await
    }

    async fn authorized_client(
        &self,
        profile: &McpProfile,
    ) -> Result<AuthClient<BoundedHttpClient>, McpError> {
        if let Some(client) = self
            .inner
            .authorized_clients
            .lock()
            .await
            .get(profile.id())
            .cloned()
        {
            return Ok(client.clone());
        }
        let lifecycle_gate = self.lifecycle_gate(profile.id());
        let _lifecycle = lifecycle_gate.lock().await;
        if let Some(client) = self
            .inner
            .authorized_clients
            .lock()
            .await
            .get(profile.id())
            .cloned()
        {
            return Ok(client);
        }
        let state = self.authorization_manager(profile).await?;
        let OAuthState::Authorized(manager) = state else {
            return Err(McpError::AuthorizationRequired {
                server: profile.id().to_string(),
            });
        };
        let client = AuthClient::new(mcp_http_client(profile)?, manager);
        self.inner
            .authorized_clients
            .lock()
            .await
            .insert(profile.id().to_string(), client.clone());
        Ok(client)
    }

    async fn connect_stdio(
        &self,
        profile: McpProfile,
        access: &McpAccess,
        cancellation: CancellationToken,
    ) -> Result<McpSession, McpError> {
        let permission = access
            .commands
            .iter()
            .find(|permission| permission.argv == profile.spec.command)
            .expect("validated stdio command permission");
        let (bin, args) = profile
            .spec
            .command
            .split_first()
            .expect("validated stdio command");
        let mut command = match permission.isolation {
            McpCommandIsolation::TrustedHost => {
                let mut command = tokio::process::Command::new(bin);
                command.args(args);
                command
            }
            McpCommandIsolation::Container => {
                let (runtime, runtime_args) = permission
                    .runtime
                    .as_ref()
                    .and_then(mcp_container_runtime_command)
                    .ok_or_else(|| {
                        McpError::Configuration(format!(
                            "MCP server `{}` requires its declared container runtime",
                            profile.id()
                        ))
                    })?;
                let image = permission.image.as_deref().ok_or_else(|| {
                    McpError::Configuration(format!(
                        "MCP server `{}` container command has no image",
                        profile.id()
                    ))
                })?;
                let mount = format!(
                    "type=bind,src={},dst=/work",
                    access.workspace.to_string_lossy()
                );
                let mut command = tokio::process::Command::new(runtime);
                command.args(runtime_args);
                command.args([
                    "--rm",
                    "--network",
                    "none",
                    "--read-only",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges",
                    "--pids-limit",
                    "256",
                    "--mount",
                    &mount,
                    "--workdir",
                    "/work",
                ]);
                for name in profile.spec.env.keys().chain(profile.spec.env_from.keys()) {
                    command.args(["--env", name]);
                }
                command.arg(image).arg(bin).args(args);
                command
            }
        };
        command
            .current_dir(&access.workspace)
            .env_clear()
            .kill_on_drop(true);
        if let Ok(path) = std::env::var("PATH") {
            command.env("PATH", path);
        }
        for (name, value) in &profile.spec.env {
            command.env(name, value);
        }
        let mut sensitive_values = Vec::new();
        for (target, source) in &profile.spec.env_from {
            let value = required_env(source)?;
            command.env(target, &value);
            sensitive_values.push(value);
        }
        let transport = BoundedChildTransport::spawn(command, profile.spec.max_response_bytes)
            .map_err(|error| McpError::Transport(error.to_string()))?;
        McpSession::serve(
            profile,
            transport,
            cancellation,
            CredentialGuard::Static(sensitive_values),
        )
        .await
    }
}

fn public_default_specs() -> Vec<McpServerSpec> {
    [
        ("exa-public", "https://mcp.exa.ai/mcp", "mcp.exa.ai"),
        (
            "parallel-public",
            "https://search.parallel.ai/mcp",
            "search.parallel.ai",
        ),
    ]
    .into_iter()
    .map(|(id, url, host)| McpServerSpec {
        id: id.to_string(),
        transport: McpTransport::StreamableHttp,
        lifecycle: McpLifecycle::Initialize,
        url: Some(url.to_string()),
        command: Vec::new(),
        env: BTreeMap::new(),
        env_from: BTreeMap::new(),
        headers: BTreeMap::new(),
        auth: McpAuth::None,
        credential_env: None,
        auth_header: None,
        auth_prefix: String::new(),
        oauth_scopes: Vec::new(),
        oauth_client_id_env: None,
        oauth_client_secret_env: None,
        oauth_store: OAuthCredentialStore::Memory,
        allowed_hosts: vec![host.to_string()],
        timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
        max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
    })
    .collect()
}

#[derive(Debug, Clone)]
pub struct McpAccess {
    pub network_hosts: BTreeSet<String>,
    pub commands: Vec<McpCommandAccess>,
    pub workspace: std::path::PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpCommandAccess {
    pub argv: Vec<String>,
    pub isolation: McpCommandIsolation,
    pub image: Option<String>,
    pub runtime: Option<McpContainerRuntime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpCommandIsolation {
    Container,
    TrustedHost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpContainerRuntime {
    Docker,
    Podman,
    DockerRunsc,
}

impl McpCommandAccess {
    pub fn trusted_host(argv: Vec<String>) -> Self {
        Self {
            argv,
            isolation: McpCommandIsolation::TrustedHost,
            image: None,
            runtime: None,
        }
    }
}

impl McpAccess {
    fn validate(&self, profile: &McpProfile) -> Result<(), McpError> {
        match profile.transport() {
            McpTransport::StreamableHttp => {
                for host in profile.allowed_hosts() {
                    if !self.network_hosts.contains(host) {
                        return Err(McpError::Configuration(format!(
                            "MCP server `{}` requires permissions.network entry `{host}`",
                            profile.id()
                        )));
                    }
                }
            }
            McpTransport::Stdio => {
                if !self
                    .commands
                    .iter()
                    .any(|allowed| allowed.argv == profile.command())
                {
                    return Err(McpError::Configuration(format!(
                        "MCP server `{}` command is not allowed by permissions.commands",
                        profile.id()
                    )));
                }
            }
        }
        Ok(())
    }
}

fn mcp_container_runtime_command(
    runtime: &McpContainerRuntime,
) -> Option<(&'static str, Vec<&'static str>)> {
    let path = std::env::var_os("PATH")?;
    let (binary, args) = match runtime {
        McpContainerRuntime::Docker => ("docker", vec!["run"]),
        McpContainerRuntime::Podman => ("podman", vec!["run"]),
        McpContainerRuntime::DockerRunsc => ("docker", vec!["run", "--runtime", "runsc"]),
    };
    std::env::split_paths(&path)
        .any(|directory| directory.join(binary).is_file())
        .then_some((binary, args))
}

pub struct McpSession {
    profile: McpProfile,
    client: Option<RunningService<RoleClient, QcgMcpClient>>,
    cancellation: CancellationToken,
    credential_guard: CredentialGuard,
    active_sessions: Option<Arc<AtomicUsize>>,
}

impl McpSession {
    async fn serve<T, E, A>(
        profile: McpProfile,
        transport: T,
        cancellation: CancellationToken,
        credential_guard: CredentialGuard,
    ) -> Result<Self, McpError>
    where
        T: rmcp::transport::IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let seconds = profile.spec.timeout_seconds;
        let sensitive_values =
            bounded_credential_values(&credential_guard, &cancellation, seconds).await?;
        let client = tokio::select! {
            _ = cancellation.cancelled() => return Err(McpError::Canceled),
            result = tokio::time::timeout(
                Duration::from_secs(seconds),
                QcgMcpClient.serve_with_lifecycle(
                    transport,
                    match profile.spec.lifecycle {
                        McpLifecycle::Initialize => ClientLifecycleMode::Initialize,
                        McpLifecycle::Discover => ClientLifecycleMode::Discover {
                            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                        },
                    },
                ),
            ) => {
                result
                    .map_err(|_| McpError::TimedOut { seconds })?
                    .map_err(|error| guarded_transport_error(error, &sensitive_values))?
            }
        };
        Ok(Self {
            profile,
            client: Some(client),
            cancellation,
            credential_guard,
            active_sessions: None,
        })
    }

    async fn sensitive_values(&self) -> Result<Vec<String>, McpError> {
        bounded_credential_values(
            &self.credential_guard,
            &self.cancellation,
            self.profile.spec.timeout_seconds,
        )
        .await
    }

    async fn sensitive_values_for_close(&self) -> Result<Vec<String>, McpError> {
        let seconds = self.profile.spec.timeout_seconds;
        tokio::time::timeout(Duration::from_secs(seconds), self.credential_guard.values())
            .await
            .map_err(|_| McpError::TimedOut { seconds })?
    }

    pub fn server_id(&self) -> &str {
        self.profile.id()
    }

    pub fn protocol_version(&self) -> Option<String> {
        self.client
            .as_ref()
            .and_then(|client| client.peer().peer_info())
            .map(|info| info.protocol_version.as_str().to_string())
    }

    pub async fn list_tools(&self) -> Result<Vec<McpTool>, McpError> {
        let seconds = self.profile.spec.timeout_seconds;
        let mut sensitive_values = self.sensitive_values().await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut tools = Vec::new();
        for _ in 0..MAX_TOOL_LIST_PAGES {
            let result = tokio::select! {
                _ = self.cancellation.cancelled() => {
                    self.cancel_transport();
                    return Err(McpError::Canceled);
                },
                result = tokio::time::timeout_at(
                    deadline,
                    self.client.as_ref().expect("active MCP client").list_tools(Some(
                        PaginatedRequestParams::default().with_cursor(cursor.clone()),
                    )),
                ) => {
                    match result {
                        Ok(result) => result
                            .map_err(|error| guarded_transport_error(error, &sensitive_values))?,
                        Err(_) => {
                            self.cancel_transport();
                            return Err(McpError::TimedOut { seconds });
                        }
                    }
                }
            };
            sensitive_values = self.sensitive_values().await?;
            tools.extend(result.tools.into_iter().map(McpTool::from));
            let encoded_size = serde_json::to_vec(&tools)
                .map_err(|error| McpError::Transport(error.to_string()))?
                .len();
            reject_credential_reflection(&tools, &sensitive_values)?;
            if encoded_size > self.profile.spec.max_response_bytes {
                return Err(McpError::Transport(format!(
                    "MCP server `{}` tool list exceeded {} bytes",
                    self.profile.id(),
                    self.profile.spec.max_response_bytes
                )));
            }
            let Some(next) = result.next_cursor else {
                return Ok(tools);
            };
            if !seen_cursors.insert(next.clone()) {
                return Err(McpError::Transport(format!(
                    "MCP server `{}` repeated a tools/list cursor",
                    self.profile.id()
                )));
            }
            cursor = Some(next);
        }
        Err(McpError::Transport(format!(
            "MCP server `{}` tools/list exceeded {MAX_TOOL_LIST_PAGES} pages",
            self.profile.id()
        )))
    }

    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<Value, McpError> {
        match self
            .call_tool_with_input(name, arguments, None, None)
            .await?
        {
            McpCallOutcome::Complete(value) => Ok(value),
            McpCallOutcome::InputRequired(_) => Err(McpError::Transport(
                "MCP tool requires input; use the resumable call interface".into(),
            )),
        }
    }

    pub async fn call_tool_with_input(
        &self,
        name: &str,
        arguments: Value,
        input_responses: Option<InputResponses>,
        request_state: Option<String>,
    ) -> Result<McpCallOutcome, McpError> {
        let arguments = arguments.as_object().cloned().ok_or_else(|| {
            McpError::Configuration(format!("MCP tool `{name}` arguments must be a JSON object"))
        })?;
        let seconds = self.profile.spec.timeout_seconds;
        let mut sensitive_values = self.sensitive_values().await?;
        let mut params = CallToolRequestParams::new(name.to_string()).with_arguments(arguments);
        params.input_responses = input_responses;
        params.request_state = request_state;
        let result = match tokio::time::timeout(
            Duration::from_secs(seconds),
            self.call_tool_with_tasks(params, &sensitive_values),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                self.cancel_transport();
                return Err(McpError::TimedOut { seconds });
            }
        };
        sensitive_values = self.sensitive_values().await?;
        let result = match result {
            ToolCallResult::Complete(result) => result,
            ToolCallResult::InputRequired(input) => {
                return Ok(McpCallOutcome::InputRequired(input));
            }
        };
        let value = serde_json::to_value(&result)
            .map_err(|error| McpError::Transport(error.to_string()))?;
        let encoded =
            serde_json::to_vec(&value).map_err(|error| McpError::Transport(error.to_string()))?;
        if encoded.len() > self.profile.spec.max_response_bytes {
            return Err(McpError::Transport(format!(
                "MCP server `{}` tool result exceeded {} bytes",
                self.profile.id(),
                self.profile.spec.max_response_bytes
            )));
        }
        reject_credential_reflection(&value, &sensitive_values)?;
        if result.is_error == Some(true) {
            return Err(McpError::ToolFailed {
                tool: name.to_string(),
                result: value,
            });
        }
        Ok(McpCallOutcome::Complete(value))
    }

    async fn call_tool_with_tasks(
        &self,
        params: CallToolRequestParams,
        sensitive_values: &[String],
    ) -> Result<ToolCallResult, McpError> {
        let client = self.client.as_ref().expect("active MCP client");
        let initial = tokio::select! {
            _ = self.cancellation.cancelled() => {
                self.cancel_transport();
                return Err(McpError::Canceled);
            }
            result = client.peer().call_tool_once(params) => {
                result.map_err(|error| guarded_transport_error(error, sensitive_values))?
            }
        };
        let task = match initial {
            CallToolResponse::Complete(result) => return Ok(ToolCallResult::Complete(result)),
            CallToolResponse::InputRequired(result) => {
                let input_requests = result
                    .input_requests
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(id, request)| {
                        serde_json::to_value(request)
                            .map(|request| (id, request))
                            .map_err(|error| McpError::Transport(error.to_string()))
                    })
                    .collect::<Result<_, _>>()?;
                return Ok(ToolCallResult::InputRequired(McpInputRequired {
                    input_requests,
                    request_state: result.request_state,
                }));
            }
            CallToolResponse::Task(task) => task.task,
            _ => {
                return Err(McpError::Transport(
                    "MCP tool returned an unsupported asynchronous response".into(),
                ));
            }
        };
        let task_id = task.task_id;
        let mut poll_interval = task.poll_interval_ms.unwrap_or(250).clamp(50, 5_000);
        loop {
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    let _ = client
                        .peer()
                        .cancel_task(CancelTaskParams::new(task_id.clone()))
                        .await;
                    return Err(McpError::Canceled);
                }
                _ = tokio::time::sleep(Duration::from_millis(poll_interval)) => {}
            }
            let detailed = client
                .peer()
                .get_task(GetTaskParams::new(task_id.clone()))
                .await
                .map_err(|error| guarded_transport_error(error, sensitive_values))?
                .task;
            poll_interval = detailed
                .task
                .poll_interval_ms
                .unwrap_or(poll_interval)
                .clamp(50, 5_000);
            match detailed.payload {
                TaskPayload::Working => {}
                TaskPayload::Completed { result } => {
                    return serde_json::from_value(Value::Object(result))
                        .map(ToolCallResult::Complete)
                        .map_err(|_| {
                            McpError::Transport(
                                "MCP task completed with an invalid tool result".into(),
                            )
                        });
                }
                TaskPayload::InputRequired { .. } => {
                    return Err(McpError::Transport(
                        "MCP task requested interactive input outside the qcg HITL boundary".into(),
                    ));
                }
                TaskPayload::Failed { error } => {
                    let error = Value::Object(error);
                    reject_credential_reflection(&error, sensitive_values)?;
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("MCP task failed");
                    return Err(McpError::ToolFailed {
                        tool: "task".into(),
                        result: serde_json::json!({
                            "content": [{ "type": "text", "text": message }],
                            "isError": true,
                            "_meta": { "qcg": { "taskError": error } }
                        }),
                    });
                }
                TaskPayload::Cancelled => return Err(McpError::Canceled),
                _ => {
                    return Err(McpError::Transport(
                        "MCP task returned an unsupported status payload".into(),
                    ));
                }
            }
        }
    }

    fn cancel_transport(&self) {
        if let Some(client) = self.client.as_ref() {
            client.cancellation_token().cancel();
        }
    }

    pub async fn close(mut self) -> Result<(), McpError> {
        let sensitive_values = self.sensitive_values_for_close().await?;
        let result = self
            .client
            .take()
            .expect("active MCP client")
            .close_with_timeout(Duration::from_secs(5))
            .await
            .map_err(|error| guarded_transport_error(error, &sensitive_values))?;
        if result.is_none() {
            return Err(McpError::TimedOut { seconds: 5 });
        }
        Ok(())
    }
}

enum ToolCallResult {
    Complete(CallToolResult),
    InputRequired(McpInputRequired),
}

async fn bounded_credential_values(
    guard: &CredentialGuard,
    cancellation: &CancellationToken,
    seconds: u64,
) -> Result<Vec<String>, McpError> {
    tokio::select! {
        _ = cancellation.cancelled() => Err(McpError::Canceled),
        result = tokio::time::timeout(Duration::from_secs(seconds), guard.values()) => {
            result.map_err(|_| McpError::TimedOut { seconds })?
        }
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        self.cancellation.cancel();
        drop(self.client.take());
        if let Some(active_sessions) = self.active_sessions.take() {
            active_sessions.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct McpTool {
    pub name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub input_schema: Value,
    pub output_schema: Option<Value>,
}

impl From<Tool> for McpTool {
    fn from(tool: Tool) -> Self {
        Self {
            name: tool.name.into_owned(),
            title: tool.title,
            description: tool.description.map(|value| value.into_owned()),
            input_schema: Value::Object((*tool.input_schema).clone()),
            output_schema: tool
                .output_schema
                .map(|value| Value::Object((*value).clone())),
        }
    }
}

fn mcp_http_client(profile: &McpProfile) -> Result<BoundedHttpClient, McpError> {
    BoundedHttpClient::new(
        profile.spec.timeout_seconds,
        profile.spec.max_response_bytes,
    )
    .map_err(|error| McpError::Configuration(error.to_string()))
}

fn required_env(name: &str) -> Result<String, McpError> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        Ok(_) | Err(std::env::VarError::NotPresent) => Err(McpError::Configuration(format!(
            "set `{name}` before using the MCP server"
        ))),
        Err(std::env::VarError::NotUnicode(_)) => Err(McpError::Configuration(format!(
            "environment variable `{name}` is not valid UTF-8"
        ))),
    }
}

fn auth_error(_error: AuthError) -> McpError {
    // OAuth and credential-store errors may contain token response bodies or
    // platform-specific secret-store details. Keep the public error stable and
    // deliberately omit the provider-supplied message.
    McpError::Authorization("authorization protocol operation failed".into())
}

fn guarded_transport_error(error: impl std::fmt::Display, sensitive_values: &[String]) -> McpError {
    let message = error.to_string();
    if contains_sensitive_value(&message, sensitive_values) {
        McpError::Transport("MCP server reflected credential material in an error".into())
    } else {
        McpError::Transport(message)
    }
}

fn reject_credential_reflection(
    value: &impl Serialize,
    sensitive_values: &[String],
) -> Result<(), McpError> {
    if sensitive_values.is_empty() {
        return Ok(());
    }
    let encoded =
        serde_json::to_string(value).map_err(|error| McpError::Transport(error.to_string()))?;
    if contains_sensitive_value(&encoded, sensitive_values) {
        return Err(McpError::Transport(
            "MCP server reflected credential material in a response".into(),
        ));
    }
    Ok(())
}

fn contains_sensitive_value(text: &str, sensitive_values: &[String]) -> bool {
    sensitive_values
        .iter()
        .filter(|value| !value.is_empty())
        .any(|value| text.contains(value))
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn valid_env_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_uppercase() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn validate_oauth_operation_url(
    url: &Url,
) -> Result<(), rmcp::transport::auth::OAuthHttpClientError> {
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err("OAuth request URL must not contain credentials or a fragment".into());
    }
    Ok(())
}

fn dangerous_process_env_name(value: &str) -> bool {
    matches!(
        value,
        "PATH"
            | "LD_PRELOAD"
            | "LD_LIBRARY_PATH"
            | "DYLD_INSERT_LIBRARIES"
            | "DYLD_LIBRARY_PATH"
            | "NODE_OPTIONS"
            | "PYTHONPATH"
            | "PYTHONHOME"
            | "RUBYOPT"
            | "PERL5OPT"
    )
}

pub(crate) fn reserved_transport_header(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "accept" | "authorization" | "content-type" | "mcp-session-id" | "last-event-id"
    )
}

fn validate_remote_url(id: &str, raw: &str) -> Result<Url, String> {
    let url =
        Url::parse(raw).map_err(|error| format!("MCP server `{id}` has invalid url: {error}"))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        return Err(format!(
            "MCP server `{id}` url must be HTTP(S) without credentials, query, or fragment"
        ));
    }
    if url.scheme() != "https" && !is_loopback(url.host_str().expect("validated host")) {
        return Err(format!("MCP server `{id}` remote url must use HTTPS"));
    }
    Ok(url)
}

fn is_secure_remote_url(url: &Url) -> bool {
    url.scheme() == "https" || (url.scheme() == "http" && url.host_str().is_some_and(is_loopback))
}

fn validate_redirect_uri(raw: &str) -> Result<Url, McpError> {
    let url = Url::parse(raw).map_err(|error| McpError::Configuration(error.to_string()))?;
    let host = url
        .host_str()
        .ok_or_else(|| McpError::Configuration("OAuth redirect URI must contain a host".into()))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || (url.scheme() != "https" && !(url.scheme() == "http" && is_loopback(host)))
    {
        return Err(McpError::Configuration(
            "OAuth redirect URI must use HTTPS, or HTTP on a loopback host, without credentials or a fragment"
                .into(),
        ));
    }
    Ok(url)
}

fn validate_host(host: &str) -> Result<(), String> {
    if host.is_empty()
        || host.contains(['/', ':', '@', '?', '#'])
        || Url::parse(&format!("https://{host}"))
            .ok()
            .and_then(|url| url.host_str().map(str::to_string))
            .as_deref()
            != Some(host)
    {
        return Err(format!("`{host}` is not a canonical host name"));
    }
    Ok(())
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote_spec() -> McpServerSpec {
        McpServerSpec {
            id: "tinyfish".into(),
            transport: McpTransport::StreamableHttp,
            lifecycle: McpLifecycle::Initialize,
            url: Some("https://agent.tinyfish.ai/mcp".into()),
            command: vec![],
            env: BTreeMap::new(),
            env_from: BTreeMap::new(),
            headers: BTreeMap::new(),
            auth: McpAuth::Oauth,
            credential_env: None,
            auth_header: None,
            auth_prefix: String::new(),
            oauth_scopes: vec![],
            oauth_client_id_env: None,
            oauth_client_secret_env: None,
            oauth_store: OAuthCredentialStore::Memory,
            allowed_hosts: vec!["agent.tinyfish.ai".into()],
            timeout_seconds: 30,
            max_response_bytes: 1024,
        }
    }

    #[test]
    fn validates_remote_oauth_profile() {
        remote_spec().validate().expect("profile should be valid");
    }

    #[test]
    fn rejects_transport_limits_above_hard_ceiling() {
        let mut timeout = remote_spec();
        timeout.timeout_seconds = MAX_TIMEOUT_SECONDS + 1;
        assert!(
            timeout
                .validate()
                .expect_err("excessive timeout must fail")
                .contains("timeout_seconds")
        );

        let mut response = remote_spec();
        response.max_response_bytes = MAX_RESPONSE_BYTES + 1;
        assert!(
            response
                .validate()
                .expect_err("excessive response limit must fail")
                .contains("max_response_bytes")
        );
    }

    #[test]
    fn public_defaults_are_anonymous_and_pinned_to_exact_hosts() {
        let runtime = McpRuntime::public_defaults();
        assert_eq!(runtime.server_ids(), ["exa-public", "parallel-public"]);
        for (id, url, host) in [
            ("exa-public", "https://mcp.exa.ai/mcp", "mcp.exa.ai"),
            (
                "parallel-public",
                "https://search.parallel.ai/mcp",
                "search.parallel.ai",
            ),
        ] {
            let profile = runtime.resolve(id).expect("public profile should resolve");
            assert_eq!(profile.spec.auth, McpAuth::None);
            assert_eq!(profile.spec.lifecycle, McpLifecycle::Initialize);
            assert_eq!(profile.spec.url.as_deref(), Some(url));
            assert_eq!(profile.spec.allowed_hosts, [host]);
            assert!(profile.spec.credential_env.is_none());
        }
    }

    #[test]
    fn public_default_ids_cannot_be_overridden() {
        let mut spec = remote_spec();
        spec.id = "exa-public".into();
        let error = McpRuntime::from_specs_with_public_defaults(vec![spec])
            .expect_err("built-in public profile ids must be reserved");
        assert!(error.contains("reserved"), "{error}");
    }

    #[test]
    fn remote_profile_requires_all_endpoint_hosts() {
        let mut spec = remote_spec();
        spec.allowed_hosts.clear();
        let error = spec.validate().expect_err("endpoint host must be allowed");
        assert!(error.contains("allowed_hosts"));
    }

    #[test]
    fn remote_profile_rejects_embedded_credentials() {
        let mut spec = remote_spec();
        spec.url = Some("https://secret@agent.tinyfish.ai/mcp".into());
        assert!(spec.validate().is_err());
    }

    #[test]
    fn profile_debug_and_static_fields_do_not_expose_credentials() {
        let mut spec = remote_spec();
        spec.auth = McpAuth::Header;
        spec.credential_env = Some("QCG_MCP_TOKEN".into());
        spec.auth_header = Some("X-Access-Token".into());
        spec.auth_prefix = "secret-prefix ".into();
        spec.oauth_store = OAuthCredentialStore::Keyring;
        let debug = format!("{spec:?}");
        assert!(!debug.contains("secret-prefix"));
        assert!(debug.contains("<redacted>"));

        let mut static_secret = remote_spec();
        static_secret.auth = McpAuth::None;
        static_secret
            .headers
            .insert("X-API-Key".into(), "secret".into());
        let error = static_secret
            .validate()
            .expect_err("credential-like static headers must be rejected");
        assert!(error.contains("must not contain credentials"));
    }

    #[test]
    fn profile_rejects_transport_headers_and_process_control_environment() {
        for name in [
            "Accept",
            "Authorization",
            "Content-Type",
            "Mcp-Session-Id",
            "Last-Event-Id",
        ] {
            let mut spec = remote_spec();
            spec.headers.insert(name.into(), "configured".into());
            let error = spec
                .validate()
                .expect_err("transport-owned headers must be rejected");
            assert!(error.contains("must not override"), "{name}: {error}");
        }

        let mut spec = remote_spec();
        spec.auth = McpAuth::None;
        spec.headers
            .insert("X-Service-Credential".into(), "configured".into());
        assert!(spec.validate().is_err());

        let mut spec = stdio_spec();
        spec.env
            .insert("NODE_OPTIONS".into(), "--require payload".into());
        assert!(spec.validate().is_err());

        let mut spec = stdio_spec();
        spec.env_from
            .insert("LD_PRELOAD".into(), "QCG_LIBRARY_PATH".into());
        assert!(spec.validate().is_err());
    }

    #[tokio::test]
    async fn authorization_status_does_not_perform_network_discovery() {
        let mut spec = remote_spec();
        spec.url = Some("https://127.0.0.1:1/mcp".into());
        spec.allowed_hosts = vec!["127.0.0.1".into()];
        let runtime = McpRuntime::from_specs(vec![spec]).expect("runtime should load");
        let authorized = tokio::time::timeout(
            Duration::from_millis(100),
            runtime.is_authorized("tinyfish"),
        )
        .await
        .expect("status lookup must remain local")
        .expect("status lookup should succeed");
        assert!(!authorized);
    }

    #[test]
    fn oauth_redirect_requires_https_or_loopback_http() {
        validate_redirect_uri("http://127.0.0.1:43123/callback")
            .expect("loopback redirect should be valid");
        assert!(validate_redirect_uri("http://example.com/callback").is_err());
        assert!(validate_redirect_uri("https://user@example.com/callback").is_err());
        assert!(is_secure_remote_url(
            &Url::parse("https://oauth.example.test/token").expect("valid URL")
        ));
        assert!(is_secure_remote_url(
            &Url::parse("http://localhost:43123/token").expect("valid URL")
        ));
        assert!(!is_secure_remote_url(
            &Url::parse("http://oauth.example.test/token").expect("valid URL")
        ));
    }

    #[test]
    fn reflected_credentials_are_rejected_without_echoing_them() {
        let credential = "mcp-test-secret-value".to_string();
        let error = reject_credential_reflection(
            &serde_json::json!({ "content": credential }),
            &["mcp-test-secret-value".into()],
        )
        .expect_err("credential reflection must fail closed");
        let message = error.to_string();
        assert!(message.contains("reflected credential material"));
        assert!(!message.contains("mcp-test-secret-value"));
    }

    #[tokio::test]
    async fn authorization_cannot_be_cleared_while_sessions_are_active() {
        let runtime = McpRuntime::from_specs(vec![remote_spec()]).expect("runtime should load");
        runtime
            .active_sessions("tinyfish")
            .store(1, Ordering::Release);
        let error = runtime
            .clear_authorization("tinyfish")
            .await
            .expect_err("active session must block credential removal");
        assert!(error.to_string().contains("sessions are active"));
    }

    #[test]
    fn stdio_profile_requires_explicit_command_permission() {
        let spec = stdio_spec();
        spec.validate().expect("stdio profile should be valid");
        let runtime = McpRuntime::from_specs(vec![spec]).expect("runtime should load");
        let profile = runtime.resolve("local").expect("profile should resolve");
        let access = McpAccess {
            network_hosts: BTreeSet::new(),
            commands: vec![McpCommandAccess::trusted_host(vec!["other".into()])],
            workspace: std::env::temp_dir(),
        };
        assert!(access.validate(profile).is_err());
    }

    fn stdio_spec() -> McpServerSpec {
        McpServerSpec {
            id: "local".into(),
            transport: McpTransport::Stdio,
            lifecycle: McpLifecycle::Initialize,
            url: None,
            command: vec!["demo-server".into(), "--stdio".into()],
            env: BTreeMap::new(),
            env_from: BTreeMap::new(),
            headers: BTreeMap::new(),
            auth: McpAuth::None,
            credential_env: None,
            auth_header: None,
            auth_prefix: String::new(),
            oauth_scopes: vec![],
            oauth_client_id_env: None,
            oauth_client_secret_env: None,
            oauth_store: OAuthCredentialStore::Memory,
            allowed_hosts: vec![],
            timeout_seconds: 30,
            max_response_bytes: 1024,
        }
    }
}
