use crate::bounded_http::BoundedHttpClient;
use crate::bounded_stdio::BoundedChildTransport;
use rmcp::ClientHandler;
use rmcp::model::{ClientCapabilities, ClientInfo, Implementation, TASKS_EXTENSION_ID};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::auth::{
    AuthClient, AuthorizationManager, AuthorizationRequest, CredentialStore,
    InMemoryCredentialStore, OAuthState,
};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::access::{McpAccess, McpCommandIsolation, mcp_container_runtime_command};
use super::error::McpError;
use super::profile::{
    AllowedOAuthHttpClient, CredentialGuard, KeyringCredentialStore, McpProfile,
    PendingAuthorization, ProfileCredentialStore,
};
use super::session::{McpSession, mcp_http_client};
use super::spec::McpServerSpec;
use super::transport::{McpAuth, McpLifecycle, McpTransport, OAuthCredentialStore};
use super::validate::{
    auth_error, is_secure_remote_url, required_env, validate_redirect_uri, validate_remote_url,
};
use qcg_policy::{DEFAULT_MCP_MAX_RESPONSE_BYTES, DEFAULT_MCP_TIMEOUT_SECONDS};

const AUTHORIZATION_TTL: Duration = Duration::from_secs(10 * 60);

pub(crate) struct QcgMcpClient;

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

    pub(crate) fn active_sessions(&self, server_id: &str) -> Arc<AtomicUsize> {
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
        timeout_seconds: DEFAULT_MCP_TIMEOUT_SECONDS,
        max_response_bytes: DEFAULT_MCP_MAX_RESPONSE_BYTES,
    })
    .collect()
}
