#[cfg(feature = "mcp-oauth")]
use crate::bounded_http::BoundedHttpClient;
use crate::bounded_stdio::BoundedChildTransport;
use rmcp::ClientHandler;
use rmcp::model::{ClientCapabilities, ClientInfo, Implementation, TASKS_EXTENSION_ID};
use rmcp::transport::StreamableHttpClientTransport;
#[cfg(feature = "mcp-oauth")]
use rmcp::transport::auth::{
    AuthClient, AuthorizationManager, AuthorizationRequest, CredentialStore,
    InMemoryCredentialStore, OAuthState,
};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "mcp-oauth")]
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "mcp-oauth")]
use url::Url;

use super::access::{McpAccess, McpCommandIsolation, mcp_container_backend};
use super::error::McpError;
#[cfg(feature = "mcp-oauth")]
use super::profile::{
    AllowedOAuthHttpClient, KeyringCredentialStore, PendingAuthorization, ProfileCredentialStore,
};
use super::profile::{CredentialGuard, McpProfile};
use super::session::{McpSession, mcp_http_client};
use super::spec::McpServerSpec;
use super::transport::{McpAuth, McpLifecycle, McpTransport, OAuthCredentialStore};
#[cfg(feature = "mcp-oauth")]
use super::validate::{auth_error, is_secure_remote_url, validate_redirect_uri};
use super::validate::{required_env, validate_remote_url};
use qcg_policy::{DEFAULT_MCP_MAX_RESPONSE_BYTES, DEFAULT_MCP_TIMEOUT_SECONDS};

#[cfg(feature = "mcp-oauth")]
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
    #[cfg(feature = "mcp-oauth")]
    stores: BTreeMap<String, ProfileCredentialStore>,
    #[cfg(feature = "mcp-oauth")]
    authorized_clients: Mutex<HashMap<String, AuthClient<BoundedHttpClient>>>,
    #[cfg(feature = "mcp-oauth")]
    pending: Mutex<HashMap<String, PendingAuthorization>>,
    active_sessions: BTreeMap<String, Arc<AtomicUsize>>,
    lifecycle_gates: BTreeMap<String, Arc<Mutex<()>>>,
}

#[derive(Clone)]
pub struct McpRuntime {
    inner: Arc<McpRuntimeInner>,
}

// RAII reservation so a dropped or failed connect future still returns the
// active session count. Ownership moves to McpSession on success via disarm.
struct ActiveSessionReservation {
    counter: Option<Arc<AtomicUsize>>,
}

impl ActiveSessionReservation {
    fn acquire(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self {
            counter: Some(counter),
        }
    }

    fn disarm(mut self) -> Arc<AtomicUsize> {
        self.counter.take().expect("reservation holds a counter")
    }
}

impl Drop for ActiveSessionReservation {
    fn drop(&mut self) {
        if let Some(counter) = self.counter.take() {
            counter.fetch_sub(1, Ordering::AcqRel);
        }
    }
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
        #[cfg(feature = "mcp-oauth")]
        let mut stores = BTreeMap::new();
        let mut active_sessions = BTreeMap::new();
        let mut lifecycle_gates = BTreeMap::new();
        for spec in specs {
            spec.validate()?;
            let id = spec.id.clone();
            if profiles.contains_key(&id) {
                return Err(format!("duplicate MCP server id `{id}`"));
            }
            #[cfg(not(feature = "mcp-oauth"))]
            if spec.auth == McpAuth::Oauth {
                return Err(format!(
                    "MCP server `{id}` uses OAuth, which requires the `mcp-oauth` cargo feature"
                ));
            }
            let url = spec
                .url
                .as_deref()
                .map(|raw| validate_remote_url(&id, raw))
                .transpose()?;
            #[cfg(feature = "mcp-oauth")]
            let keyring_account = match spec.url.as_deref() {
                Some(url) => format!("{}@{url}", spec.id),
                None => spec.id.clone(),
            };
            #[cfg(feature = "mcp-oauth")]
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
            #[cfg(feature = "mcp-oauth")]
            stores.insert(id.clone(), store);
            active_sessions.insert(id.clone(), Arc::new(AtomicUsize::new(0)));
            lifecycle_gates.insert(id, Arc::new(Mutex::new(())));
        }
        Ok(Self {
            inner: Arc::new(McpRuntimeInner {
                profiles,
                #[cfg(feature = "mcp-oauth")]
                stores,
                #[cfg(feature = "mcp-oauth")]
                authorized_clients: Mutex::new(HashMap::new()),
                #[cfg(feature = "mcp-oauth")]
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
        self.oauth_authorization_status(profile, server_id).await
    }

    #[cfg(feature = "mcp-oauth")]
    async fn oauth_authorization_status(
        &self,
        profile: &McpProfile,
        server_id: &str,
    ) -> Result<bool, McpError> {
        if self
            .inner
            .authorized_clients
            .lock()
            .await
            .contains_key(server_id)
        {
            return Ok(true);
        }
        self.store(profile)?
            .load()
            .await
            .map(|credentials| credentials.is_some())
            .map_err(auth_error)
    }

    #[cfg(not(feature = "mcp-oauth"))]
    async fn oauth_authorization_status(
        &self,
        _profile: &McpProfile,
        server_id: &str,
    ) -> Result<bool, McpError> {
        Err(McpError::Configuration(format!(
            "MCP server `{server_id}` uses OAuth, which requires the `mcp-oauth` cargo feature"
        )))
    }

    pub async fn start_authorization(
        &self,
        server_id: &str,
        redirect_uri: &str,
    ) -> Result<String, McpError> {
        let profile = self.resolve(server_id)?;
        if profile.spec.auth != McpAuth::Oauth {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` does not use OAuth"
            )));
        }
        self.start_oauth_authorization(server_id, profile, redirect_uri)
            .await
    }

    #[cfg(feature = "mcp-oauth")]
    async fn start_oauth_authorization(
        &self,
        server_id: &str,
        profile: &McpProfile,
        redirect_uri: &str,
    ) -> Result<String, McpError> {
        let lifecycle_gate = self.lifecycle_gate(server_id)?;
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
        let mut state = self.authorization_manager(profile).await?;
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

    #[cfg(not(feature = "mcp-oauth"))]
    async fn start_oauth_authorization(
        &self,
        server_id: &str,
        _profile: &McpProfile,
        _redirect_uri: &str,
    ) -> Result<String, McpError> {
        Err(McpError::Configuration(format!(
            "MCP server `{server_id}` uses OAuth, which requires the `mcp-oauth` cargo feature"
        )))
    }

    pub async fn complete_authorization(&self, callback_url: &str) -> Result<String, McpError> {
        self.complete_oauth_authorization(callback_url).await
    }

    #[cfg(feature = "mcp-oauth")]
    async fn complete_oauth_authorization(&self, callback_url: &str) -> Result<String, McpError> {
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
        let lifecycle_gate = self.lifecycle_gate(&server_id)?;
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

    #[cfg(not(feature = "mcp-oauth"))]
    async fn complete_oauth_authorization(&self, _callback_url: &str) -> Result<String, McpError> {
        Err(McpError::Configuration(
            "OAuth authorization requires the `mcp-oauth` cargo feature".into(),
        ))
    }

    pub async fn clear_authorization(&self, server_id: &str) -> Result<(), McpError> {
        let profile = self.resolve(server_id)?;
        if profile.spec.auth != McpAuth::Oauth {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` does not use OAuth"
            )));
        }
        self.clear_oauth_authorization(server_id, profile).await
    }

    #[cfg(feature = "mcp-oauth")]
    async fn clear_oauth_authorization(
        &self,
        server_id: &str,
        profile: &McpProfile,
    ) -> Result<(), McpError> {
        let lifecycle_gate = self.lifecycle_gate(server_id)?;
        let _lifecycle = lifecycle_gate.lock().await;
        if self.active_sessions(server_id)?.load(Ordering::Acquire) != 0 {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` authorization cannot be cleared while sessions are active"
            )));
        }
        self.store(profile)?.clear().await.map_err(auth_error)?;
        self.inner.authorized_clients.lock().await.remove(server_id);
        self.inner
            .pending
            .lock()
            .await
            .retain(|_, pending| pending.server_id != server_id);
        Ok(())
    }

    #[cfg(not(feature = "mcp-oauth"))]
    async fn clear_oauth_authorization(
        &self,
        server_id: &str,
        _profile: &McpProfile,
    ) -> Result<(), McpError> {
        Err(McpError::Configuration(format!(
            "MCP server `{server_id}` uses OAuth, which requires the `mcp-oauth` cargo feature"
        )))
    }

    pub async fn cancel_pending_authorization(&self, server_id: &str) -> Result<(), McpError> {
        let profile = self.resolve(server_id)?;
        if profile.spec.auth != McpAuth::Oauth {
            return Err(McpError::Configuration(format!(
                "MCP server `{server_id}` does not use OAuth"
            )));
        }
        self.cancel_oauth_authorization(server_id, profile).await
    }

    #[cfg(feature = "mcp-oauth")]
    async fn cancel_oauth_authorization(
        &self,
        server_id: &str,
        _profile: &McpProfile,
    ) -> Result<(), McpError> {
        let lifecycle_gate = self.lifecycle_gate(server_id)?;
        let _lifecycle = lifecycle_gate.lock().await;
        self.inner
            .pending
            .lock()
            .await
            .retain(|_, pending| pending.server_id != server_id);
        Ok(())
    }

    #[cfg(not(feature = "mcp-oauth"))]
    async fn cancel_oauth_authorization(
        &self,
        server_id: &str,
        _profile: &McpProfile,
    ) -> Result<(), McpError> {
        Err(McpError::Configuration(format!(
            "MCP server `{server_id}` uses OAuth, which requires the `mcp-oauth` cargo feature"
        )))
    }

    #[cfg(feature = "mcp-oauth")]
    async fn authorization_manager(&self, profile: &McpProfile) -> Result<OAuthState, McpError> {
        let oauth_client = Arc::new(AllowedOAuthHttpClient::new(profile)?);
        let oauth_url = profile.url.as_ref().ok_or_else(|| {
            McpError::Configuration(format!(
                "OAuth profile `{}` has no validated URL",
                profile.id()
            ))
        })?;
        let mut manager =
            AuthorizationManager::new_with_oauth_http_client(oauth_url.clone(), oauth_client)
                .await
                .map_err(auth_error)?;
        manager.set_credential_store(self.store(profile)?);
        if manager.initialize_from_store().await.map_err(auth_error)? {
            Ok(OAuthState::Authorized(manager))
        } else {
            Ok(OAuthState::Unauthorized(manager))
        }
    }

    #[cfg(feature = "mcp-oauth")]
    fn store(&self, profile: &McpProfile) -> Result<ProfileCredentialStore, McpError> {
        self.inner.stores.get(profile.id()).cloned().ok_or_else(|| {
            McpError::Configuration(format!(
                "MCP server `{}` has no credential store",
                profile.id()
            ))
        })
    }

    pub async fn connect(
        &self,
        server_id: &str,
        access: &McpAccess,
        cancellation: CancellationToken,
    ) -> Result<McpSession, McpError> {
        let profile = self.resolve(server_id)?.clone();
        access.validate(&profile)?;
        let lifecycle_gate = self.lifecycle_gate(server_id)?;
        let _lifecycle = tokio::select! {
            _ = cancellation.cancelled() => return Err(McpError::Canceled),
            lifecycle = lifecycle_gate.lock() => lifecycle,
        };
        let active_sessions = self.active_sessions(server_id)?;
        let reservation = ActiveSessionReservation::acquire(active_sessions);
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
            // Reservation Drop returns the count on failure or future drop.
            Err(error) => return Err(error),
        };
        session.active_sessions = Some(reservation.disarm());
        Ok(session)
    }

    pub(crate) fn active_sessions(&self, server_id: &str) -> Result<Arc<AtomicUsize>, McpError> {
        self.inner
            .active_sessions
            .get(server_id)
            .cloned()
            .ok_or_else(|| {
                McpError::Configuration(format!("MCP server `{server_id}` has no session counter"))
            })
    }

    fn lifecycle_gate(&self, server_id: &str) -> Result<Arc<Mutex<()>>, McpError> {
        self.inner
            .lifecycle_gates
            .get(server_id)
            .cloned()
            .ok_or_else(|| {
                McpError::Configuration(format!("MCP server `{server_id}` has no lifecycle gate"))
            })
    }

    async fn connect_http(
        &self,
        profile: McpProfile,
        cancellation: CancellationToken,
    ) -> Result<McpSession, McpError> {
        let http_url = profile.url.as_ref().ok_or_else(|| {
            McpError::Configuration(format!(
                "HTTP profile `{}` has no validated URL",
                profile.id()
            ))
        })?;
        let mut config = StreamableHttpClientTransportConfig::with_uri(http_url.as_str())
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
                let credential =
                    required_env(profile.spec.credential_env.as_deref().ok_or_else(|| {
                        McpError::Configuration(format!(
                            "MCP profile `{}` declares bearer auth without a credential env",
                            profile.id()
                        ))
                    })?)?;
                config = config.auth_header(credential.clone());
                CredentialGuard::Static(vec![credential])
            }
            McpAuth::Header => {
                let credential =
                    required_env(profile.spec.credential_env.as_deref().ok_or_else(|| {
                        McpError::Configuration(format!(
                            "MCP profile `{}` declares header auth without a credential env",
                            profile.id()
                        ))
                    })?)?;
                headers.insert(
                    http::HeaderName::from_bytes(
                        profile
                            .spec
                            .auth_header
                            .as_deref()
                            .ok_or_else(|| {
                                McpError::Configuration(format!(
                                    "MCP profile `{}` declares header auth without an auth header",
                                    profile.id()
                                ))
                            })?
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
                #[cfg(feature = "mcp-oauth")]
                {
                    let auth_client = self.authorized_client(&profile).await?;
                    config = config.custom_headers(headers);
                    let credential_guard = CredentialGuard::OAuth(auth_client.clone());
                    let transport = StreamableHttpClientTransport::with_client(auth_client, config);
                    return McpSession::serve(profile, transport, cancellation, credential_guard)
                        .await;
                }
                #[cfg(not(feature = "mcp-oauth"))]
                {
                    return Err(McpError::Configuration(format!(
                        "MCP profile `{}` uses OAuth, which requires the `mcp-oauth` cargo feature",
                        profile.id()
                    )));
                }
            }
        };
        config = config.custom_headers(headers);
        let transport =
            StreamableHttpClientTransport::with_client(mcp_http_client(&profile)?, config);
        McpSession::serve(profile, transport, cancellation, credential_guard).await
    }

    #[cfg(feature = "mcp-oauth")]
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
        let lifecycle_gate = self.lifecycle_gate(profile.id())?;
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
            .ok_or_else(|| {
                McpError::Configuration(format!(
                    "MCP server `{}` stdio command is not permitted",
                    profile.id()
                ))
            })?;
        let (bin, args) = profile.spec.command.split_first().ok_or_else(|| {
            McpError::Configuration(format!(
                "MCP server `{}` stdio command is empty",
                profile.id()
            ))
        })?;
        let command = match permission.isolation {
            McpCommandIsolation::TrustedHost => {
                let mut command = tokio::process::Command::new(bin);
                command.args(args);
                (command, None)
            }
            McpCommandIsolation::Container => {
                let backend = permission
                    .runtime
                    .as_ref()
                    .and_then(mcp_container_backend)
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
                // Static profile env plus resolved secret-backed env. Secret
                // values travel in Minted `--env`/`-v` argv for managed
                // families (documented host-local visibility); Docker-family
                // `-e` passthrough keeps values out of argv instead.
                let mut server_env: Vec<(String, String)> = profile
                    .spec
                    .env
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect();
                for (target, source) in &profile.spec.env_from {
                    server_env.push((target.clone(), required_env(source)?));
                }
                let workload: Vec<String> = std::iter::once(bin.to_string())
                    .chain(args.iter().cloned())
                    .collect();
                let (command, session) = match &backend {
                    qcg_container::Backend::Docker {
                        binary,
                        runtime_flag,
                    } => {
                        let mount = qcg_container::Mount {
                            host: &access.workspace,
                            guest: "/work",
                            readonly: false,
                        };
                        // cidfile stays outside the mounted workspace so the
                        // container cannot tamper with the tracked container id.
                        let cidfile = std::env::temp_dir().join(format!(
                            ".qcg-mcp-container-{}-{}.cid",
                            profile.id(),
                            uuid::Uuid::now_v7().as_simple()
                        ));
                        let env_names: Vec<String> =
                            server_env.iter().map(|(name, _)| name.clone()).collect();
                        let argv = qcg_container::docker_run_argv(&qcg_container::DockerRunSpec {
                            binary,
                            runtime_flag: runtime_flag.as_deref(),
                            cidfile: &cidfile,
                            mounts: &[mount],
                            workdir: Some("/work"),
                            env_names: &env_names,
                            image,
                            workload: &workload,
                            stdin_pipe: true,
                        });
                        let (bin, args) = argv.split_first().ok_or_else(|| {
                            McpError::Configuration("MCP server argv is empty".into())
                        })?;
                        let mut command = tokio::process::Command::new(bin);
                        command.args(args);
                        // `-e NAME` passes the client env value through; the
                        // values themselves are set by the shared env block
                        // below, so they never appear in argv.
                        let session = qcg_container::Session {
                            backend: backend.clone(),
                            id: qcg_container::InstanceId::CidFile(cidfile),
                        };
                        (command, session)
                    }
                    managed => {
                        let mount = qcg_container::Mount {
                            host: &access.workspace,
                            guest: "/work",
                            readonly: false,
                        };
                        let session = qcg_container::provision(
                            managed,
                            &qcg_container::Provision {
                                image,
                                mounts: &[mount],
                                id_prefix: "qcg-mcp-container",
                                cancel: &cancellation,
                            },
                        )
                        .await
                        .map_err(|error| McpError::Transport(error.to_string()))?;
                        let name = match &session.id {
                            qcg_container::InstanceId::Name(name) => name.clone(),
                            qcg_container::InstanceId::CidFile(_) => {
                                return Err(McpError::Transport(
                                    "managed provision returned no instance name".into(),
                                ));
                            }
                        };
                        let argv = match managed {
                            qcg_container::Backend::Incus { binary } => {
                                qcg_container::incus_exec_argv(
                                    binary,
                                    &name,
                                    None,
                                    &server_env,
                                    &workload,
                                )
                            }
                            qcg_container::Backend::Lxc => qcg_container::lxc_server_argv(
                                &name,
                                qcg_container::LXC_MINIMAL_PATH,
                                &server_env,
                                &workload,
                            ),
                            qcg_container::Backend::Docker { .. } => {
                                return Err(McpError::Configuration(
                                    "docker backends take the one-shot path".into(),
                                ));
                            }
                        };
                        let (bin, args) = argv.split_first().ok_or_else(|| {
                            McpError::Configuration("MCP server argv is empty".into())
                        })?;
                        let mut command = tokio::process::Command::new(bin);
                        command.args(args);
                        (command, session)
                    }
                };
                (command, Some(session))
            }
        };
        let (mut command, container_cleanup) = command;
        // Docker-family `-e NAME` passthrough reads values from the client
        // environment; managed families carry values in spawn argv instead,
        // so profile secrets never touch the client environment there.
        let passthrough_env = container_cleanup
            .as_ref()
            .is_none_or(|session| matches!(session.backend, qcg_container::Backend::Docker { .. }));
        command
            .current_dir(&access.workspace)
            .env_clear()
            .kill_on_drop(true);
        if let Ok(path) = std::env::var("PATH") {
            command.env("PATH", path);
        }
        // Daemon clients may need a home directory for their configuration;
        // workload isolation is unaffected because entered processes start
        // from explicit env flags.
        if !passthrough_env && let Ok(home) = std::env::var("HOME") {
            command.env("HOME", home);
        }
        if passthrough_env {
            for (name, value) in &profile.spec.env {
                command.env(name, value);
            }
        }
        let mut sensitive_values = Vec::new();
        for (target, source) in &profile.spec.env_from {
            let value = required_env(source)?;
            if passthrough_env {
                command.env(target, &value);
            }
            sensitive_values.push(value);
        }
        let mut transport = BoundedChildTransport::spawn(command, profile.spec.max_response_bytes)
            .map_err(|error| McpError::Transport(error.to_string()))?;
        if let Some(session) = container_cleanup {
            transport = transport.with_container_cleanup(session);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn reservation_returns_count_on_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let _reservation = ActiveSessionReservation::acquire(Arc::clone(&counter));
            assert_eq!(counter.load(Ordering::Acquire), 1);
            // Dropping the future (or failing to connect) must not leak the count.
        }
        assert_eq!(counter.load(Ordering::Acquire), 0);
    }

    #[test]
    fn disarmed_reservation_keeps_count_for_session() {
        let counter = Arc::new(AtomicUsize::new(0));
        let reservation = ActiveSessionReservation::acquire(Arc::clone(&counter));
        let owned = reservation.disarm();
        assert_eq!(counter.load(Ordering::Acquire), 1);
        // Session Drop returns the count instead.
        owned.fetch_sub(1, Ordering::AcqRel);
        assert_eq!(counter.load(Ordering::Acquire), 0);
    }
}
