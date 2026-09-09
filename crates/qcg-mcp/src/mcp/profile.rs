#[cfg(feature = "mcp-oauth")]
use crate::bounded_http::BoundedHttpClient;
#[cfg(feature = "mcp-oauth")]
use async_trait::async_trait;
#[cfg(feature = "mcp-oauth")]
use keyring::Entry;
#[cfg(feature = "mcp-oauth")]
use reqwest::redirect::Policy;
#[cfg(feature = "mcp-oauth")]
use rmcp::transport::auth::{
    AuthClient, AuthError, CredentialStore, InMemoryCredentialStore, OAuthHttpClient,
    OAuthHttpClientFuture, OAuthHttpRedirectPolicy, OAuthHttpRequest, OAuthState,
    StoredCredentials,
};
#[cfg(feature = "mcp-oauth")]
use std::collections::BTreeSet;
use std::sync::Arc;
#[cfg(feature = "mcp-oauth")]
use std::time::{Duration, Instant};
use url::Url;

use super::error::McpError;
use super::spec::McpServerSpec;
use super::transport::{McpAuth, McpTransport};
#[cfg(feature = "mcp-oauth")]
use super::validate::{auth_error, is_secure_remote_url, validate_oauth_operation_url};

#[cfg(feature = "mcp-oauth")]
const KEYRING_SERVICE: &str = "qcg.mcp.oauth";

#[derive(Debug, Clone)]
pub struct McpProfile {
    pub(crate) spec: Arc<McpServerSpec>,
    pub(crate) url: Option<Url>,
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

#[cfg(feature = "mcp-oauth")]
#[derive(Clone)]
pub(crate) enum ProfileCredentialStore {
    Keyring(KeyringCredentialStore),
    Memory(InMemoryCredentialStore),
}

#[derive(Clone)]
pub(crate) enum CredentialGuard {
    None,
    Static(Vec<String>),
    #[cfg(feature = "mcp-oauth")]
    OAuth(AuthClient<BoundedHttpClient>),
}

impl std::fmt::Debug for CredentialGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::None => "CredentialGuard::None",
            Self::Static(_) => "CredentialGuard::Static(<redacted>)",
            #[cfg(feature = "mcp-oauth")]
            Self::OAuth(_) => "CredentialGuard::OAuth(<redacted>)",
        })
    }
}

impl CredentialGuard {
    pub(crate) async fn values(&self) -> Result<Vec<String>, McpError> {
        match self {
            Self::None => Ok(Vec::new()),
            Self::Static(values) => Ok(values.clone()),
            #[cfg(feature = "mcp-oauth")]
            Self::OAuth(client) => client
                .get_access_token()
                .await
                .map(|value| vec![value])
                .map_err(auth_error),
        }
    }
}

#[cfg(feature = "mcp-oauth")]
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

#[cfg(feature = "mcp-oauth")]
#[derive(Debug, Clone)]
pub(crate) struct KeyringCredentialStore {
    account: String,
}

#[cfg(feature = "mcp-oauth")]
impl KeyringCredentialStore {
    pub(crate) fn new(account: impl Into<String>) -> Self {
        Self {
            account: account.into(),
        }
    }
}

#[cfg(feature = "mcp-oauth")]
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

#[cfg(feature = "mcp-oauth")]
pub(crate) struct PendingAuthorization {
    pub(crate) server_id: String,
    pub(crate) state: OAuthState,
    pub(crate) expires_at: Instant,
}

#[cfg(feature = "mcp-oauth")]
#[derive(Debug)]
pub(crate) struct AllowedOAuthHttpClient {
    client: reqwest::Client,
    stop_client: reqwest::Client,
    allowed_hosts: BTreeSet<String>,
    max_response_bytes: usize,
}

#[cfg(feature = "mcp-oauth")]
impl AllowedOAuthHttpClient {
    pub(crate) fn new(profile: &McpProfile) -> Result<Self, McpError> {
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

#[cfg(feature = "mcp-oauth")]
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

#[cfg(feature = "mcp-oauth")]
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
