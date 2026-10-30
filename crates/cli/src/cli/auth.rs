//! Standalone subscription credentials; intentionally not connected to inference.
mod oauth;
#[cfg(test)]
mod tests;

use super::args::AuthCommand;
use anyhow::{Result, bail};
use oauth::{ISSUER, Provider};
use openidconnect::{Nonce, OAuth2TokenResponse, core::CoreTokenResponse};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SERVICE: &str = "qcg.chatgpt.oauth";
const REGISTRATION: &str = "dynamic_agent_client";
const CALLBACK_PATH: &str = "/auth/callback";
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(300);
const CREDENTIAL_LIMIT: usize = 512 * 1024;

fn failure() -> anyhow::Error {
    anyhow::anyhow!("credential storage unavailable (details redacted)")
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// No Debug implementation: these values must never reach logs or error chains.
#[derive(Serialize, Deserialize, Clone)]
struct Credential {
    client_id: String,
    subject: String,
    nonce: String,
    access: String,
    refresh: String,
    expires: u64,
    // Persisted before contacting the refresh endpoint. A crash, rejection or
    // failed write cannot silently replay a potentially consumed refresh token.
    pending: bool,
}
#[derive(Serialize, Deserialize, Clone)]
struct Registration {
    client_id: String,
    subject: String,
}
trait Store {
    fn load(&self) -> Result<Option<Credential>>;
    fn save(&self, value: &Credential) -> Result<()>;
    fn clear(&self) -> Result<()>;
    fn load_registration(&self) -> Result<Option<Registration>>;
    fn save_registration(&self, value: &Registration) -> Result<()>;
    fn forget_registration(&self) -> Result<()>;
}
struct Keyring {
    credentials: keyring_store::Entry,
    registration: keyring_store::Entry,
}
impl Keyring {
    fn open(account: &str) -> Result<Self> {
        Ok(Self {
            credentials: keyring_store::open(SERVICE, account).map_err(|_| failure())?,
            registration: keyring_store::open(SERVICE, &format!("{account}-registration"))
                .map_err(|_| failure())?,
        })
    }
}
impl Store for Keyring {
    fn load(&self) -> Result<Option<Credential>> {
        match self.credentials.get_secret() {
            Ok(bytes) if bytes.len() <= CREDENTIAL_LIMIT => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|_| failure()),
            Ok(_) => Err(failure()),
            Err(keyring_store::Error::NoEntry) => Ok(None),
            Err(_) => Err(failure()),
        }
    }
    fn save(&self, value: &Credential) -> Result<()> {
        let bytes = serde_json::to_vec(value).map_err(|_| failure())?;
        if bytes.len() > CREDENTIAL_LIMIT {
            return Err(failure());
        }
        self.credentials.set_secret(&bytes).map_err(|_| failure())
    }
    fn clear(&self) -> Result<()> {
        match self.credentials.delete_credential() {
            Ok(()) | Err(keyring_store::Error::NoEntry) => Ok(()),
            Err(_) => Err(failure()),
        }
    }
    fn load_registration(&self) -> Result<Option<Registration>> {
        match self.registration.get_secret() {
            Ok(bytes) if bytes.len() <= CREDENTIAL_LIMIT => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|_| failure()),
            Ok(_) => Err(failure()),
            Err(keyring_store::Error::NoEntry) => Ok(None),
            Err(_) => Err(failure()),
        }
    }
    fn save_registration(&self, value: &Registration) -> Result<()> {
        let bytes = serde_json::to_vec(value).map_err(|_| failure())?;
        if bytes.len() > CREDENTIAL_LIMIT {
            return Err(failure());
        }
        self.registration.set_secret(&bytes).map_err(|_| failure())
    }
    fn forget_registration(&self) -> Result<()> {
        match self.registration.delete_credential() {
            Ok(()) | Err(keyring_store::Error::NoEntry) => Ok(()),
            Err(_) => Err(failure()),
        }
    }
}
fn state_dir() -> Result<PathBuf> {
    let root = if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA").map(PathBuf::from)
    } else {
        std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state"))
            })
    }
    .ok_or_else(failure)?;
    if !root.is_absolute() {
        bail!("authentication state directory must be absolute");
    }
    let path = root.join("qcg/auth");
    std::fs::create_dir_all(&path).map_err(|_| failure())?;
    Ok(path)
}
// These files contain no tokens, account labels or host identity. They are never
// removed: unlinking an advisory lock would split its serialization domain.
async fn lock(dir: &Path, key: &str) -> Result<File> {
    let path = dir.join(format!(
        "{}.lock",
        hex::encode(Sha256::digest(key.as_bytes()))
    ));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|_| failure())?;
    tokio::time::timeout(Duration::from_secs(360), async {
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(()),
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(Duration::from_millis(25)).await
                }
                Err(_) => return Err(failure()),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("credential busy; retry later"))??;
    Ok(file)
}
async fn host_id(dir: &Path) -> Result<String> {
    let _lock = lock(dir, "host").await?;
    let entry = keyring_store::open(SERVICE, "host").map_err(|_| failure())?;
    match entry.get_secret() {
        Ok(bytes) => {
            let id = String::from_utf8(bytes).map_err(|_| failure())?;
            let parsed = uuid::Uuid::parse_str(&id).map_err(|_| failure())?;
            if parsed.to_string() != id {
                return Err(failure());
            }
            Ok(id)
        }
        Err(keyring_store::Error::NoEntry) => {
            let id = uuid::Uuid::now_v7().to_string();
            entry.set_secret(id.as_bytes()).map_err(|_| failure())?;
            Ok(id)
        }
        Err(_) => Err(failure()),
    }
}
fn account_key(label: &str) -> Result<String> {
    if label.is_empty() || label.len() > 128 {
        bail!("account label must contain 1..=128 bytes");
    }
    Ok(format!(
        "account-{}",
        hex::encode(Sha256::digest(label.as_bytes()))
    ))
}
fn selected_id<'a>(selected: Option<&'a str>, issued: Option<&'a str>) -> Result<&'a str> {
    let valid =
        |id: &str| !id.is_empty() && id != REGISTRATION && !id.chars().any(char::is_control);
    if selected.is_some_and(|id| !valid(id)) || issued.is_some_and(|id| !valid(id)) {
        bail!("callback client binding failed");
    }
    match (selected, issued) {
        (None, Some(id)) => Ok(id),
        (Some(id), None) => Ok(id),
        (Some(id), Some(returned)) if id == returned => Ok(id),
        _ => bail!("callback client binding failed"),
    }
}
struct Callback {
    code: Result<String>,
    client_id: Option<String>,
}
fn parse_callback(target: &str, state: &str) -> Result<Callback> {
    let url = url::Url::parse(&format!("http://localhost{target}"))
        .map_err(|_| anyhow::anyhow!("invalid callback"))?;
    if !target.starts_with('/') || url.path() != CALLBACK_PATH || url.fragment().is_some() {
        bail!("invalid callback");
    }
    let mut values = std::collections::BTreeMap::new();
    for (key, value) in url.query_pairs() {
        if values
            .insert(key.into_owned(), value.into_owned())
            .is_some()
        {
            bail!("duplicate callback field");
        }
    }
    if values.get("state").map(String::as_str) != Some(state) {
        bail!("callback state validation failed");
    }
    let code = if values.contains_key("error") {
        Err(anyhow::anyhow!(
            "authorization refused (provider details redacted)"
        ))
    } else {
        values
            .remove("code")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("callback code missing"))
    };
    Ok(Callback {
        code,
        client_id: values.remove("client_id"),
    })
}
async fn callback(listener: &tokio::net::TcpListener, state: &str) -> Result<Callback> {
    tokio::time::timeout(CALLBACK_TIMEOUT, async {
        loop {
            let (mut socket, _) = listener.accept().await.map_err(|_| anyhow::anyhow!("callback receiver failed"))?;
            let request = tokio::time::timeout(Duration::from_secs(5), async {
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    if bytes.len() >= 8192 { bail!("callback too large"); }
                    bytes.push(socket.read_u8().await.map_err(|_| anyhow::anyhow!("invalid callback"))?);
                }
                let text = std::str::from_utf8(&bytes).map_err(|_| anyhow::anyhow!("invalid callback"))?;
                let mut parts = text.lines().next().unwrap_or_default().split_whitespace();
                if parts.next() != Some("GET") { bail!("invalid callback method"); }
                let target = parts.next().ok_or_else(|| anyhow::anyhow!("invalid callback"))?;
                parse_callback(target, state)
            }).await;
            let Ok(Ok(result)) = request else {
                let _ = socket.write_all(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await;
                continue;
            };
            let body = "Return to qcg to finish signing in.";
            let reply = format!("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}", body.len());
            let _ = socket.write_all(reply.as_bytes()).await;
            return Ok(result);
        }
    }).await.map_err(|_| anyhow::anyhow!("sign-in callback timed out"))?
}
fn token_expiry(response: &CoreTokenResponse) -> Result<u64> {
    if response.token_type() != &openidconnect::core::CoreTokenType::Bearer {
        bail!("unsupported credential token type");
    }
    let seconds = response
        .expires_in()
        .filter(|d| !d.is_zero())
        .ok_or_else(|| anyhow::anyhow!("access token expiry missing"))?
        .as_secs();
    now()
        .checked_add(seconds)
        .ok_or_else(|| anyhow::anyhow!("invalid access token expiry"))
}
fn credential(
    provider: &Provider,
    client_id: &str,
    nonce: &Nonce,
    response: CoreTokenResponse,
) -> Result<Credential> {
    let subject = provider
        .validate(client_id, &response, nonce, true)?
        .ok_or_else(|| anyhow::anyhow!("ID token missing"))?;
    let refresh = response
        .refresh_token()
        .filter(|t| !t.secret().is_empty())
        .ok_or_else(|| anyhow::anyhow!("refresh token missing"))?
        .secret()
        .clone();
    if response.access_token().secret().is_empty() {
        bail!("access token missing");
    }
    Ok(Credential {
        client_id: client_id.into(),
        subject,
        nonce: nonce.secret().clone(),
        access: response.access_token().secret().clone(),
        refresh,
        expires: token_expiry(&response)?,
        pending: false,
    })
}
// Caller holds the per-account lock, including the reread, request and commit.
async fn refresh(
    store: &impl Store,
    provider: &Provider,
    mut current: Credential,
) -> Result<Credential> {
    if current.pending {
        bail!("previous refresh unconfirmed; sign in again");
    }
    if current.expires > now().saturating_add(60) {
        return Ok(current);
    }
    current.pending = true;
    store.save(&current)?;
    let response = provider
        .refresh(&current.client_id, &current.refresh)
        .await?;
    // Preserve a rotated refresh token even when other response validation fails.
    // It remains unusable until the complete response is validated and committed.
    let rotated = response
        .refresh_token()
        .filter(|token| {
            !token.secret().is_empty() && token.secret().as_str() != current.refresh.as_str()
        })
        .ok_or_else(|| anyhow::anyhow!("refresh token rotation missing; sign in again"))?;
    current.refresh = rotated.secret().clone();
    store.save(&current)?;
    if let Some(subject) = provider.validate(
        &current.client_id,
        &response,
        &Nonce::new(current.nonce.clone()),
        false,
    )? && subject != current.subject
    {
        bail!("refresh subject changed; sign in again");
    }
    current.expires = token_expiry(&response)?;
    current.access = response.access_token().secret().clone();
    if current.access.is_empty() || current.refresh.is_empty() {
        bail!("refresh returned empty credentials");
    }
    current.pending = false;
    store.save(&current)?;
    Ok(current)
}
async fn logout(
    store: &impl Store,
    issuer: Option<&str>,
    current: Option<Credential>,
    forget: bool,
) -> Result<bool> {
    // Keep tokens available for retry if the process stops during revocation.
    // Regardless of the remote outcome, clear local tokens before returning.
    let confirmed = if let (Some(issuer), Some(current)) = (issuer, current) {
        match Provider::discover(issuer).await {
            Ok(provider) => {
                let revoked = provider.revoke(&current.client_id, &current.refresh).await;
                // A pending refresh may have consumed this token before saving
                // its replacement; HTTP 200 also covers already-invalid tokens.
                revoked && !current.pending
            }
            Err(_) => false,
        }
    } else {
        false
    };
    store.clear()?;
    if forget {
        store.forget_registration()?;
    }
    Ok(confirmed)
}
pub(crate) async fn run(command: AuthCommand) -> Result<()> {
    let (AuthCommand::Login { ref account }
    | AuthCommand::Status { ref account, .. }
    | AuthCommand::Logout { ref account, .. }) = command;
    let key = account_key(account)?;
    let dir = state_dir()?;
    let _lock = lock(&dir, &key).await?;
    let store = Keyring::open(&key)?;
    if let AuthCommand::Logout { local, forget, .. } = command {
        // A damaged record must not prevent local deletion.
        let current = store.load().ok().flatten();
        let confirmed = logout(&store, (!local).then_some(ISSUER), current, forget).await?;
        println!(
            "Local credentials cleared; account registration {}. Remote revocation {}.",
            if forget { "forgotten" } else { "retained" },
            if confirmed {
                "confirmed"
            } else {
                "not confirmed"
            }
        );
        return Ok(());
    }
    let current = store.load()?;
    let registration = store.load_registration()?;
    if let (Some(current), Some(registration)) = (&current, &registration)
        && (current.client_id != registration.client_id || current.subject != registration.subject)
    {
        bail!("local credential and account registration mismatch");
    }
    match command {
        AuthCommand::Login { .. } => {
            let host = host_id(&dir).await?;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:1455")
                .await
                .map_err(|_| anyhow::anyhow!("cannot bind sign-in callback on 127.0.0.1:1455"))?;
            let redirect = "http://localhost:1455/auth/callback";
            let provider = Provider::discover(ISSUER).await?;
            let selected = registration
                .as_ref()
                .map(|r| r.client_id.as_str())
                .or_else(|| current.as_ref().map(|c| c.client_id.as_str()));
            if let Some(id) = selected {
                selected_id(Some(id), None)?;
            }
            let auth = provider.authorize(selected.unwrap_or(REGISTRATION), &host, redirect)?;
            // The only provider URL printed is one constructed locally. Tokens,
            // provider errors and callback URLs are never printed or logged.
            println!("Open this URL in your browser to sign in:\n{}", auth.url);
            let response = callback(&listener, auth.state.secret()).await?;
            let code = response.code?;
            let id = selected_id(selected, response.client_id.as_deref())?;
            let tokens = provider.exchange(id, redirect, code, auth.verifier).await?;
            let saved = credential(&provider, id, &auth.nonce, tokens)?;
            let previous_subject = registration
                .as_ref()
                .map(|r| r.subject.as_str())
                .or_else(|| current.as_ref().map(|c| c.subject.as_str()));
            if previous_subject.is_some_and(|subject| subject != saved.subject) {
                bail!("reauthentication account changed; use a different account label");
            }
            store.save_registration(&Registration {
                client_id: saved.client_id.clone(),
                subject: saved.subject.clone(),
            })?;
            store.save(&saved)?;
            println!(
                "Credentials stored. Subscription inference remains disabled; eligibility is not established."
            );
        }
        AuthCommand::Status { refresh: renew, .. } => {
            let Some(mut current) = current else {
                println!(
                    "No local credentials; account registration {}.",
                    if registration.is_some() {
                        "retained"
                    } else {
                        "not found"
                    }
                );
                return Ok(());
            };
            if renew {
                if current.pending {
                    bail!("previous refresh unconfirmed; sign in again");
                }
                if current.expires <= now().saturating_add(60) {
                    let provider = Provider::discover(ISSUER).await?;
                    current = refresh(&store, &provider, current).await?;
                }
            }
            println!(
                "Local credentials: {}; access token expires at {} (Unix seconds). Subscription inference disabled; eligibility unknown.",
                if current.pending {
                    "reauthentication required"
                } else if current.expires <= now() {
                    "expired"
                } else {
                    "unexpired"
                },
                current.expires
            );
        }
        AuthCommand::Logout { .. } => unreachable!(),
    }
    Ok(())
}
