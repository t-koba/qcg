use super::*;
use openidconnect::{
    AccessToken, Audience, EmptyAdditionalClaims, IssuerUrl, JsonWebKeyId, PrivateSigningKey,
    StandardClaims, SubjectIdentifier,
    core::{CoreEdDsaPrivateSigningKey, CoreIdToken, CoreIdTokenClaims, CoreJwsSigningAlgorithm},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

// Public, deterministic test-only seed (32 zero bytes), never an account key.
fn signing_key() -> CoreEdDsaPrivateSigningKey {
    CoreEdDsaPrivateSigningKey::from_ed25519_pem(
        "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n-----END PRIVATE KEY-----",
        Some(JsonWebKeyId::new("offline-test".into())),
    ).unwrap()
}
fn claims(issuer: &str, audience: &str, nonce: Option<&str>) -> CoreIdTokenClaims {
    CoreIdTokenClaims::new(
        IssuerUrl::new(issuer.into()).unwrap(),
        vec![Audience::new(audience.into())],
        chrono::Utc::now() + chrono::Duration::seconds(300),
        chrono::Utc::now(),
        StandardClaims::new(SubjectIdentifier::new("test-subject".into())),
        EmptyAdditionalClaims {},
    )
    .set_nonce(nonce.map(|s| Nonce::new(s.into())))
}
fn response(claims: Option<CoreIdTokenClaims>) -> Value {
    let mut response = json!({"access_token": "test-access", "refresh_token": "test-refresh", "token_type": "Bearer", "expires_in": 3600,
        // Unknown provider extensions are not interpreted as expiry or refresh policy.
        "earliest_refresh_at": u64::MAX});
    if let Some(claims) = claims {
        let token = CoreIdToken::new(
            claims,
            &signing_key(),
            CoreJwsSigningAlgorithm::EdDsa,
            Some(&AccessToken::new("test-access".into())),
            None,
        )
        .unwrap();
        response["id_token"] = json!(token.to_string());
    }
    response
}
#[derive(Clone)]
struct Memory {
    value: Arc<Mutex<Option<Credential>>>,
    registration: Arc<Mutex<Option<Registration>>>,
    writes: Arc<Mutex<Vec<Credential>>>,
    clear_after_revoke_since: Option<(Arc<Mutex<MockState>>, usize)>,
    fail_at: usize,
}
impl Memory {
    fn new(value: Option<Credential>) -> Self {
        let registration = value.as_ref().map(|credential| Registration {
            client_id: credential.client_id.clone(),
            subject: credential.subject.clone(),
        });
        Self {
            value: Arc::new(Mutex::new(value)),
            registration: Arc::new(Mutex::new(registration)),
            writes: Arc::default(),
            clear_after_revoke_since: None,
            fail_at: usize::MAX,
        }
    }
}
impl Store for Memory {
    fn load(&self) -> Result<Option<Credential>> {
        Ok(self.value.lock().unwrap().clone())
    }
    fn save(&self, value: &Credential) -> Result<()> {
        let mut writes = self.writes.lock().unwrap();
        if writes.len() == self.fail_at {
            return Err(failure());
        }
        writes.push(value.clone());
        *self.value.lock().unwrap() = Some(value.clone());
        Ok(())
    }
    fn clear(&self) -> Result<()> {
        if let Some((state, start)) = &self.clear_after_revoke_since {
            assert!(
                state.lock().unwrap().requests[*start..]
                    .iter()
                    .any(|(path, _)| path == "/revoke")
            );
        }
        *self.value.lock().unwrap() = None;
        Ok(())
    }
    fn load_registration(&self) -> Result<Option<Registration>> {
        Ok(self.registration.lock().unwrap().clone())
    }
    fn save_registration(&self, value: &Registration) -> Result<()> {
        *self.registration.lock().unwrap() = Some(value.clone());
        Ok(())
    }
    fn forget_registration(&self) -> Result<()> {
        *self.registration.lock().unwrap() = None;
        Ok(())
    }
}
fn expired() -> Credential {
    Credential {
        client_id: "issued-client".into(),
        subject: "test-subject".into(),
        nonce: "test-nonce".into(),
        access: "test-access".into(),
        refresh: "old-refresh".into(),
        expires: 0,
        pending: false,
    }
}
struct MockState {
    metadata: Value,
    token: Value,
    token_status: u16,
    revoke_status: u16,
    requests: Vec<(String, String)>,
    raw_token: Option<String>,
}
struct Mock {
    issuer: String,
    state: Arc<Mutex<MockState>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Mock {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let metadata = json!({
            "issuer": issuer, "authorization_endpoint": format!("{issuer}/authorize"),
            "token_endpoint": format!("{issuer}/token"), "jwks_uri": format!("{issuer}/jwks"),
            "revocation_endpoint": format!("{issuer}/revoke"),
            "response_types_supported": ["code"], "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["EdDSA"],
        });
        let state = Arc::new(Mutex::new(MockState {
            metadata,
            token: response(None),
            token_status: 200,
            revoke_status: 200,
            requests: Vec::new(),
            raw_token: None,
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    bytes.push(socket.read_u8().await.unwrap());
                    assert!(bytes.len() < 8192);
                }
                let text = String::from_utf8(bytes).unwrap();
                let path = text.split_whitespace().nth(1).unwrap().to_owned();
                let length = text
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                assert!(length < 8192);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                let response = {
                    let mut state = shared.lock().unwrap();
                    state
                        .requests
                        .push((path.clone(), String::from_utf8(body).unwrap()));
                    let (status, body) = match path.as_str() {
                        "/.well-known/openid-configuration" => (200, state.metadata.clone()),
                        "/jwks" => (200, json!({"keys": [signing_key().as_verification_key()]})),
                        "/token" => (state.token_status, state.token.clone()),
                        "/revoke" => (state.revoke_status, json!({})),
                        _ => (404, json!({})),
                    };
                    if path == "/token" && state.raw_token.is_some() {
                        state.raw_token.clone().unwrap()
                    } else {
                        let body = body.to_string();
                        format!(
                            "HTTP/1.1 {status} Mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                    }
                };
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        Self {
            issuer,
            state,
            task,
        }
    }
    async fn provider(&self) -> Provider {
        Provider::discover(&self.issuer).await.unwrap()
    }
    fn forms(&self, path: &str) -> Vec<std::collections::BTreeMap<String, String>> {
        self.state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|(p, _)| p == path)
            .map(|(_, body)| {
                url::form_urlencoded::parse(body.as_bytes())
                    .into_owned()
                    .collect()
            })
            .collect()
    }
}
fn redacted(error: anyhow::Error) {
    let text = format!("{error:#?}");
    for secret in [
        "test-access",
        "test-refresh",
        "old-refresh",
        "private-provider-text",
        "secret-code",
        "evil.example",
    ] {
        assert!(!text.contains(secret), "error leaked fixture data");
    }
}

#[tokio::test]
async fn registration_callback_pkce_exchange_and_reauthorization() {
    let mock = Mock::new().await;
    let provider = mock.provider().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let redirect = format!("http://{addr}{CALLBACK_PATH}");
    let auth = provider
        .authorize(REGISTRATION, "stable-host", &redirect)
        .unwrap();
    let query: std::collections::BTreeMap<_, _> = auth.url.query_pairs().into_owned().collect();
    assert_eq!(query["client_id"], REGISTRATION);
    assert_eq!(query["resource"], oauth::RESOURCE);
    assert_eq!(query["ext_agent_host_id"], "stable-host");
    assert_eq!(query["agent_name_hint"], "qcg");
    assert_eq!(query["response_type"], "code");
    assert_eq!(query["code_challenge_method"], "S256");
    for scope in [
        "openid",
        "profile",
        "email",
        "offline_access",
        "resource.invoke",
        "chatgpt.tokens.use.direct",
    ] {
        assert!(query["scope"].split_whitespace().any(|s| s == scope));
    }
    assert_eq!(
        query["code_challenge"],
        openidconnect::PkceCodeChallenge::from_code_verifier_sha256(&auth.verifier).as_str()
    );
    let state = auth.state.secret().clone();
    let send = tokio::spawn(async move {
        for state in ["unrelated-state", &state] {
            let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
            socket.write_all(format!("GET {CALLBACK_PATH}?state={state}&code=secret-code&client_id=issued-client HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes()).await.unwrap();
            let mut response = Vec::new();
            socket.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();
            assert!(!response.contains("secret-code"));
            let (headers, body) = response.split_once("\r\n\r\n").unwrap();
            assert!(headers.contains(&format!("Content-Length: {}", body.len())));
        }
    });
    let callback = tokio::time::timeout(
        Duration::from_secs(5),
        callback(&listener, auth.state.secret()),
    )
    .await
    .unwrap()
    .unwrap();
    send.await.unwrap();
    let issued = selected_id(None, callback.client_id.as_deref()).unwrap();
    mock.state.lock().unwrap().token = response(Some(claims(
        &mock.issuer,
        issued,
        Some(auth.nonce.secret()),
    )));
    let tokens = provider
        .exchange(issued, &redirect, callback.code.unwrap(), auth.verifier)
        .await
        .unwrap();
    let saved = credential(&provider, issued, &auth.nonce, tokens).unwrap();
    assert_eq!(saved.subject, "test-subject");
    assert!(!saved.pending);
    let form = mock.forms("/token").pop().unwrap();
    assert_eq!(form["resource"], oauth::RESOURCE);
    assert_eq!(form["client_id"], "issued-client");
    assert_eq!(form["grant_type"], "authorization_code");
    assert_eq!(form["code"], "secret-code");
    assert_eq!(form["redirect_uri"], redirect);
    assert!(form.contains_key("code_verifier"));
    let reauth = provider
        .authorize(&saved.client_id, "stable-host", &redirect)
        .unwrap();
    let reauth_query: std::collections::BTreeMap<_, _> =
        reauth.url.query_pairs().into_owned().collect();
    assert_eq!(reauth_query["client_id"], "issued-client");
    assert!(!reauth_query.contains_key("agent_name_hint"));
    assert_eq!(reauth_query["ext_agent_host_id"], "stable-host");
    assert_eq!(
        selected_id(Some(&saved.client_id), None).unwrap(),
        "issued-client"
    );
    assert_eq!(
        selected_id(Some(&saved.client_id), Some("issued-client")).unwrap(),
        "issued-client"
    );
    assert!(selected_id(Some(&saved.client_id), Some("mismatch")).is_err());
    for selected in ["", REGISTRATION, "bad\nclient"] {
        assert!(selected_id(Some(selected), None).is_err());
    }
    assert_eq!(
        selected_id(None, Some("opaque:/id+")).unwrap(),
        "opaque:/id+"
    );
    for issued in [
        None,
        Some(""),
        Some(REGISTRATION),
        Some("private-provider-text\n"),
    ] {
        assert!(selected_id(None, issued).is_err());
    }
}

#[test]
fn callback_rejects_state_duplicates_errors_and_unsafe_paths_without_echo() {
    for target in [
        "/auth/callback?state=wrong&code=secret-code",
        "/auth/callback?state=test&state=test&code=secret-code",
        "/auth/callback?state=test&code=one&code=secret-code",
        "/other?state=test&code=secret-code",
        "/auth/callback?state=test&code=secret-code#private-provider-text",
    ] {
        redacted(parse_callback(target, "test").err().unwrap());
    }
    for target in [
        "/auth/callback?state=test&error=access_denied&error_description=private-provider-text",
        "/auth/callback?state=test",
    ] {
        redacted(parse_callback(target, "test").unwrap().code.err().unwrap());
    }
    assert!(parse_callback("/auth/callback?state=test&code=secret-code", "test").is_ok());
}

#[tokio::test]
async fn oidc_validates_signature_issuer_audience_nonce_expiry_and_access_binding() {
    let mock = Mock::new().await;
    let provider = mock.provider().await;
    let nonce = Nonce::new("test-nonce".into());
    let good = || claims(&mock.issuer, "issued-client", Some(nonce.secret()));
    let valid: CoreTokenResponse = serde_json::from_value(response(Some(good()))).unwrap();
    assert_eq!(
        provider
            .validate("issued-client", &valid, &nonce, true)
            .unwrap()
            .unwrap(),
        "test-subject"
    );
    for claims in [
        claims(
            "https://evil.example",
            "issued-client",
            Some(nonce.secret()),
        ),
        claims(&mock.issuer, "wrong-client", Some(nonce.secret())),
        claims(&mock.issuer, "issued-client", Some("wrong-nonce")),
        claims(&mock.issuer, "issued-client", None),
        good().set_expiration(chrono::Utc::now() - chrono::Duration::seconds(10)),
    ] {
        let tokens: CoreTokenResponse = serde_json::from_value(response(Some(claims))).unwrap();
        redacted(
            provider
                .validate("issued-client", &tokens, &nonce, true)
                .err()
                .unwrap(),
        );
    }
    let mut bad_signature = response(Some(good()));
    let mut token = bad_signature["id_token"].as_str().unwrap().to_owned();
    let signature = token.rfind('.').unwrap() + 1;
    let replacement = if &token[signature..signature + 1] == "A" {
        "B"
    } else {
        "A"
    };
    token.replace_range(signature..signature + 1, replacement);
    bad_signature["id_token"] = json!(token);
    let mut bad_binding = response(Some(good()));
    bad_binding["access_token"] = json!("substituted-access");
    for value in [bad_signature, bad_binding, response(None)] {
        let tokens: CoreTokenResponse = serde_json::from_value(value).unwrap();
        redacted(
            provider
                .validate("issued-client", &tokens, &nonce, true)
                .err()
                .unwrap(),
        );
    }
    // OIDC refresh may omit the ID token or its nonce; if present it must match.
    for nonce_value in [None, Some("test-nonce")] {
        let tokens: CoreTokenResponse = serde_json::from_value(response(Some(claims(
            &mock.issuer,
            "issued-client",
            nonce_value,
        ))))
        .unwrap();
        assert!(
            provider
                .validate("issued-client", &tokens, &nonce, false)
                .is_ok()
        );
    }
}

#[test]
fn token_expiry_requires_positive_explicit_lifetime_not_provider_extension() {
    for seconds in [None, Some(0)] {
        let mut value = response(None);
        if let Some(seconds) = seconds {
            value["expires_in"] = json!(seconds);
        } else {
            value.as_object_mut().unwrap().remove("expires_in");
        }
        let token: CoreTokenResponse = serde_json::from_value(value).unwrap();
        redacted(token_expiry(&token).err().unwrap());
    }
    let token: CoreTokenResponse = serde_json::from_value(response(None)).unwrap();
    assert!(token_expiry(&token).unwrap() <= now() + 3600);
}

#[tokio::test]
async fn refresh_requires_and_persists_rotation_before_commit() {
    let mock = Mock::new().await;
    let provider = mock.provider().await;
    for rotated in [true, false] {
        let mut tokens = response(None);
        if !rotated {
            tokens.as_object_mut().unwrap().remove("refresh_token");
        }
        mock.state.lock().unwrap().token = tokens;
        let store = Memory::new(Some(expired()));
        let result = refresh(&store, &provider, store.load().unwrap().unwrap()).await;
        if rotated {
            let renewed = result.unwrap();
            assert!(renewed.expires > now() + 3500);
            assert!(!renewed.pending);
            assert_eq!(renewed.refresh, "test-refresh");
            let writes = store.writes.lock().unwrap();
            assert!(writes.first().unwrap().pending);
            assert!(writes[1].pending);
            assert!(!writes.last().unwrap().pending);
        } else {
            redacted(result.err().unwrap());
            let saved = store.load().unwrap().unwrap();
            assert!(saved.pending);
            assert_eq!(saved.refresh, "old-refresh");
        }
    }
    for form in mock.forms("/token") {
        assert_eq!(form["resource"], oauth::RESOURCE);
        assert_eq!(form["grant_type"], "refresh_token");
        assert_eq!(form["client_id"], "issued-client");
        assert_eq!(form["refresh_token"], "old-refresh");
    }
}

#[tokio::test]
async fn refresh_rejects_a_reused_refresh_token() {
    let mock = Mock::new().await;
    let provider = mock.provider().await;
    let mut tokens = response(None);
    tokens["refresh_token"] = json!("old-refresh");
    mock.state.lock().unwrap().token = tokens;
    let store = Memory::new(Some(expired()));
    redacted(refresh(&store, &provider, expired()).await.err().unwrap());
    let saved = store.load().unwrap().unwrap();
    assert!(saved.pending);
    assert_eq!(saved.refresh, "old-refresh");
}

#[tokio::test]
async fn refresh_failure_and_write_failure_leave_reauth_marker_and_redact_errors() {
    let mock = Mock::new().await;
    let provider = mock.provider().await;
    // Failure before request must not consume the refresh token.
    let mut store = Memory::new(Some(expired()));
    store.fail_at = 0;
    redacted(refresh(&store, &provider, expired()).await.err().unwrap());
    assert!(mock.forms("/token").is_empty());
    // Failure at final commit retains the rotated refresh token, but not a usable entry.
    let mut store = Memory::new(Some(expired()));
    store.fail_at = 2;
    redacted(refresh(&store, &provider, expired()).await.err().unwrap());
    let saved = store.load().unwrap().unwrap();
    assert!(saved.pending);
    assert_eq!(saved.refresh, "test-refresh");
    assert!(refresh(&store, &provider, saved).await.is_err());
    // Invalid refreshed identity must likewise preserve rotation and fail closed.
    mock.state.lock().unwrap().token = response(Some(claims(
        &mock.issuer,
        "wrong-client",
        Some("test-nonce"),
    )));
    let store = Memory::new(Some(expired()));
    redacted(refresh(&store, &provider, expired()).await.err().unwrap());
    assert!(store.load().unwrap().unwrap().pending);
    assert_eq!(store.load().unwrap().unwrap().refresh, "test-refresh");
    mock.state.lock().unwrap().token_status = 400;
    mock.state.lock().unwrap().token =
        json!({"error": "invalid_grant", "error_description": "private-provider-text old-refresh"});
    let store = Memory::new(Some(expired()));
    redacted(refresh(&store, &provider, expired()).await.err().unwrap());
    assert!(store.load().unwrap().unwrap().pending);
}

#[tokio::test]
async fn lock_and_reread_serialize_refresh_and_skip_unexpired_credentials() {
    let mock = Mock::new().await;
    let provider = mock.provider().await;
    let store = Memory::new(Some(expired()));
    let dir = std::env::temp_dir().join(format!("qcg-auth-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&dir).unwrap();
    let update = || async {
        let _lock = lock(&dir, "account").await.unwrap();
        refresh(&store, &provider, store.load().unwrap().unwrap())
            .await
            .unwrap();
    };
    tokio::join!(update(), update());
    assert_eq!(mock.forms("/token").len(), 1);
    assert_eq!(store.load().unwrap().unwrap().refresh, "test-refresh");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn logout_success_failure_and_local_only_always_clear_locally() {
    let mock = Mock::new().await;
    for status in [200, 400, 500] {
        mock.state.lock().unwrap().revoke_status = status;
        let start = mock.state.lock().unwrap().requests.len();
        let mut store = Memory::new(Some(expired()));
        store.clear_after_revoke_since = Some((mock.state.clone(), start));
        let registration = store.load_registration().unwrap();
        assert_eq!(
            logout(&store, Some(&mock.issuer), store.load().unwrap(), false)
                .await
                .unwrap(),
            status == 200
        );
        assert!(store.load().unwrap().is_none());
        assert_eq!(
            store.load_registration().unwrap().is_some(),
            registration.is_some()
        );
    }
    // A consumed refresh token may have an unknown rotated replacement. The
    // revocation endpoint's 200 for invalid tokens cannot confirm this session.
    mock.state.lock().unwrap().revoke_status = 200;
    let start = mock.state.lock().unwrap().requests.len();
    let mut ambiguous = expired();
    ambiguous.pending = true;
    let mut store = Memory::new(Some(ambiguous));
    store.clear_after_revoke_since = Some((mock.state.clone(), start));
    assert!(
        !logout(&store, Some(&mock.issuer), store.load().unwrap(), false)
            .await
            .unwrap()
    );
    assert!(store.load().unwrap().is_none());

    let before = mock.state.lock().unwrap().requests.len();
    let store = Memory::new(Some(expired()));
    assert!(
        !logout(&store, None, store.load().unwrap(), false)
            .await
            .unwrap()
    );
    assert!(store.load().unwrap().is_none());
    assert_eq!(mock.state.lock().unwrap().requests.len(), before);
    // Missing remote revocation support must not be reported as success.
    mock.state
        .lock()
        .unwrap()
        .metadata
        .as_object_mut()
        .unwrap()
        .remove("revocation_endpoint");
    let before = mock.forms("/revoke").len();
    let store = Memory::new(Some(expired()));
    assert!(
        !logout(&store, Some(&mock.issuer), store.load().unwrap(), false)
            .await
            .unwrap()
    );
    assert!(store.load().unwrap().is_none());
    assert_eq!(mock.forms("/revoke").len(), before);
    // Discovery failure is also unconfirmed, never a local-clear failure.
    mock.state.lock().unwrap().metadata["issuer"] = json!("https://evil.example");
    let store = Memory::new(Some(expired()));
    assert!(
        !logout(&store, Some(&mock.issuer), store.load().unwrap(), false)
            .await
            .unwrap()
    );
    assert!(store.load().unwrap().is_none());
    for form in mock.forms("/revoke") {
        assert_eq!(form["token"], "old-refresh");
        assert_eq!(form["client_id"], "issued-client");
        assert_eq!(form["token_type_hint"], "refresh_token");
    }
    let store = Memory::new(Some(expired()));
    let before = store.load_registration().unwrap().unwrap();
    assert!(
        !logout(&store, None, store.load().unwrap(), false)
            .await
            .unwrap()
    );
    assert!(store.load().unwrap().is_none());
    let retained = store.load_registration().unwrap().unwrap();
    assert_eq!(retained.client_id, before.client_id);
    mock.state.lock().unwrap().metadata["issuer"] = json!(mock.issuer);
    let reauth = mock
        .provider()
        .await
        .authorize(
            &retained.client_id,
            "stable-host",
            "http://localhost:1455/auth/callback",
        )
        .unwrap();
    let query: std::collections::BTreeMap<_, _> = reauth.url.query_pairs().into_owned().collect();
    assert_eq!(query["client_id"], retained.client_id);
    assert!(!query.contains_key("agent_name_hint"));
    assert!(!logout(&store, None, None, true).await.unwrap());
    assert!(store.load().unwrap().is_none());
    assert!(store.load_registration().unwrap().is_none());
}

#[tokio::test]
async fn bounded_http_refuses_oversized_streams_redirects_and_foreign_origins() {
    let mock = Mock::new().await;
    let provider = mock.provider().await;
    for raw in [
        "HTTP/1.1 200 OK\r\nContent-Length: 300000\r\nConnection: close\r\n\r\n".to_owned(),
        format!("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{}", "x".repeat(300000)),
        "HTTP/1.1 302 Found\r\nLocation: https://evil.example/private-provider-text\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned(),
    ] {
        mock.state.lock().unwrap().raw_token = Some(raw);
        redacted(provider.refresh("issued-client", "old-refresh").await.err().unwrap());
    }
    let request = openidconnect::http::Request::builder()
        .uri("https://evil.example/test-refresh")
        .body(Vec::new())
        .unwrap();
    redacted(provider.http.send(request).await.err().unwrap().into());
    mock.state.lock().unwrap().metadata["issuer"] = json!("https://evil.example");
    redacted(Provider::discover(&mock.issuer).await.err().unwrap());
}

#[test]
fn credential_record_roundtrips_within_the_keyring_payload_bound() {
    let credential = expired();
    let payload = serde_json::to_vec(&credential).unwrap();
    assert!(payload.len() <= CREDENTIAL_LIMIT);
    let restored: Credential = serde_json::from_slice(&payload).unwrap();
    assert_eq!(restored.client_id, credential.client_id);
    assert_eq!(restored.subject, credential.subject);
    assert_eq!(restored.nonce, credential.nonce);
    assert_eq!(restored.access, credential.access);
    assert_eq!(restored.refresh, credential.refresh);
    assert_eq!(restored.expires, credential.expires);
    assert_eq!(restored.pending, credential.pending);
    let registration = Registration {
        client_id: credential.client_id,
        subject: credential.subject,
    };
    let payload = serde_json::to_vec(&registration).unwrap();
    let restored: Registration = serde_json::from_slice(&payload).unwrap();
    assert_eq!(restored.client_id, registration.client_id);
    assert_eq!(restored.subject, registration.subject);
}

#[test]
fn cli_requires_explicit_account_and_keeps_credentials_out_of_generation() {
    use crate::cli::args::Cli;
    use clap::Parser;
    for command in ["login", "status", "logout"] {
        assert!(Cli::try_parse_from(["qcg", "auth", command]).is_err());
        assert!(Cli::try_parse_from(["qcg", "auth", command, "--account", "work"]).is_ok());
    }
    assert!(
        Cli::try_parse_from(["qcg", "auth", "logout", "--account", "work", "--forget"]).is_ok()
    );
    assert_ne!(account_key("one").unwrap(), account_key("two").unwrap());
    assert!(account_key("").is_err());
    assert!(
        !account_key("private-label")
            .unwrap()
            .contains("private-label")
    );
}

// The ignored entry is a bounded subprocess helper, not an external/real-account test.
#[tokio::test]
#[ignore]
async fn lock_child() {
    let path = PathBuf::from(std::env::var_os("QCG_TEST_AUTH_LOCK").unwrap());
    let lock_path = path.join(format!("{}.lock", hex::encode(Sha256::digest(b"account"))));
    let probe = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    assert!(matches!(
        probe.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    std::fs::write(path.join("blocked"), b"ready").unwrap();
    drop(probe);
    let _lock = lock(&path, "account").await.unwrap();
    std::fs::write(path.join("acquired"), b"ready").unwrap();
}
#[tokio::test]
async fn lock_is_cross_process_and_released_on_close() {
    let dir = std::env::temp_dir().join(format!("qcg-auth-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&dir).unwrap();
    let held = lock(&dir, "account").await.unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "cli::auth::tests::lock_child", "--ignored"])
        .env("QCG_TEST_AUTH_LOCK", &dir)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !dir.join("blocked").exists() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!dir.join("acquired").exists());
    drop(held);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(dir.join("acquired").exists());
    std::fs::remove_dir_all(dir).unwrap();
}
