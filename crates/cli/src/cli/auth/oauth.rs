//! OAuth/OIDC policy is private to the CLI; the keyring stores only opaque bytes.
use anyhow::{Result, bail};
use openidconnect::{
    AccessTokenHash, AuthorizationCode, ClientId, CsrfToken, HttpRequest, HttpResponse, IssuerUrl,
    Nonce, NonceVerifier, OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl,
    RefreshToken, Scope, TokenResponse,
    core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata, CoreTokenResponse},
};
use serde::Deserialize;
use std::{io, time::Duration};
use url::Url;

pub(super) const ISSUER: &str = "https://auth.openai.com";
pub(super) const RESOURCE: &str = "https://api.openai.com/v1";
const BODY_LIMIT: usize = 256 * 1024;

fn refused() -> io::Error {
    io::Error::other("authentication HTTP request failed (details redacted)")
}

pub(super) struct Http {
    client: reqwest::Client,
    origin: Url,
}
impl Http {
    pub(super) fn new(issuer: &str) -> Result<Self> {
        let origin = Url::parse(issuer)?;
        if origin.scheme() != "https" && !cfg!(test) {
            bail!("authentication requires HTTPS");
        }
        Ok(Self {
            origin,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|_| refused())?,
        })
    }
    pub(super) async fn send(&self, request: HttpRequest) -> io::Result<HttpResponse> {
        let url = Url::parse(&request.uri().to_string()).map_err(|_| refused())?;
        if url.origin() != self.origin.origin()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(refused());
        }
        let (parts, body) = request.into_parts();
        let mut response = self
            .client
            .request(parts.method, url)
            .headers(parts.headers)
            .body(body)
            .send()
            .await
            .map_err(|_| refused())?;
        if response
            .content_length()
            .is_some_and(|n| n > BODY_LIMIT as u64)
        {
            return Err(refused());
        }
        let mut result = openidconnect::http::Response::builder().status(response.status());
        *result.headers_mut().ok_or_else(refused)? = response.headers().clone();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| refused())? {
            if chunk.len() > BODY_LIMIT - bytes.len() {
                return Err(refused());
            }
            bytes.extend_from_slice(&chunk);
        }
        result.body(bytes).map_err(|_| refused())
    }
}

pub(super) struct Provider {
    pub(super) metadata: CoreProviderMetadata,
    pub(super) http: Http,
    revocation: Option<String>,
}
impl Provider {
    pub(super) async fn discover(issuer: &str) -> Result<Self> {
        let http = Http::new(issuer)?;
        // Keep the library's discovery issuer checks and JWK parsing. Capture only the
        // optional RFC 7009 endpoint, which CoreProviderMetadata otherwise ignores.
        let revocation = std::sync::Mutex::new(None);
        let fetch = |request: HttpRequest| {
            let http = &http;
            let revocation = &revocation;
            async move {
                let discovery = request
                    .uri()
                    .path()
                    .ends_with("/.well-known/openid-configuration");
                let response = http.send(request).await?;
                if discovery {
                    #[derive(Deserialize)]
                    struct Extra {
                        revocation_endpoint: Option<String>,
                    }
                    if let Ok(extra) = serde_json::from_slice::<Extra>(response.body()) {
                        *revocation.lock().map_err(|_| refused())? = extra.revocation_endpoint;
                    }
                }
                Ok::<_, io::Error>(response)
            }
        };
        let metadata =
            CoreProviderMetadata::discover_async(IssuerUrl::new(issuer.to_owned())?, &fetch)
                .await
                .map_err(|_| anyhow::anyhow!("OIDC discovery failed (details redacted)"))?;
        if metadata.authorization_endpoint().url().origin() != http.origin.origin()
            || !metadata
                .authorization_endpoint()
                .url()
                .username()
                .is_empty()
            || metadata.authorization_endpoint().url().password().is_some()
        {
            bail!("authorization endpoint refused");
        }
        Ok(Self {
            metadata,
            http,
            revocation: revocation.into_inner().map_err(|_| refused())?,
        })
    }
    pub(super) fn authorize(
        &self,
        client_id: &str,
        host: &str,
        redirect: &str,
    ) -> Result<Authorization> {
        let client = CoreClient::from_provider_metadata(
            self.metadata.clone(),
            ClientId::new(client_id.into()),
            None,
        )
        .set_redirect_uri(RedirectUrl::new(redirect.into())?);
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let builder = client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .add_scope(Scope::new("profile".into()))
            .add_scope(Scope::new("email".into()))
            .add_scope(Scope::new("offline_access".into()))
            .add_scope(Scope::new("resource.invoke".into()))
            .add_scope(Scope::new("chatgpt.tokens.use.direct".into()))
            .add_extra_param("resource", RESOURCE)
            .add_extra_param("ext_agent_host_id", host);
        let builder = if client_id == "dynamic_agent_client" {
            builder.add_extra_param("agent_name_hint", "qcg")
        } else {
            builder
        };
        let (url, state, nonce) = builder.set_pkce_challenge(challenge).url();
        Ok(Authorization {
            url,
            state,
            nonce,
            verifier,
        })
    }
    pub(super) async fn exchange(
        &self,
        client_id: &str,
        redirect: &str,
        code: String,
        verifier: PkceCodeVerifier,
    ) -> Result<CoreTokenResponse> {
        let client = CoreClient::from_provider_metadata(
            self.metadata.clone(),
            ClientId::new(client_id.into()),
            None,
        )
        .set_redirect_uri(RedirectUrl::new(redirect.into())?);
        client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(|_| anyhow::anyhow!("token endpoint unavailable"))?
            .set_pkce_verifier(verifier)
            .add_extra_param("resource", RESOURCE)
            .request_async(&|request| self.http.send(request))
            .await
            .map_err(|_| anyhow::anyhow!("code exchange failed (details redacted)"))
    }
    pub(super) async fn refresh(&self, client_id: &str, token: &str) -> Result<CoreTokenResponse> {
        let client = CoreClient::from_provider_metadata(
            self.metadata.clone(),
            ClientId::new(client_id.into()),
            None,
        );
        client
            .exchange_refresh_token(&RefreshToken::new(token.into()))
            .map_err(|_| anyhow::anyhow!("token endpoint unavailable"))?
            .add_extra_param("resource", RESOURCE)
            .request_async(&|request| self.http.send(request))
            .await
            .map_err(|_| anyhow::anyhow!("refresh failed; sign in again (details redacted)"))
    }
    pub(super) fn validate(
        &self,
        client_id: &str,
        response: &CoreTokenResponse,
        nonce: &Nonce,
        required: bool,
    ) -> Result<Option<String>> {
        let Some(token) = response.id_token() else {
            if required {
                bail!("ID token missing");
            }
            return Ok(None);
        };
        let client = CoreClient::from_provider_metadata(
            self.metadata.clone(),
            ClientId::new(client_id.into()),
            None,
        );
        let verifier = client.id_token_verifier();
        // The library verifies signature, issuer, audience, expiry and nonce; never
        // expose its errors, which can include attacker-controlled claims.
        let claims = token
            .claims(&verifier, |actual: Option<&Nonce>| {
                if actual.is_none() && !required {
                    Ok(())
                } else {
                    nonce.verify(actual)
                }
            })
            .map_err(|_| anyhow::anyhow!("ID token validation failed"))?;
        if let Some(expected) = claims.access_token_hash() {
            let binding_error = |_| anyhow::anyhow!("access token binding failed");
            let alg = token.signing_alg().map_err(binding_error)?;
            let key = token.signing_key(&verifier).map_err(binding_error)?;
            let actual = AccessTokenHash::from_token(response.access_token(), alg, key)
                .map_err(|_| anyhow::anyhow!("access token binding failed"))?;
            if &actual != expected {
                bail!("access token binding failed");
            }
        }
        Ok(Some(claims.subject().as_str().to_owned()))
    }
    pub(super) async fn revoke(&self, client_id: &str, token: &str) -> bool {
        let Some(endpoint) = &self.revocation else {
            return false;
        };
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("client_id", client_id),
                ("token", token),
                ("token_type_hint", "refresh_token"),
            ])
            .finish();
        let Ok(request) = openidconnect::http::Request::builder()
            .method("POST")
            .uri(endpoint)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body.into_bytes())
        else {
            return false;
        };
        self.http
            .send(request)
            .await
            .is_ok_and(|response| response.status().as_u16() == 200)
    }
}
pub(super) struct Authorization {
    pub(super) url: Url,
    pub(super) state: CsrfToken,
    pub(super) nonce: Nonce,
    pub(super) verifier: PkceCodeVerifier,
}
