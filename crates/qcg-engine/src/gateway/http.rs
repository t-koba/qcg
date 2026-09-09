use qcg_contract::Permissions;
use qcg_policy::credential_like_name;
use reqwest::{Client, Method, redirect::Policy};
use std::collections::BTreeMap;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::error::GatewayError;

#[derive(Debug, Clone)]
pub struct HttpGateway {
    permissions: Permissions,
    client: Client,
    timeout: Duration,
    body_limit_bytes: Option<usize>,
    redirect_limit: Option<usize>,
    cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    /// Query parameters containing credentials. They are appended only after
    /// permission checks and removed from all returned URLs and errors.
    pub sensitive_query: BTreeMap<String, String>,
    pub body: Option<Vec<u8>>,
    pub follow_redirects: bool,
    /// Stable operation id for remote deduplication. Sent as
    /// `Idempotency-Key` when present; receivers without support ignore it.
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HttpOutput {
    pub status: u16,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
}

impl HttpGateway {
    pub fn new(
        permissions: Permissions,
        timeout: Duration,
        body_limit_bytes: Option<usize>,
        redirect_limit: Option<usize>,
    ) -> Result<Self, GatewayError> {
        let client = Client::builder().redirect(Policy::none()).build()?;
        Ok(Self {
            permissions,
            client,
            timeout,
            body_limit_bytes,
            redirect_limit,
            cancellation: CancellationToken::new(),
        })
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub async fn request(&self, request: HttpRequest) -> Result<HttpOutput, GatewayError> {
        if request.follow_redirects && !request.sensitive_query.is_empty() {
            return Err(GatewayError::UnsupportedUrl {
                url: "requests with sensitive query parameters cannot follow redirects".into(),
            });
        }
        if request.follow_redirects
            && request
                .headers
                .keys()
                .any(|name| credential_like_name(name))
        {
            return Err(GatewayError::UnsupportedUrl {
                url: "requests with credential headers cannot follow redirects".into(),
            });
        }
        if request.body.as_ref().is_some_and(|body| {
            self.body_limit_bytes
                .is_some_and(|limit| body.len() > limit)
        }) {
            return Err(GatewayError::HttpRequestBodyTooLarge {
                url: request.url.clone(),
            });
        }
        let mut url = request.url.clone();
        for _ in 0..=self.redirect_limit.unwrap_or(usize::MAX) {
            ensure_url_allowed(&self.permissions, &url)?;
            let method =
                request
                    .method
                    .parse::<Method>()
                    .map_err(|_| GatewayError::UnsupportedUrl {
                        url: request.method.clone(),
                    })?;
            let mut request_url =
                Url::parse(&url).map_err(|_| GatewayError::UnsupportedUrl { url: url.clone() })?;
            if !request.sensitive_query.is_empty() {
                let mut pairs = request_url.query_pairs_mut();
                for (key, value) in &request.sensitive_query {
                    pairs.append_pair(key, value);
                }
            }
            let mut builder = self
                .client
                .request(method, request_url)
                .timeout(self.timeout);
            for (key, value) in &request.headers {
                builder = builder.header(key, value);
            }
            if let Some(key) = request
                .idempotency_key
                .as_deref()
                .filter(|key| !key.is_empty())
            {
                builder = builder.header("Idempotency-Key", key);
            }
            if let Some(body) = &request.body {
                builder = builder.body(body.clone());
            }
            let mut response = tokio::select! {
                _ = self.cancellation.cancelled() => return Err(GatewayError::Canceled),
                response = builder.send() => response.map_err(|error| GatewayError::Http(error.without_url()))?,
            };
            let status = response.status();
            if request.follow_redirects
                && status.is_redirection()
                && let Some(location) = response.headers().get(reqwest::header::LOCATION)
            {
                let location = location
                    .to_str()
                    .map_err(|_| GatewayError::UnsupportedUrl { url: url.clone() })?;
                url = resolve_redirect(&url, location)?;
                continue;
            }
            let final_url = response.url().to_string();
            ensure_url_allowed(&self.permissions, &final_url)?;
            let public_final_url = redact_query_parameters(&final_url, &request.sensitive_query)?;
            let headers = response
                .headers()
                .iter()
                .filter_map(|(key, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (key.as_str().to_string(), value.to_string()))
                })
                .collect();
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let initial_capacity = response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or_default()
                .min(self.body_limit_bytes.unwrap_or(1024 * 1024));
            let mut body = Vec::with_capacity(initial_capacity);
            loop {
                let chunk = tokio::select! {
                    _ = self.cancellation.cancelled() => return Err(GatewayError::Canceled),
                    chunk = response.chunk() => chunk.map_err(|error| GatewayError::Http(error.without_url()))?,
                };
                let Some(chunk) = chunk else {
                    break;
                };
                if self
                    .body_limit_bytes
                    .is_some_and(|limit| body.len().saturating_add(chunk.len()) > limit)
                {
                    return Err(GatewayError::HttpBodyTooLarge {
                        url: public_final_url.clone(),
                    });
                }
                body.extend_from_slice(&chunk);
            }
            return Ok(HttpOutput {
                status: status.as_u16(),
                url: public_final_url,
                headers,
                body,
                content_type,
            });
        }
        Err(GatewayError::UnsupportedUrl { url })
    }
}

pub(crate) fn redact_query_parameters(
    value: &str,
    sensitive: &BTreeMap<String, String>,
) -> Result<String, GatewayError> {
    if sensitive.is_empty() {
        return Ok(value.to_string());
    }
    let mut url = Url::parse(value).map_err(|_| GatewayError::UnsupportedUrl {
        url: "HTTP response returned an invalid final URL".into(),
    })?;
    let retained = url
        .query_pairs()
        .filter(|(key, _)| !sensitive.contains_key(key.as_ref()))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    if !retained.is_empty() {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in retained {
            pairs.append_pair(&key, &value);
        }
    }
    Ok(url.to_string())
}

pub(crate) fn ensure_url_allowed(permissions: &Permissions, url: &str) -> Result<(), GatewayError> {
    let parsed = Url::parse(url).map_err(|_| GatewayError::UnsupportedUrl {
        url: url.to_string(),
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(GatewayError::UnsupportedUrl {
            url: url.to_string(),
        });
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| GatewayError::UnsupportedUrl {
            url: url.to_string(),
        })?
        .to_string();
    let allowed = permissions.network.iter().any(|entry| {
        entry == "*"
            || entry == &host
            || Url::parse(entry)
                .ok()
                .and_then(|url| url.host_str().map(str::to_string))
                .as_deref()
                == Some(host.as_str())
    });
    if allowed {
        Ok(())
    } else {
        Err(GatewayError::NetworkDenied { host })
    }
}

fn resolve_redirect(base: &str, location: &str) -> Result<String, GatewayError> {
    let base = Url::parse(base).map_err(|_| GatewayError::UnsupportedUrl {
        url: base.to_string(),
    })?;
    base.join(location)
        .map(|url| url.to_string())
        .map_err(|_| GatewayError::UnsupportedUrl {
            url: location.to_string(),
        })
}
