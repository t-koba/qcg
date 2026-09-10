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
    /// Follow redirects for safe methods (GET/HEAD). Side-effect methods
    /// never auto-follow: the 3xx response is returned to the caller so an
    /// already-sent effect is never replayed at a new target.
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
        let mut sent_effect = false;
        match self.request_redirected(request, &mut sent_effect).await {
            Err(error) if sent_effect && !matches!(error, GatewayError::Canceled) => {
                Err(GatewayError::AfterSend(Box::new(error)))
            }
            result => result,
        }
    }

    /// Executes the request, following safe-method redirects up to the
    /// limit. `sent_effect` records that a side-effect-bearing request was
    /// already dispatched, so the caller can tag later failures with their
    /// send history (D02).
    async fn request_redirected(
        &self,
        request: HttpRequest,
        sent_effect: &mut bool,
    ) -> Result<HttpOutput, GatewayError> {
        let mut url = request.url.clone();
        let mut method = request.method.clone();
        let mut body = request.body.clone();
        for _ in 0..=self.redirect_limit.unwrap_or(usize::MAX) {
            ensure_url_allowed(&self.permissions, &url)?;
            let parsed_method =
                method
                    .parse::<Method>()
                    .map_err(|_| GatewayError::UnsupportedUrl {
                        url: method.clone(),
                    })?;
            let safe_method = matches!(parsed_method, Method::GET | Method::HEAD);
            let head_method = parsed_method == Method::HEAD;
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
                .request(parsed_method, request_url)
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
            if let Some(body) = &body {
                builder = builder.body(body.clone());
            }
            let mut response = tokio::select! {
                _ = self.cancellation.cancelled() => return Err(GatewayError::Canceled),
                response = builder.send() => response.map_err(|error| GatewayError::Http(error.without_url()))?,
            };
            let status = response.status();
            if !safe_method {
                // The request has been dispatched: any later failure in
                // this operation can no longer prove non-application.
                *sent_effect = true;
            }
            // Only safe methods follow redirects. A side-effect request
            // (POST/PUT/...) that is replayed against the Location target
            // would duplicate the first request's effect, and a later
            // failure could not be classified as "nothing was sent"
            // (D02). The 3xx response is returned to the caller instead.
            if request.follow_redirects
                && safe_method
                && status.is_redirection()
                && let Some(location) = response.headers().get(reqwest::header::LOCATION)
            {
                let location = location
                    .to_str()
                    .map_err(|_| GatewayError::UnsupportedUrl { url: url.clone() })?;
                // RFC 9110 semantics: 303 retrieves the target
                // representation (GET; HEAD remains a fitting retrieval
                // method) and drops the body. 307/308 preserve the safe
                // method and body. 301/302 keep the safe method as sent.
                if status == reqwest::StatusCode::SEE_OTHER {
                    if !head_method {
                        method = "GET".to_string();
                    }
                    body = None;
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    async fn read_request(stream: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).await.expect("request must read");
            assert!(read > 0, "client closed before sending headers");
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buffer[..end + 4]).to_string();
                let length = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                let mut body = buffer[end + 4..].to_vec();
                while body.len() < length {
                    let read = stream.read(&mut chunk).await.expect("body must read");
                    assert!(read > 0, "client closed before sending the body");
                    body.extend_from_slice(&chunk[..read]);
                }
                return (head, body);
            }
        }
    }

    /// Serves exactly one request with `response` and returns its port plus
    /// the handle resolving to the received head and body.
    async fn serve_once(response: String) -> (u16, tokio::task::JoinHandle<(String, Vec<u8>)>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let port = listener
            .local_addr()
            .expect("listener should have an address")
            .port();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("client should connect");
            let (head, body) = read_request(&mut stream).await;
            stream
                .write_all(response.as_bytes())
                .await
                .expect("response should write");
            let _ = stream.shutdown().await;
            (head, body)
        });
        (port, handle)
    }

    fn gateway() -> HttpGateway {
        HttpGateway::new(
            Permissions {
                network: vec!["127.0.0.1".into()],
                ..Permissions::default()
            },
            Duration::from_secs(10),
            None,
            Some(4),
        )
        .expect("test gateway should build")
    }

    fn request(method: &str, url: String, body: Option<Vec<u8>>) -> HttpRequest {
        HttpRequest {
            method: method.into(),
            url,
            headers: BTreeMap::new(),
            sensitive_query: BTreeMap::new(),
            body,
            follow_redirects: true,
            idempotency_key: None,
        }
    }

    #[tokio::test]
    async fn side_effect_redirects_are_returned_never_replayed() {
        // D02: the POST reached the first server and applied its effect,
        // then a 307 pointed at a second host. Replaying the POST at the
        // target would duplicate the effect, so the 3xx must be returned
        // as the final response and the target must see nothing.
        let (target_port, target_task) = serve_once(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".into(),
        )
        .await;
        let (redirect_port, redirect_task) = serve_once(format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:{target_port}/next\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        ))
        .await;
        let output = gateway()
            .request(request(
                "POST",
                format!("http://127.0.0.1:{redirect_port}/apply"),
                Some(b"apply".to_vec()),
            ))
            .await
            .expect("a redirect response is final for side-effect methods");
        assert_eq!(output.status, 307);
        let (_head, body) = redirect_task.await.expect("redirect server should finish");
        assert_eq!(body, b"apply", "the first server received the effect once");
        // A wrong follow would have completed the target server before
        // `request` returned; a pending task proves it was never contacted.
        assert!(
            !target_task.is_finished(),
            "the POST must not be replayed at the redirect target"
        );
        target_task.abort();
    }

    #[tokio::test]
    async fn see_other_follows_safe_methods_without_replaying_the_body() {
        // D02: 303 means "retrieve the target representation", so the
        // followed request must be a body-less GET (HEAD stays HEAD), not
        // a replay of the original method and body.
        let (target_port, target_task) = serve_once(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".into(),
        )
        .await;
        let (redirect_port, redirect_task) = serve_once(format!(
            "HTTP/1.1 303 See Other\r\nLocation: http://127.0.0.1:{target_port}/next\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        ))
        .await;
        let output = gateway()
            .request(request(
                "GET",
                format!("http://127.0.0.1:{redirect_port}/start"),
                Some(b"payload".to_vec()),
            ))
            .await
            .expect("safe redirect should follow");
        assert_eq!(output.status, 200);
        let (head, body) = target_task.await.expect("target server should finish");
        assert!(
            head.starts_with("GET /next"),
            "303 must retrieve with GET: {head}"
        );
        assert!(body.is_empty(), "303 must not replay the original body");
        assert!(
            !head.to_ascii_lowercase().contains("content-length: 7"),
            "the followed request must not declare the dropped body: {head}"
        );
        redirect_task.await.expect("redirect server should finish");
    }

    /// Serves headers promising 10 bytes and then closes after 2, so the
    /// client fails while reading the body after a successful dispatch.
    async fn serve_truncated() -> (u16, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback listener should bind");
        let port = listener
            .local_addr()
            .expect("listener should have an address")
            .port();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("client should connect");
            let (_head, _body) = read_request(&mut stream).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nhe")
                .await
                .expect("response should write");
            let _ = stream.shutdown().await;
        });
        (port, handle)
    }

    #[tokio::test]
    async fn post_send_failures_keep_the_send_history() {
        // D02: a POST was dispatched, then the response body read failed.
        // The error must retain the send history so classification cannot
        // fall back to "nothing was sent". A safe request carries no such
        // history.
        use crate::OperationOutcome;
        let (post_port, post_server) = serve_truncated().await;
        let error = gateway()
            .request(request(
                "POST",
                format!("http://127.0.0.1:{post_port}/apply"),
                Some(b"apply".to_vec()),
            ))
            .await
            .expect_err("truncated body must fail");
        post_server.await.expect("server should finish");
        assert!(
            matches!(error, GatewayError::AfterSend(_)),
            "post-send failures must keep the send history, got: {error}"
        );
        assert!(
            matches!(
                OperationOutcome::gateway_error(&error, false),
                OperationOutcome::Indeterminate { .. }
            ),
            "send history must classify as indeterminate"
        );
        let (get_port, get_server) = serve_truncated().await;
        let error = gateway()
            .request(request(
                "GET",
                format!("http://127.0.0.1:{get_port}/fetch"),
                None,
            ))
            .await
            .expect_err("truncated body must fail");
        get_server.await.expect("server should finish");
        assert!(
            !matches!(error, GatewayError::AfterSend(_)),
            "safe requests have no side-effect history, got: {error}"
        );
    }
}
