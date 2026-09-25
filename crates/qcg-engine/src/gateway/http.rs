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
                // Never echo the raw URL: query values are redacted (E09).
                url: qcg_policy::redact_all_query_values(&request.url),
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
        // An omitted redirect cap means redirects are followed without a
        // numeric bound; the bound, when set, counts every dispatch.
        let mut redirects = 0_usize;
        loop {
            if let Some(limit) = self.redirect_limit
                && redirects > limit
            {
                return Err(GatewayError::UnsupportedUrl {
                    url: format!("redirects exceed the limit of {limit}"),
                });
            }
            redirects = redirects.saturating_add(1);
            ensure_url_allowed(&self.permissions, &url)?;
            let parsed_method =
                method
                    .parse::<Method>()
                    .map_err(|_| GatewayError::UnsupportedUrl {
                        url: method.clone(),
                    })?;
            let safe_method = matches!(parsed_method, Method::GET | Method::HEAD);
            let head_method = parsed_method == Method::HEAD;
            let mut request_url = Url::parse(&url).map_err(|_| GatewayError::UnsupportedUrl {
                url: error_url(&url, &request.sensitive_query),
            })?;
            if !request.sensitive_query.is_empty() {
                // Append only parameters the URL does not already carry:
                // callers that build the full URL pass the sensitive values
                // for redaction, not for duplication (E09).
                let present = request_url
                    .query_pairs()
                    .map(|(key, _)| key.into_owned())
                    .collect::<std::collections::BTreeSet<_>>();
                let mut pairs = request_url.query_pairs_mut();
                for (key, value) in &request.sensitive_query {
                    if !present.contains(key) {
                        pairs.append_pair(key, value);
                    }
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
                    .map_err(|_| GatewayError::UnsupportedUrl {
                        url: error_url(&url, &request.sensitive_query),
                    })?;
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
                // Remote-controlled response headers: non-UTF8 values are
                // display-only and skipped, never fail the run. Request
                // headers are BTreeMap<String, String> and already validated
                // at construction; response parsing must not confuse remote
                // damage with local failure (E09).
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
            // Every path through the read loop returns: the redirect
            // budget above is the only loop exit.
            return Ok(HttpOutput {
                status: status.as_u16(),
                url: public_final_url,
                headers,
                body,
                content_type,
            });
        }
    }
}

/// Best-effort redaction for error messages: declared sensitive values
/// are removed, and when the URL cannot even be parsed every query value,
/// userinfo, and fragment value is redacted rather than leaking a possible
/// credential (E09).
fn error_url(url: &str, sensitive: &BTreeMap<String, String>) -> String {
    redact_query_parameters(url, sensitive)
        .unwrap_or_else(|_| qcg_policy::redact_all_query_values(url))
}

/// Extracts declared sensitive query values from a URL for redaction and
/// approval binding. Empty input stays empty; anything else that cannot be
/// parsed or names an absent parameter fails closed so a typo cannot leak a
/// credential into the journal (E09).
pub fn sensitive_query_values(
    url: &str,
    names: &[String],
) -> Result<BTreeMap<String, String>, GatewayError> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }
    let parsed = Url::parse(url).map_err(|_| GatewayError::UnsupportedUrl {
        url: error_url(url, &BTreeMap::new()),
    })?;
    let mut values = BTreeMap::new();
    for name in names {
        let value = parsed
            .query_pairs()
            .find(|(key, _)| key == name.as_str())
            .map(|(_, value)| value.into_owned())
            .ok_or_else(|| GatewayError::UnsupportedUrl {
                url: format!("sensitive_query parameter `{name}` is not present in the url"),
            })?;
        values.insert(name.clone(), value);
    }
    Ok(values)
}

pub fn redact_query_parameters(
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

/// Placeholder that replaces secret values in journaled copies. The
/// placeholder preserves the parameter name so a redacted resume is
/// detectable and fails closed instead of sending a wrong credential
/// (E09). This is the single definition: hash placeholders and the
/// resume-time detector below all derive from it.
pub const REDACTED_PLACEHOLDER: &str = "[REDACTED]";

/// Hash placeholder for redacted blobs (bodies, file contents): the digest
/// binds the exact bytes for approval identity without journaling them.
pub fn redacted_hash_placeholder(digest_hex: &str) -> String {
    format!("[REDACTED:sha256:{digest_hex}]")
}

/// Reports whether serialized journaled args carry a redaction
/// placeholder in any of its shapes (raw or URL percent-encoded). Matching
/// is case-insensitive over the anchored `[REDACTED` prefix shared by every
/// constructor above, so legitimate content merely containing the word
/// `REDACTED` is not misdetected while `%5BrEdAcTeD`-style mixed encodings
/// still match (E07/E09).
pub fn contains_redaction_marker(serialized_args: &str) -> bool {
    let upper = serialized_args.to_ascii_uppercase();
    upper.contains("[REDACTED") || upper.contains("%5BREDACTED")
}

/// Parses a `sensitive_query` declaration (`None` or a JSON array of
/// strings) into names. A single shared parser so the plain HTTP step and
/// the agent HTTP tool cannot disagree on percent-encoding or shape (E09).
pub fn sensitive_names_from_decl(
    declared: Option<&serde_json::Value>,
) -> Result<Vec<String>, GatewayError> {
    let Some(declared) = declared else {
        return Ok(Vec::new());
    };
    let names = declared
        .as_array()
        .ok_or_else(|| GatewayError::UnsupportedUrl {
            url: "sensitive_query must be an array of strings".into(),
        })?;
    names
        .iter()
        .map(|name| {
            name.as_str()
                .map(str::to_string)
                .ok_or_else(|| GatewayError::UnsupportedUrl {
                    url: "sensitive_query entries must be strings".into(),
                })
        })
        .collect()
}

/// Shared extraction for plain HTTP steps (`&[String]`) and agent HTTP
/// tools (`Option<&Value>`): one parser, one percent-encoding behavior
/// (E09).
pub fn sensitive_query_values_from_decl(
    url: &str,
    declared: Option<&serde_json::Value>,
) -> Result<BTreeMap<String, String>, GatewayError> {
    let names = sensitive_names_from_decl(declared)?;
    sensitive_query_values(url, &names)
}

/// Redacts an agent or step HTTP `args` object for journaling: sensitive
/// query values in `url` become `[REDACTED]` (names preserved for
/// fail-closed detection), credential-like header values become
/// `[REDACTED]`, and request bodies become a hash placeholder so plaintext
/// request content never reaches the journal (E09). A malformed
/// `sensitive_query` declaration strips the query instead of leaving the
/// raw URL: execution fails closed on the raw copy, and the journal never
/// carries the unredacted form.
pub fn redact_http_args_for_journal(args: &serde_json::Value) -> serde_json::Value {
    let mut redacted = args.clone();
    let Some(object) = redacted.as_object_mut() else {
        return redacted;
    };
    if let Some(url_value) = object
        .get("url")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
    {
        let declared = object.get("sensitive_query");
        // No implicit fallback: a malformed declaration is not treated as
        // "no secrets". Strip the query fail-closed (E09).
        let names = match sensitive_names_from_decl(declared) {
            Ok(names) => names,
            Err(_) => {
                object.insert(
                    "url".into(),
                    serde_json::Value::String(
                        url_value
                            .split('?')
                            .next()
                            .unwrap_or(REDACTED_PLACEHOLDER)
                            .to_string(),
                    ),
                );
                Vec::new()
            }
        };
        if !names.is_empty() {
            match sensitive_query_values(&url_value, &names) {
                Ok(sensitive) => {
                    if let Ok(url) = redact_url_preserving_keys(&url_value, &sensitive) {
                        object.insert("url".into(), serde_json::Value::String(url));
                    } else {
                        object.insert(
                            "url".into(),
                            serde_json::Value::String(
                                url_value
                                    .split('?')
                                    .next()
                                    .unwrap_or(REDACTED_PLACEHOLDER)
                                    .to_string(),
                            ),
                        );
                    }
                }
                Err(_) => {
                    // Fail closed: a typo or absent name must not leave the
                    // raw query in the journal.
                    object.insert(
                        "url".into(),
                        serde_json::Value::String(
                            url_value
                                .split('?')
                                .next()
                                .unwrap_or(REDACTED_PLACEHOLDER)
                                .to_string(),
                        ),
                    );
                }
            }
        }
    }
    if let Some(headers) = object
        .get_mut("headers")
        .and_then(|value| value.as_object_mut())
    {
        for (key, value) in headers.iter_mut() {
            if credential_like_name(key)
                && let Some(_old) = value.as_str()
            {
                *value = serde_json::Value::String(REDACTED_PLACEHOLDER.into());
            }
        }
    }
    // Request bodies may carry credentials or PII: journal only a hash
    // placeholder, never the plaintext. Execution uses the raw body; a
    // redacted resume returns cached success or fails closed (E09).
    if let Some(body) = object.get("body").and_then(|value| value.as_str()) {
        use sha2::{Digest as _, Sha256};
        let digest = hex::encode(Sha256::digest(body.as_bytes()));
        object.insert(
            "body".into(),
            serde_json::Value::String(redacted_hash_placeholder(&digest)),
        );
    }
    redacted
}

/// Redacts an `fs.write` tool `args` object for journaling: the file
/// content becomes a hash placeholder so plaintext file bytes never reach
/// the journal (E09). Execution uses the raw content; a redacted resume
/// returns cached success or fails closed.
pub fn redact_fs_write_args_for_journal(args: &serde_json::Value) -> serde_json::Value {
    let mut redacted = args.clone();
    let Some(object) = redacted.as_object_mut() else {
        return redacted;
    };
    if let Some(content) = object.get("content").and_then(|value| value.as_str()) {
        use sha2::{Digest as _, Sha256};
        let digest = hex::encode(Sha256::digest(content.as_bytes()));
        object.insert(
            "content".into(),
            serde_json::Value::String(redacted_hash_placeholder(&digest)),
        );
    }
    redacted
}

/// Redacts an `fs.patch` tool `args` object for journaling: every
/// replacement line becomes a hash placeholder while `path`, `op`
/// ordering, and anchors stay visible for review. Execution uses the
/// raw lines; a redacted resume returns cached success or fails closed.
pub fn redact_fs_patch_args_for_journal(args: &serde_json::Value) -> serde_json::Value {
    use sha2::{Digest as _, Sha256};
    let mut redacted = args.clone();
    let Some(object) = redacted.as_object_mut() else {
        return redacted;
    };
    let Some(edits) = object
        .get_mut("edits")
        .and_then(|value| value.as_array_mut())
    else {
        return redacted;
    };
    for edit in edits.iter_mut() {
        let Some(item) = edit.as_object_mut() else {
            continue;
        };
        let Some(lines) = item.get("lines").and_then(|value| value.as_array()) else {
            continue;
        };
        let canonical = serde_json::to_vec(lines).unwrap_or_default();
        let digest = hex::encode(Sha256::digest(&canonical));
        item.insert(
            "lines".into(),
            serde_json::Value::String(redacted_hash_placeholder(&digest)),
        );
    }
    redacted
}

/// Redacts sensitive query values but keeps their names with a placeholder
/// so journaled copies never carry plaintext yet remain detectable (E09).
fn redact_url_preserving_keys(
    url: &str,
    sensitive: &BTreeMap<String, String>,
) -> Result<String, GatewayError> {
    if sensitive.is_empty() {
        return Ok(url.to_string());
    }
    let mut parsed = Url::parse(url).map_err(|_| GatewayError::UnsupportedUrl {
        url: "HTTP URL is invalid".into(),
    })?;
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (key, value) in parsed.query_pairs() {
        let key = key.into_owned();
        let value = value.into_owned();
        if sensitive.contains_key(&key) {
            pairs.push((key, REDACTED_PLACEHOLDER.to_string()));
        } else {
            pairs.push((key, value));
        }
    }
    parsed.set_query(None);
    if !pairs.is_empty() {
        let mut query = parsed.query_pairs_mut();
        for (key, value) in pairs {
            query.append_pair(&key, &value);
        }
    }
    Ok(parsed.to_string())
}

/// Recursively redacts credential-like object values for journaling (MCP
/// arguments and generic tool args). String values under a
/// credential-like key become `[REDACTED]`, including strings nested in
/// arrays under such a key (e.g. `{"api_key": ["s1", "s2"]}`) (E09).
pub fn redact_credential_values(value: &serde_json::Value) -> serde_json::Value {
    fn redact(value: &serde_json::Value, parent_credential: bool) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut redacted = serde_json::Map::with_capacity(map.len());
                for (key, val) in map {
                    if credential_like_name(key) && val.as_str().is_some() {
                        redacted.insert(
                            key.clone(),
                            serde_json::Value::String(REDACTED_PLACEHOLDER.into()),
                        );
                    } else {
                        redacted.insert(
                            key.clone(),
                            redact(val, parent_credential || credential_like_name(key)),
                        );
                    }
                }
                serde_json::Value::Object(redacted)
            }
            serde_json::Value::Array(items) => serde_json::Value::Array(
                items
                    .iter()
                    .map(|item| redact(item, parent_credential))
                    .collect(),
            ),
            serde_json::Value::String(text) if parent_credential => {
                let _ = text;
                serde_json::Value::String(REDACTED_PLACEHOLDER.into())
            }
            _ => value.clone(),
        }
    }
    redact(value, false)
}

pub(crate) fn ensure_url_allowed(permissions: &Permissions, url: &str) -> Result<(), GatewayError> {
    let redacted = qcg_policy::redact_all_query_values(url);
    let parsed = Url::parse(url).map_err(|_| GatewayError::UnsupportedUrl {
        url: redacted.clone(),
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(GatewayError::UnsupportedUrl { url: redacted });
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| GatewayError::UnsupportedUrl {
            url: redacted.clone(),
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
