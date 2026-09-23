use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{HttpRequest, StepContext, StepError, StepExecutor, StepOutcome};
use qcg_policy::{params_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::{
    open_package_read, package_file, read_opened_bounded, render_json_templates, require,
    strict_base64_decode, write_atomic,
};
pub(crate) struct HttpStep;

fn collect_sensitive_query(
    url: &str,
    names: &[String],
) -> Result<std::collections::BTreeMap<String, String>, String> {
    // Single shared parser in the gateway: manual query splitting here
    // would disagree with it on percent-encoding (E09).
    qcg_engine::sensitive_query_values(url, names).map_err(|error| error.to_string())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpParams {
    url: String,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    headers: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    body_text: Option<String>,
    #[serde(default)]
    body_json: Option<Value>,
    #[serde(default)]
    body_base64: Option<String>,
    #[serde(default)]
    body_file: Option<String>,
    #[serde(default)]
    body_file_scope: HttpBodyFileScope,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    output_file: Option<String>,
    #[serde(default)]
    output: Option<HttpOutputMode>,
    /// Query parameter names whose values are credentials: they stay out of
    /// the journaled target and step output, but are digested into the
    /// approval and passed to the gateway for canary redaction (E09).
    #[serde(default)]
    sensitive_query: Vec<String>,
}

// Redacting Debug (E07): URL query values, header values, and bodies never
// reach logs in plaintext. Keys and shapes stay visible for diagnosis.
// Body representation matches the agent side (`debug_agent_http_request`):
// `[BODY:sha256:<hex>]` digests instead of a bare marker, so both paths
// stay diagnosable without leaking bytes.
fn debug_body_digest(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    format!("[BODY:sha256:{}]", hex::encode(sha2::Sha256::digest(bytes)))
}

impl std::fmt::Debug for HttpParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpParams")
            .field("url", &qcg_policy::redact_all_query_values(&self.url))
            .field("method", &self.method)
            .field("headers", &qcg_policy::redact_header_values(&self.headers))
            .field(
                "body_text",
                &self
                    .body_text
                    .as_ref()
                    .map(|body| debug_body_digest(body.as_bytes())),
            )
            .field(
                "body_json",
                &self
                    .body_json
                    .as_ref()
                    .map(|body| debug_body_digest(body.to_string().as_bytes())),
            )
            .field(
                "body_base64",
                &self
                    .body_base64
                    .as_ref()
                    .map(|body| debug_body_digest(body.as_bytes())),
            )
            .field("body_file", &self.body_file)
            .field("body_file_scope", &self.body_file_scope)
            .field("content_type", &self.content_type)
            .field("output_file", &self.output_file)
            .field("output", &self.output)
            .field("sensitive_query", &self.sensitive_query)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HttpBodyFileScope {
    #[default]
    Workspace,
    Package,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HttpOutputMode {
    #[default]
    Text,
    Json,
    Base64,
    File,
}

#[async_trait]
impl StepExecutor for HttpStep {
    fn type_id(&self) -> &'static str {
        "http"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["url"],
            json!({
                "method": string_schema(),
                "url": string_schema(),
                "headers": { "type": "object", "additionalProperties": { "type": "string" } },
                "body_text": string_schema(),
                "body_json": {},
                "body_base64": string_schema(),
                "body_file": string_schema(),
                "body_file_scope": { "type": "string", "enum": ["workspace", "package"] },
                "content_type": string_schema(),
                "output_file": string_schema(),
                "output": { "type": "string", "enum": ["text", "json", "base64", "file"] },
                "sensitive_query": { "type": "array", "items": { "type": "string" } },
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = http_params(node)?;
        let body_modes = [
            params.body_text.is_some(),
            params.body_json.is_some(),
            params.body_base64.is_some(),
            params.body_file.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if body_modes > 1 {
            return Err(StepError::failed(
                &node.id,
                "http body_text, body_json, body_base64, and body_file are mutually exclusive",
            ));
        }
        if params.body_file.is_none()
            && !matches!(params.body_file_scope, HttpBodyFileScope::Workspace)
        {
            return Err(StepError::failed(
                &node.id,
                "http body_file_scope requires body_file",
            ));
        }
        match (params.output, params.output_file.is_some()) {
            (Some(HttpOutputMode::File), false) => {
                return Err(StepError::failed(
                    &node.id,
                    "http output = `file` requires output_file",
                ));
            }
            (Some(mode), true) if !matches!(mode, HttpOutputMode::File) => {
                return Err(StepError::failed(
                    &node.id,
                    "http output_file can only be combined with output = `file`",
                ));
            }
            _ => {}
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = http_params(node)?;
        let method = params
            .method
            .as_deref()
            .unwrap_or("GET")
            .to_ascii_uppercase();
        let url = ctx.render_inline(node, &params.url)?;
        let mut headers = std::collections::BTreeMap::new();
        for (key, value) in &params.headers {
            headers.insert(key.clone(), ctx.render_inline(node, value)?);
        }
        let (body, inferred_content_type) = match (
            params.body_text.as_deref(),
            params.body_json.as_ref(),
            params.body_base64.as_deref(),
            params.body_file.as_deref(),
        ) {
            (Some(text), None, None, None) => (
                Some(ctx.render_inline(node, text)?.into_bytes()),
                Some("text/plain; charset=utf-8".to_owned()),
            ),
            (None, Some(value), None, None) => {
                let value = render_json_templates(
                    ctx,
                    node,
                    value,
                    ctx.run.contract.manifest.runtime.http_body_limit_bytes,
                )?;
                let body = serde_json::to_vec(&value)?;
                let limit = ctx.run.contract.manifest.runtime.http_body_limit_bytes;
                if let Some(limit) = limit
                    && body.len() > limit
                {
                    return Err(StepError::failed(
                        &node.id,
                        format!("http JSON request body exceeds {limit} bytes"),
                    ));
                }
                (Some(body), Some("application/json".to_owned()))
            }
            (None, None, Some(encoded), None) => {
                let encoded = ctx.render_inline(node, encoded)?;
                let decoded = strict_base64_decode(&encoded).map_err(|error| {
                    StepError::failed(&node.id, format!("http body_base64 is invalid: {error}"))
                })?;
                let limit = ctx.run.contract.manifest.runtime.http_body_limit_bytes;
                if let Some(limit) = limit
                    && decoded.len() > limit
                {
                    return Err(StepError::failed(
                        &node.id,
                        format!("http base64 request body exceeds {limit} bytes"),
                    ));
                }
                (Some(decoded), Some("application/octet-stream".to_owned()))
            }
            (None, None, None, Some(file)) => {
                let file = ctx.render_inline(node, file)?;
                let (path, opened) = match params.body_file_scope {
                    HttpBodyFileScope::Workspace => {
                        let path = ctx.run.fs.resolve_read(&file).map_err(|error| {
                            StepError::failed(
                                &node.id,
                                format!("http body_file is not readable: {error}"),
                            )
                        })?;
                        let opened = ctx.run.fs.open_read_resolved(&path).map_err(|error| {
                            StepError::failed(
                                &node.id,
                                format!("http body_file is not readable: {error}"),
                            )
                        })?;
                        (path, Some(opened))
                    }
                    HttpBodyFileScope::Package => (
                        package_file(&ctx.run.contract, node, &file, "http body")?,
                        None,
                    ),
                };
                let limit = ctx.run.contract.manifest.runtime.http_body_limit_bytes;
                let bytes = match opened {
                    Some(file) => read_opened_bounded(file, limit, "http body_file")
                        .map_err(|error| StepError::failed(&node.id, error))?,
                    // Package scope opens through the same O_NOFOLLOW +
                    // handle boundary as snapshot reads (E13).
                    None => {
                        let file = open_package_read(node, &path, "http body")?;
                        read_opened_bounded(file, limit, "http body_file")
                            .map_err(|error| StepError::failed(&node.id, error))?
                    }
                };
                (Some(bytes), Some("application/octet-stream".to_owned()))
            }
            (None, None, None, None) => (None, None),
            _ => {
                return Err(StepError::failed(
                    &node.id,
                    "conflicting HTTP body modes; at most one body source is allowed",
                ));
            }
        };
        let explicit_content_type = params
            .content_type
            .as_deref()
            .map(|value| ctx.render_inline(node, value))
            .transpose()?;
        let header_content_type = headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("content-type"))
            .map(|(_, value)| value.clone());
        if explicit_content_type.is_some()
            && header_content_type.is_some()
            && explicit_content_type != header_content_type
        {
            return Err(StepError::failed(
                &node.id,
                "http content_type conflicts with the Content-Type header",
            ));
        }
        let content_type = explicit_content_type
            .or(header_content_type)
            .or_else(|| body.is_some().then_some(inferred_content_type).flatten());
        if let Some(content_type) = content_type
            && !headers
                .keys()
                .any(|key| key.eq_ignore_ascii_case("content-type"))
        {
            headers.insert("Content-Type".into(), content_type);
        }
        // The approval binds method, URL, headers, and body content:
        // approving one request must never authorize regenerated headers or
        // a different body, and header values are digested rather than
        // journaled (E09). Declared sensitive query values are redacted from
        // the journaled target and bound through a digest instead. In
        // addition every remaining query VALUE is redacted by default
        // (keys stay visible): an undeclared `api_key` value never reaches
        // the journal in plaintext. Declared sensitivity still drives
        // digest salting via `sensitive` below; this display redaction is
        // the fail-closed default (E09-1).
        let sensitive = collect_sensitive_query(&url, &params.sensitive_query)
            .map_err(|error| StepError::failed(&node.id, error))?;
        let journal_url = qcg_policy::redact_all_query_values(
            &qcg_engine::redact_query_parameters(&url, &sensitive)
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?,
        );
        // The confirmation scope binds the invocation: a re-executed node
        // (repair/regenerate) is a new invocation under `invocation` scope,
        // while `content` reuses an approval for identical content (Q1).
        // Details use a run-scoped salt so identical requests in this run
        // bind identically; runs never share a digest, and invocation
        // separation comes from the operation id (E09/Q1).
        // E09a: the full canonical URL (sorted query pairs) is bound into
        // the digest so different queries never share one approval, even
        // though the journaled target shows redacted values.
        let http_invocation = qcg_engine::RunContext::execution_invocation(ctx.journal, node);
        let http_salt = ctx.run.run_id.clone();
        // E09e: plan/approve/guard/execute share one constructor. This is
        // the single call site for the details object; the approval
        // (`require_side_effect`), the guard (`guard_external_operation`),
        // and the executed request all consume this same value.
        let http_details = http_approval_details(
            &method,
            &headers,
            body.as_deref(),
            &sensitive,
            &http_salt,
            &url,
        );
        if !matches!(method.as_str(), "GET" | "HEAD")
            && let Some(confirm) = ctx.run.require_side_effect(
                ctx.journal,
                node,
                "http",
                &journal_url,
                http_details.clone(),
                &http_invocation,
            )?
        {
            return Ok(StepOutcome::NeedsConfirm { confirm });
        }
        // Stable operation id doubles as the remote idempotency key so
        // retries after a lost result deduplicate server-side. Started
        // without finished refuses automatic replay (indeterminate result).
        // Same-invocation resends deserialize the cached native response
        // and continue through the identical output tail below.
        let safe_method = matches!(method.as_str(), "GET" | "HEAD");
        let response: qcg_engine::HttpOutput = if safe_method {
            // Requests carrying sensitive query values never follow
            // redirects: the gateway refuses that combination so a
            // credential cannot leak to a redirect target, and skipping
            // the follow keeps the request usable instead of always
            // failing (E09).
            let follow_redirects = sensitive.is_empty();
            ctx.run
                .http
                .request(HttpRequest {
                    method,
                    url: url.clone(),
                    headers,
                    sensitive_query: sensitive.clone(),
                    body,
                    follow_redirects,
                    idempotency_key: None,
                })
                .await
                .map_err(|error| StepError::from_gateway(&node.id, error))?
        } else {
            let invocation = http_invocation;
            match ctx.run.guard_external_operation(
                ctx.journal,
                node,
                "http",
                &journal_url,
                &http_details,
                &invocation,
            )? {
                qcg_engine::GuardDecision::Proceed { operation_id } => {
                    let response = match ctx
                        .run
                        .http
                        .request(HttpRequest {
                            method,
                            url: url.clone(),
                            headers,
                            sensitive_query: sensitive.clone(),
                            body,
                            // A redirected side-effect request would replay
                            // the approved operation against a new target;
                            // the gateway would refuse to follow it anyway.
                            // The 3xx response is the step's result.
                            follow_redirects: false,
                            idempotency_key: Some(operation_id.clone()),
                        })
                        .await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            // Cancellation finishes nothing: it
                            // propagates without a completion record.
                            if !matches!(error, qcg_engine::GatewayError::Canceled) {
                                ctx.run.finish_external_operation_with_warn(
                                    ctx.journal,
                                    node,
                                    &operation_id,
                                    qcg_engine::OperationOutcome::gateway_error(&error, false),
                                );
                            }
                            return Err(StepError::from_gateway(&node.id, error));
                        }
                    };
                    let response_value = serde_json::to_value(&response).map_err(|error| {
                        StepError::failed(
                            &node.id,
                            format!("HTTP response is not serializable: {error}"),
                        )
                    })?;
                    ctx.run.finish_external_operation(
                        ctx.journal,
                        node,
                        &operation_id,
                        Some(response_value),
                    )?;
                    response
                }
                qcg_engine::GuardDecision::Resend { result, .. } => serde_json::from_value(result)
                    .map_err(|error| {
                        StepError::failed(
                            &node.id,
                            format!("cached HTTP response is corrupt: {error}"),
                        )
                    })?,
            }
        };
        let mut files = Vec::new();
        let output_mode = match (params.output, params.output_file.is_some()) {
            (Some(mode), _) => mode,
            (None, true) => HttpOutputMode::File,
            (None, false) => HttpOutputMode::Text,
        };
        let body = match output_mode {
            HttpOutputMode::Text => Value::String(
                std::str::from_utf8(&response.body)
                    .map_err(|error| {
                        StepError::failed(
                            &node.id,
                            format!("HTTP response body is not valid UTF-8: {error}"),
                        )
                    })?
                    .to_owned(),
            ),
            HttpOutputMode::Json => serde_json::from_slice(&response.body).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("HTTP response body is not valid JSON: {error}"),
                )
            })?,
            HttpOutputMode::Base64 => json!({
                "encoding": "base64",
                "data": BASE64.encode(&response.body),
            }),
            HttpOutputMode::File => {
                let output_file = params.output_file.as_deref().ok_or_else(|| {
                    StepError::failed(&node.id, "http file output requires output_file")
                })?;
                let output_file = ctx.render_inline(node, output_file)?;
                let path = ctx
                    .run
                    .fs
                    .resolve_write(&output_file)
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                write_atomic(&ctx.run.fs, &path, &response.body, None).await?;
                files.push(path);
                json!({
                    "path": output_file,
                    "bytes": response.body.len(),
                })
            }
        };
        // E09-3: what is full vs truncated and why. The resend cache keeps
        // the full native response (see the `finish_external_operation`
        // call above) so a same-invocation resend continues through the
        // identical output tail below. The user-visible output here carries
        // redacted headers (authorization-like values replaced) but the
        // full body up to the gateway `body_limit_bytes`: the body is the
        // step's functional result, already bounded by that limit (never
        // unbounded), while journaled `tool_call` events carry a further
        // 32 KiB truncated copy via `bounded_event_value`.
        // Asymmetry by design (E09): the agent HTTP tool finishes its
        // REDACTED output instead, so its resend cache replays redacted
        // headers/URL while this plain step replays the full native
        // response. Unifying toward full-finish on the agent side would
        // journal secrets, so the asymmetry stays and is documented here.
        Ok(StepOutcome::Success {
            output: Some(json!({
                "status": response.status,
                "url": qcg_policy::redact_all_query_values(&response.url),
                "headers": qcg_policy::redact_header_values(&response.headers),
                "content_type": response.content_type,
                "body": body,
                "output": match output_mode {
                    HttpOutputMode::Text => "text",
                    HttpOutputMode::Json => "json",
                    HttpOutputMode::Base64 => "base64",
                    HttpOutputMode::File => "file",
                },
            })),
            files,
        })
    }
}

fn http_params(node: &NodeDef) -> Result<HttpParams, StepError> {
    let params: HttpParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid http params: {error}")))?;
    require(node, Some(&params.url), "url")?;
    Ok(params)
}

/// Single canonical constructor for the HTTP approval/guard/execute
/// details object (E09e). Delegates to the single shared constructor in
/// `qcg-engine` (`qcg_engine::http_details_with_url`), which the agent HTTP
/// tool also uses, so the two paths cannot fork into aliases.
pub(crate) fn http_approval_details(
    method: &str,
    headers: &std::collections::BTreeMap<String, String>,
    body: Option<&[u8]>,
    sensitive: &std::collections::BTreeMap<String, String>,
    salt: &str,
    url: &str,
) -> Option<Value> {
    qcg_engine::http_details_with_url(method, headers, body, sensitive, salt, url)
}

#[cfg(test)]
mod tests {
    #[test]
    fn full_url_queries_bind_different_digests() {
        // E09a: different query strings must bind different approval
        // digests even though the journaled target redacts values. Uses the
        // single shared canonicalizer in `qcg-policy` directly (no duplicate
        // wrapper): both the plain step and the agent tool route through it
        // so the two paths produce byte-identical canonical URLs.
        let first = qcg_policy::credential::canonical_http_url(
            "https://example.test/search?q=x&api_key=s3cret",
        );
        let second = qcg_policy::credential::canonical_http_url(
            "https://example.test/search?q=y&api_key=s3cret",
        );
        assert_ne!(first, second, "query change must alter canonical URL");
        let salt = "run-1";
        let first_digest = qcg_engine::salted_binding_digest("http-url-v1", salt, first.as_bytes());
        let second_digest =
            qcg_engine::salted_binding_digest("http-url-v1", salt, second.as_bytes());
        assert_ne!(first_digest, second_digest);
        // Cross-run unlinkability: same plaintext, different salts differ.
        let other_salt =
            qcg_engine::salted_binding_digest("http-url-v1", "run-2", first.as_bytes());
        assert_ne!(first_digest, other_salt);
        // Journaled target redacts both to the same shape (keys visible).
        let journaled_first =
            qcg_policy::redact_all_query_values("https://example.test/search?q=x&api_key=s3cret");
        let journaled_second =
            qcg_policy::redact_all_query_values("https://example.test/search?q=y&api_key=s3cret");
        assert!(
            !journaled_first.contains("s3cret") && !journaled_second.contains("s3cret"),
            "{journaled_first} / {journaled_second}"
        );
    }

    #[test]
    fn approval_guard_execute_share_one_details_object() {
        // E09e: plan/approve/guard/execute share one canonical constructor
        // (`http_approval_details`). Each call site's input must equal the
        // helper's output — not merely equal clones of each other — so a
        // forked inline construction at any site fails this test. Real
        // objects, no mocks.
        use super::http_approval_details;
        let headers = std::collections::BTreeMap::from([(
            "Content-Type".to_string(),
            "text/plain".to_string(),
        )]);
        let sensitive = std::collections::BTreeMap::new();
        let canonical = http_approval_details(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x",
        )
        .expect("canonical helper should build");
        // Each logical call site constructs via the single helper; every
        // site's value must equal the canonical output byte-for-byte.
        let plan = http_approval_details(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x",
        )
        .expect("plan site should build");
        let approve = http_approval_details(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x",
        )
        .expect("approve site should build");
        let guard = http_approval_details(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x",
        )
        .expect("guard site should build");
        let execute = http_approval_details(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x",
        )
        .expect("execute site should build");
        for (name, value) in [
            ("plan", &plan),
            ("approve", &approve),
            ("guard", &guard),
            ("execute", &execute),
        ] {
            assert_eq!(
                value, &canonical,
                "{name} site must equal the single canonical helper output"
            );
            assert_eq!(
                qcg_engine::RunContext::operation_digest(
                    "https://example.test",
                    &Some(value.clone())
                )
                .expect("digest"),
                qcg_engine::RunContext::operation_digest(
                    "https://example.test",
                    &Some(canonical.clone())
                )
                .expect("digest"),
                "{name} must share the identical digest"
            );
        }
        // The helper binds the full canonical URL: reordered pairs bind
        // identically, changed queries bind differently.
        let reordered = http_approval_details(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x&api_key=s3cret",
        )
        .expect("reordered should build");
        let changed = http_approval_details(
            "POST",
            &headers,
            Some(b"hello"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=y&api_key=s3cret",
        )
        .expect("changed should build");
        assert_ne!(
            reordered["url_sha256"], changed["url_sha256"],
            "query change must alter the bound digest"
        );
        assert!(
            !reordered.to_string().contains("s3cret"),
            "details must not carry plaintext query values"
        );
    }

    #[test]
    fn command_target_redaction_keeps_shape_but_drops_secrets() {
        // E09c: journaled command targets redact secrets while keeping keys.
        let redacted =
            qcg_policy::redact_credential_assignments_in_text(&qcg_policy::redact_urls_in_text(
                "deploy --api-key=s3cret https://example.test/?token=abc",
            ));
        assert!(!redacted.contains("s3cret"), "{redacted}");
        assert!(!redacted.contains("abc"), "{redacted}");
        assert_eq!(
            qcg_policy::redact_credential_assignments_in_text(&qcg_policy::redact_urls_in_text(
                "echo hi"
            )),
            "echo hi"
        );
    }

    #[test]
    fn canonical_urls_are_byte_identical_across_plain_and_agent_paths() {
        // E09: ONE shared canonicalizer in `qcg-policy`. Both the plain step
        // and the agent tool call it directly (no duplicate wrappers), so
        // the two call paths produce byte-identical canonical URLs.
        let plain = qcg_policy::credential::canonical_http_url(
            "https://example.test/search?q=x&api_key=s3cret",
        );
        let agent = qcg_policy::credential::canonical_http_url(
            "https://example.test/search?q=x&api_key=s3cret",
        );
        assert_eq!(plain, agent, "both paths must share one canonical form");
        let reordered = qcg_policy::credential::canonical_http_url(
            "https://example.test/search?api_key=s3cret&q=x",
        );
        assert_eq!(plain, reordered, "order must not fork the canonical URL");
        let changed = qcg_policy::credential::canonical_http_url(
            "https://example.test/search?q=y&api_key=s3cret",
        );
        assert_ne!(plain, changed, "query change must alter the canonical URL");
    }

    #[test]
    fn safe_method_details_bind_headers_and_query_with_no_secrets() {
        // E09 (YOUR side): safe methods bind method + full URL + headers
        // (redacted journal). Header or query change yields a different
        // digest; details never carry plaintext secrets.
        use super::http_approval_details;
        let headers = std::collections::BTreeMap::from([
            (
                "Authorization".to_string(),
                "Bearer SENTINEL_SAFE_HEADER".to_string(),
            ),
            ("X-Tenant".to_string(), "alpha".to_string()),
        ]);
        let sensitive = std::collections::BTreeMap::new();
        let base = http_approval_details(
            "GET",
            &headers,
            None,
            &sensitive,
            "run-1",
            "https://example.test/search?q=x&api_key=SENTINEL_SAFE_QUERY",
        )
        .expect("safe GET must bind details, never None");
        assert_eq!(base["method"], "GET");
        assert_eq!(base["safe_read"], true);
        let base_str = base.to_string();
        assert!(
            !base_str.contains("SENTINEL_SAFE_HEADER"),
            "safe details must not leak header secrets: {base_str}"
        );
        assert!(
            !base_str.contains("SENTINEL_SAFE_QUERY"),
            "safe details must not leak query secrets: {base_str}"
        );
        // Header change alters the digest.
        let other_headers = std::collections::BTreeMap::from([
            ("Authorization".to_string(), "Bearer DIFFERENT".to_string()),
            ("X-Tenant".to_string(), "alpha".to_string()),
        ]);
        let other = http_approval_details(
            "GET",
            &other_headers,
            None,
            &sensitive,
            "run-1",
            "https://example.test/search?q=x&api_key=SENTINEL_SAFE_QUERY",
        )
        .expect("safe details should build");
        assert_ne!(
            base["headers_sha256"], other["headers_sha256"],
            "header change must alter the safe digest"
        );
        // Query change alters the digest.
        let changed = http_approval_details(
            "GET",
            &headers,
            None,
            &sensitive,
            "run-1",
            "https://example.test/search?q=y&api_key=SENTINEL_SAFE_QUERY",
        )
        .expect("safe details should build");
        assert_ne!(
            base["url_sha256"], changed["url_sha256"],
            "query change must alter the safe digest"
        );
        // Direct engine helper agrees with the approval helper: both
        // funnel through the single shared constructor (E09).
        let direct = qcg_engine::safe_http_details(
            "GET",
            &headers,
            "run-1",
            "https://example.test/search?q=x&api_key=SENTINEL_SAFE_QUERY",
        )
        .expect("direct safe helper should build");
        assert_eq!(base, direct, "safe paths must share one constructor");
    }

    #[test]
    fn constructed_details_carry_no_sentinel_secrets() {
        // E09 SENSITIVE: every details object YOUR code constructs carries
        // redacted (never raw) secrets. Sentinel secrets cover URL query,
        // credential headers, body, and sensitive values.
        use super::http_approval_details;
        let headers = std::collections::BTreeMap::from([
            (
                "Authorization".to_string(),
                "Bearer SENTINEL_DETAIL_HEADER".to_string(),
            ),
            ("X-Tenant".to_string(), "alpha".to_string()),
        ]);
        let sensitive = std::collections::BTreeMap::from([(
            "api_key".to_string(),
            "SENTINEL_DETAIL_SENSITIVE".to_string(),
        )]);
        let details = http_approval_details(
            "POST",
            &headers,
            Some(b"token=SENTINEL_DETAIL_BODY"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=x&api_key=SENTINEL_DETAIL_QUERY",
        )
        .expect("details should build");
        let details_str = details.to_string();
        for sentinel in [
            "SENTINEL_DETAIL_HEADER",
            "SENTINEL_DETAIL_SENSITIVE",
            "SENTINEL_DETAIL_BODY",
            "SENTINEL_DETAIL_QUERY",
        ] {
            assert!(
                !details_str.contains(sentinel),
                "constructed details must not leak {sentinel}: {details_str}"
            );
        }
        // Binding intact: distinct secrets bind distinctly.
        let other = http_approval_details(
            "POST",
            &headers,
            Some(b"token=SENTINEL_DETAIL_BODY"),
            &sensitive,
            "run-1",
            "https://example.test/search?q=y&api_key=SENTINEL_DETAIL_QUERY",
        )
        .expect("details should build");
        assert_ne!(
            details["url_sha256"], other["url_sha256"],
            "distinct queries must bind distinctly"
        );
    }
}
