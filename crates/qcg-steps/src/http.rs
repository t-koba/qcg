use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{HttpRequest, StepContext, StepError, StepExecutor, StepOutcome};
use qcg_policy::{params_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::{
    bounded_file_bytes, package_file, render_json_templates, require, strict_base64_decode,
    write_atomic,
};
pub(crate) struct HttpStep;

#[derive(Debug, Deserialize)]
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
                if limit.is_some_and(|limit| body.len() > limit) {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "http JSON request body exceeds {} bytes",
                            limit.unwrap_or(usize::MAX)
                        ),
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
                if limit.is_some_and(|limit| decoded.len() > limit) {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "http base64 request body exceeds {} bytes",
                            limit.unwrap_or(usize::MAX)
                        ),
                    ));
                }
                (Some(decoded), Some("application/octet-stream".to_owned()))
            }
            (None, None, None, Some(file)) => {
                let file = ctx.render_inline(node, file)?;
                let path = match params.body_file_scope {
                    HttpBodyFileScope::Workspace => {
                        ctx.run.fs.resolve_read(&file).map_err(|error| {
                            StepError::failed(
                                &node.id,
                                format!("http body_file is not readable: {error}"),
                            )
                        })?
                    }
                    HttpBodyFileScope::Package => {
                        package_file(&ctx.run.contract, node, &file, "http body")?
                    }
                };
                let bytes = bounded_file_bytes(
                    &path,
                    ctx.run.contract.manifest.runtime.http_body_limit_bytes,
                )
                .await
                .map_err(|error| StepError::failed(&node.id, error))?;
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
        // The approval binds method, URL, and body content: approving one
        // request must never authorize a regenerated different body.
        let http_details = if matches!(method.as_str(), "GET" | "HEAD") {
            None
        } else {
            use sha2::{Digest as _, Sha256};
            let body_digest = body.as_ref().map(|body| hex::encode(Sha256::digest(body)));
            Some(json!({ "method": method, "body_sha256": body_digest }))
        };
        if !matches!(method.as_str(), "GET" | "HEAD")
            && let Some(confirm) = ctx.run.require_side_effect(
                ctx.journal,
                node,
                "http",
                &url,
                http_details.clone(),
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
            ctx.run
                .http
                .request(HttpRequest {
                    method,
                    url: url.clone(),
                    headers,
                    sensitive_query: std::collections::BTreeMap::new(),
                    body,
                    follow_redirects: true,
                    idempotency_key: None,
                })
                .await
                .map_err(|error| StepError::from_gateway(&node.id, error))?
        } else {
            let digest = qcg_engine::RunContext::operation_digest(&url, &http_details)?;
            let invocation = qcg_engine::content_invocation_id(&digest);
            match ctx.run.guard_external_operation(
                ctx.journal,
                node,
                "http",
                &url,
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
                            sensitive_query: std::collections::BTreeMap::new(),
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
                write_atomic(&path, &response.body, None).await?;
                files.push(path);
                json!({
                    "path": output_file,
                    "bytes": response.body.len(),
                })
            }
        };
        Ok(StepOutcome::Success {
            output: Some(json!({
                "status": response.status,
                "url": response.url,
                "headers": response.headers,
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
