use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use qcg_contract::{
    ContainerRuntime, ContainerToolBackend, Contract, ExpectDef, MAX_JSON_SCHEMA_BYTES, MountDef,
    NodeDef, ToolBackendKind, ToolBackends, ToolDef, ToolFallback, ToolNetwork, ToolResolution,
    ToolWorkspace, is_safe_relative_path, validate_bounded_json_schema, validate_form_values,
};
use qcg_engine::{
    ConfirmSpec, FieldType, Finding, FormSpec, HttpRequest, InputField, MAX_FOREACH_ITERATIONS,
    MAX_FOREACH_PARALLELISM, Severity, StepContext, StepControlFlow, StepError, StepExecutor,
    StepOutcome, StepRegistry, StepTraits, ToolCallError, ToolCallErrorCode, ToolCallEventData,
    ToolCallPhase, ToolCallSource, ToolCallStatus, tool_call_sources,
    validate_json_schema_findings,
};
use qcg_mcp::{
    McpAccess, McpCallOutcome, McpCommandAccess, McpCommandIsolation, McpContainerRuntime,
    McpError, McpInputRequired, McpRuntime, McpSession,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{BufRead as _, Read as _, Write as _};
use std::sync::Arc;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use walkdir::WalkDir;
use zip::write::SimpleFileOptions;

const MAX_INTERACTIVE_INPUT_BYTES: usize = 64 * 1024;

pub fn deterministic_registry() -> StepRegistry {
    deterministic_registry_with_mcp(Arc::new(McpRuntime::public_defaults()))
}

pub fn deterministic_registry_with_mcp(mcp: Arc<McpRuntime>) -> StepRegistry {
    let mut registry = StepRegistry::new();
    registry.register(RenderStep);
    registry.register(WriteStep);
    registry.register(CopyStep);
    registry.register(TransformStep);
    registry.register(CommandStep);
    registry.register(HttpStep);
    registry.register(McpCallStep { runtime: mcp });
    registry.register(AskUserStep);
    registry.register(CheckSchemaStep);
    registry.register(CheckFormatStep);
    registry.register(CheckCommandStep);
    registry.register(CheckToolStep);
    registry.register(CheckContainerStep);
    registry.register(CheckContractStep);
    registry.register(ForeachStep);
    registry.register(FailStep);
    registry
}

fn parallel_safe_traits() -> StepTraits {
    StepTraits {
        parallel_safe: true,
        ..StepTraits::default()
    }
}

fn params_schema(required: &[&str], properties: Value) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": properties,
    })
}

fn string_schema() -> Value {
    json!({ "type": "string" })
}

fn string_array_schema() -> Value {
    json!({ "type": "array", "items": { "type": "string" } })
}

struct AskUserStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AskUserParams {
    content: String,
    #[serde(default)]
    content_i18n: BTreeMap<String, String>,
    #[serde(default)]
    options: Vec<String>,
    #[serde(default)]
    option_labels_i18n: BTreeMap<String, BTreeMap<String, String>>,
    #[serde(default, rename = "default")]
    default_answer: Option<String>,
    #[serde(default)]
    fields: Vec<InputField>,
    /// Dotted path (for example `steps.design.output.input_fields`) whose
    /// array value supplies the form fields at run time. Mutually exclusive
    /// with static `fields`.
    #[serde(default)]
    fields_from: Option<String>,
}

#[async_trait]
impl StepExecutor for AskUserStep {
    fn type_id(&self) -> &'static str {
        "ask_user"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["content"],
            json!({
                "content": string_schema(),
                "content_i18n": { "type": "object", "additionalProperties": { "type": "string" } },
                "options": string_array_schema(),
                "option_labels_i18n": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    }
                },
                "default": string_schema(),
                "fields": { "type": "array", "items": { "type": "object" } },
                "fields_from": string_schema(),
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = ask_user_params(node)?;
        if !params.fields.is_empty() && params.fields_from.is_some() {
            return Err(StepError::failed(
                &node.id,
                "ask_user cannot combine `fields` with `fields_from`",
            ));
        }
        if let Some(default) = params.default_answer.as_deref() {
            if !params.fields.is_empty() || params.fields_from.is_some() {
                return Err(StepError::failed(
                    &node.id,
                    "ask_user `default` is only valid for scalar `options`",
                ));
            }
            if params.options.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    "ask_user `default` requires non-empty `options`",
                ));
            }
            validate_answer(node, &params.options, default)?;
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let mut params = ask_user_params(node)?;
        if let Some(path) = &params.fields_from {
            let fields = ctx
                .vars
                .get_path(path)
                .ok_or_else(|| StepError::failed(&node.id, format!("`{path}` was not found")))?;
            let Value::Array(items) = fields else {
                return Err(StepError::failed(
                    &node.id,
                    format!("`{path}` must be an array of input fields"),
                ));
            };
            params.fields = items
                .iter()
                .map(|item| {
                    serde_json::from_value::<InputField>(item.clone()).map_err(|error| {
                        StepError::failed(
                            &node.id,
                            format!("invalid input field in `{path}`: {error}"),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
        }
        let title = ctx.render_inline(node, &params.content)?;
        let title_i18n = params
            .content_i18n
            .iter()
            .map(|(language, content)| {
                ctx.render_inline(node, content)
                    .map(|rendered| (language.clone(), rendered))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if let Some(answer) = ctx.run.answers.get(&node.id) {
            if !params.fields.is_empty() {
                validate_form_values(&params.fields, answer, &ctx.run.contract.manifest.runtime)
                    .map_err(|error| {
                        StepError::failed(&node.id, format!("invalid form answer: {error}"))
                    })?;
                ctx.journal
                    .event(
                        "user_interaction",
                        json!({ "node": node.id, "source": "answer" }),
                    )
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                return Ok(StepOutcome::Success {
                    output: Some(answer.clone()),
                    files: vec![],
                });
            }
            let answer = answer
                .as_object()
                .and_then(|values| (values.len() == 1).then(|| values.get("answer")).flatten())
                .unwrap_or(answer);
            let answer = answer_to_string(node, answer)?;
            validate_answer(node, &params.options, &answer)?;
            ctx.journal
                .event(
                    "user_interaction",
                    json!({ "node": node.id, "source": "answer" }),
                )
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            return Ok(StepOutcome::Success {
                output: Some(Value::String(answer)),
                files: vec![],
            });
        }
        if !ctx.run.interactive {
            return Ok(StepOutcome::NeedsUser {
                question: FormSpec {
                    id: node.id.clone(),
                    title,
                    title_i18n,
                    fields: ask_user_fields(&params),
                },
            });
        }

        if !params.fields.is_empty() {
            return Err(StepError::failed(
                &node.id,
                "multi-field ask_user requires a form-capable client",
            ));
        }

        let answer = prompt_for_answer(
            node,
            &params.options,
            params.default_answer.as_deref(),
            &title,
        )?;
        ctx.journal
            .event("user_interaction", json!({ "node": node.id }))
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        Ok(StepOutcome::Success {
            output: Some(Value::String(answer)),
            files: vec![],
        })
    }
}

fn ask_user_params(node: &NodeDef) -> Result<AskUserParams, StepError> {
    let params: AskUserParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid ask_user params: {error}"))
    })?;
    require(node, Some(&params.content), "content")?;
    for field in &params.fields {
        if field.id.trim().is_empty() {
            return Err(StepError::failed(
                &node.id,
                "form field id must not be empty",
            ));
        }
        if matches!(field.kind, FieldType::Custom(_)) {
            return Err(StepError::failed(
                &node.id,
                format!("form field `{}` uses an unsupported custom type", field.id),
            ));
        }
    }
    Ok(params)
}

fn ask_user_fields(params: &AskUserParams) -> Vec<InputField> {
    if !params.fields.is_empty() {
        return params.fields.clone();
    }
    if params.options.is_empty() {
        vec![answer_field(FieldType::String, vec![])]
    } else {
        let mut field = answer_field(FieldType::Select, params.options.clone());
        field.option_labels_i18n = params.option_labels_i18n.clone();
        field.default = params.default_answer.clone().map(Value::String);
        vec![field]
    }
}

fn answer_field(kind: FieldType, options: Vec<String>) -> InputField {
    InputField {
        id: "answer".into(),
        label: None,
        label_i18n: BTreeMap::new(),
        description: None,
        description_i18n: BTreeMap::new(),
        placeholder: None,
        placeholder_i18n: BTreeMap::new(),
        kind,
        required: true,
        default: None,
        pattern: None,
        options,
        option_labels_i18n: BTreeMap::new(),
        min_items: None,
        item_type: None,
        schema: None,
        ui: Default::default(),
    }
}

fn prompt_for_answer(
    node: &NodeDef,
    options: &[String],
    default: Option<&str>,
    title: &str,
) -> Result<String, StepError> {
    eprintln!("{title}");
    if !options.is_empty() {
        eprintln!("Options:");
        for option in options {
            eprintln!("  - {option}");
        }
    }
    if let Some(default) = default {
        eprintln!("Default: {default}");
    }
    eprint!("> ");
    std::io::stderr()
        .flush()
        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    let answer = read_bounded_line(&mut std::io::stdin().lock(), MAX_INTERACTIVE_INPUT_BYTES)
        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    let answer = match (answer.trim(), default) {
        ("", Some(default)) => default.to_string(),
        (answer, _) => answer.to_string(),
    };
    validate_answer(node, options, &answer)?;
    Ok(answer)
}

fn read_bounded_line<R: std::io::BufRead>(
    reader: &mut R,
    max_bytes: usize,
) -> std::io::Result<String> {
    let read_limit = u64::try_from(max_bytes)
        .map_err(|_| std::io::Error::other("interactive input limit is too large"))?
        .checked_add(1)
        .ok_or_else(|| std::io::Error::other("interactive input limit is too large"))?;
    let mut bytes = Vec::with_capacity(max_bytes.min(8 * 1024));
    reader.take(read_limit).read_until(b'\n', &mut bytes)?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("interactive input exceeds {max_bytes} bytes"),
        ));
    }
    String::from_utf8(bytes).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "interactive input must be valid UTF-8",
        )
    })
}

fn answer_to_string(node: &NodeDef, value: &Value) -> Result<String, StepError> {
    match value {
        Value::String(answer) => Ok(answer.clone()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(StepError::failed(
            &node.id,
            "answer must be a scalar JSON value",
        )),
    }
}

fn validate_answer(node: &NodeDef, options: &[String], answer: &str) -> Result<(), StepError> {
    if answer.is_empty() {
        return Err(StepError::failed(&node.id, "answer must not be empty"));
    }
    if !options.is_empty() && !options.iter().any(|option| option == answer) {
        return Err(StepError::failed(
            &node.id,
            format!("answer `{answer}` is outside declared options"),
        ));
    }
    Ok(())
}

struct HttpStep;

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
                if body.len() > limit {
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
                if decoded.len() > limit {
                    return Err(StepError::failed(
                        &node.id,
                        format!("http base64 request body exceeds {limit} bytes"),
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
            _ => unreachable!("validated mutually exclusive HTTP body modes"),
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
        if !matches!(method.as_str(), "GET" | "HEAD")
            && let Some(confirm) =
                ctx.run
                    .require_side_effect(ctx.journal, node, "http", &url, None)?
        {
            return Ok(StepOutcome::NeedsConfirm { confirm });
        }
        let response = ctx
            .run
            .http
            .request(HttpRequest {
                method,
                url,
                headers,
                sensitive_query: std::collections::BTreeMap::new(),
                body,
                follow_redirects: true,
            })
            .await
            .map_err(|error| StepError::from_gateway(&node.id, error))?;
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

struct McpCallStep {
    runtime: Arc<McpRuntime>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpCallParams {
    server: String,
    tool: String,
    #[serde(default)]
    arguments: Value,
    #[serde(default)]
    input_schema: Option<Value>,
    #[serde(default)]
    output_schema: Option<Value>,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default = "default_true")]
    side_effects: bool,
}

fn default_true() -> bool {
    true
}

#[async_trait]
impl StepExecutor for McpCallStep {
    fn type_id(&self) -> &'static str {
        "mcp.call"
    }

    fn traits(&self) -> StepTraits {
        StepTraits::default()
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["server", "tool"],
            json!({
                "server": string_schema(),
                "tool": string_schema(),
                "arguments": { "type": "object" },
                "input_schema": {},
                "output_schema": {},
                "timeout_seconds": { "type": "integer", "minimum": 1 },
                "side_effects": { "type": "boolean" },
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = mcp_call_params(node)?;
        if params.server.trim().is_empty() || params.tool.trim().is_empty() {
            return Err(StepError::failed(
                &node.id,
                "mcp.call server and tool must not be empty",
            ));
        }
        if params.arguments.as_object().is_none() {
            return Err(StepError::failed(
                &node.id,
                "mcp.call arguments must be a JSON object",
            ));
        }
        if let Some(schema) = &params.input_schema {
            validate_mcp_call_schema(node, schema, "input_schema")?;
        }
        if let Some(schema) = &params.output_schema {
            validate_mcp_call_schema(node, schema, "output_schema")?;
        }
        let profile = self
            .runtime
            .resolve(&params.server)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        validate_mcp_call_timeout(node, params.timeout_seconds, profile.timeout_seconds())?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = mcp_call_params(node)?;
        let profile = self
            .runtime
            .resolve(&params.server)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        validate_mcp_call_timeout(node, params.timeout_seconds, profile.timeout_seconds())?;
        let timeout_seconds = params
            .timeout_seconds
            .unwrap_or_else(|| profile.timeout_seconds());
        let arguments =
            render_json_templates(ctx, node, &params.arguments, profile.max_response_bytes())?;
        let argument_bytes = serde_json::to_vec(&arguments)?;
        if argument_bytes.len() > profile.max_response_bytes() {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "mcp.call arguments exceed MCP profile limit of {} bytes",
                    profile.max_response_bytes()
                ),
            ));
        }
        validate_mcp_call_arguments(node, params.input_schema.as_ref(), &arguments)?;
        let access = mcp_access(ctx, profile.command());
        if params.side_effects
            && let Some(confirm) = ctx.run.require_side_effect(
                ctx.journal,
                node,
                "mcp.call",
                &format!("{}/{}", params.server, params.tool),
                Some(json!({
                    "server": params.server,
                    "tool": params.tool,
                    "argument_names": arguments
                        .as_object()
                        .map(|object| object.keys().cloned().collect::<Vec<_>>())
                        .unwrap_or_default(),
                })),
            )?
        {
            return Ok(StepOutcome::NeedsConfirm { confirm });
        }

        let runtime = self.runtime.clone();
        let server = params.server.clone();
        let tool_name = params.tool.clone();
        let cancellation = ctx.run.cancellation.clone();
        let session = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_seconds),
            runtime.connect(&server, &access, cancellation),
        )
        .await
        .map_err(|_| StepError::failed(&node.id, "MCP connection timed out"))?
        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let started = std::time::Instant::now();
        let result = execute_direct_mcp_call(
            &session,
            &tool_name,
            arguments.clone(),
            ctx,
            node,
            timeout_seconds,
            profile.max_response_bytes(),
        )
        .await;
        let close_result = session.close().await;
        if let Err(error) = close_result {
            record_direct_mcp_tool_event(
                ctx,
                node,
                DirectMcpToolEvent {
                    server: &params.server,
                    tool: &params.tool,
                    arguments: &arguments,
                    result: None,
                    error: Some(&StepError::failed(&node.id, error.to_string())),
                    duration_ms: started.elapsed().as_millis() as u64,
                },
            )?;
            return Err(StepError::failed(&node.id, error.to_string()));
        }
        let result = match result {
            Ok(DirectMcpCallOutcome::NeedsUser(question)) => {
                record_direct_mcp_tool_event(
                    ctx,
                    node,
                    DirectMcpToolEvent {
                        server: &params.server,
                        tool: &params.tool,
                        arguments: &arguments,
                        result: None,
                        error: None,
                        duration_ms: started.elapsed().as_millis() as u64,
                    },
                )?;
                return Ok(StepOutcome::NeedsUser { question });
            }
            Ok(DirectMcpCallOutcome::Complete(result)) => result,
            Err(error) => {
                record_direct_mcp_tool_event(
                    ctx,
                    node,
                    DirectMcpToolEvent {
                        server: &params.server,
                        tool: &params.tool,
                        arguments: &arguments,
                        result: None,
                        error: Some(&error),
                        duration_ms: started.elapsed().as_millis() as u64,
                    },
                )?;
                return Err(error);
            }
        };
        let raw_result = result.clone();
        let result = match validate_direct_mcp_result(
            node,
            &params.server,
            &params.tool,
            params.output_schema.as_ref(),
            result,
        ) {
            Ok(result) => result,
            Err(error) => {
                record_direct_mcp_tool_event(
                    ctx,
                    node,
                    DirectMcpToolEvent {
                        server: &params.server,
                        tool: &params.tool,
                        arguments: &arguments,
                        result: Some(&raw_result),
                        error: Some(&error),
                        duration_ms: started.elapsed().as_millis() as u64,
                    },
                )?;
                return Err(error);
            }
        };
        let sources = tool_call_sources(&result);
        record_direct_mcp_tool_event(
            ctx,
            node,
            DirectMcpToolEvent {
                server: &params.server,
                tool: &params.tool,
                arguments: &arguments,
                result: Some(&result),
                error: None,
                duration_ms: started.elapsed().as_millis() as u64,
            },
        )?;
        Ok(StepOutcome::Success {
            output: Some(json!({
                "server": params.server,
                "tool": params.tool,
                "result": result,
                "sources": sources,
            })),
            files: vec![],
        })
    }
}

fn mcp_call_params(node: &NodeDef) -> Result<McpCallParams, StepError> {
    node.deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid mcp.call params: {error}")))
}

fn validate_mcp_call_timeout(
    node: &NodeDef,
    requested: Option<u64>,
    profile_limit: u64,
) -> Result<(), StepError> {
    if requested == Some(0) {
        return Err(StepError::failed(
            &node.id,
            "mcp.call timeout_seconds must be greater than zero",
        ));
    }
    if let Some(requested) = requested
        && requested > profile_limit
    {
        return Err(StepError::failed(
            &node.id,
            format!(
                "mcp.call timeout_seconds ({requested}) must not exceed MCP profile limit ({profile_limit})"
            ),
        ));
    }
    Ok(())
}

enum DirectMcpCallOutcome {
    Complete(Value),
    NeedsUser(FormSpec),
}

fn validate_mcp_call_schema(node: &NodeDef, schema: &Value, field: &str) -> Result<(), StepError> {
    validate_bounded_json_schema(schema).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("mcp.call {field} is not a valid bounded JSON Schema: {error}"),
        )
    })?;
    Ok(())
}

fn validate_mcp_call_arguments(
    node: &NodeDef,
    schema: Option<&Value>,
    arguments: &Value,
) -> Result<(), StepError> {
    let Some(schema) = schema else {
        return Ok(());
    };
    validate_bounded_json_schema(schema).map_err(|error| {
        StepError::failed(&node.id, format!("invalid mcp.call input_schema: {error}"))
    })?;
    let validator = jsonschema::validator_for(schema).map_err(|error| {
        StepError::failed(&node.id, format!("invalid mcp.call input_schema: {error}"))
    })?;
    if let Err(error) = validator.validate(arguments) {
        return Err(StepError::failed(
            &node.id,
            format!(
                "mcp.call arguments failed input_schema at {}: {error}",
                error.instance_path()
            ),
        ));
    }
    Ok(())
}

fn mcp_access(ctx: &StepContext<'_>, command: &[String]) -> McpAccess {
    let permissions = &ctx.run.contract.manifest.permissions;
    let commands = permissions
        .commands
        .iter()
        .filter(|permission| {
            permission.bin == command.first().cloned().unwrap_or_default()
                && permission.args.len() == command.len().saturating_sub(1)
                && permission
                    .args
                    .iter()
                    .zip(command.iter().skip(1))
                    .all(|(allowed, actual)| allowed == actual || allowed == "*")
        })
        .filter_map(|permission| {
            let isolation = permission.isolation.as_ref()?;
            Some(McpCommandAccess {
                argv: command.to_vec(),
                isolation: match isolation {
                    qcg_contract::CommandIsolation::Container => McpCommandIsolation::Container,
                    qcg_contract::CommandIsolation::TrustedHost => McpCommandIsolation::TrustedHost,
                },
                image: permission.image.clone(),
                runtime: match isolation {
                    qcg_contract::CommandIsolation::TrustedHost => None,
                    qcg_contract::CommandIsolation::Container => {
                        permissions.containers.runtime.map(|runtime| match runtime {
                            qcg_contract::ContainerRuntime::Docker => McpContainerRuntime::Docker,
                            qcg_contract::ContainerRuntime::Podman => McpContainerRuntime::Podman,
                            qcg_contract::ContainerRuntime::DockerRunsc => {
                                McpContainerRuntime::DockerRunsc
                            }
                        })
                    }
                },
            })
        })
        .collect();
    McpAccess {
        network_hosts: permissions.network.iter().cloned().collect(),
        commands,
        workspace: ctx.run.fs.workspace().as_std_path().to_path_buf(),
    }
}

async fn execute_direct_mcp_call(
    session: &McpSession,
    tool_name: &str,
    arguments: Value,
    ctx: &StepContext<'_>,
    node: &NodeDef,
    timeout_seconds: u64,
    result_limit_bytes: usize,
) -> Result<DirectMcpCallOutcome, StepError> {
    let tools = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_seconds),
        session.list_tools(),
    )
    .await
    .map_err(|_| StepError::failed(&node.id, "MCP tools/list timed out"))?
    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    let tool = tools
        .iter()
        .find(|tool| tool.name == tool_name)
        .ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!(
                    "MCP server {} does not expose tool {}",
                    session.server_id(),
                    tool_name
                ),
            )
        })?;
    validate_bounded_json_schema(&tool.input_schema).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("MCP tool {tool_name} input schema is invalid or unsafe: {error}"),
        )
    })?;
    let input_validator = jsonschema::validator_for(&tool.input_schema).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("MCP tool {} input schema is invalid: {error}", tool_name),
        )
    })?;
    if let Err(error) = input_validator.validate(&arguments) {
        return Err(StepError::failed(
            &node.id,
            format!(
                "MCP tool {} arguments failed input schema at {}: {error}",
                tool_name,
                error.instance_path()
            ),
        ));
    }
    let output_validator = tool
        .output_schema
        .as_ref()
        .map(|schema| {
            validate_bounded_json_schema(schema).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("MCP tool {tool_name} output schema is invalid or unsafe: {error}"),
                )
            })?;
            jsonschema::validator_for(schema).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("MCP tool {tool_name} output schema is invalid: {error}"),
                )
            })
        })
        .transpose()?;
    let mut input_responses = None;
    let mut request_state = None;
    for _ in 0..10 {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_seconds),
            session.call_tool_with_input(
                tool_name,
                arguments.clone(),
                input_responses.take(),
                request_state.take(),
            ),
        )
        .await
        .map_err(|_| StepError::failed(&node.id, "MCP tools/call timed out"))?;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(McpError::ToolFailed { result, .. }) => McpCallOutcome::Complete(result),
            Err(error) => return Err(StepError::failed(&node.id, error.to_string())),
        };
        match outcome {
            McpCallOutcome::Complete(value) => {
                let size = serde_json::to_vec(&value)?.len();
                if size > result_limit_bytes {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "MCP tool {} result exceeded {result_limit_bytes} bytes",
                            tool_name
                        ),
                    ));
                }
                if !value
                    .get("isError")
                    .is_some_and(|is_error| is_error == &Value::Bool(true))
                    && let Some(validator) = &output_validator
                {
                    let structured = value
                        .get("structuredContent")
                        .or_else(|| value.get("structured_content"))
                        .ok_or_else(|| {
                            StepError::failed(
                                &node.id,
                                format!(
                                    "MCP tool {tool_name} declared outputSchema but omitted structuredContent"
                                ),
                            )
                        })?;
                    if let Err(error) = validator.validate(structured) {
                        return Err(StepError::failed(
                            &node.id,
                            format!(
                                "MCP tool {tool_name} result failed output schema at {}: {error}",
                                error.instance_path()
                            ),
                        ));
                    }
                }
                return Ok(DirectMcpCallOutcome::Complete(value));
            }
            McpCallOutcome::InputRequired(required) => {
                request_state = required.request_state.clone();
                if required.input_requests.is_empty() {
                    continue;
                }
                let question_id =
                    direct_mcp_question_id(&node.id, session.server_id(), tool_name, &required);
                let Some(answer) = ctx.run.answers.get(&question_id) else {
                    return Ok(DirectMcpCallOutcome::NeedsUser(direct_mcp_form_spec(
                        question_id,
                        &format!("{}/{}", session.server_id(), tool_name),
                        &required,
                    )?));
                };
                input_responses = Some(direct_mcp_input_responses(&required, answer, node)?);
            }
        }
    }
    Err(StepError::failed(
        &node.id,
        "MCP tool exceeded 10 input-required rounds",
    ))
}

fn validate_direct_mcp_result(
    node: &NodeDef,
    server: &str,
    tool: &str,
    output_schema: Option<&Value>,
    result: Value,
) -> Result<Value, StepError> {
    let object = result.as_object().ok_or_else(|| {
        StepError::failed(
            &node.id,
            format!("MCP tool {server}/{tool} result is not an object"),
        )
    })?;
    if object
        .get("isError")
        .is_some_and(|value| value != &Value::Bool(false))
    {
        return Err(StepError::failed(
            &node.id,
            format!("MCP tool {server}/{tool} returned an error result"),
        ));
    }
    if let Some(schema) = output_schema {
        let value = object
            .get("structuredContent")
            .or_else(|| object.get("structured_content"))
            .unwrap_or(&result);
        validate_bounded_json_schema(schema).map_err(|error| {
            StepError::failed(&node.id, format!("invalid mcp.call output_schema: {error}"))
        })?;
        let validator = jsonschema::validator_for(schema).map_err(|error| {
            StepError::failed(&node.id, format!("invalid mcp.call output_schema: {error}"))
        })?;
        if let Err(error) = validator.validate(value) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "MCP tool {server}/{tool} output failed output_schema at {}: {error}",
                    error.instance_path()
                ),
            ));
        }
    }
    Ok(result)
}

fn direct_mcp_question_id(
    node_id: &str,
    server: &str,
    tool: &str,
    required: &McpInputRequired,
) -> String {
    let mut requests = required
        .input_requests
        .values()
        .map(|request| serde_json::to_vec(request).unwrap_or_default())
        .collect::<Vec<_>>();
    requests.sort();
    let bytes = serde_json::to_vec(&json!({
        "server": server,
        "tool": tool,
        "requests": requests,
    }))
    .unwrap_or_default();
    let digest = hex::encode(Sha256::digest(bytes));
    format!("{node_id}:mcp:{server}/{tool}:{}", &digest[..16])
}

fn direct_mcp_form_spec(
    question_id: String,
    alias: &str,
    required: &McpInputRequired,
) -> Result<FormSpec, StepError> {
    let mut fields = Vec::with_capacity(required.input_requests.len());
    for (index, (request_id, request)) in direct_mcp_requests(required).into_iter().enumerate() {
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method != "elicitation/create" {
            return Err(StepError::failed(
                alias,
                format!("MCP input request `{request_id}` uses unsupported method `{method}`"),
            ));
        }
        let params = request.get("params").unwrap_or(request);
        let message = params
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("MCP tool requested structured input");
        let label = params
            .get("url")
            .and_then(Value::as_str)
            .map_or_else(|| message.to_string(), |url| format!("{message} ({url})"));
        fields.push(InputField {
            id: format!("response_{index}"),
            label: Some(label),
            label_i18n: Default::default(),
            description: Some(message.to_string()),
            description_i18n: Default::default(),
            placeholder: None,
            placeholder_i18n: Default::default(),
            kind: FieldType::Json,
            required: true,
            default: None,
            pattern: None,
            options: vec![],
            option_labels_i18n: Default::default(),
            min_items: None,
            item_type: None,
            schema: params.get("requestedSchema").cloned(),
            ui: Default::default(),
        });
    }
    Ok(FormSpec {
        id: question_id,
        title: format!("MCP tool `{alias}` requires input"),
        title_i18n: Default::default(),
        fields,
    })
}

fn direct_mcp_input_responses(
    required: &McpInputRequired,
    answer: &Value,
    node: &NodeDef,
) -> Result<BTreeMap<String, Value>, StepError> {
    let values = answer.as_object().ok_or_else(|| {
        StepError::failed(&node.id, "MCP input-required answer must be an object")
    })?;
    let responses = direct_mcp_requests(required)
        .into_iter()
        .enumerate()
        .map(|(index, (id, _request))| {
            let field = format!("response_{index}");
            let value = values.get(&field).cloned().ok_or_else(|| {
                StepError::failed(&node.id, format!("MCP answer omitted {field}"))
            })?;
            Ok((id.clone(), json!({ "action": "accept", "content": value })))
        })
        .collect::<Result<BTreeMap<_, _>, StepError>>()?;
    Ok(responses)
}

fn direct_mcp_requests(required: &McpInputRequired) -> Vec<(&String, &Value)> {
    let mut requests = required.input_requests.iter().collect::<Vec<_>>();
    requests.sort_by_key(|(_id, request)| serde_json::to_vec(request).unwrap_or_default());
    requests
}

struct DirectMcpToolEvent<'a> {
    server: &'a str,
    tool: &'a str,
    arguments: &'a Value,
    result: Option<&'a Value>,
    error: Option<&'a StepError>,
    duration_ms: u64,
}

fn record_direct_mcp_tool_event(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    details: DirectMcpToolEvent<'_>,
) -> Result<(), StepError> {
    let DirectMcpToolEvent {
        server,
        tool,
        arguments,
        result,
        error,
        duration_ms,
    } = details;
    let argument_bytes = serde_json::to_vec(arguments)?.len();
    let argument_names = arguments
        .as_object()
        .map(|object| {
            object
                .keys()
                .take(128)
                .map(|name| name.chars().take(256).collect::<String>())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let arguments = json!({
        "argument_names": argument_names,
        "bytes": argument_bytes,
    });
    let (status, phase, error_value) = match error {
        Some(error) => (
            ToolCallStatus::Failed,
            ToolCallPhase::Execution,
            Some(ToolCallError {
                code: ToolCallErrorCode::ExecutionFailed,
                message: error.to_string(),
            }),
        ),
        None if result.is_none() => (ToolCallStatus::NeedsUser, ToolCallPhase::Execution, None),
        None => (ToolCallStatus::Succeeded, ToolCallPhase::Completed, None),
    };
    let sources = result
        .map(tool_call_sources)
        .unwrap_or_default()
        .into_iter()
        .map(serde_json::from_value::<ToolCallSource>)
        .collect::<Result<Vec<_>, _>>()?;
    let (result, result_truncated) = result
        .map(bounded_direct_mcp_event_value)
        .unwrap_or((Value::Null, false));
    let result_summary = match result {
        Value::Null => Value::Null,
        value => {
            let bytes = serde_json::to_vec(&value)?.len();
            json!({ "bytes": bytes, "value": value })
        }
    };
    let id = format!(
        "mcp-{}",
        &hex::encode(Sha256::digest(format!("{}/{}/{}", node.id, server, tool)))[..24]
    );
    let event = ToolCallEventData {
        server: Some(server.to_owned()),
        tool: format!("mcp:{server}/{tool}"),
        id,
        status,
        phase,
        agent: None,
        error: error_value,
        duration_ms,
        arguments,
        result: result_summary,
        sources,
        truncated: result_truncated,
    };
    let mut event = serde_json::to_value(event)?;
    event["node"] = Value::String(node.id.clone());
    ctx.journal
        .event("tool_call", event)
        .map_err(|journal_error| StepError::failed(&node.id, journal_error.to_string()))
}

fn bounded_direct_mcp_event_value(value: &Value) -> (Value, bool) {
    const MAX_BYTES: usize = 64 * 1024;
    let Ok(bytes) = serde_json::to_vec(value) else {
        return (json!({ "summary": "result serialization failed" }), true);
    };
    if bytes.len() <= MAX_BYTES {
        return (value.clone(), false);
    }
    (
        json!({
            "summary": "result omitted because it exceeded the event detail limit",
            "bytes": bytes.len(),
        }),
        true,
    )
}

struct CheckSchemaStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckSchemaParams {
    source: String,
    schema: String,
}

#[async_trait]
impl StepExecutor for CheckSchemaStep {
    fn type_id(&self) -> &'static str {
        "check.schema"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "schema"],
            json!({
                "source": string_schema(),
                "schema": string_schema(),
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        parallel_safe_traits()
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = check_schema_params(node)?;
        load_package_schema(contract, node, &params.schema)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = check_schema_params(node)?;
        let source = ctx.render_inline(node, &params.source)?;
        let source_path = ctx.run.fs.resolve_read(&source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("source path is not in workspace: {error}"),
            )
        })?;
        let value_source = bounded_transform_text(
            &source_path,
            ctx.run.contract.manifest.runtime.file_input_limit_bytes,
        )
        .await
        .map_err(|error| StepError::failed(&node.id, error))?;
        let value: Value = serde_json::from_str(&value_source)
            .map_err(|error| StepError::failed(&node.id, format!("invalid JSON: {error}")))?;
        let schema = load_package_schema(&ctx.run.contract, node, &params.schema)?;
        let findings = validate_json_schema_findings(&schema, &value, "$");
        if findings.is_empty() {
            Ok(StepOutcome::Success {
                output: Some(json!({ "status": "pass", "source": source })),
                files: vec![],
            })
        } else {
            Ok(StepOutcome::CheckFailed {
                findings,
                output: None,
                files: vec![],
            })
        }
    }
}

fn check_schema_params(node: &NodeDef) -> Result<CheckSchemaParams, StepError> {
    let params: CheckSchemaParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid check.schema params: {error}"))
    })?;
    require(node, Some(&params.source), "source")?;
    require(node, Some(&params.schema), "schema")?;
    Ok(params)
}

fn load_package_schema(
    contract: &Contract,
    node: &NodeDef,
    relative: &str,
) -> Result<Value, StepError> {
    let path = package_file(contract, node, relative, "schema")?;
    let file = std::fs::File::open(&path).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("failed to read schema package file `{relative}`: {error}"),
        )
    })?;
    let mut bytes = Vec::new();
    file.take((MAX_JSON_SCHEMA_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            StepError::failed(
                &node.id,
                format!("failed to read schema package file `{relative}`: {error}"),
            )
        })?;
    if bytes.len() > MAX_JSON_SCHEMA_BYTES {
        return Err(StepError::failed(
            &node.id,
            format!(
                "schema package file `{relative}` exceeds the {MAX_JSON_SCHEMA_BYTES}-byte limit"
            ),
        ));
    }
    let schema: Value = serde_json::from_slice(&bytes).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("schema package file `{relative}` is not valid JSON: {error}"),
        )
    })?;
    validate_bounded_json_schema(&schema).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("schema package file `{relative}` is invalid or unsafe JSON Schema: {error}"),
        )
    })?;
    Ok(schema)
}

struct CheckFormatStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckFormatParams {
    source: String,
    content: String,
}

#[async_trait]
impl StepExecutor for CheckFormatStep {
    fn type_id(&self) -> &'static str {
        "check.format"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "content"],
            json!({
                "source": string_schema(),
                "content": { "type": "string", "enum": ["json", "toml"] },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        parallel_safe_traits()
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = check_format_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = check_format_params(node)?;
        let source = ctx.render_inline(node, &params.source)?;
        let source_path = ctx.run.fs.resolve_read(&source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("source path is not in workspace: {error}"),
            )
        })?;
        let text = bounded_transform_text(
            &source_path,
            ctx.run.contract.manifest.runtime.file_input_limit_bytes,
        )
        .await
        .map_err(|error| StepError::failed(&node.id, error))?;
        let format = params.content.as_str();
        let result = match format {
            "json" => serde_json::from_str::<Value>(&text)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            "toml" => toml::from_str::<toml::Value>(&text)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            _ => unreachable!("validated format"),
        };
        match result {
            Ok(()) => Ok(StepOutcome::Success {
                output: Some(json!({ "status": "pass", "source": source, "format": format })),
                files: vec![],
            }),
            Err(error) => Ok(StepOutcome::CheckFailed {
                findings: vec![Finding {
                    severity: Severity::Error,
                    message: format!("invalid {format}: {error}"),
                    location: Some(source),
                    raw_output: None,
                }],
                output: None,
                files: vec![],
            }),
        }
    }
}

fn check_format_params(node: &NodeDef) -> Result<CheckFormatParams, StepError> {
    let params: CheckFormatParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid check.format params: {error}"))
    })?;
    require(node, Some(&params.source), "source")?;
    require(node, Some(&params.content), "content")?;
    if !matches!(params.content.as_str(), "json" | "toml") {
        return Err(StepError::failed(
            &node.id,
            "content must be one of: json, toml",
        ));
    }
    Ok(params)
}

struct CheckContractStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckContractParams {
    source: String,
}

#[async_trait]
impl StepExecutor for CheckContractStep {
    fn type_id(&self) -> &'static str {
        "check.contract"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source"],
            json!({ "source": string_schema() }),
        ))
    }

    fn traits(&self) -> StepTraits {
        parallel_safe_traits()
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = check_contract_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = check_contract_params(node)?;
        let source = ctx.render_inline(node, &params.source)?;
        let generator_dir = ctx.run.fs.resolve_read(&source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("source path is not in workspace: {error}"),
            )
        })?;
        ensure_bounded_file_tree(
            &generator_dir,
            ctx.run.contract.manifest.runtime.file_input_limit_bytes,
            ctx.run.contract.manifest.runtime.file_count_limit,
        )
        .await
        .map_err(|error| StepError::failed(&node.id, error))?;
        match Contract::load(&generator_dir) {
            Ok(contract) => Ok(StepOutcome::Success {
                output: Some(json!({
                    "status": "pass",
                    "generator": contract.manifest.generator.id,
                    "version": contract.manifest.generator.version,
                    "contract_sha256": contract.sha256,
                })),
                files: vec![],
            }),
            Err(error) => Ok(StepOutcome::CheckFailed {
                findings: vec![Finding {
                    severity: Severity::Error,
                    message: error.to_string(),
                    location: Some(source),
                    raw_output: None,
                }],
                output: None,
                files: vec![],
            }),
        }
    }
}

fn check_contract_params(node: &NodeDef) -> Result<CheckContractParams, StepError> {
    let params: CheckContractParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid check.contract params: {error}"))
    })?;
    require(node, Some(&params.source), "source")?;
    Ok(params)
}

struct RenderStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RenderParams {
    template: String,
    output_file: String,
}

#[async_trait]
impl StepExecutor for RenderStep {
    fn type_id(&self) -> &'static str {
        "render"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["template", "output_file"],
            json!({
                "template": string_schema(),
                "output_file": string_schema(),
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        parallel_safe_traits()
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = render_params(node)?;
        package_file(contract, node, &params.template, "template")?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = render_params(node)?;
        let output_file = ctx.render_inline(node, &params.output_file)?;
        let template_path = package_file(&ctx.run.contract, node, &params.template, "template")?;
        let input_limit = ctx.run.contract.manifest.runtime.file_input_limit_bytes;
        let source = bounded_file_bytes(&template_path, input_limit)
            .await
            .map_err(|error| StepError::failed(&node.id, error))?;
        let source = String::from_utf8(source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("template `{}` is not valid UTF-8: {error}", params.template),
            )
        })?;
        let rendered = ctx
            .run
            .templates
            .render_inline(
                &source,
                ctx.vars.to_json(),
                &ctx.run.contract.manifest.runtime,
            )
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let path = ctx
            .run
            .fs
            .resolve_write(&output_file)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        write_atomic(&path, rendered.as_bytes(), None).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": output_file })),
            files: vec![path],
        })
    }
}

fn render_params(node: &NodeDef) -> Result<RenderParams, StepError> {
    let params: RenderParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid render params: {error}")))?;
    require(node, Some(&params.template), "template")?;
    require(node, Some(&params.output_file), "output_file")?;
    Ok(params)
}

struct WriteStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteParams {
    output_file: String,
    content: String,
    #[serde(default)]
    unix_mode: Option<String>,
}

#[async_trait]
impl StepExecutor for WriteStep {
    fn type_id(&self) -> &'static str {
        "write"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["output_file", "content"],
            json!({
                "output_file": string_schema(),
                "content": string_schema(),
                "unix_mode": { "type": "string", "pattern": "^0[6-7][0-7]{2}$" },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        parallel_safe_traits()
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = write_params(node)?;
        validate_unix_mode_template(node, params.unix_mode.as_deref())?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = write_params(node)?;
        let output_file = ctx.render_inline(node, &params.output_file)?;
        let path = ctx
            .run
            .fs
            .resolve_write(&output_file)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let content = ctx.render_inline(node, &params.content)?;
        let unix_mode = params
            .unix_mode
            .as_deref()
            .map(|mode| ctx.render_inline(node, mode))
            .transpose()?;
        let unix_mode = parse_unix_mode(node, unix_mode.as_deref())?;
        write_atomic(&path, content.as_bytes(), unix_mode).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": output_file })),
            files: vec![path],
        })
    }
}

fn write_params(node: &NodeDef) -> Result<WriteParams, StepError> {
    let params: WriteParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid write params: {error}")))?;
    require(node, Some(&params.output_file), "output_file")?;
    require(node, Some(&params.content), "content")?;
    Ok(params)
}

struct CopyStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CopyParams {
    source: String,
    target: String,
}

#[async_trait]
impl StepExecutor for CopyStep {
    fn type_id(&self) -> &'static str {
        "copy"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "target"],
            json!({
                "source": string_schema(),
                "target": string_schema(),
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        parallel_safe_traits()
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = copy_params(node)?;
        package_file(contract, node, &params.source, "copy source")?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = copy_params(node)?;
        let source = package_file(&ctx.run.contract, node, &params.source, "copy source")?;
        let target_name = ctx.render_inline(node, &params.target)?;
        let target = ctx
            .run
            .fs
            .resolve_write(&target_name)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let input_limit = ctx.run.contract.manifest.runtime.file_input_limit_bytes;
        let bytes = bounded_file_bytes(&source, input_limit)
            .await
            .map_err(|error| StepError::failed(&node.id, error))?;
        let source_mode = source_unix_mode(&source)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        write_atomic(&target, &bytes, source_mode).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": target_name })),
            files: vec![target],
        })
    }
}

fn copy_params(node: &NodeDef) -> Result<CopyParams, StepError> {
    let params: CopyParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid copy params: {error}")))?;
    require(node, Some(&params.source), "source")?;
    require(node, Some(&params.target), "target")?;
    Ok(params)
}

fn source_unix_mode(path: &camino::Utf8Path) -> Result<Option<u32>, std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
        Ok(Some(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}

struct TransformStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransformParams {
    transform: String,
    source: String,
    target: String,
    /// Second input file for `json_merge`; `source` wins on conflicting keys.
    #[serde(default)]
    with: Option<String>,
    #[serde(default)]
    secrets: Vec<String>,
    #[serde(default)]
    unix_mode: Option<String>,
    #[serde(default)]
    remove_source: bool,
}

#[async_trait]
impl StepExecutor for TransformStep {
    fn type_id(&self) -> &'static str {
        "transform"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["transform", "source", "target"],
            json!({
                "transform": {
                    "type": "string",
                    "enum": [
                        "inject_secrets",
                        "json_pretty",
                        "json_compact",
                        "toml_to_json",
                        "json_to_toml",
                        "json_merge",
                        "base64_decode",
                        "base64_encode",
                        "zip"
                    ]
                },
                "source": string_schema(),
                "target": string_schema(),
                "with": string_schema(),
                "secrets": string_array_schema(),
                "unix_mode": { "type": "string", "pattern": "^0[6-7][0-7]{2}$" },
                "remove_source": { "type": "boolean" },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        parallel_safe_traits()
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = transform_params(node)?;
        if params.transform == "json_merge" && params.with.is_none() {
            return Err(StepError::failed(
                &node.id,
                "json_merge requires a `with` input file",
            ));
        }
        validate_unix_mode_template(node, params.unix_mode.as_deref())?;
        if params.unix_mode.is_some() && params.transform != "base64_decode" {
            return Err(StepError::failed(
                &node.id,
                "transform unix_mode is only valid for base64_decode",
            ));
        }
        if params.transform == "inject_secrets" {
            if params.secrets.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    "inject_secrets requires a non-empty secrets declaration",
                ));
            }
            for secret in &params.secrets {
                if !contract.manifest.secrets.contains_key(secret) {
                    return Err(StepError::failed(
                        &node.id,
                        format!("inject_secrets references unknown secret `{secret}`"),
                    ));
                }
            }
        } else if !params.secrets.is_empty() {
            return Err(StepError::failed(
                &node.id,
                "secrets is only valid for inject_secrets",
            ));
        }
        if params.remove_source && params.transform != "base64_decode" {
            return Err(StepError::failed(
                &node.id,
                "transform remove_source is only valid for base64_decode",
            ));
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = transform_params(node)?;
        let transform = params.transform.as_str();
        let source = ctx.render_inline(node, &params.source)?;
        let target = ctx.render_inline(node, &params.target)?;
        let rendered_unix_mode = params
            .unix_mode
            .as_deref()
            .map(|mode| ctx.render_inline(node, mode))
            .transpose()?;
        let source_path = ctx.run.fs.resolve_read(&source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("source path is not in workspace: {error}"),
            )
        })?;
        let target_path = ctx
            .run
            .fs
            .resolve_write(&target)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let transform_limit = ctx.run.contract.manifest.runtime.file_input_limit_bytes;
        let output_limit = ctx.run.contract.manifest.runtime.output_file_limit_bytes;
        let mut value_output = None;
        match transform {
            "inject_secrets" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let injected = ctx
                    .run
                    .secrets
                    .inject_declared_placeholders(&text, &params.secrets)
                    .map_err(|error| StepError::failed(&node.id, error))?;
                write_transform_output(&node.id, &target_path, injected.as_bytes(), output_limit)
                    .await?;
            }
            "json_pretty" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let value: Value = serde_json::from_str(&text)?;
                let rendered = serde_json::to_string_pretty(&value)? + "\n";
                write_transform_output(&node.id, &target_path, rendered.as_bytes(), output_limit)
                    .await?;
                value_output = Some(value);
            }
            "json_compact" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let value: Value = serde_json::from_str(&text)?;
                let rendered = serde_json::to_string(&value)? + "\n";
                write_transform_output(&node.id, &target_path, rendered.as_bytes(), output_limit)
                    .await?;
                value_output = Some(value);
            }
            "toml_to_json" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let value: toml::Value = toml::from_str(&text).map_err(|error| {
                    StepError::failed(&node.id, format!("invalid TOML: {error}"))
                })?;
                let value = serde_json::to_value(value)?;
                let rendered = serde_json::to_string_pretty(&value)? + "\n";
                write_transform_output(&node.id, &target_path, rendered.as_bytes(), output_limit)
                    .await?;
                value_output = Some(value);
            }
            "json_merge" => {
                let with_path = ctx
                    .run
                    .fs
                    .resolve_read(
                        ctx.render_inline(node, params.with.as_deref().expect("validated"))?
                            .as_str(),
                    )
                    .map_err(|error| {
                        StepError::failed(
                            &node.id,
                            format!("merge base is not in workspace: {error}"),
                        )
                    })?;
                let base_text = bounded_transform_text(&with_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let overlay_text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let overlay: Value = serde_json::from_str(&overlay_text)?;
                let mut base: Value = serde_json::from_str(&base_text)?;
                merge_json_objects(&mut base, &overlay);
                let rendered = serde_json::to_string_pretty(&base)?;
                let rendered = rendered + "\n";
                write_transform_output(&node.id, &target_path, rendered.as_bytes(), output_limit)
                    .await?;
                value_output = Some(base);
            }
            "json_to_toml" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let mut value: Value = serde_json::from_str(&text)?;
                strip_null_values(&mut value);
                let value: toml::Value = toml::Value::try_from(value).map_err(|error| {
                    StepError::failed(&node.id, format!("failed to convert JSON to TOML: {error}"))
                })?;
                let text = toml::to_string_pretty(&value).map_err(|error| {
                    StepError::failed(&node.id, format!("failed to encode TOML: {error}"))
                })?;
                write_transform_output(&node.id, &target_path, text.as_bytes(), output_limit)
                    .await?;
            }
            "base64_decode" => {
                let unix_mode = parse_unix_mode(node, rendered_unix_mode.as_deref())?;
                if params.remove_source && source_path == target_path {
                    return Err(StepError::failed(
                        &node.id,
                        "base64_decode remove_source cannot target the source file",
                    ));
                }
                let decoded_bytes = decode_base64_file_atomic(
                    &source_path,
                    &target_path,
                    transform_limit,
                    unix_mode,
                )
                .await
                .map_err(|error| StepError::failed(&node.id, error))?;
                if params.remove_source {
                    tokio::fs::remove_file(&source_path).await?;
                }
                value_output = Some(json!({ "bytes": decoded_bytes, "encoding": "binary" }));
            }
            "base64_encode" => {
                let source_bytes =
                    encode_base64_file_atomic(&source_path, &target_path, transform_limit)
                        .await
                        .map_err(|error| StepError::failed(&node.id, error))?;
                value_output = Some(json!({ "bytes": source_bytes, "encoding": "base64" }));
            }
            "zip" => {
                write_zip_atomic(
                    &node.id,
                    &source_path,
                    &target_path,
                    transform_limit,
                    ctx.run.contract.manifest.runtime.file_count_limit,
                )
                .await?
            }
            other => {
                return Err(StepError::failed(
                    &node.id,
                    format!("unsupported transform `{other}`"),
                ));
            }
        }
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": target, "transform": transform, "value": value_output })),
            files: vec![target_path],
        })
    }
}

fn transform_params(node: &NodeDef) -> Result<TransformParams, StepError> {
    let params: TransformParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid transform params: {error}"))
    })?;
    require(node, Some(&params.transform), "transform")?;
    require(node, Some(&params.source), "source")?;
    require(node, Some(&params.target), "target")?;
    if !matches!(
        params.transform.as_str(),
        "inject_secrets"
            | "json_pretty"
            | "json_compact"
            | "toml_to_json"
            | "json_to_toml"
            | "json_merge"
            | "base64_decode"
            | "base64_encode"
            | "zip"
    ) {
        return Err(StepError::failed(
            &node.id,
            format!("unsupported transform `{}`", params.transform),
        ));
    }
    Ok(params)
}

fn strict_base64_decode(encoded: &str) -> Result<Vec<u8>, String> {
    let decoded = BASE64.decode(encoded).map_err(|error| error.to_string())?;
    if BASE64.encode(&decoded) != encoded {
        return Err("value is not canonical padded base64".into());
    }
    Ok(decoded)
}

async fn bounded_transform_text(path: &camino::Utf8Path, limit: usize) -> Result<String, String> {
    let bytes = bounded_file_bytes(path, limit).await?;
    String::from_utf8(bytes).map_err(|error| format!("transform input is not valid UTF-8: {error}"))
}

async fn ensure_bounded_file_tree(
    path: &camino::Utf8Path,
    limit: usize,
    count_limit: usize,
) -> Result<(), String> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut total = 0_usize;
        let mut entries = 0_usize;
        if path.is_file() {
            let size = usize::try_from(
                std::fs::metadata(&path)
                    .map_err(|error| format!("failed to inspect file input: {error}"))?
                    .len(),
            )
            .map_err(|_| "file input size does not fit in usize".to_owned())?;
            if size > limit {
                return Err(format!("file input exceeds {limit} bytes"));
            }
            return Ok(());
        }
        if !path.is_dir() {
            return Err(format!("file input `{path}` is not a file or directory"));
        }
        for entry in WalkDir::new(&path).follow_links(false) {
            let entry = entry.map_err(|error| format!("failed to inspect file input: {error}"))?;
            entries = entries
                .checked_add(1)
                .ok_or_else(|| "file input entry count overflowed".to_owned())?;
            if entries > count_limit {
                return Err(format!(
                    "file input contains more than {count_limit} entries"
                ));
            }
            if !entry.file_type().is_file() {
                continue;
            }
            let size = usize::try_from(
                entry
                    .metadata()
                    .map_err(|error| format!("failed to inspect file input: {error}"))?
                    .len(),
            )
            .map_err(|_| "file input size does not fit in usize".to_owned())?;
            total = total
                .checked_add(size)
                .ok_or_else(|| "file input size overflowed".to_owned())?;
            if total > limit {
                return Err(format!("file input exceeds {limit} bytes"));
            }
        }
        Ok(())
    })
    .await
    .map_err(|error| format!("file input bound worker failed: {error}"))??;
    Ok(())
}

fn bounded_sha256_file(path: &camino::Utf8Path, limit: usize) -> Result<String, String> {
    let mut file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut total = 0_usize;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read)
            .ok_or_else(|| "file input size overflowed".to_owned())?;
        if total > limit {
            return Err(format!("file input exceeds {limit} bytes"));
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn bounded_file_bytes(path: &camino::Utf8Path, limit: usize) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|error| error.to_string())?;
    file.take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    if bytes.len() > limit {
        return Err(format!("source exceeds {limit} bytes"));
    }
    Ok(bytes)
}

fn atomic_temp_path(path: &camino::Utf8Path) -> camino::Utf8PathBuf {
    let file_name = path.file_name().unwrap_or("output");
    path.with_file_name(format!(
        ".{file_name}.qcg-part-{}",
        uuid::Uuid::now_v7().as_simple()
    ))
}

async fn decode_base64_file_atomic(
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: usize,
    unix_mode: Option<u32>,
) -> Result<usize, String> {
    let temporary = atomic_temp_path(target_path);
    let result = async {
        let mut source = tokio::fs::File::open(source_path)
            .await
            .map_err(|error| error.to_string())?;
        let mut target = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|error| error.to_string())?;
        let mut input_bytes = 0_usize;
        let mut output_bytes = 0_usize;
        let mut quartet = [0_u8; 4];
        let mut quartet_len = 0_usize;
        let mut finished = false;
        let mut chunk = [0_u8; 64 * 1024];
        loop {
            let read = source
                .read(&mut chunk)
                .await
                .map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            input_bytes = input_bytes
                .checked_add(read)
                .ok_or_else(|| "base64 input byte count overflowed".to_owned())?;
            if input_bytes > input_limit {
                return Err(format!("base64 source exceeds {input_limit} bytes"));
            }
            for byte in &chunk[..read] {
                if finished {
                    return Err("base64 data appeared after terminal padding".into());
                }
                quartet[quartet_len] = *byte;
                quartet_len += 1;
                if quartet_len == quartet.len() {
                    let (decoded, decoded_len, terminal) = decode_base64_quartet(&quartet)?;
                    target
                        .write_all(&decoded[..decoded_len])
                        .await
                        .map_err(|error| error.to_string())?;
                    output_bytes = output_bytes
                        .checked_add(decoded_len)
                        .ok_or_else(|| "base64 output byte count overflowed".to_owned())?;
                    if output_bytes > input_limit {
                        return Err(format!("base64 decoded output exceeds {input_limit} bytes"));
                    }
                    quartet_len = 0;
                    finished = terminal;
                }
            }
        }
        if quartet_len != 0 {
            return Err("base64 input length must be a multiple of four".into());
        }
        target.sync_all().await.map_err(|error| error.to_string())?;
        drop(target);
        drop(source);
        apply_unix_mode(&temporary, unix_mode)?;
        atomic_replace(&temporary, target_path)
            .await
            .map_err(|error| error.to_string())?;
        Ok(output_bytes)
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

fn decode_base64_quartet(quartet: &[u8; 4]) -> Result<([u8; 3], usize, bool), String> {
    let first = base64_value(quartet[0])?;
    let second = base64_value(quartet[1])?;
    if quartet[2] == b'=' {
        if quartet[3] != b'=' || second & 0x0f != 0 {
            return Err("non-canonical base64 padding".into());
        }
        return Ok(([first << 2 | second >> 4, 0, 0], 1, true));
    }
    let third = base64_value(quartet[2])?;
    if quartet[3] == b'=' {
        if third & 0x03 != 0 {
            return Err("non-canonical base64 padding".into());
        }
        return Ok((
            [first << 2 | second >> 4, second << 4 | third >> 2, 0],
            2,
            true,
        ));
    }
    let fourth = base64_value(quartet[3])?;
    Ok((
        [
            first << 2 | second >> 4,
            second << 4 | third >> 2,
            third << 6 | fourth,
        ],
        3,
        false,
    ))
}

fn base64_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err("base64 contains a non-alphabet character".into()),
    }
}

async fn encode_base64_file_atomic(
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: usize,
) -> Result<usize, String> {
    let temporary = atomic_temp_path(target_path);
    let result = async {
        let mut source = tokio::fs::File::open(source_path)
            .await
            .map_err(|error| error.to_string())?;
        let mut target = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|error| error.to_string())?;
        let mut input_bytes = 0_usize;
        let mut carry = Vec::with_capacity(2);
        let mut chunk = [0_u8; 64 * 1024];
        loop {
            let read = source
                .read(&mut chunk)
                .await
                .map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            input_bytes = input_bytes
                .checked_add(read)
                .ok_or_else(|| "base64 input byte count overflowed".to_owned())?;
            if input_bytes > input_limit {
                return Err(format!("base64 source exceeds {input_limit} bytes"));
            }
            let mut input = chunk[..read].to_vec();
            if !carry.is_empty() {
                let mut combined = std::mem::take(&mut carry);
                combined.append(&mut input);
                input = combined;
            }
            let complete = input.len() / 3 * 3;
            if complete != 0 {
                let encoded = BASE64.encode(&input[..complete]);
                target
                    .write_all(encoded.as_bytes())
                    .await
                    .map_err(|error| error.to_string())?;
            }
            carry.extend_from_slice(&input[complete..]);
        }
        if !carry.is_empty() {
            let encoded = BASE64.encode(&carry);
            target
                .write_all(encoded.as_bytes())
                .await
                .map_err(|error| error.to_string())?;
        }
        target.sync_all().await.map_err(|error| error.to_string())?;
        drop(target);
        drop(source);
        atomic_replace(&temporary, target_path)
            .await
            .map_err(|error| error.to_string())?;
        Ok(input_bytes)
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

async fn write_atomic(
    path: &camino::Utf8Path,
    bytes: &[u8],
    unix_mode: Option<u32>,
) -> Result<(), std::io::Error> {
    let temporary = atomic_temp_path(path);
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        drop(file);
        apply_unix_mode(&temporary, unix_mode).map_err(std::io::Error::other)?;
        atomic_replace(&temporary, path).await
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

async fn write_transform_output(
    node_id: &str,
    path: &camino::Utf8Path,
    bytes: &[u8],
    limit: usize,
) -> Result<(), StepError> {
    if bytes.len() > limit {
        return Err(StepError::failed(
            node_id,
            format!("transform output exceeds runtime.output_file_limit_bytes ({limit} bytes)"),
        ));
    }
    write_atomic(path, bytes, None)
        .await
        .map_err(|error| StepError::failed(node_id, error.to_string()))
}

async fn atomic_replace(
    temporary: &camino::Utf8Path,
    target: &camino::Utf8Path,
) -> Result<(), std::io::Error> {
    #[cfg(not(windows))]
    {
        tokio::fs::rename(temporary, target).await
    }
    #[cfg(windows)]
    {
        let temporary = temporary.to_owned();
        let target = target.to_owned();
        tokio::task::spawn_blocking(move || {
            use std::iter::once;
            use std::os::windows::ffi::OsStrExt as _;
            use windows_sys::Win32::Storage::FileSystem::{
                MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
            };
            let source = temporary
                .as_std_path()
                .as_os_str()
                .encode_wide()
                .chain(once(0))
                .collect::<Vec<_>>();
            let destination = target
                .as_std_path()
                .as_os_str()
                .encode_wide()
                .chain(once(0))
                .collect::<Vec<_>>();
            // SAFETY: both paths are NUL-terminated UTF-16 strings owned for this call.
            let moved = unsafe {
                MoveFileExW(
                    source.as_ptr(),
                    destination.as_ptr(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            };
            if moved == 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
        .await
        .map_err(std::io::Error::other)?
    }
}

fn validate_unix_mode_template(node: &NodeDef, mode: Option<&str>) -> Result<(), StepError> {
    let Some(mode) = mode else {
        return Ok(());
    };
    if mode.contains("{{") || mode.contains("{%") {
        return Ok(());
    }
    parse_unix_mode(node, Some(mode)).map(|_| ())
}

fn parse_unix_mode(node: &NodeDef, mode: Option<&str>) -> Result<Option<u32>, StepError> {
    let Some(mode) = mode else {
        return Ok(None);
    };
    let bytes = mode.as_bytes();
    if bytes.len() != 4
        || bytes[0] != b'0'
        || !bytes[1..].iter().all(|byte| (b'0'..=b'7').contains(byte))
    {
        return Err(StepError::failed(
            &node.id,
            "unix_mode must be a canonical four-digit octal string between 0600 and 0777",
        ));
    }
    let mode = u32::from_str_radix(
        std::str::from_utf8(&bytes[1..]).expect("validated ASCII octal bytes"),
        8,
    )
    .map_err(|_| StepError::failed(&node.id, "unix_mode is not valid octal"))?;
    if !(0o600..=0o777).contains(&mode) || mode & !0o777 != 0 {
        return Err(StepError::failed(
            &node.id,
            "unix_mode must be between 0600 and 0777 without special bits",
        ));
    }
    if !cfg!(unix) {
        return Err(StepError::failed(
            &node.id,
            "unix_mode is unsupported on non-Unix platforms",
        ));
    }
    Ok(Some(mode))
}

fn apply_unix_mode(path: &camino::Utf8Path, mode: Option<u32>) -> Result<(), String> {
    let Some(mode) = mode else {
        return Ok(());
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .map_err(|error| error.to_string())?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Err("unix_mode is unsupported on non-Unix platforms".into())
    }
}

/// Recursively merges `overlay` into `base`. Objects merge key-by-key with
/// `overlay` winning; every other value replaces the base value.
pub fn merge_json_objects(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, overlay_value) in overlay_map {
                match base_map.get_mut(key) {
                    Some(base_value) if base_value.is_object() && overlay_value.is_object() => {
                        merge_json_objects(base_value, overlay_value);
                    }
                    _ => {
                        base_map.insert(key.clone(), overlay_value.clone());
                    }
                }
            }
        }
        (base, overlay) => *base = overlay.clone(),
    }
}

/// Recursively removes all `null` values from a JSON structure so the
/// result can be safely converted to TOML, which has no null type.
fn strip_null_values(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                strip_null_values(v);
            }
        }
        Value::Array(arr) => {
            arr.retain(|v| !v.is_null());
            for v in arr.iter_mut() {
                strip_null_values(v);
            }
        }
        _ => {}
    }
}

async fn write_zip_atomic(
    node_id: &str,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: usize,
    count_limit: usize,
) -> Result<(), StepError> {
    let file_name = target_path.file_name().unwrap_or("archive.zip");
    let temporary = target_path.with_file_name(format!(
        ".{file_name}.qcg-part-{}",
        uuid::Uuid::now_v7().as_simple()
    ));
    let node_id_owned = node_id.to_owned();
    let source_path_owned = source_path.to_owned();
    let temporary_for_worker = temporary.clone();
    let result = async {
        tokio::task::spawn_blocking(move || {
            write_zip(
                &node_id_owned,
                &source_path_owned,
                &temporary_for_worker,
                input_limit,
                count_limit,
            )
        })
        .await
        .map_err(|error| StepError::failed(node_id, format!("zip worker failed: {error}")))??;
        atomic_replace(&temporary, target_path).await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

fn write_zip(
    node_id: &str,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: usize,
    count_limit: usize,
) -> Result<(), StepError> {
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(target_path)?;
    let mut writer = zip::ZipWriter::new(file);
    let mut input_bytes = 0_usize;
    if source_path.is_file() {
        if count_limit == 0 {
            return Err(StepError::failed(
                node_id,
                "zip source entry count exceeds the configured limit of 0",
            ));
        }
        let name = source_path.file_name().ok_or_else(|| {
            StepError::failed(node_id, format!("source `{source_path}` has no file name"))
        })?;
        let metadata = std::fs::metadata(source_path)?;
        writer
            .start_file(name, zip_file_options(node_id, &metadata)?)
            .map_err(|error| StepError::failed(node_id, error.to_string()))?;
        let mut source = std::fs::File::open(source_path)?;
        copy_bounded(
            &mut source,
            &mut writer,
            &mut input_bytes,
            input_limit,
            node_id,
        )?;
    } else if source_path.is_dir() {
        let mut entries = Vec::new();
        for entry in WalkDir::new(source_path).min_depth(1) {
            let entry = entry.map_err(|error| StepError::failed(node_id, error.to_string()))?;
            if entries.len() >= count_limit {
                return Err(StepError::failed(
                    node_id,
                    format!("zip source contains more than {count_limit} entries"),
                ));
            }
            entries.push(entry);
        }
        entries.sort_by(|left, right| left.path().cmp(right.path()));
        for entry in entries {
            let path = camino::Utf8PathBuf::from_path_buf(entry.path().to_path_buf())
                .map_err(|_| StepError::failed(node_id, "zip source path must be UTF-8"))?;
            if path == target_path {
                continue;
            }
            let rel = path
                .strip_prefix(source_path)
                .map_err(|error| StepError::failed(node_id, error.to_string()))?;
            let entry_name = portable_relative_path(rel);
            let metadata = entry
                .metadata()
                .map_err(|error| StepError::failed(node_id, error.to_string()))?;
            if entry.file_type().is_dir() {
                writer
                    .add_directory(
                        format!("{entry_name}/"),
                        zip_directory_options(node_id, &metadata)?,
                    )
                    .map_err(|error| StepError::failed(node_id, error.to_string()))?;
            } else if entry.file_type().is_file() {
                writer
                    .start_file(entry_name, zip_file_options(node_id, &metadata)?)
                    .map_err(|error| StepError::failed(node_id, error.to_string()))?;
                let mut source = std::fs::File::open(&path)?;
                copy_bounded(
                    &mut source,
                    &mut writer,
                    &mut input_bytes,
                    input_limit,
                    node_id,
                )?;
            }
        }
    } else {
        return Err(StepError::failed(
            node_id,
            format!("source `{source_path}` is not a file or directory"),
        ));
    }
    let file = writer
        .finish()
        .map_err(|error| StepError::failed(node_id, error.to_string()))?;
    file.sync_all()?;
    Ok(())
}

fn copy_bounded<R, W>(
    reader: &mut R,
    writer: &mut W,
    total: &mut usize,
    limit: usize,
    node_id: &str,
) -> Result<(), StepError>
where
    R: std::io::Read,
    W: std::io::Write,
{
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        *total = total
            .checked_add(read)
            .ok_or_else(|| StepError::failed(node_id, "transform input byte count overflowed"))?;
        if *total > limit {
            return Err(StepError::failed(
                node_id,
                format!("transform input exceeds {limit} bytes"),
            ));
        }
        writer.write_all(&buffer[..read])?;
    }
}

fn portable_relative_path(path: &camino::Utf8Path) -> String {
    path.components()
        .map(|component| component.as_str())
        .collect::<Vec<_>>()
        .join("/")
}

fn zip_file_options(
    node_id: &str,
    metadata: &std::fs::Metadata,
) -> Result<SimpleFileOptions, StepError> {
    Ok(zip_entry_options(node_id, metadata)?.compression_method(zip::CompressionMethod::Deflated))
}

fn zip_directory_options(
    node_id: &str,
    metadata: &std::fs::Metadata,
) -> Result<SimpleFileOptions, StepError> {
    Ok(zip_entry_options(node_id, metadata)?.compression_method(zip::CompressionMethod::Stored))
}

fn zip_entry_options(
    node_id: &str,
    metadata: &std::fs::Metadata,
) -> Result<SimpleFileOptions, StepError> {
    let modified = metadata.modified().map_err(|error| {
        StepError::failed(
            node_id,
            format!("failed to read source modification time: {error}"),
        )
    })?;
    let modified = chrono::DateTime::<chrono::Utc>::from(modified).naive_utc();
    let modified = zip::DateTime::try_from(modified).map_err(|error| {
        StepError::failed(
            node_id,
            format!("source modification time is not representable in ZIP: {error}"),
        )
    })?;
    let options = SimpleFileOptions::default().last_modified_time(modified);
    #[cfg(unix)]
    let options = {
        use std::os::unix::fs::PermissionsExt as _;
        options.unix_permissions(metadata.permissions().mode())
    };
    Ok(options)
}

struct CommandStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandParams {
    command: Vec<String>,
    #[serde(default)]
    input: Option<Value>,
    #[serde(default)]
    input_file: Option<String>,
    #[serde(default)]
    input_file_scope: CommandInputFileScope,
    #[serde(default)]
    result: CommandResultMode,
    #[serde(default)]
    output_schema: Option<Value>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CommandInputFileScope {
    #[default]
    Workspace,
    Package,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CommandResultMode {
    #[default]
    Process,
    Structured,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StructuredCommandResult {
    status: StructuredCommandStatus,
    output: Value,
    files: Vec<String>,
    #[serde(default)]
    findings: Vec<Finding>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StructuredCommandStatus {
    Success,
    CheckFailed,
}

#[async_trait]
impl StepExecutor for CommandStep {
    fn type_id(&self) -> &'static str {
        "command"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["command"],
            json!({
                "command": string_array_schema(),
                "input": {},
                "input_file": string_schema(),
                "input_file_scope": { "type": "string", "enum": ["workspace", "package"] },
                "result": { "type": "string", "enum": ["process", "structured"] },
                "output_schema": {},
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = command_params(node)?;
        if params.input.is_some() && params.input_file.is_some() {
            return Err(StepError::failed(
                &node.id,
                "command input and input_file are mutually exclusive",
            ));
        }
        if params.input_file.is_none()
            && !matches!(params.input_file_scope, CommandInputFileScope::Workspace)
        {
            return Err(StepError::failed(
                &node.id,
                "command input_file_scope requires input_file",
            ));
        }
        if let Some(schema) = &params.output_schema {
            validate_command_output_schema(node, schema)?;
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = command_params(node)?;
        let command = render_command(
            ctx,
            node,
            &params.command,
            ctx.run.contract.manifest.runtime.command_input_limit_bytes,
        )?;
        let target = command.join(" ");
        let plan = ctx
            .run
            .cmd
            .command_plan(&command)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        if let Some(confirm) =
            ctx.run
                .require_side_effect(ctx.journal, node, "command", &target, Some(plan))?
        {
            return Ok(StepOutcome::NeedsConfirm { confirm });
        }
        let input = command_input(ctx, node, &params).await?;
        let output = ctx
            .run
            .cmd
            .run_with_limits_and_stdin(
                &command,
                ctx.run.contract.manifest.runtime.command_timeout_seconds,
                ctx.run.contract.manifest.runtime.command_output_limit_bytes,
                input.as_deref(),
            )
            .await
            .map_err(|error| StepError::from_gateway(&node.id, error))?;
        if output.status != 0 {
            return Err(StepError::failed(
                &node.id,
                format!("command exited with {}", output.status),
            ));
        }
        match params.result {
            CommandResultMode::Process => Ok(StepOutcome::Success {
                output: Some(json!({
                    "status": output.status,
                    "stdout": command_output_value(&output.stdout_bytes, &output.stdout),
                    "stderr": command_output_value(&output.stderr_bytes, &output.stderr),
                })),
                files: vec![],
            }),
            CommandResultMode::Structured => {
                let stdout = std::str::from_utf8(&output.stdout_bytes).map_err(|error| {
                    StepError::failed(
                        &node.id,
                        format!("structured stdout is not valid UTF-8: {error}"),
                    )
                })?;
                let result: StructuredCommandResult =
                    serde_json::from_str(stdout).map_err(|error| {
                        StepError::failed(
                            &node.id,
                            format!("structured stdout is invalid: {error}"),
                        )
                    })?;
                if matches!(result.status, StructuredCommandStatus::CheckFailed)
                    && result.findings.is_empty()
                {
                    return Err(StepError::failed(
                        &node.id,
                        "structured check_failed status requires at least one finding",
                    ));
                }
                if let Some(schema) = &params.output_schema {
                    validate_command_value(node, schema, &result.output)?;
                }
                let files = resolve_command_result_files(ctx, node, &result.files)?;
                let output = json!({
                    "status": result.status,
                    "output": result.output,
                    "files": result.files,
                    "findings": result.findings,
                });
                match result.status {
                    StructuredCommandStatus::Success => Ok(StepOutcome::Success {
                        output: Some(output),
                        files,
                    }),
                    StructuredCommandStatus::CheckFailed => Ok(StepOutcome::CheckFailed {
                        findings: result.findings,
                        output: Some(output),
                        files,
                    }),
                }
            }
        }
    }
}

fn command_params(node: &NodeDef) -> Result<CommandParams, StepError> {
    let params: CommandParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid command params: {error}")))?;
    if params.command.is_empty() {
        return Err(StepError::failed(&node.id, "command must not be empty"));
    }
    Ok(params)
}

const MAX_COMMAND_RESULT_FILES: usize = 1024;

async fn command_input(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    params: &CommandParams,
) -> Result<Option<Vec<u8>>, StepError> {
    let Some(input_file) = &params.input_file else {
        let Some(input) = &params.input else {
            return Ok(None);
        };
        let limit = ctx.run.contract.manifest.runtime.command_input_limit_bytes;
        let rendered = render_json_templates(ctx, node, input, limit)?;
        let bytes = serde_json::to_vec(&rendered)?;
        let limit = ctx.run.contract.manifest.runtime.command_input_limit_bytes;
        if bytes.len() > limit {
            return Err(StepError::failed(
                &node.id,
                format!("command JSON input exceeds {limit} bytes"),
            ));
        }
        return Ok(Some(bytes));
    };
    let input_file = ctx.render_inline(node, input_file)?;
    let path = match params.input_file_scope {
        CommandInputFileScope::Workspace => {
            ctx.run.fs.resolve_read(&input_file).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("command workspace input file is not readable: {error}"),
                )
            })?
        }
        CommandInputFileScope::Package => {
            package_file(&ctx.run.contract, node, &input_file, "command input")?
        }
    };
    let limit = ctx.run.contract.manifest.runtime.command_input_limit_bytes;
    let mut bytes = Vec::new();
    tokio::fs::File::open(&path)
        .await?
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > limit {
        return Err(StepError::failed(
            &node.id,
            format!("command input file exceeds {limit} bytes"),
        ));
    }
    let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("command input file is not valid JSON: {error}"),
        )
    })?;
    let bytes = serde_json::to_vec(&value)?;
    if bytes.len() > limit {
        return Err(StepError::failed(
            &node.id,
            format!("canonical command JSON input exceeds {limit} bytes"),
        ));
    }
    Ok(Some(bytes))
}

fn render_json_templates(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    value: &Value,
    limit: usize,
) -> Result<Value, StepError> {
    let mut rendered_bytes = 0_usize;
    render_json_templates_inner(ctx, node, value, limit, &mut rendered_bytes)
}

fn render_json_templates_inner(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    value: &Value,
    limit: usize,
    rendered_bytes: &mut usize,
) -> Result<Value, StepError> {
    match value {
        Value::String(value) => {
            let rendered = ctx.render_inline(node, value)?;
            *rendered_bytes = rendered_bytes.checked_add(rendered.len()).ok_or_else(|| {
                StepError::failed(&node.id, "rendered JSON input size overflowed")
            })?;
            if *rendered_bytes > limit {
                return Err(StepError::failed(
                    &node.id,
                    format!("rendered JSON input exceeds {limit} bytes"),
                ));
            }
            Ok(Value::String(rendered))
        }
        Value::Array(values) => values
            .iter()
            .map(|value| render_json_templates_inner(ctx, node, value, limit, rendered_bytes))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.clone(),
                    render_json_templates_inner(ctx, node, value, limit, rendered_bytes)?,
                ))
            })
            .collect::<Result<serde_json::Map<_, _>, StepError>>()
            .map(Value::Object),
        value => Ok(value.clone()),
    }
}

fn command_output_value(bytes: &[u8], text: &str) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(_) => Value::String(text.to_owned()),
        Err(_) => json!({
            "encoding": "base64",
            "data": BASE64.encode(bytes),
        }),
    }
}

fn validate_command_output_schema(node: &NodeDef, schema: &Value) -> Result<(), StepError> {
    validate_bounded_json_schema(schema).map_err(|error| {
        StepError::failed(&node.id, format!("invalid command output_schema: {error}"))
    })
}

fn validate_command_value(node: &NodeDef, schema: &Value, value: &Value) -> Result<(), StepError> {
    let validator = jsonschema::validator_for(schema).map_err(|error| {
        StepError::failed(&node.id, format!("invalid command output_schema: {error}"))
    })?;
    if let Err(error) = validator.validate(value) {
        return Err(StepError::failed(
            &node.id,
            format!(
                "command structured output failed schema validation at `{}`: {error}",
                error.instance_path()
            ),
        ));
    }
    Ok(())
}

fn resolve_command_result_files(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    files: &[String],
) -> Result<Vec<camino::Utf8PathBuf>, StepError> {
    if files.len() > MAX_COMMAND_RESULT_FILES {
        return Err(StepError::failed(
            &node.id,
            format!("structured returned more than {MAX_COMMAND_RESULT_FILES} files"),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut resolved = Vec::with_capacity(files.len());
    for file in files {
        if !is_safe_relative_path(file) || !seen.insert(file.clone()) {
            return Err(StepError::failed(
                &node.id,
                format!("structured returned an unsafe or duplicate file path `{file}`"),
            ));
        }
        let path = ctx.run.fs.resolve_read(file).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("structured returned an unreadable file `{file}`: {error}"),
            )
        })?;
        if !path.is_file() {
            return Err(StepError::failed(
                &node.id,
                format!("structured returned a non-file output `{file}`"),
            ));
        }
        resolved.push(path);
    }
    Ok(resolved)
}

struct CheckCommandStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckCommandParams {
    command: Vec<String>,
    #[serde(default)]
    expect: Option<ExpectDef>,
}

#[async_trait]
impl StepExecutor for CheckCommandStep {
    fn type_id(&self) -> &'static str {
        "check.command"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["command"],
            json!({
                "command": string_array_schema(),
                "expect": {
                    "type": "object",
                    "properties": {
                        "exit_code": { "type": "integer" },
                        "exit_code_in": { "type": "array", "items": { "type": "integer" } },
                        "stdout_contains": string_schema(),
                        "stderr_contains": string_schema(),
                        "stdout_matches": string_schema(),
                    }
                }
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = check_command_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = check_command_params(node)?;
        let command = render_command(
            ctx,
            node,
            &params.command,
            ctx.run.contract.manifest.runtime.command_input_limit_bytes,
        )?;
        let output = ctx
            .run
            .cmd
            .run(&command)
            .await
            .map_err(|error| StepError::from_gateway(&node.id, error))?;
        let mut findings = Vec::new();
        if let Some(expect) = &params.expect {
            if let Some(exit_code) = expect.exit_code
                && output.status != exit_code
            {
                findings.push(Finding {
                    severity: Severity::Error,
                    message: format!("expected exit code {exit_code}, got {}", output.status),
                    location: None,
                    raw_output: Some(output.stderr.clone()),
                });
            }
            if !expect.exit_code_in.is_empty() && !expect.exit_code_in.contains(&output.status) {
                findings.push(Finding {
                    severity: Severity::Error,
                    message: format!(
                        "expected exit code in {:?}, got {}",
                        expect.exit_code_in, output.status
                    ),
                    location: None,
                    raw_output: Some(output.stderr.clone()),
                });
            }
            if let Some(needle) = &expect.stdout_contains
                && !output.stdout.contains(needle)
            {
                findings.push(Finding {
                    severity: Severity::Error,
                    message: format!("stdout did not contain `{needle}`"),
                    location: None,
                    raw_output: Some(output.stdout.clone()),
                });
            }
            if let Some(needle) = &expect.stderr_contains
                && !output.stderr.contains(needle)
            {
                findings.push(Finding {
                    severity: Severity::Error,
                    message: format!("stderr did not contain `{needle}`"),
                    location: None,
                    raw_output: Some(output.stderr.clone()),
                });
            }
            if let Some(pattern) = &expect.stdout_matches {
                let expression = regex::Regex::new(pattern).map_err(|error| {
                    StepError::failed(
                        &node.id,
                        format!("invalid expect.stdout_matches regex: {error}"),
                    )
                })?;
                if !expression.is_match(&output.stdout) {
                    findings.push(Finding {
                        severity: Severity::Error,
                        message: format!("stdout did not match `{pattern}`"),
                        location: None,
                        raw_output: Some(output.stdout.clone()),
                    });
                }
            }
        } else if output.status != 0 {
            findings.push(Finding {
                severity: Severity::Error,
                message: format!("command exited with {}", output.status),
                location: None,
                raw_output: Some(output.stderr.clone()),
            });
        }
        if findings.is_empty() {
            Ok(StepOutcome::Success {
                output: Some(json!({ "status": output.status, "stdout": output.stdout })),
                files: vec![],
            })
        } else {
            Ok(StepOutcome::CheckFailed {
                findings,
                output: None,
                files: vec![],
            })
        }
    }
}

fn check_command_params(node: &NodeDef) -> Result<CheckCommandParams, StepError> {
    let params: CheckCommandParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid check.command params: {error}"))
    })?;
    if params.command.is_empty() {
        return Err(StepError::failed(&node.id, "command must not be empty"));
    }
    Ok(params)
}

struct CheckToolStep;

#[derive(Debug, Clone)]
struct ToolBackendCandidate {
    kind: ToolBackendKind,
    argv: Vec<String>,
    container_image: Option<String>,
    container_mounts: Vec<ContainerMountSpec>,
}

#[derive(Debug, Clone)]
struct ContainerMountSpec {
    target: String,
    mode: String,
}

#[derive(Debug, Clone)]
struct ContainerOutput {
    runtime: String,
    status: i32,
    stdout: String,
    stderr: String,
}

#[async_trait]
impl StepExecutor for CheckToolStep {
    fn type_id(&self) -> &'static str {
        "check.tool"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["tool"],
            json!({
                "tool": string_schema(),
                "input": string_schema(),
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        parallel_safe_traits()
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = check_tool_params(node)?;
        let tool_name = params.tool.as_str();
        let tool = contract.manifest.tools.get(tool_name).ok_or_else(|| {
            StepError::failed(&node.id, format!("tool `{tool_name}` is not declared"))
        })?;
        if tool.kind != "validator" {
            return Err(StepError::failed(
                &node.id,
                format!("check.tool requires a validator tool, got `{}`", tool.kind),
            ));
        }
        let input = params.input.as_deref().or(tool.input.as_deref());
        require(node, input, "input")?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = check_tool_params(node)?;
        let tool_name = params.tool.as_str();
        let tool = ctx
            .run
            .contract
            .manifest
            .tools
            .get(tool_name)
            .expect("validated tool");
        let input = ctx.render_inline(
            node,
            params
                .input
                .as_deref()
                .or(tool.input.as_deref())
                .expect("validated input"),
        )?;
        if !matches!(tool.workspace, ToolWorkspace::None) {
            let input_path = ctx.run.fs.resolve_read(&input).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("tool input path is not readable in workspace: {error}"),
                )
            })?;
            ensure_bounded_file_tree(
                &input_path,
                ctx.run.contract.manifest.runtime.file_input_limit_bytes,
                ctx.run.contract.manifest.runtime.file_count_limit,
            )
            .await
            .map_err(|error| StepError::failed(&node.id, error))?;
        }
        if !matches!(tool.network, ToolNetwork::None) {
            return Err(StepError::failed(
                &node.id,
                "check.tool currently supports network = \"none\" only for local backends",
            ));
        }
        let order = tool_backend_order(tool);
        let mut unavailable = Vec::new();
        for (index, backend) in order.iter().enumerate() {
            let candidate = match build_tool_backend_candidate(ctx, node, tool, backend, &input) {
                Ok(candidate) => candidate,
                Err(reason) => {
                    unavailable.push(json!({ "backend": backend.to_string(), "reason": reason }));
                    if matches!(tool.resolution.fallback, ToolFallback::None) {
                        break;
                    }
                    continue;
                }
            };
            if requires_tool_backend_confirmation(&tool.resolution.fallback, index) {
                let confirm_id = format!("{}:tool_backend:{}", node.id, candidate.kind);
                if !ctx
                    .run
                    .confirmations
                    .get(&confirm_id)
                    .copied()
                    .unwrap_or(false)
                {
                    return Ok(StepOutcome::NeedsConfirm {
                        confirm: ConfirmSpec {
                            id: confirm_id,
                            title: format!(
                                "Confirm fallback to `{}` backend for tool `{tool_name}`",
                                candidate.kind
                            ),
                            kind: "tool_backend_fallback".into(),
                            target: format!("{tool_name}:{}", candidate.kind),
                            dry_run: false,
                            details: Some(json!({
                                "tool": tool_name,
                                "backend": candidate.kind.to_string(),
                                "unavailable": unavailable,
                            })),
                        },
                    });
                }
            }
            ctx.journal
                .event(
                    "tool_backend_resolved",
                    json!({
                        "node": node.id,
                        "tool": tool_name,
                        "backend": candidate.kind.to_string(),
                        "argv": candidate.argv,
                    }),
                )
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            return execute_tool_candidate(ctx, node, tool, candidate).await;
        }
        Ok(StepOutcome::CheckFailed {
            findings: vec![Finding {
                severity: Severity::Error,
                message: format!("tool `{tool_name}` could not resolve an allowed backend"),
                location: Some(node.id.clone()),
                raw_output: Some(Value::Array(unavailable).to_string()),
            }],
            output: None,
            files: vec![],
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckToolParams {
    tool: String,
    #[serde(default)]
    input: Option<String>,
}

fn check_tool_params(node: &NodeDef) -> Result<CheckToolParams, StepError> {
    let params: CheckToolParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid check.tool params: {error}"))
    })?;
    if params.tool.trim().is_empty() {
        return Err(StepError::failed(&node.id, "tool is required"));
    }
    Ok(params)
}

fn tool_backend_order(tool: &ToolDef) -> Vec<ToolBackendKind> {
    if !tool.resolution.preferred_backends.is_empty() {
        return tool.resolution.preferred_backends.clone();
    }
    if !tool.resolution.allowed_backends.is_empty() {
        return tool.resolution.allowed_backends.clone();
    }
    let mut order = Vec::new();
    if tool.backends.bundled.is_some() {
        order.push(ToolBackendKind::Bundled);
    }
    if tool.backends.container.is_some() {
        order.push(ToolBackendKind::Container);
    }
    if tool.backends.host.is_some() {
        order.push(ToolBackendKind::Host);
    }
    order
}

fn requires_tool_backend_confirmation(fallback: &ToolFallback, candidate_index: usize) -> bool {
    candidate_index > 0 && matches!(fallback, ToolFallback::Explicit)
}

fn build_tool_backend_candidate(
    ctx: &StepContext<'_>,
    _node: &NodeDef,
    tool: &ToolDef,
    backend: &ToolBackendKind,
    input: &str,
) -> Result<ToolBackendCandidate, String> {
    match backend {
        ToolBackendKind::Bundled => {
            let backend = tool
                .backends
                .bundled
                .as_ref()
                .ok_or_else(|| "bundled backend is not declared".to_string())?;
            let bin = ctx
                .run
                .templates
                .render_inline(
                    &backend.bin,
                    json!({
                        "os": current_os(),
                        "arch": current_arch(),
                    }),
                    &ctx.run.contract.manifest.runtime,
                )
                .map_err(|error| format!("failed to render bundled bin: {error}"))?;
            let path = ctx
                .run
                .contract
                .resolve_package_path(&bin)
                .map_err(|error| error.to_string())?;
            if !path.is_file() {
                return Err(format!("bundled binary `{bin}` was not found"));
            }
            let sha256 = bounded_sha256_file(
                &path,
                ctx.run.contract.manifest.runtime.file_input_limit_bytes,
            )?;
            if sha256 != backend.sha256 {
                return Err(format!(
                    "bundled binary `{bin}` sha256 mismatch: expected {}, got {sha256}",
                    backend.sha256
                ));
            }
            let argv = tool_argv(path.as_str(), &tool.command, input);
            Ok(ToolBackendCandidate {
                kind: ToolBackendKind::Bundled,
                argv,
                container_image: None,
                container_mounts: vec![],
            })
        }
        ToolBackendKind::Container => {
            let backend = tool
                .backends
                .container
                .as_ref()
                .ok_or_else(|| "container backend is not declared".to_string())?;
            if container_runtime_command(&ctx.run.contract.manifest.permissions.containers)
                .is_none()
            {
                return Err("container runtime was not found".into());
            }
            let mounted_input = if matches!(tool.workspace, ToolWorkspace::None) {
                input.to_string()
            } else {
                format!("{}/{}", backend.mount.trim_end_matches('/'), input)
            };
            let argv = tool_argv(
                tool.command.first().map(String::as_str).unwrap_or_default(),
                &tool.command,
                &mounted_input,
            );
            let container_mounts = if matches!(tool.workspace, ToolWorkspace::None) {
                vec![]
            } else {
                vec![ContainerMountSpec {
                    target: backend.mount.clone(),
                    mode: match tool.workspace {
                        ToolWorkspace::Writable => "rw".into(),
                        ToolWorkspace::ReadOnly | ToolWorkspace::None => "ro".into(),
                    },
                }]
            };
            Ok(ToolBackendCandidate {
                kind: ToolBackendKind::Container,
                argv,
                container_image: Some(backend.image.clone()),
                container_mounts,
            })
        }
        ToolBackendKind::Host => {
            let backend = tool
                .backends
                .host
                .as_ref()
                .ok_or_else(|| "host backend is not declared".to_string())?;
            if !binary_available(&backend.bin) {
                return Err(format!(
                    "host binary `{}` was not found on PATH",
                    backend.bin
                ));
            }
            let argv = tool_argv(&backend.bin, &tool.command, input);
            Ok(ToolBackendCandidate {
                kind: ToolBackendKind::Host,
                argv,
                container_image: None,
                container_mounts: vec![],
            })
        }
    }
}

fn tool_argv(bin: &str, command: &[String], input: &str) -> Vec<String> {
    let mut argv = vec![bin.to_string()];
    argv.extend(
        command
            .iter()
            .skip(1)
            .map(|arg| render_tool_arg(arg, input)),
    );
    argv
}

fn render_tool_arg(arg: &str, input: &str) -> String {
    arg.replace("{input}", input)
}

fn current_os() -> &'static str {
    std::env::consts::OS
}

fn current_arch() -> &'static str {
    std::env::consts::ARCH
}

fn binary_available(bin: &str) -> bool {
    if bin.contains('/') || bin.contains('\\') {
        return executable_file_exists(std::path::Path::new(bin));
    }
    std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|dir| executable_file_exists(&dir.join(bin)))
    })
}

fn executable_file_exists(path: &std::path::Path) -> bool {
    if path.is_file() {
        return true;
    }
    #[cfg(windows)]
    if path.extension().is_none() {
        let extensions =
            std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string());
        return extensions.split(';').any(|extension| {
            let extension = extension.trim().trim_start_matches('.');
            !extension.is_empty() && path.with_extension(extension).is_file()
        });
    }
    false
}

async fn execute_tool_candidate(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    tool: &ToolDef,
    candidate: ToolBackendCandidate,
) -> Result<StepOutcome, StepError> {
    match candidate.kind {
        ToolBackendKind::Host => {
            let output = ctx
                .run
                .cmd
                .run_with_limits(
                    &candidate.argv,
                    tool.timeout_seconds,
                    tool.output_limit_bytes,
                )
                .await
                .map_err(|error| StepError::from_gateway(&node.id, error))?;
            check_tool_output(
                node,
                &candidate.kind,
                output.status,
                output.stdout,
                output.stderr,
            )
        }
        ToolBackendKind::Bundled => {
            let output = ctx
                .spawn_process(
                    node,
                    &candidate.argv,
                    tool.timeout_seconds,
                    tool.output_limit_bytes,
                )
                .await?;
            check_tool_output(
                node,
                &candidate.kind,
                output.status,
                output.stdout,
                output.stderr,
            )
        }
        ToolBackendKind::Container => {
            let kind = candidate.kind.clone();
            let output = execute_container_backend_candidate(ctx, node, tool, candidate).await?;
            check_tool_output(node, &kind, output.status, output.stdout, output.stderr)
        }
    }
}

async fn execute_container_backend_candidate(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    tool: &ToolDef,
    candidate: ToolBackendCandidate,
) -> Result<ContainerOutput, StepError> {
    let (runtime, mut args) =
        container_runtime_command(&ctx.run.contract.manifest.permissions.containers)
            .ok_or_else(|| StepError::failed(&node.id, "container runtime was not found"))?;
    let image = candidate
        .container_image
        .ok_or_else(|| StepError::failed(&node.id, "container image was not resolved"))?;
    args.extend([
        "--rm".to_string(),
        "--network".to_string(),
        "none".to_string(),
        "--read-only".to_string(),
        "--cap-drop".to_string(),
        "ALL".to_string(),
        "--security-opt".to_string(),
        "no-new-privileges".to_string(),
        "--pids-limit".to_string(),
        "256".to_string(),
        "--tmpfs".to_string(),
        "/tmp:rw,noexec,nosuid,size=64m".to_string(),
    ]);
    let cidfile_name = format!(
        ".qcg-container-{}.cid",
        hex::encode(Sha256::digest(node.id.as_bytes()))
    );
    let cidfile = ctx.run.fs.workspace().join(&cidfile_name);
    args.push("--cidfile".into());
    args.push(cidfile.to_string());
    for mount in &candidate.container_mounts {
        args.push("-v".into());
        args.push(format!(
            "{}:{}:{}",
            ctx.run.fs.workspace(),
            mount.target,
            mount.mode
        ));
    }
    args.push(image);
    args.extend(candidate.argv);
    let argv = std::iter::once(runtime.clone())
        .chain(args)
        .collect::<Vec<_>>();
    let result = ctx
        .spawn_process(node, &argv, tool.timeout_seconds, tool.output_limit_bytes)
        .await;
    if result.as_ref().is_err_and(StepError::is_cancelled)
        && let Ok(bytes) = bounded_file_bytes(&cidfile, 1024).await
        && let Ok(container_id) = String::from_utf8(bytes)
    {
        ctx.kill_container(&runtime, container_id.trim()).await;
    }
    let _ = tokio::fs::remove_file(&cidfile).await;
    let output = result?;
    Ok(ContainerOutput {
        runtime,
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

fn check_tool_output(
    node: &NodeDef,
    backend: &ToolBackendKind,
    status: i32,
    stdout: String,
    stderr: String,
) -> Result<StepOutcome, StepError> {
    if status == 0 {
        Ok(StepOutcome::Success {
            output: Some(json!({
                "status": status,
                "backend": backend.to_string(),
                "stdout": stdout,
                "stderr": stderr,
            })),
            files: vec![],
        })
    } else {
        Ok(StepOutcome::CheckFailed {
            findings: vec![Finding {
                severity: Severity::Error,
                message: format!("tool backend `{backend}` exited with {status}"),
                location: Some(node.id.clone()),
                raw_output: Some(format!("{stdout}{stderr}")),
            }],
            output: None,
            files: vec![],
        })
    }
}

struct FailStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailParams {
    #[serde(default)]
    content: Option<String>,
}

#[async_trait]
impl StepExecutor for FailStep {
    fn type_id(&self) -> &'static str {
        "fail"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(&[], json!({ "content": string_schema() })))
    }

    async fn execute(
        &self,
        _ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = fail_params(node)?;
        Err(StepError::failed(
            &node.id,
            params.content.as_deref().unwrap_or("fail step reached"),
        ))
    }
}

fn fail_params(node: &NodeDef) -> Result<FailParams, StepError> {
    node.deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid fail params: {error}")))
}

struct ForeachStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForeachParams {
    items: String,
    subflow: String,
    max_iterations: usize,
    #[serde(default = "default_foreach_parallelism")]
    parallel: usize,
}

fn default_foreach_parallelism() -> usize {
    1
}

#[async_trait]
impl StepExecutor for ForeachStep {
    fn type_id(&self) -> &'static str {
        "foreach"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["items", "subflow", "max_iterations"],
            json!({
                "items": string_schema(),
                "subflow": string_schema(),
                "max_iterations": { "type": "integer", "minimum": 1, "maximum": MAX_FOREACH_ITERATIONS },
                "parallel": { "type": "integer", "minimum": 1, "maximum": MAX_FOREACH_PARALLELISM },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits {
            control_flow: StepControlFlow::Foreach,
            ..StepTraits::default()
        }
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = foreach_params(node)?;
        if !contract.manifest.blocks.contains_key(&params.subflow) {
            return Err(StepError::failed(
                &node.id,
                format!("unknown subflow `{}`", params.subflow),
            ));
        }
        Ok(())
    }

    async fn execute(
        &self,
        _ctx: &mut StepContext<'_>,
        _node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        Err(StepError::failed(
            "foreach",
            "foreach must be executed by the engine scheduler",
        ))
    }
}

fn foreach_params(node: &NodeDef) -> Result<ForeachParams, StepError> {
    let params: ForeachParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid foreach params: {error}")))?;
    require(node, Some(&params.items), "items")?;
    require(node, Some(&params.subflow), "subflow")?;
    if !(1..=MAX_FOREACH_ITERATIONS).contains(&params.max_iterations) {
        return Err(StepError::failed(
            &node.id,
            format!("max_iterations must be from 1 through {MAX_FOREACH_ITERATIONS}"),
        ));
    }
    if !(1..=MAX_FOREACH_PARALLELISM).contains(&params.parallel) {
        return Err(StepError::failed(
            &node.id,
            format!("parallel must be from 1 through {MAX_FOREACH_PARALLELISM}"),
        ));
    }
    Ok(params)
}

fn require(node: &NodeDef, value: Option<&str>, field: &str) -> Result<(), StepError> {
    if value.unwrap_or_default().is_empty() {
        Err(StepError::failed(&node.id, format!("{field} is required")))
    } else {
        Ok(())
    }
}

fn package_file(
    contract: &Contract,
    node: &NodeDef,
    relative: &str,
    role: &str,
) -> Result<camino::Utf8PathBuf, StepError> {
    let path = contract.resolve_package_path(relative).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("{role} package path `{relative}` is invalid: {error}"),
        )
    })?;
    if !path.is_file() {
        return Err(StepError::failed(
            &node.id,
            format!("{role} package path `{relative}` must be a file"),
        ));
    }
    Ok(path)
}

struct CheckContainerStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckContainerParams {
    command: Vec<String>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    mounts: Vec<MountDef>,
    #[serde(default)]
    expect: Option<ExpectDef>,
}

#[async_trait]
impl StepExecutor for CheckContainerStep {
    fn type_id(&self) -> &'static str {
        "check.container"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["command"],
            json!({
                "image": string_schema(),
                "content": string_schema(),
                "command": string_array_schema(),
                "mounts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["from", "to"],
                        "properties": {
                            "from": string_schema(),
                            "to": string_schema(),
                            "mode": { "type": "string", "enum": ["ro", "rw"] },
                        }
                    }
                },
                "expect": {
                    "type": "object",
                    "properties": {
                        "exit_code": { "type": "integer" },
                        "exit_code_in": { "type": "array", "items": { "type": "integer" } },
                        "stdout_contains": string_schema(),
                        "stderr_contains": string_schema(),
                        "stdout_matches": string_schema(),
                    }
                }
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = check_container_params(node)?;
        let image = check_container_image(node, &params)?;
        let containers = &contract.manifest.permissions.containers;
        if !containers.enabled {
            return Err(StepError::failed(
                &node.id,
                "permissions.containers.enabled must be true",
            ));
        }
        if !containers.images.iter().any(|allowed| allowed == image) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "container image `{image}` is not declared in permissions.containers.images"
                ),
            ));
        }
        for mount in &params.mounts {
            if mount.from != "workspace" {
                return Err(StepError::failed(
                    &node.id,
                    "check.container only supports mounts from `workspace`",
                ));
            }
            if mount.to.is_empty() || !mount.to.starts_with('/') || mount.to.contains('\0') {
                return Err(StepError::failed(
                    &node.id,
                    format!("container mount target `{}` is not allowed", mount.to),
                ));
            }
            if !matches!(mount.mode.as_deref().unwrap_or("ro"), "ro" | "rw") {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "container mount mode `{}` is not allowed",
                        mount.mode.as_deref().unwrap_or("")
                    ),
                ));
            }
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        if container_runtime_command(&ctx.run.contract.manifest.permissions.containers).is_none() {
            let on_missing = ctx
                .run
                .contract
                .manifest
                .permissions
                .containers
                .on_missing
                .as_deref()
                .unwrap_or("error");
            if matches!(on_missing, "skip" | "skip_with_warning") {
                return Ok(StepOutcome::Success {
                    output: Some(json!({
                        "status": "skipped",
                        "reason": "container runtime was not found",
                    })),
                    files: vec![],
                });
            }
            return Ok(StepOutcome::CheckFailed {
                findings: vec![Finding {
                    severity: Severity::Error,
                    message: "container runtime was not found".into(),
                    location: Some(node.id.clone()),
                    raw_output: None,
                }],
                output: None,
                files: vec![],
            });
        };

        let params = check_container_params(node)?;
        let image = check_container_image(node, &params)?;
        let command = render_command(
            ctx,
            node,
            &params.command,
            ctx.run.contract.manifest.runtime.command_input_limit_bytes,
        )?;
        let (tool, candidate) = check_container_backend_invocation(image, command, &params.mounts);
        ctx.journal
            .event(
                "tool_backend_resolved",
                json!({
                    "node": node.id,
                    "tool": "check.container",
                    "backend": "container",
                    "argv": candidate.argv.clone(),
                }),
            )
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;

        let output = execute_container_backend_candidate(ctx, node, &tool, candidate).await?;
        let mut findings = Vec::new();
        let status_matches = params.expect.as_ref().map_or(output.status == 0, |expect| {
            if !expect.exit_code_in.is_empty() {
                expect.exit_code_in.contains(&output.status)
            } else {
                output.status == expect.exit_code.unwrap_or(0)
            }
        });
        if !status_matches {
            findings.push(Finding {
                severity: Severity::Error,
                message: format!("container command exited with {}", output.status),
                location: Some(node.id.clone()),
                raw_output: Some(format!("{}{}", output.stdout, output.stderr)),
            });
        }
        if let Some(needle) = params
            .expect
            .as_ref()
            .and_then(|expect| expect.stdout_contains.as_ref())
            && !output.stdout.contains(needle)
        {
            findings.push(Finding {
                severity: Severity::Error,
                message: format!("stdout did not contain `{needle}`"),
                location: Some(node.id.clone()),
                raw_output: Some(output.stdout.clone()),
            });
        }
        if let Some(needle) = params
            .expect
            .as_ref()
            .and_then(|expect| expect.stderr_contains.as_ref())
            && !output.stderr.contains(needle)
        {
            findings.push(Finding {
                severity: Severity::Error,
                message: format!("stderr did not contain `{needle}`"),
                location: Some(node.id.clone()),
                raw_output: Some(output.stderr.clone()),
            });
        }
        if let Some(pattern) = params
            .expect
            .as_ref()
            .and_then(|expect| expect.stdout_matches.as_ref())
        {
            let expression = regex::Regex::new(pattern).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("invalid expect.stdout_matches regex: {error}"),
                )
            })?;
            if !expression.is_match(&output.stdout) {
                findings.push(Finding {
                    severity: Severity::Error,
                    message: format!("stdout did not match `{pattern}`"),
                    location: Some(node.id.clone()),
                    raw_output: Some(output.stdout.clone()),
                });
            }
        }
        if findings.is_empty() {
            Ok(StepOutcome::Success {
                output: Some(json!({
                    "status": output.status,
                    "runtime": output.runtime,
                    "image": image,
                    "stdout": output.stdout,
                    "stderr": output.stderr,
                })),
                files: vec![],
            })
        } else {
            Ok(StepOutcome::CheckFailed {
                findings,
                output: None,
                files: vec![],
            })
        }
    }
}

fn check_container_params(node: &NodeDef) -> Result<CheckContainerParams, StepError> {
    let params: CheckContainerParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid check.container params: {error}"))
    })?;
    if params.command.is_empty() {
        return Err(StepError::failed(&node.id, "command must not be empty"));
    }
    Ok(params)
}

fn check_container_image<'a>(
    node: &NodeDef,
    params: &'a CheckContainerParams,
) -> Result<&'a str, StepError> {
    params
        .image
        .as_deref()
        .or(params.content.as_deref())
        .ok_or_else(|| StepError::failed(&node.id, "image is required"))
}

fn check_container_backend_invocation(
    image: &str,
    command: Vec<String>,
    mounts: &[MountDef],
) -> (ToolDef, ToolBackendCandidate) {
    let container_mounts = if mounts.is_empty() {
        vec![ContainerMountSpec {
            target: "/work".into(),
            mode: "ro".into(),
        }]
    } else {
        mounts
            .iter()
            .map(|mount| ContainerMountSpec {
                target: mount.to.clone(),
                mode: mount.mode.as_deref().unwrap_or("ro").to_string(),
            })
            .collect()
    };
    let tool = ToolDef {
        kind: "validator".into(),
        input: None,
        command: command.clone(),
        network: ToolNetwork::None,
        workspace: ToolWorkspace::None,
        timeout_seconds: 60,
        output_limit_bytes: 1024 * 1024,
        resolution: ToolResolution::default(),
        backends: ToolBackends {
            container: Some(ContainerToolBackend {
                image: image.to_string(),
                mount: "/work".into(),
            }),
            ..ToolBackends::default()
        },
    };
    let candidate = ToolBackendCandidate {
        kind: ToolBackendKind::Container,
        argv: command,
        container_image: Some(image.to_string()),
        container_mounts,
    };
    (tool, candidate)
}

fn container_runtime_command(
    permission: &qcg_contract::ContainerPermission,
) -> Option<(String, Vec<String>)> {
    let runtime = permission.runtime.as_ref()?;
    let (binary, runtime_arg) = match runtime {
        ContainerRuntime::Docker => ("docker", None),
        ContainerRuntime::Podman => ("podman", None),
        ContainerRuntime::DockerRunsc => ("docker", Some("runsc")),
    };
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .any(|dir| dir.join(binary).is_file())
        .then(|| {
            let mut args = vec!["run".to_string()];
            if let Some(runtime) = runtime_arg {
                args.extend(["--runtime".into(), runtime.into()]);
            }
            (binary.to_string(), args)
        })
}

fn render_command(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    command: &[String],
    limit: usize,
) -> Result<Vec<String>, StepError> {
    let mut total = 0_usize;
    let mut rendered = Vec::with_capacity(command.len());
    for arg in command {
        let arg = ctx.render_inline(node, arg)?;
        if arg.contains('\0') {
            return Err(StepError::failed(
                &node.id,
                "rendered command arguments must not contain NUL bytes",
            ));
        }
        total = total
            .checked_add(arg.len().saturating_add(1))
            .ok_or_else(|| StepError::failed(&node.id, "rendered command size overflowed"))?;
        if total > limit {
            return Err(StepError::failed(
                &node.id,
                format!("rendered command arguments exceed {limit} bytes"),
            ));
        }
        rendered.push(arg);
    }
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;

    #[test]
    fn interactive_input_reader_accepts_exact_limit_and_rejects_larger_line() {
        let mut exact = std::io::Cursor::new(format!("{}\n", "x".repeat(8)));
        assert_eq!(
            read_bounded_line(&mut exact, 8).expect("exact limit should pass"),
            "xxxxxxxx"
        );
        let mut excessive = std::io::Cursor::new(format!("{}\n", "x".repeat(9)));
        let error = read_bounded_line(&mut excessive, 8)
            .expect_err("line above the limit must be rejected");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    fn test_contract(name: &str) -> (Contract, Utf8PathBuf) {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-steps-package-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after epoch")
                .as_nanos()
        )))
        .expect("temporary path should be UTF-8");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temporary package should be created");
        std::fs::write(
            root.join("qcg.toml"),
            r#"
[generator]
id = "step-package-validation"
version = "0.1.0"
qcg_version = "^0.1"
"#,
        )
        .expect("manifest should be written");
        let contract = Contract::load(&root).expect("test contract should load");
        (contract, root)
    }

    fn write_package_file(root: &Utf8PathBuf, relative: &str, content: &str) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("package parent should be created");
        }
        std::fs::write(path, content).expect("package file should be written");
    }

    fn package_node(node_id: &str, step_type: &str, params: &str) -> NodeDef {
        toml::from_str(&format!(
            "id = \"{node_id}\"\ntype = \"{step_type}\"\n[params]\n{params}"
        ))
        .expect("step node should parse")
    }

    #[test]
    fn foreach_rejects_iteration_and_parallelism_above_runtime_bounds() {
        let excessive_iterations = package_node(
            "foreach-iterations",
            "foreach",
            &format!(
                "items = \"inputs.items\"\nsubflow = \"item\"\nmax_iterations = {}",
                MAX_FOREACH_ITERATIONS + 1
            ),
        );
        let error = foreach_params(&excessive_iterations)
            .expect_err("excessive foreach iterations must fail validation");
        assert!(error.to_string().contains("max_iterations"), "{error}");

        let excessive_parallelism = package_node(
            "foreach-parallel",
            "foreach",
            &format!(
                "items = \"inputs.items\"\nsubflow = \"item\"\nmax_iterations = 1\nparallel = {}",
                MAX_FOREACH_PARALLELISM + 1
            ),
        );
        let error = foreach_params(&excessive_parallelism)
            .expect_err("excessive foreach parallelism must fail validation");
        assert!(error.to_string().contains("parallel"), "{error}");
    }

    #[test]
    fn package_backed_steps_validate_package_paths_before_execution() {
        let (contract, root) = test_contract("path");
        std::fs::create_dir_all(root.join("directory")).expect("package directory should exist");
        write_package_file(&root, "templates/good.j2", "Hello {{ inputs.name }}");
        write_package_file(&root, "source.txt", "source");
        write_package_file(&root, "schema.json", r#"{"type":"string"}"#);

        let render = package_node(
            "render",
            "render",
            "template = \"templates/good.j2\"\noutput_file = \"rendered.txt\"",
        );
        RenderStep
            .validate(&render, &contract)
            .expect("render template file should validate");
        let render_directory = package_node(
            "render-directory",
            "render",
            "template = \"directory\"\noutput_file = \"rendered.txt\"",
        );
        let error = RenderStep
            .validate(&render_directory, &contract)
            .expect_err("render directory must fail validation");
        assert!(error.to_string().contains("must be a file"), "{error}");

        let copy = package_node(
            "copy",
            "copy",
            "source = \"source.txt\"\ntarget = \"copied.txt\"",
        );
        CopyStep
            .validate(&copy, &contract)
            .expect("copy source file should validate");
        let copy_directory = package_node(
            "copy-directory",
            "copy",
            "source = \"directory\"\ntarget = \"copied\"",
        );
        let error = CopyStep
            .validate(&copy_directory, &contract)
            .expect_err("copy directory must fail validation");
        assert!(error.to_string().contains("must be a file"), "{error}");

        let check_schema = package_node(
            "check-schema",
            "check.schema",
            "source = \"value.json\"\nschema = \"schema.json\"",
        );
        CheckSchemaStep
            .validate(&check_schema, &contract)
            .expect("valid schema package file should validate");
        let check_schema_directory = package_node(
            "check-schema-directory",
            "check.schema",
            "source = \"value.json\"\nschema = \"directory\"",
        );
        let error = CheckSchemaStep
            .validate(&check_schema_directory, &contract)
            .expect_err("schema directory must fail validation");
        assert!(error.to_string().contains("must be a file"), "{error}");

        let missing = package_node(
            "missing",
            "copy",
            "source = \"missing.txt\"\ntarget = \"copied.txt\"",
        );
        let error = CopyStep
            .validate(&missing, &contract)
            .expect_err("missing package source must fail validation");
        assert!(
            error.to_string().contains("copy source package path"),
            "{error}"
        );
        std::fs::remove_dir_all(root).expect("temporary package should be removed");
    }

    #[test]
    fn check_schema_validation_parses_and_compiles_the_complete_schema() {
        let (contract, root) = test_contract("schema");
        write_package_file(&root, "value.json", "{}");
        let node = package_node(
            "check-schema",
            "check.schema",
            "source = \"value.json\"\nschema = \"schema.json\"",
        );

        write_package_file(&root, "schema.json", "not-json");
        let error = CheckSchemaStep
            .validate(&node, &contract)
            .expect_err("malformed schema JSON must fail validation");
        assert!(error.to_string().contains("not valid JSON"), "{error}");

        write_package_file(&root, "schema.json", r##"{"$ref":"#/missing"}"##);
        let error = CheckSchemaStep
            .validate(&node, &contract)
            .expect_err("unresolvable schema reference must fail validation");
        assert!(
            error.to_string().contains("invalid or unsafe JSON Schema"),
            "{error}"
        );

        std::fs::write(
            root.join("schema.json"),
            vec![b' '; MAX_JSON_SCHEMA_BYTES + 1],
        )
        .expect("oversized schema should be written");
        let error = CheckSchemaStep
            .validate(&node, &contract)
            .expect_err("oversized schema must fail validation");
        assert!(error.to_string().contains("byte limit"), "{error}");

        write_package_file(
            &root,
            "schema.json",
            r#"{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}"#,
        );
        CheckSchemaStep
            .validate(&node, &contract)
            .expect("complete valid schema should compile during validation");
        std::fs::remove_dir_all(root).expect("temporary package should be removed");
    }

    #[test]
    fn schema_validator_reports_missing_required_property() {
        let schema = json!({
            "type": "object",
            "required": ["name"],
            "properties": { "name": { "type": "string" } }
        });
        let value = json!({});
        let findings = validate_json_schema_findings(&schema, &value, "$");
        assert_eq!(findings.len(), 1);
    }

    #[test]
    fn schema_validator_recurses_into_properties() {
        let schema = json!({
            "type": "object",
            "properties": {
                "count": { "type": "integer" }
            }
        });
        let value = json!({ "count": "three" });
        let findings = validate_json_schema_findings(&schema, &value, "$");
        assert_eq!(findings[0].location.as_deref(), Some("$.count"));
    }

    #[test]
    fn explicit_tool_fallback_requires_confirmation_after_first_candidate() {
        assert!(!requires_tool_backend_confirmation(
            &ToolFallback::Explicit,
            0
        ));
        assert!(requires_tool_backend_confirmation(
            &ToolFallback::Explicit,
            1
        ));
    }

    #[test]
    fn disabled_tool_fallback_does_not_request_confirmation() {
        assert!(!requires_tool_backend_confirmation(&ToolFallback::None, 1));
    }

    #[test]
    fn ask_user_preserves_localized_option_labels_without_changing_values() {
        let node: NodeDef = toml::from_str(
            r#"
id = "choose"
type = "ask_user"
[params]
content = "Choose a mode."
content_i18n = { ja = "モードを選択してください。" }
options = ["automatic", "manual"]
default = "automatic"
option_labels_i18n = { ja = { automatic = "自動", manual = "手動" } }
"#,
        )
        .unwrap();
        let params = ask_user_params(&node).unwrap();
        assert_eq!(
            params.content_i18n.get("ja").map(String::as_str),
            Some("モードを選択してください。")
        );
        let fields = ask_user_fields(&params);
        assert_eq!(fields[0].options, ["automatic", "manual"]);
        assert_eq!(fields[0].default, Some(json!("automatic")));
        assert_eq!(fields[0].option_labels_i18n["ja"]["automatic"], "自動");
    }

    #[test]
    fn ask_user_rejects_a_default_outside_options() {
        let node: NodeDef = toml::from_str(
            r#"
id = "choose"
type = "ask_user"
[params]
content = "Choose a mode."
options = ["automatic", "manual"]
default = "invalid"
"#,
        )
        .unwrap();
        let params = ask_user_params(&node).unwrap();
        let error = validate_answer(
            &node,
            &params.options,
            params.default_answer.as_deref().unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside declared options"));
    }

    #[test]
    fn check_container_invocation_uses_container_backend_candidate_with_default_mount() {
        let node: NodeDef = toml::from_str(
            r#"
id = "container_check"
type = "check.container"
[params]
command = ["sh", "-c", "echo ok"]
"#,
        )
        .unwrap();
        let params = check_container_params(&node).unwrap();
        let (tool, candidate) =
            check_container_backend_invocation("alpine:3.20", params.command, &params.mounts);
        assert_eq!(tool.workspace, ToolWorkspace::None);
        assert_eq!(tool.timeout_seconds, 60);
        assert_eq!(tool.output_limit_bytes, 1024 * 1024);
        assert_eq!(candidate.kind, ToolBackendKind::Container);
        assert_eq!(candidate.container_image.as_deref(), Some("alpine:3.20"));
        assert_eq!(candidate.container_mounts.len(), 1);
        assert_eq!(candidate.container_mounts[0].target, "/work");
        assert_eq!(candidate.container_mounts[0].mode, "ro");
    }

    #[test]
    fn check_container_invocation_preserves_declared_mounts() {
        let node: NodeDef = toml::from_str(
            r#"
id = "container_check"
type = "check.container"
[params]
command = ["sh", "-c", "echo ok"]

[[params.mounts]]
from = "workspace"
to = "/src"
mode = "rw"
"#,
        )
        .unwrap();
        let params = check_container_params(&node).unwrap();
        let (_tool, candidate) =
            check_container_backend_invocation("alpine:3.20", params.command, &params.mounts);
        assert_eq!(candidate.container_mounts.len(), 1);
        assert_eq!(candidate.container_mounts[0].target, "/src");
        assert_eq!(candidate.container_mounts[0].mode, "rw");
    }

    #[test]
    fn contract_validation_reports_misspelled_param_with_line_number() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path must be UTF-8")
            .join(format!("qcg-param-line-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("generator directory should be created");
        std::fs::write(
            root.join("qcg.toml"),
            r#"[generator]
id = "typo"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "emit"
type = "write"
[flow.params]
tempalte = "x"
output_file = "out.txt"
"#,
        )
        .expect("manifest should be written");
        let contract = Contract::load(&root).expect("common contract fields should load");
        let error = deterministic_registry()
            .validate_contract(&contract)
            .expect_err("misspelled params must fail validation");
        assert!(error.to_string().contains("line 10"));
        assert!(error.to_string().contains("tempalte"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn base64_file_transforms_round_trip_and_preserve_target_on_failure() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path must be UTF-8")
            .join(format!("qcg-base64-file-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temporary directory should be created");
        let source = root.join("source.bin");
        let encoded = root.join("encoded.txt");
        let decoded = root.join("decoded.bin");
        let original = b"binary\0payload\xff";
        std::fs::write(&source, original).expect("source should be written");
        let source_bytes = encode_base64_file_atomic(&source, &encoded, 1024)
            .await
            .expect("base64 encoding should succeed");
        assert_eq!(source_bytes, original.len());
        let decoded_bytes = decode_base64_file_atomic(&encoded, &decoded, 1024, None)
            .await
            .expect("base64 decoding should succeed");
        assert_eq!(decoded_bytes, original.len());
        assert_eq!(
            std::fs::read(&decoded).expect("decoded file should be readable"),
            original
        );

        std::fs::write(&encoded, "not canonical*").expect("invalid source should be written");
        std::fs::write(&decoded, b"previous output").expect("existing target should be written");
        let error = decode_base64_file_atomic(&encoded, &decoded, 1024, None)
            .await
            .expect_err("invalid base64 must fail");
        assert!(error.contains("base64"));
        assert_eq!(
            std::fs::read(&decoded).expect("existing target should remain readable"),
            b"previous output"
        );
        assert!(
            std::fs::read_dir(&root)
                .expect("temporary directory should be readable")
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().contains("qcg-part-")),
            "failed transforms must not leave partial files"
        );

        let inplace = root.join("inplace");
        std::fs::write(&inplace, BASE64.encode(original))
            .expect("in-place source should be written");
        decode_base64_file_atomic(&inplace, &inplace, 1024, None)
            .await
            .expect("in-place base64 decode should succeed");
        assert_eq!(
            std::fs::read(&inplace).expect("in-place output should be readable"),
            original
        );
        encode_base64_file_atomic(&inplace, &inplace, 1024)
            .await
            .expect("in-place base64 encode should succeed");
        assert_eq!(
            std::fs::read_to_string(&inplace).expect("in-place encoded output should be readable"),
            BASE64.encode(original)
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn base64_decode_applies_requested_unix_mode_after_commit() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path must be UTF-8")
            .join(format!("qcg-base64-mode-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temporary directory should be created");
        let source = root.join("source.txt");
        let target = root.join("script");
        std::fs::write(&source, "c2V0IC1l").expect("source should be written");
        decode_base64_file_atomic(&source, &target, 1024, Some(0o750))
            .await
            .expect("base64 decoding should succeed");
        assert_eq!(
            std::fs::metadata(&target)
                .expect("target metadata should be available")
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[cfg(not(unix))]
    #[test]
    fn unix_mode_is_rejected_instead_of_being_silently_ignored() {
        let error = apply_unix_mode(camino::Utf8Path::new("unused"), Some(0o750))
            .expect_err("non-Unix platforms cannot satisfy Unix permission constraints");
        assert_eq!(error, "unix_mode is unsupported on non-Unix platforms");
    }

    #[test]
    fn zip_transform_preserves_directory_entries_metadata_and_contents() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path must be UTF-8")
            .join(format!("qcg-zip-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let source = root.join("source");
        std::fs::create_dir_all(source.join("empty")).expect("empty directory should be created");
        std::fs::create_dir_all(source.join("nested")).expect("nested directory should be created");
        std::fs::write(source.join("nested/file.txt"), "archive content")
            .expect("source file should be written");
        let modified = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_704_067_200);
        std::fs::File::options()
            .write(true)
            .open(source.join("nested/file.txt"))
            .expect("source file should open")
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .expect("source modification time should be set");
        let target = root.join("result.zip");

        write_zip("archive", &source, &target, 1024 * 1024, 100).expect("zip should be written");
        let file = std::fs::File::open(&target).expect("zip should open");
        let mut archive = zip::ZipArchive::new(file).expect("zip should parse");
        assert!(
            archive
                .by_name("empty/")
                .expect("empty directory entry")
                .is_dir()
        );
        assert!(
            archive
                .by_name("nested/")
                .expect("nested directory entry")
                .is_dir()
        );
        let mut entry = archive.by_name("nested/file.txt").expect("file entry");
        let archived_modified = entry.last_modified().expect("file timestamp");
        assert_eq!(archived_modified.year(), 2024);
        assert_eq!(archived_modified.month(), 1);
        assert_eq!(archived_modified.day(), 1);
        let mut content = String::new();
        std::io::Read::read_to_string(&mut entry, &mut content)
            .expect("file entry should be readable");
        assert_eq!(content, "archive content");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn direct_mcp_source_extraction_keeps_text_and_resource_sources() {
        let result = json!({
            "content": [
                {
                    "type": "text",
                    "text": "Research source: https://example.test/search?q=rust&token=secret#part"
                },
                {
                    "type": "resource_link",
                    "name": "Reference",
                    "uri": "https://example.test/docs?page=2"
                }
            ]
        });
        let sources = tool_call_sources(&result);
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0]["url"], "https://example.test/docs?page=2");
        assert_eq!(sources[0]["title"], "Reference");
        assert_eq!(sources[1]["url"], "https://example.test/search?q=rust");
    }
}
