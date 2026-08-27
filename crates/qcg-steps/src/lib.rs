use async_trait::async_trait;
use minijinja::Environment;
use qcg_contract::{
    ContainerToolBackend, Contract, ExpectDef, MountDef, NodeDef, ToolBackendKind, ToolBackends,
    ToolDef, ToolFallback, ToolNetwork, ToolResolution, ToolWorkspace, validate_form_values,
};
use qcg_engine::{
    ConfirmSpec, FieldType, Finding, FormSpec, HttpRequest, InputField, Severity, StepContext,
    StepControlFlow, StepError, StepExecutor, StepOutcome, StepRegistry, StepTraits,
    validate_json_schema_findings,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::io::Write as _;
use walkdir::WalkDir;
use zip::write::SimpleFileOptions;

pub fn deterministic_registry() -> StepRegistry {
    let mut registry = StepRegistry::new();
    registry.register(RenderStep);
    registry.register(WriteStep);
    registry.register(CopyStep);
    registry.register(TransformStep);
    registry.register(CommandStep);
    registry.register(HttpStep);
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
    options: Vec<String>,
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
                "options": string_array_schema(),
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
        if let Some(answer) = ctx.run.answers.get(&node.id) {
            if !params.fields.is_empty() {
                validate_form_values(&params.fields, answer).map_err(|error| {
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

        let answer = prompt_for_answer(node, &params.options, &title)?;
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
        vec![answer_field(FieldType::Select, params.options.clone())]
    }
}

fn answer_field(kind: FieldType, options: Vec<String>) -> InputField {
    InputField {
        id: "answer".into(),
        kind,
        required: true,
        default: None,
        pattern: None,
        options,
        min_items: None,
        item_type: None,
    }
}

fn prompt_for_answer(node: &NodeDef, options: &[String], title: &str) -> Result<String, StepError> {
    eprintln!("{title}");
    if !options.is_empty() {
        eprintln!("Options:");
        for option in options {
            eprintln!("  - {option}");
        }
    }
    eprint!("> ");
    std::io::stderr()
        .flush()
        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
    let answer = answer.trim().to_string();
    validate_answer(node, options, &answer)?;
    Ok(answer)
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
    content: Option<String>,
    #[serde(default)]
    output_file: Option<String>,
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
                "content": string_schema(),
                "output_file": string_schema(),
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = http_params(node)?;
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
        let body = params
            .content
            .as_deref()
            .map(|content| ctx.render_inline(node, content))
            .transpose()?;
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
                body,
            })
            .await
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let mut files = Vec::new();
        if let Some(output_file) = &params.output_file {
            let output_file = ctx.render_inline(node, output_file)?;
            let path = ctx
                .run
                .fs
                .resolve_write(&output_file)
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            tokio::fs::write(&path, &response.body).await?;
            files.push(path);
        }
        Ok(StepOutcome::Success {
            output: Some(json!({
                "status": response.status,
                "url": response.url,
                "headers": response.headers,
                "body": response.body,
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

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = check_schema_params(node)?;
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
        let value_source = tokio::fs::read_to_string(&source_path).await?;
        let value: Value = serde_json::from_str(&value_source)
            .map_err(|error| StepError::failed(&node.id, format!("invalid JSON: {error}")))?;
        let schema_path = ctx.run.contract.root.join(&params.schema);
        let schema_source = std::fs::read_to_string(schema_path)?;
        let schema: Value = serde_json::from_str(&schema_source)?;
        let findings = validate_json_schema_findings(&schema, &value, "$");
        if findings.is_empty() {
            Ok(StepOutcome::Success {
                output: Some(json!({ "status": "pass", "source": source })),
                files: vec![],
            })
        } else {
            Ok(StepOutcome::CheckFailed { findings })
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
        let text = tokio::fs::read_to_string(&source_path).await?;
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

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = render_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = render_params(node)?;
        let output_file = ctx.render_inline(node, &params.output_file)?;
        let source = std::fs::read_to_string(ctx.run.contract.root.join(&params.template))?;
        let rendered = ctx
            .run
            .templates
            .render_inline(&source, ctx.vars.to_json())
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let path = ctx
            .run
            .fs
            .resolve_write(&output_file)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        tokio::fs::write(&path, rendered).await?;
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
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        parallel_safe_traits()
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = write_params(node)?;
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
        tokio::fs::write(&path, content).await?;
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

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = copy_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = copy_params(node)?;
        let source = ctx.run.contract.root.join(&params.source);
        let target_name = ctx.render_inline(node, &params.target)?;
        let target = ctx
            .run
            .fs
            .resolve_write(&target_name)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        tokio::fs::copy(source, &target).await?;
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
                        "zip"
                    ]
                },
                "source": string_schema(),
                "target": string_schema(),
                "with": string_schema(),
                "secrets": string_array_schema(),
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
        let mut value_output = None;
        match transform {
            "inject_secrets" => {
                let text = tokio::fs::read_to_string(&source_path).await?;
                let injected = ctx
                    .run
                    .secrets
                    .inject_declared_placeholders(&text, &params.secrets)
                    .map_err(|error| StepError::failed(&node.id, error))?;
                tokio::fs::write(&target_path, injected).await?;
            }
            "json_pretty" => {
                let text = tokio::fs::read_to_string(&source_path).await?;
                let value: Value = serde_json::from_str(&text)?;
                tokio::fs::write(&target_path, serde_json::to_string_pretty(&value)? + "\n")
                    .await?;
                value_output = Some(value);
            }
            "json_compact" => {
                let text = tokio::fs::read_to_string(&source_path).await?;
                let value: Value = serde_json::from_str(&text)?;
                tokio::fs::write(&target_path, serde_json::to_string(&value)? + "\n").await?;
                value_output = Some(value);
            }
            "toml_to_json" => {
                let text = tokio::fs::read_to_string(&source_path).await?;
                let value: toml::Value = toml::from_str(&text).map_err(|error| {
                    StepError::failed(&node.id, format!("invalid TOML: {error}"))
                })?;
                let value = serde_json::to_value(value)?;
                tokio::fs::write(&target_path, serde_json::to_string_pretty(&value)? + "\n")
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
                let base_text = tokio::fs::read_to_string(&with_path).await?;
                let overlay_text = tokio::fs::read_to_string(&source_path).await?;
                let overlay: Value = serde_json::from_str(&overlay_text)?;
                let mut base: Value = serde_json::from_str(&base_text)?;
                merge_json_objects(&mut base, &overlay);
                let rendered = serde_json::to_string_pretty(&base)?;
                tokio::fs::write(&target_path, rendered + "\n").await?;
                value_output = Some(base);
            }
            "json_to_toml" => {
                let text = tokio::fs::read_to_string(&source_path).await?;
                let mut value: Value = serde_json::from_str(&text)?;
                strip_null_values(&mut value);
                let value: toml::Value = toml::Value::try_from(value).map_err(|error| {
                    StepError::failed(&node.id, format!("failed to convert JSON to TOML: {error}"))
                })?;
                let text = toml::to_string_pretty(&value).map_err(|error| {
                    StepError::failed(&node.id, format!("failed to encode TOML: {error}"))
                })?;
                tokio::fs::write(&target_path, text).await?;
            }
            "zip" => write_zip(&node.id, &source_path, &target_path)?,
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
            | "zip"
    ) {
        return Err(StepError::failed(
            &node.id,
            format!("unsupported transform `{}`", params.transform),
        ));
    }
    Ok(params)
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

fn write_zip(
    node_id: &str,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
) -> Result<(), StepError> {
    let file = std::fs::File::create(target_path)?;
    let mut writer = zip::ZipWriter::new(file);
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    if source_path.is_file() {
        let name = source_path.file_name().ok_or_else(|| {
            StepError::failed(node_id, format!("source `{source_path}` has no file name"))
        })?;
        writer
            .start_file(name, options)
            .map_err(|error| StepError::failed(node_id, error.to_string()))?;
        let bytes = std::fs::read(source_path)?;
        writer.write_all(&bytes)?;
    } else if source_path.is_dir() {
        for entry in WalkDir::new(source_path)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
        {
            let path = camino::Utf8PathBuf::from_path_buf(entry.path().to_path_buf())
                .map_err(|_| StepError::failed(node_id, "zip source path must be UTF-8"))?;
            if path == target_path {
                continue;
            }
            let rel = path
                .strip_prefix(source_path)
                .map_err(|error| StepError::failed(node_id, error.to_string()))?;
            writer
                .start_file(rel.as_str(), options)
                .map_err(|error| StepError::failed(node_id, error.to_string()))?;
            let bytes = std::fs::read(&path)?;
            writer.write_all(&bytes)?;
        }
    } else {
        return Err(StepError::failed(
            node_id,
            format!("source `{source_path}` is not a file or directory"),
        ));
    }
    writer
        .finish()
        .map_err(|error| StepError::failed(node_id, error.to_string()))?;
    Ok(())
}

struct CommandStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandParams {
    command: Vec<String>,
}

#[async_trait]
impl StepExecutor for CommandStep {
    fn type_id(&self) -> &'static str {
        "command"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["command"],
            json!({ "command": string_array_schema() }),
        ))
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = command_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = command_params(node)?;
        let command = render_command(ctx, node, &params.command)?;
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
        let output = ctx
            .run
            .cmd
            .run(&command)
            .await
            .map_err(|error| StepError::from_gateway(&node.id, error))?;
        if output.status != 0 {
            return Err(StepError::failed(
                &node.id,
                format!("command exited with {}", output.status),
            ));
        }
        Ok(StepOutcome::Success {
            output: Some(
                json!({ "status": output.status, "stdout": output.stdout, "stderr": output.stderr }),
            ),
            files: vec![],
        })
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
        let command = render_command(ctx, node, &params.command)?;
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
            Ok(StepOutcome::CheckFailed { findings })
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
            ctx.run.fs.resolve_read(&input).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("tool input path is not readable in workspace: {error}"),
                )
            })?;
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
            let bin = render_tool_path(&backend.bin)
                .map_err(|error| format!("failed to render bundled bin: {error}"))?;
            let path = ctx.run.contract.root.join(&bin);
            if !path.is_file() {
                return Err(format!("bundled binary `{bin}` was not found"));
            }
            let bytes = std::fs::read(&path)
                .map_err(|error| format!("failed to read bundled binary `{bin}`: {error}"))?;
            let sha256 = hex::encode(Sha256::digest(&bytes));
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
            if find_container_runtime().is_none() {
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

fn render_tool_path(path: &str) -> Result<String, minijinja::Error> {
    let mut env = Environment::new();
    env.add_template("path", path)?;
    env.get_template("path")?.render(json!({
        "os": current_os(),
        "arch": current_arch(),
    }))
}

fn current_os() -> &'static str {
    std::env::consts::OS
}

fn current_arch() -> &'static str {
    std::env::consts::ARCH
}

fn binary_available(bin: &str) -> bool {
    if bin.contains('/') || bin.contains('\\') {
        return std::path::Path::new(bin).is_file();
    }
    std::env::var_os("PATH")
        .is_some_and(|paths| std::env::split_paths(&paths).any(|dir| dir.join(bin).is_file()))
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
    let runtime = find_container_runtime()
        .ok_or_else(|| StepError::failed(&node.id, "container runtime was not found"))?;
    let image = candidate
        .container_image
        .ok_or_else(|| StepError::failed(&node.id, "container image was not resolved"))?;
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--network".to_string(),
        "none".to_string(),
    ];
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
        && let Ok(container_id) = tokio::fs::read_to_string(&cidfile).await
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
                "max_iterations": { "type": "integer", "minimum": 1 },
                "parallel": { "type": "integer", "minimum": 1 },
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
    if params.max_iterations == 0 {
        return Err(StepError::failed(
            &node.id,
            "max_iterations must be greater than zero",
        ));
    }
    if params.parallel == 0 {
        return Err(StepError::failed(
            &node.id,
            "parallel must be greater than zero",
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
        if find_container_runtime().is_none() {
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
            });
        };

        let params = check_container_params(node)?;
        let image = check_container_image(node, &params)?;
        let command = render_command(ctx, node, &params.command)?;
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
            Ok(StepOutcome::CheckFailed { findings })
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

fn find_container_runtime() -> Option<String> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|dir| {
            for candidate in ["docker", "podman"] {
                let path = dir.join(candidate);
                if path.is_file() {
                    return Some(candidate.to_string());
                }
            }
            None
        })
    })
}

fn render_command(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    command: &[String],
) -> Result<Vec<String>, StepError> {
    command
        .iter()
        .map(|arg| ctx.render_inline(node, arg))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;

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
    fn non_explicit_tool_fallback_does_not_request_confirmation() {
        assert!(!requires_tool_backend_confirmation(&ToolFallback::None, 1));
        assert!(!requires_tool_backend_confirmation(
            &ToolFallback::Automatic,
            1
        ));
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
}
