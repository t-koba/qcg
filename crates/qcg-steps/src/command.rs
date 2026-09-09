use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome};
use qcg_policy::{
    is_safe_relative_path, params_schema, string_array_schema, string_schema,
    validate_bounded_json_schema,
};
use qcg_types::Finding;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt as _;

use super::common::{package_file, render_command, render_json_templates};
use qcg_policy::MAX_COMMAND_RESULT_FILES;

pub(crate) struct CommandStep;

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
        if let Some(confirm) = ctx.run.require_side_effect(
            ctx.journal,
            node,
            "command",
            &target,
            Some(plan.clone()),
        )? {
            return Ok(StepOutcome::NeedsConfirm { confirm });
        }
        let details = Some(plan.clone());
        let digest = qcg_engine::RunContext::operation_digest(&target, &details)?;
        let invocation = qcg_engine::content_invocation_id(&digest);
        // Same-invocation resends deserialize the cached native output and
        // continue through the identical tail below, so outputs, files, and
        // check decisions match the original execution exactly.
        let (output, operation_id): (qcg_engine::CommandOutput, Option<String>) =
            match ctx.run.guard_external_operation(
                ctx.journal,
                node,
                "command",
                &target,
                &details,
                &invocation,
            )? {
                qcg_engine::GuardDecision::Proceed { operation_id } => {
                    let input = command_input(ctx, node, &params).await?;
                    let output = match ctx
                        .run
                        .cmd
                        .run_with_limits_and_stdin(
                            &command,
                            ctx.run.contract.manifest.runtime.command_timeout_seconds,
                            ctx.run.contract.manifest.runtime.command_output_limit_bytes,
                            input.as_deref(),
                        )
                        .await
                    {
                        Ok(output) => output,
                        Err(error) => {
                            // Cancellation finishes nothing: it propagates
                            // without a completion record.
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
                    let output_value = serde_json::to_value(&output).map_err(|error| {
                        StepError::failed(
                            &node.id,
                            format!("command output is not serializable: {error}"),
                        )
                    })?;
                    ctx.run.finish_external_operation(
                        ctx.journal,
                        node,
                        &operation_id,
                        Some(output_value),
                    )?;
                    (output, Some(operation_id))
                }
                qcg_engine::GuardDecision::Resend { result, .. } => {
                    let output: qcg_engine::CommandOutput = serde_json::from_value(result)
                        .map_err(|error| {
                            StepError::failed(
                                &node.id,
                                format!("cached command output is corrupt: {error}"),
                            )
                        })?;
                    (output, None)
                }
            };
        if output.status != 0 {
            // A non-zero exit may have applied effects: indeterminate, never
            // clean. Retries need an explicit at-least-once opt-in. Cached
            // outputs always passed this check originally, so the id is
            // present whenever this arm runs.
            if let Some(operation_id) = &operation_id {
                ctx.run.finish_external_operation_with_warn(
                    ctx.journal,
                    node,
                    operation_id,
                    qcg_engine::OperationOutcome::Indeterminate {
                        reason: format!("command exited with {}", output.status),
                    },
                );
            }
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
        if limit.is_some_and(|limit| bytes.len() > limit) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "command JSON input exceeds {} bytes",
                    limit.unwrap_or(usize::MAX)
                ),
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
    let bytes = match limit {
        Some(limit) => {
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
            bytes
        }
        None => tokio::fs::read(&path).await?,
    };
    let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("command input file is not valid JSON: {error}"),
        )
    })?;
    let bytes = serde_json::to_vec(&value)?;
    if limit.is_some_and(|limit| bytes.len() > limit) {
        return Err(StepError::failed(
            &node.id,
            format!(
                "canonical command JSON input exceeds {} bytes",
                limit.unwrap_or(usize::MAX)
            ),
        ));
    }
    Ok(Some(bytes))
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
    let validator = qcg_policy::compile_bounded_validator(schema).map_err(|error| {
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
