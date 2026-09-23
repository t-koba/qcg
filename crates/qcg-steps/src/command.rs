use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use qcg_contract::{Contract, NodeDef, ToolBackendKind, ToolNetwork, ToolWorkspace};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome};
use qcg_policy::{
    is_safe_relative_path, params_schema, string_array_schema, string_schema,
    validate_bounded_json_schema,
};
use qcg_types::Finding;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::common::{
    open_package_read, package_file, read_opened_bounded, render_command, render_json_templates,
};
use super::container_backend::build_tool_backend_candidate;
use qcg_policy::MAX_COMMAND_RESULT_FILES;

pub(crate) struct CommandStep;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandParams {
    #[serde(default)]
    command: Vec<String>,
    /// Generator-packaged tool to run instead of `command`. Only bundled
    /// backends are accepted here; host and container backends stay behind
    /// `check.tool`'s validator resolution.
    #[serde(default)]
    tool: Option<String>,
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

// Redacting Debug (E07): argv and stdin may carry secrets governed by the
// same redact rules as journaled targets (E09). Values stay hidden; shapes
// stay visible for diagnosis.
impl std::fmt::Debug for CommandParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted_command: Vec<String> = self
            .command
            .iter()
            .map(|arg| {
                qcg_policy::redact_credential_assignments_in_text(&qcg_policy::redact_urls_in_text(
                    arg,
                ))
            })
            .collect();
        formatter
            .debug_struct("CommandParams")
            .field("command", &redacted_command)
            .field("tool", &self.tool)
            .field("input", &self.input.as_ref().map(|_| "[REDACTED]"))
            .field("input_file", &self.input_file)
            .field("input_file_scope", &self.input_file_scope)
            .field("result", &self.result)
            .field(
                "output_schema",
                &self.output_schema.as_ref().map(|_| "[SCHEMA]"),
            )
            .finish()
    }
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
            &[],
            json!({
                "command": string_array_schema(),
                "tool": string_schema(),
                "input": {},
                "input_file": string_schema(),
                "input_file_scope": { "type": "string", "enum": ["workspace", "package"] },
                "result": { "type": "string", "enum": ["process", "structured"] },
                "output_schema": {},
            }),
        ))
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = command_params(node)?;
        // Exactly one invocation source: an explicit argv, or a
        // generator-packaged tool declared in the contract.
        if params.command.is_empty() == params.tool.is_none() {
            return Err(StepError::failed(
                &node.id,
                "command requires exactly one of `command` or `tool`",
            ));
        }
        if let Some(tool_name) = params.tool.as_deref() {
            if tool_name.trim().is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    "command tool must not be empty",
                ));
            }
            let tool = contract.manifest.tools.get(tool_name).ok_or_else(|| {
                StepError::failed(&node.id, format!("tool `{tool_name}` is not declared"))
            })?;
            if tool.kind != "command" {
                return Err(StepError::failed(
                    &node.id,
                    format!("command tool `{tool_name}` must declare kind = \"command\""),
                ));
            }
            if tool.backends.bundled.is_none() {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "command tool `{tool_name}` must declare a bundled backend; host and container backends are resolved by check.tool"
                    ),
                ));
            }
            if !matches!(params.result, CommandResultMode::Structured) {
                return Err(StepError::failed(
                    &node.id,
                    "command tool requires result = \"structured\"",
                ));
            }
            if !matches!(tool.workspace, ToolWorkspace::None) {
                return Err(StepError::failed(
                    &node.id,
                    format!("command tool `{tool_name}` requires workspace = \"none\""),
                ));
            }
            if !matches!(tool.network, ToolNetwork::None) {
                return Err(StepError::failed(
                    &node.id,
                    format!("command tool `{tool_name}` requires network = \"none\""),
                ));
            }
        }
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
        if let Some(tool_name) = params.tool.clone() {
            return execute_bundled_tool(ctx, node, &params, &tool_name).await;
        }
        let command = render_command(
            ctx,
            node,
            &params.command,
            ctx.run.contract.manifest.runtime.command_input_limit_bytes,
        )?;
        // E09c: the journaled target never carries credential-shaped secrets.
        // Risk boundary (E09): only `key=value` / `key: value` shapes and
        // URL-embedded secrets are redacted; a bare secret argv element
        // stays in plaintext by design (see `docs/security.md`). Secrets
        // belong in `input`/`stdin`/environment, never literally in argv.
        let full_target = command.join(" ");
        let target = qcg_policy::redact_credential_assignments_in_text(
            &qcg_policy::redact_urls_in_text(&full_target),
        );
        let plan = ctx
            .run
            .cmd
            .command_plan(&command)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        // Compute stdin before approval and guarding so the exact bytes are
        // part of the content digest: a changed input must never reuse an
        // approval or resend-cache entry for different content (E09).
        let input = command_input(ctx, node, &params).await?;
        let invocation = qcg_engine::RunContext::execution_invocation(ctx.journal, node);
        // Run-scoped salt so identical stdin in this run binds identically
        // (content-scope reuse); runs never share a digest. Invocation
        // separation comes from the operation id (E09/Q1).
        let salt = ctx.run.run_id.clone();
        let mut plan = qcg_engine::bind_command_stdin(&node.id, plan, input.as_deref(), &salt)?;
        // Bind the full plaintext target so redacted journaling never aliases
        // distinct commands to one approval (E09c). `target_sha256` binds
        // the joined display string; `argv_sha256` binds the exact argv
        // array bytes so boundary-shuffled argvs never alias either. Both
        // are salted with the run id for cross-run unlinkability.
        if let Some(object) = plan.as_object_mut() {
            object.insert(
                "target_sha256".into(),
                serde_json::Value::String(qcg_engine::salted_binding_digest(
                    "command-target-v1",
                    &salt,
                    full_target.as_bytes(),
                )),
            );
            let argv_bytes = serde_json::to_vec(&command).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("command argv is not serializable: {error}"),
                )
            })?;
            object.insert(
                "argv_sha256".into(),
                serde_json::Value::String(qcg_engine::salted_binding_digest(
                    "command-argv-v1",
                    &salt,
                    &argv_bytes,
                )),
            );
        }
        let details = Some(plan.clone());
        if let Some(confirm) = ctx.run.require_side_effect(
            ctx.journal,
            node,
            "command",
            &target,
            details.clone(),
            &invocation,
        )? {
            return Ok(StepOutcome::NeedsConfirm { confirm });
        }
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
            // present whenever this arm runs. The success finish above is
            // deliberately superseded here: a success record for a failed
            // command would let a resend claim success it never earned
            // (E07).
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
            // E09-3: full vs truncated and why. The resend cache keeps the
            // full native `CommandOutput` (see `finish_external_operation`
            // above) so a same-invocation resend replays the identical
            // tail. The user-visible output here carries stdout/stderr up
            // to the configured `command_output_limit_bytes` (never
            // unbounded); journaled `tool_call` events carry a further
            // 32 KiB truncated copy via `bounded_event_value`.
            CommandResultMode::Process => Ok(StepOutcome::Success {
                output: Some(json!({
                    "status": output.status,
                    "stdout": command_output_value(&output.stdout_bytes, &output.stdout),
                    "stderr": command_output_value(&output.stderr_bytes, &output.stderr),
                })),
                files: vec![],
            }),
            CommandResultMode::Structured => structured_outcome(ctx, node, &params, &output).await,
        }
    }
}

/// Parses the structured command protocol from a process result and maps it
/// to a step outcome. Shared by argv commands and bundled tools so both
/// enforce the same schema and file rules.
async fn structured_outcome(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    params: &CommandParams,
    output: &qcg_engine::CommandOutput,
) -> Result<StepOutcome, StepError> {
    let stdout = std::str::from_utf8(&output.stdout_bytes).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("structured stdout is not valid UTF-8: {error}"),
        )
    })?;
    let result: StructuredCommandResult = serde_json::from_str(stdout).map_err(|error| {
        StepError::failed(&node.id, format!("structured stdout is invalid: {error}"))
    })?;
    if matches!(result.status, StructuredCommandStatus::CheckFailed) && result.findings.is_empty() {
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

/// Runs a generator-packaged (bundled) tool through the same structured
/// stdin/stdout protocol as an argv command. The package sha256 declared on
/// the tool is the authorization: no `permissions.commands` entry can name
/// an install-time package path, and the bytes are verified before spawn.
async fn execute_bundled_tool(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    params: &CommandParams,
    tool_name: &str,
) -> Result<StepOutcome, StepError> {
    let tool = ctx
        .run
        .contract
        .manifest
        .tools
        .get(tool_name)
        .ok_or_else(|| StepError::failed(&node.id, format!("tool `{tool_name}` is not declared")))?
        .clone();
    let input = command_input(ctx, node, params).await?;
    let candidate = build_tool_backend_candidate(ctx, node, &tool, &ToolBackendKind::Bundled, "")
        .map_err(|reason| {
        StepError::failed(
            &node.id,
            format!("bundled tool `{tool_name}` is unavailable: {reason}"),
        )
    })?;
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
    let output = ctx
        .run
        .cmd
        .run_trusted_process_with_stdin(
            &candidate.argv,
            tool.timeout_seconds,
            Some(tool.output_limit_bytes),
            input.as_deref(),
        )
        .await
        .map_err(|error| StepError::from_gateway(&node.id, error))?;
    if output.status != 0 {
        return Err(StepError::failed(
            &node.id,
            format!("bundled tool `{tool_name}` exited with {}", output.status),
        ));
    }
    structured_outcome(ctx, node, params, &output).await
}

fn command_params(node: &NodeDef) -> Result<CommandParams, StepError> {
    let params: CommandParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid command params: {error}")))?;
    if params.command.is_empty() && params.tool.is_none() {
        return Err(StepError::failed(&node.id, "command or tool is required"));
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
        if let Some(limit) = limit
            && bytes.len() > limit
        {
            return Err(StepError::failed(
                &node.id,
                format!("command JSON input exceeds {limit} bytes"),
            ));
        }
        return Ok(Some(bytes));
    };
    let input_file = ctx.render_inline(node, input_file)?;
    let (path, opened) = match params.input_file_scope {
        CommandInputFileScope::Workspace => {
            let path = ctx.run.fs.resolve_read(&input_file).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("command workspace input file is not readable: {error}"),
                )
            })?;
            let opened = ctx.run.fs.open_read_resolved(&path).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("command workspace input file is not readable: {error}"),
                )
            })?;
            (path, Some(opened))
        }
        CommandInputFileScope::Package => (
            package_file(&ctx.run.contract, node, &input_file, "command input")?,
            None,
        ),
    };
    let limit = ctx.run.contract.manifest.runtime.command_input_limit_bytes;
    let bytes = match opened {
        Some(file) => read_opened_bounded(file, limit, "command input file")
            .map_err(|error| StepError::failed(&node.id, error))?,
        // Package scope opens through the same O_NOFOLLOW + handle boundary
        // as snapshot reads: the leaf never follows a terminal symlink and
        // the handle is verified to be a file (E13).
        None => {
            let file = open_package_read(node, &path, "command input")?;
            read_opened_bounded(file, limit, "command input file")
                .map_err(|error| StepError::failed(&node.id, error))?
        }
    };
    let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("command input file is not valid JSON: {error}"),
        )
    })?;
    let bytes = serde_json::to_vec(&value)?;
    if let Some(limit) = limit
        && bytes.len() > limit
    {
        return Err(StepError::failed(
            &node.id,
            format!("canonical command JSON input exceeds {limit} bytes"),
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
        // Open handle-relative and verify the handle is a file: a pathname
        // `is_file` check alone leaves a swap window before the later hash
        // (E13).
        let opened = ctx.run.fs.open_read_resolved(&path).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("structured returned an unreadable file `{file}`: {error}"),
            )
        })?;
        if !opened
            .metadata()
            .map(|metadata| metadata.is_file())
            .unwrap_or(false)
        {
            return Err(StepError::failed(
                &node.id,
                format!("structured returned a non-file output `{file}`"),
            ));
        }
        resolved.push(path);
    }
    Ok(resolved)
}
