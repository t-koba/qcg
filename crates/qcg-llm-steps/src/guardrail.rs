use qcg_api::{GuardrailErrorKind, GuardrailErrorPolicy, GuardrailStage};
use qcg_contract::{NodeDef, RuntimeLimits, ToolDecl};
use qcg_engine::{ResultExt, StepContext, StepError};
use qcg_policy::validate_bounded_json_schema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GuardrailKind {
    RegexDeny,
    JsonSchema,
    Command,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GuardrailDecl {
    name: String,
    stage: GuardrailStage,
    kind: GuardrailKind,
    #[serde(default)]
    params: Value,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default = "default_guardrail_tripwire")]
    tripwire: bool,
    #[serde(default)]
    on_error: GuardrailErrorPolicy,
}

pub(crate) fn default_guardrail_tripwire() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, thiserror::Error)]
#[error("{message}")]
pub(crate) struct GuardrailError {
    pub(crate) kind: GuardrailErrorKind,
    pub(crate) code: String,
    pub(crate) message: String,
}

impl GuardrailError {
    fn configuration(code: &str, message: impl Into<String>) -> Self {
        Self {
            kind: GuardrailErrorKind::InvalidConfiguration,
            code: code.into(),
            message: message.into(),
        }
    }

    fn evaluation(code: &str, message: impl Into<String>) -> Self {
        Self {
            kind: GuardrailErrorKind::Evaluation,
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct GuardrailViolation {
    pub(crate) code: String,
    pub(crate) message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GuardrailDecision {
    Pass,
    Violation(GuardrailViolation),
}

pub(crate) struct RegexDenyGuardrail;

impl RegexDenyGuardrail {
    pub(crate) fn validate(&self, params: &Value) -> Result<(), GuardrailError> {
        let pattern = params
            .get("pattern")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                GuardrailError::configuration(
                    "missing_pattern",
                    "regex_deny requires params.pattern",
                )
            })?;
        regex::Regex::new(pattern).map(|_| ()).map_err(|error| {
            GuardrailError::configuration(
                "invalid_pattern",
                format!("invalid regex_deny pattern: {error}"),
            )
        })
    }

    pub(crate) fn evaluate(
        &self,
        value: &Value,
        params: &Value,
    ) -> Result<GuardrailDecision, GuardrailError> {
        let pattern = params
            .get("pattern")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                GuardrailError::configuration(
                    "missing_pattern",
                    "regex_deny requires params.pattern",
                )
            })?;
        let regex = regex::Regex::new(pattern).map_err(|error| {
            GuardrailError::configuration(
                "invalid_pattern",
                format!("invalid regex_deny pattern: {error}"),
            )
        })?;
        let encoded = serde_json::to_string(value).map_err(|error| {
            GuardrailError::evaluation("serialization_failed", error.to_string())
        })?;
        Ok(if regex.is_match(&encoded) {
            GuardrailDecision::Violation(GuardrailViolation {
                code: "denied_pattern".into(),
                message: "value matched a denied pattern".into(),
                details: None,
            })
        } else {
            GuardrailDecision::Pass
        })
    }
}

pub(crate) struct JsonSchemaGuardrail;

impl JsonSchemaGuardrail {
    pub(crate) fn validate(&self, params: &Value) -> Result<(), GuardrailError> {
        let schema = params.get("schema").ok_or_else(|| {
            GuardrailError::configuration("missing_schema", "json_schema requires params.schema")
        })?;
        validate_bounded_json_schema(schema).map_err(|error| {
            GuardrailError::configuration(
                "invalid_schema",
                format!("invalid or unsafe guardrail JSON Schema: {error}"),
            )
        })
    }

    pub(crate) fn evaluate(
        &self,
        value: &Value,
        params: &Value,
    ) -> Result<GuardrailDecision, GuardrailError> {
        let schema = params.get("schema").ok_or_else(|| {
            GuardrailError::configuration("missing_schema", "json_schema requires params.schema")
        })?;
        let validator = qcg_policy::compile_bounded_validator(schema).map_err(|error| {
            GuardrailError::configuration(
                "invalid_schema",
                format!("invalid or unsafe guardrail JSON Schema: {error}"),
            )
        })?;
        Ok(match validator.validate(value) {
            Ok(()) => GuardrailDecision::Pass,
            Err(error) => GuardrailDecision::Violation(GuardrailViolation {
                code: "schema_rejected".into(),
                message: format!("JSON Schema rejected value at `{}`", error.instance_path()),
                details: Some(json!({ "instance_path": error.instance_path().to_string() })),
            }),
        })
    }
}

pub(crate) struct CommandGuardrail;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommandGuardrailParams {
    command: Vec<String>,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default)]
    output_limit_bytes: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "status", rename_all = "snake_case")]
pub(crate) enum CommandGuardrailOutput {
    Pass {},
    Violation {
        code: String,
        message: String,
        #[serde(default)]
        details: Option<Value>,
    },
    Error {
        code: String,
        message: String,
    },
}

impl CommandGuardrail {
    fn params(params: &Value) -> Result<CommandGuardrailParams, GuardrailError> {
        serde_json::from_value(params.clone()).map_err(|error| {
            GuardrailError::configuration(
                "invalid_command_params",
                format!("command guardrail parameters are invalid: {error}"),
            )
        })
    }

    pub(crate) fn validate_output(
        output: CommandGuardrailOutput,
    ) -> Result<GuardrailDecision, GuardrailError> {
        match output {
            CommandGuardrailOutput::Pass {} => Ok(GuardrailDecision::Pass),
            CommandGuardrailOutput::Violation {
                code,
                message,
                details,
            } => {
                if code.trim().is_empty() || message.trim().is_empty() {
                    return Err(GuardrailError::evaluation(
                        "invalid_output",
                        "command guardrail violation code and message must not be empty",
                    ));
                }
                Ok(GuardrailDecision::Violation(GuardrailViolation {
                    code,
                    message,
                    details,
                }))
            }
            CommandGuardrailOutput::Error { code, message } => {
                if code.trim().is_empty() || message.trim().is_empty() {
                    return Err(GuardrailError::evaluation(
                        "invalid_output",
                        "command guardrail error code and message must not be empty",
                    ));
                }
                Err(GuardrailError::evaluation(
                    &code,
                    format!("command guardrail reported an error: {message}"),
                ))
            }
        }
    }
}

impl CommandGuardrail {
    pub(crate) fn validate(&self, params: &Value) -> Result<(), GuardrailError> {
        let params = Self::params(params)?;
        if params.command.is_empty() || params.command[0].trim().is_empty() {
            return Err(GuardrailError::configuration(
                "empty_command",
                "command guardrail command must not be empty",
            ));
        }
        if params
            .command
            .iter()
            .any(|argument| argument.contains('\0'))
        {
            return Err(GuardrailError::configuration(
                "invalid_command",
                "command guardrail arguments must not contain NUL bytes",
            ));
        }
        if params.timeout_seconds == Some(0) {
            return Err(GuardrailError::configuration(
                "invalid_timeout",
                "command guardrail timeout_seconds must be greater than zero",
            ));
        }
        if params.output_limit_bytes == Some(0) {
            return Err(GuardrailError::configuration(
                "invalid_output_limit",
                "command guardrail output_limit_bytes must be greater than zero",
            ));
        }
        Ok(())
    }

    fn validate_with_runtime(
        &self,
        params: &Value,
        runtime: &RuntimeLimits,
    ) -> Result<(), GuardrailError> {
        self.validate(params)?;
        let params = Self::params(params)?;
        if let Some(timeout_seconds) = params.timeout_seconds
            && timeout_seconds > runtime.command_timeout_seconds
        {
            return Err(GuardrailError::configuration(
                "timeout_exceeds_runtime_limit",
                format!(
                    "command guardrail timeout_seconds ({timeout_seconds}) must not exceed runtime.command_timeout_seconds ({})",
                    runtime.command_timeout_seconds
                ),
            ));
        }
        if let Some(output_limit_bytes) = params.output_limit_bytes
            && runtime
                .command_output_limit_bytes
                .is_some_and(|limit| output_limit_bytes > limit)
        {
            return Err(GuardrailError::configuration(
                "output_limit_exceeds_runtime_limit",
                format!(
                    "command guardrail output_limit_bytes ({output_limit_bytes}) must not exceed runtime.command_output_limit_bytes ({})",
                    runtime.command_output_limit_bytes.unwrap_or(usize::MAX)
                ),
            ));
        }
        Ok(())
    }

    async fn evaluate(
        &self,
        ctx: &StepContext<'_>,
        node: &NodeDef,
        value: &Value,
        params: &Value,
    ) -> Result<GuardrailDecision, GuardrailError> {
        let runtime = &ctx.run.contract.manifest.runtime;
        self.validate_with_runtime(params, runtime)?;
        let params = Self::params(params)?;
        let input = serde_json::to_vec(value).map_err(|error| {
            GuardrailError::evaluation("serialization_failed", error.to_string())
        })?;
        let timeout_seconds = params
            .timeout_seconds
            .unwrap_or(runtime.command_timeout_seconds);
        let output_limit_bytes = params
            .output_limit_bytes
            .or(runtime.command_output_limit_bytes);
        let output = ctx
            .run
            .cmd
            .run_with_limits_and_stdin(
                &params.command,
                timeout_seconds,
                output_limit_bytes,
                Some(&input),
            )
            .await
            .map_err(|error| GuardrailError::evaluation("command_failed", error.to_string()))?;
        if output.status != 0 {
            return Err(GuardrailError::evaluation(
                "command_failed",
                format!("command guardrail exited with status {}", output.status),
            ));
        }
        let stdout = std::str::from_utf8(&output.stdout_bytes).map_err(|error| {
            GuardrailError::evaluation(
                "invalid_output",
                format!("command guardrail stdout is not UTF-8: {error}"),
            )
        })?;
        let output = serde_json::from_str(stdout).map_err(|error| {
            GuardrailError::evaluation(
                "invalid_output",
                format!("command guardrail stdout is not valid JSON: {error}"),
            )
        })?;
        Self::validate_output(output).map_err(|error| {
            GuardrailError::evaluation(
                &error.code,
                format!("node `{}`: {}", node.id, error.message),
            )
        })
    }
}

pub(crate) fn validate_guardrail(
    declaration: &GuardrailDecl,
    runtime: &RuntimeLimits,
) -> Result<(), GuardrailError> {
    match declaration.kind {
        GuardrailKind::RegexDeny => RegexDenyGuardrail.validate(&declaration.params),
        GuardrailKind::JsonSchema => JsonSchemaGuardrail.validate(&declaration.params),
        GuardrailKind::Command => {
            CommandGuardrail.validate_with_runtime(&declaration.params, runtime)
        }
    }
}

pub(crate) async fn evaluate_guardrail(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    declaration: &GuardrailDecl,
    value: &Value,
) -> Result<GuardrailDecision, GuardrailError> {
    match declaration.kind {
        GuardrailKind::RegexDeny => RegexDenyGuardrail.evaluate(value, &declaration.params),
        GuardrailKind::JsonSchema => JsonSchemaGuardrail.evaluate(value, &declaration.params),
        GuardrailKind::Command => {
            CommandGuardrail
                .evaluate(ctx, node, value, &declaration.params)
                .await
        }
    }
}

pub(crate) fn validate_guardrails(
    node: &NodeDef,
    declarations: &[GuardrailDecl],
    tools: &[ToolDecl],
    runtime: &RuntimeLimits,
) -> Result<(), StepError> {
    let mut names = BTreeSet::new();
    for declaration in declarations {
        if declaration.name.trim().is_empty() || !names.insert(declaration.name.as_str()) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "guardrail name `{}` must be non-empty and unique",
                    declaration.name
                ),
            ));
        }
        validate_guardrail(declaration, runtime)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        if let Some(tool) = &declaration.tool {
            if !matches!(
                declaration.stage,
                GuardrailStage::ToolInput | GuardrailStage::ToolOutput
            ) {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "guardrail `{}` can select a tool only at a tool stage",
                        declaration.name
                    ),
                ));
            }
            if !tools.iter().any(|declared| declared.name() == tool) {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "guardrail `{}` selects undeclared tool `{tool}`",
                        declaration.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) async fn apply_guardrails(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    declarations: &[GuardrailDecl],
    stage: GuardrailStage,
    tool: Option<&str>,
    value: &Value,
) -> Result<(), StepError> {
    for declaration in declarations.iter().filter(|declaration| {
        declaration.stage == stage
            && declaration
                .tool
                .as_deref()
                .is_none_or(|selected| Some(selected) == tool)
    }) {
        let decision = match evaluate_guardrail(ctx, node, declaration, value).await {
            Ok(decision) => decision,
            Err(error) => {
                ctx.journal
                    .event(
                        "guardrail_error",
                        json!({
                            "node": node.id,
                            "guardrail": declaration.name,
                            "kind": declaration.kind,
                            "stage": stage,
                            "tool": tool,
                            "error_kind": error.kind,
                            "code": error.code,
                            "message": error.message,
                            "policy": declaration.on_error,
                        }),
                    )
                    .step_err(&node.id)?;
                match declaration.on_error {
                    GuardrailErrorPolicy::Fail => {
                        return Err(StepError::failed(&node.id, error.to_string()));
                    }
                    GuardrailErrorPolicy::Block => {
                        GuardrailDecision::Violation(GuardrailViolation {
                            code: format!("guardrail_error.{}", error.code),
                            message: error.message,
                            details: Some(json!({ "error_kind": error.kind })),
                        })
                    }
                }
            }
        };
        let violation = match &decision {
            GuardrailDecision::Pass => None,
            GuardrailDecision::Violation(violation) => Some(violation),
        };
        ctx.journal
            .event(
                "guardrail_evaluated",
                json!({
                    "node": node.id,
                    "guardrail": declaration.name,
                    "kind": declaration.kind,
                    "stage": stage,
                    "tool": tool,
                    "passed": violation.is_none(),
                    "tripwire": declaration.tripwire,
                    "violation": violation,
                }),
            )
            .step_err(&node.id)?;
        if violation.is_some() && declaration.tripwire {
            ctx.journal
                .event(
                    "guardrail_tripwire",
                    json!({
                        "node": node.id,
                        "guardrail": declaration.name,
                        "kind": declaration.kind,
                        "stage": stage,
                        "tool": tool,
                        "violation": violation,
                    }),
                )
                .step_err(&node.id)?;
            return Err(StepError::failed(
                &node.id,
                format!(
                    "guardrail tripwire `{}` blocked {:?}",
                    declaration.name, stage
                ),
            ));
        }
    }
    Ok(())
}
