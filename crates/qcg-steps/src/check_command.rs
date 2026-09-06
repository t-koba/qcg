use async_trait::async_trait;
use qcg_contract::{ExpectDef, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome};
use qcg_policy::{params_schema, string_array_schema, string_schema};
use qcg_types::{Finding, Severity};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::render_command;
use qcg_contract::Contract;
pub(crate) struct CheckCommandStep;

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
