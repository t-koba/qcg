use async_trait::async_trait;
use contract::{ExpectDef, NodeDef};
use engine::{StepContext, StepError, StepExecutor, StepOutcome};
use model::Finding;
use policy::{params_schema, string_array_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::{append_output_expect_findings, render_command};
use contract::Contract;
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
    engine::step_type_id!("check.command");

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
                findings.push(Finding::error(
                    format!("expected exit code {exit_code}, got {}", output.status),
                    None,
                    Some(output.stderr.clone()),
                ));
            }
            if !expect.exit_code_in.is_empty() && !expect.exit_code_in.contains(&output.status) {
                findings.push(Finding::error(
                    format!(
                        "expected exit code in {:?}, got {}",
                        expect.exit_code_in, output.status
                    ),
                    None,
                    Some(output.stderr.clone()),
                ));
            }
            append_output_expect_findings(
                Some(expect),
                &output.stdout,
                &output.stderr,
                None,
                &node.id,
                &mut findings,
            )?;
        } else if output.status != 0 {
            findings.push(Finding::error(
                format!("command exited with {}", output.status),
                None,
                Some(output.stderr.clone()),
            ));
        }
        if findings.is_empty() {
            Ok(StepOutcome::success(Some(
                json!({ "status": output.status, "stdout": output.stdout }),
            )))
        } else {
            Ok(StepOutcome::check_failed(findings))
        }
    }
}

fn check_command_params(node: &NodeDef) -> Result<CheckCommandParams, StepError> {
    let params: CheckCommandParams = engine::step_params(node, "check.command")?;
    if params.command.is_empty() {
        return Err(StepError::failed(&node.id, "command must not be empty"));
    }
    Ok(params)
}
