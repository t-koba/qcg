use async_trait::async_trait;
use qcg_contract::{
    ContainerToolBackend, Contract, ExpectDef, MountDef, NodeDef, ToolBackendKind, ToolBackends,
    ToolDef, ToolNetwork, ToolResolution, ToolWorkspace,
};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome};
use qcg_policy::{params_schema, string_array_schema, string_schema};
use qcg_types::{Finding, Severity};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::render_command;
use super::container_backend::{
    ContainerMountSpec, ToolBackendCandidate, execute_container_backend_candidate,
    resolve_tool_backend,
};
pub(crate) struct CheckContainerStep;

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
        if resolve_tool_backend(&ctx.run.contract.manifest.permissions.containers).is_none() {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
