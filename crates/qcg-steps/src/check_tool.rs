use async_trait::async_trait;
use qcg_contract::{NodeDef, ToolBackendKind, ToolDef, ToolFallback, ToolNetwork, ToolWorkspace};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome, StepTraits};
use qcg_policy::{params_schema, string_schema};
use qcg_types::{Finding, Severity};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::{ensure_bounded_file_tree, require};
use super::container_backend::{
    ToolBackendCandidate, build_tool_backend_candidate, execute_container_backend_candidate,
};
use qcg_api::ConfirmSpec;
use qcg_contract::Contract;
pub(crate) struct CheckToolStep;

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
        StepTraits::parallel()
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
            .ok_or_else(|| {
                StepError::failed(&node.id, format!("tool `{tool_name}` is not declared"))
            })?;
        let input = ctx.render_inline(
            node,
            params
                .input
                .as_deref()
                .or(tool.input.as_deref())
                .ok_or_else(|| {
                    StepError::failed(&node.id, format!("tool `{tool_name}` declares no input"))
                })?,
        )?;
        // Snapshot the input tree to private run metadata before any
        // external tool process reads it: the validator validated the live
        // tree handle-relative, but the child process can only open by
        // pathname, so it must read the immutable snapshot instead of the
        // swappable workspace (E13).
        let (effective_input, _snapshot_cleanup) = if !matches!(tool.workspace, ToolWorkspace::None)
        {
            let input_path = ctx.run.fs.resolve_read(&input).map_err(|error| {
                StepError::failed(
                    &node.id,
                    format!("tool input path is not readable in workspace: {error}"),
                )
            })?;
            ensure_bounded_file_tree(
                &ctx.run.fs,
                &input_path,
                ctx.run.contract.manifest.runtime.file_input_limit_bytes,
                ctx.run.contract.manifest.runtime.file_count_limit,
            )
            .map_err(|error| StepError::failed(&node.id, error))?;
            let snapshot_root = ctx
                .run
                .metadata
                .join(format!("check-tool-{}", uuid::Uuid::now_v7()));
            let snapshot_dest = snapshot_root.join(&input);
            ctx.run
                .fs
                .snapshot_tree_to(
                    &input_path,
                    &snapshot_dest,
                    ctx.run.contract.manifest.runtime.file_input_limit_bytes,
                    ctx.run.contract.manifest.runtime.file_count_limit,
                )
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            (
                snapshot_dest.as_str().to_string(),
                SnapshotCleanup {
                    path: Some(snapshot_root.clone()),
                },
            )
        } else {
            (input.clone(), SnapshotCleanup { path: None })
        };
        if !matches!(tool.network, ToolNetwork::None) {
            return Err(StepError::failed(
                &node.id,
                "check.tool currently supports network = \"none\" only for local backends",
            ));
        }
        let order = tool_backend_order(tool);
        let mut unavailable = Vec::new();
        for (index, backend) in order.iter().enumerate() {
            // Host and bundled processes open host paths directly, so they
            // receive the snapshot absolute path. Container guests see the
            // snapshot through their mount, so they keep the workspace-
            // relative input string and the mount source is swapped to the
            // snapshot root at execution (E13).
            let candidate_input = if matches!(backend, ToolBackendKind::Container) {
                &input
            } else {
                &effective_input
            };
            let candidate =
                match build_tool_backend_candidate(ctx, node, tool, backend, candidate_input) {
                    Ok(candidate) => candidate,
                    Err(reason) => {
                        unavailable
                            .push(json!({ "backend": backend.to_string(), "reason": reason }));
                        if matches!(tool.resolution.fallback, ToolFallback::None) {
                            break;
                        }
                        continue;
                    }
                };
            if requires_tool_backend_confirmation(&tool.resolution.fallback, index) {
                let target = format!("{tool_name}:{}", candidate.kind);
                let details = Some(json!({
                    "tool": tool_name,
                    "backend": candidate.kind.to_string(),
                    "unavailable": unavailable,
                }));
                let digest = qcg_engine::RunContext::operation_digest(&target, &details)?;
                let confirm_id = format!("{}:tool_backend:{}:{}", node.id, candidate.kind, digest);
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
                            target: target.clone(),
                            dry_run: false,
                            details: details.clone(),
                            operation_digest: digest,
                            scope: ctx.run.contract.manifest.permissions.side_effects_scope,
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
            // Snapshot root for container mount swapping; `None` when the
            // tool takes no workspace input.
            let snapshot_source = _snapshot_cleanup.path.clone();
            return execute_tool_candidate(ctx, node, tool, candidate, snapshot_source).await;
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

/// Removes a check.tool snapshot directory or file. Best effort: snapshot
/// cleanup must never mask the tool result.
struct SnapshotCleanup {
    path: Option<camino::Utf8PathBuf>,
}

impl Drop for SnapshotCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            if std::fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.is_dir()) {
                let _ = std::fs::remove_dir_all(&path);
            } else {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
}

async fn execute_tool_candidate(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    tool: &ToolDef,
    candidate: ToolBackendCandidate,
    snapshot_source: Option<camino::Utf8PathBuf>,
) -> Result<StepOutcome, StepError> {
    match candidate.kind {
        ToolBackendKind::Host => {
            let output = ctx
                .run
                .cmd
                .run_with_limits(
                    &candidate.argv,
                    tool.timeout_seconds,
                    Some(tool.output_limit_bytes),
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
                    Some(tool.output_limit_bytes),
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
            // When the input was snapshotted, mount the snapshot root at
            // the declared guest mount instead of the live workspace, so a
            // parent swapped after validation cannot redirect the guest read
            // (E13). The guest input path (`mount/input`) then resolves to
            // the immutable copy.
            if let Some(snapshot_root) = snapshot_source {
                let image = candidate.container_image.clone().ok_or_else(|| {
                    StepError::failed(&node.id, "container image was not resolved")
                })?;
                let mounts = candidate
                    .container_mounts
                    .iter()
                    .map(|mount| {
                        (
                            snapshot_root.clone(),
                            mount.target.clone(),
                            mount.mode == "ro",
                        )
                    })
                    .collect::<Vec<_>>();
                let output = ctx
                    .run
                    .cmd
                    .run_container_workload(
                        qcg_engine::ContainerWorkload {
                            image: &image,
                            mounts: &mounts,
                            workdir: None,
                            workload_argv: &candidate.argv,
                            stdin: None,
                        },
                        tool.timeout_seconds,
                        Some(tool.output_limit_bytes),
                    )
                    .await
                    .map_err(|error| StepError::from_gateway(&node.id, error))?;
                check_tool_output(node, &kind, output.status, output.stdout, output.stderr)
            } else {
                let output =
                    execute_container_backend_candidate(ctx, node, tool, candidate).await?;
                check_tool_output(node, &kind, output.status, output.stdout, output.stderr)
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
