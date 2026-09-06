use qcg_api::FormSpec;
use qcg_contract::NodeDef;
use qcg_engine::{StepContext, StepError, StepOutcome};
use qcg_mcp::{McpAccess, McpCommandAccess, McpCommandIsolation, McpContainerRuntime};
use qcg_policy::validate_bounded_json_schema;
use serde_json::Value;

use super::direct::{DirectMcpToolEvent, record_direct_mcp_tool_event};
use super::types::McpCallParams;

pub(crate) fn mcp_call_params(node: &NodeDef) -> Result<McpCallParams, StepError> {
    node.deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid mcp.call params: {error}")))
}

/// Record a transport failure and either fail or degrade to a null output
/// when the call is declared optional.
pub(crate) async fn degrade_optional_mcp_call(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    params: &McpCallParams,
    arguments: &Value,
    error: StepError,
) -> Result<StepOutcome, StepError> {
    record_direct_mcp_tool_event(
        ctx,
        node,
        DirectMcpToolEvent {
            server: &params.server,
            tool: &params.tool,
            arguments,
            result: None,
            error: Some(&error),
            duration_ms: 0,
            degraded: params.optional,
        },
    )?;
    if !params.optional {
        return Err(error);
    }
    Ok(StepOutcome::Success {
        output: None,
        files: vec![],
    })
}

pub(crate) fn validate_mcp_call_timeout(
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

pub(crate) enum DirectMcpCallOutcome {
    Complete(Value),
    NeedsUser(FormSpec),
}

pub(crate) fn validate_mcp_call_schema(
    node: &NodeDef,
    schema: &Value,
    field: &str,
) -> Result<(), StepError> {
    validate_bounded_json_schema(schema).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("mcp.call {field} is not a valid bounded JSON Schema: {error}"),
        )
    })?;
    Ok(())
}

pub(crate) fn validate_mcp_call_arguments(
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

pub(crate) fn mcp_access(ctx: &StepContext<'_>, command: &[String]) -> McpAccess {
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
