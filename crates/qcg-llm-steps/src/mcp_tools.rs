use qcg_api::{ConfirmSpec, FormSpec};
use qcg_contract::{CommandIsolation, NodeDef, ToolDecl};
use qcg_engine::{StepContext, StepError};
use qcg_llm::{LlmRuntime, ToolSpec};
use qcg_mcp::{
    McpAccess, McpCallOutcome, McpCommandAccess, McpCommandIsolation, McpContainerRuntime,
    McpError, McpSession,
};
use qcg_policy::validate_bounded_json_schema;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::agent::agent_command_permission;

pub(crate) enum AgentToolOutcome {
    Result(Value),
    Error(Value),
    Handoff(Value),
    NeedsUser(FormSpec),
    NeedsConfirm(ConfirmSpec),
}

pub(crate) struct McpResolvedTool {
    server: String,
    remote_name: String,
    description: String,
    model_input_schema: Value,
    input_validator: jsonschema::Validator,
    output_validator: Option<jsonschema::Validator>,
}

#[derive(Default)]
pub(crate) struct McpAgentTools {
    sessions: BTreeMap<String, McpSession>,
    tools: BTreeMap<String, McpResolvedTool>,
}

impl McpAgentTools {
    pub(crate) fn server_for(&self, alias: &str) -> Option<&str> {
        self.tools.get(alias).map(|tool| tool.server.as_str())
    }

    pub(crate) async fn prepare(
        ctx: &StepContext<'_>,
        node: &NodeDef,
        runtime: &LlmRuntime,
        declarations: &[ToolDecl],
    ) -> Result<Self, StepError> {
        let mut requested = BTreeMap::<String, Vec<(String, String, Option<String>)>>::new();
        for declaration in declarations {
            if let ToolDecl::Mcp {
                name,
                description,
                server,
                tool,
                ..
            } = declaration
            {
                requested.entry(server.clone()).or_default().push((
                    name.clone(),
                    tool.clone(),
                    description.clone(),
                ));
            }
        }
        if requested.is_empty() {
            return Ok(Self::default());
        }

        let mut permitted_commands = Vec::new();
        for server in requested.keys() {
            let profile = runtime
                .mcp
                .resolve(server)
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            if profile.transport() == qcg_mcp::McpTransport::Stdio
                && let Some(permission) = agent_command_permission(
                    &ctx.run.contract.manifest.permissions.commands,
                    profile.command(),
                )
                && let Some(isolation) = permission.isolation.as_ref()
            {
                permitted_commands.push(McpCommandAccess {
                    argv: profile.command().to_vec(),
                    isolation: match isolation {
                        CommandIsolation::Container => McpCommandIsolation::Container,
                        CommandIsolation::TrustedHost => McpCommandIsolation::TrustedHost,
                    },
                    image: permission.image.clone(),
                    runtime: match isolation {
                        CommandIsolation::TrustedHost => None,
                        CommandIsolation::Container => ctx
                            .run
                            .contract
                            .manifest
                            .permissions
                            .containers
                            .runtime
                            .map(|runtime| match runtime {
                                qcg_contract::ContainerRuntime::Docker => {
                                    McpContainerRuntime::Docker
                                }
                                qcg_contract::ContainerRuntime::Podman => {
                                    McpContainerRuntime::Podman
                                }
                                qcg_contract::ContainerRuntime::DockerRunsc => {
                                    McpContainerRuntime::DockerRunsc
                                }
                                qcg_contract::ContainerRuntime::Incus => McpContainerRuntime::Incus,
                                qcg_contract::ContainerRuntime::Lxd => McpContainerRuntime::Lxd,
                                qcg_contract::ContainerRuntime::Lxc => McpContainerRuntime::Lxc,
                            }),
                    },
                });
            }
        }
        let access = McpAccess {
            network_hosts: ctx
                .run
                .contract
                .manifest
                .permissions
                .network
                .iter()
                .cloned()
                .collect(),
            commands: permitted_commands,
            workspace: ctx.run.fs.workspace().as_std_path().to_path_buf(),
        };

        let mut joins = tokio::task::JoinSet::new();
        for server in requested.keys().cloned() {
            let mcp = runtime.mcp.clone();
            let access = access.clone();
            let cancellation = ctx.run.cancellation.clone();
            joins.spawn(async move {
                let session = mcp.connect(&server, &access, cancellation).await?;
                let tools = session.list_tools().await?;
                Ok::<_, qcg_mcp::McpError>((server, session, tools))
            });
        }

        let mut sessions = BTreeMap::new();
        let mut discovered = BTreeMap::new();
        while let Some(result) = joins.join_next().await {
            let (server, session, tools) = result
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            discovered.insert(server.clone(), tools);
            sessions.insert(server, session);
        }

        let mut resolved = BTreeMap::new();
        for (server, bindings) in requested {
            let tools = discovered.get(&server).ok_or_else(|| {
                StepError::failed(
                    &node.id,
                    format!("MCP server `{server}` discovery result is missing"),
                )
            })?;
            for (alias, remote_name, override_description) in bindings {
                let tool = tools
                    .iter()
                    .find(|tool| tool.name == remote_name)
                    .ok_or_else(|| {
                        StepError::failed(
                            &node.id,
                            format!("MCP server `{server}` does not expose tool `{remote_name}`"),
                        )
                    })?;
                if !tool.input_schema.is_object() {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "MCP server `{server}` tool `{remote_name}` returned a non-object input schema"
                        ),
                    ));
                }
                validate_bounded_json_schema(&tool.input_schema).map_err(|message| {
                    StepError::failed(
                        &node.id,
                        format!(
                            "MCP server `{server}` tool `{remote_name}` input schema is invalid or unsafe: {message}"
                        ),
                    )
                })?;
                let input_validator = qcg_policy::compile_bounded_validator(&tool.input_schema).map_err(|_| {
                    StepError::failed(
                        &node.id,
                        format!(
                            "MCP server `{server}` tool `{remote_name}` returned an invalid input schema"
                        ),
                    )
                })?;
                let output_validator = tool
                    .output_schema
                    .as_ref()
                    .map(|schema| {
                        validate_bounded_json_schema(schema).map_err(|message| {
                            StepError::failed(
                                &node.id,
                                format!(
                                    "MCP server `{server}` tool `{remote_name}` output schema is invalid or unsafe: {message}"
                                ),
                            )
                        })?;
                        qcg_policy::compile_bounded_validator(schema).map_err(|_| {
                            StepError::failed(
                                &node.id,
                                format!(
                                    "MCP server `{server}` tool `{remote_name}` returned an invalid output schema"
                                ),
                            )
                        })
                    })
                    .transpose()?;
                resolved.insert(
                    alias,
                    McpResolvedTool {
                        server: server.clone(),
                        remote_name,
                        description: override_description
                            .unwrap_or_else(|| format!("MCP tool `{server}/{}`", tool.name)),
                        model_input_schema: sanitize_untrusted_schema(&tool.input_schema),
                        input_validator,
                        output_validator,
                    },
                );
            }
        }
        Ok(Self {
            sessions,
            tools: resolved,
        })
    }

    pub(crate) fn tool_spec(&self, alias: &str) -> Result<ToolSpec, StepError> {
        let tool = self.tools.get(alias).ok_or_else(|| {
            StepError::failed(alias, format!("MCP tool alias `{alias}` was not resolved"))
        })?;
        Ok(ToolSpec {
            name: alias.to_string(),
            description: tool.description.clone(),
            input_schema: tool.model_input_schema.clone(),
        })
    }

    pub(crate) fn validate_args(
        &self,
        node: &NodeDef,
        alias: &str,
        args: &Value,
    ) -> Result<(), StepError> {
        let tool = self.tools.get(alias).ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("MCP tool alias `{alias}` was not resolved"),
            )
        })?;
        validate_mcp_value(&node.id, alias, &tool.input_validator, args, "arguments")
    }

    pub(crate) async fn call(
        &self,
        node: &NodeDef,
        alias: &str,
        args: Value,
        input_responses: Option<BTreeMap<String, Value>>,
        request_state: Option<String>,
    ) -> Result<McpCallOutcome, StepError> {
        let tool = self.tools.get(alias).ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("MCP tool alias `{alias}` was not resolved"),
            )
        })?;
        let session = self.sessions.get(&tool.server).ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("MCP server `{}` has no active session", tool.server),
            )
        })?;
        let result = agent_mcp_result(
            session
                .call_tool_with_input(&tool.remote_name, args, input_responses, request_state)
                .await,
        )
        .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let McpCallOutcome::Complete(value) = &result else {
            return Ok(result);
        };
        validate_mcp_complete_result(&node.id, alias, tool.output_validator.as_ref(), value)?;
        Ok(result)
    }
}

pub(crate) fn validate_mcp_complete_result(
    node_id: &str,
    alias: &str,
    output_validator: Option<&jsonschema::Validator>,
    value: &Value,
) -> Result<(), StepError> {
    let is_error = match value.get("isError") {
        None => false,
        Some(Value::Bool(is_error)) => *is_error,
        Some(_) => {
            return Err(StepError::failed(
                node_id,
                format!("MCP tool `{alias}` result contained non-boolean isError"),
            ));
        }
    };
    let content = value
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            StepError::failed(
                node_id,
                format!("MCP tool `{alias}` result omitted content array"),
            )
        })?;
    if content.is_empty() && value.get("structuredContent").is_none() {
        return Err(StepError::failed(
            node_id,
            format!("MCP tool `{alias}` result contained no content"),
        ));
    }
    if is_error {
        return Ok(());
    }
    if let Some(validator) = output_validator {
        let structured = value.get("structuredContent").ok_or_else(|| {
            StepError::failed(
                node_id,
                format!("MCP tool `{alias}` declared outputSchema but omitted structuredContent"),
            )
        })?;
        validate_mcp_value(node_id, alias, validator, structured, "result")?;
    }
    Ok(())
}

pub(crate) fn agent_mcp_result(
    result: Result<McpCallOutcome, McpError>,
) -> Result<McpCallOutcome, McpError> {
    match result {
        Ok(result) => Ok(result),
        Err(McpError::ToolFailed { result, .. }) => Ok(McpCallOutcome::Complete(result)),
        Err(error) => Err(error),
    }
}

pub(crate) fn validate_mcp_value(
    node_id: &str,
    alias: &str,
    validator: &jsonschema::Validator,
    value: &Value,
    value_kind: &str,
) -> Result<(), StepError> {
    let Some(error) = validator.iter_errors(value).next() else {
        return Ok(());
    };
    let path = error.instance_path().to_string();
    let location = if path.is_empty() { "/" } else { path.as_str() };
    Err(StepError::failed(
        node_id,
        format!("MCP tool `{alias}` {value_kind} failed JSON Schema validation at `{location}`"),
    ))
}

pub(crate) fn sanitize_untrusted_schema(value: &Value) -> Value {
    match value {
        Value::Array(values) => {
            Value::Array(values.iter().map(sanitize_untrusted_schema).collect())
        }
        Value::Object(values) => {
            let mut sanitized = serde_json::Map::new();
            for (name, value) in values {
                if matches!(
                    name.as_str(),
                    "description" | "title" | "$comment" | "examples" | "default"
                ) {
                    continue;
                }
                let value = if matches!(
                    name.as_str(),
                    "properties"
                        | "$defs"
                        | "definitions"
                        | "patternProperties"
                        | "dependentSchemas"
                ) {
                    match value {
                        Value::Object(entries) => Value::Object(
                            entries
                                .iter()
                                .map(|(entry_name, entry)| {
                                    (entry_name.clone(), sanitize_untrusted_schema(entry))
                                })
                                .collect(),
                        ),
                        _ => sanitize_untrusted_schema(value),
                    }
                } else {
                    sanitize_untrusted_schema(value)
                };
                sanitized.insert(name.clone(), value);
            }
            Value::Object(sanitized)
        }
        _ => value.clone(),
    }
}
