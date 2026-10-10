use api::{ConfirmSpec, FormSpec};
use contract::{CommandIsolation, NodeDef, ToolDecl};
use engine::{StepContext, StepError};
use llm::{LlmRuntime, ToolSpec};
use mcp::{
    McpAccess, McpCallOutcome, McpCommandAccess, McpCommandIsolation, McpContainerRuntime,
    McpError, McpSession,
};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::agent::agent_command_permission;

pub(crate) enum AgentToolOutcome {
    Result(Value),
    /// A successful external-operation result whose durable record has not
    /// been finished yet. The caller must finish it only after its output
    /// guardrails and secret scan pass, so a rejected result is never
    /// cached for resend (D03).
    OperationResult {
        value: Value,
        operation_id: String,
    },
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
    /// Operator `allowed_destinations` ceiling per resolved server id.
    /// Missing entries fail closed (deny everything extracted).
    destinations: BTreeMap<String, Vec<String>>,
}

impl McpAgentTools {
    pub(crate) fn server_for(&self, alias: &str) -> Option<&str> {
        self.tools.get(alias).map(|tool| tool.server.as_str())
    }

    /// Fail-closed destination-policy gate for server->destination reach:
    /// every host extracted from the tool arguments must sit within the
    /// operator's per-server `allowed_destinations` (exact match or `"*"`).
    /// Calls with no extracted host pass, so pure-query search keeps
    /// working while fetch requires an explicit operator allow. Denials
    /// are `Refused`, never silent.
    pub(crate) fn check_destinations(
        &self,
        node: &NodeDef,
        alias: &str,
        args: &Value,
    ) -> Result<Vec<String>, StepError> {
        let tool = self.tools.get(alias).ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("MCP tool alias `{alias}` was not resolved"),
            )
        })?;
        let allowed = self
            .destinations
            .get(&tool.server)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let extracted: Vec<String> = policy::extract_mcp_destinations(args).into_iter().collect();
        let denied: Vec<&String> = extracted
            .iter()
            .filter(|host| !policy::mcp_destination_is_allowed(allowed, host))
            .collect();
        if !denied.is_empty() {
            let denied = denied
                .into_iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(StepError::Refused {
                node: node.id.clone(),
                message: format!(
                    "MCP tool `{alias}` destination `{denied}` is not in server `{}` allowed_destinations; ask the operator to allow it in providers.toml",
                    tool.server
                ),
            });
        }
        Ok(extracted)
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
        let mut destinations = BTreeMap::new();
        for server in requested.keys() {
            let profile = runtime
                .mcp
                .resolve(server)
                .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
            destinations.insert(server.clone(), profile.allowed_destinations().to_vec());
            if profile.transport() == mcp::McpTransport::Stdio
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
                                contract::ContainerRuntime::Docker => McpContainerRuntime::Docker,
                                contract::ContainerRuntime::Podman => McpContainerRuntime::Podman,
                                contract::ContainerRuntime::DockerRunsc => {
                                    McpContainerRuntime::DockerRunsc
                                }
                                contract::ContainerRuntime::Incus => McpContainerRuntime::Incus,
                                contract::ContainerRuntime::Lxd => McpContainerRuntime::Lxd,
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
                Ok::<_, mcp::McpError>((server, session, tools))
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
                let input_validator = policy::compile_bounded_validator(&tool.input_schema).map_err(|message| {
                    StepError::failed(
                        &node.id,
                        format!(
                            "MCP server `{server}` tool `{remote_name}` input schema is invalid or unsafe: {message}"
                        ),
                    )
                })?;
                let output_validator = tool
                    .output_schema
                    .as_ref()
                    .map(|schema| {
                        policy::compile_bounded_validator(schema).map_err(|message| {
                            StepError::failed(
                                &node.id,
                                format!(
                                    "MCP server `{server}` tool `{remote_name}` output schema is invalid or unsafe: {message}"
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
            destinations,
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
        validate_mcp_value(&node.id, alias, &tool.input_validator, args, "arguments")?;
        self.check_destinations(node, alias, args)?;
        Ok(())
    }

    pub(crate) async fn call(
        &self,
        node: &NodeDef,
        alias: &str,
        args: Value,
        input_responses: Option<BTreeMap<String, Value>>,
        request_state: Option<String>,
        cancellation: &tokio_util::sync::CancellationToken,
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
        // Observe the caller's scope while the remote call is in flight:
        // the session carries its connect-time token, but the current
        // node scope (timeout or parent cancel) must stop the wait even
        // when the session outlives it.
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(StepError::Cancelled);
            }
            result = session.call_tool_with_input(
                &tool.remote_name,
                args,
                input_responses,
                request_state,
            ) => result,
        };
        let result = agent_mcp_result(outcome)
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

#[cfg(test)]
mod tests {
    use super::*;
    use contract::{NodeDef, OnDeps, StepType};
    use serde_json::json;

    fn node() -> NodeDef {
        NodeDef {
            id: "agent".into(),
            kind: StepType::literal("llm.agent"),
            needs: vec![],
            when: None,
            on_deps: OnDeps::AllSucceeded,
            context: vec![],
            output: None,
            artifact: None,
            on_fail: None,
            failure: None,
            retry: None,
            params: Default::default(),
        }
    }

    fn harness(allowed: Vec<String>) -> McpAgentTools {
        let schema = json!({"type": "object"});
        let input_validator =
            policy::compile_bounded_validator(&schema).expect("test schema must compile");
        McpAgentTools {
            sessions: BTreeMap::new(),
            tools: BTreeMap::from([(
                "fetch".to_string(),
                McpResolvedTool {
                    server: "exa-public".to_string(),
                    remote_name: "web_fetch_exa".to_string(),
                    description: "test".to_string(),
                    model_input_schema: schema,
                    input_validator,
                    output_validator: None,
                },
            )]),
            destinations: BTreeMap::from([("exa-public".to_string(), allowed)]),
        }
    }

    #[test]
    fn destination_policy_denies_unlisted_fetch_but_passes_query() {
        let node = node();
        // Empty default denies every extracted host; host-only calls pass.
        let tools = harness(Vec::new());
        let error = tools
            .validate_args(&node, "fetch", &json!({"url": "https://example.test/x"}))
            .expect_err("unlisted fetch destination must be refused");
        assert!(
            matches!(error, StepError::Refused { .. }),
            "denials are Refused, never silent: {error}"
        );
        tools
            .validate_args(&node, "fetch", &json!({"query": "rust", "numResults": 5}))
            .expect("pure-query search must pass the empty default");
        // An explicit operator allow passes fetch; other hosts still fail.
        let tools = harness(vec!["example.test".to_string()]);
        tools
            .validate_args(&node, "fetch", &json!({"url": "https://example.test/x"}))
            .expect("listed destination must pass");
        assert!(
            tools
                .validate_args(&node, "fetch", &json!({"url": "https://other.test/x"}))
                .is_err(),
            "unlisted hosts stay denied under a non-empty list"
        );
        // The wildcard covers every host.
        let tools = harness(vec!["*".to_string()]);
        tools
            .validate_args(&node, "fetch", &json!({"url": "https://other.test/x"}))
            .expect("wildcard must allow any destination");
    }
}
