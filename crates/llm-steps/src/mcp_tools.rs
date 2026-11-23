use api::{ConfirmSpec, FormSpec};
use contract::{CommandIsolation, NodeDef, ToolDecl};
use engine::{StepContext, StepError};
use llm::{LlmRuntime, ToolSpec};
use mcp::{
    McpAccess, McpCallOutcome, McpCommandAccess, McpCommandIsolation, McpContainerRuntime,
    McpError, McpSession,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

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
    sessions: BTreeMap<String, AgentSession>,
    tools: BTreeMap<String, McpResolvedTool>,
    /// Operator `allowed_destinations` ceiling per resolved server id.
    /// Missing entries fail closed (deny everything extracted).
    destinations: BTreeMap<String, Vec<String>>,
    /// Operator-verified non-fetching remote tool names per resolved server.
    /// Only these skip extraction; all others keep the fail-closed rules.
    non_network_tools: BTreeMap<String, BTreeSet<String>>,
    /// Contract-delegated per-tool narrowing. `None` inherits the ceiling;
    /// `Some(list)` allows only those hosts with an empty list meaning
    /// query-only. Missing entries inherit the ceiling.
    declared: BTreeMap<String, Option<Vec<String>>>,
    /// Operator destination decider per resolved server id: `(argv, timeout)`.
    /// Missing entries keep the static ceiling exactly; present entries only
    /// widen a static miss at execution time, never a narrowing denial.
    deciders: BTreeMap<String, (Vec<String>, u64)>,
    pub(crate) authorization_required: Vec<String>,
}

/// One live discovery session with the launch record that authorizes it.
/// HTTP sessions carry no launch id; stdio sessions carry the operation that
/// must be settled only after a confirmed close.
struct AgentSession {
    session: McpSession,
    launch: Option<String>,
}

struct DiscoveryFailure {
    server: String,
    error: McpError,
    launch: Option<String>,
}

/// Only errors that prove no child could have been spawned may settle a
/// launch clean. Transport, timeout, and interruption failures stay Started.
fn discovery_failure_is_clean(error: &McpError) -> bool {
    matches!(
        error,
        McpError::Configuration(_) | McpError::Authorization(_)
    )
}

fn settle_discovery_launch(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    operation_id: Option<&str>,
) -> Result<(), StepError> {
    if let Some(operation_id) = operation_id {
        ctx.run.finish_external_operation_with(
            ctx.journal,
            node,
            operation_id,
            engine::OperationOutcome::CleanError,
        )?;
    }
    Ok(())
}

/// A launch is settled only when that exact session's close was confirmed.
fn settle_after_close(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    server: &str,
    operation_id: Option<&str>,
    close: Result<(), McpError>,
) -> Result<(), StepError> {
    match close {
        Ok(()) => settle_discovery_launch(ctx, node, operation_id),
        Err(error) => Err(StepError::failed(
            &node.id,
            format!("MCP server `{server}` discovery session close failed: {error}"),
        )),
    }
}

/// Drain every owned session. A confirmed close settles that server's
/// launch; an unconfirmed close leaves its launch Started. Cleanup failures
/// are journaled for audit and reported by the caller when no primary error
/// exists, so a sibling's success never proves anything about another server.
async fn close_and_settle_sessions(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    sessions: &mut BTreeMap<String, AgentSession>,
) -> Result<(), StepError> {
    let mut first_error = None;
    for (server, entry) in std::mem::take(sessions) {
        let result = settle_after_close(
            ctx,
            node,
            &server,
            entry.launch.as_deref(),
            entry.session.close().await,
        );
        if let Err(error) = result {
            let _ = ctx.journal.event(
                "mcp_discovery_cleanup_failed",
                serde_json::json!({ "node": node.id, "server": server }),
            );
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

impl McpAgentTools {
    pub(crate) fn server_for(&self, alias: &str) -> Option<&str> {
        self.tools.get(alias).map(|tool| tool.server.as_str())
    }

    /// Fail-closed destination-policy gate for server->destination reach:
    /// every host extracted from the tool arguments must sit within the
    /// operator's per-server `allowed_destinations` (exact match or `"*"`)
    /// and, when the contract declares per-tool `destinations`, within that
    /// narrowing too (exact match, empty means query-only). Calls with no
    /// extracted host pass, so pure-query search keeps working while fetch
    /// requires an explicit allow. Denials are `Refused`, never silent.
    /// Classification errors also refuse: URL/host/target slots must not
    /// become query-only merely because their contents are malformed.
    /// The shared classifier handles arrays and schemeless references;
    /// its errors contain no argument values, including credentials.
    ///
    /// When a static miss meets a configured `destination_decider` that the
    /// narrowing would still admit, sync validation defers: it passes so the
    /// non-suspending execution gate ([`Self::authorize_destinations`]) can
    /// consult operator policy. A denied execution surfaces there as
    /// `Refused`; narrowing denials never defer. A tool the operator declares
    /// in `non_network_tools` is the sole exception: it returns no hosts
    /// before extraction, since its arguments cannot name a fetch target.
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
        if self
            .non_network_tools
            .get(&tool.server)
            .is_some_and(|names| names.contains(&tool.remote_name))
        {
            return Ok(Vec::new());
        }
        let allowed = self
            .destinations
            .get(&tool.server)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let extracted: Vec<String> = policy::extract_mcp_destinations(args)
            .map_err(|message| StepError::Refused {
                node: node.id.clone(),
                message: message.into(),
            })?
            .into_iter()
            .collect();
        let denied: Vec<&String> = extracted
            .iter()
            .filter(|host| !policy::mcp_destination_is_allowed(allowed, host))
            .collect();
        if !denied.is_empty() {
            let narrow_blocks = match self.declared.get(alias) {
                Some(Some(narrow)) => extracted
                    .iter()
                    .any(|host| !narrow.iter().any(|allowed| allowed == host)),
                _ => false,
            };
            if !narrow_blocks && self.deciders.contains_key(&tool.server) {
                // Deferred: the execution gate re-checks narrowing and the
                // ceiling, then delegates this miss to the decider.
                return Ok(extracted);
            }
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
        if let Some(Some(narrow)) = self.declared.get(alias) {
            let denied: Vec<&String> = extracted
                .iter()
                .filter(|host| !narrow.iter().any(|allowed| allowed == *host))
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
                        "MCP tool `{alias}` destination `{denied}` is not in tool `{alias}` destinations"
                    ),
                });
            }
        }
        Ok(extracted)
    }

    /// Closes every owned session and settles only the launches whose close
    /// was confirmed. All closes are attempted even if one fails; the first
    /// cleanup error is returned after the drain, so one server's failure is
    /// never treated as proof about another server.
    pub(crate) async fn close_and_settle(
        &mut self,
        ctx: &StepContext<'_>,
        node: &NodeDef,
    ) -> Result<(), StepError> {
        close_and_settle_sessions(ctx, node, &mut self.sessions).await
    }

    pub(crate) async fn prepare(
        ctx: &StepContext<'_>,
        node: &NodeDef,
        runtime: &LlmRuntime,
        declarations: &[ToolDecl],
    ) -> Result<Self, StepError> {
        let mut requested = BTreeMap::<String, Vec<(String, String, Option<String>)>>::new();
        let mut declared: BTreeMap<String, Option<Vec<String>>> = BTreeMap::new();
        for declaration in declarations {
            if let ToolDecl::Mcp {
                name,
                description,
                server,
                tool,
                destinations,
                ..
            } = declaration
            {
                requested.entry(server.clone()).or_default().push((
                    name.clone(),
                    tool.clone(),
                    description.clone(),
                ));
                declared.insert(name.clone(), destinations.clone());
            }
        }
        if requested.is_empty() {
            return Ok(Self::default());
        }

        let mut permitted_commands = Vec::new();
        let mut destinations = BTreeMap::new();
        let mut non_network_tools = BTreeMap::new();
        let mut deciders = BTreeMap::new();
        let mut stdio_launches = BTreeMap::new();
        let setup = (|| -> Result<(), StepError> {
            for server in requested.keys() {
                let profile = runtime
                    .mcp
                    .resolve(server)
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
                destinations.insert(server.clone(), profile.allowed_destinations().to_vec());
                non_network_tools.insert(
                    server.clone(),
                    profile.non_network_tools().iter().cloned().collect(),
                );
                if let Some(argv) = profile.destination_decider() {
                    deciders.insert(
                        server.clone(),
                        (argv.to_vec(), profile.destination_decider_timeout_seconds()),
                    );
                }
                if profile.transport() == mcp::McpTransport::Stdio {
                    let invocation = format!(
                        "{}::agent_discovery::{}",
                        engine::RunContext::execution_invocation(ctx.journal, node),
                        profile.id()
                    );
                    match ctx.run.guard_external_operation(
                        ctx.journal,
                        node,
                        "mcp.stdio_launch",
                        profile.id(),
                        &None,
                        &invocation,
                    )? {
                        // The task owns the id so a session is never stored
                        // without the launch record that authorizes it.
                        engine::GuardDecision::Proceed { operation_id } => {
                            stdio_launches.insert(server.clone(), operation_id);
                        }
                        engine::GuardDecision::Resend { .. } => {
                            return Err(StepError::failed(
                                &node.id,
                                format!(
                                    "MCP server `{}` discovery launch has a cached result; refusing to reuse it",
                                    profile.id()
                                ),
                            ));
                        }
                    }
                }
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
                                    contract::ContainerRuntime::Docker => {
                                        McpContainerRuntime::Docker
                                    }
                                    contract::ContainerRuntime::Podman => {
                                        McpContainerRuntime::Podman
                                    }
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
            Ok(())
        })();
        if let Err(error) = setup {
            // Nothing has been spawned yet: admitted launches were never
            // used, so close their accounting without waiting for a child.
            for operation_id in stdio_launches.values() {
                if settle_discovery_launch(ctx, node, Some(operation_id)).is_err() {
                    let _ = ctx.journal.event(
                        "mcp_discovery_cleanup_failed",
                        serde_json::json!({ "node": node.id, "operation_id": operation_id }),
                    );
                }
            }
            return Err(error);
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
            host_cgroup: ctx.run.host_cgroup.clone(),
            effects: ctx.run.cmd.effect_ledger(),
        };

        let mut joins = tokio::task::JoinSet::new();
        for server in requested.keys().cloned() {
            let mcp = runtime.mcp.clone();
            let access = access.clone();
            let cancellation = ctx.run.cancellation.clone();
            let launch = stdio_launches.get(&server).cloned();
            joins.spawn(async move {
                let session =
                    mcp.connect(&server, &access, cancellation)
                        .await
                        .map_err(|error| DiscoveryFailure {
                            server: server.clone(),
                            error,
                            launch: launch.clone(),
                        })?;
                let tools = session
                    .list_tools()
                    .await
                    .map_err(|error| DiscoveryFailure {
                        server: server.clone(),
                        error,
                        launch: launch.clone(),
                    })?;
                Ok::<_, DiscoveryFailure>((server, session, tools, launch))
            });
        }

        let mut sessions = BTreeMap::new();
        let mut discovered = BTreeMap::new();
        let mut authorization_required = Vec::new();
        let mut discovery_error = None;
        // Drain every task before cleanup: a sibling that has already
        // established a session must be closed explicitly, not dropped with
        // the JoinSet. Its own launch cannot be settled otherwise.
        while let Some(result) = joins.join_next().await {
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    if discovery_error.is_none() {
                        discovery_error = Some(StepError::failed(&node.id, error.to_string()));
                    }
                    continue;
                }
            };
            match result {
                Ok((server, session, tools, launch)) => {
                    discovered.insert(server.clone(), tools);
                    sessions.insert(server, AgentSession { session, launch });
                }
                Err(failure) => {
                    if matches!(failure.error, McpError::AuthorizationRequired { .. }) {
                        authorization_required.push(failure.server);
                        continue;
                    }
                    // Every proven clean pre-launch failure must settle its own
                    // launch, independent of which failure is selected as the
                    // primary error. Deferring settlement behind
                    // `discovery_error.is_none()` would leave a sibling's launch
                    // Started and block the next owner's recovery. Only the
                    // choice of which error to report is first-wins.
                    if discovery_failure_is_clean(&failure.error)
                        && let Err(error) =
                            settle_discovery_launch(ctx, node, failure.launch.as_deref())
                        && discovery_error.is_none()
                    {
                        discovery_error = Some(error);
                    }
                    if discovery_error.is_none() {
                        discovery_error =
                            Some(StepError::failed(&node.id, failure.error.to_string()));
                    }
                }
            }
        }
        if let Some(error) = discovery_error {
            let _ = close_and_settle_sessions(ctx, node, &mut sessions).await;
            return Err(error);
        }
        authorization_required.sort();

        let resolved = (|| -> Result<BTreeMap<String, McpResolvedTool>, StepError> {
            let mut resolved = BTreeMap::new();
            for (server, bindings) in &requested {
                let Some(tools) = discovered.get(server) else {
                    if authorization_required.contains(server) {
                        continue;
                    }
                    return Err(StepError::failed(
                        &node.id,
                        format!("MCP server `{server}` discovery result is missing"),
                    ));
                };
                for (alias, remote_name, override_description) in bindings {
                    let tool = tools
                        .iter()
                        .find(|tool| tool.name == *remote_name)
                        .ok_or_else(|| {
                            StepError::failed(
                                &node.id,
                                format!(
                                    "MCP server `{server}` does not expose tool `{remote_name}`"
                                ),
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
                    let input_validator =
                        policy::compile_bounded_validator(&tool.input_schema).map_err(|message| {
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
                        alias.clone(),
                        McpResolvedTool {
                            server: server.clone(),
                            remote_name: remote_name.clone(),
                            description: override_description
                                .clone()
                                .unwrap_or_else(|| format!("MCP tool `{server}/{}`", tool.name)),
                            model_input_schema: sanitize_untrusted_schema(&tool.input_schema),
                            input_validator,
                            output_validator,
                        },
                    );
                }
            }
            Ok(resolved)
        })();
        let tools = match resolved {
            Ok(tools) => tools,
            Err(error) => {
                let _ = close_and_settle_sessions(ctx, node, &mut sessions).await;
                return Err(error);
            }
        };
        Ok(Self {
            sessions,
            tools,
            destinations,
            non_network_tools,
            declared,
            deciders,
            authorization_required,
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

    /// Execution-time destination gate: re-extracts hosts, enforces the
    /// narrowing and the static ceiling, then delegates a remaining miss to
    /// the operator decider when one is configured. Static hits never consult
    /// the decider and narrowing denials never defer to it. Resumes re-invoke
    /// fresh, so tightening the ceiling, the narrowing, or the decider
    /// denies retroactively; a journaled allow never grants future execution.
    /// The verdict is journaled (`mcp_destination_decision`, hosts only) for
    /// audit. Cancellation propagates instead of denying.
    pub(crate) async fn authorize_destinations(
        &self,
        ctx: &StepContext<'_>,
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
        let server = tool.server.as_str();
        // Keep one policy owner for validation and execution: a deferred
        // static miss is the only non-static success returned by this gate.
        // In particular, ambiguity and narrowing denials cannot reach a
        // command even when its operator policy would answer allow.
        // An empty classified set passes the all-hosts check below.
        let extracted = self.check_destinations(node, alias, args)?;
        let allowed = self
            .destinations
            .get(server)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if extracted
            .iter()
            .all(|host| policy::mcp_destination_is_allowed(allowed, host))
        {
            return Ok(());
        }
        let (argv, timeout) = &self.deciders[server]; // check_destinations admitted this static miss.
        let gateway = engine::CmdGateway::new(
            ctx.run.contract.manifest.permissions.clone(),
            ctx.run.fs.workspace().to_owned(),
        )
        .with_cancellation(ctx.run.cancellation.clone());
        let verdict =
            engine::decide_destination(&gateway, server, alias, &extracted, argv, *timeout).await;
        let record_decision = |allow| {
            engine::record_destination_decision(
                ctx.journal,
                &node.id,
                server,
                alias,
                &extracted,
                *timeout,
                allow,
            )
        };
        match verdict {
            Err(engine::GatewayError::Canceled) => Err(StepError::Cancelled),
            Err(error) => {
                record_decision(false)?;
                Err(StepError::Refused {
                    node: node.id.clone(),
                    message: format!(
                        "MCP tool `{alias}` destination decider for server `{server}` failed ({error}); refusing the fetch"
                    ),
                })
            }
            Ok(allow) => {
                record_decision(allow)?;
                if allow {
                    Ok(())
                } else {
                    let denied = extracted.join(", ");
                    Err(StepError::Refused {
                        node: node.id.clone(),
                        message: format!(
                            "MCP tool `{alias}` destination `{denied}` was denied by the destination decider for server `{server}`"
                        ),
                    })
                }
            }
        }
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
        let entry = self.sessions.get(&tool.server).ok_or_else(|| {
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
            result = entry.session.call_tool_with_input(
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
            reuse: None,
            params: Default::default(),
        }
    }

    fn harness(allowed: Vec<String>) -> McpAgentTools {
        harness_with_declared(allowed, None)
    }

    fn harness_with_declared(allowed: Vec<String>, declared: Option<Vec<String>>) -> McpAgentTools {
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
            non_network_tools: BTreeMap::new(),
            declared: BTreeMap::from([("fetch".to_string(), declared)]),
            deciders: BTreeMap::new(),
            authorization_required: Vec::new(),
        }
    }

    #[test]
    fn destination_policy_denies_unlisted_fetch_but_passes_query() {
        let node = node();
        // Empty default denies every extracted host; query-only calls pass.
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

    #[test]
    fn declared_non_network_tool_skips_extraction_but_undeclared_tools_do_not() {
        let node = node();
        let dotted = json!({"file": "report.md"});
        // Omitted/empty declaration preserves the current fail-closed path:
        // the dotted filename is classified as a host and denied.
        let tools = harness(Vec::new());
        let error = tools
            .validate_args(&node, "fetch", &dotted)
            .expect_err("undeclared tools must still extract destinations");
        assert!(matches!(error, StepError::Refused { .. }), "{error}");
        assert!(error.to_string().contains("report.md"), "{error}");

        let mut declared = harness(Vec::new());
        declared.non_network_tools.insert(
            "exa-public".to_string(),
            BTreeSet::from(["web_fetch_exa".to_string()]),
        );
        assert_eq!(
            declared
                .check_destinations(&node, "fetch", &dotted)
                .expect("declared tool must skip extraction"),
            Vec::<String>::new()
        );
        declared
            .validate_args(&node, "fetch", &dotted)
            .expect("dotted non-URL argument must pass for a declared tool");
        declared
            .validate_args(&node, "fetch", &json!({"url": "https://example.test/x"}))
            .expect("a verified non-fetching tool has no destination to classify");

        // A declaration for another server tool does not widen this tool.
        let mut other_tool = harness(Vec::new());
        other_tool.non_network_tools.insert(
            "exa-public".to_string(),
            BTreeSet::from(["web_search_exa".to_string()]),
        );
        assert!(
            other_tool.validate_args(&node, "fetch", &dotted).is_err(),
            "only the exact remote tool name may skip extraction"
        );
        // A declaration for another server does not widen this tool either.
        let mut other_server = harness(Vec::new());
        other_server.non_network_tools.insert(
            "other-server".to_string(),
            BTreeSet::from(["web_fetch_exa".to_string()]),
        );
        assert!(
            other_server.validate_args(&node, "fetch", &dotted).is_err(),
            "declarations are per server"
        );
    }

    #[test]
    fn schemeless_destinations_obey_ceiling_and_narrowing() {
        let node = node();
        for target in [
            "https://example.test/x",
            "example.test",
            "example.test/path",
            "//example.test/x",
        ] {
            let args = json!({"urls":[target]});
            for (allowed, narrow, pass) in [
                (vec![], None, false),
                (vec!["other.test"], None, false),
                (vec!["example.test"], None, true),
                (vec!["*"], Some(vec!["example.test"]), true),
                (vec!["*"], Some(vec!["other.test"]), false),
                (vec!["example.test"], Some(vec![]), false),
            ] {
                let tools = harness_with_declared(
                    allowed.into_iter().map(str::to_owned).collect(),
                    narrow.map(|v| v.into_iter().map(str::to_owned).collect()),
                );
                assert_eq!(tools.validate_args(&node, "fetch", &args).is_ok(), pass);
                tools
                    .validate_args(&node, "fetch", &json!({"query":"rust"}))
                    .unwrap();
            }
            let tools = harness_with_decider(vec![], None, (vec!["decider".into()], 5));
            tools
                .validate_args(&node, "fetch", &args)
                .expect("parseable miss defers");
        }
        let tools = harness_with_decider(vec!["*".into()], None, (vec!["decider".into()], 5));
        for args in [
            json!({"url":"not a URL"}),
            json!({"urls":["example.test","//"]}),
            json!({"target":"private-unparseable-value"}),
        ] {
            let error = tools.validate_args(&node, "fetch", &args).unwrap_err();
            assert!(matches!(error, StepError::Refused { .. }));
            assert!(error.to_string().contains("ambiguous MCP destination"));
            assert!(!error.to_string().contains("private-unparseable-value"));
        }
    }

    fn harness_with_decider(
        allowed: Vec<String>,
        declared: Option<Vec<String>>,
        decider: (Vec<String>, u64),
    ) -> McpAgentTools {
        let mut tools = harness_with_declared(allowed, declared);
        tools.deciders.insert("exa-public".to_string(), decider);
        tools
    }

    #[test]
    fn decider_defers_static_miss_but_never_widens_narrowing() {
        let node = node();
        let decider = (vec!["decider".to_string()], 5);
        // A static miss the narrowing would admit defers to execution:
        // sync validation passes instead of refusing.
        let tools = harness_with_decider(Vec::new(), None, decider.clone());
        tools
            .validate_args(&node, "fetch", &json!({"url": "https://example.test/x"}))
            .expect("decider-configured miss must defer, not refuse");
        tools
            .validate_args(&node, "fetch", &json!({"query": "rust"}))
            .expect("query-only still passes");
        // Narrowing still denies without consulting the decider: a miss the
        // narrowing would also block is refused (static-first, same order as
        // execution), never deferred.
        let tools = harness_with_decider(
            vec!["example.test".to_string()],
            Some(vec!["example.test".to_string()]),
            decider,
        );
        let error = tools
            .validate_args(&node, "fetch", &json!({"url": "https://other.test/x"}))
            .expect_err("narrowing-blocked miss must not defer to the decider");
        assert!(
            matches!(error, StepError::Refused { .. }),
            "narrowing-blocked misses are Refused: {error}"
        );
        // Without a decider the same miss refuses as before.
        let tools = harness(Vec::new());
        assert!(
            tools
                .validate_args(&node, "fetch", &json!({"url": "https://example.test/x"}))
                .is_err(),
            "unset decider keeps the static ceiling exactly"
        );
    }

    #[test]
    fn delegated_destinations_narrow_the_operator_ceiling() {
        let node = node();
        // Absent narrowing inherits the ceiling: both hosts pass.
        let tools = harness_with_declared(
            vec!["example.test".to_string(), "other.test".to_string()],
            None,
        );
        tools
            .validate_args(&node, "fetch", &json!({"url": "https://other.test/x"}))
            .expect("absent destinations must inherit the ceiling");
        // Explicit narrowing allows its hosts while denying the rest, even
        // though the ceiling allows them. Query-only passes.
        let tools = harness_with_declared(
            vec!["example.test".to_string(), "other.test".to_string()],
            Some(vec!["example.test".to_string()]),
        );
        tools
            .validate_args(&node, "fetch", &json!({"url": "https://example.test/x"}))
            .expect("declared host must pass");
        tools
            .validate_args(&node, "fetch", &json!({"query": "rust"}))
            .expect("query-only must pass narrowing");
        let error = tools
            .validate_args(&node, "fetch", &json!({"url": "https://other.test/x"}))
            .expect_err("narrowed host must be refused");
        assert!(
            matches!(error, StepError::Refused { .. }),
            "narrowing denials are Refused, never silent: {error}"
        );
        assert!(
            error.to_string().contains("tool `fetch` destinations"),
            "narrowing denial must name the tool destinations: {error}"
        );
        // Empty narrowing is query-only: any fetch is refused.
        let tools = harness_with_declared(vec!["example.test".to_string()], Some(Vec::new()));
        let error = tools
            .validate_args(&node, "fetch", &json!({"url": "https://example.test/x"}))
            .expect_err("empty destinations must deny fetch");
        assert!(
            matches!(error, StepError::Refused { .. }),
            "empty narrowing must refuse: {error}"
        );
        tools
            .validate_args(&node, "fetch", &json!({"query": "rust"}))
            .expect("empty narrowing must pass query-only");
    }

    /// Durable launch accounting for agent discovery. The fixture is a local
    /// newline-delimited JSON-RPC stdio server; no provider, sleep, or external
    /// process beyond the test's own child is involved.
    #[cfg(unix)]
    mod launch_tests {
        use super::*;
        use contract::MANIFEST_FILE;
        use engine::OperationStatus;

        static DISCOVERY_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        const HEALTHY_SERVER: &str = r#"#!/usr/bin/env python3
import json, sys
for line in sys.stdin:
    try:
        msg = json.loads(line)
    except Exception:
        continue
    method = msg.get("method")
    if method == "initialize":
        result = {"protocolVersion": msg.get("params", {}).get("protocolVersion", "2025-06-18"),
                  "capabilities": {"tools": {}}, "serverInfo": {"name": "fixture", "version": "1"}}
    elif method == "tools/list":
        result = {"tools": [{"name": "echo", "description": "echo", "inputSchema": {"type": "object"}}]}
    elif "id" in msg:
        result = {}
    else:
        continue
    print(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": result}), flush=True)
"#;

        fn marker_server(marker: &str) -> String {
            format!(
                r#"#!/usr/bin/env python3
from pathlib import Path
Path({marker:?}).write_text("started")
raise SystemExit(1)
"#
            )
        }

        struct Fixture {
            root: camino::Utf8PathBuf,
            run: engine::RunContext,
            node: NodeDef,
        }

        impl Drop for Fixture {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.root);
            }
        }

        impl Fixture {
            fn root() -> camino::Utf8PathBuf {
                camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
                    .unwrap()
                    .join(format!(
                        "agent-discovery-{}-{}",
                        std::process::id(),
                        DISCOVERY_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    ))
            }

            fn new(root: camino::Utf8PathBuf, scripts: &[(&str, &str)]) -> Self {
                std::fs::create_dir_all(&root).unwrap();
                std::fs::create_dir_all(root.join("workspace")).unwrap();
                let commands = scripts
                .iter()
                .map(|(name, body)| {
                    let path = root.join(name);
                    std::fs::write(&path, body).unwrap();
                    use std::os::unix::fs::PermissionsExt;
                    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
                    permissions.set_mode(0o755);
                    std::fs::set_permissions(&path, permissions).unwrap();
                    format!(
                        "[[permissions.commands]]\nbin = {:?}\nargs = []\npurpose = \"discovery fixture\"\nisolation = \"trusted_host\"\n",
                        path.as_str()
                    )
                })
                .collect::<String>();
                let command_table = if scripts.is_empty() {
                    "commands = []\n".to_owned()
                } else {
                    String::new()
                };
                std::fs::write(
                    root.join(MANIFEST_FILE),
                    format!(
                        r#"
[generator]
id = "agent-discovery-test"
name = "Agent discovery test"
version = "0.1.0"
[llm]
models = []
max_tokens = 100
[permissions]
network = []
fs_read = []
fs_write = ["workspace"]
{command_table}side_effects = "none"
side_effects_scope = "invocation"
containers = {{ enabled = false }}
{commands}
[[flow]]
id = "agent"
type = "llm.agent"
[flow.params]
max_iterations = 1
max_tokens_total = 100
"#
                    ),
                )
                .unwrap();
                let contract = contract::Contract::load(&root).unwrap();
                let node = contract.manifest.flow[0].clone();
                let run = engine::RunContext::for_tests(
                    "agent-discovery-test".into(),
                    root.join("workspace"),
                    root.join("meta"),
                    contract,
                    BTreeMap::new(),
                    BTreeMap::new(),
                )
                .unwrap();
                Self { root, run, node }
            }

            fn command(&self, name: &str) -> Vec<String> {
                vec![self.root.join(name).to_string()]
            }

            fn journal(&self) -> engine::JournalWriter {
                engine::JournalWriter::create(&self.journal_path(), &self.run.run_id, false, None)
                    .unwrap()
            }

            fn journal_path(&self) -> camino::Utf8PathBuf {
                self.root.join("journal.jsonl")
            }

            fn disk_state(&self) -> engine::RunState {
                engine::RunState::fold_journal(&self.journal_path()).unwrap()
            }

            fn events(&self, kind: &str) -> Vec<serde_json::Value> {
                std::fs::read_to_string(self.journal_path())
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .filter(|event| event["t"] == kind)
                    .collect()
            }

            fn launches(&self) -> Vec<serde_json::Value> {
                self.events("operation_started")
                    .into_iter()
                    .filter(|event| event["kind"] == "mcp.stdio_launch")
                    .collect()
            }

            fn finishes(&self) -> Vec<serde_json::Value> {
                self.events("operation_finished")
            }

            fn context<'a>(
                &'a self,
                journal: &'a engine::JournalWriter,
                vars: &'a mut contract::ValueBag,
            ) -> StepContext<'a> {
                StepContext {
                    run: &self.run,
                    journal,
                    vars,
                    llm: None,
                }
            }
        }

        fn runtime(specs: Vec<serde_json::Value>) -> llm::LlmRuntime {
            let mut runtime = llm::LlmRuntime::builtins();
            runtime.mcp = mcp::McpRuntime::from_specs(
                specs
                    .into_iter()
                    .map(|spec| serde_json::from_value(spec).unwrap())
                    .collect(),
            )
            .unwrap();
            runtime
        }

        fn stdio_spec(id: &str, command: Vec<String>) -> serde_json::Value {
            serde_json::json!({
                "id": id, "transport": "stdio", "lifecycle": "initialize",
                "command": command, "auth": "none", "timeout_seconds": 3,
            })
        }

        fn declaration(server: &str, alias: &str) -> ToolDecl {
            serde_json::from_value(serde_json::json!({
                "kind": "mcp", "name": alias, "server": server, "tool": "echo",
            }))
            .unwrap()
        }

        fn launch_invocation(fixture: &Fixture, journal: &engine::JournalWriter) -> String {
            format!(
                "{}::agent_discovery",
                engine::RunContext::execution_invocation(journal, &fixture.node)
            )
        }

        fn start_launch(
            fixture: &Fixture,
            journal: &engine::JournalWriter,
            target: &str,
        ) -> String {
            let invocation = format!("{}::{target}", launch_invocation(fixture, journal));
            match fixture
                .run
                .guard_external_operation(
                    journal,
                    &fixture.node,
                    "mcp.stdio_launch",
                    target,
                    &None,
                    &invocation,
                )
                .unwrap()
            {
                engine::GuardDecision::Proceed { operation_id } => operation_id,
                engine::GuardDecision::Resend { .. } => panic!("fresh launch must start"),
            }
        }

        #[tokio::test]
        async fn child_startup_is_journaled_before_connect_and_failed_admission_spawns_no_child() {
            let root = Fixture::root();
            let marker = root.join("child-started");
            let script = marker_server(marker.as_str());
            let fixture = Fixture::new(root, &[("fail.py", &script)]);
            let runtime = runtime(vec![stdio_spec("local", fixture.command("fail.py"))]);
            let journal = fixture.journal();
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let ctx = fixture.context(&journal, &mut vars);
            assert!(
                McpAgentTools::prepare(
                    &ctx,
                    &fixture.node,
                    &runtime,
                    &[declaration("local", "echo")]
                )
                .await
                .is_err(),
                "a child that speaks no MCP must fail discovery"
            );
            assert!(marker.exists(), "the child was started");

            let launches = fixture.launches();
            assert_eq!(launches.len(), 1, "the launch is journaled before connect");
            assert_eq!(launches[0]["kind"], "mcp.stdio_launch");
            assert_eq!(launches[0]["target"], "local");
            assert!(
                fixture
                    .events("operation_started")
                    .iter()
                    .all(|event| event["kind"] == "mcp.stdio_launch"),
                "no tool call is journaled by discovery"
            );
            let id = launches[0]["operation_id"].as_str().unwrap();
            assert!(fixture.finishes().is_empty());
            assert_eq!(
                fixture.disk_state().operation_records[id].status,
                OperationStatus::Started
            );
            let state = fixture.disk_state();
            assert!(state.ensure_no_fail_closed_inflight_operations().is_err());
            assert!(state.ensure_no_inflight_operations().is_err());

            // Replay of the same interrupted launch is refused before any new
            // child can be spawned: the marker records whether spawn happened.
            std::fs::remove_file(&marker).unwrap();
            assert!(
                McpAgentTools::prepare(
                    &ctx,
                    &fixture.node,
                    &runtime,
                    &[declaration("local", "echo")]
                )
                .await
                .is_err(),
                "an unconfirmed launch must refuse replay"
            );
            assert!(!marker.exists(), "refused admission must not spawn a child");
            assert_eq!(fixture.launches().len(), 1);
        }

        #[tokio::test]
        async fn confirmed_close_settles_the_launch_and_resume_reuses_the_invocation() {
            let root = Fixture::root();
            let fixture = Fixture::new(root, &[("healthy.py", HEALTHY_SERVER)]);
            let runtime = runtime(vec![stdio_spec("local", fixture.command("healthy.py"))]);
            let journal = fixture.journal();
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let ctx = fixture.context(&journal, &mut vars);
            let mut tools = McpAgentTools::prepare(
                &ctx,
                &fixture.node,
                &runtime,
                &[declaration("local", "echo")],
            )
            .await
            .expect("healthy discovery must succeed");
            let launches = fixture.launches();
            assert_eq!(launches.len(), 1);
            let id = launches[0]["operation_id"].as_str().unwrap().to_owned();
            assert_eq!(
                fixture.disk_state().operation_records[&id].status,
                OperationStatus::Started,
                "the launch stays open while the child lives"
            );
            assert!(
                fixture
                    .disk_state()
                    .ensure_no_inflight_operations()
                    .is_err()
            );
            assert!(
                launch_invocation(&fixture, &journal).ends_with("::agent_discovery"),
                "agent discovery uses its own invocation namespace"
            );

            tools
                .close_and_settle(&ctx, &fixture.node)
                .await
                .expect("confirmed close settles cleanly");
            let finishes = fixture.finishes();
            assert_eq!(finishes.len(), 1);
            assert_eq!(finishes[0]["operation_id"], id);
            assert_eq!(finishes[0]["status"], "clean");
            assert_eq!(
                fixture.disk_state().operation_records[&id].status,
                OperationStatus::FailedClean
            );
            assert!(fixture.disk_state().ensure_no_inflight_operations().is_ok());

            let resumed = fixture.journal();
            let invocation = format!("{}::local", launch_invocation(&fixture, &resumed));
            match fixture
                .run
                .guard_external_operation(
                    &resumed,
                    &fixture.node,
                    "mcp.stdio_launch",
                    "local",
                    &None,
                    &invocation,
                )
                .unwrap()
            {
                engine::GuardDecision::Proceed { operation_id } => {
                    assert_eq!(operation_id, id, "resume reuses the same invocation id");
                }
                engine::GuardDecision::Resend { .. } => panic!("a clean launch is retried"),
            }
        }

        #[tokio::test]
        async fn one_failed_discovery_leaves_only_its_launch_started() {
            let root = Fixture::root();
            let marker = root.join("broken-started");
            let script = marker_server(marker.as_str());
            let fixture = Fixture::new(
                root,
                &[("healthy.py", HEALTHY_SERVER), ("broken.py", &script)],
            );
            let runtime = runtime(vec![
                stdio_spec("healthy", fixture.command("healthy.py")),
                stdio_spec("broken", fixture.command("broken.py")),
            ]);
            let journal = fixture.journal();
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let ctx = fixture.context(&journal, &mut vars);
            assert!(
                McpAgentTools::prepare(
                    &ctx,
                    &fixture.node,
                    &runtime,
                    &[
                        declaration("healthy", "first"),
                        declaration("healthy", "second"),
                        declaration("broken", "broken-tool"),
                    ],
                )
                .await
                .is_err(),
                "one broken server fails discovery"
            );

            let launches = fixture.launches();
            assert_eq!(launches.len(), 2, "aliases of one server share one launch");
            let mut targets = launches
                .iter()
                .map(|event| event["target"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            targets.sort();
            assert_eq!(targets, vec!["broken".to_owned(), "healthy".to_owned()]);
            let ids = launches
                .iter()
                .map(|event| event["operation_id"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            assert_ne!(ids[0], ids[1], "distinct servers get distinct operations");
            let broken = launches
                .iter()
                .find(|event| event["target"] == "broken")
                .unwrap()["operation_id"]
                .as_str()
                .unwrap()
                .to_owned();
            let healthy = launches
                .iter()
                .find(|event| event["target"] == "healthy")
                .unwrap()["operation_id"]
                .as_str()
                .unwrap()
                .to_owned();
            let finishes = fixture.finishes();
            assert_eq!(finishes.len(), 1, "only the confirmed sibling settles");
            assert_eq!(finishes[0]["operation_id"], healthy);
            let state = fixture.disk_state();
            assert_eq!(
                state.operation_records[&healthy].status,
                OperationStatus::FailedClean
            );
            assert_eq!(
                state.operation_records[&broken].status,
                OperationStatus::Started
            );
            assert!(state.ensure_no_inflight_operations().is_err());
        }

        /// Two profiles admitted to the launch journal but both refused before
        /// any child spawns must BOTH settle. A sibling's primary error must
        /// never leave another launch Started, which would block the next
        /// owner's continuation/deletion with a fictitious unresolved launch.
        #[tokio::test]
        async fn two_clean_prelaunch_refusals_settle_both_launches() {
            let root = Fixture::root();
            // No permitted commands: both stdio profiles are refused before
            // spawn, but each still gets a launch journaled in prepare's setup.
            let fixture = Fixture::new(root, &[]);
            let runtime = runtime(vec![
                stdio_spec("alpha", vec!["/unpermitted/alpha".to_owned()]),
                stdio_spec("beta", vec!["/unpermitted/beta".to_owned()]),
            ]);
            let journal = fixture.journal();
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let ctx = fixture.context(&journal, &mut vars);
            assert!(
                McpAgentTools::prepare(
                    &ctx,
                    &fixture.node,
                    &runtime,
                    &[
                        declaration("alpha", "a-tool"),
                        declaration("beta", "b-tool")
                    ],
                )
                .await
                .is_err(),
                "unpermitted stdio commands must fail discovery"
            );

            let launches = fixture.launches();
            assert_eq!(
                launches.len(),
                2,
                "both profiles were admitted with a launch before connect"
            );
            let ids = launches
                .iter()
                .map(|event| event["operation_id"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            assert_ne!(ids[0], ids[1], "distinct profiles get distinct launches");
            let finishes = fixture.finishes();
            assert_eq!(
                finishes.len(),
                2,
                "each clean pre-launch refusal settles its own launch"
            );
            let state = fixture.disk_state();
            for id in &ids {
                assert_eq!(
                    state.operation_records[id].status,
                    OperationStatus::FailedClean,
                    "launch {id} must be settled clean"
                );
            }
            assert!(
                state.ensure_no_inflight_operations().is_ok(),
                "no fictitious unresolved launch may block recovery"
            );
        }

        #[tokio::test]
        async fn unconfirmed_close_leaves_that_launch_started_and_blocks_deletion() {
            let root = Fixture::root();
            let fixture = Fixture::new(root, &[]);
            let journal = fixture.journal();
            let confirmed = start_launch(&fixture, &journal, "confirmed");
            let unconfirmed = start_launch(&fixture, &journal, "unconfirmed");
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let ctx = fixture.context(&journal, &mut vars);
            settle_after_close(&ctx, &fixture.node, "confirmed", Some(&confirmed), Ok(()))
                .expect("confirmed close settles");
            settle_after_close(
                &ctx,
                &fixture.node,
                "unconfirmed",
                Some(&unconfirmed),
                Err(McpError::TimedOut { seconds: 5 }),
            )
            .expect_err("unconfirmed close is surfaced");

            let finishes = fixture.finishes();
            assert_eq!(finishes.len(), 1);
            assert_eq!(finishes[0]["operation_id"], confirmed);
            let state = fixture.disk_state();
            assert_eq!(
                state.operation_records[&confirmed].status,
                OperationStatus::FailedClean
            );
            assert_eq!(
                state.operation_records[&unconfirmed].status,
                OperationStatus::Started
            );
            assert!(state.ensure_no_inflight_operations().is_err());
        }

        /// A refused admission for one server must not leave a freshly admitted
        /// sibling launch open: nothing has spawned yet, so that launch settles.
        #[tokio::test]
        async fn refused_peer_admission_settles_unused_launch_without_spawning() {
            let root = Fixture::root();
            let marker = root.join("must-not-start");
            let script = marker_server(marker.as_str());
            let fixture = Fixture::new(
                root,
                &[("a-healthy.py", &script), ("z-blocked.py", HEALTHY_SERVER)],
            );
            let runtime = runtime(vec![
                stdio_spec("a-healthy", fixture.command("a-healthy.py")),
                stdio_spec("z-blocked", fixture.command("z-blocked.py")),
            ]);
            let journal = fixture.journal();
            let blocked = format!("{}::z-blocked", launch_invocation(&fixture, &journal));
            match fixture
                .run
                .guard_external_operation(
                    &journal,
                    &fixture.node,
                    "mcp.stdio_launch",
                    "z-blocked",
                    &None,
                    &blocked,
                )
                .unwrap()
            {
                engine::GuardDecision::Proceed { .. } => {}
                engine::GuardDecision::Resend { .. } => panic!("fresh crash record must start"),
            }
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let ctx = fixture.context(&journal, &mut vars);
            assert!(
                McpAgentTools::prepare(
                    &ctx,
                    &fixture.node,
                    &runtime,
                    &[
                        declaration("a-healthy", "first"),
                        declaration("z-blocked", "second"),
                    ],
                )
                .await
                .is_err(),
                "the unconfirmed launch must refuse admission"
            );
            assert!(
                !marker.exists(),
                "no child is spawned after a refused guard"
            );
            let launches = fixture.launches();
            assert_eq!(launches.len(), 2);
            let finishes = fixture.finishes();
            assert_eq!(finishes.len(), 1, "only the unused launch is settled clean");
            let healthy = launches
                .iter()
                .find(|event| event["target"] == "a-healthy")
                .unwrap()["operation_id"]
                .as_str()
                .unwrap();
            let blocked = launches
                .iter()
                .find(|event| event["target"] == "z-blocked")
                .unwrap()["operation_id"]
                .as_str()
                .unwrap();
            assert_eq!(finishes[0]["operation_id"], healthy);
            let state = fixture.disk_state();
            assert_eq!(
                state.operation_records[healthy].status,
                OperationStatus::FailedClean
            );
            assert_eq!(
                state.operation_records[blocked].status,
                OperationStatus::Started
            );
            assert!(state.ensure_no_inflight_operations().is_err());
        }

        #[tokio::test]
        async fn plain_http_discovery_writes_no_launch_record() {
            let root = Fixture::root();
            let fixture = Fixture::new(root, &[]);
            let runtime = runtime(vec![serde_json::json!({
                "id": "remote", "transport": "streamable_http", "lifecycle": "discover",
                "url": "http://127.0.0.1:1/mcp", "auth": "none", "timeout_seconds": 1,
                "allowed_hosts": ["127.0.0.1"],
            })]);
            let journal = fixture.journal();
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let ctx = fixture.context(&journal, &mut vars);
            assert!(
                McpAgentTools::prepare(
                    &ctx,
                    &fixture.node,
                    &runtime,
                    &[declaration("remote", "echo")]
                )
                .await
                .is_err(),
                "the unreachable HTTP server fails discovery"
            );
            assert!(fixture.launches().is_empty());
            assert!(fixture.finishes().is_empty());
        }
    }
}
