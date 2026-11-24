use super::direct::{execute_direct_mcp_call, has_operation_record};
use super::types::McpCallStep;
use super::validate::{
    degrade_optional_mcp_call, mcp_access, mcp_call_params, validate_mcp_call_arguments,
    validate_mcp_call_schema, validate_mcp_call_timeout,
};
use crate::common::render_json_templates;
use async_trait::async_trait;
use contract::{Contract, NodeDef};
use engine::{ResultExt, StepContext, StepError, StepExecutor, StepOutcome, StepTraits};
use policy::{McpAuthorizationAction, params_schema, string_schema};
use serde_json::{Value, json};
use sha2::Digest as _;
#[async_trait]
impl StepExecutor for McpCallStep {
    engine::step_type_id!("mcp.call");
    fn traits(&self) -> StepTraits {
        StepTraits::default()
    }
    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["server", "tool"],
            json!({
                "server": string_schema(),
                "tool": string_schema(),
                "arguments": { "type": "object" },
                "input_schema": {},
                "output_schema": {},
                "timeout_seconds": { "type": "integer", "minimum": 1 },
                "side_effects": { "type": "boolean" },
                "optional": { "type": "boolean" },
                "on_authorization_required": { "type": "string", "enum": ["fail", "wait", "degrade"] },
                "cleanup_failure": { "type": "string", "enum": ["preserve_execution", "fail_fast"], "default": "preserve_execution" },
            }),
        ))
    }
    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = mcp_call_params(node)?;
        if params.server.trim().is_empty() || params.tool.trim().is_empty() {
            return Err(StepError::failed(
                &node.id,
                "mcp.call server and tool must not be empty",
            ));
        }
        if params.arguments.as_object().is_none() {
            return Err(StepError::failed(
                &node.id,
                "mcp.call arguments must be a JSON object",
            ));
        }
        if let Some(schema) = &params.input_schema {
            validate_mcp_call_schema(node, schema, "input_schema")?;
        }
        if let Some(schema) = &params.output_schema {
            validate_mcp_call_schema(node, schema, "output_schema")?;
        }
        let profile = self
            .runtime
            .resolve(&params.server)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        validate_mcp_call_timeout(node, params.timeout_seconds, profile.timeout_seconds())?;
        Ok(())
    }
    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = mcp_call_params(node)?;
        let profile = self
            .runtime
            .resolve(&params.server)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        validate_mcp_call_timeout(node, params.timeout_seconds, profile.timeout_seconds())?;
        let timeout_seconds = params
            .timeout_seconds
            .unwrap_or_else(|| profile.timeout_seconds());
        let arguments = render_json_templates(
            ctx,
            node,
            &params.arguments,
            Some(profile.max_response_bytes()),
        )?;
        let argument_bytes = serde_json::to_vec(&arguments)?;
        if argument_bytes.len() > profile.max_response_bytes() {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "mcp.call arguments exceed MCP profile limit of {} bytes",
                    profile.max_response_bytes()
                ),
            ));
        }
        validate_mcp_call_arguments(node, params.input_schema.as_ref(), &arguments)?;
        authorize_direct_destinations(ctx, node, profile, &params.server, &params.tool, &arguments)
            .await?;
        let access = mcp_access(ctx, profile.command());
        let confirm_details = Some(json!({
            "server": params.server,
            "tool": params.tool,
            "argument_names": arguments
                .as_object()
                .map(|object| {
                    let mut names: Vec<String> = object.keys().cloned().collect();
                    names.sort();
                    names
                })
                .unwrap_or_default(),
            "arguments_sha256": hex::encode(sha2::Sha256::digest(&argument_bytes)),
        }));
        let confirm_target = format!("{}/{}", params.server, params.tool);
        let confirm_invocation = engine::RunContext::execution_invocation(ctx.journal, node);
        if params.side_effects
            && let Some(confirm) = ctx.run.require_side_effect(
                ctx.journal,
                node,
                "mcp.call",
                &confirm_target,
                confirm_details,
                &confirm_invocation,
            )?
        {
            return Ok(StepOutcome::NeedsConfirm {
                confirm,
                files: Vec::new(),
            });
        }
        // A stdio connect can spawn a child before any tool call is guarded.
        // Journal its own launch identity first: a kill during connect or
        // discovery must leave an unconfirmed effect that replay/delete refuse.
        let launch_operation = if profile.command().is_empty() {
            None
        } else {
            let launch_invocation = format!("{confirm_invocation}::stdio_launch");
            match ctx.run.guard_external_operation(
                ctx.journal,
                node,
                "mcp.stdio_launch",
                &params.server,
                &None,
                &launch_invocation,
            )? {
                engine::GuardDecision::Proceed { operation_id } => Some(operation_id),
                // A prior launch is already settled; nothing left to finish.
                engine::GuardDecision::Resend { .. } => None,
            }
        };
        let runtime = self.runtime.clone();
        let server = params.server.clone();
        let tool_name = params.tool.clone();
        let cancellation = ctx.run.cancellation.clone();
        let connect = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_seconds),
            runtime.connect(&server, &access, cancellation),
        )
        .await;
        let session = match connect {
            Ok(Ok(session)) => session,
            Ok(Err(mcp::McpError::AuthorizationRequired { server })) => {
                settle_stdio_launch(ctx, node, launch_operation.as_deref())?;
                let (action, source) = params.authorization_action();
                ctx.journal
                    .event(
                        "mcp_authorization_required",
                        json!({"node": node.id, "server": &server, "phase": "connect", "action": action, "source": source}),
                    )
                    .step_err(&node.id)?;
                return match action {
                    McpAuthorizationAction::Fail => Err(StepError::McpAuthorizationRequired {
                        node: node.id.clone(),
                        server,
                    }),
                    McpAuthorizationAction::Wait => Ok(StepOutcome::NeedsUser {
                        question: engine::mcp_authorization_form_spec(format!(
                            "MCP server `{server}`"
                        )),
                        files: Vec::new(),
                    }),
                    McpAuthorizationAction::Degrade if has_operation_record(ctx, node) => {
                        Err(StepError::Refused { node: node.id.clone(), message: "cannot degrade authorization after an MCP operation may have executed".into() })
                    }
                    McpAuthorizationAction::Degrade => Ok(StepOutcome::success(None)),
                };
            }
            Ok(Err(error)) => {
                // Configuration and authorization failures prove no process
                // was launched (or no tool effect happened); transport
                // failures may have spawned it, so leave those Started.
                if matches!(
                    error,
                    mcp::McpError::Configuration(_) | mcp::McpError::Authorization(_)
                ) {
                    settle_stdio_launch(ctx, node, launch_operation.as_deref())?;
                }
                let error = StepError::failed(&node.id, error.to_string());
                return degrade_optional_mcp_call(ctx, node, &params, &arguments, error).await;
            }
            Err(_) => {
                let error = StepError::failed(&node.id, "MCP connection timed out");
                return degrade_optional_mcp_call(ctx, node, &params, &arguments, error).await;
            }
        };
        let started = std::time::Instant::now();
        let execution = execute_direct_mcp_call(
            &session,
            &tool_name,
            arguments.clone(),
            ctx,
            node,
            timeout_seconds,
            profile.max_response_bytes(),
        )
        .await;
        let close_result = session.close().await;
        let cleanup_error = close_result
            .err()
            .map(|error| StepError::failed(&node.id, error.to_string()));
        let outcome = super::settlement::settle_direct_mcp(
            ctx,
            node,
            &params,
            execution,
            &arguments,
            cleanup_error.as_ref(),
            started.elapsed().as_millis() as u64,
        );
        // Only a confirmed close proves the launched server is gone. An
        // unconfirmed close leaves the launch Started so replay/delete refuse.
        settle_stdio_launch_after_close(
            ctx,
            node,
            launch_operation.as_deref(),
            cleanup_error.as_ref(),
        )?;
        outcome
    }
}

/// Settle the launch after a tool call only when its close was confirmed:
/// otherwise the stdio server may still be alive, so keep the launch Started.
fn settle_stdio_launch_after_close(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    operation_id: Option<&str>,
    close_error: Option<&StepError>,
) -> Result<(), StepError> {
    if close_error.is_none() {
        settle_stdio_launch(ctx, node, operation_id)?;
    }
    Ok(())
}

/// Settle a stdio launch only when the exit proves no live server remains.
fn settle_stdio_launch(
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

/// Reclassify before static policy/decider checks; audit hosts only and recheck fresh policy on resume.
async fn authorize_direct_destinations(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    profile: &mcp::McpProfile,
    server: &str,
    tool: &str,
    args: &Value,
) -> Result<(), engine::StepError> {
    // The operator asserts this remote tool never fetches URLs or hosts, so
    // its arguments cannot contain a destination to classify or authorize.
    if profile.non_network_tools().iter().any(|name| name == tool) {
        return Ok(());
    }
    let extracted = check_direct_destinations(
        node,
        server,
        tool,
        profile.allowed_destinations(),
        args,
        profile.destination_decider().is_some(),
    )?;
    let Some(argv) = profile.destination_decider().filter(|_| {
        extracted
            .iter()
            .any(|host| !policy::mcp_destination_is_allowed(profile.allowed_destinations(), host))
    }) else {
        return Ok(());
    };
    let timeout = profile.destination_decider_timeout_seconds();
    let gateway = engine::CmdGateway::new(
        ctx.run.contract.manifest.permissions.clone(),
        ctx.run.fs.workspace().to_owned(),
    )
    .with_cancellation(ctx.run.cancellation.clone());
    let verdict =
        engine::decide_destination(&gateway, server, tool, &extracted, argv, timeout).await;
    let record_decision = |allow| {
        engine::record_destination_decision(
            ctx.journal,
            &node.id,
            server,
            tool,
            &extracted,
            timeout,
            allow,
        )
    };
    match verdict {
        Err(engine::GatewayError::Canceled) => Err(engine::StepError::Cancelled),
        Err(error) => {
            record_decision(false)?;
            Err(engine::StepError::Refused {
                node: node.id.clone(),
                message: format!(
                    "MCP tool `{server}/{tool}` destination decider for server `{server}` failed ({error}); refusing the fetch"
                ),
            })
        }
        Ok(allow) => {
            record_decision(allow)?;
            if allow {
                Ok(())
            } else {
                let denied = extracted.join(", ");
                Err(engine::StepError::Refused {
                    node: node.id.clone(),
                    message: format!(
                        "MCP tool `{server}/{tool}` destination `{denied}` was denied by the destination decider for server `{server}`"
                    ),
                })
            }
        }
    }
}
fn check_direct_destinations(
    node: &NodeDef,
    server: &str,
    tool: &str,
    allowed: &[String],
    args: &Value,
    defer_static_miss: bool,
) -> Result<Vec<String>, engine::StepError> {
    let extracted: Vec<String> = policy::extract_mcp_destinations(args)
        .map_err(|message| StepError::Refused {
            node: node.id.clone(),
            message: message.into(),
        })?
        .into_iter()
        .collect();
    let denied: Vec<&str> = extracted
        .iter()
        .filter(|host| !policy::mcp_destination_is_allowed(allowed, host))
        .map(String::as_str)
        .collect();
    if !defer_static_miss && !denied.is_empty() {
        return Err(engine::StepError::Refused {
            node: node.id.clone(),
            message: format!(
                "MCP tool `{server}/{tool}` destination `{}` is not in server `{server}` allowed_destinations; ask the operator to allow it in providers.toml",
                denied.join(", ")
            ),
        });
    }
    Ok(extracted)
}
#[cfg(test)]
mod tests {
    use super::*;
    use contract::{NodeDef, OnDeps, StepType};
    use serde_json::json;

    fn node() -> NodeDef {
        NodeDef {
            id: "mcp-call".into(),
            kind: StepType::literal("mcp.call"),
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

    fn direct_profile(non_network_tools: Option<&[&str]>) -> mcp::McpProfile {
        let mut config = json!({
            "id": "exa", "transport": "streamable_http", "lifecycle": "initialize",
            "url": "https://mcp.example.test/mcp", "auth": "none",
            "allowed_hosts": ["mcp.example.test"], "allowed_destinations": [],
        });
        if let Some(names) = non_network_tools {
            config["non_network_tools"] = json!(names);
        }
        let spec: mcp::McpServerSpec =
            serde_json::from_value(config).expect("test profile must deserialize");
        let runtime = mcp::McpRuntime::from_specs(vec![spec]).expect("test profile must load");
        runtime.resolve("exa").expect("profile resolves").clone()
    }

    fn direct_context(root: &camino::Utf8PathBuf) -> (engine::RunContext, engine::JournalWriter) {
        std::fs::write(
            root.join(contract::MANIFEST_FILE),
            r#"[generator]
id = "direct-destination-test"
name = "Direct destination test"
version = "0.1.0"
[[flow]]
id = "call"
type = "mcp.call"
[flow.params]
server = "exa"
tool = "local_only"
"#,
        )
        .unwrap();
        let contract = contract::Contract::load(root).expect("test contract must load");
        let run = engine::RunContext::for_tests(
            "direct-destination-test".into(),
            root.join("workspace"),
            root.join("meta"),
            contract,
            std::collections::BTreeMap::new(),
            std::collections::BTreeMap::new(),
        )
        .expect("test run context");
        let journal =
            engine::JournalWriter::create(&root.join("journal.jsonl"), &run.run_id, false, None)
                .expect("test journal");
        (run, journal)
    }

    #[tokio::test]
    async fn direct_declared_non_network_tool_skips_extraction_but_undeclared_tools_do_not() {
        let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("direct-destination-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let node = node();
        let args = json!({"file": "report.md"});
        let (run, journal) = direct_context(&root);
        let mut vars = contract::ValueBag::with_inputs(std::collections::BTreeMap::new());
        let ctx = StepContext {
            run: &run,
            journal: &journal,
            vars: &mut vars,
            llm: None,
        };

        let omitted = direct_profile(None);
        let error =
            authorize_direct_destinations(&ctx, &node, &omitted, "exa", "local_only", &args)
                .await
                .expect_err("omitted field must keep the fail-closed extraction");
        assert!(matches!(error, StepError::Refused { .. }), "{error}");
        assert!(error.to_string().contains("report.md"), "{error}");

        let declared = direct_profile(Some(&["local_only"]));
        authorize_direct_destinations(&ctx, &node, &declared, "exa", "local_only", &args)
            .await
            .expect("declared tool must pass under an empty ceiling");

        let other_tool = direct_profile(Some(&["other_tool"]));
        assert!(
            authorize_direct_destinations(&ctx, &node, &other_tool, "exa", "local_only", &args)
                .await
                .is_err(),
            "only the exact tool name may skip extraction"
        );
        drop(ctx);
        drop(journal);
        drop(run);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn direct_schemeless_and_ambiguous_destinations() {
        for args in [
            json!({"urls":["example.test"]}),
            json!({"url":"//example.test/x"}),
            json!({"nested":["https://example.test/x", ["example.test/path"]]}),
        ] {
            assert!(check_direct_destinations(&node(), "exa", "fetch", &[], &args, false).is_err());
            for allowed in ["example.test", "*"] {
                assert_eq!(
                    check_direct_destinations(
                        &node(),
                        "exa",
                        "fetch",
                        &[allowed.into()],
                        &args,
                        false
                    )
                    .unwrap(),
                    vec!["example.test".to_owned()]
                );
            }
            check_direct_destinations(&node(), "exa", "fetch", &[], &args, true).unwrap();
        }
        for defer in [false, true] {
            for args in [
                json!({"url":"not a URL"}),
                json!({"urls":["example.test","//"]}),
                json!({"target":"private-unparseable-value"}),
            ] {
                let error =
                    check_direct_destinations(&node(), "exa", "fetch", &["*".into()], &args, defer)
                        .unwrap_err();
                assert!(matches!(error, StepError::Refused { .. }));
                assert!(error.to_string().contains("ambiguous MCP destination"));
                assert!(!error.to_string().contains("private-unparseable-value"));
            }
        }
    }

    struct LaunchHarness {
        root: camino::Utf8PathBuf,
        run: engine::RunContext,
        node: NodeDef,
    }

    impl Drop for LaunchHarness {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    impl LaunchHarness {
        fn new(server: &str, command: Option<&str>) -> Self {
            let root = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .unwrap()
                .join(format!("mcp-launch-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).unwrap();
            let commands = command
                .map(|bin| {
                    format!(
                        "[[permissions.commands]]\nbin = \"{bin}\"\nargs = []\npurpose = \"launch fixture\"\nisolation = \"trusted_host\"\n"
                    )
                })
                .unwrap_or_else(|| "commands = []\n".into());
            std::fs::write(
                root.join(contract::MANIFEST_FILE),
                format!(
                    r#"
[generator]
id = "launch-test"
name = "Launch test"
version = "0.1.0"
[permissions]
network = []
fs_read = []
fs_write = ["workspace"]
side_effects = "none"
side_effects_scope = "invocation"
containers = {{ enabled = false }}
{commands}
[[flow]]
id = "call"
type = "mcp.call"
[flow.params]
server = "{server}"
tool = "echo"
arguments = {{}}
side_effects = false
"#
                ),
            )
            .unwrap();
            let contract = contract::Contract::load(&root).unwrap();
            let node = contract.manifest.flow[0].clone();
            let run = engine::RunContext::for_tests(
                "launch-test".into(),
                root.join("workspace"),
                root.join("meta"),
                contract,
                std::collections::BTreeMap::new(),
                std::collections::BTreeMap::new(),
            )
            .unwrap();
            Self { root, run, node }
        }

        fn journal(&self) -> engine::JournalWriter {
            engine::JournalWriter::create(
                &self.root.join("journal.jsonl"),
                &self.run.run_id,
                false,
                None,
            )
            .unwrap()
        }

        fn events(&self, kind: &str) -> Vec<serde_json::Value> {
            std::fs::read_to_string(self.root.join("journal.jsonl"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|event| event["t"] == kind)
                .collect()
        }
    }

    fn stdio_step(server: &str, command: &str) -> McpCallStep {
        let spec = serde_json::from_value(json!({
            "id": server, "transport": "stdio", "command": [command],
            "auth": "none", "timeout_seconds": 1,
        }))
        .unwrap();
        McpCallStep {
            runtime: std::sync::Arc::new(mcp::McpRuntime::from_specs(vec![spec]).unwrap()),
        }
    }

    async fn execute_launch(
        harness: &LaunchHarness,
        journal: &engine::JournalWriter,
        step: &McpCallStep,
    ) -> Result<StepOutcome, StepError> {
        let mut vars = contract::ValueBag::with_inputs(std::collections::BTreeMap::new());
        let mut ctx = StepContext {
            run: &harness.run,
            journal,
            vars: &mut vars,
            llm: None,
        };
        step.execute(&mut ctx, &harness.node).await
    }

    fn tool_operation_id(harness: &LaunchHarness, journal: &engine::JournalWriter) -> String {
        let invocation = engine::RunContext::execution_invocation(journal, &harness.node);
        engine::operation_id_for(&harness.run.run_id, &harness.node.id, &invocation)
    }

    #[tokio::test]
    async fn stdio_launch_is_durable_before_connect_and_distinct_from_the_tool_guard() {
        let bin = format!("/nonexistent/qcg-mcp-launch-{}", uuid::Uuid::new_v4());
        let harness = LaunchHarness::new("local", Some(&bin));
        let journal = harness.journal();
        let error = execute_launch(&harness, &journal, &stdio_step("local", &bin))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("MCP transport failed"),
            "{error}"
        );
        let starts = harness.events("operation_started");
        assert_eq!(starts.len(), 1, "connect alone must have a durable launch");
        assert_eq!(starts[0]["kind"], "mcp.stdio_launch");
        assert_ne!(
            starts[0]["operation_id"],
            tool_operation_id(&harness, &journal)
        );
    }

    #[tokio::test]
    async fn clean_connect_failure_settles_the_stdio_launch() {
        // No command permission: access validation fails before any spawn.
        let harness = LaunchHarness::new("local", None);
        let journal = harness.journal();
        let error = execute_launch(
            &harness,
            &journal,
            &stdio_step("local", "/nonexistent/qcg-mcp-clean-launch"),
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("not allowed by permissions.commands"),
            "{error}"
        );
        let starts = harness.events("operation_started");
        assert_eq!(starts.len(), 1);
        let id = starts[0]["operation_id"].as_str().unwrap();
        let finishes = harness.events("operation_finished");
        assert_eq!(finishes.len(), 1);
        assert_eq!(finishes[0]["operation_id"], id);
        assert_eq!(finishes[0]["status"], "clean");
        assert_eq!(
            journal.state().operation_records[id].status,
            engine::OperationStatus::FailedClean
        );
    }

    fn start_launch_operation(harness: &LaunchHarness, journal: &engine::JournalWriter) -> String {
        let invocation = format!(
            "{}::stdio_launch",
            engine::RunContext::execution_invocation(journal, &harness.node)
        );
        match harness
            .run
            .guard_external_operation(
                journal,
                &harness.node,
                "mcp.stdio_launch",
                "local",
                &None,
                &invocation,
            )
            .unwrap()
        {
            engine::GuardDecision::Proceed { operation_id } => operation_id,
            engine::GuardDecision::Resend { .. } => panic!("fresh launch must start"),
        }
    }

    #[test]
    fn confirmed_session_close_settles_the_stdio_launch() {
        let harness = LaunchHarness::new("local", Some("/nonexistent/unused"));
        let journal = harness.journal();
        let id = start_launch_operation(&harness, &journal);
        let mut vars = contract::ValueBag::with_inputs(std::collections::BTreeMap::new());
        let ctx = StepContext {
            run: &harness.run,
            journal: &journal,
            vars: &mut vars,
            llm: None,
        };
        settle_stdio_launch_after_close(&ctx, &harness.node, Some(&id), None).unwrap();

        let finishes = harness.events("operation_finished");
        assert_eq!(finishes.len(), 1);
        assert_eq!(finishes[0]["operation_id"], id);
        assert_eq!(finishes[0]["status"], "clean");
        assert_eq!(
            journal.state().operation_records[&id].status,
            engine::OperationStatus::FailedClean
        );
    }

    #[cfg(unix)]
    fn stdio_initialize_step(server: &str, command: &str) -> McpCallStep {
        let spec = serde_json::from_value(json!({
            "id": server, "transport": "stdio", "command": [command],
            "lifecycle": "initialize", "auth": "none", "timeout_seconds": 3,
        }))
        .unwrap();
        McpCallStep {
            runtime: std::sync::Arc::new(mcp::McpRuntime::from_specs(vec![spec]).unwrap()),
        }
    }

    /// A newline-delimited stdio MCP server: connect, tools/list and the tool
    /// call all succeed over a real stdio transport.
    #[cfg(unix)]
    fn echo_stdio_server(dir: &camino::Utf8Path) -> camino::Utf8PathBuf {
        let script = dir.join("echo_server.py");
        std::fs::write(
            &script,
            r#"#!/usr/bin/env python3
import json, sys, time
for line in sys.stdin:
    try:
        request = json.loads(line)
    except ValueError:
        continue
    if "id" not in request:
        continue
    method = request.get("method")
    if method == "initialize":
        result = {"protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
                  "serverInfo": {"name": "echo-fixture", "version": "1"}}
    elif method == "tools/list":
        result = {"tools": [{"name": "echo", "description": "echo",
                             "inputSchema": {"type": "object"}}]}
    elif method == "tools/call":
        result = {"content": [{"type": "text", "text": "ok"}],
                  "structuredContent": {"done": True}}
    else:
        result = {}
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}) + "\n")
    sys.stdout.flush()
while True:
    time.sleep(3600)
"#,
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        script
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unconfirmed_session_close_keeps_the_stdio_launch_started() {
        let script_dir = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("mcp-echo-fixture-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&script_dir).unwrap();
        let server = echo_stdio_server(&script_dir);
        let harness = LaunchHarness::new("local", Some(server.as_str()));
        std::fs::create_dir_all(&harness.run.workspace).unwrap();
        let journal = harness.journal();
        // The probe now fails the owned transport's close, which the SDK
        // service loop logs and swallows; connect, tools/list, the tool call
        // and settlement all run the real path.
        let _fail_close = mcp::fail_close();
        let step = stdio_initialize_step("local", server.as_str());
        let outcome = execute_launch(&harness, &journal, &step).await.unwrap();
        assert!(
            matches!(&outcome, StepOutcome::Success { output: Some(value), .. }
                if value["server"] == "local" && value["tool"] == "echo" && value["result"]["structuredContent"]["done"] == true),
            "{outcome:?}"
        );
        let starts = harness.events("operation_started");
        let launches: Vec<_> = starts
            .iter()
            .filter(|event| event["kind"] == "mcp.stdio_launch")
            .collect();
        assert_eq!(launches.len(), 1, "execute() must journal the launch");
        let id = launches[0]["operation_id"].as_str().unwrap();
        let finishes = harness.events("operation_finished");
        assert!(
            finishes.iter().all(|event| event["operation_id"] != id),
            "an unconfirmed close must not finish the launch: {finishes:?}"
        );
        assert_eq!(
            journal.state().operation_records[id].status,
            engine::OperationStatus::Started
        );
        let calls: Vec<_> = std::fs::read_to_string(harness.root.join("audit.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|event| event["t"] == "tool_call")
            .collect();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["cleanup_error"]["code"], "cleanup_failed");
        drop(journal);
        drop(harness);
        let _ = std::fs::remove_dir_all(&script_dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn confirmed_stdio_close_settles_the_launch() {
        let script_dir = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("mcp-echo-fixture-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&script_dir).unwrap();
        let server = echo_stdio_server(&script_dir);
        let harness = LaunchHarness::new("local", Some(server.as_str()));
        std::fs::create_dir_all(&harness.run.workspace).unwrap();
        let journal = harness.journal();
        let step = stdio_initialize_step("local", server.as_str());
        let outcome = execute_launch(&harness, &journal, &step).await.unwrap();
        assert!(
            matches!(&outcome, StepOutcome::Success { output: Some(value), .. }
                if value["server"] == "local" && value["tool"] == "echo"),
            "{outcome:?}"
        );
        let starts = harness.events("operation_started");
        let launches: Vec<_> = starts
            .iter()
            .filter(|event| event["kind"] == "mcp.stdio_launch")
            .collect();
        assert_eq!(launches.len(), 1);
        let id = launches[0]["operation_id"].as_str().unwrap();
        let finishes: Vec<_> = harness
            .events("operation_finished")
            .into_iter()
            .filter(|event| event["operation_id"] == id)
            .collect();
        assert_eq!(
            finishes.len(),
            1,
            "a confirmed close must finish the launch"
        );
        assert_eq!(finishes[0]["status"], "clean");
        assert_eq!(
            journal.state().operation_records[id].status,
            engine::OperationStatus::FailedClean
        );
        assert!(
            journal
                .state()
                .ensure_no_fail_closed_inflight_operations()
                .is_ok(),
            "a confirmed close must unblock replay/delete"
        );
        let calls: Vec<_> = std::fs::read_to_string(harness.root.join("audit.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|event| event["t"] == "tool_call")
            .collect();
        assert_eq!(calls.len(), 1);
        assert!(calls[0]["cleanup_error"].is_null());
        drop(journal);
        drop(harness);
        let _ = std::fs::remove_dir_all(&script_dir);
    }

    #[tokio::test]
    async fn indeterminate_connect_failure_leaves_the_stdio_launch_started() {
        let bin = format!("/nonexistent/qcg-mcp-launch-{}", uuid::Uuid::new_v4());
        let harness = LaunchHarness::new("local", Some(&bin));
        let journal = harness.journal();
        assert!(
            execute_launch(&harness, &journal, &stdio_step("local", &bin))
                .await
                .is_err()
        );
        let id = harness.events("operation_started")[0]["operation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(harness.events("operation_finished").is_empty());
        assert_eq!(
            journal.state().operation_records[&id].status,
            engine::OperationStatus::Started
        );
    }

    #[tokio::test]
    async fn http_profile_writes_no_stdio_launch_record() {
        let harness = LaunchHarness::new("exa", None);
        let journal = harness.journal();
        let step = McpCallStep {
            runtime: std::sync::Arc::new(
                mcp::McpRuntime::from_specs(vec![
                    serde_json::from_value(json!({
                        "id": "exa", "transport": "streamable_http",
                        "url": "https://mcp.example.test/mcp", "auth": "none",
                        "allowed_hosts": ["mcp.example.test"],
                    }))
                    .unwrap(),
                ])
                .unwrap(),
            ),
        };
        assert!(execute_launch(&harness, &journal, &step).await.is_err());
        assert!(harness.events("operation_started").is_empty());
        assert!(harness.events("operation_finished").is_empty());
    }

    #[tokio::test]
    async fn stdio_launch_does_not_change_the_original_invocation_record() {
        let harness = LaunchHarness::new("local", None);
        let journal = harness.journal();
        let invocation = engine::RunContext::execution_invocation(&journal, &harness.node);
        harness
            .run
            .guard_external_operation(
                &journal,
                &harness.node,
                "mcp:local/echo",
                "local/echo",
                &Some(json!({})),
                &invocation,
            )
            .unwrap();
        let tool_id = engine::operation_id_for(&harness.run.run_id, &harness.node.id, &invocation);
        let mut vars = contract::ValueBag::with_inputs(std::collections::BTreeMap::new());
        let ctx = StepContext {
            run: &harness.run,
            journal: &journal,
            vars: &mut vars,
            llm: None,
        };
        assert!(has_operation_record(&ctx, &harness.node));
        assert!(
            execute_launch(
                &harness,
                &journal,
                &stdio_step("local", "/nonexistent/unused")
            )
            .await
            .is_err()
        );
        assert!(has_operation_record(&ctx, &harness.node));
        assert_eq!(
            journal.state().operation_records[&tool_id].status,
            engine::OperationStatus::Started
        );
        let starts = harness.events("operation_started");
        assert_eq!(starts.len(), 2);
        assert_ne!(starts[1]["operation_id"], tool_id);
    }

    #[test]
    fn direct_destination_policy_denies_unlisted_fetch_but_passes_query() {
        let node = node();
        // Empty default denies every extracted host; query-only calls pass.
        let error = check_direct_destinations(
            &node,
            "exa-public",
            "web_fetch_exa",
            &[],
            &json!({"url": "https://example.test/x"}),
            false,
        )
        .expect_err("unlisted fetch destination must be refused");
        assert!(
            matches!(error, engine::StepError::Refused { .. }),
            "denials are Refused, never silent: {error}"
        );
        check_direct_destinations(
            &node,
            "exa-public",
            "web_search_exa",
            &[],
            &json!({"query": "rust", "numResults": 5}),
            false,
        )
        .expect("pure-query search must pass the empty default");
        // An explicit operator allow passes fetch; other hosts still fail.
        check_direct_destinations(
            &node,
            "exa-public",
            "web_fetch_exa",
            &["example.test".to_string()],
            &json!({"url": "https://example.test/x"}),
            false,
        )
        .expect("listed destination must pass");
        assert!(
            check_direct_destinations(
                &node,
                "exa-public",
                "web_fetch_exa",
                &["example.test".to_string()],
                &json!({"url": "https://other.test/x"}),
                false,
            )
            .is_err(),
            "unlisted hosts stay denied under a non-empty list"
        );
        // The wildcard covers every host.
        check_direct_destinations(
            &node,
            "exa-public",
            "web_fetch_exa",
            &["*".to_string()],
            &json!({"url": "https://other.test/x"}),
            false,
        )
        .expect("wildcard must allow any destination");
    }
}
