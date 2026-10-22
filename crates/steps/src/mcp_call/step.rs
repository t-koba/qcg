use crate::common::render_json_templates;
use async_trait::async_trait;
use contract::{Contract, NodeDef};
use engine::{StepContext, StepError, StepExecutor, StepOutcome, StepTraits, tool_call_sources};
use policy::{params_schema, string_schema};
use serde_json::{Value, json};
use sha2::Digest as _;

use super::direct::{
    DirectMcpToolEvent, execute_direct_mcp_call, record_direct_mcp_tool_event,
    validate_direct_mcp_result,
};
use super::types::McpCallStep;
use super::validate::{
    DirectMcpCallOutcome, degrade_optional_mcp_call, mcp_access, mcp_call_params,
    validate_mcp_call_arguments, validate_mcp_call_schema, validate_mcp_call_timeout,
};

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
            // The approval binds the full argument value, not just
            // names: approving one call must never authorize a
            // regenerated different payload.
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
            return Ok(StepOutcome::NeedsConfirm { confirm });
        }

        let runtime = self.runtime.clone();
        let server = params.server.clone();
        let tool_name = params.tool.clone();
        let optional = params.optional;
        let cancellation = ctx.run.cancellation.clone();
        let connect = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_seconds),
            runtime.connect(&server, &access, cancellation),
        )
        .await;
        let session = match connect {
            Ok(Ok(session)) => session,
            Ok(Err(error)) => {
                let error = StepError::failed(&node.id, error.to_string());
                return degrade_optional_mcp_call(ctx, node, &params, &arguments, error).await;
            }
            Err(_) => {
                let error = StepError::failed(&node.id, "MCP connection timed out");
                return degrade_optional_mcp_call(ctx, node, &params, &arguments, error).await;
            }
        };
        let started = std::time::Instant::now();
        // The tool call and the session cleanup are two separate facts.
        // The call settles on its own outcome, and a cleanup failure is
        // retained beside that outcome instead of erasing it.
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
        let cleanup_error = session
            .close()
            .await
            .err()
            .map(|error| StepError::failed(&node.id, error.to_string()));
        let mut event = DirectMcpToolEvent {
            server: &params.server,
            tool: &params.tool,
            arguments: &arguments,
            result: None,
            error: None,
            cleanup_error: None,
            duration_ms: started.elapsed().as_millis() as u64,
            degraded: false,
        };
        let (result, cleanup_error) = match settle_direct_mcp(execution, cleanup_error, optional) {
            DirectMcpSettlement::Suspend(question, cleanup_error) => {
                event.cleanup_error = cleanup_error.as_ref();
                record_direct_mcp_tool_event(ctx, node, event)?;
                return Ok(StepOutcome::NeedsUser { question });
            }
            DirectMcpSettlement::Complete(result, cleanup_error) => (result, cleanup_error),
            DirectMcpSettlement::Failed(error, cleanup_error) => {
                event.error = Some(&error);
                event.cleanup_error = cleanup_error.as_ref();
                record_direct_mcp_tool_event(ctx, node, event)?;
                return Err(error);
            }
            DirectMcpSettlement::Degraded(error, cleanup_error) => {
                event.error = Some(&error);
                event.cleanup_error = cleanup_error.as_ref();
                event.degraded = true;
                record_direct_mcp_tool_event(ctx, node, event)?;
                return Ok(StepOutcome::success(None));
            }
        };
        let raw_result = result.clone();
        let result = match validate_direct_mcp_result(
            node,
            &params.server,
            &params.tool,
            params.output_schema.as_ref(),
            result,
        ) {
            Ok(result) => result,
            Err(error) => {
                event.result = Some(&raw_result);
                event.error = Some(&error);
                event.cleanup_error = cleanup_error.as_ref();
                record_direct_mcp_tool_event(ctx, node, event)?;
                return Err(error);
            }
        };
        let sources = tool_call_sources(&result);
        event.result = Some(&result);
        event.cleanup_error = cleanup_error.as_ref();
        record_direct_mcp_tool_event(ctx, node, event)?;
        Ok(StepOutcome::success(Some(json!({
            "server": params.server,
            "tool": params.tool,
            "result": result,
            "sources": sources,
        }))))
    }
}

/// What a direct MCP call settled on, with the cleanup failure that
/// followed it. The execution outcome decides the settlement; the cleanup
/// outcome is carried beside it and never changes it.
#[derive(Debug)]
enum DirectMcpSettlement {
    /// The tool asked the user something: the run suspends and the
    /// already-journaled continuation stays valid.
    Suspend(api::FormSpec, Option<StepError>),
    /// The tool returned a result.
    Complete(Value, Option<StepError>),
    /// The tool call failed.
    Failed(StepError, Option<StepError>),
    /// The tool call failed and the step is optional, so the step
    /// degrades instead of failing.
    Degraded(StepError, Option<StepError>),
}

/// Settle a direct MCP call on its execution outcome alone. A session
/// cleanup failure is retained beside the settlement and never replaces
/// it: a completed tool result and a pending question both survive a
/// cleanup that failed afterwards.
fn settle_direct_mcp(
    execution: Result<DirectMcpCallOutcome, StepError>,
    cleanup_error: Option<StepError>,
    optional: bool,
) -> DirectMcpSettlement {
    match execution {
        Ok(DirectMcpCallOutcome::NeedsUser(question)) => {
            DirectMcpSettlement::Suspend(question, cleanup_error)
        }
        Ok(DirectMcpCallOutcome::Complete(result)) => {
            DirectMcpSettlement::Complete(result, cleanup_error)
        }
        Err(error) if optional => DirectMcpSettlement::Degraded(error, cleanup_error),
        Err(error) => DirectMcpSettlement::Failed(error, cleanup_error),
    }
}

/// Execution-time destination gate for the direct `mcp.call` path:
/// re-extracts hosts, enforces the static `allowed_destinations` ceiling,
/// then delegates a remaining miss to the operator `destination_decider`
/// when one is configured. Static hits never consult the decider. The
/// verdict is journaled (`mcp_destination_decision`, hosts only) for audit;
/// resumes re-invoke fresh so tightening denies retroactively, and
/// cancellation propagates instead of denying.
async fn authorize_direct_destinations(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    profile: &mcp::McpProfile,
    server: &str,
    tool: &str,
    args: &Value,
) -> Result<(), engine::StepError> {
    // Static verdict first (host-free passes, misses refuse): the decider
    // below only widens a miss, so this reuse keeps one matcher.
    if check_direct_destinations(node, server, tool, profile.allowed_destinations(), args).is_ok() {
        return Ok(());
    }
    let extracted: Vec<String> = policy::extract_mcp_destinations(args).into_iter().collect();
    if extracted.is_empty() {
        return Ok(());
    }
    let Some(argv) = profile.destination_decider() else {
        let denied = extracted.join(", ");
        return Err(engine::StepError::Refused {
            node: node.id.clone(),
            message: format!(
                "MCP tool `{server}/{tool}` destination `{denied}` is not in server `{server}` allowed_destinations; ask the operator to allow it in providers.toml"
            ),
        });
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

/// Fail-closed destination-policy gate for the direct `mcp.call` path:
/// every host extracted from the rendered arguments must sit within the
/// operator's per-profile `allowed_destinations` (exact match or `"*"`).
/// Calls with no extracted host pass, so pure-query search keeps working
/// while fetch requires an explicit operator allow. Denials are `Refused`,
/// never silent — mirroring the `llm.agent` MCP tool gate.
fn check_direct_destinations(
    node: &NodeDef,
    server: &str,
    tool: &str,
    allowed: &[String],
    args: &Value,
) -> Result<(), engine::StepError> {
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
        return Err(engine::StepError::Refused {
            node: node.id.clone(),
            message: format!(
                "MCP tool `{server}/{tool}` destination `{denied}` is not in server `{server}` allowed_destinations; ask the operator to allow it in providers.toml"
            ),
        });
    }
    Ok(())
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
            params: Default::default(),
        }
    }

    fn question() -> api::FormSpec {
        api::FormSpec {
            id: "q1".into(),
            title: "Which one?".into(),
            title_i18n: Default::default(),
            fields: Vec::new(),
        }
    }

    fn cleanup_failed() -> StepError {
        StepError::failed("mcp-call", "MCP session close timed out")
    }

    #[test]
    fn a_cleanup_failure_keeps_the_completed_tool_result() {
        let settlement = settle_direct_mcp(
            Ok(DirectMcpCallOutcome::Complete(json!({"ok": true}))),
            Some(cleanup_failed()),
            false,
        );
        let DirectMcpSettlement::Complete(result, cleanup_error) = settlement else {
            panic!("a completed call stays completed: {settlement:?}");
        };
        assert_eq!(result, json!({"ok": true}));
        assert!(
            cleanup_error.is_some(),
            "the cleanup failure is retained beside the result"
        );
    }

    #[test]
    fn a_cleanup_failure_keeps_the_pending_question() {
        let settlement = settle_direct_mcp(
            Ok(DirectMcpCallOutcome::NeedsUser(question())),
            Some(cleanup_failed()),
            false,
        );
        let DirectMcpSettlement::Suspend(question, cleanup_error) = settlement else {
            panic!("a pending question stays pending: {settlement:?}");
        };
        assert_eq!(question.id, "q1");
        assert!(
            cleanup_error.is_some(),
            "the cleanup failure is retained beside the question"
        );
    }

    #[test]
    fn a_cleanup_failure_never_replaces_the_execution_error() {
        let settlement = settle_direct_mcp(
            Err(StepError::failed("mcp-call", "tool call failed")),
            Some(cleanup_failed()),
            false,
        );
        let DirectMcpSettlement::Failed(error, cleanup_error) = settlement else {
            panic!("the execution error decides the settlement: {settlement:?}");
        };
        assert!(
            error.to_string().contains("tool call failed"),
            "the execution error is the one that is reported: {error}"
        );
        assert!(
            cleanup_error.is_some(),
            "the cleanup failure is retained beside the error"
        );
    }

    #[test]
    fn an_optional_call_degrades_on_the_execution_error() {
        let settlement = settle_direct_mcp(
            Err(StepError::failed("mcp-call", "tool call failed")),
            Some(cleanup_failed()),
            true,
        );
        assert!(matches!(settlement, DirectMcpSettlement::Degraded(..)));
    }

    #[test]
    fn direct_destination_policy_denies_unlisted_fetch_but_passes_query() {
        let node = node();
        // Empty default denies every extracted host; host-only calls pass.
        let error = check_direct_destinations(
            &node,
            "exa-public",
            "web_fetch_exa",
            &[],
            &json!({"url": "https://example.test/x"}),
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
        )
        .expect("pure-query search must pass the empty default");
        // An explicit operator allow passes fetch; other hosts still fail.
        check_direct_destinations(
            &node,
            "exa-public",
            "web_fetch_exa",
            &["example.test".to_string()],
            &json!({"url": "https://example.test/x"}),
        )
        .expect("listed destination must pass");
        assert!(
            check_direct_destinations(
                &node,
                "exa-public",
                "web_fetch_exa",
                &["example.test".to_string()],
                &json!({"url": "https://other.test/x"}),
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
        )
        .expect("wildcard must allow any destination");
    }
}
