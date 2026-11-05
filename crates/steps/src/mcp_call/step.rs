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
        let cleanup_error = session
            .close()
            .await
            .err()
            .map(|error| StepError::failed(&node.id, error.to_string()));
        super::settlement::settle_direct_mcp(
            ctx,
            node,
            &params,
            execution,
            &arguments,
            cleanup_error.as_ref(),
            started.elapsed().as_millis() as u64,
        )
    }
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
            params: Default::default(),
        }
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
