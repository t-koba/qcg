use crate::common::render_json_templates;
use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{
    StepContext, StepError, StepExecutor, StepOutcome, StepTraits, tool_call_sources,
};
use qcg_policy::{params_schema, string_schema};
use serde_json::{Value, json};

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
    fn type_id(&self) -> &'static str {
        "mcp.call"
    }

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
        let access = mcp_access(ctx, profile.command());
        if params.side_effects
            && let Some(confirm) = ctx.run.require_side_effect(
                ctx.journal,
                node,
                "mcp.call",
                &format!("{}/{}", params.server, params.tool),
                Some(json!({
                    "server": params.server,
                    "tool": params.tool,
                    "argument_names": arguments
                        .as_object()
                        .map(|object| object.keys().cloned().collect::<Vec<_>>())
                        .unwrap_or_default(),
                })),
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
        let result = execute_direct_mcp_call(
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
        if let Err(error) = close_result {
            record_direct_mcp_tool_event(
                ctx,
                node,
                DirectMcpToolEvent {
                    server: &params.server,
                    tool: &params.tool,
                    arguments: &arguments,
                    result: None,
                    error: Some(&StepError::failed(&node.id, error.to_string())),
                    duration_ms: started.elapsed().as_millis() as u64,
                    degraded: false,
                },
            )?;
            return Err(StepError::failed(&node.id, error.to_string()));
        }
        let result = match result {
            Ok(DirectMcpCallOutcome::NeedsUser(question)) => {
                record_direct_mcp_tool_event(
                    ctx,
                    node,
                    DirectMcpToolEvent {
                        server: &params.server,
                        tool: &params.tool,
                        arguments: &arguments,
                        result: None,
                        error: None,
                        duration_ms: started.elapsed().as_millis() as u64,
                        degraded: false,
                    },
                )?;
                return Ok(StepOutcome::NeedsUser { question });
            }
            Ok(DirectMcpCallOutcome::Complete(result)) => result,
            Err(error) => {
                record_direct_mcp_tool_event(
                    ctx,
                    node,
                    DirectMcpToolEvent {
                        server: &params.server,
                        tool: &params.tool,
                        arguments: &arguments,
                        result: None,
                        error: Some(&error),
                        duration_ms: started.elapsed().as_millis() as u64,
                        degraded: optional,
                    },
                )?;
                if optional {
                    return Ok(StepOutcome::Success {
                        output: None,
                        files: vec![],
                    });
                }
                return Err(error);
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
                record_direct_mcp_tool_event(
                    ctx,
                    node,
                    DirectMcpToolEvent {
                        server: &params.server,
                        tool: &params.tool,
                        arguments: &arguments,
                        result: Some(&raw_result),
                        error: Some(&error),
                        duration_ms: started.elapsed().as_millis() as u64,
                        degraded: false,
                    },
                )?;
                return Err(error);
            }
        };
        let sources = tool_call_sources(&result);
        record_direct_mcp_tool_event(
            ctx,
            node,
            DirectMcpToolEvent {
                server: &params.server,
                tool: &params.tool,
                arguments: &arguments,
                result: Some(&result),
                error: None,
                duration_ms: started.elapsed().as_millis() as u64,
                degraded: false,
            },
        )?;
        Ok(StepOutcome::Success {
            output: Some(json!({
                "server": params.server,
                "tool": params.tool,
                "result": result,
                "sources": sources,
            })),
            files: vec![],
        })
    }
}
