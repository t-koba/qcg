use super::direct::{DirectMcpToolEvent, record_direct_mcp_tool_event, validate_direct_mcp_result};
use super::types::McpCallParams;
use super::validate::DirectMcpCallOutcome;
use api::McpCleanupPolicy;
use contract::NodeDef;
use engine::{StepContext, StepError, StepOutcome, tool_call_sources};
use serde_json::{Value, json};
/// Keep execution facts intact, record cleanup/provenance, then apply cleanup policy.
pub(super) fn settle_direct_mcp(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    params: &McpCallParams,
    execution: Result<DirectMcpCallOutcome, StepError>,
    arguments: &Value,
    cleanup_error: Option<&StepError>,
    duration_ms: u64,
) -> Result<StepOutcome, StepError> {
    let mut event = DirectMcpToolEvent {
        server: &params.server,
        tool: &params.tool,
        arguments,
        result: None,
        error: None,
        cleanup_error,
        cleanup_settlement: None,
        duration_ms,
        degraded: false,
    };
    let execution = match execution {
        Err(error) => {
            event.error = Some(&error);
            event.degraded = params.optional;
            record_direct_mcp_tool_event(ctx, node, event)?;
            return if params.optional {
                Ok(StepOutcome::success(None))
            } else {
                Err(error)
            };
        }
        Ok(execution) => execution,
    };
    let settlement = params.cleanup_settlement();
    let cleanup_policy = settlement.policy;
    event.cleanup_settlement = Some(settlement);
    // Store the raw result even if output validation or cleanup settlement fails.
    let validated = match &execution {
        DirectMcpCallOutcome::Complete(result) => {
            event.result = Some(result);
            Some(validate_direct_mcp_result(
                node,
                &params.server,
                &params.tool,
                params.output_schema.as_ref(),
                result.clone(),
            ))
        }
        DirectMcpCallOutcome::NeedsUser(_) => None,
    };
    event.error = validated.as_ref().and_then(|result| result.as_ref().err());
    let cleanup_failed = event.cleanup_error.is_some();
    record_direct_mcp_tool_event(ctx, node, event)?;
    // Execution errors keep their independent handling. Cleanup never redispatches.
    let result = validated.transpose()?;
    if cleanup_failed && cleanup_policy == McpCleanupPolicy::FailFast {
        return Err(StepError::Refused {
            node: node.id.clone(),
            message: "MCP cleanup failed; cleanup_failure = fail_fast".into(),
        });
    }
    match execution {
        DirectMcpCallOutcome::NeedsUser(question) => Ok(StepOutcome::NeedsUser {
            question,
            files: Vec::new(),
        }),
        DirectMcpCallOutcome::Complete(_) => {
            let result = result.expect("completed execution was validated");
            let sources = tool_call_sources(&result);
            Ok(StepOutcome::success(Some(
                json!({"server": params.server, "tool": params.tool, "result": result, "sources": sources}),
            )))
        }
    }
}
