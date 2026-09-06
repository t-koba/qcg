use qcg_api::{ToolCallError, ToolCallErrorCode, ToolCallEventData, ToolCallPhase, ToolCallStatus};
use qcg_contract::NodeDef;
use qcg_engine::{ResultExt, StepContext, StepError, tool_call_sources};
use qcg_llm::ChatToolCall;
use qcg_mcp::McpCallOutcome;
use serde_json::{Value, json};
use std::time::Instant;

use crate::mcp_forms::{mcp_form_spec, mcp_input_responses, mcp_question_id};
use crate::mcp_tools::{AgentToolOutcome, McpAgentTools};
use crate::prompting::utf8_head;
use qcg_policy::TOOL_EVENT_VALUE_LIMIT_BYTES;

pub(crate) struct ToolCallEventOutcome {
    pub(crate) status: ToolCallStatus,
    pub(crate) phase: ToolCallPhase,
    pub(crate) error: Option<ToolCallError>,
    pub(crate) duration: std::time::Duration,
}

pub(crate) fn tool_call_outcome(
    status: ToolCallStatus,
    phase: ToolCallPhase,
    error: Option<ToolCallError>,
    started: Instant,
) -> ToolCallEventOutcome {
    ToolCallEventOutcome {
        status,
        phase,
        error,
        duration: started.elapsed(),
    }
}

pub(crate) fn tool_call_event(
    node: &str,
    agent: Option<&str>,
    server: Option<&str>,
    call: &ChatToolCall,
    result: &Value,
    outcome: ToolCallEventOutcome,
) -> Result<Value, serde_json::Error> {
    let sources = tool_call_sources(result);
    let (arguments, arguments_truncated) = bounded_event_value(&call.args)?;
    let (result, result_truncated) = bounded_event_value(result)?;
    let data = ToolCallEventData {
        server: server.map(str::to_owned),
        tool: call.name.clone(),
        id: call.id.clone(),
        status: outcome.status,
        phase: outcome.phase,
        agent: agent.map(str::to_owned),
        error: outcome.error,
        duration_ms: u64::try_from(outcome.duration.as_millis()).unwrap_or(u64::MAX),
        arguments,
        result,
        sources: serde_json::from_value(Value::Array(sources))?,
        truncated: arguments_truncated || result_truncated,
    };
    let mut event = serde_json::to_value(data)?;
    event["node"] = Value::String(node.to_owned());
    Ok(event)
}

pub(crate) fn tool_call_error(code: ToolCallErrorCode, error: &StepError) -> ToolCallError {
    let code = match error {
        StepError::Cancelled => ToolCallErrorCode::Cancelled,
        StepError::BudgetExceeded { .. } => ToolCallErrorCode::BudgetExceeded,
        _ => code,
    };
    ToolCallError {
        code,
        message: utf8_head(&error.to_string(), 2_048).to_string(),
    }
}

pub(crate) fn record_tool_call_failure(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    call: &ChatToolCall,
    error: &StepError,
    failure: ToolCallFailure<'_>,
) -> Result<(), StepError> {
    let event = tool_call_event(
        &node.id,
        failure.agent,
        None,
        call,
        &Value::Null,
        ToolCallEventOutcome {
            status: ToolCallStatus::Failed,
            phase: failure.phase,
            error: Some(tool_call_error(failure.code, error)),
            duration: failure.started.elapsed(),
        },
    )?;
    ctx.journal.event("tool_call", event).step_err(&node.id)
}

pub(crate) fn record_tool_call_failures(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    calls: &[ChatToolCall],
    error: &StepError,
    agent: Option<&str>,
    phase: ToolCallPhase,
    code: ToolCallErrorCode,
) -> Result<(), StepError> {
    for call in calls {
        record_tool_call_failure(
            ctx,
            node,
            call,
            error,
            tool_call_failure(agent, phase, code, Instant::now()),
        )?;
    }
    Ok(())
}

pub(crate) struct ToolCallFailure<'a> {
    agent: Option<&'a str>,
    phase: ToolCallPhase,
    code: ToolCallErrorCode,
    started: Instant,
}

pub(crate) fn tool_call_failure(
    agent: Option<&str>,
    phase: ToolCallPhase,
    code: ToolCallErrorCode,
    started: Instant,
) -> ToolCallFailure<'_> {
    ToolCallFailure {
        agent,
        phase,
        code,
        started,
    }
}

pub(crate) fn tool_reported_error(result: &Value) -> ToolCallError {
    let message = result
        .pointer("/error/message")
        .and_then(Value::as_str)
        .or_else(|| result.pointer("/content/0/text").and_then(Value::as_str))
        .unwrap_or("tool returned an error result");
    ToolCallError {
        code: ToolCallErrorCode::ToolReportedError,
        message: utf8_head(message, 2_048).to_string(),
    }
}

pub(crate) fn bounded_event_value(value: &Value) -> Result<(Value, bool), serde_json::Error> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() <= TOOL_EVENT_VALUE_LIMIT_BYTES {
        Ok((value.clone(), false))
    } else {
        Ok((
            json!({
                "type": match value {
                    Value::Array(_) => "array",
                    Value::Object(_) => "object",
                    Value::String(_) => "string",
                    Value::Bool(_) => "boolean",
                    Value::Number(_) => "number",
                    Value::Null => "null",
                },
                "bytes": bytes.len(),
                "truncated": true,
            }),
            true,
        ))
    }
}

pub(crate) async fn execute_mcp_tool(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    mcp: &McpAgentTools,
    alias: &str,
    args: Value,
) -> Result<AgentToolOutcome, StepError> {
    let mut input_responses = None;
    let mut request_state = None;
    for _round in 0..10 {
        match mcp
            .call(
                node,
                alias,
                args.clone(),
                input_responses.take(),
                request_state.take(),
            )
            .await?
        {
            McpCallOutcome::Complete(value) => return Ok(AgentToolOutcome::Result(value)),
            McpCallOutcome::InputRequired(required) => {
                request_state = required.request_state.clone();
                if required.input_requests.is_empty() {
                    tokio::task::yield_now().await;
                    continue;
                }
                let question_id = mcp_question_id(&node.id, alias, &required);
                let Some(answer) = ctx.run.answers.get(&question_id) else {
                    return Ok(AgentToolOutcome::NeedsUser(mcp_form_spec(
                        question_id,
                        alias,
                        &required,
                    )?));
                };
                input_responses = Some(mcp_input_responses(&required, answer)?);
            }
        }
    }
    Err(StepError::failed(
        &node.id,
        format!("MCP tool `{alias}` exceeded 10 input-required rounds"),
    ))
}
