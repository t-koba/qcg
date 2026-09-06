use qcg_api::{GuardrailStage, ToolCallErrorCode, ToolCallPhase, ToolCallStatus};
use qcg_contract::{LlmRequestPolicy, NodeDef, ToolDecl};
use qcg_engine::{ResultExt, StepContext, StepError};
use qcg_llm::{ChatContent, ChatMessage, ChatToolCall};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Instant;

use crate::agent::{agent_tool_requires_serial_execution, charge_agent_tool_call};
use crate::agent_runtime::{
    AgentToolInvocation, AgentToolServices, execute_agent_tool, validate_agent_tool_call_args,
};
use crate::agent_tools::load_agent_output_schema;
use crate::completion::complete_llm_with_policy;
use crate::context::{enforce_agent_transcript_limit, record_llm_validation_failure};
use crate::guardrail::apply_guardrails;
use crate::mcp_tools::{AgentToolOutcome, McpAgentTools};
use crate::prompting::{append_agent_validation_retry, parse_agent_final, validate_agent_stop};
use crate::request::{
    MessageRequestOptions, build_request_with_messages, scan_llm_text, tool_spec,
};
use crate::routes::invocation_routes;
use crate::tool_events::{
    record_tool_call_failure, record_tool_call_failures, tool_call_event, tool_call_failure,
    tool_call_outcome, tool_reported_error,
};

pub(crate) struct SpecialistAgentSpec<'a> {
    pub(crate) name: &'a str,
    pub(crate) invocation_id: &'a str,
    pub(crate) instructions: &'a str,
    pub(crate) delegated_names: &'a [String],
    pub(crate) max_calls: usize,
    pub(crate) max_iterations: usize,
    pub(crate) max_tokens_total: u64,
    pub(crate) max_tool_calls_total: usize,
    pub(crate) output_schema_path: Option<&'a str>,
    pub(crate) model: Option<&'a qcg_contract::ModelRef>,
    pub(crate) fallback_models: &'a [qcg_contract::ModelRef],
    pub(crate) request: &'a LlmRequestPolicy,
    pub(crate) args: &'a Value,
}

pub(crate) async fn execute_specialist_agent(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    services: AgentToolServices<'_>,
    mcp: &McpAgentTools,
    all_tools: &[ToolDecl],
    spec: SpecialistAgentSpec<'_>,
) -> Result<AgentToolOutcome, StepError> {
    let SpecialistAgentSpec {
        name: agent_name,
        invocation_id,
        instructions,
        delegated_names,
        max_calls,
        max_iterations,
        max_tokens_total,
        max_tool_calls_total,
        output_schema_path,
        model,
        fallback_models,
        request: specialist_request,
        args,
    } = spec;
    let mut invocation_policy = specialist_request.clone();
    invocation_policy.system = Some(match specialist_request.system.as_deref() {
        Some(system) => format!(
            "{system}\n\nYou are the bounded specialist `{agent_name}`. Follow only these specialist instructions:\n{instructions}"
        ),
        None => format!(
            "You are the bounded specialist `{agent_name}`. Follow only these specialist instructions:\n{instructions}"
        ),
    });
    let delegated_tools = delegated_names
        .iter()
        .map(|name| {
            all_tools
                .iter()
                .find(|tool| tool.name() == name)
                .ok_or_else(|| {
                    StepError::failed(
                        &node.id,
                        format!("specialist `{agent_name}` cannot resolve tool `{name}`"),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let output_schema = output_schema_path
        .map(|path| load_agent_output_schema(&ctx.run.contract, &node.id, agent_name, path))
        .transpose()?;
    let tool_specs = delegated_tools
        .iter()
        .map(|tool| tool_spec(tool, mcp))
        .collect::<Result<Vec<_>, _>>()?;
    let task = serde_json::to_string(args)?;
    let mut messages = vec![ChatMessage::text(
        "user",
        format!(
            "Complete the delegated task under your specialist instructions. Return the final result as JSON when possible.\n\nDelegated arguments:\n{task}"
        ),
    )];
    let mut tokens_total = 0_u64;
    let mut tool_calls_total = 0_usize;
    let mut tool_call_counts = BTreeMap::<String, usize>::new();
    let mut last_validation_error = None;
    ctx.journal
        .event(
            "agent_delegated",
            json!({
                "node": node.id,
                "agent": agent_name,
                "tool_call_id": invocation_id,
                "tools": delegated_names,
                "max_calls": max_calls,
                "max_iterations": max_iterations,
                "max_tokens_total": max_tokens_total,
                "max_tool_calls_total": max_tool_calls_total,
            }),
        )
        .step_err(&node.id)?;
    for turn in 0..max_iterations {
        enforce_agent_transcript_limit(ctx, node, &mut messages, Some(specialist_request))?;
        let request = build_request_with_messages(
            ctx,
            node,
            services.runtime,
            messages.clone(),
            MessageRequestOptions {
                response_schema: output_schema.clone(),
                tools: &tool_specs,
                model,
                policy: Some(&invocation_policy),
            },
        )?;
        let routes = invocation_routes(ctx, node, &request, model, Some(fallback_models))?;
        let response = complete_llm_with_policy(
            ctx,
            node,
            request,
            Some(&invocation_policy),
            Some(&routes),
            |usage| {
                json!({
                    "agent": agent_name,
                    "turn": turn,
                    "tokens_total": tokens_total
                        .saturating_add(usage.input)
                        .saturating_add(usage.output),
                    "max_tokens_total": max_tokens_total,
                })
            },
        )
        .await?;
        tokens_total = tokens_total
            .saturating_add(response.usage.input)
            .saturating_add(response.usage.output);
        let stop = response.stop;
        let provider_state = response.provider_state;
        let mut text_parts = Vec::new();
        let mut tool_calls = Vec::new();
        for content in response.content {
            match content {
                ChatContent::Text(text) => text_parts.push(text),
                ChatContent::ToolCall { id, name, args } => {
                    tool_calls.push(ChatToolCall { id, name, args });
                }
            }
        }
        if tokens_total > max_tokens_total {
            let error = StepError::failed(
                &node.id,
                format!(
                    "specialist `{agent_name}` token budget exceeded: {tokens_total} > {max_tokens_total}"
                ),
            );
            record_tool_call_failures(
                ctx,
                node,
                &tool_calls,
                &error,
                Some(agent_name),
                ToolCallPhase::InputValidation,
                ToolCallErrorCode::BudgetExceeded,
            )?;
            return Err(error);
        }
        if let Err(error) = validate_agent_stop(&node.id, stop, !tool_calls.is_empty()) {
            record_tool_call_failures(
                ctx,
                node,
                &tool_calls,
                &error,
                Some(agent_name),
                ToolCallPhase::InputValidation,
                ToolCallErrorCode::InvalidArguments,
            )?;
            return Err(error);
        }
        if tool_calls.is_empty() {
            let text = text_parts.join("\n");
            scan_llm_text(ctx, node, &text)?;
            let value = match parse_agent_final(&node.id, &text, output_schema.as_ref()) {
                Ok(value) => value,
                Err(error) => {
                    record_llm_validation_failure(ctx, node, turn, &error)?;
                    append_agent_validation_retry(
                        &node.id,
                        &mut messages,
                        provider_state,
                        &text,
                        &error,
                    )?;
                    enforce_agent_transcript_limit(
                        ctx,
                        node,
                        &mut messages,
                        Some(specialist_request),
                    )?;
                    last_validation_error = Some(error);
                    continue;
                }
            };
            ctx.journal
                .event(
                    "agent_completed",
                    json!({
                        "node": node.id,
                        "agent": agent_name,
                        "tool_call_id": invocation_id,
                        "turn": turn,
                        "tokens_total": tokens_total,
                    }),
                )
                .step_err(&node.id)?;
            return Ok(AgentToolOutcome::Result(value));
        }
        last_validation_error = None;
        if tool_calls.len() > 1
            && tool_calls
                .iter()
                .any(|call| agent_tool_requires_serial_execution(all_tools, &call.name))
        {
            let error = StepError::failed(
                &node.id,
                format!(
                    "specialist `{agent_name}` returned parallel interactive or side-effectful tool calls"
                ),
            );
            record_tool_call_failures(
                ctx,
                node,
                &tool_calls,
                &error,
                Some(agent_name),
                ToolCallPhase::InputValidation,
                ToolCallErrorCode::InvalidArguments,
            )?;
            return Err(error);
        }
        if let Some(state) = provider_state {
            let items = match state.as_array().cloned() {
                Some(items) => items,
                None => {
                    let error = StepError::failed(
                        &node.id,
                        "Responses API provider state must be an array",
                    );
                    record_tool_call_failures(
                        ctx,
                        node,
                        &tool_calls,
                        &error,
                        Some(agent_name),
                        ToolCallPhase::InputValidation,
                        ToolCallErrorCode::InvalidArguments,
                    )?;
                    return Err(error);
                }
            };
            messages.push(ChatMessage::provider_state(items));
        } else {
            messages.push(ChatMessage::assistant_tool_calls(
                text_parts.join("\n"),
                tool_calls.clone(),
            ));
        }
        for call in tool_calls {
            let tool_started = Instant::now();
            if !delegated_names.iter().any(|name| name == &call.name) {
                let error = StepError::failed(
                    &node.id,
                    format!(
                        "specialist `{agent_name}` called undelegated tool `{}`",
                        call.name
                    ),
                );
                record_tool_call_failure(
                    ctx,
                    node,
                    &call,
                    &error,
                    tool_call_failure(
                        Some(agent_name),
                        ToolCallPhase::InputValidation,
                        ToolCallErrorCode::InvalidArguments,
                        tool_started,
                    ),
                )?;
                return Err(error);
            }
            if let Err(error) = validate_agent_tool_call_args(node, mcp, all_tools, &call) {
                record_tool_call_failure(
                    ctx,
                    node,
                    &call,
                    &error,
                    tool_call_failure(
                        Some(agent_name),
                        ToolCallPhase::InputValidation,
                        ToolCallErrorCode::InvalidArguments,
                        tool_started,
                    ),
                )?;
                return Err(error);
            }
            if let Err(error) = charge_agent_tool_call(
                &node.id,
                &format!("specialist `{agent_name}`"),
                all_tools,
                &call.name,
                &mut tool_calls_total,
                max_tool_calls_total,
                &mut tool_call_counts,
            ) {
                record_tool_call_failure(
                    ctx,
                    node,
                    &call,
                    &error,
                    tool_call_failure(
                        Some(agent_name),
                        ToolCallPhase::InputValidation,
                        ToolCallErrorCode::BudgetExceeded,
                        tool_started,
                    ),
                )?;
                return Err(error);
            }
            if let Err(error) = apply_guardrails(
                ctx,
                node,
                services.guardrails,
                GuardrailStage::ToolInput,
                Some(&call.name),
                &call.args,
            )
            .await
            {
                record_tool_call_failure(
                    ctx,
                    node,
                    &call,
                    &error,
                    tool_call_failure(
                        Some(agent_name),
                        ToolCallPhase::InputGuardrail,
                        ToolCallErrorCode::GuardrailRejected,
                        tool_started,
                    ),
                )?;
                return Err(error);
            }
            if let Err(error) = scan_llm_text(ctx, node, &serde_json::to_string(&call.args)?) {
                record_tool_call_failure(
                    ctx,
                    node,
                    &call,
                    &error,
                    tool_call_failure(
                        Some(agent_name),
                        ToolCallPhase::InputGuardrail,
                        ToolCallErrorCode::GuardrailRejected,
                        tool_started,
                    ),
                )?;
                return Err(error);
            }
            let outcome = match Box::pin(execute_agent_tool(
                ctx,
                node,
                services,
                AgentToolInvocation {
                    mcp,
                    tools: all_tools,
                    name: &call.name,
                    call_id: &call.id,
                    call_number: tool_call_counts[&call.name],
                    args: &call.args,
                },
            ))
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            Some(agent_name),
                            ToolCallPhase::Execution,
                            ToolCallErrorCode::ExecutionFailed,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
            };
            let value = match outcome {
                AgentToolOutcome::Result(value) | AgentToolOutcome::Error(value) => value,
                AgentToolOutcome::Handoff(_) => {
                    let error = StepError::failed(
                        &node.id,
                        format!(
                            "specialist `{agent_name}` received a nested handoff from `{}`",
                            call.name
                        ),
                    );
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            Some(agent_name),
                            ToolCallPhase::Execution,
                            ToolCallErrorCode::ExecutionFailed,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                AgentToolOutcome::NeedsUser(question) => {
                    let event = tool_call_event(
                        &node.id,
                        Some(agent_name),
                        mcp.server_for(&call.name),
                        &call,
                        &serde_json::to_value(&question)?,
                        tool_call_outcome(
                            ToolCallStatus::NeedsUser,
                            ToolCallPhase::Execution,
                            None,
                            tool_started,
                        ),
                    )?;
                    ctx.journal.event("tool_call", event).step_err(&node.id)?;
                    return Ok(AgentToolOutcome::NeedsUser(question));
                }
                AgentToolOutcome::NeedsConfirm(confirm) => {
                    let event = tool_call_event(
                        &node.id,
                        Some(agent_name),
                        mcp.server_for(&call.name),
                        &call,
                        &serde_json::to_value(&confirm)?,
                        tool_call_outcome(
                            ToolCallStatus::NeedsConfirmation,
                            ToolCallPhase::Execution,
                            None,
                            tool_started,
                        ),
                    )?;
                    ctx.journal.event("tool_call", event).step_err(&node.id)?;
                    return Ok(AgentToolOutcome::NeedsConfirm(confirm));
                }
            };
            if let Err(error) = apply_guardrails(
                ctx,
                node,
                services.guardrails,
                GuardrailStage::ToolOutput,
                Some(&call.name),
                &value,
            )
            .await
            {
                record_tool_call_failure(
                    ctx,
                    node,
                    &call,
                    &error,
                    tool_call_failure(
                        Some(agent_name),
                        ToolCallPhase::OutputGuardrail,
                        ToolCallErrorCode::OutputRejected,
                        tool_started,
                    ),
                )?;
                return Err(error);
            }
            let failed = value.get("isError").and_then(Value::as_bool) == Some(true);
            let event = tool_call_event(
                &node.id,
                Some(agent_name),
                mcp.server_for(&call.name),
                &call,
                &value,
                tool_call_outcome(
                    if failed {
                        ToolCallStatus::Failed
                    } else {
                        ToolCallStatus::Succeeded
                    },
                    ToolCallPhase::Completed,
                    failed.then(|| tool_reported_error(&value)),
                    tool_started,
                ),
            )?;
            let encoded = serde_json::to_string(&value)?;
            if let Err(error) = scan_llm_text(ctx, node, &encoded) {
                record_tool_call_failure(
                    ctx,
                    node,
                    &call,
                    &error,
                    tool_call_failure(
                        Some(agent_name),
                        ToolCallPhase::OutputGuardrail,
                        ToolCallErrorCode::OutputRejected,
                        tool_started,
                    ),
                )?;
                return Err(error);
            }
            ctx.journal.event("tool_call", event).step_err(&node.id)?;
            messages.push(ChatMessage::tool_result(call.id, encoded));
        }
        enforce_agent_transcript_limit(ctx, node, &mut messages, Some(specialist_request))?;
    }
    let message = last_validation_error.map_or_else(
        || {
            format!(
                "specialist `{agent_name}` iteration budget exceeded: {max_iterations} turns"
            )
        },
        |error| {
            format!(
                "specialist `{agent_name}` failed final response validation after {max_iterations} iterations: {error}"
            )
        },
    );
    Err(StepError::failed(&node.id, message))
}
