use async_trait::async_trait;
use qcg_api::{GuardrailStage, ToolCallErrorCode, ToolCallPhase, ToolCallStatus};
use qcg_contract::AgentFailureCode;
use qcg_contract::{Contract, NodeDef, ToolDecl};
use qcg_engine::{ResultExt, StepContext, StepError, StepExecutor, StepOutcome};
use qcg_llm::{ChatContent, ChatMessage, ChatToolCall, LlmRuntime};
use qcg_policy::{params_schema, string_array_schema, string_schema};
use qcg_types::NodePath;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use crate::agent_runtime::{
    AgentToolCallFailure, AgentToolInvocation, AgentToolServices, execute_agent_tool,
    finish_rejected_external_operation, recover_agent_tool_call_failure,
    validate_agent_tool_call_args,
};
use crate::agent_tools::{validate_agent_delegations, validate_agent_tool};
use crate::completion::complete_llm;
use crate::context::{enforce_agent_transcript_limit, record_llm_validation_failure};
use crate::guardrail::{apply_guardrails, validate_guardrails};
use crate::mcp_tools::{AgentToolOutcome, McpAgentTools};
use crate::out_of_contract::{OutOfContractDecision, enforce_out_of_contract_policy};
use crate::policy::{llm_params, require_prompt};
use crate::prompting::{
    append_agent_validation_retry, load_schema, parse_agent_final, render_prompt,
    validate_agent_stop,
};
use crate::request::{
    MessageRequestOptions, build_request_with_messages, build_user_message, scan_llm_text,
    tool_spec,
};
use crate::schemas::{
    agent_failure_policy_schema, agent_tool_params_schema, llm_common_properties, model_ref_schema,
    request_policy_schema,
};
use crate::tool_events::{
    record_tool_call_failure, record_tool_call_failures, tool_call_event, tool_call_failure,
    tool_call_outcome, tool_reported_error,
};
use crate::validation::validate_llm_node;

pub(crate) struct LlmAgentStep {
    pub(crate) runtime: Arc<LlmRuntime>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentCheckpoint {
    pub(crate) messages: Vec<ChatMessage>,
    pub(crate) next_turn: usize,
    pub(crate) tokens_total: u64,
    pub(crate) tool_calls_total: usize,
    pub(crate) tool_call_counts: BTreeMap<String, usize>,
    #[serde(default)]
    pub(crate) pending_side_effect: Option<ChatToolCall>,
    /// Confirmation awaiting user decision for the pending call. Stored so
    /// resume re-emits the same operation instead of regenerating a
    /// different target with the same approval (A06).
    #[serde(default)]
    pub(crate) pending_confirm: Option<qcg_api::ConfirmSpec>,
    /// Suspended MCP input-required call. Stored so resume re-issues the
    /// exact call (same call id) instead of regenerating one that would
    /// miss the journaled continuation and duplicate the remote call (A07).
    #[serde(default)]
    pub(crate) pending_mcp_call: Option<McpSuspendedCall>,
}

/// An MCP tool call suspended for user input, identified exactly as the
/// journaled continuation key identifies it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct McpSuspendedCall {
    pub(crate) name: String,
    pub(crate) id: String,
    pub(crate) args: Value,
    pub(crate) question_id: String,
}

/// Selects the suspended MCP call to re-issue without an LLM round-trip.
/// Only an answered suspension qualifies: anything else (no checkpoint, no
/// suspension, no answer yet) stays on the model flow so regeneration can
/// never steal another call's continuation.
fn resumed_mcp_call(
    checkpoint: Option<&AgentCheckpoint>,
    answers: &std::collections::BTreeMap<String, Value>,
) -> Option<McpSuspendedCall> {
    let suspended = checkpoint?.pending_mcp_call.clone()?;
    answers
        .contains_key(&suspended.question_id)
        .then_some(suspended)
}

#[async_trait]
impl StepExecutor for LlmAgentStep {
    fn type_id(&self) -> &'static str {
        "llm.agent"
    }

    fn traits(&self) -> qcg_engine::StepTraits {
        qcg_engine::StepTraits::parallel()
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["prompt", "max_iterations", "max_tokens_total"],
            llm_common_properties(json!({
                "max_iterations": { "type": "integer", "minimum": 1 },
                "max_tokens_total": { "type": "integer", "minimum": 1 },
                "max_tool_calls_total": { "type": "integer", "minimum": 1 },
                "guardrails": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["name", "stage", "kind"],
                        "properties": {
                            "name": string_schema(),
                            "stage": { "enum": ["input", "output", "tool_input", "tool_output"] },
                            "kind": { "enum": ["regex_deny", "json_schema", "command"] },
                            "params": {},
                            "tool": string_schema(),
                            "tripwire": { "type": "boolean" },
                            "on_error": { "enum": ["fail", "block"] }
                        }
                    }
                },
                "tools": {
                    "type": "array",
                    "items": {
                        "oneOf": [
                            agent_tool_params_schema("fs.write", &["path_prefix"], json!({
                                "path_prefix": string_schema(),
                                "input_schema": { "type": "object" }
                            })),
                            agent_tool_params_schema("command", &["command"], json!({
                                "command": string_array_schema(),
                                "input_schema": { "type": "object" }
                            })),
                            agent_tool_params_schema("http", &["methods", "hosts"], json!({
                                "methods": string_array_schema(),
                                "hosts": string_array_schema(),
                                "input_schema": { "type": "object" }
                            })),
                            agent_tool_params_schema("ask_user", &[], json!({
                                "input_schema": { "type": "object" }
                            })),
                            agent_tool_params_schema(
                                "web.search",
                                &[],
                                json!({
                                    "provider": string_schema(),
                                    "max_results": { "type": "integer", "minimum": 1, "maximum": 20 },
                                    "max_calls": { "type": "integer", "minimum": 1, "maximum": 10 }
                                })
                            ),
                            agent_tool_params_schema(
                                "mcp",
                                &["server", "tool"],
                                json!({
                                    "server": string_schema(),
                                    "tool": string_schema(),
                                    "max_calls": { "type": "integer", "minimum": 1, "maximum": 10 },
                                    "side_effects": { "type": "boolean" }
                                })
                            ),
                            agent_tool_params_schema(
                                "agent",
                                &["instructions", "max_tool_calls_total"],
                                json!({
                                    "instructions": string_schema(),
                                    "tools": string_array_schema(),
                                    "input_schema": { "type": "object" },
                                    "output_schema": string_schema(),
                                    "max_calls": { "type": "integer", "minimum": 1, "maximum": 10 },
                                    "max_iterations": { "type": "integer", "minimum": 1, "maximum": 32 },
                                    "max_tokens_total": { "type": "integer", "minimum": 1 },
                                    "max_tool_calls_total": { "type": "integer", "minimum": 1 },
                                    "model": model_ref_schema(),
                                    "fallback_models": { "type": "array", "items": model_ref_schema(), "maxItems": 8 },
                                    "request": request_policy_schema(),
                                    "on_failure": agent_failure_policy_schema(),
                                    "handoff": { "type": "boolean" }
                                })
                            )
                        ]
                    }
                }
            })),
        ))
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = llm_params(node)?;
        validate_llm_node(
            node,
            contract,
            &self.runtime,
            params.schema.is_some(),
            !params.tools.is_empty(),
        )?;
        require_prompt(node, &params)?;
        if params.max_iterations.unwrap_or_default() == 0 {
            return Err(StepError::failed(&node.id, "max_iterations is required"));
        }
        if params.max_tokens_total.unwrap_or_default() == 0 {
            return Err(StepError::failed(&node.id, "max_tokens_total is required"));
        }
        if params.max_tool_calls_total == Some(0) {
            return Err(StepError::failed(
                &node.id,
                "max_tool_calls_total must be greater than zero",
            ));
        }
        let mut tool_names = BTreeSet::new();
        for tool in &params.tools {
            if tool.name().trim().is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    "agent tool name must not be empty",
                ));
            }
            if !tool_names.insert(tool.name()) {
                return Err(StepError::failed(
                    &node.id,
                    format!("agent tool name `{}` is duplicated", tool.name()),
                ));
            }
            validate_agent_tool(node, contract, &self.runtime, tool)?;
        }
        validate_agent_delegations(node, &params.tools)?;
        validate_guardrails(
            node,
            &params.guardrails,
            &params.tools,
            &contract.manifest.runtime,
        )?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let prompt = render_prompt(ctx, node)?;
        let params = llm_params(node)?;
        let response_schema = load_schema(ctx, node)?;
        apply_guardrails(
            ctx,
            node,
            &params.guardrails,
            GuardrailStage::Input,
            None,
            &json!({ "prompt": &prompt }),
        )
        .await?;
        let max_turns = params
            .max_iterations
            .ok_or_else(|| StepError::failed(&node.id, "agent max_iterations is required"))?;
        let max_tokens_total = params
            .max_tokens_total
            .ok_or_else(|| StepError::failed(&node.id, "agent max_tokens_total is required"))?;
        let max_tool_calls_total = params.max_tool_calls_total.unwrap_or(32);
        let checkpoint = ctx
            .journal
            .state()
            .checkpoints
            .get(&NodePath::root(&node.id))
            .cloned()
            .map(serde_json::from_value::<AgentCheckpoint>)
            .transpose()
            .map_err(|error| {
                StepError::failed(&node.id, format!("invalid agent checkpoint: {error}"))
            })?;
        if let Some(stored) = checkpoint.as_ref() {
            if let (Some(_pending), Some(confirm)) = (
                stored.pending_side_effect.as_ref(),
                stored.pending_confirm.as_ref(),
            ) {
                // Waiting-for-confirmation resume: re-emit the exact stored
                // operation when still unapproved, so the model never
                // substitutes a different target under the same approval.
                // Approved resumes fall through to execute the stored call
                // below instead of regenerating via the LLM (A06).
                let approved = ctx
                    .run
                    .confirmations
                    .get(&confirm.id)
                    .copied()
                    .unwrap_or(false);
                if !approved {
                    return Ok(StepOutcome::NeedsConfirm {
                        confirm: confirm.clone(),
                    });
                }
                // Approved: continue to execute the stored pending call.
                // The main loop below detects this via the checkpoint and
                // executes it directly without an LLM round-trip. Mark by
                // falling through; the turn loop handles it first.
            } else if let Some(pending) = stored.pending_side_effect.as_ref() {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "agent side effect `{}` has an indeterminate result after interruption; refusing automatic replay",
                        pending.name
                    ),
                ));
            }
        }
        let first_turn = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.next_turn)
            .unwrap_or_default();
        let mut messages = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.messages.clone())
            .map(Ok)
            .unwrap_or_else(|| {
                build_user_message(ctx, node, prompt).map(|message| vec![message])
            })?;
        let mcp_tools = McpAgentTools::prepare(ctx, node, &self.runtime, &params.tools).await?;
        let tool_specs = params
            .tools
            .iter()
            .map(|tool| tool_spec(tool, &mcp_tools))
            .collect::<Result<Vec<_>, _>>()?;
        let mut tokens_total = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.tokens_total)
            .unwrap_or_default();
        let mut tool_calls_total = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.tool_calls_total)
            .unwrap_or_default();
        let mut tool_call_counts = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.tool_call_counts.clone())
            .unwrap_or_default();
        // Approved pending call resumes without an LLM round-trip so the
        // approved operation executes exactly once (A06).
        let resumed_pending: Option<ChatToolCall> = checkpoint.as_ref().and_then(|stored| {
            let pending = stored.pending_side_effect.clone()?;
            let confirm = stored.pending_confirm.clone()?;
            let approved = ctx
                .run
                .confirmations
                .get(&confirm.id)
                .copied()
                .unwrap_or(false);
            approved.then_some(pending)
        });
        // Answered MCP call resumes without an LLM round-trip so the exact
        // suspended call (same call id) continues its journaled
        // continuation instead of regenerating a fresh remote call (A07).
        // Without an answer the model flow stays in charge.
        let resumed_mcp = resumed_mcp_call(checkpoint.as_ref(), &ctx.run.answers);
        let mut last_validation_error = None;
        for turn in first_turn..max_turns {
            enforce_agent_transcript_limit(ctx, node, &mut messages, None)?;
            let turn_start_messages = messages.clone();
            let turn_start_tool_calls_total = tool_calls_total;
            let turn_start_tool_call_counts = tool_call_counts.clone();
            // When resuming an approved pending call, execute it directly.
            let resumed_this_turn = turn == first_turn && resumed_pending.is_some();
            // When resuming an answered MCP call, re-issue it directly.
            let resumed_mcp_this_turn =
                turn == first_turn && !resumed_this_turn && resumed_mcp.is_some();
            if resumed_this_turn {
                // Clear the pending marker before execution so a crash
                // during execution becomes an indeterminate-result error
                // instead of silent replay.
                record_agent_checkpoint(
                    ctx,
                    node,
                    turn,
                    "resuming_approved_side_effect",
                    &AgentCheckpoint {
                        messages: turn_start_messages.clone(),
                        next_turn: turn,
                        tokens_total,
                        tool_calls_total: turn_start_tool_calls_total,
                        tool_call_counts: turn_start_tool_call_counts.clone(),
                        pending_side_effect: resumed_pending.clone(),
                        pending_confirm: None,
                        pending_mcp_call: None,
                    },
                )?;
            }
            if resumed_mcp_this_turn {
                // Keep the suspension marker across execution: a crash from
                // here on is an indeterminate remote result, detected via
                // the journaled resumption record on the next resume.
                record_agent_checkpoint(
                    ctx,
                    node,
                    turn,
                    "resuming_answered_mcp_call",
                    &AgentCheckpoint {
                        messages: turn_start_messages.clone(),
                        next_turn: turn,
                        tokens_total,
                        tool_calls_total: turn_start_tool_calls_total,
                        tool_call_counts: turn_start_tool_call_counts.clone(),
                        pending_side_effect: None,
                        pending_confirm: None,
                        pending_mcp_call: resumed_mcp.clone(),
                    },
                )?;
            }
            let (text_parts, tool_calls, stop, next_provider_state) = if resumed_this_turn {
                // Execute the exact approved operation without asking the
                // model to regenerate it (A06).
                (
                    Vec::new(),
                    vec![resumed_pending.clone().ok_or_else(|| {
                        StepError::failed(&node.id, "resumed turn has no pending call")
                    })?],
                    qcg_llm::StopReason::ToolUse,
                    None,
                )
            } else if resumed_mcp_this_turn {
                // Re-issue the exact suspended MCP call without asking the
                // model to regenerate it (A07).
                let suspended = resumed_mcp.clone().ok_or_else(|| {
                    StepError::failed(&node.id, "resumed turn has no suspended MCP call")
                })?;
                (
                    Vec::new(),
                    vec![ChatToolCall {
                        id: suspended.id,
                        name: suspended.name,
                        args: suspended.args,
                    }],
                    qcg_llm::StopReason::ToolUse,
                    None,
                )
            } else {
                let request = build_request_with_messages(
                    ctx,
                    node,
                    &self.runtime,
                    messages.clone(),
                    MessageRequestOptions {
                        response_schema: response_schema.clone(),
                        tools: &tool_specs,
                        model: None,
                        policy: None,
                    },
                )?;
                let response = complete_llm(ctx, node, request, |usage| {
                    let next_total = tokens_total
                        .saturating_add(usage.input)
                        .saturating_add(usage.output);
                    json!({ "turn": turn, "tokens_total": next_total, "max_tokens_total": max_tokens_total })
                })
                .await?;
                let usage = response.usage.clone();
                tokens_total = tokens_total
                    .saturating_add(usage.input)
                    .saturating_add(usage.output);

                let stop = response.stop;
                let next_provider_state = response.provider_state;
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
                (text_parts, tool_calls, stop, next_provider_state)
            };
            // Non-resumed turns validated stop below; resumed turns skip it
            // because they execute a previously validated approved call.
            if tokens_total > max_tokens_total {
                let error = StepError::failed(
                    &node.id,
                    format!("llm.agent token budget exceeded: {tokens_total} > {max_tokens_total}"),
                );
                record_tool_call_failures(
                    ctx,
                    node,
                    &tool_calls,
                    &error,
                    None,
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
                    None,
                    ToolCallPhase::InputValidation,
                    ToolCallErrorCode::InvalidArguments,
                )?;
                return Err(error);
            }
            if tool_calls.is_empty() {
                let text = text_parts.join("\n");
                let value = match parse_agent_final(&node.id, &text, response_schema.as_ref()) {
                    Ok(value) => value,
                    Err(error) => {
                        record_llm_validation_failure(ctx, node, turn, &error)?;
                        append_agent_validation_retry(
                            &node.id,
                            &mut messages,
                            next_provider_state,
                            &text,
                            &error,
                        )?;
                        enforce_agent_transcript_limit(ctx, node, &mut messages, None)?;
                        last_validation_error = Some(error);
                        record_agent_checkpoint(
                            ctx,
                            node,
                            turn,
                            "validation_retry",
                            &AgentCheckpoint {
                                messages: messages.clone(),
                                next_turn: turn.saturating_add(1),
                                tokens_total,
                                tool_calls_total,
                                tool_call_counts: tool_call_counts.clone(),
                                pending_side_effect: None,
                                pending_confirm: None,
                                pending_mcp_call: None,
                            },
                        )?;
                        continue;
                    }
                };
                let value = match enforce_out_of_contract_policy(ctx, node, value)? {
                    OutOfContractDecision::Continue(value) => value,
                    OutOfContractDecision::NeedsUser { question } => {
                        return Ok(StepOutcome::NeedsUser { question });
                    }
                };
                apply_guardrails(
                    ctx,
                    node,
                    &params.guardrails,
                    GuardrailStage::Output,
                    None,
                    &value,
                )
                .await?;
                return Ok(StepOutcome::Success {
                    output: Some(value),
                    files: vec![],
                });
            }
            last_validation_error = None;
            if tool_calls.len() > 1
                && tool_calls
                    .iter()
                    .any(|call| agent_tool_requires_serial_execution(&params.tools, &call.name))
            {
                let error = StepError::failed(
                    &node.id,
                    "llm.agent received parallel tool calls containing an interactive or side-effectful tool; refusing ambiguous replay semantics",
                );
                record_tool_call_failures(
                    ctx,
                    node,
                    &tool_calls,
                    &error,
                    None,
                    ToolCallPhase::InputValidation,
                    ToolCallErrorCode::InvalidArguments,
                )?;
                return Err(error);
            }

            if let Some(state) = next_provider_state {
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
                            None,
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
                let call_number = match charge_agent_tool_call(
                    &node.id,
                    "llm.agent",
                    &params.tools,
                    &call.name,
                    &mut tool_calls_total,
                    max_tool_calls_total,
                    &mut tool_call_counts,
                ) {
                    Ok(call_number) => call_number,
                    Err(error) => {
                        let call_number = tool_call_counts[&call.name];
                        if let Some(message) = recover_agent_tool_call_failure(
                            ctx,
                            node,
                            &params.tools,
                            &call,
                            &error,
                            AgentToolCallFailure {
                                code: AgentFailureCode::ToolCallBudgetExceeded,
                                phase: ToolCallPhase::InputValidation,
                                tool_error_code: ToolCallErrorCode::BudgetExceeded,
                                call_number,
                                retryable: false,
                                started: tool_started,
                            },
                        )? {
                            messages.push(message);
                            continue;
                        }
                        record_tool_call_failure(
                            ctx,
                            node,
                            &call,
                            &error,
                            tool_call_failure(
                                None,
                                ToolCallPhase::InputValidation,
                                ToolCallErrorCode::BudgetExceeded,
                                tool_started,
                            ),
                        )?;
                        return Err(error);
                    }
                };
                if let Err(error) =
                    validate_agent_tool_call_args(node, &mcp_tools, &params.tools, &call)
                {
                    if let Some(message) = recover_agent_tool_call_failure(
                        ctx,
                        node,
                        &params.tools,
                        &call,
                        &error,
                        AgentToolCallFailure {
                            code: AgentFailureCode::ValidationFailed,
                            phase: ToolCallPhase::InputValidation,
                            tool_error_code: ToolCallErrorCode::InvalidArguments,
                            call_number,
                            retryable: true,
                            started: tool_started,
                        },
                    )? {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputValidation,
                            ToolCallErrorCode::InvalidArguments,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if let Err(error) = ctx.checkpoint().await {
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputValidation,
                            ToolCallErrorCode::ExecutionFailed,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if let Err(error) = apply_guardrails(
                    ctx,
                    node,
                    &params.guardrails,
                    GuardrailStage::ToolInput,
                    Some(&call.name),
                    &call.args,
                )
                .await
                {
                    if let Some(message) = recover_agent_tool_call_failure(
                        ctx,
                        node,
                        &params.tools,
                        &call,
                        &error,
                        AgentToolCallFailure {
                            code: AgentFailureCode::GuardrailRejected,
                            phase: ToolCallPhase::InputGuardrail,
                            tool_error_code: ToolCallErrorCode::GuardrailRejected,
                            call_number,
                            retryable: true,
                            started: tool_started,
                        },
                    )? {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputGuardrail,
                            ToolCallErrorCode::GuardrailRejected,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if let Err(error) = scan_llm_text(ctx, node, &serde_json::to_string(&call.args)?) {
                    if let Some(message) = recover_agent_tool_call_failure(
                        ctx,
                        node,
                        &params.tools,
                        &call,
                        &error,
                        AgentToolCallFailure {
                            code: AgentFailureCode::GuardrailRejected,
                            phase: ToolCallPhase::InputGuardrail,
                            tool_error_code: ToolCallErrorCode::GuardrailRejected,
                            call_number,
                            retryable: true,
                            started: tool_started,
                        },
                    )? {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputGuardrail,
                            ToolCallErrorCode::GuardrailRejected,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if agent_tool_has_side_effects(&params.tools, &call.name)
                    && let Err(error) = record_agent_checkpoint(
                        ctx,
                        node,
                        turn,
                        "before_side_effect",
                        &AgentCheckpoint {
                            messages: messages.clone(),
                            next_turn: turn,
                            tokens_total,
                            tool_calls_total,
                            tool_call_counts: tool_call_counts.clone(),
                            pending_side_effect: Some(call.clone()),
                            pending_confirm: None,
                            pending_mcp_call: None,
                        },
                    )
                {
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::InputValidation,
                            ToolCallErrorCode::ExecutionFailed,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                let services = AgentToolServices {
                    runtime: &self.runtime,
                    guardrails: &params.guardrails,
                };
                let outcome = match execute_agent_tool(
                    ctx,
                    node,
                    services,
                    AgentToolInvocation {
                        mcp: &mcp_tools,
                        tools: &params.tools,
                        name: &call.name,
                        call_id: &call.id,
                        call_number,
                        args: &call.args,
                    },
                )
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
                                None,
                                ToolCallPhase::Execution,
                                ToolCallErrorCode::ExecutionFailed,
                                tool_started,
                            ),
                        )?;
                        return Err(error);
                    }
                };
                let (result, returned_agent_error, mut pending_operation) = match outcome {
                    AgentToolOutcome::Result(value) => (value, false, None),
                    AgentToolOutcome::OperationResult {
                        value,
                        operation_id,
                    } => (value, false, Some(operation_id)),
                    AgentToolOutcome::Error(value) => (value, true, None),
                    AgentToolOutcome::Handoff(value) => {
                        if let Err(error) = apply_guardrails(
                            ctx,
                            node,
                            &params.guardrails,
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
                                    None,
                                    ToolCallPhase::OutputGuardrail,
                                    ToolCallErrorCode::OutputRejected,
                                    tool_started,
                                ),
                            )?;
                            return Err(error);
                        }
                        if let Err(error) = apply_guardrails(
                            ctx,
                            node,
                            &params.guardrails,
                            GuardrailStage::Output,
                            None,
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
                                    None,
                                    ToolCallPhase::OutputGuardrail,
                                    ToolCallErrorCode::OutputRejected,
                                    tool_started,
                                ),
                            )?;
                            return Err(error);
                        }
                        let event = tool_call_event(
                            &node.id,
                            None,
                            mcp_tools.server_for(&call.name),
                            &call,
                            &value,
                            tool_call_outcome(
                                ToolCallStatus::Succeeded,
                                ToolCallPhase::Completed,
                                None,
                                tool_started,
                            ),
                        )?;
                        ctx.journal.event("tool_call", event).step_err(&node.id)?;
                        ctx.journal
                            .event(
                                "agent_handoff",
                                json!({
                                    "node": node.id,
                                    "agent": call.name,
                                    "tool_call_id": call.id,
                                }),
                            )
                            .step_err(&node.id)?;
                        return Ok(StepOutcome::Success {
                            output: Some(value),
                            files: vec![],
                        });
                    }
                    AgentToolOutcome::NeedsUser(question) => {
                        let event = tool_call_event(
                            &node.id,
                            None,
                            mcp_tools.server_for(&call.name),
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
                        // Persist the exact suspended MCP call so resume
                        // re-issues it (same call id) instead of regenerating
                        // one that would miss the journaled continuation
                        // (A07). Only MCP aliases suspend through the
                        // continuation store; built-in tools resume through
                        // the model flow.
                        let pending_mcp_call =
                            mcp_tools.server_for(&call.name).map(|_| McpSuspendedCall {
                                name: call.name.clone(),
                                id: call.id.clone(),
                                args: call.args.clone(),
                                question_id: question.id.clone(),
                            });
                        record_agent_checkpoint(
                            ctx,
                            node,
                            turn,
                            "waiting_for_user",
                            &AgentCheckpoint {
                                messages: turn_start_messages.clone(),
                                next_turn: turn,
                                tokens_total,
                                tool_calls_total: turn_start_tool_calls_total,
                                tool_call_counts: turn_start_tool_call_counts.clone(),
                                pending_side_effect: None,
                                pending_confirm: None,
                                pending_mcp_call,
                            },
                        )?;
                        return Ok(StepOutcome::NeedsUser { question });
                    }
                    AgentToolOutcome::NeedsConfirm(confirm) => {
                        let event = tool_call_event(
                            &node.id,
                            None,
                            mcp_tools.server_for(&call.name),
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
                        // Persist the exact approved call so resume executes
                        // the same operation instead of regenerating a
                        // different target with the same approval (A06).
                        record_agent_checkpoint(
                            ctx,
                            node,
                            turn,
                            "waiting_for_confirmation",
                            &AgentCheckpoint {
                                messages: turn_start_messages.clone(),
                                next_turn: turn,
                                tokens_total,
                                tool_calls_total: turn_start_tool_calls_total,
                                tool_call_counts: turn_start_tool_call_counts.clone(),
                                pending_side_effect: Some(call.clone()),
                                pending_confirm: Some(confirm.clone()),
                                pending_mcp_call: None,
                            },
                        )?;
                        return Ok(StepOutcome::NeedsConfirm { confirm });
                    }
                };
                if !returned_agent_error
                    && let Err(error) = apply_guardrails(
                        ctx,
                        node,
                        &params.guardrails,
                        GuardrailStage::ToolOutput,
                        Some(&call.name),
                        &result,
                    )
                    .await
                {
                    // The external effect already happened; record it as a
                    // success without a reusable result before surfacing
                    // the rejection (D03).
                    finish_rejected_external_operation(ctx, node, pending_operation.take());
                    if let Some(message) = recover_agent_tool_call_failure(
                        ctx,
                        node,
                        &params.tools,
                        &call,
                        &error,
                        AgentToolCallFailure {
                            code: AgentFailureCode::GuardrailRejected,
                            phase: ToolCallPhase::OutputGuardrail,
                            tool_error_code: ToolCallErrorCode::OutputRejected,
                            call_number,
                            retryable: true,
                            started: tool_started,
                        },
                    )? {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::OutputGuardrail,
                            ToolCallErrorCode::OutputRejected,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                if let Err(error) = ctx.checkpoint().await {
                    finish_rejected_external_operation(ctx, node, pending_operation.take());
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::OutputGuardrail,
                            ToolCallErrorCode::ExecutionFailed,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                let tool_failed = result.get("isError").and_then(Value::as_bool) == Some(true);
                let event = tool_call_event(
                    &node.id,
                    None,
                    mcp_tools.server_for(&call.name),
                    &call,
                    &result,
                    tool_call_outcome(
                        if tool_failed {
                            ToolCallStatus::Failed
                        } else {
                            ToolCallStatus::Succeeded
                        },
                        ToolCallPhase::Completed,
                        tool_failed.then(|| tool_reported_error(&result)),
                        tool_started,
                    ),
                )?;
                let encoded = serde_json::to_string(&result)?;
                if let Err(error) = scan_llm_text(ctx, node, &encoded) {
                    finish_rejected_external_operation(ctx, node, pending_operation.take());
                    if !returned_agent_error
                        && let Some(message) = recover_agent_tool_call_failure(
                            ctx,
                            node,
                            &params.tools,
                            &call,
                            &error,
                            AgentToolCallFailure {
                                code: AgentFailureCode::GuardrailRejected,
                                phase: ToolCallPhase::OutputGuardrail,
                                tool_error_code: ToolCallErrorCode::OutputRejected,
                                call_number,
                                retryable: true,
                                started: tool_started,
                            },
                        )?
                    {
                        messages.push(message);
                        continue;
                    }
                    record_tool_call_failure(
                        ctx,
                        node,
                        &call,
                        &error,
                        tool_call_failure(
                            None,
                            ToolCallPhase::OutputGuardrail,
                            ToolCallErrorCode::OutputRejected,
                            tool_started,
                        ),
                    )?;
                    return Err(error);
                }
                // Every output check has passed: the operation may now be
                // finished with its result available for resend (D03).
                if let Some(operation_id) = pending_operation.take() {
                    ctx.run.finish_external_operation(
                        ctx.journal,
                        node,
                        &operation_id,
                        Some(result.clone()),
                    )?;
                }
                ctx.journal.event("tool_call", event).step_err(&node.id)?;
                messages.push(ChatMessage::tool_result(call.id, encoded));
            }
            enforce_agent_transcript_limit(ctx, node, &mut messages, None)?;
            record_agent_checkpoint(
                ctx,
                node,
                turn,
                "turn_completed",
                &AgentCheckpoint {
                    messages: messages.clone(),
                    next_turn: turn.saturating_add(1),
                    tokens_total,
                    tool_calls_total,
                    tool_call_counts: tool_call_counts.clone(),
                    pending_side_effect: None,
                    pending_confirm: None,
                    pending_mcp_call: None,
                },
            )?;
        }
        let message = last_validation_error.map_or_else(
            || format!("llm.agent reached max_iterations {max_turns}"),
            |error| {
                format!(
                    "llm.agent failed final response validation after {max_turns} iterations: {error}"
                )
            },
        );
        Err(StepError::failed(&node.id, message))
    }
}

pub(crate) fn agent_tool_has_side_effects(tools: &[ToolDecl], name: &str) -> bool {
    tools
        .iter()
        .find(|tool| tool.name() == name)
        .is_some_and(|tool| match tool {
            ToolDecl::FsWrite { .. } | ToolDecl::Command { .. } | ToolDecl::Http { .. } => true,
            ToolDecl::Mcp { side_effects, .. } => *side_effects,
            ToolDecl::AskUser { .. } | ToolDecl::WebSearch { .. } => false,
            ToolDecl::Agent {
                tools: delegated, ..
            } => delegated
                .iter()
                .any(|delegated| agent_tool_has_side_effects(tools, delegated)),
        })
}

pub(crate) fn agent_tool_requires_serial_execution(tools: &[ToolDecl], name: &str) -> bool {
    tools
        .iter()
        .find(|tool| tool.name() == name)
        .is_some_and(|tool| {
            matches!(tool, ToolDecl::AskUser { .. }) || agent_tool_has_side_effects(tools, name)
        })
}

pub(crate) fn record_agent_checkpoint(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    turn: usize,
    phase: &str,
    checkpoint: &AgentCheckpoint,
) -> Result<(), StepError> {
    ctx.journal
        .event(
            "agent_checkpoint",
            json!({
                "node": node.id,
                "turn": turn,
                "phase": phase,
                "checkpoint": checkpoint,
            }),
        )
        .step_err(&node.id)
}

pub(crate) fn agent_tool_max_calls(tool: &ToolDecl) -> Option<usize> {
    match tool {
        ToolDecl::WebSearch { max_calls, .. } | ToolDecl::Mcp { max_calls, .. } => Some(*max_calls),
        ToolDecl::Agent { max_calls, .. } => Some(*max_calls),
        _ => None,
    }
}

pub(crate) fn charge_agent_tool_call(
    node_id: &str,
    scope: &str,
    tools: &[ToolDecl],
    name: &str,
    total: &mut usize,
    max_total: usize,
    counts: &mut BTreeMap<String, usize>,
) -> Result<usize, StepError> {
    *total = total.saturating_add(1);
    let count = counts.entry(name.to_string()).or_default();
    *count = count.saturating_add(1);
    if *total > max_total {
        return Err(StepError::failed(
            node_id,
            format!("{scope} tool call budget exceeded: {total} > {max_total}"),
        ));
    }
    if let Some(max_calls) = tools
        .iter()
        .find(|tool| tool.name() == name)
        .and_then(agent_tool_max_calls)
        && *count > max_calls
    {
        return Err(StepError::failed(
            node_id,
            format!("{scope} tool `{name}` call budget exceeded: {count} > {max_calls}"),
        ));
    }
    Ok(*count)
}

pub(crate) fn agent_command_permission<'a>(
    permissions: &'a [qcg_contract::CommandPermission],
    command: &[String],
) -> Option<&'a qcg_contract::CommandPermission> {
    let (bin, args) = command.split_first()?;
    permissions.iter().find(|permission| {
        permission.bin == *bin
            && permission.args.len() == args.len()
            && permission
                .args
                .iter()
                .zip(args)
                .all(|(pattern, actual)| pattern == "*" || pattern == actual)
    })
}

pub(crate) fn agent_command_allowed(
    permissions: &[qcg_contract::CommandPermission],
    command: &[String],
) -> bool {
    agent_command_permission(permissions, command).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn suspended_call(question_id: &str) -> AgentCheckpoint {
        AgentCheckpoint {
            messages: Vec::new(),
            next_turn: 1,
            tokens_total: 0,
            tool_calls_total: 0,
            tool_call_counts: BTreeMap::new(),
            pending_side_effect: None,
            pending_confirm: None,
            pending_mcp_call: Some(McpSuspendedCall {
                name: "search".into(),
                id: "call-1".into(),
                args: json!({"query": "x"}),
                question_id: question_id.into(),
            }),
        }
    }

    #[test]
    fn answered_mcp_suspension_resumes_with_the_same_call_id() {
        let checkpoint = suspended_call("q-1");
        let answers = BTreeMap::from([("q-1".to_string(), json!({"response_0": "go"}))]);
        let resumed = resumed_mcp_call(Some(&checkpoint), &answers)
            .expect("answered suspension should resume");
        assert_eq!(resumed.id, "call-1");
        assert_eq!(resumed.name, "search");
        assert_eq!(resumed.question_id, "q-1");
    }

    #[test]
    fn unanswered_mcp_suspension_stays_on_the_model_flow() {
        let checkpoint = suspended_call("q-1");
        assert!(resumed_mcp_call(Some(&checkpoint), &BTreeMap::new()).is_none());
        let mut other_answered = BTreeMap::new();
        other_answered.insert("q-2".to_string(), json!({}));
        assert!(resumed_mcp_call(Some(&checkpoint), &other_answered).is_none());
    }

    #[test]
    fn missing_suspension_never_resumes() {
        let mut checkpoint = suspended_call("q-1");
        checkpoint.pending_mcp_call = None;
        let answers = BTreeMap::from([("q-1".to_string(), json!({}))]);
        assert!(resumed_mcp_call(Some(&checkpoint), &answers).is_none());
        assert!(resumed_mcp_call(None, &answers).is_none());
    }

    #[test]
    fn checkpoint_without_mcp_field_still_parses() {
        // Checkpoints journaled before the MCP suspension field existed
        // must not brick resume.
        let old = json!({
            "messages": [],
            "next_turn": 1,
            "tokens_total": 0,
            "tool_calls_total": 0,
            "tool_call_counts": {},
            "pending_side_effect": null,
            "pending_confirm": null,
        });
        let checkpoint: AgentCheckpoint =
            serde_json::from_value(old).expect("old checkpoint should parse");
        assert!(checkpoint.pending_mcp_call.is_none());
    }
}
