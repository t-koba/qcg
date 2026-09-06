use qcg_contract::{ContextOverflowPolicy, LlmRequestPolicy, NodeDef};
use qcg_engine::{ResultExt, StepContext, StepError};
use qcg_llm::{ChatMessage, ChatRequest};
use serde_json::json;

use super::DEFAULT_RETRY_PROMPT;
use crate::policy::{EffectiveRequestPolicy, effective_request_policy};
use crate::prompting::{utf8_head, utf8_tail};
use qcg_policy::DEFAULT_LLM_CONTEXT_LIMIT_BYTES;

pub(crate) fn effective_context_byte_limit(policy: &EffectiveRequestPolicy) -> usize {
    policy
        .max_context_bytes
        .into_iter()
        .chain(
            policy
                .max_context_tokens
                .map(|tokens| tokens.saturating_mul(4)),
        )
        .min()
        .unwrap_or(DEFAULT_LLM_CONTEXT_LIMIT_BYTES)
}

pub(crate) fn enforce_llm_request_context_limit(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    policy: &EffectiveRequestPolicy,
    request: &mut ChatRequest,
) -> Result<(), StepError> {
    let limit = effective_context_byte_limit(policy);
    let actual = serde_json::to_vec(request)?.len();
    if actual <= limit {
        return Ok(());
    }
    let mut envelope = request.clone();
    envelope.messages.clear();
    let envelope_bytes = serde_json::to_vec(&envelope)?.len();
    let message_limit = limit.saturating_sub(envelope_bytes);
    let (compacted_results, compacted_messages, policy) = compact_agent_messages(
        node,
        &mut request.messages,
        message_limit,
        "request",
        actual,
        &policy.context_overflow,
    )?;
    let compacted = serde_json::to_vec(request)?.len();
    if compacted > limit {
        return Err(StepError::failed(
            &node.id,
            format!(
                "LLM request context byte limit exceeded after bounded compaction: {compacted} > {limit}"
            ),
        ));
    }
    record_context_compaction(
        ctx,
        node,
        ContextCompactionRecord {
            scope: "request",
            policy,
            actual,
            final_bytes: compacted,
            limit_bytes: limit,
            compacted_results,
            compacted_messages,
        },
    )
}

pub(crate) fn enforce_agent_transcript_limit(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    messages: &mut Vec<ChatMessage>,
    specialist: Option<&LlmRequestPolicy>,
) -> Result<(), StepError> {
    let llm = ctx
        .run
        .contract
        .manifest
        .llm
        .as_ref()
        .ok_or_else(|| StepError::failed(&node.id, "[llm] is required"))?;
    let policy = effective_request_policy(node, llm, specialist)?;
    let limit = effective_context_byte_limit(&policy);
    let actual = serde_json::to_vec(messages)?.len();
    if actual <= limit {
        return Ok(());
    }
    let (compacted_results, compacted_messages, policy) = compact_agent_messages(
        node,
        messages,
        limit,
        "agent_transcript",
        actual,
        &policy.context_overflow,
    )?;
    let final_bytes = serde_json::to_vec(messages)?.len();
    record_context_compaction(
        ctx,
        node,
        ContextCompactionRecord {
            scope: "agent_transcript",
            policy,
            actual,
            final_bytes,
            limit_bytes: limit,
            compacted_results,
            compacted_messages,
        },
    )
}

pub(crate) fn compact_agent_messages(
    node: &NodeDef,
    messages: &mut Vec<ChatMessage>,
    limit: usize,
    scope: &str,
    original_bytes: usize,
    policy: &ContextOverflowPolicy,
) -> Result<(usize, usize, ContextOverflowPolicy), StepError> {
    if matches!(policy, ContextOverflowPolicy::Error) {
        return Err(StepError::failed(
            &node.id,
            format!("LLM {scope} context byte limit exceeded: {original_bytes} > {limit}"),
        ));
    }
    let compacted_results = compact_tool_results(
        messages,
        matches!(policy, ContextOverflowPolicy::TruncateTail),
        limit,
    )?;
    let compacted_messages = compact_message_contents(
        messages,
        matches!(policy, ContextOverflowPolicy::TruncateTail),
        limit,
    )?;
    let final_bytes = serde_json::to_vec(messages)?.len();
    if final_bytes > limit {
        return Err(StepError::failed(
            &node.id,
            format!(
                "LLM {scope} context byte limit exceeded and non-tool context cannot be compacted safely: {final_bytes} > {limit}"
            ),
        ));
    }
    Ok((compacted_results, compacted_messages, policy.clone()))
}

pub(crate) struct ContextCompactionRecord<'a> {
    scope: &'a str,
    policy: ContextOverflowPolicy,
    actual: usize,
    final_bytes: usize,
    limit_bytes: usize,
    compacted_results: usize,
    compacted_messages: usize,
}

pub(crate) fn record_context_compaction(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    record: ContextCompactionRecord<'_>,
) -> Result<(), StepError> {
    ctx.journal
        .event(
            "context_compacted",
            json!({
                "node": node.id,
                "scope": record.scope,
                "policy": record.policy,
                "original_bytes": record.actual,
                "final_bytes": record.final_bytes,
                "limit_bytes": record.limit_bytes,
                "compacted_tool_results": record.compacted_results,
                "compacted_messages": record.compacted_messages,
            }),
        )
        .step_err(&node.id)
}

pub(crate) fn compact_tool_results(
    messages: &mut [ChatMessage],
    newest_first: bool,
    limit: usize,
) -> Result<usize, serde_json::Error> {
    if serde_json::to_vec(messages)?.len() <= limit {
        return Ok(0);
    }
    let mut indices = messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| (message.role == "tool").then_some(index))
        .collect::<Vec<_>>();
    if newest_first {
        indices.reverse();
    }
    let mut compacted_results = 0usize;
    for index in indices {
        let message = &mut messages[index];
        let original_result_bytes = message.content.len();
        message.content = serde_json::to_string(&json!({
            "qcg_truncated_tool_result": true,
            "original_bytes": original_result_bytes,
        }))?;
        compacted_results = compacted_results.saturating_add(1);
        if serde_json::to_vec(messages)?.len() <= limit {
            break;
        }
    }
    Ok(compacted_results)
}

pub(crate) fn compact_message_contents(
    messages: &mut [ChatMessage],
    newest_first: bool,
    limit: usize,
) -> Result<usize, serde_json::Error> {
    if serde_json::to_vec(messages)?.len() <= limit {
        return Ok(0);
    }
    let mut indices = messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            (message.role != "tool"
                && message.provider_state.is_none()
                && !message.content.is_empty())
            .then_some(index)
        })
        .collect::<Vec<_>>();
    if newest_first {
        indices.reverse();
    }
    let marker = "\n[QCG_CONTEXT_TRUNCATED]\n";
    let mut compacted = 0_usize;
    for index in indices {
        let current_size = serde_json::to_vec(messages)?.len();
        if current_size <= limit {
            break;
        }
        let original = messages[index].content.clone();
        let excess = current_size.saturating_sub(limit);
        let retained_bytes = original
            .len()
            .saturating_sub(excess.saturating_add(marker.len()));
        messages[index].content = if newest_first {
            format!("{}{marker}", utf8_head(&original, retained_bytes))
        } else {
            format!("{marker}{}", utf8_tail(&original, retained_bytes))
        };
        if serde_json::to_vec(messages)?.len() > limit {
            messages[index].content = marker.to_string();
        }
        compacted = compacted.saturating_add(1);
    }
    Ok(compacted)
}

pub(crate) fn retry_prompt(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    base_prompt: &str,
    attempt: usize,
    last_error: Option<&str>,
) -> Result<String, StepError> {
    let mut prompt = base_prompt.to_string();
    prompt.push_str("\n\nQCG_RETRY_ATTEMPT: ");
    prompt.push_str(&attempt.to_string());
    prompt.push('\n');
    if let Some(error) = last_error {
        let llm = ctx
            .run
            .contract
            .manifest
            .llm
            .as_ref()
            .ok_or_else(|| StepError::failed(&node.id, "[llm] is required"))?;
        let policy = effective_request_policy(node, llm, None)?;
        let template = policy
            .retry_prompt
            .as_deref()
            .unwrap_or(DEFAULT_RETRY_PROMPT);
        let rendered = ctx
            .run
            .templates
            .render_inline(
                template,
                json!({ "error": error, "attempt": attempt }),
                &ctx.run.contract.manifest.runtime,
            )
            .map_err(|render_error| {
                StepError::failed(
                    &node.id,
                    format!("[llm].retry_prompt failed to render: {render_error}"),
                )
            })?;
        prompt.push_str(&rendered);
    }
    Ok(prompt)
}

pub(crate) fn record_llm_validation_failure(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    attempt: usize,
    message: &str,
) -> Result<(), StepError> {
    ctx.journal
        .event(
            "llm_validation_failed",
            json!({ "node": node.id, "attempt": attempt, "message": message }),
        )
        .step_err(&node.id)
}
