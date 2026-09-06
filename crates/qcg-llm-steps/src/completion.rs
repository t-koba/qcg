use qcg_contract::{LlmRequestPolicy, NodeDef};
use qcg_engine::{StepContext, StepError};
use qcg_llm::{ChatRequest, LlmRuntime};
use serde_json::{Value, json};

use crate::context::enforce_llm_request_context_limit;
use crate::policy::{LlmParams, effective_request_policy};
use crate::prompting::response_text;
use crate::request::build_request;
use crate::routes::invocation_routes;

pub(crate) struct TextCompletion {
    pub(crate) text: String,
    pub(crate) usage: qcg_llm::TokenUsage,
}

pub(crate) async fn complete_text_with_prompt(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    runtime: &LlmRuntime,
    response_schema: Option<Value>,
    prompt: String,
    attempt: usize,
) -> Result<TextCompletion, StepError> {
    let request = build_request(ctx, node, runtime, prompt, response_schema)?;
    let response = complete_llm(ctx, node, request, |_| json!({ "attempt": attempt })).await?;
    let text = response_text(response.content)?;
    Ok(TextCompletion {
        text,
        usage: response.usage,
    })
}

pub(crate) fn validate_retry_budget(node: &NodeDef, params: &LlmParams) -> Result<(), StepError> {
    let iterations = params
        .max_iterations
        .ok_or_else(|| StepError::failed(&node.id, "max_iterations is required"))?;
    if iterations == 0 {
        return Err(StepError::failed(
            &node.id,
            "max_iterations must be greater than zero",
        ));
    }
    let tokens = params
        .max_tokens_total
        .ok_or_else(|| StepError::failed(&node.id, "max_tokens_total is required"))?;
    if tokens == 0 {
        return Err(StepError::failed(
            &node.id,
            "max_tokens_total must be greater than zero",
        ));
    }
    Ok(())
}

pub(crate) fn checked_usage_total(
    node: &NodeDef,
    current: u64,
    usage: &qcg_llm::TokenUsage,
) -> Result<u64, StepError> {
    current
        .checked_add(usage.input)
        .and_then(|total| total.checked_add(usage.output))
        .ok_or_else(|| StepError::failed(&node.id, "LLM token accounting overflowed"))
}

pub(crate) async fn complete_llm<F>(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    request: ChatRequest,
    event_extra: F,
) -> Result<qcg_llm::ChatResponse, StepError>
where
    F: FnOnce(&qcg_llm::TokenUsage) -> Value,
{
    complete_llm_with_policy(ctx, node, request, None, None, event_extra).await
}

pub(crate) async fn complete_llm_with_policy<F>(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    mut request: ChatRequest,
    specialist: Option<&LlmRequestPolicy>,
    route_override: Option<&[qcg_contract::ModelRef]>,
    event_extra: F,
) -> Result<qcg_llm::ChatResponse, StepError>
where
    F: FnOnce(&qcg_llm::TokenUsage) -> Value,
{
    let llm = ctx
        .run
        .contract
        .manifest
        .llm
        .as_ref()
        .ok_or_else(|| StepError::failed(&node.id, "[llm] is required"))?;
    let policy = effective_request_policy(node, llm, specialist)?;
    enforce_llm_request_context_limit(ctx, node, &policy, &mut request)?;
    let routes = route_override
        .map(<[qcg_contract::ModelRef]>::to_vec)
        .map(Ok)
        .unwrap_or_else(|| invocation_routes(ctx, node, &request, None, None))?;
    let gateway = ctx
        .llm
        .as_ref()
        .ok_or_else(|| StepError::failed(&node.id, "LLM gateway is not configured"))?;
    gateway.complete(node, request, &routes, event_extra).await
}
