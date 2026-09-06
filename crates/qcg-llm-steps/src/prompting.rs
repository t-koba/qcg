use qcg_contract::{ContextOverflowPolicy, ContextRef, Contract, NodeDef, ResourceContextRef};
use qcg_engine::{
    ResourceSelector, ResultExt, StepContext, StepError, select_resource, validate_json_schema_step,
};
use qcg_llm::{ChatContent, ChatMessage, StopReason};
use qcg_policy::{MAX_JSON_SCHEMA_BYTES, validate_bounded_json_schema};
use serde::Serialize;
use serde_json::{Value, json};
use std::io::Read;

use crate::context::effective_context_byte_limit;
use crate::policy::{EffectiveRequestPolicy, effective_request_policy, llm_params};
use qcg_policy::DEFAULT_LLM_CONTEXT_LIMIT_BYTES;

pub(crate) fn render_prompt(ctx: &StepContext<'_>, node: &NodeDef) -> Result<String, StepError> {
    let params = llm_params(node)?;
    let prompt = params.prompt.as_deref().expect("validated prompt");
    let source = load_prompt_source(&ctx.run.contract, node, prompt)?;
    let mut rendered = ctx.render_inline(node, &source)?;
    if !params.context.is_empty() {
        rendered.push_str("\n\n<QCG_DECLARED_CONTEXT>\n");
        manage_context_limits(ctx, node, &mut rendered)?;
        append_declared_context(ctx, node, &params.context, &mut rendered)?;
        rendered.push_str("</QCG_DECLARED_CONTEXT>\n");
    }
    manage_context_limits(ctx, node, &mut rendered)?;
    Ok(rendered)
}

pub(crate) fn manage_context_limits(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    prompt: &mut String,
) -> Result<(), StepError> {
    let Some(llm) = &ctx.run.contract.manifest.llm else {
        return Ok(());
    };
    let policy = effective_request_policy(node, llm, None)?;
    let byte_limit = effective_context_byte_limit(&policy);
    if matches!(policy.context_overflow, ContextOverflowPolicy::Error) {
        let actual = prompt.len();
        if actual > byte_limit {
            return Err(StepError::failed(
                &node.id,
                format!("LLM context byte limit exceeded: {actual} > {byte_limit}"),
            ));
        }
        if let Some(max_tokens) = policy.max_context_tokens {
            let actual = estimate_context_tokens(prompt);
            if actual > max_tokens {
                return Err(StepError::failed(
                    &node.id,
                    format!("LLM context token limit exceeded: {actual} > {max_tokens}"),
                ));
            }
        }
        return Ok(());
    }
    if prompt.len() <= byte_limit {
        return Ok(());
    }
    let original_bytes = prompt.len();
    let marker = "\n[QCG_CONTEXT_TRUNCATED]\n";
    if byte_limit < marker.len() {
        return Err(StepError::failed(
            &node.id,
            format!("LLM context byte limit {byte_limit} is too small for the truncation marker"),
        ));
    }
    let content_limit = byte_limit.saturating_sub(marker.len());
    let compacted = match policy.context_overflow {
        ContextOverflowPolicy::TruncateHead => {
            format!("{marker}{}", utf8_tail(prompt, content_limit))
        }
        ContextOverflowPolicy::TruncateTail => {
            format!("{}{marker}", utf8_head(prompt, content_limit))
        }
        ContextOverflowPolicy::Error => unreachable!(),
    };
    *prompt = compacted;
    ctx.journal
        .event(
            "context_compacted",
            json!({
                "node": node.id,
                "policy": policy.context_overflow,
                "original_bytes": original_bytes,
                "final_bytes": prompt.len(),
                "limit_bytes": byte_limit,
            }),
        )
        .step_err(&node.id)?;
    Ok(())
}

pub(crate) fn utf8_head(value: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

pub(crate) fn utf8_tail(value: &str, max_bytes: usize) -> &str {
    let mut start = value.len().saturating_sub(max_bytes);
    while start < value.len() && !value.is_char_boundary(start) {
        start += 1;
    }
    &value[start..]
}

pub(crate) fn estimate_context_tokens(text: &str) -> usize {
    text.chars().count().div_ceil(4).max(1)
}

pub(crate) fn render_repair_prompt(
    ctx: &StepContext<'_>,
    node: &NodeDef,
) -> Result<String, StepError> {
    let mut prompt = render_prompt(ctx, node)?;
    let params = llm_params(node)?;
    if let Some(source) = &params.source {
        let source = ctx.render_inline(node, source)?;
        let source_path = resolve_workspace_read(ctx, node, &source)?;
        let source_limit = prompt_source_byte_limit(&ctx.run.contract);
        let source_bytes = read_bytes_bounded(&source_path, source_limit).map_err(|error| {
            StepError::failed(
                &node.id,
                format!(
                    "repair source `{source}` could not be read within its byte limit: {error}"
                ),
            )
        })?;
        let source_text = String::from_utf8(source_bytes).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("repair source `{source}` is not valid UTF-8: {error}"),
            )
        })?;
        prompt.push_str("\n\n<QCG_REPAIR_SOURCE path=\"");
        prompt.push_str(&source);
        prompt.push_str("\">\n");
        prompt.push_str(&source_text);
        prompt.push_str("\n</QCG_REPAIR_SOURCE>\n");
        manage_context_limits(ctx, node, &mut prompt)?;
    }
    Ok(prompt)
}

pub(crate) fn read_bytes_bounded(
    path: &camino::Utf8Path,
    limit: usize,
) -> Result<Vec<u8>, std::io::Error> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("input exceeds {limit} bytes"),
        ));
    }
    Ok(bytes)
}

pub(crate) fn resolve_workspace_read(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    path: &str,
) -> Result<camino::Utf8PathBuf, StepError> {
    let full_path = ctx.run.fs.resolve_read(path).step_err(&node.id)?;
    if !full_path.is_file() {
        return Err(StepError::failed(
            &node.id,
            format!("source path `{path}` was not found"),
        ));
    }
    Ok(full_path)
}

pub(crate) fn load_schema(
    ctx: &StepContext<'_>,
    node: &NodeDef,
) -> Result<Option<Value>, StepError> {
    let params = llm_params(node)?;
    let Some(schema) = &params.schema else {
        return Ok(None);
    };
    load_response_schema(&ctx.run.contract, node, schema).map(Some)
}

pub(crate) fn load_prompt_source(
    contract: &Contract,
    node: &NodeDef,
    path: &str,
) -> Result<String, StepError> {
    let prompt_path = resolve_prompt_path(contract, node, path)?;
    let limit = prompt_source_byte_limit(contract);
    let bytes = read_bytes_bounded(&prompt_path, limit).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("LLM prompt source `{path}` could not be read within {limit} bytes: {error}"),
        )
    })?;
    String::from_utf8(bytes).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("LLM prompt `{path}` is not valid UTF-8: {error}"),
        )
    })
}

pub(crate) fn resolve_prompt_path(
    contract: &Contract,
    node: &NodeDef,
    path: &str,
) -> Result<camino::Utf8PathBuf, StepError> {
    let prompt_path = contract.resolve_package_path(path).step_err(&node.id)?;
    let metadata = std::fs::metadata(&prompt_path)?;
    if !metadata.is_file() {
        return Err(StepError::failed(
            &node.id,
            format!("LLM prompt `{path}` is not a file"),
        ));
    }
    let limit = prompt_source_byte_limit(contract);
    if metadata.len() > limit as u64 {
        return Err(StepError::failed(
            &node.id,
            format!(
                "LLM prompt source `{path}` exceeds byte limit: {} > {limit}",
                metadata.len()
            ),
        ));
    }
    Ok(prompt_path)
}

pub(crate) fn prompt_source_byte_limit(contract: &Contract) -> usize {
    // Explicit contract values are honored as-is; the default applies only
    // when the contract sets no bound.
    contract
        .manifest
        .llm
        .as_ref()
        .map(|llm| effective_context_byte_limit(&EffectiveRequestPolicy::from_llm(llm)))
        .unwrap_or(DEFAULT_LLM_CONTEXT_LIMIT_BYTES)
}

pub(crate) fn load_response_schema(
    contract: &Contract,
    node: &NodeDef,
    path: &str,
) -> Result<Value, StepError> {
    let schema_path = contract.resolve_package_path(path).step_err(&node.id)?;
    let metadata = std::fs::metadata(&schema_path)?;
    if !metadata.is_file() {
        return Err(StepError::failed(
            &node.id,
            format!("LLM response schema `{path}` is not a file"),
        ));
    }
    if metadata.len() > MAX_JSON_SCHEMA_BYTES as u64 {
        return Err(StepError::failed(
            &node.id,
            format!("LLM response schema `{path}` exceeds {MAX_JSON_SCHEMA_BYTES} bytes"),
        ));
    }
    let source = read_bytes_bounded(&schema_path, MAX_JSON_SCHEMA_BYTES)?;
    let schema: Value = serde_json::from_slice(&source).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("LLM response schema `{path}` is not valid JSON: {error}"),
        )
    })?;
    validate_bounded_json_schema(&schema).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("LLM response schema `{path}` is invalid or unsafe: {error}"),
        )
    })?;
    Ok(schema)
}

pub(crate) fn append_declared_context(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    context: &[ContextRef],
    rendered: &mut String,
) -> Result<(), StepError> {
    for item in context {
        let part = if let ContextRef::Short(item) = item {
            if item == "inputs.*" {
                format_context_value("inputs.*", ctx.vars.inputs())?
            } else if let Some(path) = item.strip_prefix("inputs.") {
                let key = format!("inputs.{path}");
                let value = ctx.vars.get_path(&key).ok_or_else(|| {
                    StepError::failed(&node.id, format!("context `{item}` not found"))
                })?;
                format_context_value(item, value)?
            } else if item.starts_with("steps.") {
                let value = ctx.vars.get_path(item).ok_or_else(|| {
                    StepError::failed(&node.id, format!("context `{item}` not found"))
                })?;
                format_context_value(item, value)?
            } else if let Some(resource_ref) = item.strip_prefix("resources.") {
                let (resource_name, selector) = short_resource_selector(node, resource_ref)?;
                format_resource_context(ctx, node, resource_name, selector.as_ref())?
            } else {
                return Err(StepError::failed(
                    &node.id,
                    format!("unsupported context reference `{item}`"),
                ));
            }
        } else {
            if let ContextRef::Resource(reference) = item {
                let selector = structured_resource_selector(node, reference)?;
                format_resource_context(ctx, node, &reference.resource, selector.as_ref())?
            } else {
                continue;
            }
        };
        rendered.push_str(&part);
        manage_context_limits(ctx, node, rendered)?;
    }
    Ok(())
}

pub(crate) fn format_context_value<T>(label: &str, value: &T) -> Result<String, StepError>
where
    T: Serialize + ?Sized,
{
    Ok(format!(
        "<context ref=\"{}\" type=\"json\">\n{}\n</context>\n",
        label,
        serde_json::to_string_pretty(value)?
    ))
}

pub(crate) fn format_resource_context(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    resource_name: &str,
    selector: Option<&ResourceSelector>,
) -> Result<String, StepError> {
    let resource = ctx
        .run
        .contract
        .manifest
        .resources
        .get(resource_name)
        .ok_or_else(|| {
            StepError::failed(
                &node.id,
                format!("resource `{resource_name}` is not declared"),
            )
        })?;
    if !resource.llm_visible {
        return Err(StepError::failed(
            &node.id,
            format!("resource `{resource_name}` is not llm_visible"),
        ));
    }
    let text = select_resource(ctx.run, resource_name, resource, selector).step_err(&node.id)?;
    let trust = match &resource.trust {
        qcg_contract::Trust::Trusted => "trusted",
        qcg_contract::Trust::Untrusted => "untrusted",
    };
    Ok(format!(
        "<context ref=\"resources.{resource_name}\" type=\"resource\" trust=\"{trust}\">\n{}\n</context>\n",
        text
    ))
}

pub(crate) fn short_resource_selector<'a>(
    node: &NodeDef,
    resource_ref: &'a str,
) -> Result<(&'a str, Option<ResourceSelector>), StepError> {
    let Some((name, selector)) = resource_ref.split_once('#') else {
        return Ok((resource_ref, None));
    };
    if name.is_empty() || selector.is_empty() {
        return Err(StepError::failed(
            &node.id,
            format!("invalid resource context reference `resources.{resource_ref}`"),
        ));
    }
    let selector = if selector == "operations" {
        ResourceSelector::Operations { tag: None }
    } else if let Some(tag) = selector
        .strip_prefix("operations(tag=")
        .and_then(|value| value.strip_suffix(')'))
    {
        if tag.is_empty() {
            return Err(StepError::failed(
                &node.id,
                "resource operation tag is empty",
            ));
        }
        ResourceSelector::Operations {
            tag: Some(tag.to_string()),
        }
    } else if let Some(path) = selector.strip_prefix("files/") {
        ResourceSelector::File {
            path: path.to_string(),
        }
    } else {
        ResourceSelector::Named(selector.to_string())
    };
    Ok((name, Some(selector)))
}

pub(crate) fn structured_resource_selector(
    node: &NodeDef,
    reference: &ResourceContextRef,
) -> Result<Option<ResourceSelector>, StepError> {
    let Some(select) = reference.select.as_deref() else {
        if reference.tag.is_some() || reference.path.is_some() {
            return Err(StepError::failed(
                &node.id,
                "resource context tag/path requires select",
            ));
        }
        return Ok(None);
    };
    match select {
        "operations" if reference.path.is_none() => Ok(Some(ResourceSelector::Operations {
            tag: reference.tag.clone(),
        })),
        "file" | "files" if reference.tag.is_none() => {
            let path = reference.path.clone().ok_or_else(|| {
                StepError::failed(&node.id, "resource file selector requires path")
            })?;
            Ok(Some(ResourceSelector::File { path }))
        }
        _ if reference.tag.is_none() && reference.path.is_none() => {
            Ok(Some(ResourceSelector::Named(select.to_string())))
        }
        _ => Err(StepError::failed(
            &node.id,
            format!("resource selector `{select}` does not accept tag/path"),
        )),
    }
}

/// Parses a JSON value out of an LLM response. Real models frequently wrap
/// JSON in markdown fences or add prose before and after the payload, so the
/// parser tries the raw text first and then falls back to extracting the
/// fenced block or any balanced object or array region.
pub(crate) fn parse_llm_json(text: &str) -> Result<Value, String> {
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return unwrap_text_wrapped(value);
    }
    if trimmed.starts_with("```") {
        let without_first_fence = trimmed
            .split_once('\n')
            .map(|(_, rest)| rest)
            .unwrap_or(trimmed);
        let body = match without_first_fence.rfind("```") {
            Some(end) => &without_first_fence[..end],
            None => without_first_fence,
        };
        if let Ok(value) = serde_json::from_str::<Value>(body.trim()) {
            return Ok(value);
        }
    }
    // Prose before/after the payload is handled by the balanced candidate
    // scanner below.
    for (start, end) in balanced_json_candidates(trimmed) {
        if let Ok(value) = serde_json::from_str::<Value>(&trimmed[start..=end]) {
            return unwrap_text_wrapped(value);
        }
    }
    Err(trimmed.chars().take(120).collect())
}

pub(crate) fn balanced_json_candidates(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut stack: Vec<(usize, u8)> = Vec::new();
    let mut candidates = Vec::new();
    let mut in_string = false;
    let mut escaped = false;

    for (index, byte) in bytes.iter().copied().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => stack.push((index, b'}')),
            b'[' => stack.push((index, b']')),
            b'}' | b']' => {
                if stack.last().is_some_and(|(_, close)| *close == byte) {
                    let (start, _) = stack.pop().expect("matching opener exists");
                    candidates.push((start, index));
                } else {
                    // A prose brace can interrupt an otherwise unrelated
                    // region. Discard the malformed nesting and keep scanning
                    // for later valid JSON.
                    stack.clear();
                }
            }
            _ => {}
        }
    }

    // Prefer a complete outer candidate at the same starting byte, while
    // retaining response order between separate top-level candidates.
    candidates.sort_unstable_by(|(left_start, left_end), (right_start, right_end)| {
        left_start
            .cmp(right_start)
            .then_with(|| right_end.cmp(left_end))
    });
    candidates
}

pub(crate) fn validate_agent_stop(
    node_id: &str,
    stop: StopReason,
    has_tool_calls: bool,
) -> Result<(), StepError> {
    match (stop, has_tool_calls) {
        (StopReason::EndTurn, false) | (StopReason::ToolUse, true) => Ok(()),
        (StopReason::MaxTokens, _) => Err(StepError::failed(
            node_id,
            "LLM agent response reached the provider output-token limit",
        )),
        (StopReason::Refusal, _) => Err(StepError::failed(
            node_id,
            "LLM provider refused the agent request",
        )),
        (StopReason::ToolUse, false) => Err(StepError::failed(
            node_id,
            "LLM provider reported tool use without a tool call",
        )),
        (StopReason::EndTurn, true) => Err(StepError::failed(
            node_id,
            "LLM provider returned tool calls with an end-turn stop reason",
        )),
    }
}

pub(crate) fn parse_agent_final(
    node_id: &str,
    text: &str,
    schema: Option<&Value>,
) -> Result<Value, String> {
    if text.trim().is_empty() {
        return Err("LLM agent final response was empty".into());
    }
    let parsed = parse_llm_json(text);
    let value = match (parsed, schema) {
        (Ok(value), _) => value,
        (Err(error), Some(_)) => {
            return Err(format!("LLM agent final response was not JSON: {error}"));
        }
        (Err(_), None) => json!({ "text": text }),
    };
    if let Some(schema) = schema {
        validate_json_schema_step(node_id, schema, &value, "LLM agent final response")
            .map_err(|error| error.to_string())?;
    }
    Ok(value)
}

pub(crate) fn append_agent_validation_retry(
    node_id: &str,
    messages: &mut Vec<ChatMessage>,
    provider_state: Option<Value>,
    text: &str,
    error: &str,
) -> Result<(), StepError> {
    if let Some(state) = provider_state {
        let items = state.as_array().cloned().ok_or_else(|| {
            StepError::failed(node_id, "Responses API provider state must be an array")
        })?;
        messages.push(ChatMessage::provider_state(items));
    } else {
        messages.push(ChatMessage::text("assistant", text));
    }
    messages.push(ChatMessage::text(
        "user",
        format!(
            "Your final response failed local validation: {error}. Return a corrected final response without calling additional tools unless new evidence is strictly required."
        ),
    ));
    Ok(())
}

/// Unwraps a single-key `{"text": "<json string>"}` response wrapper. Some
/// models echo the runtime output shape instead of the bare payload.
pub(crate) fn unwrap_text_wrapped(value: Value) -> Result<Value, String> {
    if let Value::Object(map) = &value
        && map.len() == 1
        && let Some(Value::String(inner)) = map.get("text")
        && let Ok(nested) = serde_json::from_str::<Value>(inner)
    {
        return Ok(nested);
    }
    Ok(value)
}

pub(crate) fn response_text(content: Vec<ChatContent>) -> Result<String, StepError> {
    content
        .into_iter()
        .find_map(|content| match content {
            ChatContent::Text(text) => Some(text),
            ChatContent::ToolCall { .. } => None,
        })
        .ok_or_else(|| StepError::failed("llm", "LLM response did not contain text"))
}
