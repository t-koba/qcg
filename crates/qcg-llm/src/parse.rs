use serde_json::{Value, json};

use crate::types::{ChatContent, ChatResponse, LlmError, LlmErrorKind, StopReason, TokenUsage};

pub(crate) fn parse_chat_completions_response(value: Value) -> Result<ChatResponse, LlmError> {
    let message = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .ok_or_else(|| {
            LlmError::invalid_response(
                "OpenAI-compatible response did not include choices[0].message",
            )
        })?;
    let mut content = Vec::new();
    if let Some(text) = message.get("content").and_then(Value::as_str)
        && !text.is_empty()
    {
        content.push(ChatContent::Text(text.to_string()));
    }
    let refused = message
        .get("refusal")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty());
    if let Some(text) = refused {
        content.push(ChatContent::Text(text.to_string()));
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for call in tool_calls {
            content.push(parse_openai_tool_call(call)?);
        }
    }
    if content.is_empty() {
        return Err(LlmError::invalid_response(
            "OpenAI-compatible response did not include text or tool calls",
        ));
    }
    let usage = TokenUsage {
        input: required_usage(&value, "/usage/prompt_tokens")?,
        output: required_usage(&value, "/usage/completion_tokens")?,
        reasoning: value
            .pointer("/usage/completion_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        cached_input: value
            .pointer("/usage/prompt_tokens_details/cached_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    };
    let finish_reason = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(Value::as_str);
    let stop = match (finish_reason, refused.is_some()) {
        (_, true) | (Some("content_filter"), _) => StopReason::Refusal,
        (Some("length"), _) => StopReason::MaxTokens,
        (Some("tool_calls"), _) => StopReason::ToolUse,
        (Some("stop"), _) => StopReason::EndTurn,
        (other, _) => {
            return Err(LlmError::invalid_response(format!(
                "OpenAI-compatible response returned unknown finish_reason `{}`",
                other.unwrap_or("missing")
            )));
        }
    };
    Ok(ChatResponse {
        content,
        usage,
        stop,
        provider_state: None,
    })
}

fn parse_openai_tool_call(call: &Value) -> Result<ChatContent, LlmError> {
    let id = call
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| {
            LlmError::invalid_response("OpenAI-compatible tool call did not include id")
        })?
        .to_string();
    let function = call.get("function").ok_or_else(|| {
        LlmError::invalid_response("OpenAI-compatible tool call did not include function")
    })?;
    let name = function
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            LlmError::invalid_response("OpenAI-compatible tool call did not include function.name")
        })?
        .to_string();
    let args_text = function
        .get("arguments")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            LlmError::invalid_response(
                "OpenAI-compatible tool call did not include string function.arguments",
            )
        })?;
    let args = serde_json::from_str(args_text).map_err(|error| {
        LlmError::invalid_response(format!(
            "OpenAI-compatible tool call arguments were invalid JSON: {error}"
        ))
    })?;
    Ok(ChatContent::ToolCall { id, name, args })
}

pub(crate) fn parse_responses_response(value: Value) -> Result<ChatResponse, LlmError> {
    let mut content = Vec::new();
    let output_items = value.get("output").and_then(Value::as_array);
    let has_message_items = output_items.is_some_and(|items| {
        items
            .iter()
            .any(|item| item.get("type").and_then(Value::as_str) == Some("message"))
    });
    if !has_message_items
        && let Some(text) = value.get("output_text").and_then(Value::as_str)
        && !text.is_empty()
    {
        content.push(ChatContent::Text(text.to_string()));
    }
    let mut refused = false;
    for item in output_items.into_iter().flatten() {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                for block in item
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    match block.get("type").and_then(Value::as_str) {
                        Some("output_text") | Some("text") => {
                            if let Some(text) = block
                                .get("text")
                                .or_else(|| block.get("content"))
                                .and_then(Value::as_str)
                                && !text.is_empty()
                            {
                                content.push(ChatContent::Text(text.to_string()));
                            }
                        }
                        Some("refusal") => {
                            if let Some(text) = block.get("refusal").and_then(Value::as_str) {
                                content.push(ChatContent::Text(text.to_string()));
                                refused = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Some("function_call") => {
                let id = item
                    .get("call_id")
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        LlmError::invalid_response(
                            "Responses API function_call did not include call_id",
                        )
                    })?
                    .to_string();
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        LlmError::invalid_response(
                            "Responses API function_call did not include name",
                        )
                    })?
                    .to_string();
                let args_text = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        LlmError::invalid_response(
                            "Responses API function_call did not include string arguments",
                        )
                    })?;
                let args = serde_json::from_str(args_text).map_err(|error| {
                    LlmError::invalid_response(format!(
                        "Responses API function_call arguments were invalid JSON: {error}"
                    ))
                })?;
                content.push(ChatContent::ToolCall { id, name, args });
            }
            _ => {}
        }
    }
    if content.is_empty() {
        return Err(LlmError::invalid_response(
            "Responses API response did not include text or tool calls",
        ));
    }
    let usage = TokenUsage {
        input: required_usage(&value, "/usage/input_tokens")?,
        output: required_usage(&value, "/usage/output_tokens")?,
        reasoning: value
            .pointer("/usage/output_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        cached_input: value
            .pointer("/usage/input_tokens_details/cached_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    };
    let stop = match value.get("status").and_then(Value::as_str) {
        Some("incomplete") => match value
            .pointer("/incomplete_details/reason")
            .and_then(Value::as_str)
        {
            Some("max_output_tokens") => StopReason::MaxTokens,
            Some("content_filter") => StopReason::Refusal,
            other => {
                return Err(LlmError::invalid_response(format!(
                    "Responses API returned unknown incomplete reason `{}`",
                    other.unwrap_or("missing")
                )));
            }
        },
        Some("completed") if refused => StopReason::Refusal,
        Some("completed")
            if content
                .iter()
                .any(|item| matches!(item, ChatContent::ToolCall { .. })) =>
        {
            StopReason::ToolUse
        }
        Some("completed") => StopReason::EndTurn,
        other => {
            return Err(LlmError::invalid_response(format!(
                "Responses API returned unsupported status `{}`",
                other.unwrap_or("missing")
            )));
        }
    };
    let provider_state = matches!(stop, StopReason::ToolUse)
        .then(|| value.get("output").cloned())
        .flatten();
    Ok(ChatResponse {
        content,
        usage,
        stop,
        provider_state,
    })
}

pub(crate) fn parse_anthropic_response(value: Value) -> Result<ChatResponse, LlmError> {
    let blocks = value
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            LlmError::invalid_response("Anthropic response did not include content array")
        })?;
    let mut content = Vec::new();
    let mut structured_response = false;
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = block.get("text").and_then(Value::as_str)
                    && !text.is_empty()
                {
                    content.push(ChatContent::Text(text.to_string()));
                }
            }
            Some("tool_use") => {
                let id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        LlmError::invalid_response("Anthropic tool_use did not include id")
                    })?
                    .to_string();
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        LlmError::invalid_response("Anthropic tool_use did not include name")
                    })?
                    .to_string();
                let args = block.get("input").cloned().unwrap_or_else(|| json!({}));
                if name == "qcg_response" {
                    structured_response = true;
                    content.push(ChatContent::Text(args.to_string()));
                } else {
                    content.push(ChatContent::ToolCall { id, name, args });
                }
            }
            _ => {}
        }
    }
    if content.is_empty() {
        return Err(LlmError::invalid_response(
            "Anthropic response did not include text or tool calls",
        ));
    }
    let usage = TokenUsage {
        input: required_usage(&value, "/usage/input_tokens")?,
        output: required_usage(&value, "/usage/output_tokens")?,
        reasoning: 0,
        cached_input: value
            .get("usage")
            .and_then(|usage| usage.get("cache_read_input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    };
    let stop = match value.get("stop_reason").and_then(Value::as_str) {
        Some("tool_use") => StopReason::ToolUse,
        Some("max_tokens") => StopReason::MaxTokens,
        Some("refusal") => StopReason::Refusal,
        Some("end_turn") | Some("stop_sequence") => StopReason::EndTurn,
        other => {
            return Err(LlmError::invalid_response(format!(
                "Anthropic response returned unsupported stop_reason `{}`",
                other.unwrap_or("missing")
            )));
        }
    };
    if structured_response
        && content
            .iter()
            .any(|item| matches!(item, ChatContent::ToolCall { .. }))
    {
        return Err(LlmError::invalid_response(
            "Anthropic response mixed qcg_response with external tool calls",
        ));
    }
    let stop = if structured_response {
        if stop != StopReason::ToolUse {
            return Err(LlmError::invalid_response(
                "Anthropic response returned qcg_response without tool_use stop_reason",
            ));
        }
        StopReason::EndTurn
    } else {
        stop
    };
    Ok(ChatResponse {
        content,
        usage,
        stop,
        provider_state: None,
    })
}

fn required_usage(value: &Value, pointer: &str) -> Result<u64, LlmError> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            LlmError::invalid_response(format!(
                "LLM provider response did not include integer `{pointer}`"
            ))
        })
}

pub(crate) fn llm_http_error(error: reqwest::Error) -> LlmError {
    if error.is_timeout() {
        return LlmError {
            message: "LLM provider request timed out".into(),
            kind: LlmErrorKind::TimedOut,
        };
    }
    if error.is_decode() {
        return LlmError {
            message: "LLM provider response could not be decoded".into(),
            kind: LlmErrorKind::InvalidResponse,
        };
    }
    LlmError {
        message: "LLM provider request failed".into(),
        kind: LlmErrorKind::Network,
    }
}

pub(crate) fn marker_line(prompt: &str, marker: &str) -> Option<String> {
    prompt
        .lines()
        .find_map(|line| line.trim().strip_prefix(marker).map(str::trim))
        .map(ToOwned::to_owned)
}

pub(crate) fn marker_block(prompt: &str, marker: &str) -> Option<String> {
    let mut lines = prompt.lines();
    while let Some(line) = lines.next() {
        if let Some(first) = line.trim().strip_prefix(marker).map(str::trim) {
            let mut value = first.to_owned();
            let rest = lines.collect::<Vec<_>>().join("\n");
            if !rest.trim().is_empty() {
                if !value.is_empty() {
                    value.push('\n');
                }
                value.push_str(&rest);
            }
            return Some(value);
        }
    }
    None
}
