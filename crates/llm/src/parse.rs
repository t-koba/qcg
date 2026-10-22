use model::{ProviderSearchAction, ProviderSearchEvent, ProviderWebSearchOutcome, UrlCitation};
use serde_json::{Value, json};

use crate::types::{ChatContent, ChatResponse, LlmError, LlmErrorKind, StopReason};

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
    let usage = crate::usage::openai_usage(&value["usage"], true)?;
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
        web_search: None,
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

pub(crate) fn parse_responses_response(
    value: Value,
    provider_web_search: bool,
) -> Result<ChatResponse, LlmError> {
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
    // Reasoning-only token exhaustion is a documented terminal: incomplete
    // with max_output_tokens may carry no visible text or calls while still
    // reporting billed input and reasoning usage. Let that empty content
    // through so usage and the MaxTokens classification survive; every other
    // empty response stays rejected below.
    let incomplete_exhaustion = value.get("status").and_then(Value::as_str) == Some("incomplete")
        && value
            .pointer("/incomplete_details/reason")
            .and_then(Value::as_str)
            == Some("max_output_tokens");
    if content.is_empty() && !incomplete_exhaustion {
        return Err(LlmError::invalid_response(
            "Responses API response did not include text or tool calls",
        ));
    }
    let usage = crate::usage::openai_usage(&value["usage"], false)?;
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
    // A hosted search call is provider state too: the replayed output array
    // must carry it, so a resumed turn neither repeats the search nor its
    // charge.
    let hosted_call = output_items.into_iter().flatten().any(is_web_search_call);
    let provider_state = (matches!(stop, StopReason::ToolUse) || hosted_call)
        .then(|| value.get("output").cloned())
        .flatten();
    let web_search = provider_web_search_outcome(&value, provider_web_search)?;
    Ok(ChatResponse {
        content,
        usage,
        stop,
        provider_state,
        web_search,
    })
}

/// True when an output item is a hosted web search call.
pub(crate) fn is_web_search_call(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("web_search_call")
}

/// Reads the citations one message output item carries.
pub(crate) fn parse_message_citations(item: &Value) -> Result<Vec<UrlCitation>, LlmError> {
    let mut citations = Vec::new();
    for block in item
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for annotation in block
            .get("annotations")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if annotation.get("type").and_then(Value::as_str) == Some("url_citation") {
                citations.push(parse_url_citation(annotation)?);
            }
        }
    }
    Ok(citations)
}

/// Reads what the provider's hosted search did. The count is derived from
/// the reported `web_search_call` items: a response without an output array
/// did not report the count, which is unknown and never zero.
fn provider_web_search_outcome(
    value: &Value,
    provider_web_search: bool,
) -> Result<Option<ProviderWebSearchOutcome>, LlmError> {
    if !provider_web_search {
        return Ok(None);
    }
    let Some(output) = value.get("output").and_then(Value::as_array) else {
        return Ok(Some(ProviderWebSearchOutcome::default()));
    };
    let mut outcome = ProviderWebSearchOutcome {
        calls: Some(
            output
                .iter()
                .filter(|item| is_web_search_call(item))
                .count() as u64,
        ),
        ..ProviderWebSearchOutcome::default()
    };
    for item in output {
        if is_web_search_call(item) {
            outcome.events.push(parse_web_search_event(item)?);
        }
        if item.get("type").and_then(Value::as_str) == Some("message") {
            outcome.citations.extend(parse_message_citations(item)?);
        }
    }
    Ok(Some(outcome))
}

/// Reads one hosted search call. The action names its kind; a search action
/// carries the queries it ran, a page action the page it opened.
pub(crate) fn parse_web_search_event(item: &Value) -> Result<ProviderSearchEvent, LlmError> {
    let reported = item.get("action").ok_or_else(|| {
        LlmError::invalid_response("Responses API web_search_call did not include an action")
    })?;
    let kind = match reported {
        Value::String(text) => text.as_str(),
        other => other
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    };
    let action = match kind {
        "search" => ProviderSearchAction::Search,
        "open_page" => ProviderSearchAction::OpenPage,
        "find_in_page" => ProviderSearchAction::FindInPage,
        other => {
            return Err(LlmError::invalid_response(format!(
                "Responses API web_search_call returned an unknown action `{other}`"
            )));
        }
    };
    let queries = web_search_field(reported, item, "queries")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .or_else(|| {
            web_search_field(reported, item, "query")
                .and_then(Value::as_str)
                .map(|query| vec![query.to_string()])
        })
        .unwrap_or_default();
    let url = web_search_field(reported, item, "url")
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(ProviderSearchEvent {
        id: item.get("id").and_then(Value::as_str).map(str::to_string),
        action,
        queries,
        url,
    })
}

/// Reads a field of the action, falling back to the output item: the
/// provider may report the action as an object or as a bare kind.
fn web_search_field<'a>(action: &'a Value, item: &'a Value, name: &str) -> Option<&'a Value> {
    action.get(name).or_else(|| item.get(name))
}

/// Reads one citation the provider attached to its answer.
fn parse_url_citation(annotation: &Value) -> Result<UrlCitation, LlmError> {
    let url = annotation
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            LlmError::invalid_response("Responses API url_citation did not include a url")
        })?
        .to_string();
    Ok(UrlCitation {
        url,
        title: annotation
            .get("title")
            .and_then(Value::as_str)
            .map(str::to_string),
        start_index: annotation.get("start_index").and_then(Value::as_u64),
        end_index: annotation.get("end_index").and_then(Value::as_u64),
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
                if name == "response" {
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
    let mut usage = crate::usage::anthropic_input(&value["usage"])?;
    usage.output = crate::usage::count(&value["usage"], "output_tokens", true)?;
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
            "Anthropic response mixed response with external tool calls",
        ));
    }
    let stop = if structured_response {
        if stop != StopReason::ToolUse {
            return Err(LlmError::invalid_response(
                "Anthropic response returned response without tool_use stop_reason",
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
        web_search: None,
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
