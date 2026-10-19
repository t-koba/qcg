//! Provider usage normalization. OpenAI reports total input with read/write
//! subsets; Anthropic reports disjoint ordinary/read/creation input counters.
use serde_json::Value;

use crate::{LlmError, TokenUsage};

pub(crate) fn count(value: &Value, field: &str, required: bool) -> Result<u64, LlmError> {
    match value.get(field) {
        None if !required => Ok(0),
        Some(value) if value.as_u64().is_some() => Ok(value.as_u64().unwrap()),
        _ => Err(LlmError::invalid_response(format!(
            "provider usage requires unsigned integer `{field}`"
        ))),
    }
}

fn detail(value: &Value, field: &str, counter: &str) -> Result<u64, LlmError> {
    match value.get(field) {
        None => Ok(0),
        Some(value) if value.is_object() => count(value, counter, false),
        _ => Err(LlmError::invalid_response(format!(
            "provider usage requires object `{field}`"
        ))),
    }
}

pub(crate) fn openai_usage(value: &Value, chat: bool) -> Result<TokenUsage, LlmError> {
    let (input, output, input_details, output_details) = if chat {
        (
            "prompt_tokens",
            "completion_tokens",
            "prompt_tokens_details",
            "completion_tokens_details",
        )
    } else {
        (
            "input_tokens",
            "output_tokens",
            "input_tokens_details",
            "output_tokens_details",
        )
    };
    let usage = TokenUsage {
        input: count(value, input, true)?,
        output: count(value, output, true)?,
        reasoning: detail(value, output_details, "reasoning_tokens")?,
        cached_input: detail(value, input_details, "cached_tokens")?,
        cache_creation_input: detail(value, input_details, "cache_write_tokens")?,
    };
    usage.ordinary_input()?;
    Ok(usage)
}

pub(crate) fn anthropic_input(value: &Value) -> Result<TokenUsage, LlmError> {
    let ordinary = count(value, "input_tokens", true)?;
    let cached_input = count(value, "cache_read_input_tokens", false)?;
    let cache_creation_input = count(value, "cache_creation_input_tokens", false)?;
    let input = ordinary
        .checked_add(cached_input)
        .and_then(|total| total.checked_add(cache_creation_input))
        .ok_or_else(|| LlmError::invalid_response("Anthropic input usage overflowed"))?;
    Ok(TokenUsage {
        input,
        cached_input,
        cache_creation_input,
        ..Default::default()
    })
}

/// Anthropic message deltas may update cumulative counters; omitted input
/// categories retain the message-start values, never get added a second time.
pub(crate) fn anthropic_update(
    value: &Value,
    previous: &TokenUsage,
) -> Result<TokenUsage, LlmError> {
    let mut counters = serde_json::json!({
        "input_tokens": previous.ordinary_input()?,
        "cache_read_input_tokens": previous.cached_input,
        "cache_creation_input_tokens": previous.cache_creation_input,
    });
    for field in [
        "input_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
    ] {
        if value.get(field).is_some() {
            counters[field] = count(value, field, true)?.into();
        }
    }
    anthropic_input(&counters)
}
