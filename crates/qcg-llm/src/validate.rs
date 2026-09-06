use qcg_types::{StructuredOutputMode, ToolChoice, ToolChoiceMode};
use serde_json::Value;

use crate::payload::{
    native_response_schema, native_schema_syntax_compatible, strict_schema_syntax_compatible,
};
use crate::provider::ApiFlavor;
use crate::types::{Capabilities, ChatContentPart, ChatRequest, LlmError};
use qcg_policy::{MAX_JSON_SCHEMA_BYTES, MAX_JSON_SCHEMA_DEPTH, MAX_JSON_SCHEMA_NODES};

pub(crate) fn validate_chat_request(req: &ChatRequest, api: ApiFlavor) -> Result<(), LlmError> {
    if req.max_tokens == 0 {
        return Err(LlmError::new("max_tokens must be greater than zero"));
    }
    if req
        .temperature
        .is_some_and(|value| !value.is_finite() || !(0.0..=2.0).contains(&value))
    {
        return Err(LlmError::new(
            "temperature must be finite and between 0 and 2",
        ));
    }
    if req
        .top_p
        .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
    {
        return Err(LlmError::new("top_p must be finite and between 0 and 1"));
    }
    if req.temperature.is_some() && req.top_p.is_some() {
        return Err(LlmError::new(
            "temperature and top_p are mutually exclusive",
        ));
    }
    if req.reasoning_effort.is_some() && (req.temperature.is_some() || req.top_p.is_some()) {
        return Err(LlmError::new(
            "temperature and top_p must be omitted when reasoning_effort is set",
        ));
    }
    if req.reasoning_effort.is_some() && req.seed.is_some() {
        return Err(LlmError::new(
            "seed must be omitted when reasoning_effort is set",
        ));
    }
    if req.stop_sequences.len() > 8
        || req
            .stop_sequences
            .iter()
            .any(|sequence| sequence.is_empty() || sequence.len() > 1_024)
    {
        return Err(LlmError::new(
            "stop_sequences must contain at most 8 non-empty strings of at most 1024 bytes",
        ));
    }
    if (req.tool_choice.is_some() || req.parallel_tool_calls.is_some()) && req.tools.is_empty() {
        return Err(LlmError::new(
            "tool_choice and parallel_tool_calls require at least one tool",
        ));
    }
    if let Some(ToolChoice::Tool { tool }) = &req.tool_choice
        && (tool.trim().is_empty() || !req.tools.iter().any(|candidate| candidate.name == *tool))
    {
        return Err(LlmError::new(
            "tool_choice.tool must name one of the request tools",
        ));
    }
    if api == ApiFlavor::AnthropicMessages
        && req.response_schema.is_some()
        && req.structured_output != StructuredOutputMode::Prompt
        && req
            .tool_choice
            .as_ref()
            .is_some_and(|choice| !matches!(choice, ToolChoice::Mode(ToolChoiceMode::Auto)))
    {
        return Err(LlmError::new(
            "Anthropic native structured output owns tool_choice",
        ));
    }
    if let Some(schema) = &req.response_schema {
        qcg_policy::compile_bounded_validator(schema)
            .map_err(|error| LlmError::new(format!("response_schema is invalid: {error}")))?;
        match req.structured_output {
            StructuredOutputMode::NativeStrict if !strict_schema_syntax_compatible(schema) => {
                return Err(LlmError::new(
                    "native_strict response_schema contains unsupported keywords or is not fully closed",
                ));
            }
            StructuredOutputMode::NativeCompatible if !native_schema_syntax_compatible(schema) => {
                return Err(LlmError::new(
                    "native_compatible response_schema contains unsupported keywords",
                ));
            }
            _ => {}
        }
    }
    let mut tool_names = std::collections::BTreeSet::new();
    for tool in &req.tools {
        if tool.name.trim().is_empty() {
            return Err(LlmError::new("tool name must not be empty"));
        }
        if tool.name == "qcg_response" {
            return Err(LlmError::new(
                "tool name `qcg_response` is reserved for structured output",
            ));
        }
        if !tool_names.insert(tool.name.as_str()) {
            return Err(LlmError::new(format!(
                "duplicate tool name `{}`",
                tool.name
            )));
        }
        validate_tool_input_schema(&tool.name, &tool.input_schema)?;
    }
    for message in &req.messages {
        if message.provider_state.is_some() && api != ApiFlavor::Responses {
            return Err(LlmError::new(
                "provider state messages are only valid for the Responses API",
            ));
        }
        if message.role == "tool" && message.tool_call_id.as_deref().is_none_or(str::is_empty) {
            return Err(LlmError::new("tool result is missing its tool call id"));
        }
        if !message.parts.is_empty() && message.role != "user" {
            return Err(LlmError::new(
                "multimodal content parts are only valid on user messages",
            ));
        }
        for part in &message.parts {
            let (media_type, data) = match part {
                ChatContentPart::Text { text } => {
                    if text.is_empty() {
                        return Err(LlmError::new("text content parts must not be empty"));
                    }
                    continue;
                }
                ChatContentPart::InputImage {
                    media_type, data, ..
                }
                | ChatContentPart::InputAudio { media_type, data }
                | ChatContentPart::InputFile {
                    media_type, data, ..
                } => (media_type, data),
            };
            if !valid_media_type(media_type) || data.is_empty() {
                return Err(LlmError::new(
                    "multimodal content requires a valid MIME type and non-empty base64 data",
                ));
            }
            if api == ApiFlavor::AnthropicMessages
                && matches!(part, ChatContentPart::InputAudio { .. })
            {
                return Err(LlmError::new(
                    "Anthropic Messages does not support audio input content parts",
                ));
            }
        }
        if !message.tool_calls.is_empty() {
            if message.role != "assistant" {
                return Err(LlmError::new(
                    "tool calls must be carried by an assistant message",
                ));
            }
            if message
                .tool_calls
                .iter()
                .any(|call| call.id.is_empty() || call.name.is_empty())
            {
                return Err(LlmError::new(
                    "assistant tool calls must include non-empty ids and names",
                ));
            }
        }
    }
    Ok(())
}

fn validate_tool_input_schema(name: &str, schema: &Value) -> Result<(), LlmError> {
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err(LlmError::new(format!(
            "tool `{name}` input_schema root must have type `object`"
        )));
    }
    let size = serde_json::to_vec(schema)
        .map_err(|error| LlmError::new(format!("tool `{name}` input_schema is invalid: {error}")))?
        .len();
    if size > MAX_JSON_SCHEMA_BYTES {
        return Err(LlmError::new(format!(
            "tool `{name}` input_schema exceeds {MAX_JSON_SCHEMA_BYTES} bytes"
        )));
    }
    let mut stack = vec![(schema, 1_usize)];
    let mut nodes = 0_usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_JSON_SCHEMA_NODES || depth > MAX_JSON_SCHEMA_DEPTH {
            return Err(LlmError::new(format!(
                "tool `{name}` input_schema exceeds complexity limits"
            )));
        }
        match value {
            Value::Object(object) => {
                if object.iter().any(|(key, value)| {
                    matches!(key.as_str(), "$ref" | "$dynamicRef" | "$recursiveRef")
                        && value
                            .as_str()
                            .is_some_and(|reference| !reference.starts_with('#'))
                }) {
                    return Err(LlmError::new(format!(
                        "tool `{name}` input_schema contains an external reference"
                    )));
                }
                stack.extend(object.values().map(|value| (value, depth + 1)));
            }
            Value::Array(array) => {
                stack.extend(array.iter().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    qcg_policy::compile_bounded_validator(schema).map_err(|error| {
        LlmError::new(format!("tool `{name}` input_schema is invalid: {error}"))
    })?;
    Ok(())
}

pub(crate) fn validate_structured_output_capabilities(
    req: &ChatRequest,
    api: ApiFlavor,
    capabilities: &Capabilities,
) -> Result<(), LlmError> {
    if req.response_schema.is_none() {
        return Ok(());
    }
    let uses_native = native_response_schema(req).is_some();
    if uses_native && !capabilities.json_schema {
        return Err(LlmError::new(
            "provider does not support native structured output",
        ));
    }
    if req.tools.is_empty() {
        return Ok(());
    }
    if uses_native && !capabilities.structured_output_with_tools {
        return Err(LlmError::new(
            "provider does not support native structured output with external tools",
        ));
    }
    if uses_native && api == ApiFlavor::AnthropicMessages {
        return Err(LlmError::new(
            "Anthropic Messages cannot force qcg_response while external tools are available",
        ));
    }
    Ok(())
}

pub(crate) fn validate_multimodal_capabilities(
    req: &ChatRequest,
    capabilities: &Capabilities,
) -> Result<(), LlmError> {
    for part in req.messages.iter().flat_map(|message| &message.parts) {
        let supported = match part {
            ChatContentPart::Text { .. } => true,
            ChatContentPart::InputImage { .. } => capabilities.image_input,
            ChatContentPart::InputAudio { .. } => capabilities.audio_input,
            ChatContentPart::InputFile { .. } => capabilities.file_input,
        };
        if !supported {
            return Err(LlmError::new(format!(
                "provider `{}` does not advertise support for `{}`",
                req.provider,
                content_part_capability(part)
            )));
        }
    }
    Ok(())
}

fn content_part_capability(part: &ChatContentPart) -> &'static str {
    match part {
        ChatContentPart::Text { .. } => "text_input",
        ChatContentPart::InputImage { .. } => "image_input",
        ChatContentPart::InputAudio { .. } => "audio_input",
        ChatContentPart::InputFile { .. } => "file_input",
    }
}

fn valid_media_type(media_type: &str) -> bool {
    let Some((kind, subtype)) = media_type.split_once('/') else {
        return false;
    };
    !kind.is_empty()
        && !subtype.is_empty()
        && media_type
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'+' | b'.' | b'-'))
}
