use model::{StructuredOutputMode, ToolChoice, ToolChoiceMode};
use serde_json::Value;

use crate::payload::{
    native_response_schema, native_schema_syntax_compatible, strict_schema_syntax_compatible,
};
use crate::provider::ApiFlavor;
use crate::types::{Capabilities, ChatContentPart, ChatRequest, LlmError};

pub(crate) fn validate_chat_request(req: &ChatRequest, api: ApiFlavor) -> Result<(), LlmError> {
    reject_if(
        api == ApiFlavor::SystemOne,
        "system_one requires llm.decide, not a chat request",
    )?;
    reject_if(req.max_tokens == 0, "max_tokens must be greater than zero")?;
    reject_if(
        req.temperature
            .is_some_and(|value| !value.is_finite() || !(0.0..=2.0).contains(&value)),
        "temperature must be finite and between 0 and 2",
    )?;
    reject_if(
        req.top_p
            .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value)),
        "top_p must be finite and between 0 and 1",
    )?;
    reject_if(
        req.temperature.is_some() && req.top_p.is_some(),
        "temperature and top_p are mutually exclusive",
    )?;
    reject_if(
        req.reasoning_effort.is_some() && (req.temperature.is_some() || req.top_p.is_some()),
        "temperature and top_p must be omitted when reasoning_effort is set",
    )?;
    reject_if(
        req.reasoning_effort.is_some() && req.seed.is_some(),
        "seed must be omitted when reasoning_effort is set",
    )?;
    reject_if(
        req.stop_sequences.len() > 8
            || req
                .stop_sequences
                .iter()
                .any(|sequence| sequence.is_empty() || sequence.len() > 1_024),
        "stop_sequences must contain at most 8 non-empty strings of at most 1024 bytes",
    )?;
    reject_if(
        (req.tool_choice.is_some() || req.parallel_tool_calls.is_some()) && req.tools.is_empty(),
        "tool_choice and parallel_tool_calls require at least one tool",
    )?;
    if let Some(ToolChoice::Tool { tool }) = &req.tool_choice
        && (tool.trim().is_empty() || !req.tools.iter().any(|candidate| candidate.name == *tool))
    {
        return Err(LlmError::new(
            "tool_choice.tool must name one of the request tools",
        ));
    }
    reject_if(
        api == ApiFlavor::AnthropicMessages
            && req.response_schema.is_some()
            && req.structured_output != StructuredOutputMode::Prompt
            && req
                .tool_choice
                .as_ref()
                .is_some_and(|choice| !matches!(choice, ToolChoice::Mode(ToolChoiceMode::Auto))),
        "Anthropic native structured output owns tool_choice",
    )?;
    if let Some(schema) = &req.response_schema {
        policy::compile_bounded_validator(schema)
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
        reject_if(tool.name.trim().is_empty(), "tool name must not be empty")?;
        reject_if(
            tool.name == "response",
            "tool name `response` is reserved for structured output",
        )?;
        if !tool_names.insert(tool.name.as_str()) {
            return Err(LlmError::new(format!(
                "duplicate tool name `{}`",
                tool.name
            )));
        }
        validate_tool_input_schema(&tool.name, &tool.input_schema, api)?;
    }
    for message in &req.messages {
        reject_if(
            message.provider_state.is_some() && api != ApiFlavor::Responses,
            "provider state messages are only valid for the Responses API",
        )?;
        reject_if(
            message.role == "tool" && message.tool_call_id.as_deref().is_none_or(str::is_empty),
            "tool result is missing its tool call id",
        )?;
        reject_if(
            !message.parts.is_empty() && message.role != "user",
            "multimodal content parts are only valid on user messages",
        )?;
        for part in &message.parts {
            let (media_type, data) = match part {
                ChatContentPart::Text { text } => {
                    reject_if(text.is_empty(), "text content parts must not be empty")?;
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
            reject_if(
                !valid_media_type(media_type) || data.is_empty(),
                "multimodal content requires a valid MIME type and non-empty base64 data",
            )?;
            if api == ApiFlavor::AnthropicMessages
                && matches!(part, ChatContentPart::InputAudio { .. })
            {
                return Err(LlmError::new(
                    "Anthropic Messages does not support audio input content parts",
                ));
            }
        }
        if !message.tool_calls.is_empty() {
            reject_if(
                message.role != "assistant",
                "tool calls must be carried by an assistant message",
            )?;
            reject_if(
                message
                    .tool_calls
                    .iter()
                    .any(|call| call.id.is_empty() || call.name.is_empty()),
                "assistant tool calls must include non-empty ids and names",
            )?;
        }
    }
    Ok(())
}

fn validate_tool_input_schema(name: &str, schema: &Value, api: ApiFlavor) -> Result<(), LlmError> {
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err(LlmError::new(format!(
            "tool `{name}` input_schema root must have type `object`"
        )));
    }
    if api == ApiFlavor::AnthropicMessages
        && let Some(object) = schema.as_object()
        && let Some(keyword) = ["oneOf", "anyOf", "allOf"]
            .iter()
            .find(|keyword| object.contains_key(**keyword))
    {
        return Err(LlmError::new(format!(
            "tool `{name}` input_schema uses top-level `{keyword}` which Anthropic input_schema rejects"
        )));
    }
    // Bounds, external-reference ban, and compilation all run through the
    // shared policy choke point; this keeps tool schemas under the same
    // limits as every other untrusted schema.
    policy::compile_bounded_validator(schema).map_err(|error| {
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
    reject_if(
        uses_native && !capabilities.json_schema,
        "provider does not support native structured output",
    )?;
    if req.tools.is_empty() {
        return Ok(());
    }
    reject_if(
        uses_native && !capabilities.structured_output_with_tools,
        "provider does not support native structured output with external tools",
    )?;
    reject_if(
        uses_native && api == ApiFlavor::AnthropicMessages,
        "Anthropic Messages cannot force response while external tools are available",
    )?;
    Ok(())
}

pub(crate) fn validate_multimodal_capabilities(
    req: &ChatRequest,
    capabilities: &Capabilities,
) -> Result<(), LlmError> {
    for part in req.messages.iter().flat_map(|message| &message.parts) {
        let (supported, capability) = match part {
            ChatContentPart::Text { .. } => (true, "text_input"),
            ChatContentPart::InputImage { .. } => (capabilities.image_input, "image_input"),
            ChatContentPart::InputAudio { .. } => (capabilities.audio_input, "audio_input"),
            ChatContentPart::InputFile { .. } => (capabilities.file_input, "file_input"),
        };
        if !supported {
            return Err(LlmError::new(format!(
                "provider `{}` does not advertise support for `{capability}`",
                req.provider
            )));
        }
    }
    Ok(())
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

/// Revalidate the unchanged request on every declared route. Do not strip an
/// unsupported parameter to make a fallback work. API-specific schema/payload
/// validation remains the provider's responsibility.
pub fn validate_route_capabilities(
    provider: &dyn crate::LlmProvider,
    req: &ChatRequest,
) -> Result<(), LlmError> {
    if let Some(error) = provider.configuration_error_for(&req.provider) {
        return Err(LlmError::new(error));
    }
    let capabilities = provider
        .model_capabilities_for(&req.provider, &req.model)
        .ok_or_else(|| {
            LlmError::new(format!("LLM provider `{}` is not registered", req.provider))
        })?;
    reject_if(
        req.cache_retention.is_some()
            && provider
                .prompt_cache_retention_for(&req.provider, &req.model)
                .is_none(),
        "LLM route does not declare extended cache retention",
    )?;
    validate_request_capabilities(req, &capabilities)
}

pub(crate) fn validate_request_capabilities(
    req: &ChatRequest,
    capabilities: &Capabilities,
) -> Result<(), LlmError> {
    validate_multimodal_capabilities(req, capabilities)?;
    let native = native_response_schema(req).is_some();
    for (required, supported, name) in [
        (!req.tools.is_empty(), capabilities.tool_use, "tool_use"),
        (native, capabilities.json_schema, "json_schema"),
        (
            native && !req.tools.is_empty(),
            capabilities.structured_output_with_tools,
            "structured_output_with_tools",
        ),
        (req.seed.is_some(), capabilities.seed, "seed"),
        (req.stream, capabilities.streaming, "streaming"),
        (
            req.temperature.is_some(),
            capabilities.temperature,
            "temperature",
        ),
        (req.top_p.is_some(), capabilities.top_p, "top_p"),
        (
            !req.stop_sequences.is_empty(),
            capabilities.stop_sequences,
            "stop_sequences",
        ),
        (
            req.tool_choice.is_some(),
            capabilities.tool_choice,
            "tool_choice",
        ),
        (
            req.parallel_tool_calls.is_some(),
            capabilities.parallel_tool_calls,
            "parallel_tool_calls",
        ),
        (req.verbosity.is_some(), capabilities.verbosity, "verbosity"),
        (
            req.prompt_cache == crate::PromptCache::Auto,
            capabilities.prompt_cache,
            "prompt_cache",
        ),
        (
            req.provider_web_search.is_some(),
            capabilities.web_search,
            "web_search",
        ),
    ] {
        if required && !supported {
            return Err(LlmError::new(format!(
                "LLM route `{}/{}` does not support {name}",
                req.provider, req.model
            )));
        }
    }
    if let Some(effort) = req.reasoning_effort
        && !capabilities.reasoning_effort.contains(&effort)
    {
        return Err(LlmError::new(format!(
            "LLM route `{}/{}` does not support reasoning_effort `{effort}`",
            req.provider, req.model
        )));
    }
    Ok(())
}

// Keep validation order at the call sites; only the refusal mechanics are shared.
fn reject_if(invalid: bool, message: &str) -> Result<(), LlmError> {
    if invalid {
        Err(LlmError::new(message))
    } else {
        Ok(())
    }
}
