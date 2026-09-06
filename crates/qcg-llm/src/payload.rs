use qcg_types::{StructuredOutputMode, ToolChoice, ToolChoiceMode};
use serde_json::{Value, json};

use crate::provider::ChatTokenLimitField;
use crate::types::{ChatContentPart, ChatMessage, ChatRequest, ImageDetail, ToolSpec};

pub(crate) fn chat_completions_payload(
    req: &ChatRequest,
    send_seed: bool,
    token_limit_field: ChatTokenLimitField,
) -> Value {
    let mut payload = json!({
        "model": req.model,
        "messages": openai_messages(req),
    });
    if let Some(effort) = req.reasoning_effort {
        payload["reasoning_effort"] = json!(effort);
    }
    let token_limit_key = match token_limit_field {
        ChatTokenLimitField::MaxTokens => "max_tokens",
        ChatTokenLimitField::MaxCompletionTokens => "max_completion_tokens",
    };
    payload[token_limit_key] = json!(req.max_tokens);
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = req.top_p {
        payload["top_p"] = json!(top_p);
    }
    if !req.stop_sequences.is_empty() {
        payload["stop"] = json!(req.stop_sequences);
    }
    if send_seed && let Some(seed) = req.seed {
        payload["seed"] = json!(seed);
    }
    if let Some((schema, strict)) = native_response_schema(req) {
        payload["response_format"] = json!({
            "type": "json_schema",
            "json_schema": {
                "name": "qcg_response",
                "strict": strict,
                "schema": schema
            }
        });
    }
    if !req.tools.is_empty() {
        payload["tools"] = Value::Array(req.tools.iter().map(openai_tool).collect());
        payload["tool_choice"] = openai_chat_tool_choice(req.tool_choice.as_ref());
        if let Some(parallel) = req.parallel_tool_calls {
            payload["parallel_tool_calls"] = json!(parallel);
        }
    }
    payload
}

pub(crate) fn responses_payload(req: &ChatRequest) -> Value {
    let mut payload = json!({
        "model": req.model,
        "input": responses_input(req),
        "max_output_tokens": req.max_tokens,
        "store": false,
    });
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = req.top_p {
        payload["top_p"] = json!(top_p);
    }
    if let Some(effort) = req.reasoning_effort {
        payload["reasoning"] = json!({ "effort": effort });
        if !req.tools.is_empty() {
            payload["include"] = json!(["reasoning.encrypted_content"]);
        }
    }
    if let Some(verbosity) = req.verbosity {
        payload["text"]["verbosity"] = json!(verbosity);
    }
    if let Some((schema, strict)) = native_response_schema(req) {
        payload["text"] = json!({
            "format": {
                "type": "json_schema",
                "name": "qcg_response",
                "strict": strict,
                "schema": schema
            }
        });
    }
    if !req.tools.is_empty() {
        payload["tools"] = Value::Array(req.tools.iter().map(responses_tool).collect());
        payload["tool_choice"] = responses_tool_choice(req.tool_choice.as_ref());
        if let Some(parallel) = req.parallel_tool_calls {
            payload["parallel_tool_calls"] = json!(parallel);
        }
    }
    payload
}

pub(crate) fn anthropic_payload(req: &ChatRequest) -> Value {
    let mut tools: Vec<Value> = req.tools.iter().map(anthropic_tool).collect();
    if let Some((schema, _)) = native_response_schema(req) {
        tools.push(json!({
            "name": "qcg_response",
            "description": "Return the final structured qcg response.",
            "input_schema": schema,
        }));
    }
    let mut payload = json!({
        "model": req.model,
        "max_tokens": req.max_tokens,
        "messages": anthropic_messages(req),
    });
    if let Some(system) = &req.system {
        payload["system"] = Value::String(system.clone());
    }
    if let Some(temperature) = req.temperature {
        payload["temperature"] = json!(temperature);
    }
    if let Some(top_p) = req.top_p {
        payload["top_p"] = json!(top_p);
    }
    if !req.stop_sequences.is_empty() {
        payload["stop_sequences"] = json!(req.stop_sequences);
    }
    if !tools.is_empty() {
        payload["tools"] = Value::Array(tools);
        payload["tool_choice"] = match req.tool_choice.as_ref().unwrap_or(&ToolChoice::auto()) {
            ToolChoice::Mode(ToolChoiceMode::None) => json!({ "type": "none" }),
            ToolChoice::Mode(ToolChoiceMode::Auto) => json!({ "type": "auto" }),
            ToolChoice::Mode(ToolChoiceMode::Required) => json!({ "type": "any" }),
            ToolChoice::Tool { tool } => json!({ "type": "tool", "name": tool }),
        };
        if req.parallel_tool_calls == Some(false) {
            payload["tool_choice"]["disable_parallel_tool_use"] = json!(true);
        }
    }
    if payload["tools"]
        .as_array()
        .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == "qcg_response"))
    {
        payload["tool_choice"] = json!({ "type": "tool", "name": "qcg_response" });
    }
    payload
}

fn openai_chat_tool_choice(choice: Option<&ToolChoice>) -> Value {
    match choice.unwrap_or(&ToolChoice::auto()) {
        ToolChoice::Mode(mode) => json!(mode),
        ToolChoice::Tool { tool } => json!({
            "type": "function",
            "function": { "name": tool }
        }),
    }
}

fn responses_tool_choice(choice: Option<&ToolChoice>) -> Value {
    match choice.unwrap_or(&ToolChoice::auto()) {
        ToolChoice::Mode(mode) => json!(mode),
        ToolChoice::Tool { tool } => json!({ "type": "function", "name": tool }),
    }
}

pub(crate) fn native_response_schema(req: &ChatRequest) -> Option<(&Value, bool)> {
    let schema = req.response_schema.as_ref()?;
    match req.structured_output {
        StructuredOutputMode::Prompt => None,
        StructuredOutputMode::NativeStrict => Some((schema, true)),
        StructuredOutputMode::NativeCompatible => Some((schema, false)),
        StructuredOutputMode::Auto if native_schema_syntax_compatible(schema) => {
            Some((schema, strict_schema_syntax_compatible(schema)))
        }
        StructuredOutputMode::Auto => None,
    }
}

pub fn native_schema_compatible(schema: &Value) -> bool {
    native_schema_syntax_compatible(schema) && qcg_policy::compile_bounded_validator(schema).is_ok()
}

pub(crate) fn native_schema_syntax_compatible(schema: &Value) -> bool {
    let Value::Object(root) = schema else {
        return false;
    };
    if root.get("type").and_then(Value::as_str) != Some("object") || root.contains_key("anyOf") {
        return false;
    }
    let mut limits = NativeSchemaLimits::default();
    native_schema_node_compatible(schema, 1, &mut limits)
}

#[derive(Default)]
struct NativeSchemaLimits {
    properties: usize,
    string_chars: usize,
    enum_values: usize,
}

fn native_schema_node_compatible(
    schema: &Value,
    depth: usize,
    limits: &mut NativeSchemaLimits,
) -> bool {
    let Value::Object(object) = schema else {
        return false;
    };
    if depth > 10 {
        return false;
    }
    const SUPPORTED: &[&str] = &[
        "$defs",
        "$ref",
        "additionalProperties",
        "anyOf",
        "const",
        "description",
        "enum",
        "exclusiveMaximum",
        "exclusiveMinimum",
        "format",
        "items",
        "maximum",
        "maxItems",
        "minimum",
        "minItems",
        "multipleOf",
        "pattern",
        "properties",
        "required",
        "title",
        "type",
    ];
    if object.keys().any(|key| !SUPPORTED.contains(&key.as_str())) {
        return false;
    }
    if object.get("type").is_some_and(|value| match value {
        Value::String(kind) => !matches!(
            kind.as_str(),
            "object" | "array" | "string" | "number" | "integer" | "boolean" | "null"
        ),
        Value::Array(kinds) => {
            kinds.len() != 2
                || !kinds.iter().any(|kind| kind == "null")
                || kinds.iter().any(|kind| {
                    !kind.as_str().is_some_and(|kind| {
                        matches!(
                            kind,
                            "object"
                                | "array"
                                | "string"
                                | "number"
                                | "integer"
                                | "boolean"
                                | "null"
                        )
                    })
                })
        }
        _ => true,
    }) {
        return false;
    }
    let allows_type = |expected: &str| match object.get("type") {
        Some(Value::String(kind)) => kind == expected,
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind == expected),
        _ => false,
    };
    if (["properties", "required", "additionalProperties"]
        .iter()
        .any(|keyword| object.contains_key(*keyword))
        && !allows_type("object"))
        || (["items", "minItems", "maxItems"]
            .iter()
            .any(|keyword| object.contains_key(*keyword))
            && !allows_type("array"))
        || (["pattern", "format"]
            .iter()
            .any(|keyword| object.contains_key(*keyword))
            && !allows_type("string"))
        || ([
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
        ]
        .iter()
        .any(|keyword| object.contains_key(*keyword))
            && !allows_type("number")
            && !allows_type("integer"))
    {
        return false;
    }
    if object
        .get("properties")
        .is_some_and(|value| !value.is_object())
        || object.get("$defs").is_some_and(|value| !value.is_object())
        || object.get("required").is_some_and(|value| {
            !value
                .as_array()
                .is_some_and(|items| items.iter().all(Value::is_string))
        })
        || object
            .get("enum")
            .is_some_and(|value| !value.as_array().is_some_and(|items| !items.is_empty()))
        || object
            .get("anyOf")
            .is_some_and(|value| !value.as_array().is_some_and(|items| !items.is_empty()))
    {
        return false;
    }
    if object.get("$ref").is_some_and(|value| {
        !value
            .as_str()
            .is_some_and(|reference| reference.starts_with('#'))
    }) {
        return false;
    }
    if object.get("format").is_some_and(|value| {
        !value.as_str().is_some_and(|format| {
            matches!(
                format,
                "date-time"
                    | "time"
                    | "date"
                    | "duration"
                    | "email"
                    | "hostname"
                    | "ipv4"
                    | "ipv6"
                    | "uuid"
            )
        })
    }) {
        return false;
    }
    if let Some(properties) = object.get("properties").and_then(Value::as_object) {
        limits.properties = limits.properties.saturating_add(properties.len());
        limits.string_chars = limits.string_chars.saturating_add(
            properties
                .keys()
                .map(|name| name.chars().count())
                .sum::<usize>(),
        );
        if limits.properties > 5_000
            || !properties
                .values()
                .all(|schema| native_schema_node_compatible(schema, depth + 1, limits))
        {
            return false;
        }
    }
    if let Some(definitions) = object.get("$defs").and_then(Value::as_object) {
        limits.string_chars = limits.string_chars.saturating_add(
            definitions
                .keys()
                .map(|name| name.chars().count())
                .sum::<usize>(),
        );
        if !definitions
            .values()
            .all(|schema| native_schema_node_compatible(schema, depth + 1, limits))
        {
            return false;
        }
    }
    if let Some(values) = object.get("enum").and_then(Value::as_array) {
        limits.enum_values = limits.enum_values.saturating_add(values.len());
        let enum_string_chars = values
            .iter()
            .filter_map(Value::as_str)
            .map(|value| value.chars().count())
            .sum::<usize>();
        limits.string_chars = limits.string_chars.saturating_add(enum_string_chars);
        if limits.enum_values > 1_000 || (values.len() > 250 && enum_string_chars > 15_000) {
            return false;
        }
    }
    if let Some(value) = object.get("const").and_then(Value::as_str) {
        limits.string_chars = limits.string_chars.saturating_add(value.chars().count());
    }
    if limits.string_chars > 120_000 {
        return false;
    }
    if let Some(nested) = object.get("items")
        && !nested.is_boolean()
        && !native_schema_node_compatible(nested, depth + 1, limits)
    {
        return false;
    }
    if let Some(additional) = object.get("additionalProperties")
        && !additional.is_boolean()
    {
        return false;
    }
    if let Some(required) = object.get("required").and_then(Value::as_array)
        && let Some(properties) = object.get("properties").and_then(Value::as_object)
        && required
            .iter()
            .filter_map(Value::as_str)
            .any(|name| !properties.contains_key(name))
    {
        return false;
    }
    object
        .get("anyOf")
        .and_then(Value::as_array)
        .is_none_or(|schemas| {
            schemas
                .iter()
                .all(|schema| native_schema_node_compatible(schema, depth + 1, limits))
        })
}

pub fn strict_schema_compatible(schema: &Value) -> bool {
    if !native_schema_compatible(schema) {
        return false;
    }
    strict_schema_node_compatible(schema)
}

pub(crate) fn strict_schema_syntax_compatible(schema: &Value) -> bool {
    native_schema_syntax_compatible(schema) && strict_schema_node_compatible(schema)
}

fn strict_schema_node_compatible(schema: &Value) -> bool {
    let object = schema
        .as_object()
        .expect("native-compatible schema is an object");
    let object_is_closed = !schema_allows_object(object)
        || (object.get("additionalProperties").and_then(Value::as_bool) == Some(false)
            && object
                .get("properties")
                .and_then(Value::as_object)
                .is_none_or(|properties| {
                    let required = object
                        .get("required")
                        .and_then(Value::as_array)
                        .map(|required| {
                            required
                                .iter()
                                .filter_map(Value::as_str)
                                .collect::<std::collections::BTreeSet<_>>()
                        })
                        .unwrap_or_default();
                    properties
                        .keys()
                        .all(|name| required.contains(name.as_str()))
                }));
    object_is_closed
        && object
            .get("properties")
            .and_then(Value::as_object)
            .is_none_or(|properties| properties.values().all(strict_schema_node_compatible))
        && object
            .get("$defs")
            .and_then(Value::as_object)
            .is_none_or(|definitions| definitions.values().all(strict_schema_node_compatible))
        && ["items", "additionalProperties"].iter().all(|keyword| {
            object
                .get(*keyword)
                .is_none_or(|nested| nested.is_boolean() || strict_schema_node_compatible(nested))
        })
        && object
            .get("anyOf")
            .and_then(Value::as_array)
            .is_none_or(|schemas| schemas.iter().all(strict_schema_node_compatible))
}

fn schema_allows_object(schema: &serde_json::Map<String, Value>) -> bool {
    match schema.get("type") {
        Some(Value::String(kind)) => kind == "object",
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind == "object"),
        _ => schema.contains_key("properties"),
    }
}

pub(crate) fn openai_messages(req: &ChatRequest) -> Vec<Value> {
    let mut messages = Vec::new();
    if let Some(system) = &req.system {
        messages.push(json!({ "role": "system", "content": system }));
    }
    messages.extend(req.messages.iter().map(|message| {
        if !message.tool_calls.is_empty() {
            let tool_calls: Vec<Value> = message
                .tool_calls
                .iter()
                .map(|call| {
                    json!({
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.name,
                            "arguments": call.args.to_string(),
                        }
                    })
                })
                .collect();
            json!({
                "role": "assistant",
                "content": if message.content.is_empty() { Value::Null } else { json!(message.content) },
                "tool_calls": tool_calls,
            })
        } else if message.role == "tool" {
            json!({
                "role": "tool",
                "tool_call_id": message.tool_call_id,
                "content": message.content,
            })
        } else {
            json!({ "role": message.role, "content": openai_message_content(message) })
        }
    }));
    messages
}

fn openai_message_content(message: &ChatMessage) -> Value {
    if message.parts.is_empty() {
        return Value::String(message.content.clone());
    }
    let mut parts = Vec::new();
    if !message.content.is_empty() {
        parts.push(json!({ "type": "text", "text": message.content }));
    }
    parts.extend(message.parts.iter().map(|part| match part {
        ChatContentPart::Text { text } => json!({ "type": "text", "text": text }),
        ChatContentPart::InputImage {
            media_type,
            data,
            detail,
        } => json!({
            "type": "image_url",
            "image_url": {
                "url": data_url(media_type, data),
                "detail": detail.unwrap_or(ImageDetail::Auto),
            }
        }),
        ChatContentPart::InputAudio { media_type, data } => json!({
            "type": "input_audio",
            "input_audio": {
                "data": data,
                "format": media_subtype(media_type),
            }
        }),
        ChatContentPart::InputFile {
            media_type,
            data,
            filename,
        } => json!({
            "type": "file",
            "file": {
                "filename": filename,
                "file_data": data_url(media_type, data),
            }
        }),
    }));
    Value::Array(parts)
}

pub(crate) fn openai_tool(tool: &ToolSpec) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.input_schema
        }
    })
}

pub(crate) fn responses_input(req: &ChatRequest) -> Vec<Value> {
    let mut messages = Vec::new();
    if let Some(system) = &req.system {
        messages.push(json!({ "role": "system", "content": system }));
    }
    for message in &req.messages {
        if let Some(state) = &message.provider_state {
            messages.extend(state.iter().cloned());
        } else if !message.tool_calls.is_empty() {
            messages.extend(message.tool_calls.iter().map(|call| {
                json!({
                    "type": "function_call",
                    "call_id": call.id,
                    "name": call.name,
                    "arguments": call.args.to_string(),
                })
            }));
        } else if message.role == "tool" {
            messages.push(json!({
                "type": "function_call_output",
                "call_id": message.tool_call_id,
                "output": message.content,
            }));
        } else {
            messages.push(json!({
                "role": message.role,
                "content": responses_message_content(message),
            }));
        }
    }
    messages
}

fn responses_message_content(message: &ChatMessage) -> Value {
    if message.parts.is_empty() {
        return Value::String(message.content.clone());
    }
    let mut parts = Vec::new();
    if !message.content.is_empty() {
        parts.push(json!({ "type": "input_text", "text": message.content }));
    }
    parts.extend(message.parts.iter().map(|part| match part {
        ChatContentPart::Text { text } => json!({ "type": "input_text", "text": text }),
        ChatContentPart::InputImage {
            media_type,
            data,
            detail,
        } => json!({
            "type": "input_image",
            "image_url": data_url(media_type, data),
            "detail": detail.unwrap_or(ImageDetail::Auto),
        }),
        ChatContentPart::InputAudio { media_type, data } => json!({
            "type": "input_audio",
            "input_audio": {
                "data": data,
                "format": media_subtype(media_type),
            }
        }),
        ChatContentPart::InputFile {
            media_type,
            data,
            filename,
        } => json!({
            "type": "input_file",
            "file_data": data_url(media_type, data),
            "filename": filename,
        }),
    }));
    Value::Array(parts)
}

fn responses_tool(tool: &ToolSpec) -> Value {
    json!({
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": tool.input_schema,
    })
}

pub(crate) fn anthropic_messages(req: &ChatRequest) -> Vec<Value> {
    req.messages
        .iter()
        .filter_map(|message| match message.role.as_str() {
            "system" => None,
            "tool" => Some(json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": message.tool_call_id,
                    "content": message.content,
                }]
            })),
            "assistant" if !message.tool_calls.is_empty() => {
                let mut content = Vec::new();
                if !message.content.is_empty() {
                    content.push(json!({ "type": "text", "text": message.content }));
                }
                content.extend(message.tool_calls.iter().map(|call| {
                    json!({
                        "type": "tool_use",
                        "id": call.id,
                        "name": call.name,
                        "input": call.args,
                    })
                }));
                Some(json!({ "role": "assistant", "content": content }))
            }
            role => Some(json!({
                "role": role,
                "content": anthropic_message_content(message),
            })),
        })
        .collect()
}

fn anthropic_message_content(message: &ChatMessage) -> Value {
    if message.parts.is_empty() {
        return Value::String(message.content.clone());
    }
    let mut parts = Vec::new();
    if !message.content.is_empty() {
        parts.push(json!({ "type": "text", "text": message.content }));
    }
    parts.extend(message.parts.iter().map(|part| match part {
        ChatContentPart::Text { text } => json!({ "type": "text", "text": text }),
        ChatContentPart::InputImage {
            media_type, data, ..
        } => json!({
            "type": "image",
            "source": { "type": "base64", "media_type": media_type, "data": data }
        }),
        ChatContentPart::InputFile {
            media_type,
            data,
            filename,
        } => json!({
            "type": "document",
            "title": filename,
            "source": { "type": "base64", "media_type": media_type, "data": data }
        }),
        ChatContentPart::InputAudio { .. } => json!({
            "type": "text",
            "text": "[QCG_UNSUPPORTED_AUDIO_INPUT]"
        }),
    }));
    Value::Array(parts)
}

fn data_url(media_type: &str, data: &str) -> String {
    format!("data:{media_type};base64,{data}")
}

fn media_subtype(media_type: &str) -> &str {
    media_type
        .split_once('/')
        .map(|(_, subtype)| subtype)
        .unwrap_or(media_type)
}

fn anthropic_tool(tool: &ToolSpec) -> Value {
    json!({
        "name": tool.name,
        "description": tool.description,
        "input_schema": tool.input_schema,
    })
}
