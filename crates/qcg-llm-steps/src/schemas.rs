use qcg_policy::string_schema;
use serde_json::{Value, json};

pub(crate) fn agent_tool_params_schema(
    kind: &str,
    required_fields: &[&str],
    properties: Value,
) -> Value {
    let mut required = vec![Value::String("name".into()), Value::String("kind".into())];
    required.extend(
        required_fields
            .iter()
            .map(|field| Value::String((*field).to_string())),
    );
    let mut all_properties = serde_json::Map::from_iter([
        ("name".into(), string_schema()),
        ("kind".into(), json!({ "const": kind })),
        ("description".into(), string_schema()),
    ]);
    if let Value::Object(properties) = properties {
        all_properties.extend(properties);
    }
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": all_properties,
    })
}

pub(crate) fn context_array_schema() -> Value {
    json!({
        "type": "array",
        "items": {
            "oneOf": [
                { "type": "string" },
                {
                    "type": "object",
                    "required": ["resource"],
                    "additionalProperties": false,
                    "properties": {
                        "resource": { "type": "string" },
                        "select": { "type": "string" },
                        "tag": { "type": "string" },
                        "path": { "type": "string" }
                    }
                }
            ]
        }
    })
}

pub(crate) fn model_ref_schema() -> Value {
    json!({
        "type": "object",
        "required": ["provider", "model"],
        "additionalProperties": false,
        "properties": {
            "clear": {
                "type": "array",
                "uniqueItems": true,
                "items": { "enum": ["temperature", "top_p", "stop_sequences", "seed", "reasoning_effort", "tool_choice", "parallel_tool_calls", "verbosity"] }
            },
            "provider": { "type": "string", "minLength": 1 },
            "model": { "type": "string", "minLength": 1 },
            "input_cost_per_million_usd": { "type": "number", "minimum": 0 },
            "output_cost_per_million_usd": { "type": "number", "minimum": 0 }
        }
    })
}

pub(crate) fn request_policy_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "system": string_schema(),
            "temperature": { "type": "number", "minimum": 0, "maximum": 2 },
            "top_p": { "type": "number", "minimum": 0, "maximum": 1 },
            "max_tokens": { "type": "integer", "minimum": 1 },
            "stop_sequences": {
                "type": "array",
                "maxItems": 8,
                "items": { "type": "string", "minLength": 1, "maxLength": 1024 }
            },
            "seed": { "type": "integer", "minimum": 0 },
            "reasoning_effort": { "enum": ["none", "minimal", "low", "medium", "high", "xhigh", "max"] },
            "structured_output": { "enum": ["auto", "native_strict", "native_compatible", "prompt"] },
            "tool_choice": {
                "oneOf": [
                    { "enum": ["none", "auto", "required"] },
                    {
                        "type": "object",
                        "required": ["tool"],
                        "additionalProperties": false,
                        "properties": { "tool": { "type": "string", "minLength": 1 } }
                    }
                ]
            },
            "parallel_tool_calls": { "type": "boolean" },
            "verbosity": { "enum": ["low", "medium", "high"] },
            "stream": { "type": "boolean" },
            "requires": {
                "type": "array",
                "uniqueItems": true,
                "items": { "enum": ["tool_use", "json_schema", "structured_output_with_tools", "seed", "reasoning_effort", "image_input", "audio_input", "file_input", "streaming", "temperature", "top_p", "stop_sequences", "tool_choice", "parallel_tool_calls", "verbosity"] }
            },
            "max_context_bytes": {
                "type": "integer",
                "minimum": 1
            },
            "max_context_tokens": {
                "type": "integer",
                "minimum": 1
            },
            "max_media_bytes": {
                "type": "integer",
                "minimum": 1
            },
            "context_overflow": { "enum": ["error", "truncate_head", "truncate_tail"] },
            "retry_prompt": string_schema()
        }
    })
}

pub(crate) fn agent_failure_policy_schema() -> Value {
    let codes = [
        "tool_failed",
        "guardrail_rejected",
        "token_budget_exceeded",
        "tool_call_budget_exceeded",
        "iteration_budget_exceeded",
        "validation_failed",
        "provider_failed",
    ];
    let actions = ["fail", "return_error"];
    let mut by_code = serde_json::Map::new();
    for code in codes {
        by_code.insert(code.into(), json!({ "enum": actions }));
    }
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "default": { "enum": actions },
            "by_code": {
                "type": "object",
                "additionalProperties": false,
                "properties": by_code
            }
        }
    })
}

pub(crate) fn llm_common_properties(extra: Value) -> Value {
    let mut properties = serde_json::Map::new();
    properties.insert("prompt".into(), string_schema());
    properties.insert("output_file".into(), string_schema());
    properties.insert("schema".into(), string_schema());
    properties.insert("context".into(), context_array_schema());
    properties.insert(
        "media".into(),
        json!({
            "type": "array",
            "maxItems": 16,
            "items": {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "path", "media_type"],
                "properties": {
                    "kind": { "enum": ["image", "audio", "file", "video"] },
                    "path": string_schema(),
                    "media_type": string_schema(),
                    "detail": { "enum": ["auto", "low", "high"] }
                }
            }
        }),
    );
    properties.insert("model".into(), model_ref_schema());
    properties.insert(
        "fallback_models".into(),
        json!({ "type": "array", "items": model_ref_schema(), "maxItems": 8 }),
    );
    properties.insert("request".into(), request_policy_schema());
    if let Value::Object(extra) = extra {
        for (key, value) in extra {
            properties.insert(key, value);
        }
    }
    Value::Object(properties)
}
