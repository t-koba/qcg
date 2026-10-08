use super::*;
use async_trait::async_trait;
use camino::Utf8PathBuf;
use model::{ReasoningEffort, ResponseVerbosity, StructuredOutputMode, ToolChoice};
use policy::credential_like_name;
use serde_json::json;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::mpsc;

fn sample_request() -> ChatRequest {
    ChatRequest {
        provider: "openai".into(),
        model: "gpt-test".into(),
        system: Some("system".into()),
        messages: vec![ChatMessage::text("user", "hello")],
        tools: vec![],
        response_schema: None,
        structured_output: StructuredOutputMode::Auto,
        temperature: Some(0.5),
        top_p: None,
        max_tokens: 128,
        stop_sequences: vec![],
        seed: Some(42),
        reasoning_effort: None,
        tool_choice: None,
        parallel_tool_calls: None,
        verbosity: None,
        stream: false,
        prompt_cache: PromptCache::Off,
    }
}

fn spawn_http_response(status: u16, body: String, headers: String) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("test listener should have an address");
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("test request should arrive");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        let response = format!(
            "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
    });
    (format!("http://{address}"), handle)
}

/// A `providers.toml` document holding exactly one `[[provider]]` row,
/// assembled from the fields the case under test varies. `extra` carries
/// the remaining keys - `capabilities`, retry bounds, `[[provider.models]]`
/// sections - and is either empty or newline-terminated. The reserved
/// `example.invalid` host keeps a row offline: no case here may reach
/// the network.
fn test_provider_row(id: &str, api: &str, base_url: &str, extra: &str) -> String {
    format!("\n[[provider]]\nid = \"{id}\"\napi = \"{api}\"\nbase_url = \"{base_url}\"\n{extra}")
}

#[test]
fn parses_chat_completions_text_response() {
    let response = parse_chat_completions_response(json!({
        "choices": [{
            "message": { "content": "hello" },
            "finish_reason": "stop"
        }],
        "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
    }))
    .expect("response should parse");

    assert_eq!(response.usage.input, 3);
    assert_eq!(response.usage.output, 2);
    assert!(matches!(response.stop, StopReason::EndTurn));
    assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "hello"));
}

#[test]
fn parses_chat_completions_tool_call_response() {
    let response = parse_chat_completions_response(json!({
        "choices": [{
            "message": {
                "content": null,
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {
                        "name": "write_draft",
                        "arguments": "{\"path\":\"drafts/result.txt\",\"content\":\"ok\"}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": { "prompt_tokens": 10, "completion_tokens": 4 }
    }))
    .expect("response should parse");

    assert!(matches!(response.stop, StopReason::ToolUse));
    match &response.content[0] {
        ChatContent::ToolCall { id, name, args } => {
            assert_eq!(id, "call_1");
            assert_eq!(name, "write_draft");
            assert_eq!(args["path"], "drafts/result.txt");
        }
        other => panic!("expected tool call, got {other:?}"),
    }
}

#[test]
fn formats_openai_tools() {
    let tool = openai_tool(&ToolSpec {
        name: "fetch".into(),
        description: "fetch something".into(),
        input_schema: json!({ "type": "object" }),
    });
    assert_eq!(tool["type"], "function");
    assert_eq!(tool["function"]["name"], "fetch");
    assert_eq!(tool["function"]["parameters"]["type"], "object");
}

#[test]
fn tool_continuations_keep_provider_call_ids() {
    let call = ChatToolCall {
        id: "call_1".into(),
        name: "fetch".into(),
        args: json!({ "key": "value" }),
    };
    let mut request = sample_request();
    request.messages = vec![
        ChatMessage::assistant_tool_calls("", vec![call.clone()]),
        ChatMessage::tool_result("call_1", "{\"ok\":true}"),
    ];

    let chat = openai_messages(&request);
    assert_eq!(chat[1]["tool_calls"][0]["id"], "call_1");
    assert_eq!(chat[2]["tool_call_id"], "call_1");

    let responses = responses_input(&request);
    assert_eq!(responses[1]["call_id"], "call_1");
    assert_eq!(responses[2]["type"], "function_call_output");
    assert_eq!(responses[2]["call_id"], "call_1");

    let anthropic = anthropic_messages(&request);
    assert_eq!(anthropic[0]["content"][0]["id"], "call_1");
    assert_eq!(anthropic[1]["content"][0]["tool_use_id"], "call_1");
}

#[test]
fn responses_continuation_preserves_reasoning_state() {
    let mut request = sample_request();
    request.system = None;
    request.messages = vec![
        ChatMessage::text("user", "hello"),
        ChatMessage::provider_state(vec![
            json!({ "type": "reasoning", "encrypted_content": "opaque" }),
            json!({ "type": "function_call", "call_id": "call_1", "name": "fetch", "arguments": "{}" }),
        ]),
        ChatMessage::tool_result("call_1", "done"),
    ];

    let input = responses_input(&request);

    assert_eq!(input[0]["role"], "user");
    assert_eq!(input[1]["type"], "reasoning");
    assert_eq!(input[2]["type"], "function_call");
    assert_eq!(input[3]["type"], "function_call_output");
    assert_eq!(input[3]["call_id"], "call_1");
}

#[test]
fn chat_completions_payload_gates_seed_by_capability() {
    let request = sample_request();

    let payload = chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens, None);
    assert_eq!(payload["seed"], 42);

    let payload = chat_completions_payload(&request, false, ChatTokenLimitField::MaxTokens, None);
    assert!(payload.get("seed").is_none());
}

#[test]
fn chat_completions_payload_omits_null_fields_and_includes_schema_and_tools() {
    let mut request = sample_request();
    request.temperature = None;
    request.response_schema = Some(json!({ "type": "object" }));
    request.tools = vec![ToolSpec {
        name: "fetch".into(),
        description: "fetch".into(),
        input_schema: json!({ "type": "object" }),
    }];

    let payload = chat_completions_payload(&request, false, ChatTokenLimitField::MaxTokens, None);

    assert!(payload.get("temperature").is_none());
    assert_eq!(payload["max_tokens"], 128);
    assert_eq!(payload["response_format"]["type"], "json_schema");
    assert_eq!(payload["response_format"]["json_schema"]["strict"], false);
    assert_eq!(payload["tool_choice"], "auto");
    assert_eq!(payload["messages"][0]["role"], "system");
}

#[test]
fn structured_output_mode_selects_strict_compatible_or_prompt_transport() {
    let mut request = sample_request();
    request.response_schema = Some(json!({
        "type": "object",
        "additionalProperties": false,
        "properties": { "answer": { "type": "string" } },
        "required": ["answer"]
    }));
    let auto = chat_completions_payload(
        &request,
        false,
        ChatTokenLimitField::MaxCompletionTokens,
        None,
    );
    assert_eq!(auto["response_format"]["json_schema"]["strict"], true);

    request.structured_output = StructuredOutputMode::NativeCompatible;
    let compatible = responses_payload(&request, None);
    assert_eq!(compatible["text"]["format"]["strict"], false);

    request.structured_output = StructuredOutputMode::Prompt;
    let prompt = chat_completions_payload(
        &request,
        false,
        ChatTokenLimitField::MaxCompletionTokens,
        None,
    );
    assert!(prompt.get("response_format").is_none());
    let anthropic = anthropic_payload(&request, None);
    assert!(anthropic.get("tool_choice").is_none());
}

#[test]
fn auto_matches_explicit_native_transport_or_fail_closed() {
    let strict_closed = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": { "answer": { "type": "string" } },
        "required": ["answer"]
    });
    let mut auto_req = sample_request();
    auto_req.temperature = None;
    auto_req.seed = None;
    auto_req.response_schema = Some(strict_closed.clone());
    auto_req.structured_output = StructuredOutputMode::Auto;
    let mut strict_req = auto_req.clone();
    strict_req.structured_output = StructuredOutputMode::NativeStrict;

    for api in [
        ApiFlavor::ChatCompletions,
        ApiFlavor::Responses,
        ApiFlavor::AnthropicMessages,
    ] {
        validate_chat_request(&auto_req, api).expect("auto strict-closed should validate");
        validate_chat_request(&strict_req, api).expect("explicit strict-closed should validate");
    }
    assert_eq!(
        native_response_schema(&auto_req),
        native_response_schema(&strict_req),
        "auto must select the same native transport as explicit"
    );
    assert_eq!(
        native_response_schema(&auto_req),
        Some((&strict_closed, true))
    );
    assert_eq!(
        chat_completions_payload(
            &auto_req,
            false,
            ChatTokenLimitField::MaxCompletionTokens,
            None
        )["response_format"],
        chat_completions_payload(
            &strict_req,
            false,
            ChatTokenLimitField::MaxCompletionTokens,
            None
        )["response_format"]
    );
    assert_eq!(
        responses_payload(&auto_req, None)["text"],
        responses_payload(&strict_req, None)["text"]
    );
    let auto_anthropic = anthropic_payload(&auto_req, None);
    let strict_anthropic = anthropic_payload(&strict_req, None);
    assert_eq!(auto_anthropic["tools"], strict_anthropic["tools"]);
    assert_eq!(
        auto_anthropic["tool_choice"],
        strict_anthropic["tool_choice"]
    );
    assert_eq!(auto_anthropic["tool_choice"]["name"], "response");

    // Native-incompatible schema: auto falls back to prompt while explicit
    // fails closed before transport.
    let open_schema = json!({
        "type": "object",
        "anyOf": [{ "type": "object", "additionalProperties": false }]
    });
    let mut auto_open = sample_request();
    auto_open.temperature = None;
    auto_open.seed = None;
    auto_open.response_schema = Some(open_schema.clone());
    auto_open.structured_output = StructuredOutputMode::Auto;
    let mut strict_open = auto_open.clone();
    strict_open.structured_output = StructuredOutputMode::NativeStrict;

    validate_chat_request(&auto_open, ApiFlavor::ChatCompletions)
        .expect("auto must fall back to prompt validation for incompatible schema");
    assert!(
        native_response_schema(&auto_open).is_none(),
        "auto must not select native transport for incompatible schema"
    );
    assert!(
        chat_completions_payload(
            &auto_open,
            false,
            ChatTokenLimitField::MaxCompletionTokens,
            None
        )
        .get("response_format")
        .is_none()
    );
    assert!(
        anthropic_payload(&auto_open, None)["tools"]
            .as_array()
            .is_none_or(|tools| tools.iter().all(|tool| tool["name"] != "response"))
    );
    let error = validate_chat_request(&strict_open, ApiFlavor::ChatCompletions)
        .expect_err("explicit native_strict must reject incompatible schema");
    assert!(
        error.to_string().contains("unsupported keywords")
            || error.to_string().contains("fully closed"),
        "{error}"
    );

    // Anthropic owns tool_choice on both paths.
    for mode in [
        StructuredOutputMode::Auto,
        StructuredOutputMode::NativeStrict,
    ] {
        let mut owned = sample_request();
        owned.temperature = None;
        owned.seed = None;
        owned.response_schema = Some(strict_closed.clone());
        owned.structured_output = mode;
        owned.tools = vec![ToolSpec {
            name: "lookup".into(),
            description: "lookup".into(),
            input_schema: json!({ "type": "object" }),
        }];
        owned.tool_choice = Some(ToolChoice::required());
        let error = validate_chat_request(&owned, ApiFlavor::AnthropicMessages)
            .expect_err("Anthropic must own tool_choice on every native path");
        assert!(error.to_string().contains("owns tool_choice"), "{error}");
    }
}

#[test]
fn provider_boundary_rejects_native_schema_without_capability() {
    let mut request = sample_request();
    request.response_schema = Some(json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    }));
    request.structured_output = StructuredOutputMode::Auto;

    let error = validate_structured_output_capabilities(
        &request,
        ApiFlavor::ChatCompletions,
        &Capabilities::default(),
    )
    .expect_err("native transport must require the provider capability");
    assert!(error.to_string().contains("native structured output"));

    request.structured_output = StructuredOutputMode::Prompt;
    validate_structured_output_capabilities(
        &request,
        ApiFlavor::ChatCompletions,
        &Capabilities::default(),
    )
    .expect("prompt mode must remain available without native support");
}

#[test]
fn provider_boundary_rejects_reserved_and_invalid_tool_schemas() {
    let mut request = sample_request();
    request.tools = vec![ToolSpec {
        name: "response".into(),
        description: "reserved".into(),
        input_schema: json!({ "type": "object" }),
    }];
    let error = validate_chat_request(&request, ApiFlavor::ChatCompletions)
        .expect_err("reserved tool name must fail");
    assert!(error.to_string().contains("reserved"));

    request.tools[0].name = "broken".into();
    request.tools[0].input_schema = json!({ "type": "object", "required": "value" });
    let error = validate_chat_request(&request, ApiFlavor::ChatCompletions)
        .expect_err("invalid tool schema must fail");
    assert!(error.to_string().contains("input_schema is invalid"));

    request.tools[0].input_schema = json!({
        "type": "object",
        "properties": { "value": { "$dynamicRef": "https://example.invalid/schema.json" } }
    });
    let error = validate_chat_request(&request, ApiFlavor::ChatCompletions)
        .expect_err("external tool schema reference must fail");
    assert!(error.to_string().contains("external reference"));
}

#[test]
fn anthropic_rejects_top_level_union_tool_schemas() {
    for keyword in ["anyOf", "oneOf", "allOf"] {
        let mut request = sample_request();
        request.tools = vec![ToolSpec {
            name: "union_tool".into(),
            description: "union".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "value": { "type": "string" } },
                keyword: [{ "type": "string" }]
            }),
        }];
        let error = validate_chat_request(&request, ApiFlavor::AnthropicMessages)
            .expect_err("Anthropic must reject top-level unions fail-closed");
        assert!(
            error.to_string().contains(keyword),
            "{error} missing {keyword}"
        );
        validate_chat_request(&request, ApiFlavor::ChatCompletions)
            .expect("other APIs still accept the same schema");
    }
}

#[test]
fn native_schema_compatibility_rejects_unsupported_keywords_and_external_refs() {
    let supported = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "answer": { "type": "string", "pattern": "^[a-z]+$" },
            "score": { "type": "number", "minimum": 0, "maximum": 1 },
            "tags": {
                "type": "array",
                "minItems": 1,
                "maxItems": 3,
                "items": { "type": "string" }
            }
        },
        "required": ["answer", "score", "tags"]
    });
    assert!(native_schema_compatible(&supported));
    assert!(strict_schema_compatible(&supported));

    for schema in [
        json!({ "type": "array", "items": { "type": "string" } }),
        json!({ "type": "object", "properties": { "answer": { "type": "string", "minLength": 1 } } }),
        json!({ "type": "object", "properties": { "answer": { "$ref": "https://example.invalid/schema.json" } } }),
        json!({ "type": "object", "properties": { "answer": { "$ref": "#/$defs/missing" } } }),
        json!({ "type": "object", "properties": { "answer": { "type": "string", "minimum": 1 } } }),
        json!({ "type": "object", "properties": { "answer": { "properties": {} } } }),
        json!({ "type": "object", "properties": [] }),
        json!({ "type": "object", "properties": {}, "required": "answer" }),
        json!({ "type": "object", "anyOf": [{ "type": "object" }] }),
    ] {
        assert!(!native_schema_compatible(&schema), "{schema}");
        assert!(!strict_schema_compatible(&schema), "{schema}");
    }
}

#[tokio::test]
async fn fake_provider_schema_response_uses_an_explicit_default() {
    let schema = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["summary"],
        "properties": {
            "summary": { "type": "string", "minLength": 1 }
        },
        "default": { "summary": "bounded specialist result" }
    });
    let mut request = sample_request();
    request.response_schema = Some(schema);
    request.structured_output = StructuredOutputMode::Prompt;
    let response = FakeLlmProvider
        .complete(request)
        .await
        .expect("fake provider should complete");
    assert!(matches!(
        &response.content[0],
        ChatContent::Text(text) if text == r#"{"summary":"bounded specialist result"}"#
    ));
}

#[test]
fn multimodal_parts_map_to_provider_native_payloads() {
    let mut request = sample_request();
    request.messages = vec![ChatMessage::with_parts(
        "user",
        vec![
            ChatContentPart::Text {
                text: "inspect".into(),
            },
            ChatContentPart::InputImage {
                media_type: "image/png".into(),
                data: "aGVsbG8=".into(),
                detail: Some(ImageDetail::High),
            },
            ChatContentPart::InputFile {
                media_type: "application/pdf".into(),
                data: "cGRm".into(),
                filename: "input.pdf".into(),
            },
        ],
    )];
    let chat = chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens, None);
    assert_eq!(chat["messages"][1]["content"][1]["type"], "image_url");
    assert_eq!(
        chat["messages"][1]["content"][1]["image_url"]["url"],
        "data:image/png;base64,aGVsbG8="
    );
    let responses = responses_payload(&request, None);
    assert_eq!(responses["input"][1]["content"][2]["type"], "input_file");
    assert_eq!(responses["input"][1]["content"][2]["filename"], "input.pdf");
    let anthropic = anthropic_payload(&request, None);
    assert_eq!(anthropic["messages"][0]["content"][1]["type"], "image");
    assert_eq!(
        anthropic["messages"][0]["content"][2]["source"]["media_type"],
        "application/pdf"
    );
}

#[test]
fn provider_payloads_map_explicit_invocation_policy() {
    let mut request = sample_request();
    request.temperature = None;
    request.top_p = Some(0.25);
    request.stop_sequences = vec!["END".into()];
    request.tools = vec![ToolSpec {
        name: "lookup".into(),
        description: "Look up a value".into(),
        input_schema: json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {}
        }),
    }];
    request.tool_choice = Some(ToolChoice::required());
    request.parallel_tool_calls = Some(false);
    request.verbosity = Some(ResponseVerbosity::High);

    validate_chat_request(&request, ApiFlavor::ChatCompletions)
        .expect("portable request policy should validate");
    let chat = chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens, None);
    assert_eq!(chat["top_p"], 0.25);
    assert_eq!(chat["stop"], json!(["END"]));
    assert_eq!(chat["tool_choice"], "required");
    assert_eq!(chat["parallel_tool_calls"], false);

    let responses = responses_payload(&request, None);
    assert_eq!(responses["top_p"], 0.25);
    assert!(responses.get("stop").is_none());
    assert_eq!(responses["tool_choice"], "required");
    assert_eq!(responses["parallel_tool_calls"], false);
    assert_eq!(responses["text"]["verbosity"], "high");

    let anthropic = anthropic_payload(&request, None);
    assert_eq!(anthropic["top_p"], 0.25);
    assert_eq!(anthropic["stop_sequences"], json!(["END"]));
    assert_eq!(anthropic["tool_choice"]["type"], "any");
    assert_eq!(anthropic["tool_choice"]["disable_parallel_tool_use"], true);

    request.tool_choice = Some(ToolChoice::Tool {
        tool: "lookup".into(),
    });
    assert_eq!(
        chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens, None)["tool_choice"]
            ["function"]["name"],
        "lookup"
    );
    assert_eq!(
        responses_payload(&request, None)["tool_choice"]["name"],
        "lookup"
    );
    assert_eq!(
        anthropic_payload(&request, None)["tool_choice"]["name"],
        "lookup"
    );
}

#[test]
fn invocation_policy_rejects_conflicts_and_tool_controls_without_tools() {
    let mut request = sample_request();
    request.top_p = Some(0.5);
    assert!(
        validate_chat_request(&request, ApiFlavor::ChatCompletions)
            .unwrap_err()
            .to_string()
            .contains("mutually exclusive")
    );

    request.top_p = None;
    request.tool_choice = Some(ToolChoice::required());
    assert!(
        validate_chat_request(&request, ApiFlavor::ChatCompletions)
            .unwrap_err()
            .to_string()
            .contains("require at least one tool")
    );
}

#[tokio::test]
async fn chat_stream_accumulates_text_deltas_and_usage() {
    let (events, mut receiver) = mpsc::channel(4);
    let mut accumulator = ChatCompletionAccumulator::default();
    accumulator
        .ingest(
            &json!({"choices":[{"delta":{"content":"hel"},"finish_reason":null}]}),
            &events,
        )
        .await
        .unwrap();
    accumulator
        .ingest(
            &json!({
                "choices":[{"delta":{"content":"lo"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":2,"completion_tokens":1}
            }),
            &events,
        )
        .await
        .unwrap();
    let response = accumulator.finish().unwrap();
    assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "hello"));
    assert_eq!(response.usage.input, 2);
    assert!(
        matches!(receiver.try_recv(), Ok(ChatStreamEvent::TextDelta { text }) if text == "hel")
    );
    assert!(matches!(receiver.try_recv(), Ok(ChatStreamEvent::TextDelta { text }) if text == "lo"));
}

#[tokio::test]
async fn circuit_breaker_opens_after_declared_failures() {
    let mut spec = spec_with_base_url("guarded", "http://127.0.0.1:1/v1/");
    spec.circuit_breaker_failures = Some(2);
    let provider = HttpProvider::from_spec(spec);
    for _ in 0..2 {
        provider.record_request_result::<()>(&Err(LlmError {
            message: "unavailable".into(),
            kind: LlmErrorKind::HttpStatus(503),
        }));
    }
    let error = provider
        .acquire_request_slot()
        .await
        .expect_err("open circuit must reject without issuing a request");
    assert_eq!(error.kind, LlmErrorKind::CircuitOpen);
}

#[test]
fn chat_completions_payload_maps_reasoning_effort_and_completion_limit() {
    let mut request = sample_request();
    request.temperature = None;
    request.seed = None;
    request.reasoning_effort = Some(ReasoningEffort::High);

    let payload = chat_completions_payload(
        &request,
        false,
        ChatTokenLimitField::MaxCompletionTokens,
        None,
    );

    assert_eq!(payload["reasoning_effort"], "high");
    assert_eq!(payload["max_completion_tokens"], 128);
    assert!(payload.get("max_tokens").is_none());
}

#[test]
fn responses_payload_maps_reasoning_effort() {
    let mut request = sample_request();
    request.temperature = None;
    request.seed = None;
    request.reasoning_effort = Some(ReasoningEffort::Max);

    let payload = responses_payload(&request, None);

    assert_eq!(payload["reasoning"]["effort"], "max");
    assert_eq!(payload["max_output_tokens"], 128);
    assert_eq!(payload["store"], false);
}

#[test]
fn parses_responses_text_response() {
    let response = parse_responses_response(json!({
        "status": "completed",
        "output": [{
            "type": "message",
            "content": [{ "type": "output_text", "text": "hello" }]
        }],
        "usage": { "input_tokens": 7, "output_tokens": 2 }
    }))
    .expect("response should parse");

    assert_eq!(response.usage.input, 7);
    assert_eq!(response.usage.output, 2);
    assert!(matches!(response.stop, StopReason::EndTurn));
    assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "hello"));
}

#[test]
fn parses_responses_function_call() {
    let response = parse_responses_response(json!({
        "status": "completed",
        "output": [{
            "type": "function_call",
            "call_id": "call_1",
            "name": "write_draft",
            "arguments": "{\"path\":\"drafts/result.txt\",\"content\":\"ok\"}"
        }],
        "usage": { "input_tokens": 9, "output_tokens": 3 }
    }))
    .expect("response should parse");

    assert!(matches!(response.stop, StopReason::ToolUse));
    assert!(response.provider_state.is_some());
    match &response.content[0] {
        ChatContent::ToolCall { id, name, args } => {
            assert_eq!(id, "call_1");
            assert_eq!(name, "write_draft");
            assert_eq!(args["content"], "ok");
        }
        other => panic!("expected tool call, got {other:?}"),
    }
}

#[test]
fn response_parsers_report_reasoning_tokens_as_output_detail() {
    let chat = parse_chat_completions_response(json!({
        "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
        "usage": {
            "prompt_tokens": 3,
            "completion_tokens": 11,
            "completion_tokens_details": { "reasoning_tokens": 7 }
        }
    }))
    .expect("chat response should parse");
    assert_eq!(chat.usage.output, 11);
    assert_eq!(chat.usage.reasoning, 7);

    let responses = parse_responses_response(json!({
        "status": "completed",
        "output": [{
            "type": "message",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": {
            "input_tokens": 3,
            "output_tokens": 11,
            "output_tokens_details": { "reasoning_tokens": 7 }
        }
    }))
    .expect("Responses API response should parse");
    assert_eq!(responses.usage.output, 11);
    assert_eq!(responses.usage.reasoning, 7);
}

#[test]
fn response_parsers_report_cached_input_tokens() {
    let chat = parse_chat_completions_response(json!({
        "choices": [{ "message": { "content": "ok" }, "finish_reason": "stop" }],
        "usage": {
            "prompt_tokens": 100,
            "completion_tokens": 10,
            "prompt_tokens_details": { "cached_tokens": 40 }
        }
    }))
    .expect("chat response should parse");
    assert_eq!(chat.usage.input, 100);
    assert_eq!(chat.usage.cached_input, 40);

    let responses = parse_responses_response(json!({
        "status": "completed",
        "output": [{
            "type": "message",
            "content": [{ "type": "output_text", "text": "ok" }]
        }],
        "usage": {
            "input_tokens": 100,
            "output_tokens": 10,
            "input_tokens_details": { "cached_tokens": 25 }
        }
    }))
    .expect("Responses API response should parse");
    assert_eq!(responses.usage.input, 100);
    assert_eq!(responses.usage.cached_input, 25);

    let anthropic = parse_anthropic_response(json!({
        "content": [{ "type": "text", "text": "hello" }],
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": 100,
            "output_tokens": 10,
            "cache_read_input_tokens": 60,
            "cache_creation_input_tokens": 5
        }
    }))
    .expect("Anthropic response should parse");
    assert_eq!(anthropic.usage.input, 100);
    assert_eq!(anthropic.usage.cached_input, 60);
}

#[test]
fn anthropic_payload_forces_response_tool_choice() {
    let mut request = sample_request();
    request.response_schema = Some(json!({ "type": "object" }));

    let payload = anthropic_payload(&request, None);

    assert_eq!(payload["tool_choice"]["name"], "response");
    assert_eq!(payload["system"], "system");
    assert!(
        payload["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .any(|tool| tool["name"] == "response")
    );
}

#[test]
fn parses_anthropic_text_response() {
    let response = parse_anthropic_response(json!({
        "content": [{ "type": "text", "text": "hello" }],
        "stop_reason": "end_turn",
        "usage": { "input_tokens": 5, "output_tokens": 2 }
    }))
    .expect("response should parse");

    assert_eq!(response.usage.input, 5);
    assert_eq!(response.usage.output, 2);
    assert!(matches!(response.stop, StopReason::EndTurn));
    assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "hello"));
}

#[test]
fn parses_anthropic_tool_use_response() {
    let response = parse_anthropic_response(json!({
        "content": [{
            "type": "tool_use",
            "id": "toolu_1",
            "name": "write_file",
            "input": { "path": "out.txt", "content": "ok" }
        }],
        "stop_reason": "tool_use",
        "usage": { "input_tokens": 8, "output_tokens": 3 }
    }))
    .expect("response should parse");

    assert!(matches!(response.stop, StopReason::ToolUse));
    match &response.content[0] {
        ChatContent::ToolCall { id, name, args } => {
            assert_eq!(id, "toolu_1");
            assert_eq!(name, "write_file");
            assert_eq!(args["path"], "out.txt");
        }
        other => panic!("expected tool call, got {other:?}"),
    }
}

#[test]
fn parses_anthropic_schema_tool_as_text() {
    let response = parse_anthropic_response(json!({
        "content": [{
            "type": "tool_use",
            "id": "toolu_schema",
            "name": "response",
            "input": { "title": "structured" }
        }],
        "stop_reason": "tool_use",
        "usage": { "input_tokens": 8, "output_tokens": 3 }
    }))
    .expect("response should parse");

    assert!(
        matches!(&response.content[0], ChatContent::Text(text) if text == "{\"title\":\"structured\"}")
    );
    assert_eq!(response.stop, StopReason::EndTurn);
}

#[tokio::test]
async fn streams_anthropic_schema_tool_as_a_completed_structured_response() {
    let (events, _receiver) = mpsc::channel(4);
    let mut accumulator = AnthropicAccumulator::default();
    for event in [
        json!({
            "type": "message_start",
            "message": { "usage": { "input_tokens": 8 } }
        }),
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "tool_use",
                "id": "toolu_schema",
                "name": "response"
            }
        }),
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "input_json_delta",
                "partial_json": "{\"title\":\"structured\"}"
            }
        }),
        json!({
            "type": "message_delta",
            "delta": { "stop_reason": "tool_use" },
            "usage": { "output_tokens": 3 }
        }),
    ] {
        accumulator
            .ingest(&event, &events)
            .await
            .expect("stream event should parse");
    }
    let response = accumulator.finish().expect("stream should finish");
    assert_eq!(response.stop, StopReason::EndTurn);
    assert!(
        matches!(&response.content[0], ChatContent::Text(text) if text == "{\"title\":\"structured\"}")
    );
}

#[test]
fn interpolates_environment_placeholders() {
    if env_process::isolated() {
        return;
    }
    // SAFETY: environment mutation is serialized with ENV_LOCK.
    let _guard = super::ENV_LOCK.blocking_lock();
    // SAFETY: lock held; unique variable name.
    unsafe { std::env::set_var("LLM_INTERP_TEST", "resolved") };
    let resolved =
        interpolate_env("https://host/{LLM_INTERP_TEST}/v1").expect("placeholder should resolve");
    assert_eq!(resolved, "https://host/resolved/v1");
    let error = interpolate_env("https://host/{LLM_MISSING_VARIABLE_XYZ}/v1").unwrap_err();
    assert!(error.contains("LLM_MISSING_VARIABLE_XYZ"), "{error}");
    let error = interpolate_env("https://host/{not-an-env}/v1").unwrap_err();
    assert!(error.contains("invalid environment placeholder"), "{error}");
}

fn spec_with_base_url(id: &str, base_url: &str) -> ProviderSpec {
    let capabilities = Capabilities {
        temperature: true,
        ..Capabilities::default()
    };
    ProviderSpec {
        id: id.into(),
        api: ApiFlavor::ChatCompletions,
        base_url: Some(base_url.into()),
        base_url_env: None,
        api_key_env: None,
        api_key_file_env: None,
        auth_header: None,
        capabilities,
        models: Vec::new(),
        models_discovery: None,
        catalog_id: None,
        path_template: None,
        query: BTreeMap::new(),
        timeout_seconds: None,
        retry_attempts: None,
        retry_base_backoff_ms: None,
        stream_retry_attempts: None,
        retry_rate_limit_floor_ms: None,
        retry_backoff_exponent_cap: None,
        chat_token_limit_field: None,
        prompt_cache_field: None,
        decision_input_wrapper: DecisionInputWrapper::None,
        decision_model_in_body: true,
        decision_response_wrapper: DecisionResponseWrapper::Auto,
        response_body_limit_bytes: None,
        max_concurrency: None,
        requests_per_minute: None,
        circuit_breaker_failures: None,
        circuit_breaker_cooldown_seconds: None,
    }
}

#[test]
fn endpoint_uses_flavor_default_path() {
    let provider = HttpProvider::from_spec(spec_with_base_url("x", "http://host/v1/"));
    assert_eq!(
        provider
            .endpoint_for("m")
            .expect("endpoint should be valid")
            .as_str(),
        "http://host/v1/chat/completions"
    );
}

#[test]
fn endpoint_supports_path_template_and_query_interpolation() {
    if env_process::isolated() {
        return;
    }
    let mut spec = spec_with_base_url("azure", "https://resource.example");
    spec.path_template = Some("openai/deployments/{model}/chat/completions".into());
    spec.query
        .insert("api-version".into(), "2024-10-21&unexpected=true".into());
    // SAFETY: environment mutation is serialized with ENV_LOCK; unique variable name.
    let _guard = super::ENV_LOCK.blocking_lock();
    // SAFETY: lock held.
    unsafe { std::env::remove_var("LLM_AZURE_VERSION_TEST") };

    let provider = HttpProvider::from_spec(spec);

    assert_eq!(
        provider
            .endpoint_for("deploy-1")
            .expect("endpoint should be valid")
            .as_str(),
        "https://resource.example/openai/deployments/deploy-1/chat/completions?api-version=2024-10-21%26unexpected%3Dtrue"
    );
}

#[test]
fn endpoint_expands_model_as_path_segments() {
    let mut spec = spec_with_base_url("x", "https://resource.example/v1");
    spec.path_template = Some("deployments/{model}/chat".into());
    let provider = HttpProvider::from_spec(spec);

    // `{model}` expands as segments so namespaced models such as
    // `typesafe/jev` address `.../run/typesafe/jev`. Delimiters that would
    // escape the path (`?`) stay encoded; empty, `.`, and `..` pieces fail.
    assert_eq!(
        provider
            .endpoint_for("deployment/with?delimiters")
            .expect("endpoint should be valid")
            .as_str(),
        "https://resource.example/v1/deployments/deployment/with%3Fdelimiters/chat"
    );
    assert_eq!(
        provider
            .endpoint_for("typesafe/jev")
            .expect("endpoint should be valid")
            .as_str(),
        "https://resource.example/v1/deployments/typesafe/jev/chat"
    );
    for unsafe_model in ["", "a//b", "a/./b", "a/../b", "/leading", "trailing/"] {
        assert!(
            provider.endpoint_for(unsafe_model).is_err(),
            "unsafe model `{unsafe_model}` must fail"
        );
    }
}

#[test]
fn query_rejects_credential_environment_placeholders() {
    for value in [
        "{PROVIDER_API_KEY_XYZ}",
        "{PROVIDER_KEY_XYZ}",
        "prefix-{PROVIDER_TOKEN_XYZ}",
        "{PROVIDER_SECRET_XYZ}",
        "{PROVIDER_PASSWORD_XYZ}",
        "{PROVIDER_AUTH_XYZ}",
        "{PROVIDER_CREDENTIAL_XYZ}",
    ] {
        let mut spec = spec_with_base_url("query-secret", "https://example.test/v1");
        spec.query.insert("version".into(), value.into());
        let error = ProvidersFile {
            default: None,
            provider: vec![spec],
            search_provider: vec![],
            mcp_server: vec![],
            catalog: None,
        }
        .validate()
        .expect_err("credential query interpolation must be rejected");
        assert!(error.contains("must not interpolate credential"), "{error}");
    }

    let mut spec = spec_with_base_url("query-secret", "https://example.test/v1");
    spec.query.insert("api-key".into(), "literal".into());
    let error = spec
        .validate()
        .expect_err("credential-like query names must be rejected");
    assert!(error.contains("must not carry credentials"), "{error}");
}

#[test]
fn query_rejects_the_configured_credential_env_without_name_heuristics() {
    let mut spec = spec_with_base_url("query-secret", "https://example.test/v1");
    spec.api_key_env = Some("LLM_AUTH_XYZ".into());
    spec.query.insert("version".into(), "{LLM_AUTH_XYZ}".into());

    let error = spec
        .validate()
        .expect_err("the configured credential must not be interpolated");
    assert!(error.contains("LLM_AUTH_XYZ"), "{error}");
}

#[test]
fn query_allows_non_credential_environment_placeholders() {
    let mut spec = spec_with_base_url("query-version", "https://example.test/v1");
    spec.query
        .insert("api-version".into(), "{PROVIDER_API_VERSION_XYZ}".into());
    assert!(
        spec.validate().is_ok(),
        "version placeholders are not credentials"
    );
}

#[test]
fn credential_like_environment_names_use_token_boundaries() {
    for name in [
        "PROVIDER_APIKEY_XYZ",
        "PROVIDER_AUTH_XYZ",
        "PROVIDER_KEY_XYZ",
        "PROVIDER_PASSWORD_XYZ",
        "PROVIDER_TOKEN_XYZ",
    ] {
        assert!(
            credential_like_name(name),
            "{name} should be credential-like"
        );
    }
    for name in [
        "PROVIDER_API_VERSION_XYZ",
        "PROVIDER_AUTHORITY_XYZ",
        "PROVIDER_KEYBOARD_XYZ",
        "PROVIDER_PASSWORDLESS_XYZ",
        "PROVIDER_TOKENIZER_XYZ",
    ] {
        assert!(
            !credential_like_name(name),
            "{name} should not be classified as a credential"
        );
    }
}

#[test]
fn base_url_rejects_credential_environment_placeholders() {
    let mut spec = spec_with_base_url(
        "base-secret",
        "https://example.test/v1/{PROVIDER_API_KEY_XYZ}",
    );
    spec.api_key_env = None;
    let error = spec
        .validate()
        .expect_err("base URL must not interpolate credentials");
    assert!(error.contains("must not interpolate credential"), "{error}");
}

#[test]
fn base_url_rejects_userinfo_query_and_fragment() {
    for (base_url, expected) in [
        ("https://user:password@example.test/v1", "userinfo"),
        ("https://example.test/v1?api-version=1", "query"),
        ("https://example.test/v1#fragment", "fragment"),
    ] {
        let mut spec = spec_with_base_url("unsafe", base_url);
        spec.api_key_env = None;
        let provider = HttpProvider::from_spec(spec);
        let error = provider
            .configuration_error_for("unsafe")
            .expect("unsafe base URL should be rejected");
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("password"), "{error}");
    }
}

#[test]
fn credentialed_http_requires_https_except_for_loopback() {
    if env_process::isolated() {
        return;
    }
    let mut remote = spec_with_base_url("remote", "http://example.test/v1");
    remote.api_key_env = Some("LLM_HTTP_REMOTE_KEY_XYZ".into());
    let provider = HttpProvider::from_spec(remote);
    let error = provider
        .configuration_error_for("remote")
        .expect("remote credentialed HTTP should be rejected");
    assert!(error.contains("loopback"), "{error}");

    // SAFETY: environment mutation is serialized with ENV_LOCK.
    let _guard = super::ENV_LOCK.blocking_lock();
    // SAFETY: lock held; unique variable name.
    unsafe { std::env::set_var("LLM_HTTP_LOOPBACK_KEY_XYZ", "loopback-secret") };
    let mut loopback = spec_with_base_url("loopback", "http://127.0.0.7:8080/v1");
    loopback.api_key_env = Some("LLM_HTTP_LOOPBACK_KEY_XYZ".into());
    let provider = HttpProvider::from_spec(loopback);
    assert!(
        provider.configuration_error_for("loopback").is_none(),
        "loopback HTTP should be allowed"
    );
    // SAFETY: see above; restore the unique test variable.
    unsafe { std::env::remove_var("LLM_HTTP_LOOPBACK_KEY_XYZ") };
}

#[test]
fn credential_env_names_expose_names_without_values() {
    if env_process::isolated() {
        return;
    }
    // SAFETY: environment mutation is serialized with ENV_LOCK.
    let _guard = super::ENV_LOCK.blocking_lock();
    // SAFETY: lock held; unique variable name.
    unsafe { std::env::set_var("LLM_ENV_NAME_ONLY_XYZ", "do-not-expose") };
    let mut spec = spec_with_base_url("keyed", "https://example.test/v1");
    spec.api_key_env = Some("LLM_ENV_NAME_ONLY_XYZ".into());
    let provider = HttpProvider::from_spec(spec);
    assert_eq!(
        provider.credential_env_names(),
        vec!["LLM_ENV_NAME_ONLY_XYZ".to_owned()]
    );
    // SAFETY: see above; restore the unique test variable.
    unsafe { std::env::remove_var("LLM_ENV_NAME_ONLY_XYZ") };
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let (base_url, server) = spawn_http_response(
        302,
        "upstream redirect body must stay private".into(),
        "Location: http://127.0.0.1:1/redirected\r\n".into(),
    );
    let provider = HttpProvider::from_spec(spec_with_base_url("redirect", &base_url));
    let mut request = sample_request();
    request.seed = None;

    let error = provider
        .complete(request)
        .await
        .expect_err("redirect responses must not be followed");
    assert_eq!(error.kind, LlmErrorKind::HttpStatus(302));
    assert!(!error.message.contains("upstream redirect body"));
    server.join().expect("test server should stop");
}

#[tokio::test]
async fn non_success_body_is_not_returned_in_error() {
    let (base_url, server) =
        spawn_http_response(401, "sensitive upstream error details".into(), "".into());
    let provider = HttpProvider::from_spec(spec_with_base_url("status", &base_url));
    let mut request = sample_request();
    request.seed = None;

    let error = provider
        .complete(request)
        .await
        .expect_err("non-success responses must fail");
    assert_eq!(error.kind, LlmErrorKind::HttpStatus(401));
    assert!(!error.message.contains("sensitive upstream error details"));
    server.join().expect("test server should stop");
}

#[tokio::test]
async fn reflected_credential_is_never_returned() {
    if env_process::isolated() {
        return;
    }
    let key = "test<reflected-credential-unique";
    // SAFETY: environment mutation is serialized with ENV_LOCK.
    let _guard = super::ENV_LOCK.lock().await;
    // SAFETY: lock held; unique variable name.
    unsafe { std::env::set_var("LLM_REFLECTION_KEY_XYZ", key) };
    let body = r#"{"choices":[{"message":{"content":"test\u003creflected-credential-unique"}}]}"#
        .to_string();
    let (base_url, server) = spawn_http_response(200, body, "".into());
    let mut spec = spec_with_base_url("reflection", &base_url);
    spec.api_key_env = Some("LLM_REFLECTION_KEY_XYZ".into());
    let provider = HttpProvider::from_spec(spec);
    let mut request = sample_request();
    request.seed = None;

    let error = provider
        .complete(request)
        .await
        .expect_err("a reflected credential must fail closed");
    assert!(!error.message.contains(key));
    assert!(error.message.contains("configured credential"));
    // SAFETY: see above; restore the unique test variable.
    unsafe { std::env::remove_var("LLM_REFLECTION_KEY_XYZ") };
    server.join().expect("test server should stop");
}

#[tokio::test]
async fn oversized_response_body_is_rejected_before_json_parsing() {
    let body = r#"{"oversized":"sensitive upstream body"}"#.to_string();
    let (base_url, server) = spawn_http_response(200, body, "".into());
    let mut spec = spec_with_base_url("bounded", &base_url);
    spec.response_body_limit_bytes = Some(8);
    let provider = HttpProvider::from_spec(spec);
    let mut request = sample_request();
    request.seed = None;

    let error = provider
        .complete(request)
        .await
        .expect_err("an oversized response must fail closed");
    assert_eq!(error.kind, LlmErrorKind::InvalidResponse);
    assert!(error.message.contains("response_body_limit_bytes"));
    assert!(!error.message.contains("sensitive upstream body"));
    server.join().expect("test server should stop");
}

#[tokio::test]
async fn reqwest_errors_do_not_expose_endpoint_url() {
    let provider = HttpProvider::from_spec(spec_with_base_url(
        "network",
        "http://127.0.0.1:9/llm-network-test",
    ));
    let mut request = sample_request();
    request.seed = None;

    let error = provider
        .complete(request)
        .await
        .expect_err("closed endpoint should fail");
    assert_eq!(error.kind, LlmErrorKind::Network);
    assert!(!error.message.contains("127.0.0.1"));
    assert!(!error.message.contains("llm-network-test"));
}

#[test]
fn unresolved_base_url_placeholder_is_a_configuration_error() {
    let mut spec = spec_with_base_url(
        "cloudy",
        "https://api.example/accounts/{LLM_MISSING_ACCOUNT_XYZ}/ai/v1",
    );

    // Simulate an env override that is absent so the literal placeholder path runs.
    spec.base_url_env = Some("LLM_MISSING_OVERRIDE_XYZ".into());

    let provider = HttpProvider::from_spec(spec);

    let error = provider
        .configuration_error_for("cloudy")
        .expect("unresolved placeholder should be reported");
    assert!(error.contains("LLM_MISSING_ACCOUNT_XYZ"), "{error}");
}

#[test]
fn providers_file_rejects_unknown_keys_and_duplicates() {
    let parsed = ProvidersFile::parse(&format!(
        "{}{}",
        test_provider_row("a", "chat_completions", "https://example.invalid", ""),
        test_provider_row("a", "responses", "https://example.invalid", "")
    ));
    assert!(parsed.is_err());
    assert!(parsed.unwrap_err().contains("duplicate"));

    let parsed = ProvidersFile::parse(&test_provider_row(
        "a",
        "chat_completions",
        "https://example.invalid",
        "unknown_field = true\n",
    ));
    assert!(parsed.is_err());

    // A missing `base_url` is refused, so the row is written out in full.
    let parsed = ProvidersFile::parse(
        r#"
[[provider]]
id = "a"
api = "chat_completions"
"#,
    );
    assert!(parsed.is_err());
    assert!(parsed.unwrap_err().contains("base_url"));
}

#[test]
fn provider_models_declare_per_model_capabilities_and_pricing() {
    let file = ProvidersFile::parse(&test_provider_row(
            "openai",
            "chat_completions",
            "https://example.invalid",
            "chat_token_limit_field = \"max_completion_tokens\"\ncapabilities = { tool_use = true, reasoning_effort = [\"low\", \"high\"] }\n\
             \n\
             [[provider.models]]\n\
             id = \"gpt-5\"\n\
             label = \"GPT-5\"\n\
             reasoning_effort = [\"none\", \"high\"]\n\
             input_cost_per_million_usd = 1.25\n\
             output_cost_per_million_usd = 10.0\n\
             context_tokens = 400000\n\
             max_output_tokens = 128000\n\
             \n\
             [[provider.models]]\n\
             id = \"gpt-5-mini\"\n\
             enabled = false\n",
        ))
        .expect("model declarations must validate");
    let provider = &file.provider[0];
    let capabilities = provider.capabilities_for_model("gpt-5");
    assert_eq!(
        capabilities.reasoning_effort,
        vec![ReasoningEffort::None, ReasoningEffort::High]
    );
    assert!(capabilities.tool_use);
    let pricing = provider.pricing_for_model("gpt-5").expect("pricing");
    assert_eq!(pricing.input_cost_per_million_usd, Some(1.25));
    assert_eq!(pricing.output_cost_per_million_usd, Some(10.0));
    assert!(
        !provider
            .model("gpt-5-mini")
            .expect("declared model")
            .is_enabled()
    );
    // Generic rows keep provider capabilities for undeclared models.
    assert_eq!(
        provider
            .capabilities_for_model("undeclared")
            .reasoning_effort,
        vec![ReasoningEffort::Low, ReasoningEffort::High]
    );
    assert!(provider.pricing_for_model("undeclared").is_none());
}

#[test]
fn provider_models_reject_invalid_declarations() {
    for (source, expected) in [
        (
            test_provider_row(
                "openai",
                "chat_completions",
                "https://example.invalid",
                "\n[[provider.models]]\nid = \"dup\"\n\n[[provider.models]]\nid = \"dup\"\n",
            ),
            "duplicate model id",
        ),
        (
            test_provider_row(
                "openai",
                "chat_completions",
                "https://example.invalid",
                "\n[[provider.models]]\nid = \"bad-price\"\ninput_cost_per_million_usd = -1.0\n",
            ),
            "non-negative",
        ),
        (
            test_provider_row(
                "openai",
                "responses",
                "https://example.invalid",
                "\n[[provider.models]]\nid = \"bad-effort\"\ncapabilities = { stop_sequences = true }\n",
            ),
            "Responses",
        ),
        (
            test_provider_row(
                "chat-model-effort",
                "chat_completions",
                "https://example.invalid",
                "\n[[provider.models]]\nid = \"reasoner\"\nreasoning_effort = [\"high\"]\n",
            ),
            "max_completion_tokens",
        ),
    ] {
        let error = ProvidersFile::parse(&source).expect_err("invalid model must be rejected");
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn providers_file_rejects_invalid_capability_and_transport_combinations() {
    for (source, expected) in [
        (
            test_provider_row(
                "typo",
                "chat_completions",
                "https://example.invalid",
                "capabilities = { reasonng_effort = [\"high\"] }\n",
            ),
            "reasonng_effort",
        ),
        (
            test_provider_row(
                "anthropic-reasoning",
                "anthropic_messages",
                "https://example.invalid",
                "capabilities = { reasoning_effort = [\"high\"] }\n",
            ),
            "anthropic_messages",
        ),
        (
            test_provider_row(
                "anthropic-combined",
                "anthropic_messages",
                "https://example.invalid",
                "capabilities = { tool_use = true, json_schema = true, structured_output_with_tools = true }\n",
            ),
            "structured_output_with_tools",
        ),
        (
            test_provider_row(
                "chat-reasoning",
                "chat_completions",
                "https://example.invalid",
                "capabilities = { reasoning_effort = [\"high\"] }\n",
            ),
            "max_completion_tokens",
        ),
        (
            test_provider_row(
                "responses-seed",
                "responses",
                "https://example.invalid",
                "capabilities = { seed = true }\n",
            ),
            "seed",
        ),
        (
            test_provider_row(
                "chat-verbosity",
                "chat_completions",
                "https://example.invalid",
                "capabilities = { verbosity = true }\n",
            ),
            "Responses API",
        ),
        (
            test_provider_row(
                "chat-decision",
                "chat_completions",
                "https://example.invalid",
                "decision_input_wrapper = \"input\"\n",
            ),
            "system_one",
        ),
        (
            test_provider_row(
                "chat-decision-model",
                "responses",
                "https://example.invalid",
                "decision_model_in_body = false\npath_template = \"run/{model}\"\n",
            ),
            "system_one",
        ),
        (
            test_provider_row(
                "chat-decision-response",
                "chat_completions",
                "https://example.invalid",
                "decision_response_wrapper = \"result\"\n",
            ),
            "system_one",
        ),
        (
            test_provider_row(
                "decision-no-path-model",
                "system_one",
                "https://example.invalid",
                "decision_model_in_body = false\n",
            ),
            "{model}",
        ),
    ] {
        let error = ProvidersFile::parse(&source)
            .expect_err("invalid provider combinations must fail validation");
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn system_one_decision_transports_validate_without_network() {
    // Direct (default), Cloudflare body-style (`/ai/run`), and Cloudflare
    // path-style (`/ai/run/{model}`) are all pure registry mechanism.
    for extra in [
        "",
        "decision_input_wrapper = \"input\"\npath_template = \"run\"\n",
        "path_template = \"run/{model}\"\ndecision_model_in_body = false\n",
        "decision_input_wrapper = \"input\"\npath_template = \"run/{model}\"\ndecision_model_in_body = false\ndecision_response_wrapper = \"result\"\n",
    ] {
        let source = test_provider_row(
            "decisions",
            "system_one",
            "https://example.invalid/v1",
            extra,
        );
        ProvidersFile::parse(&source).expect("decision transports must validate");
    }
}

#[test]
fn providers_file_rejects_invalid_defaults_reserved_ids_and_zero_limits() {
    for (source, expected) in [
        // An unregistered default model is refused before any row is read.
        (
            r#"
[default]
model = { provider = "missing", model = "model" }
"#,
            "unregistered provider",
        ),
        (
            &test_provider_row("fake", "chat_completions", "https://example.invalid", ""),
            "reserved",
        ),
        (
            &test_provider_row(
                "invalid id",
                "chat_completions",
                "https://example.invalid",
                "",
            ),
            "lowercase ASCII",
        ),
        (
            &test_provider_row(
                "bounded",
                "chat_completions",
                "https://example.invalid",
                "response_body_limit_bytes = 0\n",
            ),
            "response_body_limit_bytes",
        ),
        (
            &test_provider_row(
                "retry-bounds",
                "chat_completions",
                "https://example.invalid",
                "retry_attempts = 0\n",
            ),
            "retry_attempts",
        ),
        (
            &test_provider_row(
                "retry-bounds",
                "chat_completions",
                "https://example.invalid",
                "retry_attempts = 11\n",
            ),
            "retry_attempts",
        ),
        (
            &test_provider_row(
                "retry-bounds",
                "chat_completions",
                "https://example.invalid",
                "retry_base_backoff_ms = 60001\n",
            ),
            "retry_base_backoff_ms",
        ),
        (
            &test_provider_row(
                "retry-bounds",
                "chat_completions",
                "https://example.invalid",
                "retry_rate_limit_floor_ms = 60001\n",
            ),
            "retry_rate_limit_floor_ms",
        ),
        (
            &test_provider_row(
                "retry-bounds",
                "chat_completions",
                "https://example.invalid",
                "retry_backoff_exponent_cap = 0\n",
            ),
            "retry_backoff_exponent_cap",
        ),
        (
            &test_provider_row(
                "retry-bounds",
                "chat_completions",
                "https://example.invalid",
                "retry_backoff_exponent_cap = 17\n",
            ),
            "retry_backoff_exponent_cap",
        ),
    ] {
        let error = ProvidersFile::parse(source)
            .expect_err("invalid provider registry invariants must fail");
        assert!(error.contains(expected), "{error}");
    }
    // Explicit large values have no mechanistic ceiling.
    for source in [
        test_provider_row(
            "bounded",
            "chat_completions",
            "https://example.invalid",
            "timeout_seconds = 604801\n",
        ),
        test_provider_row(
            "bounded",
            "chat_completions",
            "https://example.invalid",
            "response_body_limit_bytes = 67108865\n",
        ),
        test_provider_row(
            "bounded",
            "chat_completions",
            "https://example.invalid",
            "max_concurrency = 1025\n",
        ),
    ] {
        ProvidersFile::parse(&source).expect("explicit large provider values must validate");
    }
}

#[test]
fn router_reports_configuration_errors_from_specs() {
    let router = LlmRouter::parse_text(&test_provider_row(
        "keyed",
        "chat_completions",
        "https://example.invalid/v1",
        "api_key_env = \"LLM_MISSING_KEY_XYZ\"\n",
    ))
    .expect("router should build");

    let error = router
        .configuration_error_for("keyed")
        .expect("configuration error should surface");
    assert!(error.contains("LLM_MISSING_KEY_XYZ"), "{error}");
    assert!(router.configuration_error_for("fake").is_none());
    assert!(router.capabilities_for("keyed").is_some());
    assert!(router.capabilities_for("nope").is_none());
}

#[test]
fn workspace_registry_enables_local_endpoints_by_default() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = Utf8PathBuf::from(manifest_dir).join("../../providers.toml");
    if !path.is_file() {
        panic!("workspace providers.toml must exist at {}", path);
    }
    let router = LlmRouter::from_file(&path).expect("providers.toml should be valid");
    // The fake provider backs the test suite and local endpoints work
    // without credentials; credentialed remotes ship disabled.
    for id in ["fake", "ollama", "lmstudio", "openai_client"] {
        assert!(
            router.capabilities_for(id).is_some(),
            "{id} should stay active by default"
        );
    }
    for id in ["openai", "anthropic"] {
        assert!(
            router.capabilities_for(id).is_none(),
            "{id} must ship disabled until it is uncommented"
        );
    }
    assert!(router.default_model().is_none());
}

/// Strips `# ` from commented `[[provider]]` blocks so the shipped
/// catalog can be validated as real TOML. Header prose stays outside
/// blocks and is never uncommented.
fn uncomment_provider_blocks(text: &str) -> String {
    let mut out = String::new();
    let mut in_block = false;
    for line in text.lines() {
        if line.starts_with("# [[provider]]") {
            in_block = true;
        }
        if in_block {
            if line.is_empty() {
                in_block = false;
                out.push('\n');
                continue;
            }
            match line.strip_prefix("# ") {
                Some(rest) => out.push_str(rest),
                None => out.push_str(line),
            }
            out.push('\n');
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

#[test]
fn workspace_commented_rows_enable_into_valid_toml() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = Utf8PathBuf::from(manifest_dir).join("../../providers.toml");
    let text = std::fs::read_to_string(&path).expect("providers.toml should be readable");
    let enabled = uncomment_provider_blocks(&text);
    let router = LlmRouter::parse_text(&enabled)
        .expect("uncommenting every catalog row must yield a valid registry");
    // Spot-check that representative commented rows register once
    // enabled and stay present as templates.
    for id in ["anthropic", "openai", "gemini", "fake"] {
        assert!(
            router.capabilities_for(id).is_some(),
            "{id} should register once its row is enabled"
        );
    }
    for id in ["anthropic", "openai", "gemini"] {
        assert!(
            text.contains(&format!("# id = \"{id}\"")),
            "{id} must remain present as an enable-able template"
        );
    }
}

#[test]
fn load_prefers_explicit_path_and_lists_candidates_when_missing() {
    let dir = std::env::temp_dir().join(format!(
        "llm-load-test-{}-{}",
        std::process::id(),
        uuid_like_suffix()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir should be created");
    let explicit = dir.join("explicit.toml");
    std::fs::write(
        &explicit,
        r#"
[default]
model = { provider = "fake", model = "fake" }

[[provider]]
id = "local"
api = "chat_completions"
base_url = "http://127.0.0.1:9/v1"
"#,
    )
    .expect("registry should be written");

    let router = LlmRouter::load(Some(
        Utf8PathBuf::from_path_buf(explicit.clone())
            .unwrap()
            .as_path(),
    ))
    .expect("explicit registry should load");
    assert!(router.capabilities_for("local").is_some());
    assert_eq!(
        router.default_model(),
        Some(&ModelSelection {
            provider: "fake".into(),
            model: "fake".into(),
        })
    );

    let missing = dir.join("missing.toml");
    let error = LlmRouter::load(Some(Utf8PathBuf::from_path_buf(missing).unwrap().as_path()))
        .expect_err("missing explicit registry should fail");
    assert!(error.to_string().contains("looked at"), "{error}");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn load_optional_honors_the_environment_override() {
    if env_process::isolated() {
        return;
    }
    let dir = std::env::temp_dir().join(format!(
        "llm-load-optional-env-{}-{}",
        std::process::id(),
        uuid_like_suffix()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir should be created");
    let present = dir.join("present.toml");
    std::fs::write(&present, REGISTRY_FIXTURE).expect("registry should be written");
    let missing = dir.join("missing.toml");

    // SAFETY: environment mutation is serialized with ENV_LOCK.
    // PROVIDERS is read by production loading paths, so this
    // shared name must not race any parallel test.
    let _guard = super::ENV_LOCK.blocking_lock();
    // SAFETY: lock held; restored below.
    unsafe { std::env::set_var("PROVIDERS", &present) };
    let router = LlmRouter::load_optional(None)
        .expect("configured env registry should load")
        .expect("env override should resolve");
    assert!(router.capabilities_for("local").is_some());

    unsafe { std::env::set_var("PROVIDERS", &missing) };
    let error =
        LlmRouter::load_optional(None).expect_err("missing env registry must stay a hard error");
    assert!(error.is_not_found());

    // SAFETY: see above; the variable is removed to restore state.
    unsafe { std::env::remove_var("PROVIDERS") };

    let _ = std::fs::remove_dir_all(&dir);
}

fn uuid_like_suffix() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos()
}

const REGISTRY_FIXTURE: &str = r#"
[[provider]]
id = "local"
api = "chat_completions"
base_url = "http://127.0.0.1:9/v1"
"#;

struct RetryProvider {
    calls: AtomicUsize,
}

#[async_trait]
impl LlmProvider for RetryProvider {
    fn id(&self) -> &str {
        "retry"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    async fn complete(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            return Err(LlmError {
                message: "rate limited".into(),
                kind: LlmErrorKind::HttpStatus(429),
            });
        }
        Ok(ChatResponse {
            content: vec![ChatContent::Text("ok".into())],
            usage: TokenUsage {
                input: 1,
                output: 1,
                reasoning: 0,
                cached_input: 0,
            },
            stop: StopReason::EndTurn,
            provider_state: None,
        })
    }
}

struct StreamRetryProvider {
    calls: AtomicUsize,
    emit_before_failure: bool,
}

#[async_trait]
impl LlmProvider for StreamRetryProvider {
    fn id(&self) -> &str {
        "stream-retry"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn stream_retry_attempts(&self) -> usize {
        1
    }

    async fn complete(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
        Err(LlmError::new("complete is unused in stream tests"))
    }

    async fn stream(
        &self,
        _req: ChatRequest,
        events: mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            if self.emit_before_failure {
                let _ = events
                    .send(ChatStreamEvent::TextDelta {
                        text: "partial".into(),
                    })
                    .await;
            }
            return Err(LlmError::new("stream failed"));
        }
        let _ = events
            .send(ChatStreamEvent::TextDelta {
                text: "recovered".into(),
            })
            .await;
        Ok(())
    }
}

fn stream_request() -> ChatRequest {
    ChatRequest {
        provider: "stream-retry".into(),
        model: "test".into(),
        system: None,
        messages: vec![],
        tools: vec![],
        response_schema: None,
        structured_output: StructuredOutputMode::Auto,
        temperature: None,
        top_p: None,
        max_tokens: 8,
        stop_sequences: vec![],
        seed: None,
        reasoning_effort: None,
        tool_choice: None,
        parallel_tool_calls: None,
        verbosity: None,
        stream: true,
        prompt_cache: PromptCache::Off,
    }
}

fn stream_router(provider: Arc<StreamRetryProvider>) -> LlmRouter {
    let mut router = LlmRouter {
        providers: BTreeMap::new(),
        default_model: None,
        search: SearchRuntime::unavailable(),
        mcp: mcp::McpRuntime::unavailable(),
        catalog: Arc::new(CatalogService::empty()),
    };
    router.register(provider);
    router
}

#[tokio::test]
async fn stream_retries_only_before_the_first_delta() {
    // A failure before any event is retried within the configured
    // attempts.
    let provider = Arc::new(StreamRetryProvider {
        calls: AtomicUsize::new(0),
        emit_before_failure: false,
    });
    let observed = Arc::clone(&provider);
    let router = stream_router(provider);
    let (events, mut receiver) = mpsc::channel(4);
    router
        .stream(stream_request(), events)
        .await
        .expect("a pre-delta failure must retry");
    assert_eq!(observed.calls.load(Ordering::SeqCst), 2);
    let event = receiver.recv().await.expect("a delta should be forwarded");
    assert!(matches!(event, ChatStreamEvent::TextDelta { text } if text == "recovered"));

    // Once an event is delivered, a retry would interleave two model
    // responses and is refused.
    let provider = Arc::new(StreamRetryProvider {
        calls: AtomicUsize::new(0),
        emit_before_failure: true,
    });
    let observed = Arc::clone(&provider);
    let router = stream_router(provider);
    let (events, _receiver) = mpsc::channel(4);
    router
        .stream(stream_request(), events)
        .await
        .expect_err("a post-delta failure must not retry");
    assert_eq!(observed.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn router_retries_retryable_provider_error() {
    let provider = Arc::new(RetryProvider {
        calls: AtomicUsize::new(0),
    });
    let observed = Arc::clone(&provider);
    let mut router = LlmRouter {
        providers: BTreeMap::new(),
        default_model: None,
        search: SearchRuntime::unavailable(),
        mcp: mcp::McpRuntime::unavailable(),
        catalog: Arc::new(CatalogService::empty()),
    };
    router.register(provider);
    let response = router
        .complete(ChatRequest {
            provider: "retry".into(),
            model: "test".into(),
            system: None,
            messages: vec![],
            tools: vec![],
            response_schema: None,
            structured_output: StructuredOutputMode::Auto,
            temperature: None,
            top_p: None,
            max_tokens: 1,
            stop_sequences: vec![],
            seed: None,
            reasoning_effort: None,
            tool_choice: None,
            parallel_tool_calls: None,
            verbosity: None,
            stream: false,
            prompt_cache: PromptCache::Off,
        })
        .await
        .expect("router should retry once and succeed");
    assert_eq!(observed.calls.load(Ordering::SeqCst), 2);
    assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "ok"));
}

#[test]
fn default_retry_bounds_have_floor_and_cap() {
    let provider = HttpProvider::from_spec(spec_with_base_url("defaults", "http://host/v1"));
    assert_eq!(
        provider.retry_rate_limit_floor(),
        Duration::from_millis(5000)
    );
    assert_eq!(provider.retry_backoff_exponent_cap(), 8);
    let base = provider.retry_base_backoff();
    let floor = provider.retry_rate_limit_floor();
    let cap = provider.retry_backoff_exponent_cap();
    assert_eq!(
        retry_delay(1, base, floor, cap, true),
        Duration::from_millis(5000)
    );
    assert_eq!(
        retry_delay(2, base, floor, cap, true),
        Duration::from_millis(10_000)
    );
    assert_eq!(
        retry_delay(3, base, floor, cap, true),
        Duration::from_millis(15_000)
    );
    assert_eq!(
        retry_delay(1, base, floor, cap, false),
        Duration::from_millis(200)
    );
    assert_eq!(
        retry_delay(100, base, floor, cap, false),
        Duration::from_millis(51_200)
    );
}

#[test]
fn explicit_retry_bounds_override_defaults() {
    let mut spec = spec_with_base_url("tuned", "http://host/v1");
    spec.retry_base_backoff_ms = Some(100);
    spec.retry_rate_limit_floor_ms = Some(0);
    spec.retry_backoff_exponent_cap = Some(2);
    spec.validate()
        .expect("in-range retry bounds must validate");
    let provider = HttpProvider::from_spec(spec);
    assert_eq!(provider.retry_rate_limit_floor(), Duration::ZERO);
    assert_eq!(provider.retry_backoff_exponent_cap(), 2);
    let base = provider.retry_base_backoff();
    let floor = provider.retry_rate_limit_floor();
    let cap = provider.retry_backoff_exponent_cap();
    assert_eq!(
        retry_delay(2, base, floor, cap, true),
        Duration::from_millis(200)
    );
    assert_eq!(
        retry_delay(5, base, floor, cap, false),
        Duration::from_millis(400)
    );
}

#[test]
fn retryability_is_structural_not_string_based() {
    assert!(!is_retryable_llm_error(&LlmError {
        message: "model gpt-500 failed".into(),
        kind: LlmErrorKind::Other,
    }));
    assert!(is_retryable_llm_error(&LlmError {
        message: "server error".into(),
        kind: LlmErrorKind::HttpStatus(503),
    }));
    assert!(is_retryable_llm_error(&LlmError {
        message: "slow".into(),
        kind: LlmErrorKind::TimedOut,
    }));
    assert!(!is_retryable_llm_error(&LlmError {
        message: "bad request".into(),
        kind: LlmErrorKind::HttpStatus(400),
    }));
    assert!(!is_retryable_llm_error(&LlmError {
        message: "refused".into(),
        kind: LlmErrorKind::Network,
    }));
    assert!(is_retryable_llm_error(&LlmError {
        message: "empty body".into(),
        kind: LlmErrorKind::EmptyResponse,
    }));
    assert!(!is_retryable_llm_error(&LlmError {
        message: "malformed response".into(),
        kind: LlmErrorKind::InvalidResponse,
    }));
}

/// Captured decision HTTP request: path plus raw JSON body.
type CapturedDecisionRequest = Arc<std::sync::Mutex<Option<(String, Vec<u8>)>>>;

fn spawn_decision_server(
    response_body: String,
) -> (String, CapturedDecisionRequest, JoinHandle<()>) {
    use std::sync::{Arc, Mutex};
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = listener.local_addr().expect("address");
    let captured: CapturedDecisionRequest = Arc::new(Mutex::new(None));
    let captured_server = Arc::clone(&captured);
    let handle = std::thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        // Read headers then exactly Content-Length bytes so the JSON body is
        // complete even when it arrives in multiple TCP segments.
        let mut raw = Vec::new();
        let mut buf = [0_u8; 4096];
        let header_end = loop {
            let n = stream.read(&mut buf).expect("request should arrive");
            if n == 0 {
                break None;
            }
            raw.extend_from_slice(&buf[..n]);
            if let Some(pos) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
                break Some(pos + 4);
            }
        };
        let Some(header_end) = header_end else {
            return;
        };
        let headers = String::from_utf8_lossy(&raw[..header_end]).into_owned();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                (name.trim().eq_ignore_ascii_case("content-length"))
                    .then(|| value.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0);
        while raw.len() < header_end + content_length {
            let n = stream.read(&mut buf).expect("body should arrive");
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        let request_line = headers.lines().next().unwrap_or_default().to_string();
        let body = raw[header_end..].to_vec();
        *captured_server.lock().unwrap() = Some((request_line, body));
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        let _ = stream.write_all(response.as_bytes());
    });
    (format!("http://{address}"), captured, handle)
}

fn decision_fixture_request(provider: &str, model: &str) -> DecisionRequest {
    serde_json::from_value(json!({
        "provider": provider, "model": model,
        "state": "Help! My payouts have been failing for three days.",
        "questions": {"urgent": {"type": "noul", "instructions": "Does this convey urgency?"}}
    }))
    .unwrap()
}

fn decision_fixture_answer(model: &str) -> serde_json::Value {
    json!({"model": model, "usage": {"input_tokens": 10, "output_tokens": 2}, "answers": {
        "urgent": {"type": "noul", "noul": 0.95}
    }})
}

#[tokio::test]
async fn decide_supports_cloudflare_body_style_envelope() {
    // Cloudflare `POST .../ai/run`: `{"model","input":{...}}` request and a
    // `{"result":{...}}` response envelope. Pure registry mechanism: the
    // contract still sends logical `{provider, model, state, questions}`.
    let answer = decision_fixture_answer("jev-1.13.0");
    let response_body = serde_json::to_string(&json!({
        "result": answer, "success": true, "errors": [], "messages": []
    }))
    .unwrap();
    let (base_url, captured, server) = spawn_decision_server(response_body);
    let router = LlmRouter::parse_text(&format!(
        r#"
[[provider]]
id = "cf-body"
api = "system_one"
base_url = "{base_url}"
path_template = "run"
decision_input_wrapper = "input"
"#
    ))
    .expect("registry should parse");
    let provider = router
        .providers
        .get("cf-body")
        .expect("provider should register");
    let request = decision_fixture_request("cf-body", "typesafe/jev");
    let response = provider
        .decide(request.clone())
        .await
        .expect("enveloped decision should succeed");
    response.validate(&request).expect("answer should validate");
    assert_eq!(response.model, "jev-1.13.0");
    let (request_line, body) = captured.lock().unwrap().take().expect("request");
    assert!(request_line.contains("POST /run"), "{request_line}");
    let sent: serde_json::Value = serde_json::from_slice(&body).expect("body");
    assert_eq!(sent["model"], json!("typesafe/jev"));
    assert_eq!(
        sent["input"]["state"],
        json!("Help! My payouts have been failing for three days.")
    );
    assert!(sent["input"]["questions"]["urgent"].is_object());
    assert!(sent.get("state").is_none());
    server.join().expect("server should stop");
}

#[tokio::test]
async fn decide_supports_cloudflare_path_style_envelope() {
    // Cloudflare `POST .../ai/run/{model}`: model in the path only,
    // bare `{"state","questions"}` body, `result` envelope response.
    let answer = decision_fixture_answer("jev-1.13.0");
    let response_body = serde_json::to_string(&json!({
        "result": answer, "success": true, "errors": [], "messages": []
    }))
    .unwrap();
    let (base_url, captured, server) = spawn_decision_server(response_body);
    let router = LlmRouter::parse_text(&format!(
        r#"
[[provider]]
id = "cf-path"
api = "system_one"
base_url = "{base_url}"
path_template = "run/{{model}}"
decision_model_in_body = false
"#
    ))
    .expect("registry should parse");
    let provider = router
        .providers
        .get("cf-path")
        .expect("provider should register");
    let request = decision_fixture_request("cf-path", "typesafe/jev");
    let response = provider
        .decide(request.clone())
        .await
        .expect("path-style decision should succeed");
    assert_eq!(response.model, "jev-1.13.0");
    let (request_line, body) = captured.lock().unwrap().take().expect("request");
    assert!(
        request_line.contains("POST /run/typesafe/jev"),
        "{request_line}"
    );
    let sent: serde_json::Value = serde_json::from_slice(&body).expect("body");
    assert!(sent.get("model").is_none());
    assert!(sent["state"].is_string());
    assert!(sent["questions"]["urgent"].is_object());
    server.join().expect("server should stop");
}

#[cfg(test)]
mod env_process {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/test-support/environment.rs"
    ));
}

#[tokio::test]
async fn anonymous_registry_supports_http_streaming_and_preserves_forbidden() {
    if env_process::isolated() {
        return;
    }
    let _guard = super::ENV_LOCK.lock().await;
    // SAFETY: this test runs in an isolated process and holds ENV_LOCK.
    unsafe {
        std::env::set_var("OPENCODE_API_KEY", "unused-test-credential");
        std::env::remove_var("QCG_ANONYMOUS_TEST_MISSING_KEY");
    }
    for (streaming, status) in [(false, 200), (true, 200), (false, 403), (true, 403)] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut raw = Vec::new();
            while !raw.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                socket.read_exact(&mut byte).unwrap();
                raw.push(byte[0]);
            }
            let headers = String::from_utf8(raw).unwrap();
            assert!(headers.starts_with("POST /v1/chat/completions HTTP/1.1"));
            assert!(!headers.to_ascii_lowercase().contains("authorization:"));
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            let mut body = vec![0; length];
            socket.read_exact(&mut body).unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["model"], "example-free");
            if streaming {
                assert_eq!(body["stream"], true);
            }
            let (content_type, response) = if status == 403 {
                (
                    "application/json",
                    r#"{"error":{"type":"FreeTierError","message":"restricted"}}"#.to_owned(),
                )
            } else if streaming {
                ("text/event-stream", "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n".to_owned())
            } else {
                ("application/json", r#"{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}"#.to_owned())
            };
            write!(socket, "HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
        });
        let router = LlmRouter::parse_text(&format!(
            r#"
[[provider]]
id = "anonymous"
api = "chat_completions"
base_url = "http://{address}/v1"
capabilities = {{ streaming = true }}
[[provider.models]]
id = "example-free"
input_cost_per_million_usd = 0.0
output_cost_per_million_usd = 0.0
[[provider]]
id = "paid"
api = "chat_completions"
base_url = "http://127.0.0.1:1/v1"
api_key_env = "QCG_ANONYMOUS_TEST_MISSING_KEY"
"#
        ))
        .unwrap();
        let view = router.catalog.view(false).await;
        let profile = view.providers.iter().find(|p| p.id == "anonymous").unwrap();
        assert!(profile.available);
        assert!(!profile.discovery);
        assert_eq!(profile.models.len(), 1);
        assert_eq!(profile.models[0].input_cost_per_million_usd, Some(0.0));
        assert_eq!(profile.models[0].output_cost_per_million_usd, Some(0.0));
        let mut request = sample_request();
        request.provider = "anonymous".into();
        request.model = "example-free".into();
        request.temperature = None;
        request.seed = None;
        request.stream = streaming;
        let mut paid_request = request.clone();
        paid_request.provider = "paid".into();
        paid_request.stream = false;
        let error = router.complete(paid_request).await.unwrap_err();
        assert!(error.message.contains("QCG_ANONYMOUS_TEST_MISSING_KEY"));
        let result = if streaming {
            let (events, mut receiver) = mpsc::channel(8);
            let result = router.stream(request, events).await;
            if status == 200 {
                assert!(result.is_ok(), "{result:?}");
                assert!(
                    matches!(receiver.try_recv(), Ok(ChatStreamEvent::TextDelta { text }) if text == "OK")
                );
                let mut completed = false;
                while let Ok(event) = receiver.try_recv() {
                    if let ChatStreamEvent::Completed { response } = event {
                        assert!(
                            matches!(&response.content[0], ChatContent::Text(text) if text == "OK")
                        );
                        completed = true;
                    }
                }
                assert!(completed);
            }
            result
        } else {
            router.complete(request).await.map(|response| {
                assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "OK"));
            })
        };
        if status == 403 {
            let error = result.unwrap_err();
            assert_eq!(error.kind, LlmErrorKind::HttpStatus(403));
            assert!(!is_retryable_llm_error(&error));
        } else {
            result.unwrap();
        }
        server.join().unwrap();
    }
}

#[test]
fn bundled_anonymous_example_parses_without_credentials_or_discovery() {
    let bundled = include_str!("../../../providers.toml");
    let start = bundled
        .find("# [[provider]]\n# id = \"opencode-zen-free\"")
        .unwrap();
    let block = bundled[start..].split("\n\n").next().unwrap();
    let text = block
        .lines()
        .map(|line| line.strip_prefix("# ").unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    let file = ProvidersFile::parse(&text).unwrap();
    let spec = &file.provider[0];
    assert!(spec.api_key_env.is_none());
    assert!(spec.api_key_file_env.is_none());
    assert!(spec.models_discovery.is_none());
    assert!(spec.catalog_id.is_none());
    assert_eq!(spec.models[0].id, "big-pickle");
    assert_eq!(
        spec.models[0].pricing().unwrap().input_cost_per_million_usd,
        Some(0.0)
    );
    assert!(!spec.capabilities.json_schema);
}
