mod search;

mod config;
mod http_provider;
mod parse;
mod payload;
mod provider;
mod router;
mod stream;
mod types;
mod validate;

#[cfg(test)]
pub(crate) use config::*;
pub use http_provider::*;
#[cfg(test)]
pub(crate) use parse::*;
pub use payload::*;
pub use provider::*;
pub use router::*;
pub use search::{SearchMethod, SearchProfile, SearchProviderSpec, SearchRuntime};
#[cfg(test)]
pub(crate) use stream::*;
pub use types::*;
#[cfg(test)]
pub(crate) use validate::*;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use camino::Utf8PathBuf;
    use qcg_policy::credential_like_name;
    use qcg_types::{ReasoningEffort, ResponseVerbosity, StructuredOutputMode, ToolChoice};
    use serde_json::{Value, json};
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

        let payload = chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens);
        assert_eq!(payload["seed"], 42);

        let payload = chat_completions_payload(&request, false, ChatTokenLimitField::MaxTokens);
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

        let payload = chat_completions_payload(&request, false, ChatTokenLimitField::MaxTokens);

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
        let auto =
            chat_completions_payload(&request, false, ChatTokenLimitField::MaxCompletionTokens);
        assert_eq!(auto["response_format"]["json_schema"]["strict"], true);

        request.structured_output = StructuredOutputMode::NativeCompatible;
        let compatible = responses_payload(&request);
        assert_eq!(compatible["text"]["format"]["strict"], false);

        request.structured_output = StructuredOutputMode::Prompt;
        let prompt =
            chat_completions_payload(&request, false, ChatTokenLimitField::MaxCompletionTokens);
        assert!(prompt.get("response_format").is_none());
        let anthropic = anthropic_payload(&request);
        assert!(anthropic.get("tool_choice").is_none());
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
            name: "qcg_response".into(),
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
        let chat = chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens);
        assert_eq!(chat["messages"][1]["content"][1]["type"], "image_url");
        assert_eq!(
            chat["messages"][1]["content"][1]["image_url"]["url"],
            "data:image/png;base64,aGVsbG8="
        );
        let responses = responses_payload(&request);
        assert_eq!(responses["input"][1]["content"][2]["type"], "input_file");
        assert_eq!(responses["input"][1]["content"][2]["filename"], "input.pdf");
        let anthropic = anthropic_payload(&request);
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
        let chat = chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens);
        assert_eq!(chat["top_p"], 0.25);
        assert_eq!(chat["stop"], json!(["END"]));
        assert_eq!(chat["tool_choice"], "required");
        assert_eq!(chat["parallel_tool_calls"], false);

        let responses = responses_payload(&request);
        assert_eq!(responses["top_p"], 0.25);
        assert!(responses.get("stop").is_none());
        assert_eq!(responses["tool_choice"], "required");
        assert_eq!(responses["parallel_tool_calls"], false);
        assert_eq!(responses["text"]["verbosity"], "high");

        let anthropic = anthropic_payload(&request);
        assert_eq!(anthropic["top_p"], 0.25);
        assert_eq!(anthropic["stop_sequences"], json!(["END"]));
        assert_eq!(anthropic["tool_choice"]["type"], "any");
        assert_eq!(anthropic["tool_choice"]["disable_parallel_tool_use"], true);

        request.tool_choice = Some(ToolChoice::Tool {
            tool: "lookup".into(),
        });
        assert_eq!(
            chat_completions_payload(&request, true, ChatTokenLimitField::MaxTokens)["tool_choice"]
                ["function"]["name"],
            "lookup"
        );
        assert_eq!(responses_payload(&request)["tool_choice"]["name"], "lookup");
        assert_eq!(anthropic_payload(&request)["tool_choice"]["name"], "lookup");
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
    async fn chat_stream_accumulates_deltas_tool_calls_and_usage() {
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
        assert!(
            matches!(receiver.try_recv(), Ok(ChatStreamEvent::TextDelta { text }) if text == "lo")
        );
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

        let payload =
            chat_completions_payload(&request, false, ChatTokenLimitField::MaxCompletionTokens);

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

        let payload = responses_payload(&request);

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
    fn anthropic_payload_forces_qcg_response_tool_choice() {
        let mut request = sample_request();
        request.response_schema = Some(json!({ "type": "object" }));

        let payload = anthropic_payload(&request);

        assert_eq!(payload["tool_choice"]["name"], "qcg_response");
        assert_eq!(payload["system"], "system");
        assert!(
            payload["tools"]
                .as_array()
                .expect("tools")
                .iter()
                .any(|tool| tool["name"] == "qcg_response")
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
                "name": "qcg_response",
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
                    "name": "qcg_response"
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
    fn interpolates_known_environment_placeholders() {
        // SAFETY: single-threaded test binary section; unique variable name.
        unsafe { std::env::set_var("QCG_LLM_INTERP_TEST", "resolved") };
        let resolved = interpolate_env("https://host/{QCG_LLM_INTERP_TEST}/v1")
            .expect("placeholder should resolve");
        assert_eq!(resolved, "https://host/resolved/v1");
    }

    #[test]
    fn interpolation_reports_missing_variables() {
        let error = interpolate_env("https://host/{QCG_LLM_MISSING_VARIABLE_XYZ}/v1").unwrap_err();
        assert!(error.contains("QCG_LLM_MISSING_VARIABLE_XYZ"), "{error}");
    }

    #[test]
    fn interpolation_rejects_invalid_placeholder_names() {
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
            path_template: None,
            query: BTreeMap::new(),
            timeout_seconds: None,
            retry_attempts: None,
            retry_base_backoff_ms: None,
            chat_token_limit_field: None,
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
        let mut spec = spec_with_base_url("azure", "https://resource.example");
        spec.path_template = Some("openai/deployments/{model}/chat/completions".into());
        spec.query
            .insert("api-version".into(), "2024-10-21&unexpected=true".into());
        // SAFETY: single-threaded test binary section; unique variable name.
        unsafe { std::env::remove_var("QCG_LLM_AZURE_VERSION_TEST") };

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
    fn endpoint_encodes_model_as_a_path_segment() {
        let mut spec = spec_with_base_url("x", "https://resource.example/v1");
        spec.path_template = Some("deployments/{model}/chat".into());
        let provider = HttpProvider::from_spec(spec);

        assert_eq!(
            provider
                .endpoint_for("deployment/with?delimiters")
                .expect("endpoint should be valid")
                .as_str(),
            "https://resource.example/v1/deployments/deployment%2Fwith%3Fdelimiters/chat"
        );
    }

    #[test]
    fn query_rejects_credential_environment_placeholders() {
        for value in [
            "{QCG_PROVIDER_API_KEY_XYZ}",
            "{QCG_PROVIDER_KEY_XYZ}",
            "prefix-{QCG_PROVIDER_TOKEN_XYZ}",
            "{QCG_PROVIDER_SECRET_XYZ}",
            "{QCG_PROVIDER_PASSWORD_XYZ}",
            "{QCG_PROVIDER_AUTH_XYZ}",
            "{QCG_PROVIDER_CREDENTIAL_XYZ}",
        ] {
            let mut spec = spec_with_base_url("query-secret", "https://example.test/v1");
            spec.query.insert("version".into(), value.into());
            let error = ProvidersFile {
                default: None,
                provider: vec![spec],
                search_provider: vec![],
                mcp_server: vec![],
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
        spec.api_key_env = Some("QCG_LLM_AUTH_XYZ".into());
        spec.query
            .insert("version".into(), "{QCG_LLM_AUTH_XYZ}".into());

        let error = spec
            .validate()
            .expect_err("the configured credential must not be interpolated");
        assert!(error.contains("QCG_LLM_AUTH_XYZ"), "{error}");
    }

    #[test]
    fn query_allows_non_credential_environment_placeholders() {
        let mut spec = spec_with_base_url("query-version", "https://example.test/v1");
        spec.query.insert(
            "api-version".into(),
            "{QCG_PROVIDER_API_VERSION_XYZ}".into(),
        );
        assert!(
            spec.validate().is_ok(),
            "version placeholders are not credentials"
        );
    }

    #[test]
    fn credential_like_environment_names_use_token_boundaries() {
        for name in [
            "QCG_PROVIDER_APIKEY_XYZ",
            "QCG_PROVIDER_AUTH_XYZ",
            "QCG_PROVIDER_KEY_XYZ",
            "QCG_PROVIDER_PASSWORD_XYZ",
            "QCG_PROVIDER_TOKEN_XYZ",
        ] {
            assert!(
                credential_like_name(name),
                "{name} should be credential-like"
            );
        }
        for name in [
            "QCG_PROVIDER_API_VERSION_XYZ",
            "QCG_PROVIDER_AUTHORITY_XYZ",
            "QCG_PROVIDER_KEYBOARD_XYZ",
            "QCG_PROVIDER_PASSWORDLESS_XYZ",
            "QCG_PROVIDER_TOKENIZER_XYZ",
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
            "https://example.test/v1/{QCG_PROVIDER_API_KEY_XYZ}",
        );
        spec.api_key_env = None;
        let error = spec
            .validate()
            .expect_err("base URL must not interpolate credentials");
        assert!(error.contains("must not interpolate credential"), "{error}");
    }

    #[test]
    fn base_url_rejects_credential_like_placeholders() {
        for name in [
            "QCG_PROVIDER_AUTH_XYZ",
            "QCG_PROVIDER_KEY_XYZ",
            "QCG_PROVIDER_PASSWORD_XYZ",
        ] {
            let mut spec = spec_with_base_url(
                "base-secret",
                &format!("https://example.test/v1/{{{name}}}"),
            );
            spec.api_key_env = None;
            let error = spec
                .validate()
                .expect_err("base URL must not interpolate credentials");
            assert!(error.contains("must not interpolate credential"), "{error}");
        }
    }

    #[test]
    fn base_url_rejects_the_configured_credential_env_without_name_heuristics() {
        let mut spec =
            spec_with_base_url("base-secret", "https://example.test/v1/{QCG_LLM_AUTH_XYZ}");
        spec.api_key_env = Some("QCG_LLM_AUTH_XYZ".into());

        let error = spec
            .validate()
            .expect_err("the configured credential must not be interpolated");
        assert!(error.contains("QCG_LLM_AUTH_XYZ"), "{error}");
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
        let mut remote = spec_with_base_url("remote", "http://example.test/v1");
        remote.api_key_env = Some("QCG_LLM_HTTP_REMOTE_KEY_XYZ".into());
        let provider = HttpProvider::from_spec(remote);
        let error = provider
            .configuration_error_for("remote")
            .expect("remote credentialed HTTP should be rejected");
        assert!(error.contains("loopback"), "{error}");

        // SAFETY: single-threaded test section; unique variable name.
        unsafe { std::env::set_var("QCG_LLM_HTTP_LOOPBACK_KEY_XYZ", "loopback-secret") };
        let mut loopback = spec_with_base_url("loopback", "http://127.0.0.7:8080/v1");
        loopback.api_key_env = Some("QCG_LLM_HTTP_LOOPBACK_KEY_XYZ".into());
        let provider = HttpProvider::from_spec(loopback);
        assert!(
            provider.configuration_error_for("loopback").is_none(),
            "loopback HTTP should be allowed"
        );
        // SAFETY: see above; restore the unique test variable.
        unsafe { std::env::remove_var("QCG_LLM_HTTP_LOOPBACK_KEY_XYZ") };
    }

    #[test]
    fn credential_env_names_expose_names_without_values() {
        // SAFETY: single-threaded test section; unique variable name.
        unsafe { std::env::set_var("QCG_LLM_ENV_NAME_ONLY_XYZ", "do-not-expose") };
        let mut spec = spec_with_base_url("keyed", "https://example.test/v1");
        spec.api_key_env = Some("QCG_LLM_ENV_NAME_ONLY_XYZ".into());
        let provider = HttpProvider::from_spec(spec);
        assert_eq!(
            provider.credential_env_names(),
            vec!["QCG_LLM_ENV_NAME_ONLY_XYZ".to_owned()]
        );
        // SAFETY: see above; restore the unique test variable.
        unsafe { std::env::remove_var("QCG_LLM_ENV_NAME_ONLY_XYZ") };
    }

    #[tokio::test]
    #[ignore = "requires loopback socket permissions"]
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
    #[ignore = "requires loopback socket permissions"]
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
    #[ignore = "requires loopback socket permissions"]
    async fn reflected_credential_is_never_returned() {
        let key = "qcg<reflected-credential-unique";
        // SAFETY: single-threaded test section; unique variable name.
        unsafe { std::env::set_var("QCG_LLM_REFLECTION_KEY_XYZ", key) };
        let body =
            r#"{"choices":[{"message":{"content":"qcg\u003creflected-credential-unique"}}]}"#
                .to_string();
        let (base_url, server) = spawn_http_response(200, body, "".into());
        let mut spec = spec_with_base_url("reflection", &base_url);
        spec.api_key_env = Some("QCG_LLM_REFLECTION_KEY_XYZ".into());
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
        unsafe { std::env::remove_var("QCG_LLM_REFLECTION_KEY_XYZ") };
        server.join().expect("test server should stop");
    }

    #[tokio::test]
    #[ignore = "requires loopback socket permissions"]
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

    #[test]
    fn decoded_json_reflection_scan_catches_escaped_credentials() {
        let value: Value = serde_json::from_str(
            r#"{"choices":[{"message":{"content":"qcg\u003creflected-credential"}}]}"#,
        )
        .expect("response should be valid JSON");
        assert!(json_contains_string_fragment(
            &value,
            "qcg<reflected-credential"
        ));
    }

    #[tokio::test]
    async fn reqwest_errors_do_not_expose_endpoint_url() {
        let provider = HttpProvider::from_spec(spec_with_base_url(
            "network",
            "http://127.0.0.1:9/qcg-llm-network-test",
        ));
        let mut request = sample_request();
        request.seed = None;

        let error = provider
            .complete(request)
            .await
            .expect_err("closed endpoint should fail");
        assert_eq!(error.kind, LlmErrorKind::Network);
        assert!(!error.message.contains("127.0.0.1"));
        assert!(!error.message.contains("qcg-llm-network-test"));
    }

    #[test]
    fn missing_api_key_is_reported_as_configuration_error() {
        let mut spec = spec_with_base_url("keyed", "https://example.invalid/v1");
        spec.api_key_env = Some("QCG_LLM_MISSING_KEY_XYZ".into());

        let provider = HttpProvider::from_spec(spec);

        let error = provider
            .configuration_error_for("keyed")
            .expect("missing key should be reported");
        assert!(error.contains("QCG_LLM_MISSING_KEY_XYZ"), "{error}");
        assert!(provider.configuration_error_for("other").is_none());
    }

    #[test]
    fn unresolved_base_url_placeholder_is_a_configuration_error() {
        let mut spec = spec_with_base_url(
            "cloudy",
            "https://api.example/accounts/{QCG_LLM_MISSING_ACCOUNT_XYZ}/ai/v1",
        );

        // Simulate an env override that is absent so the literal placeholder path runs.
        spec.base_url_env = Some("QCG_LLM_MISSING_OVERRIDE_XYZ".into());

        let provider = HttpProvider::from_spec(spec);

        let error = provider
            .configuration_error_for("cloudy")
            .expect("unresolved placeholder should be reported");
        assert!(error.contains("QCG_LLM_MISSING_ACCOUNT_XYZ"), "{error}");
    }

    #[test]
    fn providers_file_rejects_unknown_keys_and_duplicates() {
        let parsed = ProvidersFile::parse(
            r#"
[[provider]]
id = "a"
api = "chat_completions"
base_url = "https://example.invalid"

[[provider]]
id = "a"
api = "responses"
base_url = "https://example.invalid"
"#,
        );
        assert!(parsed.is_err());
        assert!(parsed.unwrap_err().contains("duplicate"));

        let parsed = ProvidersFile::parse(
            r#"
[[provider]]
id = "a"
api = "chat_completions"
base_url = "https://example.invalid"
unknown_field = true
"#,
        );
        assert!(parsed.is_err());

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
    fn providers_registry_accepts_explicit_large_entry_counts() {
        let mut entries = String::new();
        for index in 0..300 {
            entries.push_str(&format!(
                "[[provider]]\nid = \"provider-{index}\"\napi = \"chat_completions\"\nbase_url = \"https://example.invalid\"\n"
            ));
        }
        ProvidersFile::parse(&entries).expect("explicit large entry counts must validate");
    }

    #[test]
    fn providers_file_rejects_invalid_capability_and_transport_combinations() {
        for (source, expected) in [
            (
                r#"
[[provider]]
id = "typo"
api = "chat_completions"
base_url = "https://example.invalid"
capabilities = { reasonng_effort = ["high"] }
"#,
                "reasonng_effort",
            ),
            (
                r#"
[[provider]]
id = "anthropic-reasoning"
api = "anthropic_messages"
base_url = "https://example.invalid"
capabilities = { reasoning_effort = ["high"] }
"#,
                "anthropic_messages",
            ),
            (
                r#"
[[provider]]
id = "anthropic-combined"
api = "anthropic_messages"
base_url = "https://example.invalid"
capabilities = { tool_use = true, json_schema = true, structured_output_with_tools = true }
"#,
                "structured_output_with_tools",
            ),
            (
                r#"
[[provider]]
id = "chat-reasoning"
api = "chat_completions"
base_url = "https://example.invalid"
capabilities = { reasoning_effort = ["high"] }
"#,
                "max_completion_tokens",
            ),
            (
                r#"
[[provider]]
id = "responses-seed"
api = "responses"
base_url = "https://example.invalid"
capabilities = { seed = true }
"#,
                "seed",
            ),
            (
                r#"
[[provider]]
id = "chat-verbosity"
api = "chat_completions"
base_url = "https://example.invalid"
capabilities = { verbosity = true }
"#,
                "Responses API",
            ),
        ] {
            let error = ProvidersFile::parse(source)
                .expect_err("invalid provider combinations must fail validation");
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn providers_file_rejects_invalid_defaults_reserved_ids_and_zero_limits() {
        for (source, expected) in [
            (
                r#"
[default]
model = { provider = "missing", model = "model" }
"#,
                "unregistered provider",
            ),
            (
                r#"
[[provider]]
id = "fake"
api = "chat_completions"
base_url = "https://example.invalid"
"#,
                "reserved",
            ),
            (
                r#"
[[provider]]
id = "invalid id"
api = "chat_completions"
base_url = "https://example.invalid"
"#,
                "lowercase ASCII",
            ),
            (
                r#"
[[provider]]
id = "bounded"
api = "chat_completions"
base_url = "https://example.invalid"
response_body_limit_bytes = 0
"#,
                "response_body_limit_bytes",
            ),
            (
                r#"
[[provider]]
id = "retry-bounds"
api = "chat_completions"
base_url = "https://example.invalid"
retry_attempts = 0
"#,
                "retry_attempts",
            ),
            (
                r#"
[[provider]]
id = "retry-bounds"
api = "chat_completions"
base_url = "https://example.invalid"
retry_attempts = 11
"#,
                "retry_attempts",
            ),
            (
                r#"
[[provider]]
id = "retry-bounds"
api = "chat_completions"
base_url = "https://example.invalid"
retry_base_backoff_ms = 60001
"#,
                "retry_base_backoff_ms",
            ),
        ] {
            let error = ProvidersFile::parse(source)
                .expect_err("invalid provider registry invariants must fail");
            assert!(error.contains(expected), "{error}");
        }
        // Explicit large values have no mechanistic ceiling.
        for source in [
            r#"
[[provider]]
id = "bounded"
api = "chat_completions"
base_url = "https://example.invalid"
timeout_seconds = 604801
"#,
            r#"
[[provider]]
id = "bounded"
api = "chat_completions"
base_url = "https://example.invalid"
response_body_limit_bytes = 67108865
"#,
            r#"
[[provider]]
id = "bounded"
api = "chat_completions"
base_url = "https://example.invalid"
max_concurrency = 1025
"#,
        ] {
            ProvidersFile::parse(source).expect("explicit large provider values must validate");
        }
    }

    #[test]
    fn provider_reasoning_effort_levels_are_explicit() {
        let parsed = ProvidersFile::parse(
            r#"
[[provider]]
id = "reasoning"
api = "responses"
base_url = "https://example.invalid"
capabilities = { reasoning_effort = ["none", "high", "max"] }
"#,
        )
        .expect("reasoning provider should parse");
        assert_eq!(
            parsed.provider[0].capabilities.reasoning_effort,
            vec![
                ReasoningEffort::None,
                ReasoningEffort::High,
                ReasoningEffort::Max
            ]
        );
    }

    #[test]
    fn router_reports_configuration_errors_from_specs() {
        let router = LlmRouter::parse_text(
            r#"
[[provider]]
id = "keyed"
api = "chat_completions"
base_url = "https://example.invalid/v1"
api_key_env = "QCG_LLM_MISSING_KEY_XYZ"
"#,
        )
        .expect("router should build");

        let error = router
            .configuration_error_for("keyed")
            .expect("configuration error should surface");
        assert!(error.contains("QCG_LLM_MISSING_KEY_XYZ"), "{error}");
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
        for id in ["fake", "ollama", "lmstudio", "openai_compat"] {
            assert!(
                router.capabilities_for(id).is_some(),
                "{id} should stay active by default"
            );
        }
        for id in [
            "openai",
            "anthropic",
            "openrouter",
            "groq",
            "azure-openai",
            "opencode-zen",
        ] {
            assert!(
                router.capabilities_for(id).is_none(),
                "{id} must ship disabled until it is uncommented"
            );
        }
        assert!(router.default_model().is_none());
        assert_eq!(router.search_runtime().default_provider(), None);
        assert_eq!(
            router.search_runtime().provider_ids(),
            vec![
                "brave",
                "exa",
                "firecrawl",
                "parallel-advanced",
                "parallel-fast",
                "serpapi",
                "serper",
                "tavily",
                "tinyfish-api",
            ]
        );
        assert_eq!(
            router.mcp_runtime().server_ids(),
            vec!["exa-public", "parallel-public", "tinyfish"]
        );
        assert!(
            router
                .search_runtime()
                .credential_env_names()
                .contains(&"TINYFISH_API_KEY".to_string())
        );
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
        for id in [
            "anthropic",
            "openai",
            "openai_responses",
            "ollama",
            "lmstudio",
            "openai_compat",
            "openrouter",
            "gemini",
            "sakura",
            "cloudflare",
            "opencode-go",
            "opencode-zen",
            "opencode-go-responses",
            "opencode-zen-responses",
            "groq",
            "deepseek",
            "mistral",
            "xai",
            "together",
            "fireworks",
            "azure-openai",
            "fake",
        ] {
            assert!(
                router.capabilities_for(id).is_some(),
                "{id} should register once its row is enabled"
            );
        }
        for id in [
            "anthropic",
            "openai",
            "groq",
            "deepseek",
            "mistral",
            "xai",
            "together",
            "fireworks",
            "azure-openai",
        ] {
            assert!(
                text.contains(&format!("# id = \"{id}\"")),
                "{id} must remain present as an enable-able template"
            );
        }
    }

    #[test]
    fn load_prefers_explicit_path_and_lists_candidates_when_missing() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-llm-load-test-{}-{}",
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
    fn load_optional_treats_explicit_paths_as_authoritative() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-llm-load-optional-explicit-{}-{}",
            std::process::id(),
            uuid_like_suffix()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");

        let explicit = dir.join("present.toml");
        std::fs::write(&explicit, REGISTRY_FIXTURE).expect("registry should be written");
        let router = LlmRouter::load_optional(Some(
            Utf8PathBuf::from_path_buf(explicit.clone())
                .unwrap()
                .as_path(),
        ))
        .expect("explicit registry should load")
        .expect("explicit registry should resolve");
        assert!(router.capabilities_for("local").is_some());

        let missing = dir.join("missing.toml");
        let error =
            LlmRouter::load_optional(Some(Utf8PathBuf::from_path_buf(missing).unwrap().as_path()))
                .expect_err("missing explicit registry must stay a hard error");
        assert!(error.is_not_found());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_optional_honors_the_environment_override() {
        let dir = std::env::temp_dir().join(format!(
            "qcg-llm-load-optional-env-{}-{}",
            std::process::id(),
            uuid_like_suffix()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        let present = dir.join("present.toml");
        std::fs::write(&present, REGISTRY_FIXTURE).expect("registry should be written");
        let missing = dir.join("missing.toml");

        // SAFETY: single-threaded test binary section; restored below.
        unsafe { std::env::set_var("QCG_PROVIDERS", &present) };
        let router = LlmRouter::load_optional(None)
            .expect("configured env registry should load")
            .expect("env override should resolve");
        assert!(router.capabilities_for("local").is_some());

        unsafe { std::env::set_var("QCG_PROVIDERS", &missing) };
        let error = LlmRouter::load_optional(None)
            .expect_err("missing env registry must stay a hard error");
        assert!(error.is_not_found());

        // SAFETY: see above; the variable is removed to restore state.
        unsafe { std::env::remove_var("QCG_PROVIDERS") };

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
            mcp: qcg_mcp::McpRuntime::unavailable(),
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
            })
            .await
            .expect("router should retry once and succeed");
        assert_eq!(observed.calls.load(Ordering::SeqCst), 2);
        assert!(matches!(&response.content[0], ChatContent::Text(text) if text == "ok"));
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        let base = Duration::from_millis(200);
        assert_eq!(retry_backoff(1, base), Duration::from_millis(200));
        assert_eq!(retry_backoff(2, base), Duration::from_millis(400));
        assert_eq!(retry_backoff(3, base), Duration::from_millis(800));
        assert_eq!(retry_backoff(100, base), Duration::from_millis(51_200));
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
}
