use serde_json::Value;
use std::collections::BTreeMap;
use tokio::sync::mpsc;

use crate::parse::parse_responses_response;
use crate::provider::ApiFlavor;
use crate::types::{ChatContent, ChatResponse, ChatStreamEvent, LlmError, StopReason, TokenUsage};

/// Emits one ingest delta without ever blocking. The gate path drains only
/// between ingest calls, so a blocking send would deadlock the stream loop
/// on a fan-out chunk (many tool-call deltas in one SSE value) instead of
/// failing. A full channel fails closed with a loud error; a closed
/// receiver keeps its existing error.
fn send_ingest_event(
    events: &mpsc::Sender<ChatStreamEvent>,
    event: ChatStreamEvent,
) -> Result<(), LlmError> {
    use tokio::sync::mpsc::error::TrySendError;
    events.try_send(event).map_err(|error| match error {
        TrySendError::Closed(_) => LlmError::new("LLM stream receiver closed"),
        TrySendError::Full(_) => {
            LlmError::new("LLM provider stream fan-out exceeded the ingest channel capacity")
        }
    })
}

pub(crate) enum HttpStreamAccumulator {
    Chat(ChatCompletionAccumulator),
    Responses,
    Anthropic(AnthropicAccumulator),
}

impl HttpStreamAccumulator {
    pub(crate) fn new(api: ApiFlavor) -> Result<Self, LlmError> {
        Ok(match api {
            ApiFlavor::SystemOne => {
                return Err(LlmError::new("system_one does not support streaming"));
            }
            ApiFlavor::ChatCompletions => Self::Chat(ChatCompletionAccumulator::default()),
            ApiFlavor::Responses => Self::Responses,
            ApiFlavor::AnthropicMessages => Self::Anthropic(AnthropicAccumulator::default()),
        })
    }

    pub(crate) async fn ingest(
        &mut self,
        value: Value,
        events: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<Option<ChatResponse>, LlmError> {
        match self {
            Self::Chat(accumulator) => {
                accumulator.ingest(&value, events).await?;
                Ok(None)
            }
            Self::Responses => ingest_responses_stream(value, events).await,
            Self::Anthropic(accumulator) => {
                accumulator.ingest(&value, events).await?;
                Ok(None)
            }
        }
    }

    pub(crate) fn finish(self) -> Result<ChatResponse, LlmError> {
        match self {
            Self::Chat(accumulator) => accumulator.finish(),
            Self::Responses => Err(LlmError::invalid_response(
                "Responses API stream ended without terminal response",
            )),
            Self::Anthropic(accumulator) => accumulator.finish(),
        }
    }
}

#[derive(Default)]
struct PartialToolCall {
    id: String,
    name: String,
    arguments: String,
}

#[derive(Default)]
pub(crate) struct ChatCompletionAccumulator {
    text: String,
    tool_calls: BTreeMap<usize, PartialToolCall>,
    usage: Option<TokenUsage>,
    usage_invalid: bool,
    stop: Option<StopReason>,
    refused: bool,
}

impl ChatCompletionAccumulator {
    pub(crate) async fn ingest(
        &mut self,
        value: &Value,
        events: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        if let Some(usage) = value.get("usage").filter(|usage| !usage.is_null()) {
            match crate::usage::openai_usage(usage, true) {
                Ok(usage) => self.usage = Some(usage),
                Err(_) => self.usage_invalid = true,
            }
        }
        let Some(choice) = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return Ok(());
        };
        let delta = choice.get("delta").unwrap_or(&Value::Null);
        // Refusal deltas join the text stream but keep their classification:
        // like the direct parser, any nonempty refusal wins as Refusal at
        // finish regardless of finish_reason.
        for (field, is_refusal) in [("content", false), ("refusal", true)] {
            if let Some(text) = delta.get(field).and_then(Value::as_str)
                && !text.is_empty()
            {
                self.text.push_str(text);
                self.refused |= is_refusal;
                send_ingest_event(
                    events,
                    ChatStreamEvent::TextDelta {
                        text: text.to_string(),
                    },
                )?;
            }
        }
        for call in delta
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let index = call
                .get("index")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize;
            let partial = self.tool_calls.entry(index).or_default();
            if let Some(id) = call.get("id").and_then(Value::as_str) {
                partial.id.push_str(id);
            }
            if let Some(function) = call.get("function") {
                if let Some(name) = function.get("name").and_then(Value::as_str) {
                    partial.name.push_str(name);
                }
                if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                    partial.arguments.push_str(arguments);
                }
            }
        }
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            self.stop = Some(parse_openai_stop_reason(reason)?);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<ChatResponse, LlmError> {
        let mut content = Vec::new();
        if !self.text.is_empty() {
            content.push(ChatContent::Text(self.text));
        }
        for (_, call) in self.tool_calls {
            if call.id.is_empty() || call.name.is_empty() {
                return Err(LlmError::invalid_response(
                    "OpenAI-compatible stream returned an incomplete tool call",
                ));
            }
            let args = serde_json::from_str(&call.arguments).map_err(|_| {
                LlmError::invalid_response(
                    "OpenAI-compatible stream returned invalid tool arguments",
                )
            })?;
            content.push(ChatContent::ToolCall {
                id: call.id,
                name: call.name,
                args,
            });
        }
        if content.is_empty() {
            return Err(LlmError::invalid_response(
                "OpenAI-compatible stream did not include text or tool calls",
            ));
        }
        let stop = self.stop.ok_or_else(|| {
            LlmError::invalid_response("OpenAI-compatible stream did not include finish_reason")
        })?;
        // Direct-parser parity: a nonempty refusal classifies as Refusal even
        // when finish_reason says otherwise.
        let stop = if self.refused {
            StopReason::Refusal
        } else {
            stop
        };
        if self.usage_invalid {
            return Err(LlmError::invalid_response(
                "OpenAI-compatible stream returned invalid usage",
            ));
        }
        Ok(ChatResponse {
            content,
            usage: self.usage.ok_or_else(|| {
                LlmError::invalid_response("OpenAI-compatible stream did not include usage")
            })?,
            stop,
            provider_state: None,
        })
    }
}

fn parse_openai_stop_reason(reason: &str) -> Result<StopReason, LlmError> {
    match reason {
        "stop" => Ok(StopReason::EndTurn),
        "tool_calls" => Ok(StopReason::ToolUse),
        "length" => Ok(StopReason::MaxTokens),
        "content_filter" => Ok(StopReason::Refusal),
        _ => Err(LlmError::invalid_response(
            "OpenAI-compatible stream returned an unknown finish_reason",
        )),
    }
}

async fn ingest_responses_stream(
    value: Value,
    events: &mpsc::Sender<ChatStreamEvent>,
) -> Result<Option<ChatResponse>, LlmError> {
    match value.get("type").and_then(Value::as_str) {
        Some("response.output_text.delta") | Some("response.refusal.delta") => {
            if let Some(text) = value.get("delta").and_then(Value::as_str)
                && !text.is_empty()
            {
                send_ingest_event(
                    events,
                    ChatStreamEvent::TextDelta {
                        text: text.to_string(),
                    },
                )?;
            }
            Ok(None)
        }
        Some("response.completed") | Some("response.incomplete") => {
            let kind = value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("response.completed");
            value
                .get("response")
                .cloned()
                .ok_or_else(|| {
                    LlmError::invalid_response(format!("{kind} did not include response"))
                })
                .and_then(parse_responses_response)
                .map(Some)
        }
        _ => Ok(None),
    }
}

#[derive(Default)]
pub(crate) struct AnthropicAccumulator {
    text: String,
    tool_calls: BTreeMap<usize, PartialToolCall>,
    input_usage: Option<TokenUsage>,
    usage_invalid: bool,
    output_tokens: Option<u64>,
    stop: Option<StopReason>,
}

impl AnthropicAccumulator {
    pub(crate) async fn ingest(
        &mut self,
        value: &Value,
        events: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<(), LlmError> {
        match value.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                match crate::usage::anthropic_input(&value["message"]["usage"]) {
                    Ok(usage) => self.input_usage = Some(usage),
                    Err(_) => self.usage_invalid = true,
                }
                if value["message"]["usage"].get("output_tokens").is_some() {
                    match crate::usage::count(&value["message"]["usage"], "output_tokens", true) {
                        Ok(output) => self.output_tokens = Some(output),
                        Err(_) => self.usage_invalid = true,
                    }
                }
            }
            Some("content_block_start") => {
                let index = value
                    .get("index")
                    .and_then(Value::as_u64)
                    .unwrap_or_default() as usize;
                let block = value.get("content_block").unwrap_or(&Value::Null);
                if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                    let partial = self.tool_calls.entry(index).or_default();
                    partial.id = block
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    partial.name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                }
            }
            Some("content_block_delta") => {
                let delta = value.get("delta").unwrap_or(&Value::Null);
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") | Some("refusal_delta") => {
                        if let Some(text) = delta.get("text").and_then(Value::as_str) {
                            self.text.push_str(text);
                            send_ingest_event(
                                events,
                                ChatStreamEvent::TextDelta {
                                    text: text.to_string(),
                                },
                            )?;
                        }
                    }
                    Some("input_json_delta") => {
                        let index = value
                            .get("index")
                            .and_then(Value::as_u64)
                            .unwrap_or_default() as usize;
                        if let Some(json) = delta.get("partial_json").and_then(Value::as_str) {
                            self.tool_calls
                                .entry(index)
                                .or_default()
                                .arguments
                                .push_str(json);
                        }
                    }
                    _ => {}
                }
            }
            Some("message_delta") => {
                if let Some(previous) = &self.input_usage {
                    match crate::usage::anthropic_update(&value["usage"], previous) {
                        Ok(usage) => self.input_usage = Some(usage),
                        Err(_) => self.usage_invalid = true,
                    }
                }
                match crate::usage::count(&value["usage"], "output_tokens", true) {
                    Ok(output) => self.output_tokens = Some(output),
                    Err(_) => self.usage_invalid = true,
                }
                self.stop = value
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                    .map(parse_anthropic_stop_reason)
                    .transpose()?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<ChatResponse, LlmError> {
        if self.usage_invalid {
            return Err(LlmError::invalid_response(
                "Anthropic stream included invalid usage",
            ));
        }
        let mut content = Vec::new();
        let mut structured_response = false;
        if !self.text.is_empty() {
            content.push(ChatContent::Text(self.text));
        }
        for (_, call) in self.tool_calls {
            if call.id.is_empty() || call.name.is_empty() {
                return Err(LlmError::invalid_response(
                    "Anthropic stream returned an incomplete tool call",
                ));
            }
            let args: Value = serde_json::from_str(&call.arguments).map_err(|_| {
                LlmError::invalid_response("Anthropic stream returned invalid tool arguments")
            })?;
            if call.name == "response" {
                structured_response = true;
                content.push(ChatContent::Text(args.to_string()));
            } else {
                content.push(ChatContent::ToolCall {
                    id: call.id,
                    name: call.name,
                    args,
                });
            }
        }
        if content.is_empty() {
            return Err(LlmError::invalid_response(
                "Anthropic stream did not include text or tool calls",
            ));
        }
        if structured_response
            && content
                .iter()
                .any(|item| matches!(item, ChatContent::ToolCall { .. }))
        {
            return Err(LlmError::invalid_response(
                "Anthropic stream mixed response with external tool calls",
            ));
        }
        let stop = self.stop.ok_or_else(|| {
            LlmError::invalid_response("Anthropic stream did not include stop_reason")
        })?;
        let stop = if structured_response {
            if stop != StopReason::ToolUse {
                return Err(LlmError::invalid_response(
                    "Anthropic stream returned response without tool_use stop_reason",
                ));
            }
            StopReason::EndTurn
        } else {
            stop
        };
        Ok(ChatResponse {
            content,
            usage: TokenUsage {
                output: self.output_tokens.ok_or_else(|| {
                    LlmError::invalid_response("Anthropic stream did not include output usage")
                })?,
                ..self.input_usage.ok_or_else(|| {
                    LlmError::invalid_response("Anthropic stream did not include input usage")
                })?
            },
            stop,
            provider_state: None,
        })
    }
}

fn parse_anthropic_stop_reason(reason: &str) -> Result<StopReason, LlmError> {
    match reason {
        "end_turn" | "stop_sequence" => Ok(StopReason::EndTurn),
        "tool_use" => Ok(StopReason::ToolUse),
        "max_tokens" => Ok(StopReason::MaxTokens),
        "refusal" => Ok(StopReason::Refusal),
        _ => Err(LlmError::invalid_response(
            "Anthropic stream returned an unknown stop_reason",
        )),
    }
}

pub(crate) fn json_contains_string_fragment(value: &Value, fragment: &str) -> bool {
    match value {
        Value::String(value) => value.contains(fragment),
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string_fragment(value, fragment)),
        Value::Object(values) => values.iter().any(|(key, value)| {
            key.contains(fragment) || json_contains_string_fragment(value, fragment)
        }),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}
