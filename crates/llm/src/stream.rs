use model::{ProviderSearchAction, ProviderSearchEvent, UrlCitation};
use serde_json::Value;
use std::collections::BTreeMap;
use tokio::sync::mpsc;

use crate::parse::{
    is_web_search_call, parse_message_citations, parse_responses_response, parse_web_search_event,
};
use crate::provider::{ApiFlavor, WebSearchBilling};
use crate::types::{ChatContent, ChatResponse, ChatStreamEvent, LlmError, StopReason, TokenUsage};

/// Emits one ingest delta without ever blocking. The gate path drains only
/// between ingest calls, so a blocking send would deadlock the stream loop
/// on a fan-out chunk (many tool-call deltas in one SSE value) instead of
/// failing. A full channel fails closed with a loud error; a closed
/// receiver keeps its existing error.
fn send_text_delta(events: &mpsc::Sender<ChatStreamEvent>, text: &str) -> Result<(), LlmError> {
    use tokio::sync::mpsc::error::TrySendError;
    events
        .try_send(ChatStreamEvent::TextDelta {
            text: text.to_string(),
        })
        .map_err(|error| match error {
            TrySendError::Closed(_) => LlmError::new("LLM stream receiver closed"),
            TrySendError::Full(_) => {
                LlmError::new("LLM provider stream fan-out exceeded the ingest channel capacity")
            }
        })
}

pub(crate) enum HttpStreamAccumulator {
    Chat(ChatCompletionAccumulator),
    Responses(ResponsesAccumulator),
    Anthropic(AnthropicAccumulator),
}

impl HttpStreamAccumulator {
    pub(crate) fn new(
        api: ApiFlavor,
        provider_web_search: bool,
        billing: WebSearchBilling,
    ) -> Result<Self, LlmError> {
        Ok(match api {
            ApiFlavor::SystemOne => {
                return Err(LlmError::new("system_one does not support streaming"));
            }
            ApiFlavor::ChatCompletions => Self::Chat(ChatCompletionAccumulator::default()),
            ApiFlavor::Responses => Self::Responses(ResponsesAccumulator {
                provider_web_search,
                billing,
                ..ResponsesAccumulator::default()
            }),
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
            Self::Responses(accumulator) => accumulator.ingest(value, events).await,
            Self::Anthropic(accumulator) => {
                accumulator.ingest(&value, events).await?;
                Ok(None)
            }
        }
    }

    pub(crate) fn finish(self) -> Result<ChatResponse, LlmError> {
        match self {
            Self::Chat(accumulator) => accumulator.finish(),
            Self::Responses(_) => Err(LlmError::invalid_response(
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

impl PartialToolCall {
    fn finish(self, api: &str, allow_partial: bool) -> Result<(String, String, Value), LlmError> {
        if self.id.is_empty() || self.name.is_empty() {
            return Err(LlmError::invalid_response(format!(
                "{api} stream returned an incomplete tool call"
            )));
        }
        let args = serde_json::from_str(&self.arguments)
            .or_else(|error| {
                if allow_partial {
                    Ok(Value::String(self.arguments)) // Retain partial arguments, never executable.
                } else {
                    Err(error)
                }
            })
            .map_err(|_| {
                LlmError::invalid_response(format!("{api} stream returned invalid tool arguments"))
            })?;
        Ok((self.id, self.name, args))
    }
}

fn block_index(value: &Value) -> usize {
    value
        .get("index")
        .and_then(Value::as_u64)
        .unwrap_or_default() as usize
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
                send_text_delta(events, text)?;
            }
        }
        for call in delta
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let index = block_index(call);
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
            let (id, name, args) = call.finish("OpenAI-compatible", false)?;
            content.push(ChatContent::ToolCall { id, name, args });
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
        Ok(ChatResponse::new(
            content,
            self.usage.ok_or_else(|| {
                LlmError::invalid_response("OpenAI-compatible stream did not include usage")
            })?,
            stop,
        ))
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

/// Retains streamed hosted-search actions and citations when terminal output is
/// absent. Only OpenAI billing may derive calls from observed search actions;
/// other modes need provider usage even when action events are available.
#[derive(Default)]
pub(crate) struct ResponsesAccumulator {
    provider_web_search: bool,
    billing: WebSearchBilling,
    events: Vec<ProviderSearchEvent>,
    citations: Vec<UrlCitation>,
    calls: Option<u64>,
}

impl ResponsesAccumulator {
    async fn ingest(
        &mut self,
        value: Value,
        events: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<Option<ChatResponse>, LlmError> {
        match value.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") | Some("response.refusal.delta") => {
                if let Some(text) = value.get("delta").and_then(Value::as_str)
                    && !text.is_empty()
                {
                    send_text_delta(events, text)?;
                }
                Ok(None)
            }
            Some("response.output_item.done") => {
                self.ingest_output_item(&value)?;
                Ok(None)
            }
            Some("response.completed") | Some("response.incomplete") => {
                let kind = value
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("response.completed");
                let has_output = value
                    .pointer("/response/output")
                    .is_some_and(Value::is_array);
                let response = value
                    .get("response")
                    .cloned()
                    .ok_or_else(|| {
                        LlmError::invalid_response(format!("{kind} did not include response"))
                    })
                    .and_then(|response| {
                        parse_responses_response(response, self.provider_web_search, self.billing)
                    })?;
                Ok(Some(self.merge(response, has_output)))
            }
            _ => Ok(None),
        }
    }

    /// Reads one finished output item. The items are the same ones the
    /// terminal response carries, so the outcome does not depend on which
    /// of the two the provider sent.
    fn ingest_output_item(&mut self, value: &Value) -> Result<(), LlmError> {
        if !self.provider_web_search {
            return Ok(());
        }
        let Some(item) = value.get("item") else {
            return Ok(());
        };
        if is_web_search_call(item) {
            let event = parse_web_search_event(item)?;
            if self.billing == WebSearchBilling::OpenaiSearchActions
                && event.action == ProviderSearchAction::Search
            {
                self.calls = Some(self.calls.unwrap_or_default().saturating_add(1));
            }
            self.events.push(event);
        }
        if item.get("type").and_then(Value::as_str) == Some("message") {
            self.citations.extend(parse_message_citations(item)?);
        }
        Ok(())
    }

    /// The terminal response is authoritative when it carries the output
    /// array. Without one, the items the stream reported are the outcome.
    fn merge(&self, mut response: ChatResponse, has_output: bool) -> ChatResponse {
        if let Some(outcome) = response.web_search.as_mut()
            && !has_output
        {
            if self.billing == WebSearchBilling::OpenaiSearchActions {
                outcome.calls = self.calls;
            }
            outcome.events = self.events.clone();
            outcome.citations = self.citations.clone();
        }
        response
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
                let index = block_index(value);
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
                            send_text_delta(events, text)?;
                        }
                    }
                    Some("input_json_delta") => {
                        let index = block_index(value);
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
                    .map(crate::parse::parse_anthropic_stop_reason)
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
        let stop = self.stop.ok_or_else(|| {
            LlmError::invalid_response("Anthropic stream did not include stop_reason")
        })?;
        let mut content = Vec::new();
        let mut structured_response = false;
        if !self.text.is_empty() {
            content.push(ChatContent::Text(self.text));
        }
        for (_, call) in self.tool_calls {
            let (id, name, args) =
                call.finish("Anthropic", stop == StopReason::ModelContextWindowExceeded)?;
            if name == "response" {
                structured_response = true;
                content.push(ChatContent::Text(args.to_string()));
            } else {
                content.push(ChatContent::ToolCall { id, name, args });
            }
        }
        if content.is_empty() && stop != StopReason::ModelContextWindowExceeded {
            return Err(LlmError::invalid_response(
                "Anthropic stream did not include text or tool calls",
            ));
        }
        if stop != StopReason::ModelContextWindowExceeded
            && structured_response
            && content
                .iter()
                .any(|item| matches!(item, ChatContent::ToolCall { .. }))
        {
            return Err(LlmError::invalid_response(
                "Anthropic stream mixed response with external tool calls",
            ));
        }
        let stop = crate::parse::anthropic_structured_stop(stop, structured_response)?;
        Ok(ChatResponse::new(
            content,
            TokenUsage {
                output: self.output_tokens.ok_or_else(|| {
                    LlmError::invalid_response("Anthropic stream did not include output usage")
                })?,
                ..self.input_usage.ok_or_else(|| {
                    LlmError::invalid_response("Anthropic stream did not include input usage")
                })?
            },
            stop,
        ))
    }
}
