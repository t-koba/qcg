use super::*;

fn chat(usage: Value) -> Value {
    json!({"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":usage})
}
fn responses(usage: Value) -> Value {
    json!({"status":"completed","output_text":"ok","usage":usage})
}
fn anthropic(usage: Value) -> Value {
    json!({"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":usage})
}
async fn stream_chat(usage: Value) -> Result<ChatResponse, LlmError> {
    let (events, _receiver) = mpsc::channel(4);
    let mut stream = ChatCompletionAccumulator::default();
    stream
        .ingest(
            &json!({"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}],"usage":usage}),
            &events,
        )
        .await?;
    stream.finish()
}
async fn stream_responses(usage: Value) -> Result<ChatResponse, LlmError> {
    let (events, _receiver) = mpsc::channel(4);
    HttpStreamAccumulator::new(ApiFlavor::Responses)?
        .ingest(
            json!({"type":"response.completed","response":responses(usage)}),
            &events,
        )
        .await?
        .ok_or_else(|| LlmError::new("missing response"))
}
async fn stream_anthropic(usage: Value, delta: Value) -> Result<ChatResponse, LlmError> {
    let (events, _receiver) = mpsc::channel(4);
    let mut stream = crate::stream::AnthropicAccumulator::default();
    stream
        .ingest(
            &json!({"type":"message_start","message":{"usage":usage}}),
            &events,
        )
        .await?;
    stream
        .ingest(
            &json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"ok"}}),
            &events,
        )
        .await?;
    stream
        .ingest(
            &json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":delta}),
            &events,
        )
        .await?;
    stream.finish()
}
fn assert_usage(usage: &TokenUsage, input: u64, read: u64, creation: u64) {
    assert_eq!(usage.input, input);
    assert_eq!(usage.cached_input, read);
    assert_eq!(usage.cache_creation_input, creation);
    assert_eq!(usage.ordinary_input().unwrap(), input - read - creation);
    assert_eq!(usage.output, 10);
}

#[tokio::test]
async fn inclusive_and_split_cache_usage_stream_parity() {
    for (read, creation) in [
        (None, None),
        (Some(0), Some(0)),
        (Some(40), Some(10)),
        (Some(100), Some(0)),
    ] {
        let read_count = read.unwrap_or(0);
        let creation_count = creation.unwrap_or(0);
        let mut details = json!({});
        if let Some(read) = read {
            details["cached_tokens"] = json!(read);
        }
        if let Some(creation) = creation {
            details["cache_write_tokens"] = json!(creation);
        }
        let mut usage = json!({"prompt_tokens":100,"completion_tokens":10});
        if read.is_some() || creation.is_some() {
            usage["prompt_tokens_details"] = details.clone();
        }
        assert_usage(
            &parse_chat_completions_response(chat(usage.clone()))
                .unwrap()
                .usage,
            100,
            read_count,
            creation_count,
        );
        assert_usage(
            &stream_chat(usage).await.unwrap().usage,
            100,
            read_count,
            creation_count,
        );
        let mut usage = json!({"input_tokens":100,"output_tokens":10});
        if read.is_some() || creation.is_some() {
            usage["input_tokens_details"] = details;
        }
        assert_usage(
            &parse_responses_response(responses(usage.clone()))
                .unwrap()
                .usage,
            100,
            read_count,
            creation_count,
        );
        assert_usage(
            &stream_responses(usage).await.unwrap().usage,
            100,
            read_count,
            creation_count,
        );
    }
    for (read, creation) in [(None, None), (Some(0), Some(0)), (Some(60), Some(5))] {
        let mut usage = json!({"input_tokens":100,"output_tokens":10});
        if let Some(read) = read {
            usage["cache_read_input_tokens"] = json!(read);
        }
        if let Some(creation) = creation {
            usage["cache_creation_input_tokens"] = json!(creation);
        }
        let total = 100 + read.unwrap_or(0) + creation.unwrap_or(0);
        assert_usage(
            &parse_anthropic_response(anthropic(usage.clone()))
                .unwrap()
                .usage,
            total,
            read.unwrap_or(0),
            creation.unwrap_or(0),
        );
        assert_usage(
            &stream_anthropic(usage.clone(), json!({"output_tokens":10}))
                .await
                .unwrap()
                .usage,
            total,
            read.unwrap_or(0),
            creation.unwrap_or(0),
        );
        // Final cumulative counts replace, not add to, message-start counts.
        assert_usage(
            &stream_anthropic(usage.clone(), usage).await.unwrap().usage,
            total,
            read.unwrap_or(0),
            creation.unwrap_or(0),
        );
    }
}

#[tokio::test]
async fn malformed_or_inconsistent_cache_counts_fail_closed_in_all_parsers() {
    for field in ["cached_tokens", "cache_write_tokens"] {
        for bad in [
            json!(null),
            json!(-1),
            json!(0.5),
            json!("4"),
            json!(true),
            json!(101),
            json!(u64::MAX),
        ] {
            let mut details = json!({});
            details[field] = bad.clone();
            let usage = json!({
                "prompt_tokens":100,
                "completion_tokens":10,
                "prompt_tokens_details":details
            });
            assert!(
                parse_chat_completions_response(chat(usage.clone())).is_err(),
                "{usage}"
            );
            assert!(stream_chat(usage).await.is_err());
            let mut details = json!({});
            details[field] = bad;
            let usage = json!({
                "input_tokens":100,
                "output_tokens":10,
                "input_tokens_details":details
            });
            assert!(
                parse_responses_response(responses(usage.clone())).is_err(),
                "{usage}"
            );
            assert!(stream_responses(usage).await.is_err());
        }
    }
    for (read, creation) in [(70, 31), (u64::MAX, 0), (0, u64::MAX)] {
        let usage = json!({
            "prompt_tokens":100,
            "completion_tokens":10,
            "prompt_tokens_details":{"cached_tokens":read,"cache_write_tokens":creation}
        });
        assert!(parse_chat_completions_response(chat(usage.clone())).is_err());
        assert!(stream_chat(usage).await.is_err());
        let usage = json!({
            "input_tokens":100,
            "output_tokens":10,
            "input_tokens_details":{"cached_tokens":read,"cache_write_tokens":creation}
        });
        assert!(parse_responses_response(responses(usage.clone())).is_err());
        assert!(stream_responses(usage).await.is_err());
    }
    for field in ["cache_read_input_tokens", "cache_creation_input_tokens"] {
        for bad in [
            json!(null),
            json!(-1),
            json!(0.5),
            json!("4"),
            json!(true),
            json!(u64::MAX),
        ] {
            let mut usage = json!({"input_tokens":100,"output_tokens":10});
            usage[field] = bad.clone();
            assert!(
                parse_anthropic_response(anthropic(usage.clone())).is_err(),
                "{usage}"
            );
            assert!(
                stream_anthropic(usage, json!({"output_tokens":10}))
                    .await
                    .is_err()
            );
            let mut delta = json!({"output_tokens":10});
            delta[field] = bad;
            assert!(
                stream_anthropic(json!({"input_tokens":100}), delta)
                    .await
                    .is_err()
            );
        }
    }
    // Even when both subsets fit individually, their sum must fit and not overflow.
    for (total, read, creation) in [(100, 60, 50), (u64::MAX, u64::MAX, 1)] {
        assert!(
            TokenUsage {
                input: total,
                cached_input: read,
                cache_creation_input: creation,
                ..Default::default()
            }
            .ordinary_input()
            .is_err()
        );
    }
    let max = json!({"input_tokens":u64::MAX,"cache_read_input_tokens":0,"cache_creation_input_tokens":0,"output_tokens":10});
    assert_usage(
        &parse_anthropic_response(anthropic(max.clone()))
            .unwrap()
            .usage,
        u64::MAX,
        0,
        0,
    );
    assert_usage(
        &stream_anthropic(max, json!({"output_tokens":10}))
            .await
            .unwrap()
            .usage,
        u64::MAX,
        0,
        0,
    );
}

#[test]
fn cache_rates_validate_and_preserve_explicit_zero_in_model_config() {
    for field in [
        "cache_read_input_cost_per_million_usd",
        "cache_creation_input_cost_per_million_usd",
    ] {
        for rate in ["-1.0", "nan", "inf"] {
            let extra = format!("\n[[provider.models]]\nid = \"m\"\n{field} = {rate}\n");
            assert!(
                ProvidersFile::parse(&test_provider_row(
                    "openai",
                    "chat_completions",
                    "https://example.invalid",
                    &extra
                ))
                .is_err()
            );
        }
    }
    let extra = "\n[[provider.models]]\nid = \"m\"\ninput_cost_per_million_usd = 2.0\noutput_cost_per_million_usd = 3.0\ncache_read_input_cost_per_million_usd = 0.0\ncache_creation_input_cost_per_million_usd = 4.0\n";
    let file = ProvidersFile::parse(&test_provider_row(
        "openai",
        "chat_completions",
        "https://example.invalid",
        extra,
    ))
    .unwrap();
    let pricing = file.provider[0].pricing_for_model("m").unwrap();
    assert_eq!(pricing.cache_read_input_cost_per_million_usd, Some(0.0));
    assert_eq!(pricing.cache_creation_input_cost_per_million_usd, Some(4.0));
}

#[tokio::test]
async fn malformed_usage_details_and_start_output_cannot_be_overwritten() {
    for details in [json!(null), json!(false), json!("4"), json!([])] {
        let usage =
            json!({"prompt_tokens":100,"completion_tokens":10,"prompt_tokens_details":details});
        assert!(parse_chat_completions_response(chat(usage.clone())).is_err());
        assert!(stream_chat(usage).await.is_err());
        let usage = json!({"input_tokens":100,"output_tokens":10,"input_tokens_details":details});
        assert!(parse_responses_response(responses(usage.clone())).is_err());
        assert!(stream_responses(usage).await.is_err());
    }
    assert!(
        stream_anthropic(
            json!({"input_tokens":100,"output_tokens":-1}),
            json!({"output_tokens":10})
        )
        .await
        .is_err()
    );
}
