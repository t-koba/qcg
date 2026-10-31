//! The provider's own hosted web search: the request shape, the derived
//! count, the replay state and the refusals. Every case is offline: no
//! provider is contacted and no credential exists.
use super::*;
use model::{ProviderSearchAction, ProviderWebSearch, SearchContextSize, UserLocation};

fn search_request() -> ChatRequest {
    let mut request = sample_request();
    request.provider_web_search = Some(ProviderWebSearch {
        allowed_domains: vec!["example.com".into()],
        blocked_domains: vec![],
        search_context_size: Some(SearchContextSize::Medium),
        user_location: Some(UserLocation {
            country: Some("JP".into()),
            region: None,
            city: None,
            timezone: None,
        }),
        max_calls: 3,
    });
    request
}

/// A response the provider would return after a hosted search: the calls it
/// made, the answer, and the citation it attached.
fn searched_response() -> Value {
    json!({
        "status": "completed",
        "output": [
            {
                "id": "ws_1",
                "type": "web_search_call",
                "action": { "type": "search", "queries": ["qcg", "bounded generation"] }
            },
            {
                "id": "ws_2",
                "type": "web_search_call",
                "action": { "type": "open_page", "url": "https://example.com/a" }
            },
            {
                "id": "ws_3",
                "type": "web_search_call",
                "action": { "type": "find_in_page", "url": "https://example.com/a", "pattern": "qcg" }
            },
            {
                "type": "message",
                "content": [{
                    "type": "output_text",
                    "text": "answer",
                    "annotations": [{
                        "type": "url_citation",
                        "url": "https://example.com/a",
                        "title": "Example",
                        "start_index": 0,
                        "end_index": 6
                    }]
                }]
            }
        ],
        "usage": { "input_tokens": 10, "output_tokens": 4 }
    })
}

#[test]
fn hosted_search_is_off_by_default() {
    let request = sample_request();
    assert!(
        request.provider_web_search.is_none(),
        "selecting a model must never enable the hosted search"
    );
    let payload = responses_payload(&request, None, None, false);
    assert!(
        payload.get("tools").is_none(),
        "no hosted tool without the opt-in: {payload}"
    );
    assert!(
        payload.get("max_tool_calls").is_none(),
        "no call bound without the opt-in: {payload}"
    );
}

#[test]
fn responses_payload_sends_the_hosted_search_tool() {
    let payload = responses_payload(&search_request(), None, None, false);
    assert_eq!(
        payload["tools"],
        json!([{
            "type": "web_search",
            "filters": { "allowed_domains": ["example.com"] },
            "search_context_size": "medium",
            "user_location": { "type": "approximate", "country": "JP" }
        }])
    );
    assert_eq!(payload["max_tool_calls"], json!(3));
}

#[test]
fn hosted_search_keeps_the_function_tools_and_the_call_bound() {
    let mut request = search_request();
    request.tools = vec![ToolSpec {
        name: "write_draft".into(),
        description: "Write a draft".into(),
        input_schema: json!({"type": "object"}),
    }];
    let payload = responses_payload(&request, None, None, false);
    assert_eq!(payload["tools"][0]["name"], json!("write_draft"));
    assert_eq!(payload["tools"][1]["type"], json!("web_search"));
    assert_eq!(payload["max_tool_calls"], json!(3));
}

#[test]
fn parses_hosted_search_calls_and_citations() {
    let response = parse_responses_response(
        searched_response(),
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect("the searched response should parse");
    let outcome = response
        .web_search
        .expect("the hosted search outcome should be reported");
    assert_eq!(outcome.calls, Some(1));
    assert_eq!(outcome.events.len(), 3);
    assert_eq!(outcome.events[2].action, ProviderSearchAction::FindInPage);
    assert_eq!(outcome.events[0].action, ProviderSearchAction::Search);
    assert_eq!(outcome.events[0].id.as_deref(), Some("ws_1"));
    assert_eq!(
        outcome.events[0].queries,
        vec!["qcg".to_string(), "bounded generation".to_string()]
    );
    assert_eq!(outcome.events[1].action, ProviderSearchAction::OpenPage);
    assert_eq!(
        outcome.events[1].url.as_deref(),
        Some("https://example.com/a")
    );
    assert_eq!(outcome.citations.len(), 1);
    assert_eq!(outcome.citations[0].url, "https://example.com/a");
    assert_eq!(outcome.citations[0].title.as_deref(), Some("Example"));
    assert_eq!(outcome.citations[0].start_index, Some(0));
    assert_eq!(outcome.citations[0].end_index, Some(6));
}

#[test]
fn openai_search_count_is_zero_with_no_search_actions() {
    let response = parse_responses_response(
        json!({
            "status": "completed",
            "output": [{
                "type": "message",
                "content": [{ "type": "output_text", "text": "answer" }]
            }],
            "usage": { "input_tokens": 4, "output_tokens": 1 }
        }),
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect("the response should parse");
    assert_eq!(
        response
            .web_search
            .expect("the hosted search outcome should be reported")
            .calls,
        Some(0),
        "a response without a hosted call reports zero calls"
    );
}

#[test]
fn hosted_search_count_is_unknown_without_an_output_array() {
    let response = parse_responses_response(
        json!({
            "status": "completed",
            "output_text": "answer",
            "usage": { "input_tokens": 4, "output_tokens": 1 }
        }),
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect("the response should parse");
    assert_eq!(
        response
            .web_search
            .expect("the hosted search outcome should be reported")
            .calls,
        None,
        "an unreported count is unknown, never zero"
    );
}

#[test]
fn hosted_search_is_absent_when_the_request_did_not_ask_for_it() {
    let response = parse_responses_response(
        searched_response(),
        false,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect("the response should parse");
    assert!(
        response.web_search.is_none(),
        "a request without the opt-in reports no hosted search"
    );
}

#[test]
fn hosted_search_carries_provider_state_for_the_replay() {
    let response = parse_responses_response(
        searched_response(),
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect("the searched response should parse");
    let state = response
        .provider_state
        .expect("the hosted call must be replayed with the next request");
    assert_eq!(state[0]["type"], json!("web_search_call"));

    let plain = parse_responses_response(
        json!({
            "status": "completed",
            "output": [{
                "type": "message",
                "content": [{ "type": "output_text", "text": "answer" }]
            }],
            "usage": { "input_tokens": 4, "output_tokens": 1 }
        }),
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect("the response should parse");
    assert!(
        plain.provider_state.is_none(),
        "a response without a hosted call replays no provider state"
    );
}

#[tokio::test]
async fn hosted_search_stream_outcome_matches_the_direct_parse() {
    let (events, _receiver) = mpsc::channel(8);
    let mut accumulator = HttpStreamAccumulator::new(
        ApiFlavor::Responses,
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect("the Responses accumulator should build");
    let streamed = accumulator
        .ingest(
            json!({ "type": "response.completed", "response": searched_response() }),
            &events,
        )
        .await
        .expect("the stream should ingest")
        .expect("the stream should end with a response");
    let direct = parse_responses_response(
        searched_response(),
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect("the searched response should parse");
    assert_eq!(streamed.web_search, direct.web_search);
}

#[tokio::test]
async fn openai_search_stream_falls_back_to_observed_search_actions() {
    let (events, _receiver) = mpsc::channel(8);
    let mut accumulator = HttpStreamAccumulator::new(
        ApiFlavor::Responses,
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect("the Responses accumulator should build");
    accumulator
        .ingest(
            json!({
                "type": "response.output_item.done",
                "item": {
                    "id": "ws_1",
                    "type": "web_search_call",
                    "action": { "type": "search", "query": "qcg" }
                }
            }),
            &events,
        )
        .await
        .expect("the stream should ingest");
    let streamed = accumulator
        .ingest(
            json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "output_text": "answer",
                    "usage": { "input_tokens": 4, "output_tokens": 1 }
                }
            }),
            &events,
        )
        .await
        .expect("the stream should ingest")
        .expect("the stream should end with a response");
    let outcome = streamed
        .web_search
        .expect("the hosted search outcome should be reported");
    assert_eq!(outcome.calls, Some(1));
    assert_eq!(outcome.events.len(), 1);
}

#[tokio::test]
async fn hosted_search_is_refused_on_a_non_responses_endpoint() {
    let router = LlmRouter::parse_text(&test_provider_row(
        "chat",
        "chat_completions",
        "https://example.invalid/v1",
        "",
    ))
    .expect("the registry should build");
    let provider = router
        .providers
        .get("chat")
        .expect("the provider should be registered");
    let mut request = sample_request();
    // The row advertises no sampling controls, so the request must not
    // carry any: the refusal under test is the hosted search's.
    request.seed = None;
    request.temperature = None;
    request.provider = "chat".into();
    request.provider_web_search = Some(ProviderWebSearch::default());
    let error = provider
        .complete(request)
        .await
        .expect_err("the hosted search must be refused before the request");
    assert!(
        error.message.contains("only on the Responses API"),
        "{error}"
    );
    assert!(!error.is_retryable(), "the refusal must not be retried");
}

#[tokio::test]
async fn hosted_search_is_refused_without_the_capability() {
    let router = LlmRouter::parse_text(&test_provider_row(
        "responses",
        "responses",
        "https://example.invalid/v1",
        "",
    ))
    .expect("the registry should build");
    let provider = router
        .providers
        .get("responses")
        .expect("the provider should be registered");
    let mut request = sample_request();
    // The row advertises no sampling controls, so the request must not
    // carry any: the refusal under test is the hosted search's.
    request.seed = None;
    request.temperature = None;
    request.provider = "responses".into();
    request.provider_web_search = Some(ProviderWebSearch::default());
    let error = provider
        .complete(request)
        .await
        .expect_err("the hosted search must be refused before the request");
    assert!(
        error
            .message
            .contains("does not support provider web search"),
        "{error}"
    );
    assert!(!error.is_retryable(), "the refusal must not be retried");
}

#[test]
fn a_malformed_hosted_call_is_an_error_not_a_panic() {
    let error = parse_responses_response(
        json!({
            "status": "completed",
            "output": [
                {
                    "type": "web_search_call",
                    "action": { "type": "teleport" }
                },
                {
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "answer" }]
                }
            ],
            "usage": { "input_tokens": 4, "output_tokens": 1 }
        }),
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .expect_err("an unknown action must be refused");
    assert!(error.message.contains("unknown action"), "{error}");
}

async fn stream_search_response(
    terminal: Value,
    billing: WebSearchBilling,
    items: Vec<Value>,
) -> ChatResponse {
    let (events, _receiver) = mpsc::channel(8);
    let mut accumulator = HttpStreamAccumulator::new(ApiFlavor::Responses, true, billing).unwrap();
    for item in items {
        assert!(
            accumulator
                .ingest(
                    json!({
                        "type": "response.output_item.done", "item": item
                    }),
                    &events
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    accumulator
        .ingest(
            json!({
                "type": "response.completed", "response": terminal
            }),
            &events,
        )
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn hosted_search_billing_modes_preserve_events_and_citations() {
    use WebSearchBilling::*;
    let expected = parse_responses_response(searched_response(), true, OpenaiSearchActions)
        .unwrap()
        .web_search
        .unwrap();
    for (billing, usage, calls) in [
        (
            OpenaiSearchActions,
            json!({"web_search": {"num_requests": 7}}),
            Some(1),
        ),
        (Unknown, json!({"web_search": {"num_requests": 7}}), None),
        (
            AzureNumRequests,
            json!({"web_search": {"num_requests": 7}}),
            Some(7),
        ),
        (
            AzureNumRequests,
            json!({"web_search": {"num_requests": 0}}),
            Some(0),
        ),
        (AzureNumRequests, Value::Null, None),
        (AzureNumRequests, json!({"web_search": {}}), None),
        (
            AzureNumRequests,
            json!({"web_search": {"num_requests": null}}),
            None,
        ),
        (
            AzureNumRequests,
            json!({"web_search": {"num_requests": -1}}),
            None,
        ),
        (
            AzureNumRequests,
            json!({"web_search": {"num_requests": 1.5}}),
            None,
        ),
        (
            AzureNumRequests,
            json!({"web_search": {"num_requests": 1.0}}),
            None,
        ),
        (
            AzureNumRequests,
            json!({"web_search": {"num_requests": "7"}}),
            None,
        ),
        (
            AzureNumRequests,
            json!({"web_search": {"num_requests": true}}),
            None,
        ),
    ] {
        let mut terminal = searched_response();
        terminal["tool_usage"] = usage;
        let items = terminal["output"].as_array().unwrap().clone();
        let direct = parse_responses_response(terminal.clone(), true, billing).unwrap();
        let outcome = direct.web_search.as_ref().unwrap();
        assert_eq!(outcome.calls, calls, "{billing:?}: {terminal}");
        assert_eq!(outcome.events, expected.events);
        assert_eq!(outcome.citations, expected.citations);
        // The full terminal output is authoritative, not added to streamed items.
        let streamed = stream_search_response(terminal.clone(), billing, items.clone()).await;
        assert_eq!(streamed.web_search, direct.web_search);
        // Usage remains authoritative even without terminal output. Actions/citations
        // survive independently of whether a billable count could be determined.
        terminal.as_object_mut().unwrap().remove("output");
        terminal["output_text"] = json!("answer");
        let direct = parse_responses_response(terminal.clone(), true, billing).unwrap();
        assert_eq!(
            direct.web_search.unwrap().calls,
            if billing == OpenaiSearchActions {
                None
            } else {
                calls
            }
        );
        let streamed = stream_search_response(terminal, billing, items).await;
        let outcome = streamed.web_search.unwrap();
        assert_eq!(outcome.calls, calls, "{billing:?}");
        assert_eq!(outcome.events, expected.events);
        assert_eq!(outcome.citations, expected.citations);
    }
}

#[tokio::test]
async fn hosted_search_terminal_output_controls_stream_fallback() {
    use WebSearchBilling::*;
    let fixture = searched_response();
    let items = fixture["output"].as_array().unwrap().clone();
    let mut terminal = fixture.clone();
    terminal["output"] = json!([items.last().unwrap()]); // No search action.
    for billing in [OpenaiSearchActions, AzureNumRequests, Unknown] {
        let direct = parse_responses_response(terminal.clone(), true, billing).unwrap();
        let streamed = stream_search_response(terminal.clone(), billing, items.clone()).await;
        assert_eq!(streamed.web_search, direct.web_search);
        assert!(streamed.web_search.unwrap().events.is_empty());
    }
    terminal.as_object_mut().unwrap().remove("output");
    terminal["output_text"] = json!("answer");
    for billing in [OpenaiSearchActions, AzureNumRequests, Unknown] {
        // Citation-only and completely absent stream output must not lose citations
        // or manufacture a zero count from the absence of search actions.
        for items in [vec![items.last().unwrap().clone()], vec![]] {
            let expected_citations = usize::from(!items.is_empty());
            let outcome = stream_search_response(terminal.clone(), billing, items)
                .await
                .web_search
                .unwrap();
            assert_eq!(outcome.calls, None);
            assert!(outcome.events.is_empty());
            assert_eq!(outcome.citations.len(), expected_citations);
        }
    }
}

#[test]
fn hosted_search_billing_config_is_explicit_and_responses_only() {
    for id in ["openai_responses", "azure-openai", "arbitrary-id"] {
        let row = test_provider_row(id, "responses", "https://api.openai.com/v1", "");
        let file = ProvidersFile::parse(&row).unwrap();
        assert_eq!(file.provider[0].web_search_billing, None);
    }
    for (name, mode) in [
        ("unknown", WebSearchBilling::Unknown),
        (
            "openai_search_actions",
            WebSearchBilling::OpenaiSearchActions,
        ),
        ("azure_num_requests", WebSearchBilling::AzureNumRequests),
    ] {
        for api in [
            "responses",
            "chat_completions",
            "anthropic_messages",
            "system_one",
        ] {
            let row = test_provider_row(
                "arbitrary-id",
                api,
                "https://example.invalid",
                &format!("web_search_billing = \"{name}\"\n"),
            );
            if api == "responses" {
                let file = ProvidersFile::parse(&row).unwrap();
                assert_eq!(file.provider[0].web_search_billing, Some(mode));
            } else {
                let error = ProvidersFile::parse(&row).unwrap_err();
                assert!(error.to_string().contains("web_search_billing"), "{error}");
            }
        }
    }
    assert!(
        ProvidersFile::parse(&test_provider_row(
            "p",
            "responses",
            "https://example.invalid",
            "web_search_billing = \"guess\"\n"
        ))
        .is_err()
    );
}

#[test]
fn bundled_responses_profiles_declare_their_search_billing_units() {
    let bundled = include_str!("../../../../providers.toml");
    for (id, mode) in [
        ("openai_responses", WebSearchBilling::OpenaiSearchActions),
        ("azure-openai", WebSearchBilling::AzureNumRequests),
    ] {
        let start = bundled
            .find(&format!("# [[provider]]\n# id = \"{id}\""))
            .unwrap();
        let text = bundled[start..]
            .split("\n\n")
            .next()
            .unwrap()
            .lines()
            .map(|line| line.strip_prefix("# ").unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let file = ProvidersFile::parse(&text).unwrap();
        assert_eq!(file.provider[0].web_search_billing, Some(mode));
    }
}

#[tokio::test]
async fn openai_search_count_uses_normalized_actions() {
    let mut terminal = searched_response();
    terminal["output"][0]["action"] = json!("search");
    terminal["output"][0]["query"] = json!("qcg");
    let direct = parse_responses_response(
        terminal.clone(),
        true,
        WebSearchBilling::OpenaiSearchActions,
    )
    .unwrap()
    .web_search
    .unwrap();
    assert_eq!(direct.calls, Some(1));
    assert_eq!(direct.events[0].queries, vec!["qcg"]);
    let items = terminal["output"].as_array().unwrap().clone();
    terminal.as_object_mut().unwrap().remove("output");
    terminal["output_text"] = json!("answer");
    let streamed =
        stream_search_response(terminal, WebSearchBilling::OpenaiSearchActions, items).await;
    assert_eq!(streamed.web_search.unwrap(), direct);
}
