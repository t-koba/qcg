#[test]
fn run_event_reference_matches_generated_markdown() {
    let docs_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("docs/run-event-reference.md");
    let docs = std::fs::read_to_string(docs_path).unwrap();
    let start = "<!-- run-events:start -->";
    let end = "<!-- run-events:end -->";
    let block = docs
        .split_once(start)
        .and_then(|(_, tail)| tail.split_once(end).map(|(block, _)| block))
        .expect("run event reference must contain generated markers");
    let expected = format!("\n{}", api::run_event_reference_markdown());
    assert_eq!(block.trim_end(), expected.trim_end());
    assert!(block.contains("`web_search` (`calls`, `cost_microusd`, `events`, `citations`)"));
    assert!(docs.contains("web_search.calls * web_search_call_cost_usd"));
}

#[test]
fn cache_creation_usage_is_reported_in_event_schema_and_roundtrips() {
    let usage: api::TokenUsageEventData = serde_json::from_value(serde_json::json!({
        "input": 165, "output": 10, "cached_input": 60, "cache_creation_input": 5
    }))
    .expect("split normalized usage");
    assert_eq!(usage.cache_creation_input, 5);
    assert_eq!(
        serde_json::to_value(&usage).unwrap()["cache_creation_input"],
        5
    );
    let absent: api::TokenUsageEventData = serde_json::from_value(serde_json::json!({
        "input": 100, "output": 10
    }))
    .expect("optional cache counters default to zero");
    assert_eq!(absent.cache_creation_input, 0);
    let components = api::openapi_components();
    assert!(
        components
            .pointer("/schemas/TokenUsageEventData/properties/cache_creation_input")
            .is_some()
    );
    assert!(
        components
            .pointer("/schemas/RunMetrics/properties/tokens_cache_creation_input")
            .is_some()
    );
    for field in [
        "cache_read_input_cost_per_million_usd",
        "cache_creation_input_cost_per_million_usd",
    ] {
        assert!(
            components
                .pointer(&format!("/schemas/LlmCatalogModel/properties/{field}"))
                .is_some()
        );
    }
}

#[test]
fn catalog_tier_schedule_schema_is_typed_nullable_and_defaults_empty() {
    let components = api::openapi_components();
    let schedule = &components["schemas"]["LlmCatalogModel"]["properties"]["cost_tiers"];
    assert_eq!(schedule["default"], serde_json::json!([]));
    assert_eq!(
        schedule["items"]["$ref"],
        "#/components/schemas/LlmCatalogCostTier"
    );
    let tier = &components["schemas"]["LlmCatalogCostTier"];
    assert_eq!(tier["properties"]["threshold_tokens"]["type"], "integer");
    for field in [
        "input_cost_per_million_usd",
        "cache_read_input_cost_per_million_usd",
        "cache_creation_input_cost_per_million_usd",
        "output_cost_per_million_usd",
    ] {
        assert_eq!(
            tier["properties"][field]["type"],
            serde_json::json!(["number", "null"])
        );
    }
}
