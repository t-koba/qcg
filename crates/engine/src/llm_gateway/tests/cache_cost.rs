use super::*;

struct CacheMeteredProvider;

#[async_trait]
impl LlmProvider for CacheMeteredProvider {
    fn id(&self) -> &str {
        "cache-metered"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, _: ChatRequest) -> Result<ChatResponse, LlmError> {
        Ok(ChatResponse {
            content: vec![ChatContent::Text("ok".into())],
            usage: TokenUsage {
                input: 1_000_000,
                output: 10,
                cached_input: 400_000,
                cache_creation_input: 200_000,
                reasoning: 0,
            },
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search: None,
        })
    }
}

fn cache_pricing(read: Option<f64>, creation: Option<f64>) -> ModelRef {
    ModelRef {
        provider: "cache-metered".into(),
        model: "test".into(),
        input_cost_per_million_usd: Some(2.0),
        output_cost_per_million_usd: Some(3.0),
        cache_read_input_cost_per_million_usd: read,
        cache_creation_input_cost_per_million_usd: creation,
        web_search_call_cost_usd: None,
        cost_tiers: Vec::new(),
    }
}

#[tokio::test]
async fn cache_categories_price_once_and_enforce_post_usage_budget() {
    for (read, creation, expected) in [
        (Some(0.5), Some(4.0), 1_800_030),
        (None, None, 2_000_030),
        (Some(0.0), Some(0.0), 800_030),
        (Some(0.0), None, 1_200_030),
        (None, Some(0.0), 1_600_030),
    ] {
        for over_budget in [false, true] {
            let dir = test_dir("cache-budget");
            let journal = test_journal(&dir, &format!("cache-{}", uuid::Uuid::now_v7()));
            let secrets = SecretStore::default();
            let pricing = cache_pricing(read, creation);
            let gateway = LlmGateway::new(
                Arc::new(CacheMeteredProvider),
                &secrets,
                &journal,
                CancellationToken::new(),
                LlmCostBudget {
                    max_tokens: None,
                    max_cost_microusd: Some(expected - u64::from(over_budget)),
                    require_pricing: true,
                },
                vec![],
            );
            let mut request = test_request();
            request.provider = "cache-metered".into();
            let result = gateway
                .complete(&test_node(), request, &[pricing], |_| json!({}))
                .await;
            if over_budget {
                assert!(
                    matches!(result,Err(StepError::BudgetExceeded {resource:"cost_microusd",used,limit}) if used==expected && limit==expected-1)
                );
            } else {
                result.expect("exact budget must pass");
            }
            let budget = journal.state().budget;
            assert_eq!(budget.cost_microusd, expected);
            assert_eq!(budget.tokens_input, 1_000_000);
            assert_eq!(budget.tokens_cached_input, 400_000);
            assert_eq!(budget.tokens_cache_creation_input, 200_000);
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

#[tokio::test]
async fn admission_uses_resolved_route_and_highest_cache_input_rate() {
    let dir = test_dir("cache-admission");
    let journal = test_journal(&dir, &format!("cache-admission-{}", uuid::Uuid::now_v7()));
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(CacheMeteredProvider),
        &secrets,
        &journal,
        CancellationToken::new(),
        LlmCostBudget {
            max_tokens: None,
            max_cost_microusd: Some(0),
            require_pricing: true,
        },
        vec![],
    );
    let mut request = test_request();
    request.provider = "cache-metered".into();
    let input_est = (serde_json::to_vec(&request).unwrap().len() as u64).div_ceil(4);
    let ordinary_rate_estimate =
        (input_est as f64 * 2.0 + u64::from(request.max_tokens) as f64 * 3.0).ceil() as u64;
    let cache_creation_rate_estimate =
        (input_est as f64 * 4.0 + u64::from(request.max_tokens) as f64 * 3.0).ceil() as u64;
    assert!(cache_creation_rate_estimate > ordinary_rate_estimate);
    let mut pricing = cache_pricing(Some(0.5), Some(4.0));
    pricing.provider = request.provider.clone();
    let result = gateway
        .complete(&test_node(), request, &[pricing], |_| json!({}))
        .await;
    assert!(matches!(
        result,
        Err(StepError::BudgetExceeded {
            resource: "cost_microusd",
            used,
            limit: 0,
        }) if used == cache_creation_rate_estimate
    ));
    assert_eq!(journal.state().budget.llm_calls, 0);
    assert_eq!(journal.state().budget.cost_microusd, 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn cache_cost_validates_counts_and_does_not_mix_pricing_rows() {
    let dir = test_dir("cache-pricing");
    let journal = test_journal(&dir, "cache-pricing");
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(CacheMeteredProvider),
        &secrets,
        &journal,
        CancellationToken::new(),
        LlmCostBudget {
            max_tokens: None,
            max_cost_microusd: None,
            require_pricing: true,
        },
        vec![cache_pricing(Some(0.0), Some(0.0))],
    );
    let usage = TokenUsage {
        input: 100,
        output: 10,
        cached_input: 40,
        cache_creation_input: 20,
        reasoning: 0,
    };
    let cost = |routes: &[ModelRef], usage: &TokenUsage| {
        gateway.cost_microusd(
            &test_node(),
            routes,
            "cache-metered",
            "test",
            usage,
            None,
            None,
        )
    };
    // A complete invocation row wins as a whole: its omitted cache rates
    // fall back to its own input rate, not the discounted registry row.
    assert_eq!(cost(&[cache_pricing(None, None)], &usage).unwrap(), 230);
    assert_eq!(cost(&[], &usage).unwrap(), 110);
    for (input, read, creation) in [(100, 80, 30), (u64::MAX, u64::MAX, 1)] {
        assert!(
            cost(
                &[],
                &TokenUsage {
                    input,
                    cached_input: read,
                    cache_creation_input: creation,
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
    let mut incomplete = cache_pricing(Some(0.0), Some(0.0));
    incomplete.output_cost_per_million_usd = None;
    let no_defaults = LlmGateway::new(
        Arc::new(CacheMeteredProvider),
        &secrets,
        &journal,
        CancellationToken::new(),
        LlmCostBudget {
            max_tokens: None,
            max_cost_microusd: None,
            require_pricing: true,
        },
        vec![],
    );
    assert!(
        no_defaults
            .cost_microusd(
                &test_node(),
                &[incomplete],
                "cache-metered",
                "test",
                &usage,
                None,
                None
            )
            .is_err()
    );
    // Missing pricing must not bypass validation of malformed provider usage.
    let unpriced = test_gateway(Arc::new(CacheMeteredProvider), &secrets, &journal);
    assert!(
        unpriced
            .cost_microusd(
                &test_node(),
                &[],
                "unknown",
                "test",
                &TokenUsage {
                    input: 1,
                    cached_input: 2,
                    ..Default::default()
                },
                None,
                None,
            )
            .is_err()
    );
    let _ = std::fs::remove_dir_all(dir);
}
