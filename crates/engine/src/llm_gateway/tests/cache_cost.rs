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

struct CountingCacheProvider {
    calls: std::sync::atomic::AtomicUsize,
    usage: TokenUsage,
}

#[async_trait]
impl LlmProvider for CountingCacheProvider {
    fn id(&self) -> &str {
        "cache-metered"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, request: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut response = CacheMeteredProvider.complete(request).await?;
        response.usage = self.usage.clone();
        Ok(response)
    }
}

fn tier_without_base_input_pricing() -> ModelRef {
    let mut route = cache_pricing(None, None);
    route.input_cost_per_million_usd = None;
    route.cost_tiers = vec![policy::CostTier {
        threshold_tokens: 1,
        input_cost_per_million_usd: Some(5.0),
        output_cost_per_million_usd: Some(3.0),
        cache_read_input_cost_per_million_usd: None,
        cache_creation_input_cost_per_million_usd: None,
    }];
    route
}

#[tokio::test]
async fn required_cache_pricing_is_checked_per_row_before_dispatch() {
    // Tier maxima must not hide missing cache prices in the base row.
    for (read, creation, tier_caches) in [
        (None, None, false),
        (None, None, true),
        (Some(0.5), None, true),
        (None, Some(4.0), true),
    ] {
        for require_pricing in [true, false] {
            let dir = test_dir("unpriced-cache-admission");
            let journal = test_journal(&dir, &format!("cache-{}", uuid::Uuid::now_v7()));
            let secrets = SecretStore::default();
            let provider = Arc::new(CountingCacheProvider {
                calls: Default::default(),
                usage: TokenUsage {
                    input: 100,
                    output: 10,
                    ..Default::default()
                },
            });
            let mut route = tier_without_base_input_pricing();
            route.cache_read_input_cost_per_million_usd = read;
            route.cache_creation_input_cost_per_million_usd = creation;
            if tier_caches {
                route.cost_tiers[0].cache_read_input_cost_per_million_usd = Some(0.5);
                route.cost_tiers[0].cache_creation_input_cost_per_million_usd = Some(4.0);
            }
            let gateway = LlmGateway::new(
                provider.clone(),
                &secrets,
                &journal,
                CancellationToken::new(),
                LlmCostBudget {
                    max_tokens: None,
                    max_cost_microusd: Some(10_000_000),
                    require_pricing,
                },
                vec![],
            );
            let mut request = test_request();
            request.provider = "cache-metered".into();
            let result = gateway
                .complete(&test_node(), request, &[route], |_| json!({}))
                .await;
            if require_pricing {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("lacks cache input pricing")
                );
            } else {
                result.expect("optional pricing preserves admission using the tier maximum");
            }
            assert_eq!(
                provider.calls.load(std::sync::atomic::Ordering::SeqCst),
                usize::from(!require_pricing)
            );
            assert_eq!(
                journal.state().budget.llm_calls,
                u64::from(!require_pricing)
            );
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

#[tokio::test]
async fn required_cache_pricing_rejects_used_unpriced_categories_without_a_cost_cap() {
    for (read, creation) in [(40, 0), (0, 20), (40, 20), (0, 0)] {
        for base_input in [None, Some(2.0)] {
            for require_pricing in [true, false] {
                let dir = test_dir("unpriced-cache-usage");
                let journal = test_journal(&dir, &format!("cache-{}", uuid::Uuid::now_v7()));
                let secrets = SecretStore::default();
                let provider = Arc::new(CountingCacheProvider {
                    calls: Default::default(),
                    usage: TokenUsage {
                        input: 100,
                        output: 10,
                        cached_input: read,
                        cache_creation_input: creation,
                        reasoning: 0,
                    },
                });
                let mut route = tier_without_base_input_pricing();
                route.input_cost_per_million_usd = base_input;
                let gateway = LlmGateway::new(
                    provider.clone(),
                    &secrets,
                    &journal,
                    CancellationToken::new(),
                    LlmCostBudget {
                        max_tokens: None,
                        max_cost_microusd: None,
                        require_pricing,
                    },
                    vec![],
                );
                let mut request = test_request();
                request.provider = "cache-metered".into();
                let result = gateway
                    .complete(&test_node(), request, &[route], |_| json!({}))
                    .await;
                let unpriced_usage = require_pricing && base_input.is_none() && read + creation > 0;
                let budget = journal.state().budget;
                if unpriced_usage {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("incomplete pricing")
                    );
                    assert_eq!(
                        budget.llm_calls, 0,
                        "unpriced usage must not be journaled as free"
                    );
                    assert_eq!(budget.cost_microusd, 0);
                    let events = std::fs::read_to_string(dir.join("journal.jsonl")).unwrap();
                    assert!(
                        !events
                            .lines()
                            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                            .any(|event| event["t"] == "llm_call")
                    );
                } else {
                    result.expect("unused cache categories and optional pricing remain supported");
                    assert_eq!(budget.llm_calls, 1);
                    // Cache fallback inherits base input (2), never tier input (5).
                    let expected = (100 - read - creation) * 5
                        + 30
                        + (read + creation) * if base_input.is_some() { 2 } else { 0 };
                    assert_eq!(budget.cost_microusd, expected);
                }
                assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    }
}
