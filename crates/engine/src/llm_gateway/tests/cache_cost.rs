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

#[test]
fn unrepresentable_model_and_search_costs_fail_closed() {
    let dir = test_dir("unrepresentable-cost");
    let journal = test_journal(&dir, &format!("unrepresentable-{}", uuid::Uuid::now_v7()));
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
        vec![],
    );
    let mut model_price = cache_pricing(Some(0.0), Some(0.0));
    model_price.input_cost_per_million_usd = Some(f64::MAX);
    model_price.output_cost_per_million_usd = Some(0.0);
    let usage = TokenUsage {
        input: 1,
        ..Default::default()
    };
    assert!(
        gateway
            .cost_microusd(
                &test_node(),
                &[model_price],
                "cache-metered",
                "test",
                &usage,
                None,
                None,
            )
            .is_err()
    );

    let mut search_price = cache_pricing(Some(0.0), Some(0.0));
    search_price.input_cost_per_million_usd = Some(0.0);
    search_price.output_cost_per_million_usd = Some(0.0);
    search_price.web_search_call_cost_usd = Some(f64::MAX);
    assert_eq!(
        gateway
            .web_search_cost_microusd(&[search_price.clone()], "cache-metered", "test", Some(1),),
        None,
        "an unrepresentable search charge is unknown, not saturated"
    );
    assert!(
        gateway
            .cost_microusd(
                &test_node(),
                &[search_price],
                "cache-metered",
                "test",
                &TokenUsage::default(),
                None,
                Some(1),
            )
            .is_err()
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn unrepresentable_cost_estimate_refuses_dispatch() {
    let dir = test_dir("unrepresentable-estimate");
    let journal = test_journal(
        &dir,
        &format!("unrepresentable-estimate-{}", uuid::Uuid::now_v7()),
    );
    let secrets = SecretStore::default();
    let provider = Arc::new(CountingCacheProvider {
        calls: Default::default(),
        usage: TokenUsage::default(),
    });
    let gateway = LlmGateway::new(
        provider.clone(),
        &secrets,
        &journal,
        CancellationToken::new(),
        LlmCostBudget {
            max_tokens: None,
            max_cost_microusd: Some(u64::MAX),
            require_pricing: true,
        },
        vec![],
    );
    let mut request = test_request();
    request.provider = "cache-metered".into();
    request.model = "test".into();
    let mut pricing = cache_pricing(Some(0.0), Some(0.0));
    pricing.input_cost_per_million_usd = Some(f64::MAX);
    pricing.output_cost_per_million_usd = Some(f64::MAX);
    let error = gateway
        .complete(&test_node(), request, &[pricing], |_| json!({}))
        .await
        .expect_err("a non-representable reservation must fail before spending");
    assert!(error.to_string().contains("not representable in microusd"));
    assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(journal.state().budget.llm_calls, 0);
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
        if request.model == "unavailable" {
            return Err(LlmError {
                kind: LlmErrorKind::TimedOut,
                message: "primary route unavailable".into(),
                ..Default::default()
            });
        }
        let mut response = CacheMeteredProvider.complete(request).await?;
        response.usage = self.usage.clone();
        Ok(response)
    }

    async fn decide(
        &self,
        request: llm::DecisionRequest,
    ) -> Result<llm::DecisionResponse, LlmError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(llm::DecisionResponse {
            model: request.model,
            answers: BTreeMap::from([("accept".into(), llm::DecisionAnswer::Noul { noul: 0.5 })]),
            usage: llm::DecisionUsage {
                input_tokens: self.usage.input,
                output_tokens: self.usage.output,
            },
        })
    }
}

fn unrepresentable_usage() -> TokenUsage {
    TokenUsage {
        input: 3_000_000_000,
        ..TokenUsage::default()
    }
}

fn liability_pricing() -> ModelRef {
    let mut pricing = cache_pricing(Some(0.0), Some(0.0));
    pricing.input_cost_per_million_usd = Some(10_000_000_000.0);
    pricing.output_cost_per_million_usd = Some(0.0);
    pricing
}

fn no_budget() -> LlmCostBudget {
    LlmCostBudget {
        max_tokens: None,
        max_cost_microusd: None,
        require_pricing: false,
    }
}

fn wide_budget() -> LlmCostBudget {
    LlmCostBudget {
        max_tokens: None,
        max_cost_microusd: Some(u64::MAX),
        require_pricing: false,
    }
}

#[tokio::test]
async fn unrepresentable_chat_charge_is_durable_and_blocks_after_reopen() {
    let dir = test_dir("unrepresentable-chat-actual");
    let run_id = format!("unrepresentable-chat-{}", uuid::Uuid::now_v7());
    let journal_path = dir.join("journal.jsonl");
    let journal = JournalWriter::create(&journal_path, &run_id, false, None).unwrap();
    let secrets = SecretStore::default();
    let provider = Arc::new(CountingCacheProvider {
        calls: Default::default(),
        usage: unrepresentable_usage(),
    });
    let gateway = LlmGateway::new(
        provider.clone(),
        &secrets,
        &journal,
        CancellationToken::new(),
        wide_budget(),
        vec![],
    );
    let mut request = test_request();
    request.provider = "cache-metered".into();
    let error = gateway
        .complete(
            &test_node(),
            request.clone(),
            &[liability_pricing()],
            |_| json!({}),
        )
        .await
        .expect_err("billed but unrepresentable actual cost must fail");
    assert!(error.to_string().contains("not representable in microusd"));
    assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(journal.state().budget.unresolved_llm_cost);
    assert_eq!(journal.state().budget.llm_calls, 1);
    assert_eq!(journal.state().budget.tokens_input, 3_000_000_000);
    assert_eq!(journal.state().budget.cost_microusd, 0);
    let recorded: Vec<serde_json::Value> = persisted_text(&dir)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let marker = recorded
        .iter()
        .find(|event| event["t"] == "llm_cost_unresolved")
        .expect("unknown-cost liability must be journaled");
    assert_eq!(marker["provider"], "cache-metered");
    assert_eq!(marker["model"], "test");
    assert_eq!(marker["tokens"]["input"], 3_000_000_000_u64);
    assert_eq!(marker["liability"], "actual_cost_unrepresentable");

    drop(gateway);
    drop(journal);
    super::super::complete::clear_pending_for_test(&run_id);
    let replay = JournalWriter::create(&journal_path, &run_id, false, None).unwrap();
    assert!(replay.state().budget.unresolved_llm_cost);
    assert_eq!(replay.state().budget.tokens_input, 3_000_000_000);
    let retry_provider = Arc::new(CountingCacheProvider {
        calls: Default::default(),
        usage: TokenUsage::default(),
    });
    let retry = LlmGateway::new(
        retry_provider.clone(),
        &secrets,
        &replay,
        CancellationToken::new(),
        wide_budget(),
        vec![],
    );
    let refused = retry
        .complete(&test_node(), request, &[liability_pricing()], |_| json!({}))
        .await
        .expect_err("replayed unknown cost must refuse admission");
    assert!(
        refused
            .to_string()
            .contains("unresolved LLM cost liability")
    );
    assert_eq!(
        retry_provider
            .calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn unrepresentable_decision_charge_is_recorded_and_restores_unknown_budget() {
    let dir = test_dir("unrepresentable-decision-actual");
    let run_id = format!("unrepresentable-decision-{}", uuid::Uuid::now_v7());
    let journal_path = dir.join("journal.jsonl");
    let journal = JournalWriter::create(&journal_path, &run_id, false, None).unwrap();
    let secrets = SecretStore::default();
    let provider = Arc::new(CountingCacheProvider {
        calls: Default::default(),
        usage: unrepresentable_usage(),
    });
    let gateway = LlmGateway::new(
        provider.clone(),
        &secrets,
        &journal,
        CancellationToken::new(),
        wide_budget(),
        vec![],
    );
    let request = serde_json::from_value(json!({
        "provider": "cache-metered",
        "model": "test",
        "state": {},
        "questions": {"accept": {"type": "noul", "instructions": "Accept?"}}
    }))
    .unwrap();
    let error = gateway
        .decide(&test_node(), request, &liability_pricing(), 1)
        .await
        .expect_err("billed decision with unrepresentable charge must fail");
    assert!(error.to_string().contains("not representable in microusd"));
    assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(journal.state().budget.unresolved_llm_cost);
    assert_eq!(journal.state().budget.llm_calls, 1);
    assert_eq!(journal.state().budget.tokens_input, 3_000_000_000);
    drop(gateway);
    drop(journal);

    let replayed = crate::RunState::fold_journal(&journal_path).unwrap();
    assert!(replayed.budget.unresolved_llm_cost);
    assert_eq!(replayed.budget.tokens_input, 3_000_000_000);
}

#[tokio::test]
async fn failed_liability_append_keeps_concurrent_admission_poisoned() {
    let dir = test_dir("unrepresentable-marker-write-failure");
    let run_id = format!("unrepresentable-marker-failure-{}", uuid::Uuid::now_v7());
    let journal = JournalWriter::create_with_limits(
        &dir.join("journal.jsonl"),
        &run_id,
        false,
        None,
        crate::JournalLimits {
            max_event_bytes: Some(1),
            max_total_bytes: None,
            max_event_count: None,
            max_state_bytes: None,
            scan_window_bytes: None,
        },
    )
    .unwrap();
    let secrets = SecretStore::default();
    let provider = Arc::new(CountingCacheProvider {
        calls: Default::default(),
        usage: unrepresentable_usage(),
    });
    let gateway = LlmGateway::new(
        provider.clone(),
        &secrets,
        &journal,
        CancellationToken::new(),
        no_budget(),
        vec![],
    );
    let mut request = test_request();
    request.provider = "cache-metered".into();
    let error = gateway
        .complete(
            &test_node(),
            request.clone(),
            &[liability_pricing()],
            |_| json!({}),
        )
        .await
        .expect_err("failure to write the liability marker must surface");
    assert!(
        error
            .to_string()
            .contains("unable to durably record unknown-cost liability")
    );
    assert!(!journal.state().budget.unresolved_llm_cost);
    assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let node = test_node();
    let routes = [liability_pricing()];
    let (first, second, third) = tokio::join!(
        gateway.complete(&node, request.clone(), &routes, |_| json!({})),
        gateway.complete(&node, request.clone(), &routes, |_| json!({})),
        gateway.complete(&node, request, &routes, |_| json!({})),
    );
    for refused in [first, second, third] {
        assert!(
            refused
                .unwrap_err()
                .to_string()
                .contains("unresolved LLM cost liability")
        );
    }
    assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
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
        for (require_pricing, cap) in [
            (true, None),
            (true, Some(10_000_000)),
            (false, None),
            (false, Some(10_000_000)),
        ] {
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
                    max_cost_microusd: cap,
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
            let events = std::fs::read_to_string(dir.join("journal.jsonl")).unwrap();
            assert_eq!(
                events.lines().any(|line| {
                    serde_json::from_str::<serde_json::Value>(line).unwrap()["t"] == "llm_call"
                }),
                !require_pricing
            );
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

#[tokio::test]
async fn required_cache_pricing_preflights_even_unused_categories_without_a_cost_cap() {
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
                let actual_cost = gateway.cost_microusd(
                    &test_node(),
                    &[route.clone()],
                    "cache-metered",
                    "test",
                    &provider.usage,
                    None,
                    None,
                );
                assert_eq!(
                    actual_cost.is_err(),
                    require_pricing && base_input.is_none() && read + creation > 0
                );
                let result = gateway
                    .complete(&test_node(), request, &[route], |_| json!({}))
                    .await;
                let unpriced_route = require_pricing && base_input.is_none();
                let budget = journal.state().budget;
                if unpriced_route {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("lacks cache input pricing")
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
                    result.expect("base-input fallback and optional pricing remain supported");
                    assert_eq!(budget.llm_calls, 1);
                    // Cache fallback inherits base input (2), never tier input (5).
                    let expected = (100 - read - creation) * 5
                        + 30
                        + (read + creation) * if base_input.is_some() { 2 } else { 0 };
                    assert_eq!(budget.cost_microusd, expected);
                }
                assert_eq!(
                    provider.calls.load(std::sync::atomic::Ordering::SeqCst),
                    usize::from(!unpriced_route)
                );
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    }
}

#[tokio::test]
async fn required_cache_pricing_preflights_fallback_before_dispatch() {
    for require_pricing in [true, false] {
        for cap in [None, Some(10_000_000)] {
            let dir = test_dir("unpriced-fallback-cache");
            let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
            let secrets = SecretStore::default();
            let provider = Arc::new(CountingCacheProvider {
                calls: Default::default(),
                usage: TokenUsage {
                    input: 100,
                    output: 10,
                    cached_input: 40,
                    cache_creation_input: 20,
                    reasoning: 0,
                },
            });
            let gateway = LlmGateway::new(
                provider.clone(),
                &secrets,
                &journal,
                CancellationToken::new(),
                LlmCostBudget {
                    max_tokens: None,
                    max_cost_microusd: cap,
                    require_pricing,
                },
                vec![],
            );
            let mut primary = cache_pricing(None, None);
            primary.model = "unavailable".into();
            let mut request = test_request();
            request.provider = primary.provider.clone();
            request.model = primary.model.clone();
            let result = gateway
                .complete(
                    &test_node(),
                    request,
                    &[primary, tier_without_base_input_pricing()],
                    |_| json!({}),
                )
                .await;
            if require_pricing {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("lacks cache input pricing")
                );
            } else {
                result.expect("optional pricing still dispatches the fallback");
            }
            assert_eq!(
                provider.calls.load(std::sync::atomic::Ordering::SeqCst),
                if require_pricing { 1 } else { 2 }
            );
            assert_eq!(
                journal.state().budget.llm_calls,
                u64::from(!require_pricing)
            );
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}
