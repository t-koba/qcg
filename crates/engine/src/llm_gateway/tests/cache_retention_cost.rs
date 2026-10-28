use super::*;
use llm::{CacheRetention, PromptCacheRetention};
use std::sync::atomic::{AtomicUsize, Ordering};

struct RetentionProvider {
    mechanism: PromptCacheRetention,
    calls: AtomicUsize,
}

#[async_trait]
impl LlmProvider for RetentionProvider {
    fn id(&self) -> &str {
        "tier"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    fn prompt_cache_retention_for(&self, provider: &str, _: &str) -> Option<PromptCacheRetention> {
        (provider == self.id()).then_some(self.mechanism)
    }
    async fn complete(&self, _: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ChatResponse {
            content: vec![ChatContent::Text("ok".into())],
            // Inclusive input: 400k ordinary, 400k read, 200k written.
            // Deliberately no TTL details, as with streaming usage totals.
            usage: TokenUsage {
                input: 1_000_000,
                cached_input: 400_000,
                cache_creation_input: 200_000,
                output: 10,
                reasoning: 0,
            },
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search: None,
        })
    }
}

fn pricing() -> ModelRef {
    ModelRef {
        provider: "tier".into(),
        model: "test".into(),
        input_cost_per_million_usd: Some(2.0),
        // Deliberately not 1.25x input: `retention_24h` and default requests
        // must keep this configured write rate, while TTLs replace it with
        // the tier input rate times the multiplier.
        cache_creation_input_cost_per_million_usd: Some(4.0),
        cache_read_input_cost_per_million_usd: Some(0.25),
        output_cost_per_million_usd: Some(3.0),
        web_search_call_cost_usd: None,
        cost_tiers: vec![policy::CostTier {
            threshold_tokens: 1_000_000,
            input_cost_per_million_usd: Some(5.0),
            cache_creation_input_cost_per_million_usd: Some(9.0),
            cache_read_input_cost_per_million_usd: Some(0.5),
            output_cost_per_million_usd: Some(6.0),
        }],
    }
}

fn request(extended: bool) -> ChatRequest {
    let mut request = test_request();
    request.provider = "tier".into();
    request.prompt_cache = llm::PromptCache::Auto;
    request.cache_retention = extended.then_some(CacheRetention::Extended);
    request
}

#[tokio::test]
async fn requested_ttl_prices_only_writes_at_the_selected_context_tier() {
    for stream in [false, true] {
        for tier in [false, true] {
            for (mechanism, extended, multiplier) in [
                (PromptCacheRetention::Ttl1h, true, 2.0),
                (PromptCacheRetention::Ttl12h, true, 2.5),
                (PromptCacheRetention::Retention24h, true, 0.0),
                (PromptCacheRetention::Ttl1h, false, 0.0),
                (PromptCacheRetention::Ttl12h, false, 0.0),
            ] {
                let dir = test_dir("retention-cost");
                let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
                let secrets = SecretStore::default();
                let provider = Arc::new(RetentionProvider {
                    mechanism,
                    calls: AtomicUsize::new(0),
                });
                let gateway = test_gateway(provider.clone(), &secrets, &journal);
                let mut route = pricing();
                if !tier {
                    route.cost_tiers.clear();
                }
                let mut request = request(extended);
                request.stream = stream;
                gateway
                    .complete(&test_node(), request, &[route], |_| json!({}))
                    .await
                    .unwrap();
                let (input, read, output, configured_creation) = if tier {
                    (5.0, 0.5, 6.0, 9.0)
                } else {
                    (2.0, 0.25, 3.0, 4.0)
                };
                // TTL writes replace only the creation rate with the selected
                // tier input rate times the multiplier; `retention_24h` and
                // default requests keep the configured write rate.
                let creation = if extended
                    && matches!(
                        mechanism,
                        PromptCacheRetention::Ttl1h | PromptCacheRetention::Ttl12h
                    ) {
                    input * multiplier
                } else {
                    configured_creation
                };
                let expected =
                    (400_000.0 * input + 400_000.0 * read + 200_000.0 * creation + 10.0 * output)
                        as u64;
                let budget = journal.state().budget;
                assert_eq!(
                    budget.cost_microusd, expected,
                    "{mechanism:?}, extended={extended}, tier={tier}, stream={stream}"
                );
                assert_eq!(budget.tokens_input, 1_000_000);
                assert_eq!(budget.tokens_cache_creation_input, 200_000);
                assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    }
}

#[tokio::test]
async fn reservations_include_the_requested_ttl_at_the_dearest_context_rate() {
    for (mechanism, extended, creation) in [
        (PromptCacheRetention::Ttl1h, true, 10.0),
        (PromptCacheRetention::Ttl12h, true, 12.5),
        (PromptCacheRetention::Retention24h, true, 9.0),
        (PromptCacheRetention::Ttl12h, false, 9.0),
    ] {
        let dir = test_dir("retention-reservation");
        let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
        let secrets = SecretStore::default();
        let provider = Arc::new(RetentionProvider {
            mechanism,
            calls: AtomicUsize::new(0),
        });
        let mut gateway = test_gateway(provider.clone(), &secrets, &journal);
        gateway.budget.max_cost_microusd = Some(0);
        gateway.budget.require_pricing = true;
        let request = request(extended);
        let input_est = (serde_json::to_vec(&request).unwrap().len() as u64).div_ceil(4);
        let expected =
            (input_est as f64 * creation + f64::from(request.max_tokens) * 6.0).ceil() as u64;
        let result = gateway
            .complete(&test_node(), request, &[pricing()], |_| json!({}))
            .await;
        assert!(
            matches!(result, Err(StepError::BudgetExceeded { resource: "cost_microusd", used, limit: 0 }) if used == expected),
            "{result:?}"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(journal.state().budget.llm_calls, 0);
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test]
async fn an_unpriced_ttl_input_rate_refuses_dispatch_even_with_priced_cache_rates() {
    for mechanism in [PromptCacheRetention::Ttl1h, PromptCacheRetention::Ttl12h] {
        let dir = test_dir("retention-unpriced");
        let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
        let secrets = SecretStore::default();
        let provider = Arc::new(RetentionProvider {
            mechanism,
            calls: AtomicUsize::new(0),
        });
        let mut gateway = test_gateway(provider.clone(), &secrets, &journal);
        gateway.budget.max_cost_microusd = Some(u64::MAX);
        gateway.budget.require_pricing = true;
        let mut route = pricing();
        // The tier has input pricing but the base does not: taking only
        // max_cost_rates.input would mask this missing TTL-specific price.
        route.input_cost_per_million_usd = None;
        let result = gateway
            .complete(&test_node(), request(true), &[route], |_| json!({}))
            .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("ordinary input pricing for extended cache TTL")
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(journal.state().budget.llm_calls, 0);
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test]
async fn required_pricing_accepts_ttl_derived_creation_rate() {
    // A missing explicit cache-creation rate is valid when an Extended TTL
    // derives the write price from ordinary input; require_pricing must not
    // reject that derived rate in post-response accounting or preflight.
    for (mechanism, factor) in [
        (PromptCacheRetention::Ttl1h, 2.0),
        (PromptCacheRetention::Ttl12h, 2.5),
    ] {
        for tier in [false, true] {
            let mut route = pricing();
            route.cache_creation_input_cost_per_million_usd = None;
            if tier {
                route.cost_tiers[0].cache_creation_input_cost_per_million_usd = None;
            } else {
                route.cost_tiers.clear();
            }
            let (input, read, output) = if tier {
                (5.0, 0.5, 6.0)
            } else {
                (2.0, 0.25, 3.0)
            };
            let creation = input * factor;
            let expected =
                (400_000.0 * input + 400_000.0 * read + 200_000.0 * creation + 10.0 * output)
                    as u64;
            // Post-response accounting without a cost cap.
            {
                let dir = test_dir("retention-required-ttl");
                let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
                let secrets = SecretStore::default();
                let provider = Arc::new(RetentionProvider {
                    mechanism,
                    calls: AtomicUsize::new(0),
                });
                let mut gateway = test_gateway(provider.clone(), &secrets, &journal);
                gateway.budget.require_pricing = true;
                let usage = TokenUsage {
                    input: 1_000_000,
                    cached_input: 400_000,
                    cache_creation_input: 200_000,
                    output: 10,
                    reasoning: 0,
                };
                let cost = gateway
                    .cost_microusd(
                        &test_node(),
                        &[route.clone()],
                        "tier",
                        "test",
                        &usage,
                        Some(CacheRetention::Extended),
                        None,
                    )
                    .expect("TTL-derived creation must satisfy required pricing");
                assert_eq!(cost, expected, "{mechanism:?}, tier={tier}");
                gateway
                    .complete(&test_node(), request(true), &[route.clone()], |_| json!({}))
                    .await
                    .expect("TTL-derived creation must pass without a cap");
                assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
                assert_eq!(journal.state().budget.cost_microusd, expected);
                let _ = std::fs::remove_dir_all(dir);
            }
            // Preflight with an enforced cost cap must see the same derived rate.
            {
                let dir = test_dir("retention-required-ttl-cap");
                let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
                let secrets = SecretStore::default();
                let provider = Arc::new(RetentionProvider {
                    mechanism,
                    calls: AtomicUsize::new(0),
                });
                let mut gateway = test_gateway(provider.clone(), &secrets, &journal);
                gateway.budget.max_cost_microusd = Some(u64::MAX);
                gateway.budget.require_pricing = true;
                gateway
                    .complete(&test_node(), request(true), &[route], |_| json!({}))
                    .await
                    .expect("TTL preflight must not refuse the derived rate");
                assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
                assert_eq!(journal.state().budget.cost_microusd, expected);
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    }
    // A tier-selected TTL derives the write price even when neither the tier
    // nor the base row names a creation rate.
    {
        let dir = test_dir("retention-required-ttl-tier");
        let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
        let secrets = SecretStore::default();
        let provider = Arc::new(RetentionProvider {
            mechanism: PromptCacheRetention::Ttl1h,
            calls: AtomicUsize::new(0),
        });
        let mut gateway = test_gateway(provider.clone(), &secrets, &journal);
        gateway.budget.require_pricing = true;
        let mut route = pricing();
        route.input_cost_per_million_usd = None;
        route.cache_read_input_cost_per_million_usd = None;
        route.cache_creation_input_cost_per_million_usd = None;
        route.output_cost_per_million_usd = None;
        route.cost_tiers[0].cache_creation_input_cost_per_million_usd = None;
        let usage = TokenUsage {
            input: 1_000_000,
            cached_input: 400_000,
            cache_creation_input: 200_000,
            output: 10,
            reasoning: 0,
        };
        assert!(
            gateway
                .cost_microusd(
                    &test_node(),
                    &[route.clone()],
                    "tier",
                    "test",
                    &usage,
                    None,
                    None
                )
                .is_err(),
            "tier creation without a TTL must stay unpriced"
        );
        let cost = gateway
            .cost_microusd(
                &test_node(),
                &[route],
                "tier",
                "test",
                &usage,
                Some(CacheRetention::Extended),
                None,
            )
            .expect("tier TTL must derive creation from tier input");
        assert_eq!(cost, 4_200_060);
        let _ = std::fs::remove_dir_all(dir);
    }
    // Preflight without a TTL still refuses a truly unpriced creation row.
    {
        let dir = test_dir("retention-required-no-ttl");
        let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
        let secrets = SecretStore::default();
        let provider = Arc::new(RetentionProvider {
            mechanism: PromptCacheRetention::Ttl1h,
            calls: AtomicUsize::new(0),
        });
        let mut gateway = test_gateway(provider.clone(), &secrets, &journal);
        gateway.budget.require_pricing = true;
        gateway.budget.max_cost_microusd = Some(u64::MAX);
        let mut route = pricing();
        route.input_cost_per_million_usd = None;
        route.cache_creation_input_cost_per_million_usd = None;
        route.cost_tiers.clear();
        let result = gateway
            .complete(&test_node(), request(false), &[route], |_| json!({}))
            .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("lacks cache input pricing")
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[test]
fn billing_metadata_follows_the_router_and_model_override() {
    let router = llm::LlmRouter::parse_text(
        r#"
[[provider]]
id = "ttl"
api = "responses"
base_url = "http://127.0.0.1:9/v1"
prompt_cache_field = "prompt_cache_key"
prompt_cache_retention = "retention_24h"
[provider.capabilities]
prompt_cache = true
[[provider.models]]
id = "new"
prompt_cache_retention = "ttl_12h"
"#,
    )
    .unwrap();
    assert_eq!(
        router.prompt_cache_retention_for("ttl", "new"),
        Some(PromptCacheRetention::Ttl12h)
    );
    assert_eq!(
        router.prompt_cache_retention_for("ttl", "old"),
        Some(PromptCacheRetention::Retention24h)
    );
    assert_eq!(router.prompt_cache_retention_for("unknown", "new"), None);
}

/// The journaled `llm_call` event of the run.
fn llm_call_event(dir: &camino::Utf8PathBuf) -> serde_json::Value {
    super::persisted_text(dir)
        .lines()
        .map(|line| serde_json::from_str(line).expect("journal line should be valid JSON"))
        .find(|event: &serde_json::Value| event["t"] == "llm_call")
        .expect("the run should journal an llm_call event")
}

#[tokio::test]
async fn requested_cache_policy_rides_with_the_call_and_defaults_stay_absent() {
    use llm::CacheWritePolicy;
    // Extended TTL requests record the requested tier so the 2x/2.5x write
    // basis stays inspectable even when usage omits the TTL breakdown.
    let dir = test_dir("retention-event");
    let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
    let secrets = SecretStore::default();
    let provider = std::sync::Arc::new(RetentionProvider {
        mechanism: PromptCacheRetention::Ttl1h,
        calls: AtomicUsize::new(0),
    });
    let gateway = test_gateway(provider, &secrets, &journal);
    let mut extended = request(true);
    extended.cache_write = CacheWritePolicy::Allow;
    gateway
        .complete(&test_node(), extended, &[pricing()], |_| json!({}))
        .await
        .unwrap();
    let event = llm_call_event(&dir);
    assert_eq!(event["cache_retention"], json!("extended"));
    assert!(
        event.get("cache_write_policy").is_none(),
        "default allow stays absent: {event}"
    );
    let _ = std::fs::remove_dir_all(dir);

    // A write-disable records only the disable; the retention tier is absent.
    let dir = test_dir("retention-event-forbid");
    let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
    let gateway = test_gateway(
        std::sync::Arc::new(RetentionProvider {
            mechanism: PromptCacheRetention::Ttl1h,
            calls: AtomicUsize::new(0),
        }),
        &secrets,
        &journal,
    );
    let mut forbid = request(false);
    forbid.cache_write = CacheWritePolicy::Forbid;
    gateway
        .complete(&test_node(), forbid, &[pricing()], |_| json!({}))
        .await
        .unwrap();
    let event = llm_call_event(&dir);
    assert_eq!(event["cache_write_policy"], json!("forbid"));
    assert!(
        event.get("cache_retention").is_none(),
        "absent retention stays absent: {event}"
    );
    let _ = std::fs::remove_dir_all(dir);

    // Defaults omit both fields so older journals keep decoding.
    let dir = test_dir("retention-event-default");
    let journal = test_journal(&dir, &uuid::Uuid::now_v7().to_string());
    let gateway = test_gateway(
        std::sync::Arc::new(RetentionProvider {
            mechanism: PromptCacheRetention::Ttl1h,
            calls: AtomicUsize::new(0),
        }),
        &secrets,
        &journal,
    );
    gateway
        .complete(&test_node(), request(false), &[pricing()], |_| json!({}))
        .await
        .unwrap();
    let event = llm_call_event(&dir);
    assert!(
        event.get("cache_write_policy").is_none(),
        "default allow stays absent: {event}"
    );
    assert!(
        event.get("cache_retention").is_none(),
        "absent retention stays absent: {event}"
    );
    let _ = std::fs::remove_dir_all(dir);
}
