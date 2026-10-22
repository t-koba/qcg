use super::*;

/// A provider that reports the usage a tier test needs.
struct TierProvider {
    usage: TokenUsage,
}

#[async_trait]
impl LlmProvider for TierProvider {
    fn id(&self) -> &str {
        "tier"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, _: ChatRequest) -> Result<ChatResponse, LlmError> {
        Ok(ChatResponse {
            content: vec![ChatContent::Text("ok".into())],
            usage: self.usage.clone(),
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search: None,
        })
    }
}

/// A route whose base row prices at 2/3 and whose tier prices at 5/6
/// from one million prompt input tokens on.
fn tier_pricing() -> ModelRef {
    ModelRef {
        provider: "tier".into(),
        model: "test".into(),
        input_cost_per_million_usd: Some(2.0),
        cache_creation_input_cost_per_million_usd: None,
        cache_read_input_cost_per_million_usd: None,
        output_cost_per_million_usd: Some(3.0),
        web_search_call_cost_usd: None,
        cost_tiers: vec![policy::CostTier {
            threshold_tokens: 1_000_000,
            input_cost_per_million_usd: Some(5.0),
            cache_read_input_cost_per_million_usd: Some(0.5),
            cache_creation_input_cost_per_million_usd: Some(4.0),
            output_cost_per_million_usd: Some(6.0),
        }],
    }
}

/// A route whose base row names its cache rates and whose tier names
/// only input and output rates.
fn tier_pricing_without_cache_rates() -> ModelRef {
    ModelRef {
        provider: "tier".into(),
        model: "test".into(),
        input_cost_per_million_usd: Some(2.0),
        cache_creation_input_cost_per_million_usd: Some(2.5),
        cache_read_input_cost_per_million_usd: Some(0.25),
        output_cost_per_million_usd: Some(3.0),
        web_search_call_cost_usd: None,
        cost_tiers: vec![policy::CostTier {
            threshold_tokens: 1_000_000,
            input_cost_per_million_usd: Some(5.0),
            cache_read_input_cost_per_million_usd: None,
            cache_creation_input_cost_per_million_usd: None,
            output_cost_per_million_usd: Some(6.0),
        }],
    }
}

fn usage(input: u64, cached_input: u64, cache_creation_input: u64) -> TokenUsage {
    TokenUsage {
        input,
        output: 10,
        reasoning: 0,
        cached_input,
        cache_creation_input,
    }
}

/// The cost a gateway records for one call at `usage`.
async fn metered_cost(usage: TokenUsage, pricing: ModelRef) -> u64 {
    let dir = test_dir("tier-cost");
    let journal = test_journal(&dir, &format!("tier-{}", uuid::Uuid::now_v7()));
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(TierProvider { usage }),
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
    let mut request = test_request();
    request.provider = "tier".into();
    gateway
        .complete(&test_node(), request, &[pricing], |_| json!({}))
        .await
        .expect("the call must succeed");
    let cost = journal.state().budget.cost_microusd;
    let _ = std::fs::remove_dir_all(dir);
    cost
}

#[tokio::test]
async fn a_request_below_the_threshold_is_priced_at_the_base_row() {
    // 999,999 prompt input tokens stay below the tier's threshold.
    let cost = metered_cost(usage(999_999, 0, 0), tier_pricing()).await;
    assert_eq!(cost, 999_999 * 2 + 10 * 3);
}

#[tokio::test]
async fn a_request_that_reaches_the_threshold_is_priced_at_the_tier_row() {
    let cost = metered_cost(usage(1_000_000, 0, 0), tier_pricing()).await;
    assert_eq!(cost, 1_000_000 * 5 + 10 * 6);
}

#[tokio::test]
async fn the_tier_is_chosen_by_the_whole_prompt_input_counted_once() {
    // The cache read and creation subsets are part of the prompt input:
    // 400,000 + 400,000 + 200,000 reaches the threshold exactly, and no
    // token is priced twice.
    let cost = metered_cost(usage(1_000_000, 400_000, 200_000), tier_pricing()).await;
    assert_eq!(cost, 400_000 * 5 + 400_000 * 5 / 10 + 200_000 * 4 + 10 * 6);
}

#[tokio::test]
async fn a_tier_keeps_the_base_row_cache_rates_at_the_threshold() {
    // 800,000 ordinary + 100,000 cached + 100,000 created reaches the
    // one-million threshold exactly, so the tier prices the request.
    let cost = metered_cost(
        usage(1_000_000, 100_000, 100_000),
        tier_pricing_without_cache_rates(),
    )
    .await;
    // The tier's input and output rates price the request; the cache
    // tokens keep the base row's cache rates, because the tier's own
    // ordinary input rate is not a cache price.
    assert_eq!(cost, 800_000 * 5 + 100_000 / 4 + 100_000 * 5 / 2 + 10 * 6);
}

#[tokio::test]
async fn a_request_below_the_threshold_keeps_the_base_row_cache_rates() {
    // One token short of the threshold: the base row prices the request.
    let cost = metered_cost(
        usage(999_999, 100_000, 100_000),
        tier_pricing_without_cache_rates(),
    )
    .await;
    assert_eq!(cost, 799_999 * 2 + 100_000 / 4 + 100_000 * 5 / 2 + 10 * 3);
}

#[tokio::test]
async fn the_reservation_uses_the_dearest_rate_any_tier_names() {
    let dir = test_dir("tier-reservation");
    let journal = test_journal(&dir, &format!("tier-{}", uuid::Uuid::now_v7()));
    let secrets = SecretStore::default();
    let gateway = LlmGateway::new(
        Arc::new(TierProvider {
            usage: usage(1_000_000, 0, 0),
        }),
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
    request.provider = "tier".into();
    let input_est = (serde_json::to_vec(&request).unwrap().len() as u64).div_ceil(4);
    // The estimate cannot prove which tier the request reaches, so it
    // reserves at the dearest rate the tiers name: 5 per input token and
    // 6 per output token.
    let expected =
        (input_est as f64 * 5.0 + u64::from(request.max_tokens) as f64 * 6.0).ceil() as u64;
    let result = gateway
        .complete(&test_node(), request, &[tier_pricing()], |_| json!({}))
        .await;
    assert!(matches!(
        result,
        Err(StepError::BudgetExceeded {
            resource: "cost_microusd",
            used,
            limit: 0,
        }) if used == expected
    ));
    assert_eq!(journal.state().budget.llm_calls, 0);
    let _ = std::fs::remove_dir_all(dir);
}
