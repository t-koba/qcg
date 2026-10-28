use crate::StepError;
use contract::{ModelRef, NodeDef};
use llm::{CacheRetention, PromptCacheRetention, TokenUsage};

use super::types::LlmGateway;

impl<'a> LlmGateway<'a> {
    /// Price the requested TTL; streaming usage may omit its breakdown.
    pub(super) fn cache_write_multiplier(
        &self,
        provider: &str,
        model: &str,
        retention: Option<CacheRetention>,
    ) -> Option<f64> {
        if retention != Some(CacheRetention::Extended) {
            return None;
        }
        match self.provider.prompt_cache_retention_for(provider, model)? {
            PromptCacheRetention::Ttl1h => Some(2.0),
            PromptCacheRetention::Ttl12h => Some(2.5),
            PromptCacheRetention::Retention24h => None,
        }
    }

    /// The route that bills this call: the contract's routes first, then
    /// the gateway's priced models. The selection rule lives in the policy
    /// crate, so the meter and the pre-call reservation pick the same row.
    pub(super) fn selected_route<'b>(
        &'b self,
        routes: &'b [ModelRef],
        provider: &str,
        model: &str,
    ) -> Option<&'b ModelRef> {
        let entries: Vec<&ModelRef> = routes.iter().chain(&self.pricing).collect();
        let rows: Vec<policy::PricingRow<'_>> = entries
            .iter()
            .map(|entry| policy::PricingRow {
                provider: entry.provider.as_str(),
                model: entry.model.as_str(),
                input_cost_per_million_usd: entry.input_cost_per_million_usd,
                cache_creation_input_cost_per_million_usd: entry
                    .cache_creation_input_cost_per_million_usd,
                cache_read_input_cost_per_million_usd: entry.cache_read_input_cost_per_million_usd,
                output_cost_per_million_usd: entry.output_cost_per_million_usd,
                web_search_call_cost_usd: entry.web_search_call_cost_usd,
            })
            .collect();
        let index = policy::select_pricing(&rows, provider, model)?;
        entries.get(index).copied()
    }

    /// The hosted search charge of one response, in microusd. `None` means
    /// the run does not price the search: the charge is unmetered, never
    /// free.
    pub(crate) fn web_search_cost_microusd(
        &self,
        routes: &[ModelRef],
        provider: &str,
        model: &str,
        calls: Option<u64>,
    ) -> Option<u64> {
        let calls = calls.filter(|calls| *calls > 0)?;
        let price = self
            .selected_route(routes, provider, model)
            .and_then(|route| route.web_search_call_cost_usd)?;
        Some(
            (calls as f64 * price * 1_000_000.0)
                .round()
                .clamp(0.0, u64::MAX as f64) as u64,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn cost_microusd(
        &self,
        node: &NodeDef,
        routes: &[ModelRef],
        provider: &str,
        model: &str,
        usage: &TokenUsage,
        retention: Option<CacheRetention>,
        web_search_calls: Option<u64>,
    ) -> Result<u64, StepError> {
        let ordinary_input = usage
            .ordinary_input()
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let Some(route) = self.selected_route(routes, provider, model) else {
            if self.budget.require_pricing {
                return Err(StepError::failed(
                    &node.id,
                    format!("model `{provider}/{model}` has no pricing for the run cost budget"),
                ));
            }
            return Ok(0);
        };
        // The tier is decided by whole prompt input (ordinary plus the
        // disjoint cache subsets); the selected tier prices the request.
        let prompt_input = ordinary_input
            .saturating_add(usage.cached_input)
            .saturating_add(usage.cache_creation_input);
        let tier = policy::select_cost_tier(&route.cost_tiers, prompt_input);
        let mut rates = route.rates().resolve(tier);
        // Extended TTL uses the tier's input rate, never the cache-write rate.
        if let Some(factor) = self.cache_write_multiplier(provider, model, retention) {
            rates.cache_creation = rates.input.map(|rate| rate * factor);
        }
        if self.budget.require_pricing
            && (rates.input.is_none()
                || rates.output.is_none()
                || (usage.cached_input > 0 && rates.cache_read.is_none())
                || (usage.cache_creation_input > 0 && rates.cache_creation.is_none()))
        {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "model `{provider}/{model}` has incomplete pricing for the run cost budget"
                ),
            ));
        }
        if rates.invalid_rate().is_some() {
            return Err(StepError::failed(&node.id, "invalid model pricing"));
        }
        let input = rates.input.unwrap_or_default();
        let output = rates.output.unwrap_or_default();
        let read = rates.cache_read.unwrap_or_default();
        let creation = rates.cache_creation.unwrap_or_default();
        let mut microusd = ordinary_input as f64 * input
            + usage.cached_input as f64 * read
            + usage.cache_creation_input as f64 * creation
            + usage.output as f64 * output;
        // The provider bills its hosted search per call. The count comes
        // from the response, and a run that requires pricing refuses a
        // reported call it cannot price.
        if let Some(calls) = web_search_calls.filter(|calls| *calls > 0) {
            match route.web_search_call_cost_usd {
                Some(price) => {
                    if !price.is_finite() || price < 0.0 {
                        return Err(StepError::failed(&node.id, "invalid model pricing"));
                    }
                    microusd += calls as f64 * price * 1_000_000.0;
                }
                None if self.budget.require_pricing => {
                    return Err(StepError::failed(
                        &node.id,
                        format!(
                            "model `{provider}/{model}` reports {calls} hosted web search call(s) without a `web_search_call_cost_usd` price for the run cost budget"
                        ),
                    ));
                }
                None => {}
            }
        }
        Ok(microusd.round().clamp(0.0, u64::MAX as f64) as u64)
    }
}
