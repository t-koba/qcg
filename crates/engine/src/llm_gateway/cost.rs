use crate::StepError;
use contract::{ModelRef, NodeDef};
use llm::TokenUsage;

use super::types::LlmGateway;

impl<'a> LlmGateway<'a> {
    pub(super) fn selected_pricing<'b>(
        &'b self,
        routes: &'b [ModelRef],
        provider: &str,
        model: &str,
    ) -> Option<policy::PricingRow<'b>> {
        let rows: Vec<policy::PricingRow<'_>> = routes
            .iter()
            .chain(&self.pricing)
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
        policy::select_pricing(&rows, provider, model)
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
            .selected_pricing(routes, provider, model)
            .and_then(|pricing| pricing.web_search_call_cost_usd)?;
        Some(
            (calls as f64 * price * 1_000_000.0)
                .round()
                .clamp(0.0, u64::MAX as f64) as u64,
        )
    }

    pub(crate) fn cost_microusd(
        &self,
        node: &NodeDef,
        routes: &[ModelRef],
        provider: &str,
        model: &str,
        usage: &TokenUsage,
        web_search_calls: Option<u64>,
    ) -> Result<u64, StepError> {
        let ordinary_input = usage
            .ordinary_input()
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let Some(pricing) = self.selected_pricing(routes, provider, model) else {
            if self.budget.require_pricing {
                return Err(StepError::failed(
                    &node.id,
                    format!("model `{provider}/{model}` has no pricing for the run cost budget"),
                ));
            }
            return Ok(0);
        };
        let input = pricing.input_cost_per_million_usd;
        let output = pricing.output_cost_per_million_usd;
        if self.budget.require_pricing && (input.is_none() || output.is_none()) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "model `{provider}/{model}` has incomplete pricing for the run cost budget"
                ),
            ));
        }
        let input = input.unwrap_or_default();
        let read = pricing
            .cache_read_input_cost_per_million_usd
            .unwrap_or(input);
        let creation = pricing
            .cache_creation_input_cost_per_million_usd
            .unwrap_or(input);
        if [input, output.unwrap_or_default(), read, creation]
            .into_iter()
            .any(|rate| !rate.is_finite() || rate < 0.0)
        {
            return Err(StepError::failed(&node.id, "invalid model pricing"));
        }
        let mut microusd = ordinary_input as f64 * input
            + usage.cached_input as f64 * read
            + usage.cache_creation_input as f64 * creation
            + usage.output as f64 * output.unwrap_or_default();
        // The provider bills its hosted search per call. The count comes
        // from the response, and a run that requires pricing refuses a
        // reported call it cannot price.
        if let Some(calls) = web_search_calls.filter(|calls| *calls > 0) {
            match pricing.web_search_call_cost_usd {
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
