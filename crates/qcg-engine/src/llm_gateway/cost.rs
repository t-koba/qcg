use crate::StepError;
use qcg_contract::{ModelRef, NodeDef};
use qcg_llm::TokenUsage;

use super::types::LlmGateway;

impl<'a> LlmGateway<'a> {
    pub(crate) fn cost_microusd(
        &self,
        node: &NodeDef,
        routes: &[ModelRef],
        provider: &str,
        model: &str,
        usage: &TokenUsage,
    ) -> Result<u64, StepError> {
        let rows: Vec<qcg_policy::PricingRow<'_>> = routes
            .iter()
            .chain(&self.pricing)
            .map(|entry| qcg_policy::PricingRow {
                provider: entry.provider.as_str(),
                model: entry.model.as_str(),
                input_cost_per_million_usd: entry.input_cost_per_million_usd,
                output_cost_per_million_usd: entry.output_cost_per_million_usd,
            })
            .collect();
        let Some(pricing) = qcg_policy::select_pricing(&rows, provider, model) else {
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
        let microusd = usage.input as f64 * input.unwrap_or_default()
            + usage.output as f64 * output.unwrap_or_default();
        Ok(microusd.round().clamp(0.0, u64::MAX as f64) as u64)
    }
}
