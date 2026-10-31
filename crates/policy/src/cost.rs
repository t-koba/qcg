//! LLM cost policy: budget values and pricing-entry selection.
//!
//! Selection is pure over caller-supplied rows so this crate never depends
//! on contract or transport types: callers project their own records into
//! [`PricingRow`] once per billing decision.

/// Resolved per-run cost enforcement. The contract supplies the values;
/// consumers enforce them without knowing `RunBudget` or `LlmConfig`.
#[derive(Debug, Clone, Copy)]
pub struct LlmCostBudget {
    pub max_tokens: Option<u64>,
    pub max_cost_microusd: Option<u64>,
    /// True when a cost budget exists, so unpriced models and requested
    /// hosted searches with unknown usage fail instead of billing zero.
    pub require_pricing: bool,
}

/// One billable pricing row projected from the caller's model registry.
#[derive(Debug, Clone, Copy)]
pub struct PricingRow<'a> {
    pub provider: &'a str,
    pub model: &'a str,
    pub input_cost_per_million_usd: Option<f64>,
    pub cache_creation_input_cost_per_million_usd: Option<f64>,
    pub cache_read_input_cost_per_million_usd: Option<f64>,
    pub output_cost_per_million_usd: Option<f64>,
    /// Hosted web search USD per provider-billable unit. Absent means unmetered, not free.
    pub web_search_call_cost_usd: Option<f64>,
}

/// Selects the row to bill: among provider/model matches, entries with
/// complete pricing win; otherwise the first match applies. Returns the
/// index of the selected row, so the caller keeps its own record for it,
/// or `None` when nothing matches.
pub fn select_pricing(rows: &[PricingRow<'_>], provider: &str, model: &str) -> Option<usize> {
    let mut first = None;
    for (index, row) in rows.iter().enumerate() {
        if row.provider != provider || row.model != model {
            continue;
        }
        if first.is_none() {
            first = Some(index);
        }
        if row.input_cost_per_million_usd.is_some() && row.output_cost_per_million_usd.is_some() {
            return Some(index);
        }
    }
    first
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row<'a>(
        provider: &'a str,
        model: &'a str,
        input: Option<f64>,
        output: Option<f64>,
    ) -> PricingRow<'a> {
        PricingRow {
            provider,
            model,
            input_cost_per_million_usd: input,
            cache_creation_input_cost_per_million_usd: None,
            cache_read_input_cost_per_million_usd: None,
            output_cost_per_million_usd: output,
            web_search_call_cost_usd: None,
        }
    }

    #[test]
    fn prefers_complete_pricing_over_first_match() {
        let rows = [
            row("openai", "gpt", None, None),
            row("openai", "gpt", Some(1.0), Some(2.0)),
        ];
        let index = select_pricing(&rows, "openai", "gpt").expect("should match");
        assert_eq!(index, 1);
        assert_eq!(rows[index].input_cost_per_million_usd, Some(1.0));
    }

    #[test]
    fn falls_back_to_first_match_without_pricing() {
        let rows = [row("openai", "gpt", None, None)];
        let index = select_pricing(&rows, "openai", "gpt").expect("should match");
        assert_eq!(index, 0);
        assert_eq!(rows[index].input_cost_per_million_usd, None);
    }

    #[test]
    fn rejects_other_providers_and_models() {
        let rows = [row("openai", "gpt", Some(1.0), Some(2.0))];
        assert!(select_pricing(&rows, "other", "gpt").is_none());
        assert!(select_pricing(&rows, "openai", "other").is_none());
        assert!(select_pricing(&[][..], "openai", "gpt").is_none());
    }
}
