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
    /// True when a cost budget exists, so unpriced models fail instead of
    /// billing zero.
    pub require_pricing: bool,
}

/// One billable pricing row projected from the caller's model registry.
#[derive(Debug, Clone, Copy)]
pub struct PricingRow<'a> {
    pub provider: &'a str,
    pub model: &'a str,
    pub input_cost_per_million_usd: Option<f64>,
    pub output_cost_per_million_usd: Option<f64>,
}

/// Selects the row to bill: among provider/model matches, entries with
/// complete pricing win; otherwise the first match applies. Returns `None`
/// when nothing matches.
pub fn select_pricing<'a>(
    rows: &[PricingRow<'a>],
    provider: &str,
    model: &str,
) -> Option<PricingRow<'a>> {
    let mut first = None;
    for row in rows {
        if row.provider != provider || row.model != model {
            continue;
        }
        if first.is_none() {
            first = Some(*row);
        }
        if row.input_cost_per_million_usd.is_some() && row.output_cost_per_million_usd.is_some() {
            return Some(*row);
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
            output_cost_per_million_usd: output,
        }
    }

    #[test]
    fn prefers_complete_pricing_over_first_match() {
        let rows = [
            row("openai", "gpt", None, None),
            row("openai", "gpt", Some(1.0), Some(2.0)),
        ];
        let selected = select_pricing(&rows, "openai", "gpt").expect("should match");
        assert_eq!(selected.input_cost_per_million_usd, Some(1.0));
    }

    #[test]
    fn falls_back_to_first_match_without_pricing() {
        let rows = [row("openai", "gpt", None, None)];
        let selected = select_pricing(&rows, "openai", "gpt").expect("should match");
        assert_eq!(selected.input_cost_per_million_usd, None);
    }

    #[test]
    fn rejects_other_providers_and_models() {
        let rows = [row("openai", "gpt", Some(1.0), Some(2.0))];
        assert!(select_pricing(&rows, "other", "gpt").is_none());
        assert!(select_pricing(&rows, "openai", "other").is_none());
        assert!(select_pricing(&[][..], "openai", "gpt").is_none());
    }
}
