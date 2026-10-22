//! Context-tier pricing: the rates that apply to a whole request once its
//! prompt input reaches a tier's threshold.
//!
//! A tier prices a request, not a slice of it: the selected tier's rates
//! apply to every token of the request, chosen by the request's whole prompt
//! input (ordinary input plus the disjoint cache read and cache creation
//! subsets), so no token is counted twice or split across two tiers.
//!
//! The rules live here so the manifest check, the catalog reader, the route
//! selection and the cost meter all price the same way.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

/// One context tier of a model's price list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CostTier {
    /// Prompt-input tokens from which this tier's rates apply: a request
    /// that reaches this threshold is priced entirely at this tier's rates.
    pub threshold_tokens: u64,
    /// Ordinary input USD/1M. Absent inherits the base row's input rate.
    #[serde(default)]
    pub input_cost_per_million_usd: Option<f64>,
    /// Cache read USD/1M. Absent inherits the base row's cache rate.
    #[serde(default)]
    pub cache_read_input_cost_per_million_usd: Option<f64>,
    /// Cache creation USD/1M. Absent inherits the base row's cache rate.
    #[serde(default)]
    pub cache_creation_input_cost_per_million_usd: Option<f64>,
    /// Output USD/1M. Absent inherits the base row's output rate.
    #[serde(default)]
    pub output_cost_per_million_usd: Option<f64>,
}

impl CostTier {
    /// The first rate that is not finite and non-negative, named by its
    /// field. `None` when every configured rate is valid.
    pub fn invalid_rate(&self) -> Option<&'static str> {
        [
            (
                "input_cost_per_million_usd",
                self.input_cost_per_million_usd,
            ),
            (
                "cache_read_input_cost_per_million_usd",
                self.cache_read_input_cost_per_million_usd,
            ),
            (
                "cache_creation_input_cost_per_million_usd",
                self.cache_creation_input_cost_per_million_usd,
            ),
            (
                "output_cost_per_million_usd",
                self.output_cost_per_million_usd,
            ),
        ]
        .into_iter()
        .find(|(_, value)| value.is_some_and(|value| !value.is_finite() || value < 0.0))
        .map(|(name, _)| name)
    }
}

/// The four rates one request is billed at.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CostRates {
    pub input: Option<f64>,
    pub cache_read: Option<f64>,
    pub cache_creation: Option<f64>,
    pub output: Option<f64>,
}

impl CostRates {
    /// The rates that apply to a request, given the tier it reaches. A tier
    /// is a whole price row: the rates it names win, and a rate it does not
    /// name inherits the base row's rate for that category. The base row
    /// prices an absent cache rate at its own ordinary input rate, and a
    /// tier's own input rate never prices cache tokens.
    pub fn resolve(&self, tier: Option<&CostTier>) -> CostRates {
        let input = tier
            .and_then(|tier| tier.input_cost_per_million_usd)
            .or(self.input);
        let output = tier
            .and_then(|tier| tier.output_cost_per_million_usd)
            .or(self.output);
        let base_cache_read = self.cache_read.or(self.input);
        let base_cache_creation = self.cache_creation.or(self.input);
        let (cache_read, cache_creation) = match tier {
            Some(tier) => (
                tier.cache_read_input_cost_per_million_usd
                    .or(base_cache_read),
                tier.cache_creation_input_cost_per_million_usd
                    .or(base_cache_creation),
            ),
            None => (base_cache_read, base_cache_creation),
        };
        CostRates {
            input,
            cache_read,
            cache_creation,
            output,
        }
    }

    /// The first rate that is not finite and non-negative, named by its
    /// field. `None` when every configured rate is valid.
    pub fn invalid_rate(&self) -> Option<&'static str> {
        [
            ("input_cost_per_million_usd", self.input),
            ("cache_read_input_cost_per_million_usd", self.cache_read),
            (
                "cache_creation_input_cost_per_million_usd",
                self.cache_creation,
            ),
            ("output_cost_per_million_usd", self.output),
        ]
        .into_iter()
        .find(|(_, value)| value.is_some_and(|value| !value.is_finite() || value < 0.0))
        .map(|(name, _)| name)
    }

    /// The dearest of the input-category rates: the rate an input token
    /// costs when the request's cache split is unknown.
    pub fn max_input_rate(&self) -> Option<f64> {
        [self.input, self.cache_read, self.cache_creation]
            .into_iter()
            .flatten()
            .fold(None, |dearest: Option<f64>, rate| {
                Some(dearest.map_or(rate, |dearest| dearest.max(rate)))
            })
    }
}

/// The tier that prices a request whose prompt input is
/// `prompt_input_tokens`: the tier with the greatest threshold the request
/// reaches. `None` when the request stays below every tier, so the base
/// row's rates apply.
pub fn select_cost_tier(tiers: &[CostTier], prompt_input_tokens: u64) -> Option<&CostTier> {
    tiers
        .iter()
        .filter(|tier| tier.threshold_tokens <= prompt_input_tokens)
        .max_by_key(|tier| tier.threshold_tokens)
}

/// The dearest rates a request can be billed at: the base row and every tier
/// it can reach. A pre-call estimate cannot know which tier a request reaches,
/// so it reserves at these rates rather than at a tier it cannot prove.
pub fn max_cost_rates(base: &CostRates, tiers: &[CostTier]) -> CostRates {
    let mut rates = base.resolve(None);
    for tier in tiers {
        let row = base.resolve(Some(tier));
        rates.input = max_rate(rates.input, row.input);
        rates.cache_read = max_rate(rates.cache_read, row.cache_read);
        rates.cache_creation = max_rate(rates.cache_creation, row.cache_creation);
        rates.output = max_rate(rates.output, row.output);
    }
    rates
}

fn max_rate(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, right) => right,
    }
}

/// Why a tier list cannot be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CostTierError {
    /// A tier names a rate that is not finite and non-negative.
    InvalidRate { index: usize, field: &'static str },
    /// A tier's threshold does not exceed the previous tier's threshold, so
    /// two tiers could claim the same request.
    ThresholdNotIncreasing {
        index: usize,
        threshold: u64,
        previous: u64,
    },
}

impl fmt::Display for CostTierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRate { index, field } => write!(
                formatter,
                "cost tier {index} has an invalid {field}: the rate must be finite and non-negative"
            ),
            Self::ThresholdNotIncreasing {
                index,
                threshold,
                previous,
            } => write!(
                formatter,
                "cost tier {index} has threshold {threshold}, which does not exceed the previous tier's threshold {previous}"
            ),
        }
    }
}

impl std::error::Error for CostTierError {}

/// Checks that a tier list can price requests: every rate is finite and
/// non-negative, and the thresholds strictly increase so exactly one tier
/// can claim a request.
pub fn validate_cost_tiers(tiers: &[CostTier]) -> Result<(), CostTierError> {
    let mut previous: Option<u64> = None;
    for (index, tier) in tiers.iter().enumerate() {
        if let Some(field) = tier.invalid_rate() {
            return Err(CostTierError::InvalidRate { index, field });
        }
        if let Some(previous) = previous
            && tier.threshold_tokens <= previous
        {
            return Err(CostTierError::ThresholdNotIncreasing {
                index,
                threshold: tier.threshold_tokens,
                previous,
            });
        }
        previous = Some(tier.threshold_tokens);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tier(threshold_tokens: u64, input: Option<f64>) -> CostTier {
        CostTier {
            threshold_tokens,
            input_cost_per_million_usd: input,
            cache_read_input_cost_per_million_usd: None,
            cache_creation_input_cost_per_million_usd: None,
            output_cost_per_million_usd: None,
        }
    }

    fn base() -> CostRates {
        CostRates {
            input: Some(1.0),
            cache_read: Some(0.1),
            cache_creation: None,
            output: Some(2.0),
        }
    }

    #[test]
    fn the_tier_a_request_reaches_is_the_highest_threshold_it_reaches() {
        let tiers = [tier(200_000, Some(2.0)), tier(400_000, Some(4.0))];
        assert_eq!(select_cost_tier(&tiers, 199_999), None);
        assert_eq!(
            select_cost_tier(&tiers, 200_000).map(|tier| tier.threshold_tokens),
            Some(200_000)
        );
        assert_eq!(
            select_cost_tier(&tiers, 399_999).map(|tier| tier.threshold_tokens),
            Some(200_000)
        );
        assert_eq!(
            select_cost_tier(&tiers, 400_000).map(|tier| tier.threshold_tokens),
            Some(400_000)
        );
    }

    #[test]
    fn a_tier_rate_wins_and_an_absent_rate_inherits_the_base_row() {
        let rows = base().resolve(Some(&CostTier {
            threshold_tokens: 200_000,
            input_cost_per_million_usd: Some(5.0),
            cache_read_input_cost_per_million_usd: None,
            cache_creation_input_cost_per_million_usd: None,
            output_cost_per_million_usd: None,
        }));
        assert_eq!(rows.input, Some(5.0));
        // A cache rate the tier does not name inherits the base row's
        // cache rate, never the tier's own input rate. The base row names
        // no cache-creation rate, so it prices cache creation at its own
        // ordinary input rate.
        assert_eq!(rows.cache_read, Some(0.1));
        assert_eq!(rows.cache_creation, Some(1.0));
        // An absent tier output rate keeps the base row's output rate.
        assert_eq!(rows.output, Some(2.0));
    }

    /// A base row that names its cache rates.
    fn base_with_cache_rates() -> CostRates {
        CostRates {
            input: Some(2.0),
            cache_read: Some(0.2),
            cache_creation: Some(2.5),
            output: Some(3.0),
        }
    }

    /// A tier that names only its input and output rates.
    fn tier_without_cache_rates() -> CostTier {
        CostTier {
            threshold_tokens: 200_000,
            input_cost_per_million_usd: Some(5.0),
            cache_read_input_cost_per_million_usd: None,
            cache_creation_input_cost_per_million_usd: None,
            output_cost_per_million_usd: Some(6.0),
        }
    }

    #[test]
    fn a_tier_that_names_no_cache_rate_inherits_the_base_row_cache_rate() {
        let rows = base_with_cache_rates().resolve(Some(&tier_without_cache_rates()));
        assert_eq!(rows.input, Some(5.0));
        assert_eq!(rows.output, Some(6.0));
        // The base row's cache rates price the cache tokens: the provider's
        // cache price, not the tier's ordinary input price.
        assert_eq!(rows.cache_read, Some(0.2));
        assert_eq!(rows.cache_creation, Some(2.5));
    }

    #[test]
    fn a_tier_never_prices_cache_tokens_at_its_own_input_rate() {
        let base = CostRates {
            input: Some(2.0),
            cache_read: None,
            cache_creation: None,
            output: Some(3.0),
        };
        let rows = base.resolve(Some(&tier_without_cache_rates()));
        // The base row names no cache rate, so it prices cache tokens at
        // its own ordinary input rate; the tier's higher input rate does
        // not price them.
        assert_eq!(rows.cache_read, Some(2.0));
        assert_eq!(rows.cache_creation, Some(2.0));
    }

    #[test]
    fn the_reservation_does_not_price_cache_tokens_at_a_tier_input_rate() {
        let rates = max_cost_rates(&base_with_cache_rates(), &[tier_without_cache_rates()]);
        assert_eq!(rates.input, Some(5.0));
        assert_eq!(rates.cache_read, Some(0.2));
        assert_eq!(rates.cache_creation, Some(2.5));
    }

    #[test]
    fn the_base_row_prices_cache_reads_at_its_own_input_rate() {
        let rows = base().resolve(None);
        assert_eq!(rows.input, Some(1.0));
        assert_eq!(rows.cache_read, Some(0.1));
        assert_eq!(rows.cache_creation, Some(1.0));
        assert_eq!(rows.output, Some(2.0));
    }

    #[test]
    fn the_dearest_rates_cover_every_tier_a_request_can_reach() {
        let tiers = [tier(200_000, Some(2.0)), tier(400_000, Some(4.0))];
        let rates = max_cost_rates(&base(), &tiers);
        assert_eq!(rates.max_input_rate(), Some(4.0));
        assert_eq!(rates.output, Some(2.0));
    }

    #[test]
    fn tiers_must_have_valid_rates_and_increasing_thresholds() {
        assert!(validate_cost_tiers(&[tier(200_000, Some(1.0)), tier(400_000, None)]).is_ok());
        let invalid = validate_cost_tiers(&[CostTier {
            threshold_tokens: 200_000,
            input_cost_per_million_usd: Some(f64::NAN),
            cache_read_input_cost_per_million_usd: None,
            cache_creation_input_cost_per_million_usd: None,
            output_cost_per_million_usd: None,
        }])
        .expect_err("a non-finite rate must fail");
        assert_eq!(
            invalid,
            CostTierError::InvalidRate {
                index: 0,
                field: "input_cost_per_million_usd"
            }
        );
        let unordered = validate_cost_tiers(&[tier(400_000, None), tier(200_000, None)])
            .expect_err("an unordered threshold must fail");
        assert_eq!(
            unordered,
            CostTierError::ThresholdNotIncreasing {
                index: 1,
                threshold: 200_000,
                previous: 400_000
            }
        );
    }
}
