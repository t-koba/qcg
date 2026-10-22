use contract::NodeDef;
use engine::{StepContext, StepError};
use llm::ChatRequest;
use std::collections::BTreeSet;

use crate::request_policy::llm_params;

pub(crate) fn invocation_routes(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    request: &ChatRequest,
    model_override: Option<&contract::ModelRef>,
    fallback_override: Option<&[contract::ModelRef]>,
) -> Result<Vec<contract::ModelRef>, StepError> {
    let llm = ctx
        .run
        .contract
        .manifest
        .llm
        .as_ref()
        .ok_or_else(|| StepError::failed(&node.id, "[llm] is required"))?;
    let params = llm_params(node)?;
    let pricing = model_override
        .into_iter()
        .chain(params.model.iter())
        .chain(llm.model.iter())
        .chain(llm.models.iter())
        .find(|model| model.provider == request.provider && model.model == request.model);
    // Catalog prices fill contract gaps so operator-selected models can be
    // metered under `budget.max_cost_usd` without duplicating prices in every
    // contract. Explicit contract values always win, field by field.
    let route = |provider: &str, model: &str, contract: Option<&contract::ModelRef>| {
        let catalog = ctx
            .run
            .llm_provider
            .as_ref()
            .and_then(|registry| registry.model_pricing_for(provider, model));
        priced_route(provider, model, contract, catalog)
    };
    let mut routes = vec![route(&request.provider, &request.model, pricing)];
    for fallback in fallback_override.unwrap_or(&params.fallback_models) {
        routes.push(route(&fallback.provider, &fallback.model, Some(fallback)));
    }
    validate_route_sequence(node, routes.first(), &routes[1..], "LLM routes")?;
    Ok(routes)
}

pub(crate) fn validate_route_sequence(
    node: &NodeDef,
    primary: Option<&contract::ModelRef>,
    fallbacks: &[contract::ModelRef],
    field: &str,
) -> Result<(), StepError> {
    let mut seen = BTreeSet::new();
    if let Some(primary) = primary {
        seen.insert((primary.provider.as_str(), primary.model.as_str()));
    }
    for fallback in fallbacks {
        if !seen.insert((fallback.provider.as_str(), fallback.model.as_str())) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "{field} contains duplicate route `{}/{}`",
                    fallback.provider, fallback.model
                ),
            ));
        }
    }
    Ok(())
}

fn priced_route(
    provider: &str,
    model: &str,
    contract: Option<&contract::ModelRef>,
    catalog: Option<llm::ModelPricing>,
) -> contract::ModelRef {
    let cost_tiers = priced_tiers(contract, catalog.as_ref());
    contract::ModelRef {
        provider: provider.to_owned(),
        model: model.to_owned(),
        input_cost_per_million_usd: contract
            .and_then(|pricing| pricing.input_cost_per_million_usd)
            .or_else(|| {
                catalog
                    .as_ref()
                    .and_then(|pricing| pricing.input_cost_per_million_usd)
            }),
        cache_read_input_cost_per_million_usd: contract
            .and_then(|pricing| pricing.cache_read_input_cost_per_million_usd)
            .or_else(|| {
                catalog
                    .as_ref()
                    .and_then(|pricing| pricing.cache_read_input_cost_per_million_usd)
            }),
        cache_creation_input_cost_per_million_usd: contract
            .and_then(|pricing| pricing.cache_creation_input_cost_per_million_usd)
            .or_else(|| {
                catalog
                    .as_ref()
                    .and_then(|pricing| pricing.cache_creation_input_cost_per_million_usd)
            }),
        output_cost_per_million_usd: contract
            .and_then(|pricing| pricing.output_cost_per_million_usd)
            .or_else(|| {
                catalog
                    .as_ref()
                    .and_then(|pricing| pricing.output_cost_per_million_usd)
            }),
        web_search_call_cost_usd: None,
        cost_tiers,
    }
}

/// The context tiers that price this route. A contract that declares its
/// own tiers owns the whole schedule; otherwise the catalog's tiers apply.
/// A rate the contract pinned is never overridden by a catalog tier: the
/// pinned rate is carried into every tier, so it prices every tier, and
/// the tier contributes only the rates the contract left open.
fn priced_tiers(
    contract: Option<&contract::ModelRef>,
    catalog: Option<&llm::ModelPricing>,
) -> Vec<policy::CostTier> {
    let Some(contract) = contract else {
        return catalog
            .map(|pricing| pricing.cost_tiers.clone())
            .unwrap_or_default();
    };
    if !contract.cost_tiers.is_empty() {
        return contract.cost_tiers.clone();
    }
    let Some(catalog) = catalog else {
        return Vec::new();
    };
    let rate = |declared: Option<f64>, tier: Option<f64>| declared.or(tier);
    catalog
        .cost_tiers
        .iter()
        .map(|tier| policy::CostTier {
            threshold_tokens: tier.threshold_tokens,
            input_cost_per_million_usd: rate(
                contract.input_cost_per_million_usd,
                tier.input_cost_per_million_usd,
            ),
            cache_read_input_cost_per_million_usd: rate(
                contract.cache_read_input_cost_per_million_usd,
                tier.cache_read_input_cost_per_million_usd,
            ),
            cache_creation_input_cost_per_million_usd: rate(
                contract.cache_creation_input_cost_per_million_usd,
                tier.cache_creation_input_cost_per_million_usd,
            ),
            output_cost_per_million_usd: rate(
                contract.output_cost_per_million_usd,
                tier.output_cost_per_million_usd,
            ),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_contract_rates_override_catalog_field_by_field() {
        let contract = contract::ModelRef {
            provider: "p".into(),
            model: "m".into(),
            input_cost_per_million_usd: Some(9.0),
            cache_read_input_cost_per_million_usd: None,
            cache_creation_input_cost_per_million_usd: Some(0.0),
            output_cost_per_million_usd: None,
            web_search_call_cost_usd: None,
            cost_tiers: Vec::new(),
        };
        let catalog = llm::ModelPricing {
            input_cost_per_million_usd: Some(1.0),
            cache_creation_input_cost_per_million_usd: Some(1.5),
            cache_read_input_cost_per_million_usd: Some(0.2),
            output_cost_per_million_usd: Some(3.0),
            cost_tiers: Vec::new(),
        };
        let resolved = priced_route("p", "m", Some(&contract), Some(catalog));
        assert_eq!(resolved.input_cost_per_million_usd, Some(9.0));
        assert_eq!(resolved.cache_read_input_cost_per_million_usd, Some(0.2));
        assert_eq!(
            resolved.cache_creation_input_cost_per_million_usd,
            Some(0.0)
        );
        assert_eq!(resolved.output_cost_per_million_usd, Some(3.0));
    }

    #[test]
    fn a_pinned_rate_prices_every_tier_and_a_contract_schedule_owns_the_route() {
        let catalog_tier = policy::CostTier {
            threshold_tokens: 200_000,
            input_cost_per_million_usd: Some(2.0),
            cache_read_input_cost_per_million_usd: Some(0.2),
            cache_creation_input_cost_per_million_usd: None,
            output_cost_per_million_usd: Some(4.0),
        };
        let catalog = llm::ModelPricing {
            input_cost_per_million_usd: Some(1.0),
            cache_creation_input_cost_per_million_usd: Some(1.5),
            cache_read_input_cost_per_million_usd: Some(0.2),
            output_cost_per_million_usd: Some(3.0),
            cost_tiers: vec![catalog_tier.clone()],
        };
        let contract = contract::ModelRef {
            provider: "p".into(),
            model: "m".into(),
            input_cost_per_million_usd: Some(9.0),
            cache_read_input_cost_per_million_usd: None,
            cache_creation_input_cost_per_million_usd: None,
            output_cost_per_million_usd: None,
            web_search_call_cost_usd: None,
            cost_tiers: Vec::new(),
        };
        let resolved = priced_route("p", "m", Some(&contract), Some(catalog.clone()));
        assert_eq!(resolved.cost_tiers.len(), 1);
        assert_eq!(resolved.cost_tiers[0].threshold_tokens, 200_000);
        // The pinned rate prices every tier, and the catalog tier fills
        // only the rates the contract left open: the pinned input rate
        // is carried into the tier, so the catalog's tier rate for that
        // category never overrides it.
        assert_eq!(resolved.cost_tiers[0].input_cost_per_million_usd, Some(9.0));
        assert_eq!(
            resolved.cost_tiers[0].cache_read_input_cost_per_million_usd,
            Some(0.2)
        );
        // A contract that declares its own schedule owns the whole
        // schedule.
        let mut own = contract.clone();
        own.cost_tiers = vec![policy::CostTier {
            threshold_tokens: 100_000,
            input_cost_per_million_usd: Some(5.0),
            ..catalog_tier
        }];
        let resolved = priced_route("p", "m", Some(&own), Some(catalog));
        assert_eq!(resolved.cost_tiers, own.cost_tiers);
    }

    #[test]
    fn a_pinned_cache_rate_is_never_overridden_by_a_catalog_tier() {
        let catalog = llm::ModelPricing {
            input_cost_per_million_usd: Some(1.0),
            cache_creation_input_cost_per_million_usd: Some(1.5),
            cache_read_input_cost_per_million_usd: Some(0.2),
            output_cost_per_million_usd: Some(3.0),
            cost_tiers: vec![policy::CostTier {
                threshold_tokens: 200_000,
                input_cost_per_million_usd: Some(2.0),
                cache_read_input_cost_per_million_usd: Some(0.2),
                cache_creation_input_cost_per_million_usd: None,
                output_cost_per_million_usd: Some(4.0),
            }],
        };
        let contract = contract::ModelRef {
            provider: "p".into(),
            model: "m".into(),
            input_cost_per_million_usd: None,
            cache_read_input_cost_per_million_usd: Some(0.1),
            cache_creation_input_cost_per_million_usd: None,
            output_cost_per_million_usd: None,
            web_search_call_cost_usd: None,
            cost_tiers: Vec::new(),
        };
        let resolved = priced_route("p", "m", Some(&contract), Some(catalog));
        // The contract pinned the cache read rate, so the catalog tier's
        // own rate for that category never overrides it.
        assert_eq!(
            resolved.cost_tiers[0].cache_read_input_cost_per_million_usd,
            Some(0.1)
        );
    }

    #[test]
    fn a_pinned_input_rate_does_not_price_cache_tokens_at_the_tier_threshold() {
        // The catalog's base row names a cache rate; its tier names none.
        let catalog = llm::ModelPricing {
            input_cost_per_million_usd: Some(1.0),
            cache_creation_input_cost_per_million_usd: Some(1.5),
            cache_read_input_cost_per_million_usd: Some(0.2),
            output_cost_per_million_usd: Some(3.0),
            cost_tiers: vec![policy::CostTier {
                threshold_tokens: 200_000,
                input_cost_per_million_usd: Some(2.0),
                cache_read_input_cost_per_million_usd: None,
                cache_creation_input_cost_per_million_usd: None,
                output_cost_per_million_usd: Some(4.0),
            }],
        };
        // The contract pins the input rate and no cache rate.
        let contract = contract::ModelRef {
            provider: "p".into(),
            model: "m".into(),
            input_cost_per_million_usd: Some(9.0),
            cache_read_input_cost_per_million_usd: None,
            cache_creation_input_cost_per_million_usd: None,
            output_cost_per_million_usd: None,
            web_search_call_cost_usd: None,
            cost_tiers: Vec::new(),
        };
        let resolved = priced_route("p", "m", Some(&contract), Some(catalog));
        // The tier names no cache rate, so its cache slots stay open.
        assert_eq!(
            resolved.cost_tiers[0].cache_read_input_cost_per_million_usd,
            None
        );
        assert_eq!(
            resolved.cost_tiers[0].cache_creation_input_cost_per_million_usd,
            None
        );
        // Below the threshold the base row prices the request.
        let below = resolved
            .rates()
            .resolve(policy::select_cost_tier(&resolved.cost_tiers, 199_999));
        assert_eq!(below.cache_read, Some(0.2));
        assert_eq!(below.cache_creation, Some(1.5));
        // At the threshold the tier prices it. The pinned input rate
        // prices the ordinary input, and the cache tokens are priced at
        // the base row's cache rate - not at the pinned input rate the
        // tier carries.
        let at = resolved
            .rates()
            .resolve(policy::select_cost_tier(&resolved.cost_tiers, 200_000));
        assert_eq!(at.input, Some(9.0));
        assert_eq!(at.cache_read, Some(0.2));
        assert_eq!(at.cache_creation, Some(1.5));
    }
}
