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
    }
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
        };
        let catalog = llm::ModelPricing {
            input_cost_per_million_usd: Some(1.0),
            cache_creation_input_cost_per_million_usd: Some(1.5),
            cache_read_input_cost_per_million_usd: Some(0.2),
            output_cost_per_million_usd: Some(3.0),
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
}
