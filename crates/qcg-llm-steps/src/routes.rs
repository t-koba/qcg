use qcg_contract::NodeDef;
use qcg_engine::{StepContext, StepError};
use qcg_llm::ChatRequest;
use std::collections::BTreeSet;

use crate::policy::llm_params;

pub(crate) fn invocation_routes(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    request: &ChatRequest,
    model_override: Option<&qcg_contract::ModelRef>,
    fallback_override: Option<&[qcg_contract::ModelRef]>,
) -> Result<Vec<qcg_contract::ModelRef>, StepError> {
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
    let mut routes = vec![qcg_contract::ModelRef {
        provider: request.provider.clone(),
        model: request.model.clone(),
        input_cost_per_million_usd: pricing.and_then(|model| model.input_cost_per_million_usd),
        output_cost_per_million_usd: pricing.and_then(|model| model.output_cost_per_million_usd),
    }];
    routes.extend(
        fallback_override
            .unwrap_or(&params.fallback_models)
            .iter()
            .cloned(),
    );
    validate_route_sequence(node, routes.first(), &routes[1..], "LLM routes")?;
    Ok(routes)
}

pub(crate) fn validate_route_sequence(
    node: &NodeDef,
    primary: Option<&qcg_contract::ModelRef>,
    fallbacks: &[qcg_contract::ModelRef],
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
