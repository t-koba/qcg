use async_trait::async_trait;
use qcg_contract::NodeDef;
use qcg_engine::{StepContext, StepControlFlow, StepError, StepExecutor, StepOutcome, StepTraits};
use qcg_policy::{MAX_FOREACH_ITERATIONS, MAX_FOREACH_PARALLELISM, params_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::require;
use qcg_contract::Contract;
pub(crate) struct ForeachStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForeachParams {
    items: String,
    subflow: String,
    max_iterations: usize,
    #[serde(default = "default_foreach_parallelism")]
    parallel: usize,
}

fn default_foreach_parallelism() -> usize {
    1
}

#[async_trait]
impl StepExecutor for ForeachStep {
    fn type_id(&self) -> &'static str {
        "foreach"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["items", "subflow", "max_iterations"],
            json!({
                "items": string_schema(),
                "subflow": string_schema(),
                "max_iterations": { "type": "integer", "minimum": 1, "maximum": MAX_FOREACH_ITERATIONS },
                "parallel": { "type": "integer", "minimum": 1, "maximum": MAX_FOREACH_PARALLELISM },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits {
            control_flow: StepControlFlow::Foreach,
            ..StepTraits::default()
        }
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = foreach_params(node)?;
        if !contract.manifest.blocks.contains_key(&params.subflow) {
            return Err(StepError::failed(
                &node.id,
                format!("unknown subflow `{}`", params.subflow),
            ));
        }
        Ok(())
    }

    async fn execute(
        &self,
        _ctx: &mut StepContext<'_>,
        _node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        Err(StepError::failed(
            "foreach",
            "foreach must be executed by the engine scheduler",
        ))
    }
}

fn foreach_params(node: &NodeDef) -> Result<ForeachParams, StepError> {
    let params: ForeachParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid foreach params: {error}")))?;
    require(node, Some(&params.items), "items")?;
    require(node, Some(&params.subflow), "subflow")?;
    if !(1..=MAX_FOREACH_ITERATIONS).contains(&params.max_iterations) {
        return Err(StepError::failed(
            &node.id,
            format!("max_iterations must be from 1 through {MAX_FOREACH_ITERATIONS}"),
        ));
    }
    if !(1..=MAX_FOREACH_PARALLELISM).contains(&params.parallel) {
        return Err(StepError::failed(
            &node.id,
            format!("parallel must be from 1 through {MAX_FOREACH_PARALLELISM}"),
        ));
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::test_helpers::*;

    #[test]
    fn foreach_rejects_iteration_and_parallelism_above_runtime_bounds() {
        let excessive_iterations = package_node(
            "foreach-iterations",
            "foreach",
            &format!(
                "items = \"inputs.items\"\nsubflow = \"item\"\nmax_iterations = {}",
                MAX_FOREACH_ITERATIONS + 1
            ),
        );
        let error = foreach_params(&excessive_iterations)
            .expect_err("excessive foreach iterations must fail validation");
        assert!(error.to_string().contains("max_iterations"), "{error}");

        let excessive_parallelism = package_node(
            "foreach-parallel",
            "foreach",
            &format!(
                "items = \"inputs.items\"\nsubflow = \"item\"\nmax_iterations = 1\nparallel = {}",
                MAX_FOREACH_PARALLELISM + 1
            ),
        );
        let error = foreach_params(&excessive_parallelism)
            .expect_err("excessive foreach parallelism must fail validation");
        assert!(error.to_string().contains("parallel"), "{error}");
    }
}
