use async_trait::async_trait;
use qcg_contract::NodeDef;
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome};
use qcg_policy::{params_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};
pub(crate) struct FailStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailParams {
    #[serde(default)]
    content: Option<String>,
}

#[async_trait]
impl StepExecutor for FailStep {
    fn type_id(&self) -> &'static str {
        "fail"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(&[], json!({ "content": string_schema() })))
    }

    async fn execute(
        &self,
        _ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = fail_params(node)?;
        Err(StepError::failed(
            &node.id,
            params.content.as_deref().unwrap_or("fail step reached"),
        ))
    }
}

fn fail_params(node: &NodeDef) -> Result<FailParams, StepError> {
    node.deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid fail params: {error}")))
}
