use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{ResultExt, StepContext, StepError, StepExecutor, StepOutcome};
use qcg_llm::LlmRuntime;
use qcg_policy::{params_schema, string_schema};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::completion::complete_llm;
use crate::policy::{llm_params, require_prompt};
use crate::prompting::{render_repair_prompt, response_text};
use crate::request::build_request;
use crate::schemas::llm_common_properties;
use crate::validation::validate_llm_node;

pub(crate) struct LlmRepairStep {
    pub(crate) runtime: Arc<LlmRuntime>,
}
#[async_trait]
impl StepExecutor for LlmRepairStep {
    fn type_id(&self) -> &'static str {
        "llm.repair"
    }

    fn traits(&self) -> qcg_engine::StepTraits {
        qcg_engine::StepTraits::parallel()
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["prompt"],
            llm_common_properties(json!({
                "source": string_schema(),
                "target": string_schema(),
            })),
        ))
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = llm_params(node)?;
        validate_llm_node(node, contract, &self.runtime, false, false)?;
        require_prompt(node, &params)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let prompt = render_repair_prompt(ctx, node)?;
        let request = build_request(ctx, node, &self.runtime, prompt, None)?;
        let response = complete_llm(ctx, node, request, |_| json!({ "repair": true })).await?;
        let text = response_text(response.content)?;

        let mut files = Vec::new();
        let params = llm_params(node)?;
        let output_path = params.target.as_ref().or(params.output_file.as_ref());
        if let Some(output_path) = output_path {
            let output_path = ctx.render_inline(node, output_path)?;
            let target = ctx.run.fs.resolve_write(&output_path).step_err(&node.id)?;
            tokio::fs::write(&target, &text).await?;
            files.push(target);
        }
        Ok(StepOutcome::Success {
            output: Some(json!({ "text": text })),
            files,
        })
    }
}
