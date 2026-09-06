use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome};
use qcg_llm::LlmRuntime;
use qcg_policy::{params_schema, string_array_schema};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::completion::{
    checked_usage_total, complete_llm, complete_text_with_prompt, validate_retry_budget,
};
use crate::context::{record_llm_validation_failure, retry_prompt};
use crate::policy::{llm_params, require_prompt};
use crate::prompting::{render_prompt, response_text};
use crate::request::build_request;
use crate::schemas::llm_common_properties;
use crate::validation::validate_llm_node;

pub(crate) struct LlmChooseStep {
    pub(crate) runtime: Arc<LlmRuntime>,
}
#[async_trait]
impl StepExecutor for LlmChooseStep {
    fn type_id(&self) -> &'static str {
        "llm.choose"
    }

    fn traits(&self) -> qcg_engine::StepTraits {
        qcg_engine::StepTraits::parallel()
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["prompt", "options", "max_iterations", "max_tokens_total"],
            llm_common_properties(json!({
                "options": string_array_schema(),
                "max_iterations": { "type": "integer", "minimum": 1 },
                "max_tokens_total": { "type": "integer", "minimum": 1 },
            })),
        ))
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = llm_params(node)?;
        validate_llm_node(node, contract, &self.runtime, false, false)?;
        require_prompt(node, &params)?;
        if params.options.is_empty() {
            return Err(StepError::failed(&node.id, "options is required"));
        }
        validate_retry_budget(node, &params)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let mut base_prompt = render_prompt(ctx, node)?;
        base_prompt.push_str("\nQCG_OPTIONS: ");
        let params = llm_params(node)?;
        base_prompt.push_str(&serde_json::to_string(&params.options)?);
        let max_attempts = params.max_iterations.expect("validated max_iterations");
        let max_tokens_total = params.max_tokens_total.expect("validated max_tokens_total");
        let mut tokens_total = 0_u64;
        let mut last_error = None;
        for attempt in 0..max_attempts {
            let prompt = if attempt == 0 {
                base_prompt.clone()
            } else {
                retry_prompt(ctx, node, &base_prompt, attempt, last_error.as_deref())?
            };
            let request = build_request(ctx, node, &self.runtime, prompt, None)?;
            let response =
                complete_llm(ctx, node, request, |_| json!({ "attempt": attempt })).await?;
            tokens_total = checked_usage_total(node, tokens_total, &response.usage)?;
            if tokens_total > max_tokens_total {
                return Err(StepError::failed(
                    &node.id,
                    format!(
                        "llm.choose token budget exceeded: {tokens_total} > {max_tokens_total}"
                    ),
                ));
            }
            let choice = response_text(response.content)?;
            if params.options.iter().any(|option| option == &choice) {
                return Ok(StepOutcome::Success {
                    output: Some(Value::String(choice)),
                    files: vec![],
                });
            }
            let message = format!("LLM chose `{choice}`, which is outside declared options");
            record_llm_validation_failure(ctx, node, attempt, &message)?;
            last_error = Some(message);
        }
        Err(StepError::failed(
            &node.id,
            format!("LLM did not choose one of declared options after {max_attempts} attempt(s)"),
        ))
    }
}

pub(crate) async fn complete_text(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    runtime: &LlmRuntime,
    response_schema: Option<Value>,
) -> Result<String, StepError> {
    let prompt = render_prompt(ctx, node)?;
    complete_text_with_prompt(ctx, node, runtime, response_schema, prompt, 0)
        .await
        .map(|completion| completion.text)
}
