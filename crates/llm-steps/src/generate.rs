use async_trait::async_trait;
use contract::{Contract, NodeDef};
use engine::{StepContext, StepError, StepExecutor, StepOutcome, validate_json_schema_step};
use llm::LlmRuntime;
use policy::{params_schema, string_schema};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::choose::complete_text;
use crate::completion::{
    checked_usage_total, complete_text_with_prompt, local_exhaustion, validate_retry_budget,
};
use crate::context::{record_llm_validation_failure, retry_prompt};
use crate::out_of_contract::{OutOfContractDecision, enforce_out_of_contract_policy};
use crate::output::write_output_file;
use crate::prompting::{load_schema, parse_llm_json, render_prompt};
use crate::request_policy::{llm_params, require_prompt};
use crate::schemas::llm_common_properties;
use crate::validation::validate_llm_node;

pub(crate) struct LlmGenerateStep {
    pub(crate) runtime: Arc<LlmRuntime>,
}

pub(crate) struct LlmFillStep {
    pub(crate) runtime: Arc<LlmRuntime>,
}
#[async_trait]
impl StepExecutor for LlmGenerateStep {
    engine::step_type_id!("llm.generate");

    engine::parallel_step_traits!();

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(&["prompt"], llm_common_properties(json!({}))))
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
        let response = complete_text(ctx, node, &self.runtime, None).await?;
        let params = llm_params(node)?;
        let files = write_output_file(
            ctx,
            node,
            params.output_file.as_deref(),
            response.as_bytes(),
        )
        .await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "text": response })),
            files,
        })
    }
}

#[async_trait]
impl StepExecutor for LlmFillStep {
    engine::step_type_id!("llm.fill");

    engine::parallel_step_traits!();

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["prompt", "max_iterations", "max_tokens_total"],
            llm_common_properties(json!({
                "schema": string_schema(),
                "max_iterations": { "type": "integer", "minimum": 1 },
                "max_tokens_total": { "type": "integer", "minimum": 1 },
            })),
        ))
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = llm_params(node)?;
        validate_llm_node(
            node,
            contract,
            &self.runtime,
            params.schema.is_some(),
            false,
        )?;
        require_prompt(node, &params)?;
        validate_retry_budget(node, &params)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let schema = load_schema(ctx, node)?;
        let base_prompt = render_prompt(ctx, node)?;
        let params = llm_params(node)?;
        let max_attempts = params.max_iterations.expect("validated max_iterations");
        let max_tokens_total = params.max_tokens_total.expect("validated max_tokens_total");
        let mut tokens_total = 0_u64;
        let mut last_error = None;
        for attempt in 0..max_attempts {
            let prompt = retry_prompt(ctx, node, &base_prompt, attempt, last_error.as_deref())?;
            let completion = complete_text_with_prompt(
                ctx,
                node,
                &self.runtime,
                schema.clone(),
                prompt,
                attempt,
            )
            .await?;
            tokens_total = checked_usage_total(node, tokens_total, &completion.usage)?;
            if tokens_total > max_tokens_total {
                return Err(local_exhaustion(
                    &node.id,
                    "llm.fill token budget exceeded",
                    ("tokens_total", tokens_total, max_tokens_total),
                ));
            }
            let text = completion.text;
            let value = match parse_llm_json(&text) {
                Ok(value) => value,
                Err(error) => {
                    let message = format!("LLM response was not JSON: {error}");
                    record_llm_validation_failure(ctx, node, attempt, &message)?;
                    last_error = Some(message);
                    continue;
                }
            };
            let value = match enforce_out_of_contract_policy(ctx, node, value)? {
                OutOfContractDecision::Continue(value) => value,
                OutOfContractDecision::NeedsUser { question } => {
                    return Ok(StepOutcome::NeedsUser {
                        question,
                        files: Vec::new(),
                    });
                }
            };
            if let Some(schema) = &schema
                && let Err(error) =
                    validate_json_schema_step(&node.id, schema, &value, "LLM response")
            {
                let message = error.to_string();
                record_llm_validation_failure(ctx, node, attempt, &message)?;
                last_error = Some(message);
                continue;
            }
            let content = serde_json::to_vec(&value)?;
            let files =
                write_output_file(ctx, node, params.output_file.as_deref(), &content).await?;
            return Ok(StepOutcome::Success {
                output: Some(value),
                files,
            });
        }
        Err(StepError::failed(
            &node.id,
            format!(
                "LLM response did not satisfy schema after {max_attempts} attempt(s): {}",
                last_error.unwrap_or_else(|| "unknown validation error".into())
            ),
        ))
    }
}
