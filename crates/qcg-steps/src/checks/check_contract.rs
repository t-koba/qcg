use crate::common::require;
use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome, StepTraits};
use qcg_policy::{params_schema, string_schema};
use qcg_types::{Finding, Severity};
use serde::Deserialize;
use serde_json::{Value, json};

pub(crate) struct CheckContractStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckContractParams {
    source: String,
}

#[async_trait]
impl StepExecutor for CheckContractStep {
    fn type_id(&self) -> &'static str {
        "check.contract"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source"],
            json!({ "source": string_schema() }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = check_contract_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = check_contract_params(node)?;
        let source = ctx.render_inline(node, &params.source)?;
        let generator_dir = ctx.run.fs.resolve_read(&source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("source path is not in workspace: {error}"),
            )
        })?;
        // Load from a private handle-relative snapshot instead of the live
        // workspace tree: a parent swapped after resolution cannot redirect
        // the parse, and the snapshot enforces the input limits itself (E13).
        // The `.qcg-part-` prefix is the single unified temp prefix swept
        // at startup.
        let snapshot = ctx
            .run
            .metadata
            .join(format!(".qcg-part-{}", uuid::Uuid::now_v7()));
        let loaded = ctx
            .run
            .fs
            .snapshot_tree_to(
                &generator_dir,
                &snapshot,
                ctx.run.contract.manifest.runtime.file_input_limit_bytes,
                ctx.run.contract.manifest.runtime.file_count_limit,
            )
            .map_err(|error| StepError::failed(&node.id, error.to_string()))
            .and_then(|()| {
                Contract::load(&snapshot)
                    .map_err(|error| StepError::failed(&node.id, error.to_string()))
            });
        // Temp cleanup failures propagate fail-closed (E13-9): a leftover
        // snapshot must surface instead of silently accumulating.
        std::fs::remove_dir_all(&snapshot)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        match loaded {
            Ok(contract) => Ok(StepOutcome::Success {
                output: Some(json!({
                    "status": "pass",
                    "generator": contract.manifest.generator.id,
                    "version": contract.manifest.generator.version,
                    "contract_sha256": contract.sha256,
                })),
                files: vec![],
            }),
            Err(error) => Ok(StepOutcome::CheckFailed {
                findings: vec![Finding {
                    severity: Severity::Error,
                    message: error.to_string(),
                    location: Some(source),
                    raw_output: None,
                }],
                output: None,
                files: vec![],
            }),
        }
    }
}

fn check_contract_params(node: &NodeDef) -> Result<CheckContractParams, StepError> {
    let params: CheckContractParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid check.contract params: {error}"))
    })?;
    require(node, Some(&params.source), "source")?;
    Ok(params)
}
