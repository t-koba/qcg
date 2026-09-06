use crate::common::{bounded_transform_text, package_file, require};
use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{
    StepContext, StepError, StepExecutor, StepOutcome, StepTraits, validate_json_schema_findings,
};
use qcg_policy::{
    MAX_JSON_SCHEMA_BYTES, params_schema, string_schema, validate_bounded_json_schema,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Read as _;

pub(crate) struct CheckSchemaStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckSchemaParams {
    source: String,
    schema: String,
}

#[async_trait]
impl StepExecutor for CheckSchemaStep {
    fn type_id(&self) -> &'static str {
        "check.schema"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "schema"],
            json!({
                "source": string_schema(),
                "schema": string_schema(),
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = check_schema_params(node)?;
        load_package_schema(contract, node, &params.schema)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = check_schema_params(node)?;
        let source = ctx.render_inline(node, &params.source)?;
        let source_path = ctx.run.fs.resolve_read(&source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("source path is not in workspace: {error}"),
            )
        })?;
        let value_source = bounded_transform_text(
            &source_path,
            ctx.run.contract.manifest.runtime.file_input_limit_bytes,
        )
        .await
        .map_err(|error| StepError::failed(&node.id, error))?;
        let value: Value = serde_json::from_str(&value_source)
            .map_err(|error| StepError::failed(&node.id, format!("invalid JSON: {error}")))?;
        let schema = load_package_schema(&ctx.run.contract, node, &params.schema)?;
        let findings = validate_json_schema_findings(&schema, &value, "$");
        if findings.is_empty() {
            Ok(StepOutcome::Success {
                output: Some(json!({ "status": "pass", "source": source })),
                files: vec![],
            })
        } else {
            Ok(StepOutcome::CheckFailed {
                findings,
                output: None,
                files: vec![],
            })
        }
    }
}

fn check_schema_params(node: &NodeDef) -> Result<CheckSchemaParams, StepError> {
    let params: CheckSchemaParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid check.schema params: {error}"))
    })?;
    require(node, Some(&params.source), "source")?;
    require(node, Some(&params.schema), "schema")?;
    Ok(params)
}

fn load_package_schema(
    contract: &Contract,
    node: &NodeDef,
    relative: &str,
) -> Result<Value, StepError> {
    let path = package_file(contract, node, relative, "schema")?;
    let file = std::fs::File::open(&path).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("failed to read schema package file `{relative}`: {error}"),
        )
    })?;
    let mut bytes = Vec::new();
    file.take((MAX_JSON_SCHEMA_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            StepError::failed(
                &node.id,
                format!("failed to read schema package file `{relative}`: {error}"),
            )
        })?;
    if bytes.len() > MAX_JSON_SCHEMA_BYTES {
        return Err(StepError::failed(
            &node.id,
            format!(
                "schema package file `{relative}` exceeds the {MAX_JSON_SCHEMA_BYTES}-byte limit"
            ),
        ));
    }
    let schema: Value = serde_json::from_slice(&bytes).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("schema package file `{relative}` is not valid JSON: {error}"),
        )
    })?;
    validate_bounded_json_schema(&schema).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("schema package file `{relative}` is invalid or unsafe JSON Schema: {error}"),
        )
    })?;
    Ok(schema)
}
