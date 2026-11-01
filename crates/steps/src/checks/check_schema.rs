use crate::common::{bounded_transform_text, package_file, parse_json, require};
use async_trait::async_trait;
use contract::{Contract, NodeDef};
use engine::{StepContext, StepError, StepExecutor, StepOutcome, validate_json_schema_findings};
use policy::{MAX_JSON_SCHEMA_BYTES, params_schema, string_schema, validate_bounded_json_schema};
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
    engine::step_type_id!("check.schema");

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "schema"],
            json!({
                "source": string_schema(),
                "schema": string_schema(),
            }),
        ))
    }

    engine::parallel_step_traits!();

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
            &ctx.run.fs,
            &source_path,
            ctx.run.contract.manifest.runtime.file_input_limit_bytes,
        )
        .await
        .map_err(|error| StepError::failed(&node.id, error))?;
        let value: Value = parse_json(value_source.as_bytes())
            .map_err(|error| StepError::failed(&node.id, format!("invalid JSON: {error}")))?;
        let schema = load_package_schema(&ctx.run.contract, node, &params.schema)?;
        let findings = validate_json_schema_findings(&schema, &value, "$");
        if findings.is_empty() {
            Ok(StepOutcome::success(Some(
                json!({ "status": "pass", "source": source }),
            )))
        } else {
            Ok(StepOutcome::check_failed(findings))
        }
    }
}

fn check_schema_params(node: &NodeDef) -> Result<CheckSchemaParams, StepError> {
    let params: CheckSchemaParams = engine::step_params(node, "check.schema")?;
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
    let schema: Value = parse_json(&bytes).map_err(|error| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{json_test_helpers::Harness, test_helpers::package_node};

    #[tokio::test]
    async fn check_schema_guards_number_tokens_before_deserialization() {
        let mut harness = Harness::new("check-schema-json");
        let node = package_node(
            "check-schema",
            "check.schema",
            "source = \"value.json\"\nschema = \"schema.json\"",
        );
        std::fs::write(
            harness.run.contract.root.join("schema.json"),
            r#"{"type":"object"}"#,
        )
        .unwrap();
        std::fs::write(
            harness.run.workspace.join("value.json"),
            r#"{"id":18446744073709551616}"#,
        )
        .unwrap();
        let error = CheckSchemaStep
            .execute(&mut harness.context(), &node)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("invalid JSON:") && error.contains("[-2^63, 2^64-1]"),
            "{error}"
        );
        std::fs::write(harness.run.workspace.join("value.json"), r#"{"id":1}"#).unwrap();
        std::fs::write(
            harness.run.contract.root.join("schema.json"),
            r#"{"type":"object","properties":{"id":{"const":18446744073709551616}}}"#,
        )
        .unwrap();
        let error = CheckSchemaStep
            .execute(&mut harness.context(), &node)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("is not valid JSON:") && error.contains("[-2^63, 2^64-1]"),
            "{error}"
        );
        std::fs::write(
            harness.run.contract.root.join("schema.json"),
            r#"{"type":"object","properties":{"id":{"const":1}},"required":["id"]}"#,
        )
        .unwrap();
        std::fs::write(harness.run.workspace.join("value.json"), r#"{"id":1}"#).unwrap();
        let StepOutcome::Success { output, .. } = CheckSchemaStep
            .execute(&mut harness.context(), &node)
            .await
            .unwrap()
        else {
            panic!("in-range match should pass");
        };
        assert_eq!(output.as_ref().unwrap()["status"], "pass");
        std::fs::write(harness.run.workspace.join("value.json"), r#"{"id":2}"#).unwrap();
        let StepOutcome::CheckFailed { .. } = CheckSchemaStep
            .execute(&mut harness.context(), &node)
            .await
            .unwrap()
        else {
            panic!("in-range mismatch should check-fail");
        };
    }
}
