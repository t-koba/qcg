use crate::common::{bounded_transform_text, require};
use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome, StepTraits};
use qcg_policy::{params_schema, string_schema};
use qcg_types::{Finding, Severity};
use serde::Deserialize;
use serde_json::{Value, json};

pub(crate) struct CheckFormatStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckFormatParams {
    source: String,
    content: String,
}

#[async_trait]
impl StepExecutor for CheckFormatStep {
    fn type_id(&self) -> &'static str {
        "check.format"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "content"],
            json!({
                "source": string_schema(),
                "content": { "type": "string", "enum": ["json", "toml"] },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let _params = check_format_params(node)?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = check_format_params(node)?;
        let source = ctx.render_inline(node, &params.source)?;
        let source_path = ctx.run.fs.resolve_read(&source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("source path is not in workspace: {error}"),
            )
        })?;
        let text = bounded_transform_text(
            &source_path,
            ctx.run.contract.manifest.runtime.file_input_limit_bytes,
        )
        .await
        .map_err(|error| StepError::failed(&node.id, error))?;
        let format = params.content.as_str();
        let result = match format {
            "json" => serde_json::from_str::<Value>(&text)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            "toml" => toml::from_str::<toml::Value>(&text)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            _ => unreachable!("validated format"),
        };
        match result {
            Ok(()) => Ok(StepOutcome::Success {
                output: Some(json!({ "status": "pass", "source": source, "format": format })),
                files: vec![],
            }),
            Err(error) => Ok(StepOutcome::CheckFailed {
                findings: vec![Finding {
                    severity: Severity::Error,
                    message: format!("invalid {format}: {error}"),
                    location: Some(source),
                    raw_output: None,
                }],
                output: None,
                files: vec![],
            }),
        }
    }
}

fn check_format_params(node: &NodeDef) -> Result<CheckFormatParams, StepError> {
    let params: CheckFormatParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid check.format params: {error}"))
    })?;
    require(node, Some(&params.source), "source")?;
    require(node, Some(&params.content), "content")?;
    if !matches!(params.content.as_str(), "json" | "toml") {
        return Err(StepError::failed(
            &node.id,
            "content must be one of: json, toml",
        ));
    }
    Ok(params)
}
