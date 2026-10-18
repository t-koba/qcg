use crate::common::{bounded_transform_text, require};
use async_trait::async_trait;
use contract::{Contract, NodeDef};
use engine::{StepContext, StepError, StepExecutor, StepOutcome};
use model::Finding;
use policy::{params_schema, string_schema};
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
    engine::step_type_id!("check.format");

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "content"],
            json!({
                "source": string_schema(),
                "content": { "type": "string", "enum": ["json", "toml"] },
            }),
        ))
    }

    engine::parallel_step_traits!();

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
            &ctx.run.fs,
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
            _ => {
                // Guarded by `check_format_params` above; a future caller
                // bypassing validation must fail the step, never panic.
                return Err(StepError::failed(
                    &node.id,
                    format!("unsupported check.format content `{format}`"),
                ));
            }
        };
        match result {
            Ok(()) => Ok(StepOutcome::success(Some(
                json!({ "status": "pass", "source": source, "format": format }),
            ))),
            Err(error) => Ok(StepOutcome::check_failed(vec![Finding::error(
                format!("invalid {format}: {error}"),
                Some(source),
                None,
            )])),
        }
    }
}

fn check_format_params(node: &NodeDef) -> Result<CheckFormatParams, StepError> {
    let params: CheckFormatParams = engine::step_params(node, "check.format")?;
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
