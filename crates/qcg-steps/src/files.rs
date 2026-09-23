use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome, StepTraits};
use qcg_policy::{params_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::{
    package_file, parse_unix_mode, read_opened_bounded, require, validate_unix_mode_template,
    write_atomic,
};
pub(crate) struct RenderStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RenderParams {
    template: String,
    output_file: String,
}

#[async_trait]
impl StepExecutor for RenderStep {
    fn type_id(&self) -> &'static str {
        "render"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["template", "output_file"],
            json!({
                "template": string_schema(),
                "output_file": string_schema(),
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = render_params(node)?;
        package_file(contract, node, &params.template, "template")?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = render_params(node)?;
        let output_file = ctx.render_inline(node, &params.output_file)?;
        let template_path = package_file(&ctx.run.contract, node, &params.template, "template")?;
        let input_limit = ctx.run.contract.manifest.runtime.file_input_limit_bytes;
        // Package sources are service-trusted generator content, not
        // workspace input: only workspace paths go through the gateway.
        let template = std::fs::File::open(&template_path).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("template `{}` is not readable: {error}", params.template),
            )
        })?;
        let source = read_opened_bounded(template, input_limit, "template")
            .map_err(|error| StepError::failed(&node.id, error))?;
        let source = String::from_utf8(source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("template `{}` is not valid UTF-8: {error}", params.template),
            )
        })?;
        let rendered = ctx
            .run
            .templates
            .render_inline(
                &source,
                ctx.vars.to_json(),
                &ctx.run.contract.manifest.runtime,
            )
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let path = ctx
            .run
            .fs
            .resolve_write(&output_file)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        write_atomic(&ctx.run.fs, &path, rendered.as_bytes(), None).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": output_file })),
            files: vec![path],
        })
    }
}

fn render_params(node: &NodeDef) -> Result<RenderParams, StepError> {
    let params: RenderParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid render params: {error}")))?;
    require(node, Some(&params.template), "template")?;
    require(node, Some(&params.output_file), "output_file")?;
    Ok(params)
}

pub(crate) struct WriteStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteParams {
    output_file: String,
    content: String,
    #[serde(default)]
    unix_mode: Option<String>,
}

#[async_trait]
impl StepExecutor for WriteStep {
    fn type_id(&self) -> &'static str {
        "write"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["output_file", "content"],
            json!({
                "output_file": string_schema(),
                "content": string_schema(),
                "unix_mode": { "type": "string", "pattern": "^0[6-7][0-7]{2}$" },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn validate(&self, node: &NodeDef, _contract: &Contract) -> Result<(), StepError> {
        let params = write_params(node)?;
        validate_unix_mode_template(node, params.unix_mode.as_deref())?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = write_params(node)?;
        let output_file = ctx.render_inline(node, &params.output_file)?;
        let path = ctx
            .run
            .fs
            .resolve_write(&output_file)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let content = ctx.render_inline(node, &params.content)?;
        let unix_mode = params
            .unix_mode
            .as_deref()
            .map(|mode| ctx.render_inline(node, mode))
            .transpose()?;
        let unix_mode = parse_unix_mode(node, unix_mode.as_deref())?;
        write_atomic(&ctx.run.fs, &path, content.as_bytes(), unix_mode).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": output_file })),
            files: vec![path],
        })
    }
}

fn write_params(node: &NodeDef) -> Result<WriteParams, StepError> {
    let params: WriteParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid write params: {error}")))?;
    require(node, Some(&params.output_file), "output_file")?;
    require(node, Some(&params.content), "content")?;
    Ok(params)
}

pub(crate) struct CopyStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CopyParams {
    source: String,
    target: String,
}

#[async_trait]
impl StepExecutor for CopyStep {
    fn type_id(&self) -> &'static str {
        "copy"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "target"],
            json!({
                "source": string_schema(),
                "target": string_schema(),
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = copy_params(node)?;
        package_file(contract, node, &params.source, "copy source")?;
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = copy_params(node)?;
        let source = package_file(&ctx.run.contract, node, &params.source, "copy source")?;
        let target_name = ctx.render_inline(node, &params.target)?;
        let target = ctx
            .run
            .fs
            .resolve_write(&target_name)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let input_limit = ctx.run.contract.manifest.runtime.file_input_limit_bytes;
        // Package sources are service-trusted generator content; the
        // target below is written through the gateway (E13).
        let source_file = std::fs::File::open(&source)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let bytes = read_opened_bounded(source_file, input_limit, "copy source")
            .map_err(|error| StepError::failed(&node.id, error))?;
        let source_mode = source_unix_mode(&source)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        write_atomic(&ctx.run.fs, &target, &bytes, source_mode).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": target_name })),
            files: vec![target],
        })
    }
}

fn copy_params(node: &NodeDef) -> Result<CopyParams, StepError> {
    let params: CopyParams = node
        .deserialize_params()
        .map_err(|error| StepError::failed(&node.id, format!("invalid copy params: {error}")))?;
    require(node, Some(&params.source), "source")?;
    require(node, Some(&params.target), "target")?;
    Ok(params)
}

fn source_unix_mode(path: &camino::Utf8Path) -> Result<Option<u32>, std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
        Ok(Some(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}
