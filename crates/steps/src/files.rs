use async_trait::async_trait;
use contract::{Contract, NodeDef};
use engine::{ResultExt, StepContext, StepError, StepExecutor, StepOutcome};
use policy::{params_schema, string_schema};
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
    engine::step_type_id!("render");

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["template", "output_file"],
            json!({
                "template": string_schema(),
                "output_file": string_schema(),
            }),
        ))
    }

    engine::parallel_step_traits!();

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
        let rendered = ctx.render_inline(node, &source)?;
        let path = ctx.run.fs.resolve_write(&output_file).step_err(&node.id)?;
        write_atomic(&ctx.run.fs, &path, rendered.as_bytes(), None).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": output_file })),
            files: vec![path],
        })
    }
}

fn render_params(node: &NodeDef) -> Result<RenderParams, StepError> {
    let params: RenderParams = engine::step_params(node, "render")?;
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
    engine::step_type_id!("write");

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

    engine::parallel_step_traits!();

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
        let path = ctx.run.fs.resolve_write(&output_file).step_err(&node.id)?;
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
    let params: WriteParams = engine::step_params(node, "write")?;
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
    engine::step_type_id!("copy");

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["source", "target"],
            json!({
                "source": string_schema(),
                "target": string_schema(),
            }),
        ))
    }

    engine::parallel_step_traits!();

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
        let target = ctx.run.fs.resolve_write(&target_name).step_err(&node.id)?;
        let input_limit = ctx.run.contract.manifest.runtime.file_input_limit_bytes;
        // Package sources are service-trusted generator content; the
        // target below is written through the gateway (E13).
        let source_file = std::fs::File::open(&source).step_err(&node.id)?;
        let bytes = read_opened_bounded(source_file, input_limit, "copy source")
            .map_err(|error| StepError::failed(&node.id, error))?;
        let source_mode = source_unix_mode(&source).step_err(&node.id)?;
        write_atomic(&ctx.run.fs, &target, &bytes, source_mode).await?;
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": target_name })),
            files: vec![target],
        })
    }
}

fn copy_params(node: &NodeDef) -> Result<CopyParams, StepError> {
    let params: CopyParams = engine::step_params(node, "copy")?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{json_test_helpers::Harness, test_helpers::package_node};
    use contract::{ReadIdentity, ReadOwner};
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn file_template_body_and_destination_are_owned_reads() {
        let mut harness = Harness::new("file-read-trace");
        std::fs::write(
            harness.run.contract.root.join("source.j2"),
            "hello {{ inputs.name }}",
        )
        .unwrap();
        let node = package_node(
            "render",
            "render",
            r#"template = "source.j2"
output_file = "{{ inputs.file }}""#,
        );
        let mut ctx = harness.context();
        ctx.vars.set_inputs(BTreeMap::from([
            ("file".into(), json!("out.txt")),
            ("name".into(), json!("world")),
        ]));
        RenderStep.execute(&mut ctx, &node).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(ctx.run.workspace.join("out.txt")).unwrap(),
            "hello world"
        );
        let trace = ctx.vars.read_trace();
        let owner = ReadOwner::Node("render".into());
        assert!(trace.complete_for(&owner));
        for (field, value) in [("file", "out.txt"), ("name", "world")] {
            assert!(trace.reads.iter().any(|r| r.owner == owner
                && r.path == ["inputs", field]
                && r.identity == ReadIdentity::of(Some(&json!(value)))));
        }
    }
}
