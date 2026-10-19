use camino::Utf8PathBuf;
use contract::NodeDef;
use engine::{ResultExt, StepContext, StepError};

pub(crate) async fn write_output_file(
    ctx: &mut StepContext<'_>,
    node: &NodeDef,
    output_file: Option<&str>,
    content: &[u8],
) -> Result<Vec<Utf8PathBuf>, StepError> {
    let Some(output_file) = output_file else {
        return Ok(Vec::new());
    };
    let output_file = ctx.render_inline(node, output_file)?;
    let path = ctx.run.fs.resolve_write(&output_file).step_err(&node.id)?;
    ctx.run
        .fs
        .write_file_atomic(&path, content)
        .await
        .map_err(|error| StepError::from_gateway(&node.id, error))?;
    Ok(vec![path])
}
