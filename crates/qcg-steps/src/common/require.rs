use qcg_contract::{Contract, NodeDef};
use qcg_engine::{StepContext, StepError};

pub(crate) fn require(node: &NodeDef, value: Option<&str>, field: &str) -> Result<(), StepError> {
    if value.unwrap_or_default().is_empty() {
        Err(StepError::failed(&node.id, format!("{field} is required")))
    } else {
        Ok(())
    }
}

pub(crate) fn package_file(
    contract: &Contract,
    node: &NodeDef,
    relative: &str,
    role: &str,
) -> Result<camino::Utf8PathBuf, StepError> {
    let path = contract.resolve_package_path(relative).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("{role} package path `{relative}` is invalid: {error}"),
        )
    })?;
    if !path.is_file() {
        return Err(StepError::failed(
            &node.id,
            format!("{role} package path `{relative}` must be a file"),
        ));
    }
    Ok(path)
}

pub(crate) fn render_command(
    ctx: &StepContext<'_>,
    node: &NodeDef,
    command: &[String],
    limit: Option<usize>,
) -> Result<Vec<String>, StepError> {
    let mut total = 0_usize;
    let mut rendered = Vec::with_capacity(command.len());
    for arg in command {
        let arg = ctx.render_inline(node, arg)?;
        if arg.contains('\0') {
            return Err(StepError::failed(
                &node.id,
                "rendered command arguments must not contain NUL bytes",
            ));
        }
        total = total
            .checked_add(arg.len().saturating_add(1))
            .ok_or_else(|| StepError::failed(&node.id, "rendered command size overflowed"))?;
        if limit.is_some_and(|limit| total > limit) {
            return Err(StepError::failed(
                &node.id,
                format!(
                    "rendered command arguments exceed {} bytes",
                    limit.unwrap_or(usize::MAX)
                ),
            ));
        }
        rendered.push(arg);
    }
    Ok(rendered)
}
