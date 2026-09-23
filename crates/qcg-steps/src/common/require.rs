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
    // Containment is enforced by `resolve_package_path`; the `is_file`
    // pathname check below is only a fast path. The authoritative file
    // check happens on the opened handle in `open_package_read`
    // (O_NOFOLLOW + handle `is_file`), so a symlink swap between this
    // probe and the open cannot redirect the read (E13).
    let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("{role} package path `{relative}` is not readable: {error}"),
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(StepError::failed(
            &node.id,
            format!("{role} package path `{relative}` must not be a symlink"),
        ));
    }
    if !metadata.is_file() {
        return Err(StepError::failed(
            &node.id,
            format!("{role} package path `{relative}` must be a file"),
        ));
    }
    Ok(path)
}

/// Opens a package file through the same O_NOFOLLOW + handle boundary as
/// snapshot reads (E13). The leaf is opened without following a terminal
/// symlink via the shared `qcg_fs` helper (Unix O_NOFOLLOW authoritative,
/// non-Unix symlink pre-check plus handle verification); the returned
/// handle is verified to be a file, so a pathname `is_file` probe alone
/// never authorizes the read.
pub(crate) fn open_package_read(
    node: &NodeDef,
    path: &camino::Utf8Path,
    role: &str,
) -> Result<std::fs::File, StepError> {
    qcg_fs::open_read_nofollow(path).map_err(|error| {
        StepError::failed(
            &node.id,
            format!("{role} package path is not readable: {error}"),
        )
    })
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
        if let Some(limit) = limit
            && total > limit
        {
            return Err(StepError::failed(
                &node.id,
                format!("rendered command arguments exceed {limit} bytes"),
            ));
        }
        rendered.push(arg);
    }
    Ok(rendered)
}
