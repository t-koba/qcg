use qcg_engine::StepError;
use sha2::{Digest, Sha256};
use std::io::Read as _;

pub(crate) async fn bounded_transform_text(
    fs: &qcg_engine::FsGateway,
    path: &camino::Utf8Path,
    limit: Option<usize>,
) -> Result<String, String> {
    let file = fs
        .open_read_resolved(path)
        .map_err(|error| error.to_string())?;
    let bytes = read_opened_bounded(file, limit, "transform input")?;
    String::from_utf8(bytes).map_err(|error| format!("transform input is not valid UTF-8: {error}"))
}

pub(crate) fn ensure_bounded_file_tree(
    fs: &qcg_engine::FsGateway,
    path: &camino::Utf8Path,
    limit: Option<usize>,
    count_limit: Option<usize>,
) -> Result<(), String> {
    fs.bounded_tree_stats(path, limit, count_limit)
        .map_err(|error| error.to_string())
}

/// Bounded read over an already-opened handle, used for workspace files
/// opened through the gateway's handle-relative walk (E13a).
pub(crate) fn read_opened_bounded(
    file: std::fs::File,
    limit: Option<usize>,
    resource: &str,
) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    let reader = file;
    let read_limit = limit.map_or(u64::MAX, |limit| limit.saturating_add(1) as u64);
    reader
        .take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if let Some(limit) = limit
        && bytes.len() > limit
    {
        return Err(format!("{resource} exceeds {limit} bytes"));
    }
    Ok(bytes)
}

pub(crate) fn hash_opened_bounded(
    file: std::fs::File,
    limit: Option<usize>,
) -> Result<String, String> {
    let mut hasher = Sha256::new();
    let mut total = 0_usize;
    let mut reader = std::io::BufReader::new(file);
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read =
            std::io::Read::read(&mut reader, &mut buffer).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read)
            .ok_or_else(|| "file input size overflowed".to_owned())?;
        if let Some(limit) = limit
            && total > limit
        {
            return Err(format!("file input exceeds {limit} bytes"));
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) async fn write_atomic(
    fs: &qcg_engine::FsGateway,
    path: &camino::Utf8Path,
    bytes: &[u8],
    unix_mode: Option<u32>,
) -> Result<(), std::io::Error> {
    // Handle-relative replace through the gateway: a parent swapped after
    // resolution cannot redirect the staged file or the rename (E13).
    fs.write_file_atomic_with_mode(path, bytes, unix_mode.map(|mode| mode & 0o777))
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))
}

pub(crate) async fn write_transform_output(
    fs: &qcg_engine::FsGateway,
    node_id: &str,
    path: &camino::Utf8Path,
    bytes: &[u8],
    limit: Option<usize>,
) -> Result<(), StepError> {
    if let Some(limit) = limit
        && bytes.len() > limit
    {
        return Err(StepError::failed(
            node_id,
            format!("transform output exceeds runtime.output_file_limit_bytes ({limit} bytes)"),
        ));
    }
    write_atomic(fs, path, bytes, None)
        .await
        .map_err(|error| StepError::failed(node_id, error.to_string()))
}
