use qcg_engine::StepError;
use walkdir::WalkDir;
use zip::write::SimpleFileOptions;

use super::files::atomic_replace;

pub(crate) async fn write_zip_atomic(
    node_id: &str,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: Option<usize>,
    count_limit: Option<usize>,
) -> Result<(), StepError> {
    let file_name = target_path.file_name().unwrap_or("archive.zip");
    let temporary = target_path.with_file_name(format!(
        ".{file_name}.qcg-part-{}",
        uuid::Uuid::now_v7().as_simple()
    ));
    let node_id_owned = node_id.to_owned();
    let source_path_owned = source_path.to_owned();
    let temporary_for_worker = temporary.clone();
    let result = async {
        tokio::task::spawn_blocking(move || {
            write_zip(
                &node_id_owned,
                &source_path_owned,
                &temporary_for_worker,
                input_limit,
                count_limit,
            )
        })
        .await
        .map_err(|error| StepError::failed(node_id, format!("zip worker failed: {error}")))??;
        atomic_replace(&temporary, target_path).await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

pub(crate) fn write_zip(
    node_id: &str,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: Option<usize>,
    count_limit: Option<usize>,
) -> Result<(), StepError> {
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(target_path)?;
    let mut writer = zip::ZipWriter::new(file);
    let mut input_bytes = 0_usize;
    if source_path.is_file() {
        if count_limit == Some(0) {
            return Err(StepError::failed(
                node_id,
                "zip source entry count exceeds the configured limit of 0",
            ));
        }
        let name = source_path.file_name().ok_or_else(|| {
            StepError::failed(node_id, format!("source `{source_path}` has no file name"))
        })?;
        let metadata = std::fs::metadata(source_path)?;
        writer
            .start_file(name, zip_file_options(node_id, &metadata)?)
            .map_err(|error| StepError::failed(node_id, error.to_string()))?;
        let mut source = std::fs::File::open(source_path)?;
        copy_bounded(
            &mut source,
            &mut writer,
            &mut input_bytes,
            input_limit,
            node_id,
        )?;
    } else if source_path.is_dir() {
        let mut entries = Vec::new();
        for entry in WalkDir::new(source_path).min_depth(1) {
            let entry = entry.map_err(|error| StepError::failed(node_id, error.to_string()))?;
            if count_limit.is_some_and(|limit| entries.len() >= limit) {
                return Err(StepError::failed(
                    node_id,
                    format!(
                        "zip source contains more than {} entries",
                        count_limit.unwrap_or(usize::MAX)
                    ),
                ));
            }
            entries.push(entry);
        }
        entries.sort_by(|left, right| left.path().cmp(right.path()));
        for entry in entries {
            let path = camino::Utf8PathBuf::from_path_buf(entry.path().to_path_buf())
                .map_err(|_| StepError::failed(node_id, "zip source path must be UTF-8"))?;
            if path == target_path {
                continue;
            }
            let rel = path
                .strip_prefix(source_path)
                .map_err(|error| StepError::failed(node_id, error.to_string()))?;
            let entry_name = qcg_policy::portable_relative_path(rel);
            let metadata = entry
                .metadata()
                .map_err(|error| StepError::failed(node_id, error.to_string()))?;
            if entry.file_type().is_dir() {
                writer
                    .add_directory(
                        format!("{entry_name}/"),
                        zip_directory_options(node_id, &metadata)?,
                    )
                    .map_err(|error| StepError::failed(node_id, error.to_string()))?;
            } else if entry.file_type().is_file() {
                writer
                    .start_file(entry_name, zip_file_options(node_id, &metadata)?)
                    .map_err(|error| StepError::failed(node_id, error.to_string()))?;
                let mut source = std::fs::File::open(&path)?;
                copy_bounded(
                    &mut source,
                    &mut writer,
                    &mut input_bytes,
                    input_limit,
                    node_id,
                )?;
            }
        }
    } else {
        return Err(StepError::failed(
            node_id,
            format!("source `{source_path}` is not a file or directory"),
        ));
    }
    let file = writer
        .finish()
        .map_err(|error| StepError::failed(node_id, error.to_string()))?;
    file.sync_all()?;
    Ok(())
}

pub(crate) fn copy_bounded<R, W>(
    reader: &mut R,
    writer: &mut W,
    total: &mut usize,
    limit: Option<usize>,
    node_id: &str,
) -> Result<(), StepError>
where
    R: std::io::Read,
    W: std::io::Write,
{
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        *total = total
            .checked_add(read)
            .ok_or_else(|| StepError::failed(node_id, "transform input byte count overflowed"))?;
        if limit.is_some_and(|limit| *total > limit) {
            return Err(StepError::failed(
                node_id,
                format!(
                    "transform input exceeds {} bytes",
                    limit.unwrap_or(usize::MAX)
                ),
            ));
        }
        writer.write_all(&buffer[..read])?;
    }
}

pub(crate) fn zip_file_options(
    node_id: &str,
    metadata: &std::fs::Metadata,
) -> Result<SimpleFileOptions, StepError> {
    Ok(zip_entry_options(node_id, metadata)?.compression_method(zip::CompressionMethod::Deflated))
}

pub(crate) fn zip_directory_options(
    node_id: &str,
    metadata: &std::fs::Metadata,
) -> Result<SimpleFileOptions, StepError> {
    Ok(zip_entry_options(node_id, metadata)?.compression_method(zip::CompressionMethod::Stored))
}

pub(crate) fn zip_entry_options(
    node_id: &str,
    metadata: &std::fs::Metadata,
) -> Result<SimpleFileOptions, StepError> {
    let modified = metadata.modified().map_err(|error| {
        StepError::failed(
            node_id,
            format!("failed to read source modification time: {error}"),
        )
    })?;
    let modified = chrono::DateTime::<chrono::Utc>::from(modified).naive_utc();
    let modified = zip::DateTime::try_from(modified).map_err(|error| {
        StepError::failed(
            node_id,
            format!("source modification time is not representable in ZIP: {error}"),
        )
    })?;
    let options = SimpleFileOptions::default().last_modified_time(modified);
    #[cfg(unix)]
    let options = {
        use std::os::unix::fs::PermissionsExt as _;
        options.unix_permissions(metadata.permissions().mode())
    };
    Ok(options)
}
