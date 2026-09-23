use qcg_engine::StepError;
use qcg_fs::WalkDir;
use zip::write::SimpleFileOptions;

pub(crate) async fn write_zip_atomic(
    fs: &qcg_engine::FsGateway,
    metadata: &camino::Utf8Path,
    node_id: &str,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: Option<usize>,
    count_limit: Option<usize>,
) -> Result<(), StepError> {
    // Archive a private handle-relative snapshot of the source tree: a
    // parent swapped after resolution cannot redirect the collection, and
    // the snapshot enforces the input limits (E13). The `.qcg-part-`
    // prefix is the single unified temp prefix swept at startup.
    let snapshot = metadata.join(format!(".qcg-part-{}", uuid::Uuid::now_v7()));
    fs.snapshot_tree_to(source_path, &snapshot, input_limit, count_limit)
        .map_err(|error| StepError::failed(node_id, error.to_string()))?;
    // Preserve the original skip: a previous output inside the source tree
    // must not be archived (the snapshot may hold an earlier revision).
    let snapshot_target = target_path
        .strip_prefix(source_path)
        .ok()
        .map(|relative| snapshot.join(relative));
    let node_id_owned = node_id.to_owned();
    let snapshot_owned = snapshot.clone();
    let build_target = snapshot_target.unwrap_or_else(|| target_path.to_owned());
    // Stream the archive straight into the staged target file: the zip
    // bytes are never buffered whole in memory (E13).
    let streamed = fs
        .write_file_atomic_stream(target_path, None, move |file| {
            build_zip(
                &node_id_owned,
                &snapshot_owned,
                &build_target,
                input_limit,
                count_limit,
                file,
            )
            .map_err(|error| std::io::Error::other(error.to_string()))?;
            Ok(())
        })
        .await;
    // Temp cleanup failures propagate fail-closed (E13-9): a leftover
    // snapshot must surface instead of silently accumulating, and the
    // uniquely named directory cannot collide with a later run.
    streamed.map_err(|error| StepError::failed(node_id, error.to_string()))?;
    std::fs::remove_dir_all(&snapshot)
        .map_err(|error| StepError::failed(node_id, error.to_string()))?;
    Ok(())
}

/// Opens a zip source file without following a terminal symlink
/// (E13o). The pre-check denies a symlink fail-closed; the authoritative
/// open uses `O_NOFOLLOW` on Unix via `qcg_fs::open_read_nofollow` with an
/// `fstat` type check on the opened handle, so a symlink swapped in
/// between the pre-check and the open fails instead of being followed.
/// Non-Unix has no `O_NOFOLLOW` handle: the pre-plus-post check is
/// best-effort there and documented as such. A symlink is denied, never
/// silently skipped.
fn open_zip_source_nofollow(
    node_id: &str,
    path: &camino::Utf8Path,
) -> Result<std::fs::File, StepError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(StepError::failed(
                node_id,
                format!("zip source `{path}` is a symbolic link; refusing"),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(StepError::failed(
                node_id,
                format!("zip source `{path}` was not found"),
            ));
        }
        Err(error) => return Err(StepError::failed(node_id, error.to_string())),
    }
    let file = qcg_fs::open_read_nofollow(path)
        .map_err(|error| StepError::failed(node_id, error.to_string()))?;
    if file
        .metadata()
        .map(|metadata| !metadata.is_file())
        .unwrap_or(true)
    {
        return Err(StepError::failed(
            node_id,
            format!("zip source `{path}` is not a regular file"),
        ));
    }
    Ok(file)
}

pub(crate) fn build_zip<W: std::io::Write + std::io::Seek>(
    node_id: &str,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: Option<usize>,
    count_limit: Option<usize>,
    writer: W,
) -> Result<(), StepError> {
    let mut writer = zip::ZipWriter::new(writer);
    let mut input_bytes = 0_usize;
    // Deny a terminal symlink at the source root itself (fail-closed).
    if std::fs::symlink_metadata(source_path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Err(StepError::failed(
            node_id,
            format!("zip source `{source_path}` is a symbolic link; refusing"),
        ));
    }
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
        let mut source = open_zip_source_nofollow(node_id, source_path)?;
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
            if let Some(limit) = count_limit
                && entries.len() >= limit
            {
                return Err(StepError::failed(
                    node_id,
                    format!("zip source contains more than {limit} entries"),
                ));
            }
            entries.push(entry);
        }
        entries.sort_by(|left, right| left.path().cmp(right.path()));
        for entry in entries {
            let path = entry.path().to_path_buf();
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
            // E13o: symlinks are denied fail-closed, never silently skipped.
            if entry.file_type().is_symlink() {
                return Err(StepError::failed(
                    node_id,
                    format!("zip source `{path}` is a symbolic link; refusing"),
                ));
            }
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
                let mut source = open_zip_source_nofollow(node_id, path.as_path())?;
                copy_bounded(
                    &mut source,
                    &mut writer,
                    &mut input_bytes,
                    input_limit,
                    node_id,
                )?;
            } else {
                return Err(StepError::failed(
                    node_id,
                    format!("zip source `{path}` is not a file or directory; refusing"),
                ));
            }
        }
    } else {
        return Err(StepError::failed(
            node_id,
            format!("source `{source_path}` is not a file or directory"),
        ));
    }
    writer
        .finish()
        .map_err(|error| StepError::failed(node_id, error.to_string()))?;
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
        if let Some(limit) = limit
            && *total > limit
        {
            return Err(StepError::failed(
                node_id,
                format!("transform input exceeds {limit} bytes"),
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
        // Never preserve world-writable bits into the archive: package
        // extraction strips them, so preserving them here would diverge
        // pack/unpack round-trip guarantees. Single shared helper (E15).
        options.unix_permissions(qcg_fs::sanitize_mode_bits(metadata.permissions().mode()))
    };
    Ok(options)
}
