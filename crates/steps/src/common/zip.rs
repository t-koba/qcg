use engine::{ResultExt, StepError};
use files::WalkDir;
use zip::write::SimpleFileOptions;

pub(crate) async fn write_zip_atomic(
    fs: &engine::FsGateway,
    metadata: &camino::Utf8Path,
    node_id: &str,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: Option<usize>,
    count_limit: Option<usize>,
) -> Result<(), StepError> {
    // Archive a private handle-relative snapshot of the source tree: a
    // parent swapped after resolution cannot redirect the collection, and
    // the snapshot enforces the input limits (E13). The `.part-`
    // prefix is the single unified temp prefix swept at startup.
    let snapshot = metadata.join(format!(".part-{}", uuid::Uuid::now_v7()));
    fs.snapshot_tree_to(source_path, &snapshot, input_limit, count_limit)
        .step_err(node_id)?;
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
    streamed.step_err(node_id)?;
    std::fs::remove_dir_all(&snapshot).step_err(node_id)?;
    Ok(())
}

/// Denies terminal symlinks, including swaps, via `open_read_nofollow` and
/// opened-handle type checks (E13o). Non-Unix pre/post checks are best-effort.
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
    let file = files::open_read_nofollow(path).step_err(node_id)?;
    if !file.metadata().is_ok_and(|metadata| metadata.is_file()) {
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
    let mut entries = Vec::new();
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
    if source_path.is_dir() {
        for entry in WalkDir::new(source_path).min_depth(1) {
            let entry = entry.step_err(node_id)?;
            if let Some(limit) = count_limit
                && entries.len() >= limit
            {
                return Err(StepError::failed(
                    node_id,
                    format!("zip source contains more than {limit} entries"),
                ));
            }
            let name = policy::portable_relative_path(
                entry.path().strip_prefix(source_path).step_err(node_id)?,
            );
            entries.push((name, entry));
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
    }
    policy::path::validate_path_tree_set(
        entries
            .iter()
            .filter(|(_, e)| e.path() != target_path)
            .map(|(p, e)| (p.as_str(), e.file_type().is_dir())),
    )
    .step_err(node_id)?;
    if source_path.is_file() {
        policy::path::validate_path_tree_set([(source_path.file_name().unwrap_or(""), false)])
            .step_err(node_id)?;
    }
    let mut writer = zip::ZipWriter::new(writer);
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
            .step_err(node_id)?;
        let mut source = open_zip_source_nofollow(node_id, source_path)?;
        copy_bounded(
            &mut source,
            &mut writer,
            &mut input_bytes,
            input_limit,
            node_id,
        )?;
    } else if source_path.is_dir() {
        for (entry_name, entry) in entries {
            let path = entry.path().to_path_buf();
            if path == target_path {
                continue;
            }
            let metadata = entry.metadata().step_err(node_id)?;
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
                    .step_err(node_id)?;
            } else if entry.file_type().is_file() {
                writer
                    .start_file(entry_name, zip_file_options(node_id, &metadata)?)
                    .step_err(node_id)?;
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
    writer.finish().step_err(node_id)?;
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
        options.unix_permissions(files::sanitize_mode_bits(metadata.permissions().mode()))
    };
    Ok(options)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    #[test]
    fn zip_path_sets_reject_before_emitting_headers() {
        let root = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("zip-path-sets-{}", uuid::Uuid::now_v7())),
        )
        .unwrap();
        for pair in [
            ["Report.md", "report.md"],
            ["Maße", "MASSE"],
            ["é", "e\u{301}"],
            ["A", "a/x"],
            ["A/x", "a/y"],
        ] {
            std::fs::create_dir_all(&root).unwrap();
            for name in pair {
                let path = root.join(name);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, b"x").unwrap();
            }
            let mut out = std::io::Cursor::new(Vec::new());
            assert!(build_zip("zip", &root, &root.join("skip"), None, Some(8), &mut out).is_err());
            assert!(
                out.into_inner().is_empty(),
                "no header or drop finalization"
            );
            std::fs::remove_dir_all(&root).unwrap();
        }
        std::fs::create_dir_all(root.join("shared")).unwrap();
        std::fs::write(root.join("shared/x"), b"x").unwrap();
        std::fs::write(root.join("shared/y"), b"y").unwrap();
        let mut out = std::io::Cursor::new(Vec::new());
        build_zip("zip", &root, &root.join("skip"), None, Some(8), &mut out).unwrap();
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(out.into_inner())).unwrap();
        assert_eq!(zip.by_name("shared/x").unwrap().size(), 1);
        assert_eq!(zip.by_name("shared/y").unwrap().size(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }
}
