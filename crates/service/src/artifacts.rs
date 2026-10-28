use crate::package::ArtifactZipLimits;
use crate::summaries::{run_meta_dir, run_workspace_dir};
use crate::types::ServiceError;
use api::{ApiError, RunEvent};
use camino::{Utf8Path, Utf8PathBuf};
use engine::resolve_artifact_path;
use model::OutputManifest;
use policy::is_safe_path_component;
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Cursor, Read as _, Write};
use zip::write::SimpleFileOptions;

pub fn read_artifacts_zip(run_dir: &Utf8Path) -> Result<Vec<u8>, ServiceError> {
    read_artifacts_zip_with_limits(run_dir, &ArtifactZipLimits::default())
}

pub fn read_artifacts_zip_with_limits(
    run_dir: &Utf8Path,
    limits: &ArtifactZipLimits,
) -> Result<Vec<u8>, ServiceError> {
    let mut cursor = Cursor::new(Vec::new());
    write_artifacts_zip_stream_with_limits(run_dir, &mut cursor, limits)?;
    Ok(cursor.into_inner())
}

/// Verified workspace files behind an output manifest: logical path,
/// filesystem path, byte size, and expected SHA-256.
#[derive(Debug, Clone)]
pub struct VerifiedArtifact {
    pub path: String,
    pub fs_path: Utf8PathBuf,
    pub bytes: u64,
    pub sha256: String,
}

pub(crate) fn collect_verified_with_limits(
    run_dir: &Utf8Path,
    limits: &ArtifactZipLimits,
) -> Result<Vec<VerifiedArtifact>, ServiceError> {
    let manifest: OutputManifest = match limits.max_bytes {
        Some(limit) => {
            let capped = usize::try_from(limit)
                .map_err(|_| ServiceError::Invalid("artifact manifest limit overflowed".into()))?;
            let raw =
                files::read_bounded(&run_meta_dir(run_dir).join("outputs.json"), Some(capped))?;
            serde_json::from_slice(&raw).map_err(ServiceError::Json)?
        }
        None => engine::read_output_manifest(&run_meta_dir(run_dir))?,
    };
    if let Some(limit) = limits.max_entries
        && manifest.artifacts.len() > limit
    {
        return Err(ServiceError::Invalid(format!(
            "artifact entries exceed limit: {} > {limit}",
            manifest.artifacts.len()
        )));
    }
    let mut verified = Vec::new();
    for artifact in &manifest.artifacts {
        let fs_path = resolve_artifact_path(&run_workspace_dir(run_dir), &artifact.path)?;
        let bytes = std::fs::metadata(&fs_path)?.len();
        if bytes != artifact.bytes {
            return Err(ServiceError::Invalid(format!(
                "artifact `{}` bytes mismatch: manifest={}, actual={bytes}",
                artifact.path, artifact.bytes
            )));
        }
        verified.push(VerifiedArtifact {
            path: artifact.path.clone(),
            fs_path,
            bytes,
            sha256: artifact.sha256.clone(),
        });
    }
    Ok(verified)
}

pub(crate) fn check_verified_artifact_hashes(
    verified: &[VerifiedArtifact],
) -> Result<(), ServiceError> {
    for artifact in verified {
        let (actual_sha256, _) =
            files::hash_file_sha256(&artifact.fs_path, Some(artifact.bytes.saturating_add(1)))?;
        if actual_sha256 != artifact.sha256 {
            return Err(ServiceError::Invalid(format!(
                "artifact `{}` sha256 mismatch: manifest={}, actual={actual_sha256}",
                artifact.path, artifact.sha256
            )));
        }
    }
    Ok(())
}

pub fn write_artifacts_zip_stream_with_limits<W: Write>(
    run_dir: &Utf8Path,
    writer: W,
    limits: &ArtifactZipLimits,
) -> Result<(), ServiceError> {
    let verified = collect_verified_with_limits(run_dir, limits)?;
    let total_bytes = verified
        .iter()
        .try_fold(0_u64, |t, a| t.checked_add(a.bytes))
        .ok_or_else(|| ServiceError::Invalid("artifact zip size overflowed".into()))?;
    if let Some(limit) = limits.max_bytes
        && total_bytes > limit
    {
        return Err(ServiceError::Invalid(format!(
            "artifact zip source bytes exceed limit: {total_bytes} > {limit}"
        )));
    }
    let workspace = run_workspace_dir(run_dir);
    let mut directories = BTreeSet::new();
    for artifact in &verified {
        let mut parent = Utf8Path::new(&artifact.path).parent();
        while let Some(directory) = parent.filter(|directory| !directory.as_str().is_empty()) {
            directories.insert(directory.to_path_buf());
            if limits
                .max_entries
                .is_some_and(|limit| directories.len().saturating_add(verified.len()) > limit)
            {
                return Err(ServiceError::Invalid(
                    "artifact zip entries exceed limit".into(),
                ));
            }
            parent = directory.parent();
        }
    }
    check_verified_artifact_hashes(&verified)?;
    let mut zip = zip::ZipWriter::new_stream(CountingWriter::new(writer, limits.max_bytes));
    for directory in directories {
        let metadata = std::fs::metadata(workspace.join(&directory))?;
        zip.add_directory(
            format!("{directory}/"),
            artifact_zip_options(&metadata, true)?,
        )
        .map_err(artifact_zip_error)?;
    }
    for artifact in verified {
        let metadata = std::fs::metadata(&artifact.fs_path)?;
        let mut file = File::open(&artifact.fs_path)?.take(artifact.bytes.saturating_add(1));
        zip.start_file(&artifact.path, artifact_zip_options(&metadata, false)?)
            .map_err(artifact_zip_error)?;
        copy_verified_artifact(&mut file, &mut zip, &artifact)?;
    }
    finish_artifact_archive(zip, "artifact zip")
}

fn finish_artifact_archive<W: Write>(
    zip: zip::ZipWriter<zip::write::StreamWriter<CountingWriter<W>>>,
    label: &str,
) -> Result<(), ServiceError> {
    let writer = zip.finish().map_err(artifact_zip_error)?.into_inner();
    if writer.exceeded {
        let detail = match writer.limit {
            Some(limit) => format!("{label} output exceeds {limit} bytes"),
            None => format!("{label} output exceeded its bound"),
        };
        return Err(ServiceError::Invalid(detail));
    }
    Ok(())
}

fn artifact_zip_error(error: zip::result::ZipError) -> ServiceError {
    match error {
        zip::result::ZipError::Io(error) => ServiceError::Invalid(error.to_string()),
        error => ServiceError::Zip(error),
    }
}

/// Recheck prepared admission against caller limits, then stream pinned lengths.
pub fn write_run_bundle_stream_with_limits<W: Write>(
    parts: &crate::types::RunBundleParts,
    writer: W,
    limits: &ArtifactZipLimits,
) -> Result<(), ServiceError> {
    check_bundle_limits(parts.source_bytes, parts.entry_count, limits)?;
    let mut zip = zip::ZipWriter::new_stream(CountingWriter::new(writer, limits.max_bytes));
    let mut metas = vec![
        ("snapshot.json", &parts.snapshot_bytes),
        ("inputs.json", &parts.inputs_bytes),
    ];
    if let Some(bytes) = &parts.outputs_bytes {
        metas.push(("outputs.json", bytes));
    }
    for (name, bytes) in metas {
        zip.start_file(name, bundle_entry_options(false))
            .map_err(artifact_zip_error)?;
        zip.write_all(bytes)?;
    }
    for (name, path, len) in std::iter::once(("journal", parts.journal.clone(), parts.journal_len))
        .chain(
            parts
                .audit_len
                .map(|len| ("audit", parts.journal.with_file_name("audit.jsonl"), len)),
        )
    {
        zip.start_file(format!("{name}.jsonl"), bundle_entry_options(false))
            .map_err(artifact_zip_error)?;
        let mut src = File::open(path)?.take(len);
        if std::io::copy(&mut src, &mut zip)? != len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("{name} shorter than prepared length"),
            )
            .into());
        }
    }
    for dir in &parts.directories {
        zip.add_directory(format!("{dir}/"), bundle_entry_options(true))
            .map_err(artifact_zip_error)?;
    }
    for artifact in &parts.verified {
        let file = File::open(&artifact.fs_path)?;
        let mut src = file.take(artifact.bytes);
        zip.start_file(
            format!("artifacts/{}", artifact.path),
            bundle_entry_options(false),
        )
        .map_err(artifact_zip_error)?;
        copy_verified_artifact(&mut src, &mut zip, artifact)?;
    }
    finish_artifact_archive(zip, "run bundle")
}

// The abort flag also blocks ZipWriter's implicit finalization on drop.
fn copy_verified_artifact<W: Write>(
    mut src: impl std::io::Read,
    zip: &mut zip::ZipWriter<zip::write::StreamWriter<CountingWriter<W>>>,
    artifact: &VerifiedArtifact,
) -> Result<(), ServiceError> {
    let result = (|| {
        let mut hash = Sha256::new();
        let mut copied = 0_u64;
        let mut buffer = [0; 8192];
        loop {
            let count = match src.read(&mut buffer) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if count == 0 {
                break;
            }
            zip.write_all(&buffer[..count])?;
            hash.update(&buffer[..count]);
            copied += count as u64;
        }
        if copied < artifact.bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "artifact shorter than prepared length",
            )
            .into());
        }
        if copied != artifact.bytes || hex::encode(hash.finalize()) != artifact.sha256 {
            return Err(ServiceError::Invalid(
                "streamed artifact bytes mismatch or sha256 mismatch".into(),
            ));
        }
        Ok(())
    })();
    if result.is_err()
        && let Some(writer) = zip.get_ref()
    {
        writer.get_ref().aborted.set(true);
        tracing::warn!("streamed artifact verification/copy failed; archive aborted");
    }
    result
}

pub(crate) fn json_size<T: serde::Serialize>(value: &T) -> Result<u64, ServiceError> {
    let mut out = CountingWriter::new(std::io::sink(), None);
    serde_json::to_writer_pretty(&mut out, value)?;
    Ok(out.written)
}

pub(crate) fn check_bundle_limits(
    bytes: u64,
    entries: usize,
    limits: &ArtifactZipLimits,
) -> Result<(), ServiceError> {
    if let Some(limit) = limits.max_bytes
        && bytes > limit
    {
        return Err(ServiceError::TooLarge {
            actual: usize::try_from(bytes)
                .map_err(|_| ServiceError::Invalid("bundle size overflow".into()))?,
            limit: usize::try_from(limit)
                .map_err(|_| ServiceError::Invalid("bundle limit overflow".into()))?,
        });
    }
    if limits.max_entries.is_some_and(|limit| entries > limit) {
        return Err(ServiceError::ArtifactEntries);
    }
    Ok(())
}

pub(crate) struct CountingWriter<W> {
    pub(crate) inner: W,
    pub(crate) written: u64,
    limit: Option<u64>,
    pub(crate) exceeded: bool,
    aborted: Cell<bool>,
}

impl<W> CountingWriter<W> {
    pub(crate) fn new(inner: W, limit: Option<u64>) -> Self {
        Self {
            inner,
            written: 0,
            limit,
            exceeded: false,
            aborted: Cell::new(false),
        }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.aborted.get() {
            return Err(std::io::Error::other("artifact archive aborted"));
        }
        let remaining = match self.limit {
            Some(limit) => usize::try_from(limit.saturating_sub(self.written))
                .map_err(|_| std::io::Error::other("artifact zip output size overflowed"))?
                .min(bytes.len()),
            None => bytes.len(),
        };
        self.inner.write_all(&bytes[..remaining])?;
        self.written = self
            .written
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("artifact zip output size overflowed"))?;
        self.exceeded |= remaining < bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Explicit zip modes for artifact entries. Unix file modes come from the
/// source file itself; synthesized bundle entries carry fixed modes. No
/// path relies on the zip default and no platform synthesizes a different
/// readonly fallback: every entry declares its mode explicitly (E15).
fn artifact_zip_options(
    metadata: &std::fs::Metadata,
    directory: bool,
) -> Result<SimpleFileOptions, ServiceError> {
    // Share the explicit platform/compression defaults, then apply source metadata.
    let mut options = bundle_entry_options(directory);
    let modified = metadata.modified()?;
    let modified = chrono::DateTime::<chrono::Utc>::from(modified).naive_utc();
    let modified = zip::DateTime::try_from(modified).map_err(|error| {
        ServiceError::Invalid(format!(
            "artifact modification time is not representable in ZIP: {error}"
        ))
    })?;
    options = options.last_modified_time(modified);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = metadata.permissions().mode() & 0o777;
        if mode == 0 {
            return Err(ServiceError::Invalid(
                "artifact file mode is zero; refusing to archive a modeless entry".into(),
            ));
        }
        options = options.unix_permissions(mode);
    }
    #[cfg(not(unix))]
    {
        // No silent default: non-Unix metadata carries no Unix mode, so
        // declare the entry mode from the file type and readonly bit
        // instead of letting the zip writer synthesize one (E15).
        let mode = if directory {
            0o755
        } else if metadata.permissions().readonly() {
            0o444
        } else {
            0o644
        };
        options = options.unix_permissions(mode);
    }
    Ok(options)
}

/// Fixed zip options for synthesized bundle entries (no source file metadata).
/// The mode is always explicit (0o755 for directories, 0o644 for files) on
/// every platform: callers must not rely on zip defaults or platform
/// readonly synthesis (E15).
fn bundle_entry_options(directory: bool) -> SimpleFileOptions {
    let compression = if directory {
        zip::CompressionMethod::Stored
    } else {
        zip::CompressionMethod::Deflated
    };
    SimpleFileOptions::default()
        .system(zip::System::Unix)
        .compression_method(compression)
        .unix_permissions(if directory { 0o755 } else { 0o644 })
}

pub(crate) fn api_bad_request(message: impl Into<String>) -> ApiError {
    ApiError::invalid(message)
}

pub(crate) fn api_not_found(message: impl Into<String>) -> ApiError {
    ApiError::not_found(message)
}

pub(crate) fn api_internal(error: impl std::fmt::Display) -> ApiError {
    ApiError::internal(error.to_string())
}

pub(crate) fn is_safe_run_id(id: &str) -> bool {
    is_safe_path_component(id)
}

pub(crate) fn event_kinds(events: &[RunEvent]) -> Vec<String> {
    events.iter().map(|event| event.kind.clone()).collect()
}
