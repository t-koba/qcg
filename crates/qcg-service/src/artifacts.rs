use crate::package::ArtifactZipLimits;
use crate::summaries::{run_meta_dir, run_workspace_dir};
use crate::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::RunEvent;
use qcg_api::{ApiError, RunSnapshot};
use qcg_engine::{
    JournalLimits, append_serialized_json_line, read_journal_values, read_output_manifest,
    resolve_artifact_path, serialize_bounded,
};
use qcg_policy::is_safe_relative_path;
use qcg_types::OutputManifest;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Cursor, Write};
use zip::write::SimpleFileOptions;

pub fn append_journal_event(run_dir: &Utf8Path, event: &Value) -> Result<(), ServiceError> {
    let meta_dir = run_meta_dir(run_dir);
    std::fs::create_dir_all(&meta_dir)?;
    let limits = JournalLimits::default();
    let journal_path = meta_dir.join("journal.jsonl");
    let scan = read_journal_values(&journal_path, limits)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal_path)?;
    let bytes = serialize_bounded(event, limits.max_event_bytes, "event")
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    let mut stats = scan.stats;
    append_serialized_json_line(&mut file, bytes, &mut stats, limits)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    file.sync_data()?;
    Ok(())
}

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

pub fn write_artifacts_zip_stream<W: Write>(
    run_dir: &Utf8Path,
    writer: W,
) -> Result<(), ServiceError> {
    write_artifacts_zip_stream_with_limits(run_dir, writer, &ArtifactZipLimits::default())
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

pub(crate) fn collect_verified_artifacts(
    run_dir: &Utf8Path,
) -> Result<Vec<VerifiedArtifact>, ServiceError> {
    let manifest: OutputManifest = read_output_manifest(&run_meta_dir(run_dir))?;
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
        let (actual_sha256, _) = qcg_policy::hash_file_sha256(&artifact.fs_path, None)?;
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
    write_artifacts_zip_stream_bounded(
        run_dir,
        CountingWriter::new(writer, limits.max_bytes),
        limits,
    )
}

fn write_artifacts_zip_stream_bounded<W: Write>(
    run_dir: &Utf8Path,
    writer: CountingWriter<W>,
    limits: &ArtifactZipLimits,
) -> Result<(), ServiceError> {
    let verified = collect_verified_artifacts(run_dir)?;
    let total_bytes = verified
        .iter()
        .try_fold(0_u64, |total, artifact| total.checked_add(artifact.bytes))
        .ok_or_else(|| ServiceError::Invalid("artifact zip size overflowed".into()))?;
    if limits.max_bytes.is_some_and(|limit| total_bytes > limit) {
        return Err(ServiceError::Invalid(format!(
            "artifact zip source bytes exceed limit: {total_bytes} > {}",
            limits.max_bytes.unwrap_or(u64::MAX)
        )));
    }
    check_verified_artifact_hashes(&verified)?;
    let workspace = run_workspace_dir(run_dir);
    let mut directories = BTreeSet::new();
    for artifact in &verified {
        let mut parent = Utf8Path::new(&artifact.path).parent();
        while let Some(directory) = parent.filter(|directory| !directory.as_str().is_empty()) {
            directories.insert(directory.to_path_buf());
            parent = directory.parent();
        }
    }
    let entry_count = directories
        .len()
        .checked_add(verified.len())
        .ok_or_else(|| ServiceError::Invalid("artifact zip entry count overflowed".into()))?;
    if limits.max_entries.is_some_and(|limit| entry_count > limit) {
        return Err(ServiceError::Invalid(format!(
            "artifact zip entries exceed limit: {entry_count} > {}",
            limits.max_entries.unwrap_or(usize::MAX)
        )));
    }
    let mut zip = zip::ZipWriter::new_stream(writer);
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
        let mut file = File::open(&artifact.fs_path)?;
        zip.start_file(artifact.path, artifact_zip_options(&metadata, false)?)
            .map_err(artifact_zip_error)?;
        std::io::copy(&mut file, &mut zip)?;
    }
    let writer = zip.finish().map_err(artifact_zip_error)?.into_inner();
    if writer.exceeded {
        return Err(ServiceError::Invalid(format!(
            "artifact zip output exceeds {} bytes",
            writer.limit.unwrap_or(u64::MAX)
        )));
    }
    Ok(())
}

fn artifact_zip_error(error: zip::result::ZipError) -> ServiceError {
    match error {
        zip::result::ZipError::Io(error) => ServiceError::Invalid(error.to_string()),
        error => ServiceError::Zip(error),
    }
}

/// Write a self-contained run bundle: snapshot, inputs, journal, outputs,
/// and verified artifacts. Meta files stream through the same output cap as
/// artifacts; entry counts cover every zip entry.
pub fn write_run_bundle_stream_with_limits<W: Write>(
    snapshot: &RunSnapshot,
    inputs: &BTreeMap<String, Value>,
    journal: &Utf8Path,
    outputs: Option<&OutputManifest>,
    verified: &[VerifiedArtifact],
    writer: W,
    limits: &ArtifactZipLimits,
) -> Result<(), ServiceError> {
    let mut meta = vec![
        (
            "snapshot.json".to_string(),
            serde_json::to_vec_pretty(snapshot).map_err(ServiceError::Json)?,
        ),
        (
            "inputs.json".to_string(),
            serde_json::to_vec_pretty(inputs).map_err(ServiceError::Json)?,
        ),
    ];
    if let Some(outputs) = outputs {
        meta.push((
            "outputs.json".to_string(),
            serde_json::to_vec_pretty(outputs).map_err(ServiceError::Json)?,
        ));
    }
    let mut directories = BTreeSet::from([Utf8PathBuf::from("artifacts")]);
    for artifact in verified {
        let mut parent = Utf8Path::new(&artifact.path).parent();
        while let Some(directory) = parent.filter(|directory| !directory.as_str().is_empty()) {
            directories.insert(Utf8PathBuf::from("artifacts").join(directory));
            parent = directory.parent();
        }
    }
    let entry_count = meta
        .len()
        .checked_add(1)
        .and_then(|count| count.checked_add(directories.len()))
        .and_then(|count| count.checked_add(verified.len()))
        .ok_or_else(|| ServiceError::Invalid("run bundle entry count overflowed".into()))?;
    if limits.max_entries.is_some_and(|limit| entry_count > limit) {
        return Err(ServiceError::Invalid(format!(
            "run bundle entries exceed limit: {entry_count} > {}",
            limits.max_entries.unwrap_or(usize::MAX)
        )));
    }
    let mut zip = zip::ZipWriter::new_stream(CountingWriter::new(writer, limits.max_bytes));
    for (name, bytes) in &meta {
        zip.start_file(name, bundle_entry_options(false))
            .map_err(artifact_zip_error)?;
        zip.write_all(bytes)?;
    }
    {
        let mut journal_file = File::open(journal)?;
        zip.start_file("journal.jsonl", bundle_entry_options(false))
            .map_err(artifact_zip_error)?;
        std::io::copy(&mut journal_file, &mut zip)?;
    }
    for directory in directories {
        zip.add_directory(format!("{directory}/"), bundle_entry_options(true))
            .map_err(artifact_zip_error)?;
    }
    for artifact in verified {
        let mut file = File::open(&artifact.fs_path)?;
        zip.start_file(
            format!("artifacts/{}", artifact.path),
            bundle_entry_options(false),
        )
        .map_err(artifact_zip_error)?;
        std::io::copy(&mut file, &mut zip)?;
    }
    let writer = zip.finish().map_err(artifact_zip_error)?.into_inner();
    if writer.exceeded {
        return Err(ServiceError::Invalid(format!(
            "run bundle output exceeds {} bytes",
            writer.limit.unwrap_or(u64::MAX)
        )));
    }
    Ok(())
}

struct CountingWriter<W> {
    inner: W,
    written: u64,
    limit: Option<u64>,
    exceeded: bool,
}

impl<W> CountingWriter<W> {
    fn new(inner: W, limit: Option<u64>) -> Self {
        Self {
            inner,
            written: 0,
            limit,
            exceeded: false,
        }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(limit) = self.limit else {
            self.inner.write_all(bytes)?;
            self.written =
                self.written
                    .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                        std::io::Error::other("artifact zip output size overflowed")
                    })?)
                    .ok_or_else(|| std::io::Error::other("artifact zip output size overflowed"))?;
            return Ok(bytes.len());
        };
        let remaining = usize::try_from(limit.saturating_sub(self.written))
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        if remaining > 0 {
            self.inner.write_all(&bytes[..remaining])?;
            self.written =
                self.written
                    .checked_add(u64::try_from(remaining).map_err(|_| {
                        std::io::Error::other("artifact zip output size overflowed")
                    })?)
                    .ok_or_else(|| std::io::Error::other("artifact zip output size overflowed"))?;
        }
        if remaining < bytes.len() {
            self.exceeded = true;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn artifact_zip_options(
    metadata: &std::fs::Metadata,
    directory: bool,
) -> Result<SimpleFileOptions, ServiceError> {
    let compression = if directory {
        zip::CompressionMethod::Stored
    } else {
        zip::CompressionMethod::Deflated
    };
    let mut options = SimpleFileOptions::default().compression_method(compression);
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
        options = options.unix_permissions(metadata.permissions().mode());
    }
    Ok(options)
}

/// Fixed zip options for synthesized bundle entries (no source file metadata).
fn bundle_entry_options(directory: bool) -> SimpleFileOptions {
    let compression = if directory {
        zip::CompressionMethod::Stored
    } else {
        zip::CompressionMethod::Deflated
    };
    let options = SimpleFileOptions::default().compression_method(compression);
    #[cfg(unix)]
    {
        options.unix_permissions(if directory { 0o755 } else { 0o644 })
    }
    #[cfg(not(unix))]
    {
        options
    }
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

pub(crate) fn is_safe_id(id: &str) -> bool {
    is_safe_relative_path(id)
}

pub(crate) fn event_kinds(events: &[RunEvent]) -> Vec<String> {
    events.iter().map(|event| event.kind.clone()).collect()
}
