use camino::Utf8Path;
use qcg_contract::RuntimeLimits;
use qcg_types::OutputManifest;
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read as _, Write as _};
use tempfile::NamedTempFile;

use super::collect::OutputLimits;

pub fn write_output_manifest(
    workspace: &Utf8Path,
    manifest: &OutputManifest,
) -> Result<(), std::io::Error> {
    write_output_manifest_with_limits(workspace, manifest, &RuntimeLimits::default())
}

pub fn write_output_manifest_with_limits(
    workspace: &Utf8Path,
    manifest: &OutputManifest,
    runtime: &RuntimeLimits,
) -> Result<(), std::io::Error> {
    let limits = OutputLimits::from_runtime(runtime)?;
    validate_output_manifest(manifest, limits)?;
    match limits.file_bytes {
        Some(limit) => {
            let mut writer = BoundedManifestWriter::new(
                usize::try_from(limit)
                    .map_err(|_| io::Error::other("output file limit does not fit in usize"))?,
            );
            serde_json::to_writer_pretty(&mut writer, manifest).map_err(|error| {
                if writer.exceeded {
                    io::Error::other(format!("outputs.json exceeds {limit} bytes"))
                } else {
                    io::Error::other(error)
                }
            })?;
            let path = workspace.join("outputs.json");
            let parent = path
                .parent()
                .ok_or_else(|| io::Error::other("outputs.json has no parent directory"))?;
            let mut temporary = NamedTempFile::new_in(parent.as_std_path())?;
            temporary.write_all(writer.bytes())?;
            temporary.as_file().sync_all()?;
            temporary
                .persist(path.as_std_path())
                .map_err(|error| io::Error::other(error.error))?;
            Ok(())
        }
        None => {
            let path = workspace.join("outputs.json");
            let parent = path
                .parent()
                .ok_or_else(|| io::Error::other("outputs.json has no parent directory"))?;
            let mut temporary = NamedTempFile::new_in(parent.as_std_path())?;
            serde_json::to_writer_pretty(&mut temporary, manifest)
                .map_err(std::io::Error::other)?;
            temporary.as_file().sync_all()?;
            temporary
                .persist(path.as_std_path())
                .map_err(|error| io::Error::other(error.error))?;
            Ok(())
        }
    }
}

pub fn read_output_manifest(workspace: &Utf8Path) -> Result<OutputManifest, std::io::Error> {
    read_output_manifest_with_limits(workspace, &RuntimeLimits::default())
}

pub fn read_output_manifest_with_limits(
    workspace: &Utf8Path,
    runtime: &RuntimeLimits,
) -> Result<OutputManifest, std::io::Error> {
    let limits = OutputLimits::from_runtime(runtime)?;
    let path = workspace.join("outputs.json");
    let file = fs::File::open(&path)?;
    if let Some(limit) = limits.file_bytes {
        let mut bytes = Vec::new();
        let mut limited = file.take(limit.saturating_add(1));
        std::io::Read::read_to_end(&mut limited, &mut bytes)?;
        if bytes.len() as u64 > limit {
            return Err(io::Error::other(format!(
                "outputs.json exceeds {limit} bytes"
            )));
        }
        return serde_json::from_slice(&bytes).map_err(std::io::Error::other);
    }
    serde_json::from_reader(file).map_err(std::io::Error::other)
}

fn validate_output_manifest(
    manifest: &OutputManifest,
    limits: OutputLimits,
) -> Result<(), std::io::Error> {
    if limits
        .artifact_count
        .is_some_and(|limit| manifest.artifacts.len() > limit)
    {
        return Err(io::Error::other(format!(
            "output artifact count exceeds {}",
            limits.artifact_count.unwrap_or(usize::MAX)
        )));
    }
    let mut paths = BTreeSet::new();
    let mut total = 0_u64;
    for artifact in &manifest.artifacts {
        if !paths.insert(&artifact.path) {
            return Err(io::Error::other(format!(
                "output manifest contains duplicate artifact `{}`",
                artifact.path
            )));
        }
        if limits
            .file_bytes
            .is_some_and(|limit| artifact.bytes > limit)
        {
            return Err(io::Error::other(format!(
                "output artifact `{}` exceeds {} bytes",
                artifact.path,
                limits.file_bytes.unwrap_or(u64::MAX)
            )));
        }
        total = total
            .checked_add(artifact.bytes)
            .ok_or_else(|| io::Error::other("output byte accounting overflowed"))?;
        if limits.total_bytes.is_some_and(|limit| total > limit) {
            return Err(io::Error::other(format!(
                "output bytes exceed {}",
                limits.total_bytes.unwrap_or(u64::MAX)
            )));
        }
    }
    Ok(())
}

struct BoundedManifestWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedManifestWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl io::Write for BoundedManifestWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("outputs.json size overflowed"))?;
        if next > self.limit {
            self.exceeded = true;
            return Err(io::Error::other("outputs.json exceeds its limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
