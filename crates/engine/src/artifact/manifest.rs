use camino::Utf8Path;
use contract::RuntimeLimits;
use model::OutputManifest;
use std::io::{self, Read as _, Write as _};

use super::collect::OutputLimits;

pub fn write_output_manifest(workspace: &Utf8Path, manifest: &OutputManifest) -> io::Result<()> {
    write_output_manifest_with_limits(workspace, manifest, &RuntimeLimits::default())
}

pub fn write_output_manifest_with_limits(
    workspace: &Utf8Path,
    manifest: &OutputManifest,
    runtime: &RuntimeLimits,
) -> io::Result<()> {
    let limits = OutputLimits::from_runtime(runtime)?;
    validate_output_manifest(manifest, limits)?;
    let path = workspace.join("outputs.json");
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
            files::write_file_atomic(&path, |file| file.write_all(writer.bytes()))?;
        }
        None => {
            files::write_file_atomic(&path, |file| {
                serde_json::to_writer_pretty(file, manifest).map_err(std::io::Error::other)
            })?;
        }
    }
    Ok(())
}

pub fn read_output_manifest(workspace: &Utf8Path) -> io::Result<OutputManifest> {
    read_output_manifest_with_limits(workspace, &RuntimeLimits::default())
}

pub fn read_output_manifest_with_limits(
    workspace: &Utf8Path,
    runtime: &RuntimeLimits,
) -> io::Result<OutputManifest> {
    let limits = OutputLimits::from_runtime(runtime)?;
    let file = std::fs::File::open(workspace.join("outputs.json"))?;
    let manifest = if let Some(limit) = limits.file_bytes {
        let mut bytes = Vec::new();
        let mut limited = file.take(limit.saturating_add(1));
        std::io::Read::read_to_end(&mut limited, &mut bytes)?;
        if bytes.len() as u64 > limit {
            return Err(io::Error::other(format!(
                "outputs.json exceeds {limit} bytes"
            )));
        }
        serde_json::from_slice(&bytes)
    } else {
        serde_json::from_reader(file)
    }
    .map_err(std::io::Error::other)?;
    validate_output_manifest(&manifest, limits)?;
    Ok(manifest)
}

pub fn validate_output_manifest_paths(manifest: &OutputManifest) -> io::Result<()> {
    policy::path::validate_path_tree_set(
        manifest.artifacts.iter().map(|a| (a.path.as_str(), false)),
    )
    .map_err(|e| io::Error::other(format!("unsafe or duplicate artifact paths: {e}")))
}

fn validate_output_manifest(manifest: &OutputManifest, limits: OutputLimits) -> io::Result<()> {
    if let Some(limit) = limits.artifact_count
        && manifest.artifacts.len() > limit
    {
        return Err(io::Error::other(format!(
            "output artifact count exceeds {limit}"
        )));
    }
    validate_output_manifest_paths(manifest)?;
    let mut total = 0_u64;
    for artifact in &manifest.artifacts {
        if let Some(limit) = limits.file_bytes
            && artifact.bytes > limit
        {
            return Err(io::Error::other(format!(
                "output artifact `{}` exceeds {limit} bytes",
                artifact.path
            )));
        }
        total = total
            .checked_add(artifact.bytes)
            .ok_or_else(|| io::Error::other("output byte accounting overflowed"))?;
        if let Some(limit) = limits.total_bytes
            && total > limit
        {
            return Err(io::Error::other(format!("output bytes exceed {limit}")));
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
