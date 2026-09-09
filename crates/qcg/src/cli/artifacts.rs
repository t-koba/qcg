use anyhow::Result;
use camino::Utf8Path;
use qcg_contract::RuntimeLimits;
use qcg_types::{OutputArtifact, OutputManifest};

pub(crate) fn declared_artifact<'a>(
    manifest: &'a OutputManifest,
    path: &str,
) -> Result<&'a OutputArtifact, String> {
    if !qcg_policy::is_safe_relative_path(path) {
        return Err(format!("artifact assertion path `{path}` is unsafe"));
    }
    manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.path == path)
        .ok_or_else(|| format!("artifact `{path}` does not exist"))
}

pub(crate) fn read_declared_artifact(
    output_root: &Utf8Path,
    manifest: &OutputManifest,
    path: &str,
    runtime: &RuntimeLimits,
) -> Result<Vec<u8>, String> {
    let artifact = declared_artifact(manifest, path)?;
    let declared = usize::try_from(artifact.bytes)
        .map_err(|_| format!("artifact `{path}` byte count does not fit this platform"))?;
    if runtime
        .output_file_limit_bytes
        .is_some_and(|limit| artifact.bytes > limit as u64)
    {
        return Err(format!(
            "artifact `{path}` exceeds runtime.output_file_limit_bytes ({})",
            runtime.output_file_limit_bytes.unwrap_or(usize::MAX)
        ));
    }
    let bytes = qcg_fs::read_bounded(&output_root.join(&artifact.path), Some(declared))
        .map_err(|error| format!("artifact `{path}` could not be read: {error}"))?;
    if bytes.len() != declared {
        return Err(format!(
            "artifact `{path}` size changed: manifest declares {declared} bytes, found {}",
            bytes.len()
        ));
    }
    Ok(bytes)
}
