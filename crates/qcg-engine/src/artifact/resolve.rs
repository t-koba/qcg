use camino::{Utf8Path, Utf8PathBuf};
use qcg_types::OutputArtifact;
use sha2::{Digest, Sha256};
use std::fs;

pub fn resolve_artifact_path(
    workspace: &Utf8Path,
    artifact_path: &str,
) -> Result<Utf8PathBuf, std::io::Error> {
    validate_relative_artifact_path(artifact_path, false)?;
    let path = workspace.join(artifact_path);
    if !path.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("artifact `{artifact_path}` was not found"),
        ));
    }
    let canonical_workspace = dunce::canonicalize(workspace).map_err(std::io::Error::other)?;
    let canonical_path = dunce::canonicalize(&path).map_err(std::io::Error::other)?;
    if !canonical_path.starts_with(&canonical_workspace) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("artifact `{artifact_path}` resolves outside the workspace"),
        ));
    }
    Ok(path)
}

pub(crate) fn validate_relative_artifact_path(
    artifact_path: &str,
    allow_glob: bool,
) -> Result<(), std::io::Error> {
    if artifact_path.is_empty()
        || artifact_path.starts_with('/')
        || artifact_path.contains('\0')
        || artifact_path.contains('\\')
        || artifact_path.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || (!allow_glob && part.contains(['*', '?']))
        })
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("artifact path `{artifact_path}` is not allowed"),
        ));
    }
    Ok(())
}

pub(crate) fn build_artifact(
    workspace: &Utf8Path,
    path: &Utf8Path,
    metadata: ArtifactMetadata<'_>,
    file_limit: Option<u64>,
) -> Result<OutputArtifact, std::io::Error> {
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let bytes = {
        let mut writer = DigestWriter {
            digest: &mut digest,
            bytes: 0,
            limit: file_limit,
        };
        std::io::copy(&mut file, &mut writer)?;
        writer.bytes
    };
    let sha256 = hex::encode(digest.finalize());
    let rel = path
        .strip_prefix(workspace)
        .map_err(std::io::Error::other)?;
    let rel = qcg_policy::portable_relative_path(rel);
    Ok(OutputArtifact {
        path: rel,
        sha256,
        bytes,
        label: metadata.label.to_string(),
        required: metadata.required,
        mime: metadata.mime.map(str::to_string),
        description: metadata.description.to_string(),
        preview: metadata.preview,
    })
}

#[derive(Clone, Copy)]
pub(crate) struct ArtifactMetadata<'a> {
    pub(crate) label: &'a str,
    pub(crate) required: bool,
    pub(crate) mime: Option<&'a str>,
    pub(crate) description: &'a str,
    pub(crate) preview: qcg_types::ArtifactPreview,
}

struct DigestWriter<'a> {
    digest: &'a mut Sha256,
    bytes: u64,
    limit: Option<u64>,
}

impl std::io::Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next =
            self.bytes
                .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                    std::io::Error::other("artifact byte count does not fit in u64")
                })?)
                .ok_or_else(|| std::io::Error::other("artifact byte count overflowed"))?;
        if self.limit.is_some_and(|limit| next > limit) {
            return Err(std::io::Error::other(format!(
                "artifact file exceeds {} bytes",
                self.limit.unwrap_or(u64::MAX)
            )));
        }
        self.digest.update(bytes);
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
