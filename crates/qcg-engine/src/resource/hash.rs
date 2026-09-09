use crate::{EngineError, RunContext};
use camino::Utf8PathBuf;
use sha2::{Digest, Sha256};

use super::snapshot::DirectoryLimits;
use super::types::{ResourceError, ResourceFileSnapshot};

pub(crate) fn hash_resource_file(
    path: &camino::Utf8Path,
    max_bytes: Option<u64>,
) -> Result<(String, usize), std::io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let bytes = match max_bytes {
        Some(limit) => {
            let copied = std::io::copy(
                &mut std::io::Read::take(&mut file, limit.saturating_add(1)),
                &mut DigestWriter(&mut digest),
            )?;
            if copied > limit {
                return Err(std::io::Error::other(format!(
                    "resource file `{path}` exceeds max_bytes ({limit})"
                )));
            }
            copied
        }
        None => std::io::copy(&mut file, &mut DigestWriter(&mut digest))?,
    };
    let bytes = usize::try_from(bytes)
        .map_err(|_| std::io::Error::other(format!("resource file `{path}` is too large")))?;
    Ok((hex::encode(digest.finalize()), bytes))
}

pub(crate) fn hash_resource_dir(
    path: &camino::Utf8Path,
    limits: DirectoryLimits,
) -> Result<(String, Vec<ResourceFileSnapshot>), std::io::Error> {
    let mut files = Vec::new();
    let mut total_bytes = 0_u64;
    let mut entries = 0_usize;
    for entry in qcg_fs::WalkDir::new(path).min_depth(1) {
        let entry = entry.map_err(std::io::Error::other)?;
        if limits.max_depth.is_some_and(|limit| entry.depth() > limit) {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` exceeds max_depth ({})",
                limits.max_depth.unwrap_or(usize::MAX)
            )));
        }
        entries = entries
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("resource directory entry count overflowed"))?;
        if limits.max_entries.is_some_and(|limit| entries > limit) {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` exceeds max_entries ({})",
                limits.max_entries.unwrap_or(usize::MAX)
            )));
        }
        if entry.file_type().is_symlink() {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` contains a symbolic link"
            )));
        }
        if !entry.file_type().is_file() && !entry.file_type().is_dir() {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` contains an unsupported entry"
            )));
        }
        if !entry.file_type().is_file() {
            continue;
        }
        if limits.max_files.is_some_and(|limit| files.len() >= limit) {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` exceeds max_files ({})",
                limits.max_files.unwrap_or(usize::MAX)
            )));
        }
        let file_path = entry.path().to_path_buf();
        let relative = file_path
            .strip_prefix(path)
            .map_err(std::io::Error::other)?;
        let rel = relative
            .components()
            .map(|component| component.as_str())
            .collect::<Vec<_>>()
            .join("/");
        let file = std::fs::File::open(&file_path)?;
        let mut digest = Sha256::new();
        let bytes = match limits.max_bytes {
            Some(limit) => {
                let remaining = limit.saturating_sub(total_bytes);
                let copied = std::io::copy(
                    &mut std::io::Read::take(file, remaining.saturating_add(1)),
                    &mut DigestWriter(&mut digest),
                )?;
                if copied > remaining {
                    return Err(std::io::Error::other(format!(
                        "resource directory `{path}` exceeds max_bytes ({limit})"
                    )));
                }
                copied
            }
            None => {
                let mut file = file;
                std::io::copy(&mut file, &mut DigestWriter(&mut digest))?
            }
        };
        total_bytes = total_bytes.saturating_add(bytes);
        files.push(ResourceFileSnapshot {
            path: rel,
            sha256: hex::encode(digest.finalize()),
            bytes: usize::try_from(bytes).map_err(|_| {
                std::io::Error::other(format!("resource file `{file_path}` is too large"))
            })?,
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let mut digest = Sha256::new();
    for file in &files {
        digest.update(file.path.as_bytes());
        digest.update([0]);
        digest.update(file.sha256.as_bytes());
        digest.update([0]);
        digest.update(file.bytes.to_string().as_bytes());
        digest.update([0]);
    }
    Ok((hex::encode(digest.finalize()), files))
}

struct DigestWriter<'a>(&'a mut Sha256);

impl std::io::Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn resolve_resource_path(
    context: &RunContext,
    resource: &str,
    path: &str,
) -> Result<Utf8PathBuf, ResourceError> {
    context
        .contract
        .resolve_package_path(path)
        .map_err(|source| ResourceError::PackagePath {
            resource: resource.to_string(),
            source,
        })
}

pub(crate) fn resolve_resource_path_for_engine(
    context: &RunContext,
    resource: &str,
    path: &str,
) -> Result<Utf8PathBuf, EngineError> {
    context
        .contract
        .resolve_package_path(path)
        .map_err(|error| {
            EngineError::Failed(format!("resource `{resource}` path is invalid: {error}"))
        })
}
