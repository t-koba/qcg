use crate::{FilePin, ResourceSnapshot, RunState};
use camino::Utf8PathBuf;
use qcg_contract::RuntimeLimits;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use super::types::EngineError;

pub(crate) fn default_metadata_dir(workspace: &camino::Utf8Path, run_id: &str) -> Utf8PathBuf {
    workspace
        .parent()
        .unwrap_or_else(|| camino::Utf8Path::new("."))
        .join(".qcg/runs")
        .join(run_id)
        .join("meta")
}

#[derive(Default)]
pub(crate) struct CheckpointAccounting {
    bytes_by_path: BTreeMap<String, u64>,
    total_bytes: u64,
}

impl CheckpointAccounting {
    pub(crate) fn record(
        &mut self,
        path: &camino::Utf8Path,
        bytes: u64,
        limits: &RuntimeLimits,
    ) -> Result<Option<u64>, EngineError> {
        let file_limit = limits
            .output_file_limit_bytes
            .map(|limit| {
                u64::try_from(limit).map_err(|_| {
                    EngineError::Failed(
                        "runtime.output_file_limit_bytes does not fit in u64".into(),
                    )
                })
            })
            .transpose()?;
        let total_limit = limits
            .output_total_limit_bytes
            .map(|limit| {
                u64::try_from(limit).map_err(|_| {
                    EngineError::Failed(
                        "runtime.output_total_limit_bytes does not fit in u64".into(),
                    )
                })
            })
            .transpose()?;
        if file_limit == Some(0)
            || total_limit == Some(0)
            || limits.output_artifact_limit == Some(0)
        {
            return Err(EngineError::Failed(
                "runtime output limits must be greater than zero".into(),
            ));
        }
        if let Some(limit) = file_limit
            && bytes > limit
        {
            return Err(EngineError::Failed(format!(
                "output file `{path}` exceeds {limit} bytes"
            )));
        }
        let key = path.as_str().to_owned();
        let previous = self.bytes_by_path.get(&key).copied();
        let next_count = self
            .bytes_by_path
            .len()
            .checked_add(if previous.is_some() { 0 } else { 1 })
            .ok_or_else(|| EngineError::Failed("output artifact count overflowed".into()))?;
        if let Some(limit) = limits.output_artifact_limit
            && next_count > limit
        {
            return Err(EngineError::Failed(format!(
                "output artifact count exceeds {limit}"
            )));
        }
        let total_without_previous = self
            .total_bytes
            .checked_sub(previous.unwrap_or(0))
            .ok_or_else(|| EngineError::Failed("output byte accounting underflowed".into()))?;
        let next_total = total_without_previous
            .checked_add(bytes)
            .ok_or_else(|| EngineError::Failed("output byte accounting overflowed".into()))?;
        if let Some(limit) = total_limit
            && next_total > limit
        {
            return Err(EngineError::Failed(format!("output bytes exceed {limit}")));
        }
        self.total_bytes = next_total;
        self.bytes_by_path.insert(key, bytes);
        Ok(previous)
    }

    fn rollback(&mut self, path: &camino::Utf8Path, bytes: u64, previous: Option<u64>) {
        let key = path.as_str();
        self.total_bytes = self
            .total_bytes
            .saturating_sub(bytes)
            .saturating_add(previous.unwrap_or(0));
        match previous {
            Some(previous) => {
                self.bytes_by_path.insert(key.to_owned(), previous);
            }
            None => {
                self.bytes_by_path.remove(key);
            }
        }
    }
}

pub(crate) fn pin_files(
    workspace: &camino::Utf8Path,
    metadata: &camino::Utf8Path,
    files: &[Utf8PathBuf],
    limits: &RuntimeLimits,
    accounting: &Arc<Mutex<CheckpointAccounting>>,
) -> Result<Vec<FilePin>, EngineError> {
    let canonical_workspace = dunce::canonicalize(workspace)?;
    let canonical_workspace = Utf8PathBuf::from_path_buf(canonical_workspace).map_err(|path| {
        EngineError::Failed(format!(
            "workspace path is not valid UTF-8: {}",
            path.display()
        ))
    })?;
    files
        .iter()
        .map(|path| {
            // Refuse symlink outputs at pin time, symmetric with resume
            // verification: pinning through a link while resume refuses it
            // would create pins that can never resume (E06).
            if is_symlink_no_follow(path)? {
                return Err(EngineError::Failed(format!(
                    "step output `{path}` is a symbolic link"
                )));
            }
            let canonical_path = dunce::canonicalize(path)?;
            let canonical_path = Utf8PathBuf::from_path_buf(canonical_path).map_err(|path| {
                EngineError::Failed(format!(
                    "step output path is not valid UTF-8: {}",
                    path.display()
                ))
            })?;
            let relative = if canonical_path.is_absolute() {
                canonical_path
                    .strip_prefix(&canonical_workspace)
                    .map_err(|_| {
                        EngineError::Failed(format!(
                            "step output `{path}` is outside workspace `{workspace}`"
                        ))
                    })?
            } else {
                canonical_path.as_path()
            };
            if relative
                .components()
                .any(|component| matches!(component, camino::Utf8Component::ParentDir))
            {
                return Err(EngineError::Failed(format!(
                    "step output `{relative}` escapes the workspace"
                )));
            }
            // Journaled pins must use portable separators on every
            // platform: resume validation only accepts slash-separated
            // paths, so a natively-separated pin could never resume (E06).
            let relative =
                camino::Utf8PathBuf::from(qcg_policy::portable_relative_path(relative));
            let source = workspace.join(&relative);
            let digest = hash_file(&source, limits.output_file_limit_bytes)?;
            let previous = {
                let mut accounting = accounting.lock().map_err(|error| {
                    EngineError::Failed(format!(
                        "checkpoint accounting mutex was poisoned: {error}"
                    ))
                })?;
                accounting.record(&relative, digest.bytes, limits)?
            };
            if let Err(error) = persist_checkpoint_blob(
                metadata,
                &digest.sha256,
                &source,
                limits.output_file_limit_bytes,
            ) {
                let mut accounting = accounting.lock().map_err(|poison| {
                    EngineError::Failed(format!(
                        "checkpoint accounting mutex was poisoned while rolling back `{relative}`: {poison}; \
                         original persist error: {error}"
                    ))
                })?;
                accounting.rollback(&relative, digest.bytes, previous);
                return Err(error);
            }
            Ok(FilePin {
                path: relative.to_path_buf(),
                sha256: digest.sha256,
            })
        })
        .collect()
}

pub(crate) fn existing_regular_files(
    files: Vec<Utf8PathBuf>,
) -> Result<Vec<Utf8PathBuf>, EngineError> {
    let mut existing = Vec::with_capacity(files.len());
    for path in files {
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => existing.push(path),
            Ok(_) => {
                return Err(EngineError::Failed(format!(
                    "step output `{path}` is not a regular file"
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(existing)
}

fn persist_checkpoint_blob(
    metadata: &camino::Utf8Path,
    sha256: &str,
    source: &camino::Utf8Path,
    file_limit: Option<usize>,
) -> Result<(), EngineError> {
    let blobs = metadata.join("checkpoint-blobs");
    std::fs::create_dir_all(&blobs)?;
    let destination = blobs.join(sha256);
    // Never follow a pre-planted symlink: a symlink at the blob path
    // pointing at matching content would pass an existence check. A symlink
    // here is always planted damage, never a valid blob (E06). Inspection
    // failures propagate instead of reading as absent (E06).
    if is_symlink_no_follow(&destination).map_err(EngineError::Io)? {
        return Err(EngineError::Failed(format!(
            "checkpoint blob `{sha256}` collides with a symbolic link"
        )));
    }
    // Never use `exists()` here: it follows a symlink planted between the
    // probe above and this check. `symlink_metadata` observes the leaf
    // itself (E06).
    match std::fs::symlink_metadata(&destination) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(EngineError::Failed(format!(
                "checkpoint blob `{sha256}` collides with a symbolic link"
            )));
        }
        Ok(_) => {
            if hash_file(&destination, file_limit)?.sha256 != sha256 {
                return Err(EngineError::Failed(format!(
                    "checkpoint blob `{sha256}` does not match its content digest"
                )));
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(EngineError::Io(error)),
    }
    let temporary = blobs.join(format!(".{sha256}.tmp-{}", Uuid::now_v7()));
    let copy_result = (|| -> Result<(), std::io::Error> {
        // Open without following a terminal symlink, matching the hash
        // above: a link swapped in after verification is refused instead
        // of copied (E06). Non-Unix falls back to a pre-open probe.
        #[cfg(unix)]
        let mut input = {
            use std::os::unix::fs::OpenOptionsExt as _;
            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(source)?
        };
        #[cfg(not(unix))]
        let mut input = {
            if is_symlink_no_follow(source)? {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("checkpoint source `{source}` is a symbolic link"),
                ));
            }
            std::fs::File::open(source)?
        };
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        {
            let mut writer = LimitedFileWriter {
                file: &mut file,
                bytes: 0,
                limit: file_limit
                    .map(|limit| {
                        u64::try_from(limit).map_err(|_| {
                            std::io::Error::other("output file limit does not fit in u64")
                        })
                    })
                    .transpose()?,
            };
            std::io::copy(&mut input, &mut writer)?;
        }
        file.sync_all()?;
        Ok(())
    })();
    if let Err(error) = copy_result {
        // Best-effort reclaim documented here: the copy error below stays
        // authoritative, and the startup sweep reaps anything left behind.
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    match std::fs::rename(&temporary, &destination) {
        Ok(()) => Ok(()),
        Err(_rename_error)
            if matches!(
                std::fs::symlink_metadata(&destination).map(|m| m.file_type().is_symlink()),
                Ok(false)
            ) =>
        {
            // Best-effort reclaim documented here: the digest check below
            // decides the outcome. A symlink at the destination is never
            // treated as a competing writer (E06).
            let _ = std::fs::remove_file(&temporary);
            if hash_file(&destination, file_limit)?.sha256 == sha256 {
                Ok(())
            } else {
                Err(EngineError::Failed(format!(
                    "checkpoint blob `{sha256}` was replaced with invalid content"
                )))
            }
        }
        Err(error) => {
            let _ = std::fs::remove_file(&temporary);
            Err(error.into())
        }
    }
}

pub(crate) struct FileDigest {
    pub(crate) sha256: String,
    pub(crate) bytes: u64,
}

/// Reports whether `path` is a symlink without following it. A missing path
/// is not a symlink; any other inspection failure propagates so callers fail
/// closed instead of treating an unreadable path as safe (E06).
pub(crate) fn is_symlink_no_follow(path: &camino::Utf8Path) -> Result<bool, std::io::Error> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_symlink()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub(crate) fn hash_file(
    path: &camino::Utf8Path,
    file_limit: Option<usize>,
) -> Result<FileDigest, std::io::Error> {
    // Open with O_NOFOLLOW on Unix so a terminal symlink is refused instead
    // of followed: verification must hash the pinned file itself, never a
    // link target planted after the pin (E06). The opened handle is then
    // validated as a regular file via fstat, closing the swap window between
    // a pathname check and the open.
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt as _;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let file_type = file.metadata()?.file_type();
        if !file_type.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("`{path}` is not a regular file"),
            ));
        }
        file
    };
    // No handle exists that can express O_NOFOLLOW on this platform; the
    // pathname check below is the only available validation and is
    // documented as best-effort here (E13). Non-Unix resume verification
    // additionally refuses symlinked parents at the call site.
    #[cfg(not(unix))]
    let file = {
        if is_symlink_no_follow(path)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("`{path}` is a symbolic link"),
            ));
        }
        let file = std::fs::File::open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("`{path}` is not a regular file"),
            ));
        }
        file
    };
    let mut file = file;
    let mut digest = Sha256::new();
    let bytes = {
        let mut writer = Sha256Writer {
            digest: &mut digest,
            bytes: 0,
            limit: file_limit
                .map(|limit| {
                    u64::try_from(limit)
                        .map_err(|_| std::io::Error::other("output file limit does not fit in u64"))
                })
                .transpose()?,
        };
        std::io::copy(&mut file, &mut writer)?;
        writer.bytes
    };
    Ok(FileDigest {
        sha256: hex::encode(digest.finalize()),
        bytes,
    })
}

struct Sha256Writer<'a> {
    digest: &'a mut Sha256,
    bytes: u64,
    limit: Option<u64>,
}

impl std::io::Write for Sha256Writer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| std::io::Error::other("output byte count does not fit in u64"))?,
            )
            .ok_or_else(|| std::io::Error::other("output byte count overflowed"))?;
        if let Some(limit) = self.limit
            && next > limit
        {
            return Err(std::io::Error::other(format!(
                "output file exceeds {limit} bytes"
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

struct LimitedFileWriter<'a> {
    file: &'a mut std::fs::File,
    bytes: u64,
    limit: Option<u64>,
}

impl std::io::Write for LimitedFileWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| std::io::Error::other("output byte count does not fit in u64"))?,
            )
            .ok_or_else(|| std::io::Error::other("output byte count overflowed"))?;
        if let Some(limit) = self.limit
            && next > limit
        {
            return Err(std::io::Error::other(format!(
                "output file exceeds {limit} bytes"
            )));
        }
        self.file.write_all(bytes)?;
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

pub(crate) fn verify_resource_pins(
    state: &RunState,
    snapshots: &[ResourceSnapshot],
) -> Result<(), EngineError> {
    if state.run_id.is_none() {
        return Ok(());
    }
    let current = snapshots
        .iter()
        .map(|snapshot| (snapshot.name.clone(), snapshot.sha256.clone()))
        .collect::<BTreeMap<_, _>>();
    for (name, expected) in &state.resource_pins {
        let actual = current.get(name).ok_or_else(|| {
            EngineError::Failed(format!(
                "pinned resource `{name}` is unavailable while resuming"
            ))
        })?;
        if actual != expected {
            return Err(EngineError::Failed(format!(
                "resource `{name}` changed while resuming: expected {expected}, got {actual}"
            )));
        }
    }
    Ok(())
}
