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
        if file_limit.is_some_and(|limit| bytes > limit) {
            return Err(EngineError::Failed(format!(
                "output file `{path}` exceeds {} bytes",
                file_limit.unwrap_or(u64::MAX)
            )));
        }
        let key = path.as_str().to_owned();
        let previous = self.bytes_by_path.get(&key).copied();
        let next_count = self
            .bytes_by_path
            .len()
            .checked_add(if previous.is_some() { 0 } else { 1 })
            .ok_or_else(|| EngineError::Failed("output artifact count overflowed".into()))?;
        if limits
            .output_artifact_limit
            .is_some_and(|limit| next_count > limit)
        {
            return Err(EngineError::Failed(format!(
                "output artifact count exceeds {}",
                limits.output_artifact_limit.unwrap_or(usize::MAX)
            )));
        }
        let total_without_previous = self
            .total_bytes
            .checked_sub(previous.unwrap_or(0))
            .ok_or_else(|| EngineError::Failed("output byte accounting underflowed".into()))?;
        let next_total = total_without_previous
            .checked_add(bytes)
            .ok_or_else(|| EngineError::Failed("output byte accounting overflowed".into()))?;
        if total_limit.is_some_and(|limit| next_total > limit) {
            return Err(EngineError::Failed(format!(
                "output bytes exceed {}",
                total_limit.unwrap_or(u64::MAX)
            )));
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
            let source = workspace.join(relative);
            let digest = hash_file(&source, limits.output_file_limit_bytes)?;
            let previous = {
                let mut accounting = accounting.lock().map_err(|_| {
                    EngineError::Failed("checkpoint accounting mutex was poisoned".into())
                })?;
                accounting.record(relative, digest.bytes, limits)?
            };
            if let Err(error) = persist_checkpoint_blob(
                metadata,
                &digest.sha256,
                &source,
                limits.output_file_limit_bytes,
            ) {
                let mut accounting = accounting.lock().map_err(|_| {
                    EngineError::Failed("checkpoint accounting mutex was poisoned".into())
                })?;
                accounting.rollback(relative, digest.bytes, previous);
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
    if destination.exists() {
        if hash_file(&destination, file_limit)?.sha256 != sha256 {
            return Err(EngineError::Failed(format!(
                "checkpoint blob `{sha256}` does not match its content digest"
            )));
        }
        return Ok(());
    }
    let temporary = blobs.join(format!(".{sha256}.tmp-{}", Uuid::now_v7()));
    let copy_result = (|| -> Result<(), std::io::Error> {
        let mut input = std::fs::File::open(source)?;
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
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    match std::fs::rename(&temporary, &destination) {
        Ok(()) => Ok(()),
        Err(_error) if destination.exists() => {
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

pub(crate) fn hash_file(
    path: &camino::Utf8Path,
    file_limit: Option<usize>,
) -> Result<FileDigest, std::io::Error> {
    let mut file = std::fs::File::open(path)?;
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
        if self.limit.is_some_and(|limit| next > limit) {
            let limit = self.limit.unwrap_or(u64::MAX);
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
        if self.limit.is_some_and(|limit| next > limit) {
            return Err(std::io::Error::other(format!(
                "output file exceeds {} bytes",
                self.limit.unwrap_or(u64::MAX)
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
