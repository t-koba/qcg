use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{CommandIsolation, Permissions};

use super::error::GatewayError;

#[derive(Debug, Clone)]
pub struct CommandPermissionSummary {
    pub bin: String,
    pub args: Vec<String>,
    pub purpose: String,
    pub isolation: Option<CommandIsolation>,
    pub image: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FsGateway {
    workspace: Utf8PathBuf,
    can_read_workspace: bool,
    can_write_workspace: bool,
}

impl FsGateway {
    pub fn new(workspace: Utf8PathBuf, permissions: &Permissions) -> Self {
        Self {
            workspace,
            can_read_workspace: permissions.fs_read.iter().any(|scope| scope == "workspace"),
            can_write_workspace: permissions
                .fs_write
                .iter()
                .any(|scope| scope == "workspace"),
        }
    }

    pub fn resolve_read(&self, path: &str) -> Result<Utf8PathBuf, GatewayError> {
        if !self.can_read_workspace {
            return Err(GatewayError::FsReadDenied);
        }
        self.resolve_workspace_path(path, false)
    }

    pub fn resolve_write(&self, path: &str) -> Result<Utf8PathBuf, GatewayError> {
        if !self.can_write_workspace {
            return Err(GatewayError::FsWriteDenied);
        }
        // Secure parent creation and validation happen inside
        // resolve_workspace_path before any external directory is created.
        self.resolve_workspace_path(path, true)
    }

    /// Privileged internal placement sharing the path-isolation invariant
    /// without requiring the general fs_write permission (A04). Used only
    /// for contract-declared file inputs, never for general step writes.
    pub fn resolve_internal_write(&self, path: &str) -> Result<Utf8PathBuf, GatewayError> {
        self.resolve_workspace_path(path, true)
    }

    fn resolve_workspace_path(
        &self,
        path: &str,
        allow_missing_leaf: bool,
    ) -> Result<Utf8PathBuf, GatewayError> {
        if path == "." && !allow_missing_leaf {
            let workspace = dunce::canonicalize(&self.workspace)?;
            return Utf8PathBuf::from_path_buf(workspace).map_err(|_| GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        let relative = Utf8Path::new(path);
        if path.contains('\0')
            || path.contains('\\')
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, camino::Utf8Component::Normal(_)))
        {
            return Err(GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        let joined = self.workspace.join(path);
        // Ensure the trusted base exists before canonicalization. This only
        // creates the workspace itself, never attacker-controlled children.
        std::fs::create_dir_all(&self.workspace)?;
        let workspace = dunce::canonicalize(&self.workspace)?;
        let workspace =
            Utf8PathBuf::from_path_buf(workspace).map_err(|_| GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            })?;
        if !allow_missing_leaf {
            let candidate = dunce::canonicalize(&joined)?;
            let candidate =
                Utf8PathBuf::from_path_buf(candidate).map_err(|_| GatewayError::PathDenied {
                    path: Utf8PathBuf::from(path),
                    workspace: self.workspace.clone(),
                })?;
            if !candidate.starts_with(&workspace) {
                return Err(GatewayError::PathDenied {
                    path: Utf8PathBuf::from(path),
                    workspace: self.workspace.clone(),
                });
            }
            return Ok(candidate);
        }
        let parts: Vec<String> = relative
            .components()
            .filter_map(|component| match component {
                camino::Utf8Component::Normal(part) => Some(part.to_string()),
                _ => None,
            })
            .collect();
        let Some((file_name, parent_parts)) = parts.split_last() else {
            return Err(GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        };
        // Validate and create each parent level before touching the next one.
        // A symlinked parent pointing outside is rejected without creating
        // anything inside the external target.
        let mut current = workspace.clone();
        for part in parent_parts {
            let next = current.join(part);
            match std::fs::symlink_metadata(&next) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(GatewayError::PathDenied {
                        path: Utf8PathBuf::from(path),
                        workspace: self.workspace.clone(),
                    });
                }
                Ok(metadata) if !metadata.is_dir() => {
                    return Err(GatewayError::PathDenied {
                        path: Utf8PathBuf::from(path),
                        workspace: self.workspace.clone(),
                    });
                }
                Ok(_) => {
                    let canonical = dunce::canonicalize(&next)?;
                    let canonical = Utf8PathBuf::from_path_buf(canonical).map_err(|_| {
                        GatewayError::PathDenied {
                            path: Utf8PathBuf::from(path),
                            workspace: self.workspace.clone(),
                        }
                    })?;
                    if !canonical.starts_with(&workspace) {
                        return Err(GatewayError::PathDenied {
                            path: Utf8PathBuf::from(path),
                            workspace: self.workspace.clone(),
                        });
                    }
                    current = canonical;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::create_dir(&next)?;
                    let canonical = dunce::canonicalize(&next)?;
                    let canonical = Utf8PathBuf::from_path_buf(canonical).map_err(|_| {
                        GatewayError::PathDenied {
                            path: Utf8PathBuf::from(path),
                            workspace: self.workspace.clone(),
                        }
                    })?;
                    if !canonical.starts_with(&workspace) {
                        // Race or symlink swap: remove what we just created
                        // inside the rejected location when possible.
                        let _ = std::fs::remove_dir(&canonical);
                        return Err(GatewayError::PathDenied {
                            path: Utf8PathBuf::from(path),
                            workspace: self.workspace.clone(),
                        });
                    }
                    current = canonical;
                }
                Err(error) => return Err(GatewayError::Io(error)),
            }
        }
        let candidate = current.join(file_name);
        // Reject an existing terminal symlink instead of following it.
        // Atomic replace below also replaces the link itself, but the write
        // must be denied outright so pin checks cannot be bypassed.
        if let Ok(metadata) = std::fs::symlink_metadata(&candidate)
            && metadata.file_type().is_symlink()
        {
            return Err(GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        if !candidate.starts_with(&workspace) {
            return Err(GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        Ok(candidate)
    }

    pub fn workspace(&self) -> &Utf8Path {
        &self.workspace
    }

    /// Atomically write bytes to an already-resolved workspace path.
    ///
    /// The target must have been returned by `resolve_write`. The terminal
    /// component is re-checked for symlinks and the parent is re-validated
    /// against the workspace before the rename, then the content is staged
    /// to a `create_new` temporary file and renamed over the target. Rename
    /// replaces a terminal symlink itself, but we deny that case outright.
    pub async fn write_file_atomic(
        &self,
        target: &Utf8Path,
        bytes: &[u8],
    ) -> Result<(), GatewayError> {
        let workspace = dunce::canonicalize(&self.workspace)?;
        let workspace =
            Utf8PathBuf::from_path_buf(workspace).map_err(|_| GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            })?;
        let Some(parent) = target.parent() else {
            return Err(GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            });
        };
        let canonical_parent =
            dunce::canonicalize(parent).map_err(|_| GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            })?;
        let canonical_parent =
            Utf8PathBuf::from_path_buf(canonical_parent).map_err(|_| GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            })?;
        if !canonical_parent.starts_with(&workspace) {
            return Err(GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            });
        }
        let resolved =
            canonical_parent.join(target.file_name().ok_or_else(|| GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            })?);
        if !resolved.starts_with(&workspace) {
            return Err(GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            });
        }
        if let Ok(metadata) = std::fs::symlink_metadata(&resolved)
            && metadata.file_type().is_symlink()
        {
            return Err(GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            });
        }
        let file_name = resolved.file_name().unwrap_or("output").to_string();
        let temporary = resolved.with_file_name(format!(
            ".{file_name}.qcg-part-{}",
            uuid::Uuid::now_v7().as_simple()
        ));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .await?;
            tokio::io::AsyncWriteExt::write_all(&mut file, bytes).await?;
            file.sync_all().await?;
            drop(file);
            #[cfg(not(windows))]
            {
                tokio::fs::rename(&temporary, &resolved).await?;
            }
            #[cfg(windows)]
            {
                let target = resolved.clone();
                let temporary = temporary.clone();
                tokio::task::spawn_blocking(move || {
                    use std::iter::once;
                    use std::os::windows::ffi::OsStrExt as _;
                    use windows_sys::Win32::Storage::FileSystem::{
                        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
                    };
                    let source = temporary
                        .as_std_path()
                        .as_os_str()
                        .encode_wide()
                        .chain(once(0))
                        .collect::<Vec<_>>();
                    let destination = target
                        .as_std_path()
                        .as_os_str()
                        .encode_wide()
                        .chain(once(0))
                        .collect::<Vec<_>>();
                    // SAFETY: both paths are NUL-terminated UTF-16 strings owned for this call.
                    let moved = unsafe {
                        MoveFileExW(
                            source.as_ptr(),
                            destination.as_ptr(),
                            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                        )
                    };
                    if moved == 0 {
                        Err(std::io::Error::last_os_error())
                    } else {
                        Ok(())
                    }
                })
                .await
                .map_err(std::io::Error::other)??;
            }
            Ok::<(), std::io::Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result.map_err(GatewayError::Io)
    }
}
