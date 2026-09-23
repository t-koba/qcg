//! Workspace filesystem gateway: containment resolution, handle-relative
//! Unix I/O (`super::handle::ParentHandle`), and staging ownership
//! (`AsyncStagingGuard` on Unix / `AsyncStreamGuard` on non-Unix).
//!
//! Responsibility split (C-5, E13): `ParentHandle` is workspace-scoped
//! (never merge with `qcg-fs` trusted-path helpers without a containment
//! review); a future split moves it to `gateway/handle.rs` and the guards
//! to `gateway/staging.rs` with no behavior change.
use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{CommandIsolation, Permissions};

use super::error::GatewayError;
#[cfg(unix)]
use super::staging::AsyncStagingGuard;
#[cfg(not(unix))]
use super::staging::AsyncStreamGuard;

// Workspace filesystem isolation boundary (E13).
//
// Unix: handle-relative traversal with `O_NOFOLLOW` at every component,
// parent identity (`dev`/`ino`) re-verified before commit, staging at
// `0600` with the final mode applied via `fchmodat` before the atomic
// rename. Non-Unix: pathname checks only (best-effort); concurrent
// symlink swaps by another same-user process can slip between validation
// and use there. Non-cooperative concurrent writers (another process,
// container workload, or non-Unix same-user writer racing validation) are
// outside the guaranteed boundary on every platform: flows must not
// parallelize container and host writes to overlapping paths, and
// single-process pack is the supported boundary for archiving. Filenames
// follow portable rules on every platform (`\` is rejected even where
// Unix would allow it) so archives and snapshots stay portable.
//
// Separation from `qcg-fs` (E13): `super::handle::ParentHandle` below is
// workspace-scoped (containment resolution plus `AsyncStagingGuard` plus
// mode commit on one open parent fd), while `qcg-fs::unix_atomic_write`
// is for trusted internal paths (run metadata, artifacts) with no
// workspace containment. Do not merge them without a containment review:
// the gateway must never delegate workspace writes to the trusted-path
// helper, and the trusted-path helper must never assume workspace
// containment.

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

    /// Default owner-only mode for staged files. One constant serves every
    /// atomic-write path on every platform so the fallback cannot diverge
    /// between them (E13).
    const DEFAULT_STAGED_MODE: u32 = 0o600;

    /// Resolves an already-masked caller mode to the staged-file mode.
    /// Special bits never stage: setuid/setgid/sticky are stripped and a
    /// `0777` request lands `0775` (other-write stripped, group-write kept
    /// by design), so gateway staging is fail-closed on its own without
    /// relying on later sanitizers (E13/E15).
    fn staged_mode(mode: Option<u32>) -> u32 {
        mode.map(qcg_fs::sanitize_mode_bits)
            .unwrap_or(Self::DEFAULT_STAGED_MODE)
    }

    /// Verifies a workspace file or directory tree against the byte and
    /// entry limits without following symlinks at any component, so a
    /// parent swapped after validation cannot redirect the walk (E13).
    #[cfg(unix)]
    pub fn bounded_tree_stats(
        &self,
        target: &Utf8Path,
        limit: Option<usize>,
        count_limit: Option<usize>,
    ) -> Result<(), GatewayError> {
        let denied = || GatewayError::PathDenied {
            path: target.to_path_buf(),
            workspace: self.workspace.clone(),
        };
        let workspace = dunce::canonicalize(&self.workspace).map_err(|_| denied())?;
        let workspace = Utf8PathBuf::from_path_buf(workspace).map_err(|_| denied())?;
        let relative = target.strip_prefix(&workspace).map_err(|_| denied())?;
        super::handle::bounded_tree_stats(&workspace, relative.as_str(), limit, count_limit)
            .map_err(GatewayError::Io)
    }

    /// Non-Unix fallback: validate the already-resolved path string. No
    /// handle exists that can express O_NOFOLLOW traversal on this
    /// platform, so these pathname checks are the only available validation
    /// and are documented as best-effort here (E13).
    #[cfg(not(unix))]
    pub fn bounded_tree_stats(
        &self,
        target: &Utf8Path,
        limit: Option<usize>,
        count_limit: Option<usize>,
    ) -> Result<(), GatewayError> {
        let mut total = 0_usize;
        let mut entries = 0_usize;
        let metadata = std::fs::symlink_metadata(target)?;
        if metadata.file_type().is_symlink() {
            return Err(GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            });
        }
        if metadata.is_file() {
            let size = usize::try_from(metadata.len()).map_err(|_| {
                GatewayError::Io(std::io::Error::other("file input size is invalid"))
            })?;
            if let Some(limit) = limit
                && size > limit
            {
                return Err(GatewayError::Io(std::io::Error::other(format!(
                    "file input exceeds {limit} bytes"
                ))));
            }
            return Ok(());
        }
        let mut stack = vec![target.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let file_type = entry.file_type()?;
                if file_type.is_symlink() {
                    let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|_| {
                        GatewayError::PathDenied {
                            path: target.to_path_buf(),
                            workspace: self.workspace.clone(),
                        }
                    })?;
                    return Err(GatewayError::PathDenied {
                        path,
                        workspace: self.workspace.clone(),
                    });
                }
                // Counter overflow fails closed instead of saturating (E13).
                entries = entries.checked_add(1).ok_or_else(|| {
                    GatewayError::Io(std::io::Error::other("file input entry count overflowed"))
                })?;
                if let Some(limit) = count_limit
                    && entries > limit
                {
                    return Err(GatewayError::Io(std::io::Error::other(format!(
                        "file input contains more than {limit} entries"
                    ))));
                }
                if file_type.is_dir() {
                    stack.push(Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                        GatewayError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("path is not UTF-8: {}", path.display()),
                        ))
                    })?);
                } else if file_type.is_file() {
                    // The cumulative total is checked BEFORE reading the
                    // next file so an oversized tree cannot bloat past the
                    // limit mid-walk; overflow fails closed (E13).
                    total = total
                        .checked_add(usize::try_from(entry.metadata()?.len()).map_err(|_| {
                            GatewayError::Io(std::io::Error::other("file input size is invalid"))
                        })?)
                        .ok_or_else(|| {
                            GatewayError::Io(std::io::Error::other("file input size overflowed"))
                        })?;
                    if let Some(limit) = limit
                        && total > limit
                    {
                        return Err(GatewayError::Io(std::io::Error::other(format!(
                            "file input exceeds {limit} bytes"
                        ))));
                    }
                }
            }
        }
        Ok(())
    }

    /// Copies a workspace file or directory tree into `dest` (outside the
    /// workspace, for example per-run metadata) through handle-relative
    /// reads, applying the same byte and entry limits as
    /// [`Self::bounded_tree_stats`]. Consumers that must parse a tree with
    /// path-based APIs read the private snapshot instead of the workspace,
    /// so a parent swapped after validation cannot redirect them (E13).
    #[cfg(unix)]
    pub fn snapshot_tree_to(
        &self,
        target: &Utf8Path,
        dest: &Utf8Path,
        limit: Option<usize>,
        count_limit: Option<usize>,
    ) -> Result<(), GatewayError> {
        let denied = || GatewayError::PathDenied {
            path: target.to_path_buf(),
            workspace: self.workspace.clone(),
        };
        let workspace = dunce::canonicalize(&self.workspace).map_err(|_| denied())?;
        let workspace = Utf8PathBuf::from_path_buf(workspace).map_err(|_| denied())?;
        let relative = target.strip_prefix(&workspace).map_err(|_| denied())?;
        super::handle::snapshot_tree(&workspace, relative.as_str(), dest, limit, count_limit)
            .map_err(GatewayError::Io)
    }

    /// Non-Unix fallback: copy the already-resolved tree by path. No
    /// handle exists that can express O_NOFOLLOW traversal on this
    /// platform, so this pathname walk is the only available validation and
    /// is documented as best-effort here (E13). Symlinks are refused, never
    /// followed.
    #[cfg(not(unix))]
    pub fn snapshot_tree_to(
        &self,
        target: &Utf8Path,
        dest: &Utf8Path,
        limit: Option<usize>,
        count_limit: Option<usize>,
    ) -> Result<(), GatewayError> {
        let metadata = std::fs::symlink_metadata(target)?;
        if metadata.file_type().is_symlink() {
            return Err(GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            });
        }
        if metadata.is_file() {
            if let Some(parent) = dest.parent() {
                create_dest_dir(parent).map_err(GatewayError::Io)?;
            }
            // Refuse a planted symlink at the destination even on this
            // best-effort path: copying through it would write outside the
            // snapshot (E13). E13i: permission errors fail closed (propagated),
            // only NotFound (absent destination) proceeds.
            match std::fs::symlink_metadata(dest) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    return Err(GatewayError::PathDenied {
                        path: dest.to_path_buf(),
                        workspace: self.workspace.clone(),
                    });
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(GatewayError::Io(error)),
            }
            std::fs::copy(target, dest)?;
            return Ok(());
        }
        create_dest_dir(dest).map_err(GatewayError::Io)?;
        let mut total = 0_usize;
        let mut entries = 0_usize;
        let mut stack = vec![(target.to_path_buf(), dest.to_path_buf())];
        while let Some((source, out)) = stack.pop() {
            for entry in std::fs::read_dir(&source)? {
                let entry = entry?;
                let file_type = entry.file_type()?;
                // Non-UTF-8 names cannot be validated against the
                // workspace, so they fail closed instead of passing
                // through or being silently skipped.
                let denied = || GatewayError::PathDenied {
                    path: target.to_path_buf(),
                    workspace: self.workspace.clone(),
                };
                let entry_path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|_| denied())?;
                if file_type.is_symlink() {
                    return Err(GatewayError::PathDenied {
                        path: entry_path,
                        workspace: self.workspace.clone(),
                    });
                }
                entries = entries.checked_add(1).ok_or_else(|| {
                    GatewayError::Io(std::io::Error::other("file input entry count overflowed"))
                })?;
                if count_limit.is_some_and(|limit| entries > limit) {
                    return Err(GatewayError::Io(std::io::Error::other(
                        "file input entry count exceeded during snapshot",
                    )));
                }
                let entry_name = entry.file_name();
                let entry_name = entry_name.to_str().ok_or_else(denied)?;
                let entry_dest = out.join(entry_name);
                if file_type.is_dir() {
                    create_dest_dir(&entry_dest).map_err(GatewayError::Io)?;
                    stack.push((entry_path, entry_dest));
                } else if file_type.is_file() {
                    // Best-effort destination link check, mirroring the
                    // single-file path above (E13). E13i: only NotFound
                    // proceeds; permission and other inspection failures
                    // fail closed.
                    match std::fs::symlink_metadata(&entry_dest) {
                        Ok(meta) if meta.file_type().is_symlink() => {
                            return Err(GatewayError::PathDenied {
                                path: entry_dest,
                                workspace: self.workspace.clone(),
                            });
                        }
                        Ok(_) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(GatewayError::Io(error)),
                    }
                    std::fs::copy(entry.path(), &entry_dest)?;
                    // Cumulative total checked during the copy walk with
                    // overflow failing closed (E13).
                    total = total
                        .checked_add(usize::try_from(entry.metadata()?.len()).map_err(|_| {
                            GatewayError::Io(std::io::Error::other("file input size is invalid"))
                        })?)
                        .ok_or_else(|| {
                            GatewayError::Io(std::io::Error::other("file input size overflowed"))
                        })?;
                    if limit.is_some_and(|limit| total > limit) {
                        return Err(GatewayError::Io(std::io::Error::other(
                            "file input byte limit exceeded during snapshot",
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Opens a workspace-relative file for reading through directory
    /// handles with `O_NOFOLLOW` on every component, so a parent directory
    /// swapped after validation cannot redirect the open outside the
    /// workspace (E13a).
    #[cfg(unix)]
    pub fn open_read(&self, path: &str) -> Result<std::fs::File, GatewayError> {
        self.resolve_read(path)?;
        // Canonicalize the base before the handle walk: on macOS the
        // configured workspace may contain a symlinked component
        // (`/var` -> `/private/var`) and opening it with `O_NOFOLLOW`
        // fails with `ELOOP`. The canonical root denotes the same
        // directory without symlink components (E13).
        let workspace = dunce::canonicalize(&self.workspace).map_err(GatewayError::Io)?;
        let workspace =
            Utf8PathBuf::from_path_buf(workspace).map_err(|_| GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            })?;
        super::handle::open_read(&workspace, path).map_err(GatewayError::Io)
    }

    /// Non-Unix fallback: open the already-resolved path. No handle exists
    /// that can express an O_NOFOLLOW open on this platform, so the
    /// resolve-time symlink checks are the only available validation and
    /// are documented as best-effort here (E13).
    #[cfg(not(unix))]
    pub fn open_read(&self, path: &str) -> Result<std::fs::File, GatewayError> {
        let resolved = self.resolve_read(path)?;
        std::fs::File::open(resolved).map_err(GatewayError::Io)
    }

    /// Opens a path previously returned by [`Self::resolve_read`], re-walking
    /// it handle-relative so a parent swapped after resolution cannot
    /// redirect the open (E13a).
    #[cfg(unix)]
    pub fn open_read_resolved(&self, target: &Utf8Path) -> Result<std::fs::File, GatewayError> {
        let denied = || GatewayError::PathDenied {
            path: target.to_path_buf(),
            workspace: self.workspace.clone(),
        };
        let workspace = dunce::canonicalize(&self.workspace).map_err(|_| denied())?;
        let workspace = Utf8PathBuf::from_path_buf(workspace).map_err(|_| denied())?;
        // Accept targets anchored at either the canonical or the configured
        // (possibly symlinked, e.g. `/var` -> `/private/var`) workspace
        // prefix, then re-anchor on the canonical root for the handle walk.
        let relative = target
            .strip_prefix(&workspace)
            .or_else(|_| target.strip_prefix(&self.workspace))
            .map_err(|_| denied())?;
        super::handle::open_read(&workspace, relative.as_str()).map_err(GatewayError::Io)
    }

    #[cfg(not(unix))]
    pub fn open_read_resolved(&self, target: &Utf8Path) -> Result<std::fs::File, GatewayError> {
        std::fs::File::open(target).map_err(GatewayError::Io)
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
        // Parent directories for a missing leaf are never created here:
        // creation happens handle-relative inside the write path, so a
        // parent swapped between validation and use cannot divert a `mkdir`
        // side effect outside the workspace (E13).
        std::fs::create_dir_all(&self.workspace)?;
        let workspace = dunce::canonicalize(&self.workspace)?;
        let workspace =
            Utf8PathBuf::from_path_buf(workspace).map_err(|_| GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            })?;
        #[cfg(unix)]
        if allow_missing_leaf {
            return self.resolve_workspace_path_unix_no_mkdir(path, &workspace, &joined);
        }
        if !allow_missing_leaf {
            // Read-path resolution by full canonicalization: symlinks
            // resolve first, then containment is checked on the resolved
            // path. On Unix this is a pre-check only — every read re-walks
            // handle-relative with O_NOFOLLOW at use time (`open_read`,
            // `open_read_resolved`, `bounded_tree_stats`), which enforces
            // the invariant. On non-Unix no O_NOFOLLOW handle exists, so
            // this resolution is the enforcement and is documented as
            // best-effort here (E13).
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
                        // Race or symlink swap: reclaim what was just
                        // created inside the rejected location when possible.
                        // Best-effort and documented here: the denial below
                        // stays authoritative (E13).
                        if let Err(error) = std::fs::remove_dir(&canonical) {
                            tracing::warn!(
                                path = %canonical.as_str(),
                                %error,
                                "failed to reclaim a directory created inside a rejected location"
                            );
                        }
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
        // must be denied outright so pin checks cannot be bypassed. This is
        // the non-Unix enforcement (Unix enforces on the open parent handle
        // at use time); inspection failures fail closed (E13).
        match std::fs::symlink_metadata(&candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(GatewayError::PathDenied {
                    path: Utf8PathBuf::from(path),
                    workspace: self.workspace.clone(),
                });
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(GatewayError::Io(error)),
        }
        if !candidate.starts_with(&workspace) {
            return Err(GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        Ok(candidate)
    }

    /// Unix resolution for a missing leaf without any `mkdir` side effect.
    /// Existing parents are validated by opening each one with
    /// `O_NOFOLLOW` (a symlink at any component fails the open instead of
    /// being followed); missing parents are allowed lexically and created
    /// later handle-relative inside the write path. No directory is created
    /// here, so a parent swapped between validation and use cannot divert
    /// creation outside the workspace (E13). The use-time handle walk
    /// (`ParentHandle`) re-enforces the same invariant, so this walk is a
    /// pre-check, not the authority.
    #[cfg(unix)]
    fn resolve_workspace_path_unix_no_mkdir(
        &self,
        path: &str,
        workspace: &Utf8PathBuf,
        _joined: &Utf8PathBuf,
    ) -> Result<Utf8PathBuf, GatewayError> {
        let denied = || GatewayError::PathDenied {
            path: Utf8PathBuf::from(path),
            workspace: self.workspace.clone(),
        };
        let relative = Utf8Path::new(path);
        let parts: Vec<String> = relative
            .components()
            .filter_map(|component| match component {
                camino::Utf8Component::Normal(part) => Some(part.to_string()),
                _ => None,
            })
            .collect();
        let Some((file_name, parent_parts)) = parts.split_last() else {
            return Err(denied());
        };
        // Handle-relative parent validation: every existing component must
        // open as a directory without following symlinks. The walk stops at
        // the first missing component; the remainder stays lexical for
        // handle-relative creation at use time.
        let mut missing = false;
        let mut dir = super::handle::open_root(workspace).map_err(|_| denied())?;
        for part in parent_parts {
            if missing {
                break;
            }
            match super::handle::open_dir_no_follow(dir, part) {
                Ok(next) => {
                    super::handle::close_fd(dir);
                    dir = next;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing = true;
                }
                // ELOOP (symlink) and ENOTDIR (non-directory) both mean the
                // path escapes the workspace layout: deny. Other I/O errors
                // (e.g. permission) are infrastructure failures, not
                // traversal, and surface as Io (E13).
                Err(error)
                    if error.raw_os_error() == Some(libc::ELOOP)
                        || error.raw_os_error() == Some(libc::ENOTDIR) =>
                {
                    super::handle::close_fd(dir);
                    return Err(denied());
                }
                Err(error) => {
                    super::handle::close_fd(dir);
                    return Err(GatewayError::Io(error));
                }
            }
        }
        super::handle::close_fd(dir);
        // Lexical containment holds because every part is a normal
        // component (no `..`, no absolute).
        let mut current = workspace.clone();
        for part in parent_parts {
            current = current.join(part);
        }
        let candidate = current.join(file_name);
        if !missing {
            // Fully walked parents: inspect the terminal handle-relative.
            // Inspection failures fail closed instead of reading as absent.
            match super::handle::leaf_is_symlink(workspace, path) {
                Ok(true) => return Err(denied()),
                Ok(false) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(GatewayError::Io(error)),
            }
        } else {
            // Missing parents force a pathname pre-check: no handle exists
            // for a not-yet-existing tree, so this advisory check is the
            // only available validation here and is documented as such
            // (E13). The stage/commit leaf checks on the open parent handle
            // enforce the invariant at use time.
            match std::fs::symlink_metadata(&candidate) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(denied());
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(GatewayError::Io(error)),
            }
        }
        if !candidate.starts_with(workspace) {
            return Err(denied());
        }
        // The canonical-workspace-joined lexical path is returned for
        // missing parents so no canonicalization of a not-yet-existing path
        // is required. The write path re-resolves handle-relative before
        // creating.
        Ok(candidate)
    }

    /// Remove a workspace file that was returned by [`Self::resolve_read`].
    /// Unix removes handle-relative from the workspace root with
    /// `O_NOFOLLOW` at every component, so a parent swap cannot redirect
    /// the unlink outside the workspace (E13).
    pub async fn remove_file_resolved(&self, target: &Utf8Path) -> Result<(), GatewayError> {
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
        let Some(file_name) = target.file_name() else {
            return Err(GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            });
        };
        let resolved = canonical_parent.join(file_name);
        if !resolved.starts_with(&workspace) {
            return Err(GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            });
        }
        // Terminal pre-check: advisory on Unix (the handle-relative unlink
        // below enforces it) and the enforcement on non-Unix, where no
        // O_NOFOLLOW unlink exists; both documented here (E13). Inspection
        // failures fail closed instead of reading as absent.
        match std::fs::symlink_metadata(&resolved) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(GatewayError::PathDenied {
                    path: target.to_path_buf(),
                    workspace: self.workspace.clone(),
                });
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(GatewayError::Io(error)),
        }
        #[cfg(unix)]
        {
            let workspace = workspace.clone();
            let resolved = resolved.clone();
            tokio::task::spawn_blocking(move || super::handle::unlink(&workspace, &resolved))
                .await
                .map_err(std::io::Error::other)?
                .map_err(GatewayError::Io)
        }
        #[cfg(not(unix))]
        {
            tokio::fs::remove_file(&resolved)
                .await
                .map_err(GatewayError::Io)
        }
    }

    pub fn workspace(&self) -> &Utf8Path {
        &self.workspace
    }

    /// Removal of orphaned staging files left by a killed process (E13b).
    /// Only regular files (and symlinks, which are removed as links, never
    /// followed) whose name contains `name_fragment` and whose mtime is
    /// older than `ttl` are removed: unique staging names mean a live
    /// concurrent writer's files are always young, so the age gate keeps
    /// the sweep from touching them even without holding a lease. Failures
    /// fail closed: unreadable directories and removal errors propagate
    /// instead of being silently skipped (E13). A missing sweep root is not
    /// an error (nothing to reap).
    ///
    /// Returns the number of entries removed.
    pub fn sweep_orphaned_staging_files(
        dir: &Utf8Path,
        name_fragment: &str,
        ttl: std::time::Duration,
    ) -> Result<usize, std::io::Error> {
        // A clock behind the file mtimes would make everything look young;
        // saturating to "now" fails the sweep closed (removes nothing)
        // instead of wiping fresh files.
        let cutoff = std::time::SystemTime::now()
            .checked_sub(ttl)
            .unwrap_or(std::time::SystemTime::now());
        #[cfg(unix)]
        {
            super::handle::sweep_staging_files(dir, name_fragment, cutoff)
        }
        #[cfg(not(unix))]
        {
            Self::sweep_staging_files_path(dir, name_fragment, cutoff)
        }
    }

    /// Non-Unix pathname sweep: no handle exists that can express
    /// O_NOFOLLOW directory iteration on this platform, so this
    /// pathname walk is the only available validation and is documented
    /// as best-effort here (E13). Symlinked directories are never
    /// descended into (`symlink_metadata` never reports a symlink as a
    /// dir), and symlinks are removed as links, never followed.
    #[cfg(not(unix))]
    fn sweep_staging_files_path(
        dir: &Utf8Path,
        fragment: &str,
        cutoff: std::time::SystemTime,
    ) -> Result<usize, std::io::Error> {
        const MAX_ENTRIES: usize = 10_000;
        const MAX_DEPTH: usize = 32;
        fn walk(
            dir: &std::path::Path,
            depth: usize,
            fragment: &str,
            cutoff: std::time::SystemTime,
            budget: &mut usize,
            removed: &mut usize,
        ) -> Result<(), std::io::Error> {
            if depth > MAX_DEPTH {
                return Err(std::io::Error::other(
                    "staging sweep exceeded the maximum directory depth",
                ));
            }
            let entries = match std::fs::read_dir(dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };
            for entry in entries {
                if *budget == 0 {
                    return Err(std::io::Error::other(
                        "staging sweep exceeded the maximum entry budget",
                    ));
                }
                *budget -= 1;
                // A directory listing is fail-closed: an unreadable entry
                // propagates instead of being silently skipped (E13). Only
                // a file that vanished between listing and inspection is
                // gone already and needs no action.
                let entry = entry?;
                let path = entry.path();
                let metadata = match std::fs::symlink_metadata(&path) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                if metadata.file_type().is_dir() {
                    walk(&path, depth + 1, fragment, cutoff, budget, removed)?;
                    continue;
                }
                let name = path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_default();
                // Anchored staging match: dot-prefixed `.qcg-part-`
                // fragments only, so an unrelated real file merely
                // containing the substring is never reaped (E13).
                if !(name.starts_with('.') && name.contains(fragment)) {
                    continue;
                }
                // An unreadable mtime cannot prove age: the file stays
                // (fail closed, remove nothing) instead of being reaped.
                let old_enough = match metadata.modified() {
                    Ok(mtime) => mtime <= cutoff,
                    Err(_) => false,
                };
                if !old_enough {
                    continue;
                }
                // `remove_file` on a symlink removes the link itself. A
                // file that vanished concurrently is already gone.
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        *removed = removed.checked_add(1).ok_or_else(|| {
                            std::io::Error::other("staging sweep removal count overflowed")
                        })?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        }
        let mut budget = MAX_ENTRIES;
        let mut removed = 0;
        match std::fs::symlink_metadata(dir.as_std_path()) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "staging sweep root is a symbolic link",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error),
        }
        walk(
            dir.as_std_path(),
            0,
            fragment,
            cutoff,
            &mut budget,
            &mut removed,
        )?;
        Ok(removed)
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
        self.write_file_atomic_with_mode(target, bytes, None).await
    }

    /// [`Self::write_file_atomic`] with an explicit owner permission mode
    /// applied to the staged file before the replace (E13). Callers pass an
    /// already-masked mode.
    pub async fn write_file_atomic_with_mode(
        &self,
        target: &Utf8Path,
        bytes: &[u8],
        mode: Option<u32>,
    ) -> Result<(), GatewayError> {
        let (workspace, resolved) = self.resolve_atomic_target(target)?;
        // `workspace` travels only into the Unix handle path below; keep
        // the binding visible without an unused warning on platforms whose
        // fallback never consumes it.
        #[cfg(not(unix))]
        let _ = &workspace;
        // Unix: staging and commit are split around the await. The staging
        // leaf name is generated here so this future owns the staging path
        // across the blocking stage: dropping the future aborts the owned
        // tasks and unlinks the staging file even when a blocking task is
        // detached and still running, and the commit only runs after the
        // stage succeeds without cancellation (E13a/E13b). A single opened
        // parent handle travels from stage to commit: the workspace tree is
        // walked once, and both steps re-check the leaf on the same
        // descriptor with `O_NOFOLLOW`, rejecting symlinks at every
        // component.
        #[cfg(unix)]
        {
            let workspace = workspace.clone();
            let resolved = resolved.clone();
            let bytes = bytes.to_vec();
            let mode = Self::staged_mode(mode);
            // Fail closed on a missing leaf (C-4): `resolve_atomic_target`
            // denies the workspace root itself, so this is unreachable in
            // practice, but a default name would stage under a guessed leaf.
            let leaf = resolved
                .file_name()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "atomic write target has no file name",
                    )
                })?
                .to_string();
            let staging = super::handle::staging_name_for(&leaf);
            let mut guard =
                AsyncStagingGuard::new(workspace.clone(), resolved.clone(), staging.clone());
            let stage_task = tokio::task::spawn_blocking(move || {
                let parent = super::handle::ParentHandle::open(&workspace, &resolved, true)?;
                parent.stage(&staging, mode, |file| {
                    use std::io::Write as _;
                    file.write_all(&bytes)?;
                    Ok(())
                })?;
                Ok::<_, std::io::Error>(parent)
            });
            guard.track(&stage_task);
            let parent = stage_task
                .await
                .map_err(std::io::Error::other)?
                .map_err(GatewayError::Io)?;
            let staging_commit = guard.staging().to_string();
            let commit_task =
                tokio::task::spawn_blocking(move || parent.commit_with_mode(&staging_commit, mode));
            guard.track(&commit_task);
            commit_task
                .await
                .map_err(std::io::Error::other)?
                .map_err(GatewayError::Io)?;
            guard.disarm();
            Ok(())
        }
        #[cfg(not(unix))]
        {
            // Fail closed on a missing leaf (C-4): unreachable via
            // `resolve_atomic_target`, never a guessed default.
            let file_name = resolved
                .file_name()
                .ok_or_else(|| GatewayError::PathDenied {
                    path: target.to_path_buf(),
                    workspace: self.workspace.clone(),
                })?
                .to_string();
            let temporary = resolved.with_file_name(format!(
                ".{file_name}.qcg-part-{}",
                uuid::Uuid::now_v7().as_simple()
            ));
            // Apply the same staged-mode mapping as the stream path so the
            // non-stream fallback honors explicit modes instead of ignoring
            // them (E15).
            let staged = Self::staged_mode(mode);
            // Own the staging file for the whole write: if this future is
            // dropped mid-flight (node timeout, cancellation, shutdown), Drop
            // reclaims it instead of leaving `.qcg-part-*` behind (E13b).
            let mut staging = AsyncStreamGuard::new(temporary.clone());
            let result = async {
                let mut file = tokio::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temporary)
                    .await?;
                tokio::io::AsyncWriteExt::write_all(&mut file, bytes).await?;
                file.sync_all().await?;
                // Non-Unix cannot carry POSIX modes; map the owner-write bit
                // to the read-only flag so an explicit 0o4xx mode is still
                // honored instead of silently ignored (E15).
                let readonly = staged & 0o200 == 0;
                let mut permissions = file.metadata().await?.permissions();
                permissions.set_readonly(readonly);
                tokio::fs::set_permissions(&temporary, permissions).await?;
                drop(file);
                replace_file(&temporary, &resolved).await
            }
            .await;
            if result.is_err() {
                // Reclaim the staging file on failure. Best-effort and
                // documented here: the write error below stays authoritative
                // and the startup sweep reaps anything left behind (E13).
                let _ = tokio::fs::remove_file(&temporary).await;
            }
            if result.is_ok() {
                staging.disarm();
            }
            result.map_err(GatewayError::Io)
        }
    }

    /// [`Self::write_file_atomic`] with a streaming producer: `write` runs
    /// against the staged handle, so large generated content never has to be
    /// buffered in memory and a failed producer leaves the target untouched
    /// (E13 cleanup).
    pub async fn write_file_atomic_stream<T, F>(
        &self,
        target: &Utf8Path,
        mode: Option<u32>,
        write: F,
    ) -> Result<T, GatewayError>
    where
        T: Send + 'static,
        F: FnOnce(&mut std::fs::File) -> std::io::Result<T> + Send + 'static,
    {
        let (workspace, resolved) = self.resolve_atomic_target(target)?;
        #[cfg(unix)]
        {
            let workspace = workspace.clone();
            let resolved = resolved.clone();
            let mode = Self::staged_mode(mode);
            // Fail closed on a missing leaf (C-4): unreachable via
            // `resolve_atomic_target`, never a guessed default.
            let leaf = resolved
                .file_name()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "atomic stream target has no file name",
                    )
                })?
                .to_string();
            let staging = super::handle::staging_name_for(&leaf);
            let mut guard =
                AsyncStagingGuard::new(workspace.clone(), resolved.clone(), staging.clone());
            let stage_task = tokio::task::spawn_blocking(move || {
                let parent = super::handle::ParentHandle::open(&workspace, &resolved, true)?;
                let value = parent.stage(&staging, mode, write)?;
                Ok::<_, std::io::Error>((parent, value))
            });
            guard.track(&stage_task);
            let (parent, value) = stage_task
                .await
                .map_err(std::io::Error::other)?
                .map_err(GatewayError::Io)?;
            let staging_commit = guard.staging().to_string();
            let commit_task =
                tokio::task::spawn_blocking(move || parent.commit_with_mode(&staging_commit, mode));
            guard.track(&commit_task);
            commit_task
                .await
                .map_err(std::io::Error::other)?
                .map_err(GatewayError::Io)?;
            guard.disarm();
            Ok(value)
        }
        #[cfg(not(unix))]
        {
            let _workspace = workspace;
            // Symmetric with the Unix path above: the staging leaf is
            // generated before spawning so this future owns it across the
            // await, and the guard tracks the blocking task (E13).
            let leaf = resolved
                .file_name()
                .ok_or_else(|| GatewayError::PathDenied {
                    path: target.to_path_buf(),
                    workspace: self.workspace.clone(),
                })?
                .to_string();
            let temporary = resolved.with_file_name(format!(
                ".{leaf}.qcg-part-{}",
                uuid::Uuid::now_v7().as_simple()
            ));
            let mut guard = AsyncStreamGuard::new(temporary.clone());
            let stage_task = tokio::task::spawn_blocking(move || {
                write_file_atomic_stream_path(&resolved, &temporary, Self::staged_mode(mode), write)
            });
            guard.track(&stage_task);
            let value = stage_task
                .await
                .map_err(std::io::Error::other)?
                .map_err(GatewayError::Io)?;
            guard.disarm();
            Ok(value)
        }
    }

    /// Validates workspace containment and the terminal non-symlink
    /// invariant shared by every atomic replace. On Unix missing parents
    /// are allowed: they are created handle-relative inside the write, so
    /// validation performs no `mkdir` side effect (E13).
    fn resolve_atomic_target(
        &self,
        target: &Utf8Path,
    ) -> Result<(Utf8PathBuf, Utf8PathBuf), GatewayError> {
        let workspace = dunce::canonicalize(&self.workspace)?;
        let workspace =
            Utf8PathBuf::from_path_buf(workspace).map_err(|_| GatewayError::PathDenied {
                path: target.to_path_buf(),
                workspace: self.workspace.clone(),
            })?;
        let denied = || GatewayError::PathDenied {
            path: target.to_path_buf(),
            workspace: self.workspace.clone(),
        };
        // The target may be built from either the canonical workspace or
        // the configured (possibly symlinked, e.g. /var -> /private/var)
        // workspace prefix. Accept both, then re-anchor on the canonical
        // root so the handle walk below sees one root.
        let relative = target
            .strip_prefix(&workspace)
            .or_else(|_| target.strip_prefix(&self.workspace))
            .map_err(|_| denied())?;
        if relative.as_str().is_empty() {
            return Err(denied());
        }
        for component in relative.components() {
            if !matches!(component, camino::Utf8Component::Normal(_)) {
                return Err(denied());
            }
        }
        #[cfg(unix)]
        {
            // Unix performs no pathname parent walk here: containment above
            // is lexical only, and the single opened parent handle
            // (`ParentHandle`) enforces the symlink invariant
            // handle-relative at stage and commit time. A pathname walk
            // here would be the first of three walks over the same tree
            // (resolve, stage, commit) with a TOCTOU window between each;
            // the single handle replaces all three (E13).
        }
        #[cfg(not(unix))]
        {
            // Non-Unix pathname fallback: no handle exists that can express
            // O_NOFOLLOW parent validation on this platform, so the
            // canonicalized parent check below is the only available
            // validation and is documented as best-effort here (E13).
            let Some(parent) = target.parent() else {
                return Err(denied());
            };
            let canonical_parent = dunce::canonicalize(parent).map_err(|_| denied())?;
            let canonical_parent =
                Utf8PathBuf::from_path_buf(canonical_parent).map_err(|_| denied())?;
            if !canonical_parent.starts_with(&workspace) {
                return Err(denied());
            }
        }
        let resolved = workspace.join(relative);
        // The terminal symlink pre-check below is advisory on Unix (the
        // stage/commit leaf checks on the open parent handle enforce it)
        // and the enforcement on non-Unix, where no O_NOFOLLOW open
        // exists; both are documented here (E13). Inspection failures fail
        // closed instead of reading as absent.
        match std::fs::symlink_metadata(&resolved) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(denied());
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(GatewayError::Io(error)),
        }
        Ok((workspace, resolved))
    }
}

/// Rejects symlinked destination parents before creating or writing a
/// snapshot destination: a planted link in the destination tree would
/// otherwise divert the copy outside the run meta dir (E13). Shared by the
/// Unix handle-relative snapshot path and the non-Unix pathname fallback
/// (which documents its best-effort status at its own site).
///
/// E13n: containment is enforced, not merely documented. Every ancestor
/// symlink is refused fail-closed, including contained diversions three or
/// more levels above the destination that previously passed as
/// self-consistent. Platform links (`/var` to `/private/var`, `/tmp` to
/// `/private/tmp`) are tolerated only when they resolve inside their own
/// canonical parent AND the destination itself was created by the run
/// (parent/grandparent levels, which hold run-created snapshot directories,
/// allow no link at all). Any other link is planted and refused. Missing
/// components will be created fresh below and carry nothing to check;
/// inspection failures other than absence fail closed. Callers re-run this
/// after creating directories (see `create_dest_dir`) to close the
/// plant-between-check-and-create window.
fn reject_symlinked_dest_parents(dest: &Utf8Path) -> std::io::Result<()> {
    let Some(parent) = dest.parent() else {
        return Ok(());
    };
    // Lexical ancestors top-down, from the filesystem root toward the
    // destination parent.
    let mut ancestors = Vec::new();
    let mut current = parent.as_std_path();
    loop {
        ancestors.push(current.to_path_buf());
        match current.parent() {
            Some(next) => current = next,
            None => break,
        }
    }
    ancestors.reverse();
    // Strict levels: the two ancestors closest to the destination hold
    // run-created directories; any link there is planted, contained or not.
    let strict_from = ancestors.len().saturating_sub(2);
    for (index, ancestor) in ancestors.iter().enumerate() {
        let metadata = match std::fs::symlink_metadata(ancestor) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !metadata.file_type().is_symlink() {
            continue;
        }
        if index >= strict_from {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "snapshot destination parent `{}` is a symbolic link",
                    ancestor.display()
                ),
            ));
        }
        // E13n: contained diversions are also refused. Only the two known
        // platform links (`/var` and `/tmp` resolving inside `/private`)
        // are tolerated; any other symlink — escaping or contained — is
        // planted and refused fail-closed.
        let ancestor_str = ancestor.to_string_lossy();
        let is_platform_link =
            ancestor_str == "/var" || ancestor_str == "/tmp" || ancestor_str == "/private";
        let lexical_parent = ancestor.parent().unwrap_or(ancestor);
        let canonical_parent = dunce::canonicalize(lexical_parent).map_err(|error| {
            std::io::Error::other(format!(
                "snapshot destination ancestor `{}` cannot be resolved: {error}",
                lexical_parent.display()
            ))
        })?;
        let canonical_ancestor = dunce::canonicalize(ancestor).map_err(|error| {
            std::io::Error::other(format!(
                "snapshot destination link `{}` cannot be resolved: {error}",
                ancestor.display()
            ))
        })?;
        let contained = canonical_ancestor.starts_with(&canonical_parent);
        let platform_target = canonical_ancestor.starts_with("/private");
        if !(contained && is_platform_link && platform_target) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "snapshot destination parent `{}` is a symbolic link; refusing",
                    ancestor.display()
                ),
            ));
        }
    }
    Ok(())
}

/// Creates a snapshot destination directory with the symlink-escape check
/// on both sides of the creation: the pre-check refuses planted links, the
/// post-check closes the plant-between-check-and-create window (E13).
/// Failures fail closed.
pub(crate) fn create_dest_dir(dest: &Utf8Path) -> std::io::Result<()> {
    reject_symlinked_dest_parents(dest)?;
    std::fs::create_dir_all(dest)?;
    reject_symlinked_dest_parents(&dest.join(".probe"))?;
    Ok(())
}

#[cfg(not(unix))]
async fn replace_file(temporary: &Utf8Path, target: &Utf8Path) -> std::io::Result<()> {
    let temporary = temporary.to_path_buf();
    let target = target.to_path_buf();
    tokio::task::spawn_blocking(move || replace_file_blocking(&temporary, &target))
        .await
        .map_err(std::io::Error::other)?
}

#[cfg(not(unix))]
fn replace_file_blocking(temporary: &Utf8Path, target: &Utf8Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
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
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(temporary.as_std_path(), target.as_std_path())
    }
}

/// Blocking pathname-based streaming replace for non-Unix targets: the
/// ownership guard reclaims a staging file when the producer fails. The
/// staging path is pre-generated by the async caller so the outer future
/// owns it across the `spawn_blocking` await (symmetric with the Unix
/// `AsyncStagingGuard` path); the inner guard covers producer failures,
/// the outer `AsyncStreamGuard` covers outer abandonment (E13).
#[cfg(not(unix))]
fn write_file_atomic_stream_path<T>(
    target: &Utf8Path,
    temporary: &Utf8Path,
    mode: u32,
    write: impl FnOnce(&mut std::fs::File) -> std::io::Result<T>,
) -> std::io::Result<T> {
    let mut staging = AsyncStreamGuard::new(temporary.to_path_buf());
    let result = (|| -> std::io::Result<T> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(temporary.as_std_path())?;
        let value = write(&mut file)?;
        file.sync_all()?;
        // Non-Unix cannot carry POSIX modes; map the owner-write bit to the
        // read-only flag so an explicit 0o4xx mode is still honored instead
        // of silently ignored (E15).
        let mut permissions = file.metadata()?.permissions();
        permissions.set_readonly(mode & 0o200 == 0);
        std::fs::set_permissions(temporary.as_std_path(), permissions)?;
        drop(file);
        replace_file_blocking(temporary, target)?;
        Ok(value)
    })();
    if result.is_err() {
        // Best-effort reclaim documented here: the producer error below
        // stays authoritative and the staging guard also reclaims on drop
        // (E13).
        let _ = std::fs::remove_file(temporary.as_std_path());
    }
    if result.is_ok() {
        staging.disarm();
    }
    result
}

/// Platform-independent staging tests. The non-Unix guard tests below run
/// everywhere the guard compiles; the Unix outer-drop test proves an
/// aborted outer future leaves no `.qcg-part-*` behind on this platform
/// (E13b).
#[cfg(all(test, unix))]
mod unix_staging_tests {
    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04). Best-effort and documented here: test cleanup
    /// cannot propagate from `Drop`.
    struct TempGuard(Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0.as_std_path());
        }
    }

    #[test]
    fn sweep_reaps_only_aged_staging_and_never_follows_symlinks() {
        // E13b: orphaned staging from a killed process is reaped; fresh
        // files (a live writer's) and symlinked trees are untouched.
        use std::time::{Duration, SystemTime};
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-gateway-sweep-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(workspace.join("out")).expect("workspace should be created");
        let aged = workspace.join("out/.data.qcg-part-old");
        let fresh = workspace.join("out/.data.qcg-part-new");
        let keeper = workspace.join("out/data.txt");
        std::fs::write(&aged, b"orphan").expect("aged staging should be written");
        std::fs::write(&fresh, b"live").expect("fresh staging should be written");
        std::fs::write(&keeper, b"real").expect("real file should be written");
        let past = SystemTime::now()
            .checked_sub(Duration::from_secs(7200))
            .expect("past time should exist");
        std::fs::File::options()
            .write(true)
            .open(&aged)
            .expect("aged file should open")
            .set_modified(past)
            .expect("aged file should age");
        let outside = base.join("outside");
        std::fs::create_dir_all(&outside).expect("outside dir should be created");
        std::os::unix::fs::symlink(&outside, workspace.join("out/link"))
            .expect("symlink should be created");
        let removed = FsGateway::sweep_orphaned_staging_files(
            &workspace,
            ".qcg-part-",
            Duration::from_secs(3600),
        )
        .expect("sweep should succeed");
        assert_eq!(removed, 1, "only the aged orphan must be reaped");
        assert!(!aged.exists(), "the aged orphan must be gone");
        assert!(fresh.exists(), "a live writer's fresh staging must survive");
        assert!(keeper.exists(), "real files must survive");
        assert!(
            workspace.join("out/link").is_symlink(),
            "the symlinked tree must not be followed or removed"
        );
    }

    use super::*;
    use qcg_contract::Permissions;

    #[tokio::test]
    async fn aborted_outer_write_leaves_no_staging_and_keeps_target() {
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-gateway-outer-drop-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(workspace.join("out")).expect("test workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        let target = gateway
            .resolve_write("out/data.txt")
            .expect("target should resolve");
        gateway
            .write_file_atomic(&target, b"original")
            .await
            .expect("first write should succeed");
        let gateway_clone = FsGateway::new(workspace.clone(), &permissions);
        let target_clone = target.clone();
        let handle = tokio::spawn(async move {
            gateway_clone
                .write_file_atomic_stream(&target_clone, None, |file| -> std::io::Result<()> {
                    use std::io::Write as _;
                    file.write_all(b"partial")?;
                    std::thread::sleep(std::time::Duration::from_millis(1500));
                    file.write_all(b"-more")?;
                    Ok(())
                })
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        handle.abort();
        let aborted = handle
            .await
            .expect_err("an aborted write must not complete");
        assert!(
            aborted.is_cancelled(),
            "the aborted write must report cancellation"
        );
        // The guard owns the stage/commit tasks and aborts them on drop,
        // then reclaims staging handle-relative: no detached continuation
        // can commit afterwards (E13). The outer abort above drops the
        // guard, so no settle delay is needed before asserting.
        tokio::time::sleep(std::time::Duration::from_millis(1800)).await;
        let leaked: Vec<String> = std::fs::read_dir(workspace.join("out"))
            .expect("out dir should read")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("qcg-part"))
            .collect();
        assert!(
            leaked.is_empty(),
            "an aborted outer write must not leak staging files: {leaked:?}"
        );
        assert_eq!(
            std::fs::read(workspace.join("out/data.txt")).expect("target should read"),
            b"original",
            "an aborted write must not replace the target"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn swapped_parent_cannot_redirect_read_or_write_outside() {
        // E13: a parent directory swapped for a symlink AFTER validation
        // must not redirect the open outside the workspace. Real temp dirs,
        // real symlink plant, gateway entry points (no helper-only proof).
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-gateway-swap-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(workspace.join("out")).expect("test workspace should be created");
        std::fs::write(workspace.join("out/data.txt"), b"original").expect("seed should write");
        let outside = base.join("outside");
        std::fs::create_dir_all(&outside).expect("outside dir should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        permissions.fs_read.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        // Validate (resolve) before the swap, use after it.
        let target = gateway
            .resolve_write("out/data.txt")
            .expect("target should resolve");
        std::fs::rename(workspace.join("out"), workspace.join("out-real"))
            .expect("parent should move");
        std::os::unix::fs::symlink(&outside, workspace.join("out"))
            .expect("symlink plant should succeed");
        // Write through the swapped parent must fail closed.
        gateway
            .write_file_atomic(&target, b"evil")
            .await
            .expect_err("a swapped parent must refuse the write");
        // Read through the swapped parent must fail closed too.
        let _ = gateway
            .open_read("out/data.txt")
            .expect_err("a swapped parent must refuse the read");
        // Nothing may have escaped: outside stays empty and the moved
        // original is untouched.
        assert!(
            std::fs::read_dir(&outside)
                .expect("outside should read")
                .next()
                .is_none(),
            "no write may escape the workspace through the swapped parent"
        );
        assert_eq!(
            std::fs::read(workspace.join("out-real/data.txt")).expect("moved file should read"),
            b"original",
            "the pre-swap bytes must be untouched"
        );
    }

    #[test]
    fn concurrent_overlapping_writes_stay_atomic() {
        // Host parallel-access: barrier-synchronized OS threads write
        // overlapping paths through real temp dirs (no containers). Distinct
        // paths must land exact contents; the shared path must land exactly
        // one complete writer (no torn prefix, no cross-write mixing), and
        // no staging residue may remain.
        use std::sync::{Arc, Barrier};
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-gateway-parallel-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(workspace.join("out")).expect("test workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        // Distinct paths: one target per worker, pre-resolved so the
        // barrier synchronizes the writes themselves.
        const DISTINCT: usize = 8;
        let distinct_targets: Vec<(Utf8PathBuf, Vec<u8>)> = (0..DISTINCT)
            .map(|index| {
                let target = gateway
                    .resolve_write(&format!("out/distinct-{index}.txt"))
                    .expect("distinct target should resolve");
                let bytes = format!("distinct-{index:02}-{}", "x".repeat(4096)).into_bytes();
                (target, bytes)
            })
            .collect();
        let barrier = Arc::new(Barrier::new(DISTINCT));
        let handles: Vec<_> = distinct_targets
            .into_iter()
            .map(|(target, bytes)| {
                let barrier = barrier.clone();
                let gateway = gateway.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("worker runtime should build");
                    runtime
                        .block_on(gateway.write_file_atomic(&target, &bytes))
                        .expect("distinct write should succeed");
                    (target, bytes)
                })
            })
            .collect();
        let mut distinct_results = Vec::new();
        for handle in handles {
            distinct_results.push(handle.join().expect("distinct worker should not panic"));
        }
        for (target, expected) in &distinct_results {
            let actual = std::fs::read(target).expect("distinct result should be readable");
            assert_eq!(
                &actual, expected,
                "distinct path must hold its exact bytes without cross-write"
            );
        }
        // Shared path: every worker writes a different complete payload.
        // The final file must equal exactly one payload at full length.
        const SHARED: usize = 8;
        let shared_target = gateway
            .resolve_write("out/shared.txt")
            .expect("shared target should resolve");
        let payloads: Vec<Vec<u8>> = (0..SHARED)
            .map(|index| format!("shared-{index:02}-{}", "y".repeat(8192)).into_bytes())
            .collect();
        let barrier = Arc::new(Barrier::new(SHARED));
        let handles: Vec<_> = payloads
            .clone()
            .into_iter()
            .map(|bytes| {
                let barrier = barrier.clone();
                let gateway = gateway.clone();
                let target = shared_target.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("worker runtime should build");
                    runtime
                        .block_on(gateway.write_file_atomic(&target, &bytes))
                        .expect("shared write should succeed");
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("shared worker should not panic");
        }
        let final_bytes = std::fs::read(&shared_target).expect("shared result should be readable");
        assert!(
            payloads.iter().any(|payload| payload == &final_bytes),
            "shared path must hold exactly one complete writer, got {} bytes",
            final_bytes.len()
        );
        assert!(
            payloads
                .iter()
                .all(|payload| payload.len() == final_bytes.len()),
            "all payloads share one length so a torn write would differ in content, not length"
        );
        // No torn staging may remain visible after the join.
        let leaked: Vec<String> = std::fs::read_dir(workspace.join("out"))
            .expect("out dir should read")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("qcg-part"))
            .collect();
        assert!(
            leaked.is_empty(),
            "parallel writes must not leak staging files: {leaked:?}"
        );
    }
}

#[cfg(all(test, not(unix)))]
mod tests {
    use super::*;

    #[cfg(not(unix))]
    fn temp_path(name: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-staging-{name}-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8")
    }

    #[cfg(not(unix))]
    #[test]
    fn dropped_staging_guard_reclaims_the_file() {
        // E13b: a future dropped mid-write must not leave `.qcg-part-*`.
        let staging = temp_path("drop");
        std::fs::write(&staging, b"partial").expect("staging should be written");
        {
            let _guard = AsyncStreamGuard::new(staging.clone());
        }
        assert!(
            !staging.exists(),
            "a dropped staging guard must reclaim its file"
        );
    }

    #[cfg(not(unix))]
    #[test]
    fn disarmed_staging_guard_keeps_the_committed_file() {
        let staging = temp_path("commit");
        std::fs::write(&staging, b"complete").expect("staging should be written");
        {
            let mut guard = AsyncStreamGuard::new(staging.clone());
            guard.disarm();
        }
        assert!(
            staging.exists(),
            "a disarmed guard must not remove the committed file"
        );
        let _ = std::fs::remove_file(&staging);
    }
}

#[cfg(test)]
mod staged_mode_tests {
    use super::FsGateway;
    #[test]
    fn staged_mode_masks_world_writable() {
        // E13/E15: staged files never land world-accessible.
        assert_eq!(FsGateway::staged_mode(Some(0o777)), 0o775);
        assert_eq!(FsGateway::staged_mode(Some(0o666)), 0o664);
        assert_eq!(FsGateway::staged_mode(None), 0o600);
    }
}
