//! Workspace filesystem gateway: containment resolution, handle-relative
//! Unix I/O (`super::handle::ParentHandle`), and staging ownership
//! (`AsyncStagingGuard` on Unix / `AsyncStreamGuard` on non-Unix).
//!
//! Responsibility split (C-5, E13): `ParentHandle` is workspace-scoped
//! (never merge with `files` trusted-path helpers without a containment
//! review); a future split moves it to `gateway/handle.rs` and the guards
//! to `gateway/staging.rs` with no behavior change.
use camino::{Utf8Path, Utf8PathBuf};
use contract::{CommandIsolation, Permissions};

use super::error::GatewayError;
#[cfg(unix)]
use super::staging::AsyncStagingGuard;
#[cfg(not(unix))]
use super::staging::AsyncStreamGuard;

/// Q01: effective patch target ceiling for the commit-time base re-check.
/// A patch target larger than this is refused instead of replaced, because a
/// node must never overwrite a file it could not re-read within the same
/// bound it used to read it. This is the supported upper bound for patch and
/// repair targets regardless of higher caller `PatchLimits`: callers asking
/// for more fail fast at admission (see `apply_anchored_patch`) with this
/// ceiling displayed instead of failing later at the re-check.
/// Must equal `policy::MAX_PATCH_TARGET_BYTES`.
const PATCH_COMMIT_RECHECK_BYTES: usize = policy::MAX_PATCH_TARGET_BYTES;

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
// Separation from `files` (E13): `super::handle::ParentHandle` below is
// workspace-scoped (containment resolution plus `AsyncStagingGuard` plus
// mode commit on one open parent fd), while `files::unix_atomic_write`
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

/// Hash-anchored read snapshot returned by the mechanism layer.
/// Policy layers decide windowing budgets and approval; this struct
/// carries only the verified snapshot.
#[derive(Debug, Clone)]
pub struct AnchoredRead {
    pub base_sha256: String,
    pub lines: Vec<files::AnchoredLine>,
    pub total_lines: usize,
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
        mode.map(files::sanitize_mode_bits)
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
    ///
    /// G02/G02-03: deletions hold the same per-file exclusion as writes
    /// and patches, so a patch that observed "present" (or "absent") keeps
    /// that expectation through commit instead of racing a concurrent
    /// delete/recreate silently.
    pub async fn remove_file_resolved(&self, target: &Utf8Path) -> Result<(), GatewayError> {
        let _guard = super::lock_patch_paths(&[target]).await;
        self.remove_file_resolved_under_guard(target, &_guard).await
    }

    /// [`Self::remove_file_resolved`] with the per-file exclusion already
    /// held (G02-04): proof-of-ownership variant for callers that own the
    /// guard across a read-delete or delete-create sequence.
    pub async fn remove_file_resolved_under_guard(
        &self,
        target: &Utf8Path,
        _guard: &super::PatchPathsGuard,
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
                // Anchored staging match: dot-prefixed `.part-`
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
    ///
    /// G02: every unconditional writer holds the process-wide per-file
    /// exclusion for its target, so a concurrent conditional patch either
    /// lands fully before this write (then the patch base check sees it)
    /// or waits until after (then this write lands first and the patch
    /// re-check sees it). Overlap can only serialize, never silently lose
    /// an intervening update.
    pub async fn write_file_atomic_with_mode(
        &self,
        target: &Utf8Path,
        bytes: &[u8],
        mode: Option<u32>,
    ) -> Result<(), GatewayError> {
        let _guard = super::lock_patch_paths(&[target]).await;
        self.write_file_atomic_with_mode_under_guard(target, bytes, mode, &_guard)
            .await
    }

    /// [`Self::write_file_atomic_with_mode`] with the per-file exclusion
    /// already held (G02): patch and repair bodies that own the guard call
    /// this instead of re-locking, because `tokio::sync::Mutex` is not
    /// reentrant and re-acquiring our own shard would self-deadlock
    /// (G02-04). The `_guard` parameter is proof of ownership only.
    pub async fn write_file_atomic_with_mode_under_guard(
        &self,
        target: &Utf8Path,
        bytes: &[u8],
        mode: Option<u32>,
        _guard: &super::PatchPathsGuard,
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
            // H03: the fence is commit authority for both workers below.
            // A cancelled outer future revokes it on drop; each worker
            // refuses to commit once revoked, so a stale worker cannot
            // overwrite a newer writer (spawn_blocking abort is best-effort
            // only and never stops a started worker).
            let stage_fence = guard.fence();
            let stage_task = tokio::task::spawn_blocking(move || {
                if stage_fence.is_revoked() {
                    return Err::<_, std::io::Error>(super::staging::interrupted_error());
                }
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
            // H03: the outer future may have been cancelled while the stage
            // worker ran (guard would already be revoked on the abort path,
            // but a racing cancel lands here): refuse before committing.
            if guard.fence().is_revoked() {
                return Err(GatewayError::Io(super::staging::interrupted_error()));
            }
            let staging_commit = guard.staging().to_string();
            let commit_fence = guard.fence();
            let commit_task = tokio::task::spawn_blocking(move || {
                if commit_fence.is_revoked() {
                    return Err(super::staging::interrupted_error());
                }
                parent.commit_with_mode(&staging_commit, mode)
            });
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
                ".{file_name}.part-{}",
                uuid::Uuid::now_v7().as_simple()
            ));
            // Apply the same staged-mode mapping as the stream path so the
            // non-stream fallback honors explicit modes instead of ignoring
            // them (E15).
            let staged = Self::staged_mode(mode);
            // Own the staging file for the whole write: if this future is
            // dropped mid-flight (node timeout, cancellation, shutdown), Drop
            // reclaims it instead of leaving `.part-*` behind (E13b).
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
                // H03 barrier 3 (byte path): refuse the replace itself when
                // the outer future was cancelled while staging ran, so a
                // stale byte write never overwrites a newer writer's result.
                // The residual window is the atomic rename itself.
                if staging.fence().is_revoked() {
                    let _ = tokio::fs::remove_file(&temporary).await;
                    return Err(super::staging::interrupted_error());
                }
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
    ///
    /// G02: holds the same per-file exclusion as the byte-slice write, so
    /// streaming writers serialize against conditional patches too.
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
        let _guard = super::lock_patch_paths(&[target]).await;
        self.write_file_atomic_stream_under_guard(target, mode, write, &_guard)
            .await
    }

    /// [`Self::write_file_atomic_stream`] with the per-file exclusion
    /// already held (G02/G02-04): proof-of-ownership variant, see
    /// [`Self::write_file_atomic_with_mode_under_guard`].
    pub async fn write_file_atomic_stream_under_guard<T, F>(
        &self,
        target: &Utf8Path,
        mode: Option<u32>,
        write: F,
        _guard: &super::PatchPathsGuard,
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
            // H03: same commit fence as the byte-write path (see above).
            let stage_fence = guard.fence();
            let stage_task = tokio::task::spawn_blocking(move || {
                if stage_fence.is_revoked() {
                    return Err::<_, std::io::Error>(super::staging::interrupted_error());
                }
                let parent = super::handle::ParentHandle::open(&workspace, &resolved, true)?;
                let value = parent.stage(&staging, mode, write)?;
                // H03: a cancel that landed during the producer must not
                // commit its partial staging afterwards.
                if stage_fence.is_revoked() {
                    return Err(super::staging::interrupted_error());
                }
                Ok::<_, std::io::Error>((parent, value))
            });
            guard.track(&stage_task);
            let stage_outcome: Result<(super::handle::ParentHandle, T), GatewayError> = stage_task
                .await
                .map_err(std::io::Error::other)?
                .map_err(GatewayError::Io);
            let (parent, value) = match stage_outcome {
                Ok(pair) => pair,
                Err(error) => {
                    // H03: a revoked producer stages nothing committable;
                    // surface cancellation instead of committing.
                    return Err(error);
                }
            };
            if guard.fence().is_revoked() {
                return Err(GatewayError::Io(super::staging::interrupted_error()));
            }
            let staging_commit = guard.staging().to_string();
            let commit_fence = guard.fence();
            let commit_task = tokio::task::spawn_blocking(move || {
                if commit_fence.is_revoked() {
                    return Err(super::staging::interrupted_error());
                }
                parent.commit_with_mode(&staging_commit, mode)
            });
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
            // H03: the fence is commit authority for the single blocking
            // worker below (see Unix path); abort cannot stop a started
            // worker, so the worker refuses to commit once revoked.
            let leaf = resolved
                .file_name()
                .ok_or_else(|| GatewayError::PathDenied {
                    path: target.to_path_buf(),
                    workspace: self.workspace.clone(),
                })?
                .to_string();
            let temporary = resolved
                .with_file_name(format!(".{leaf}.part-{}", uuid::Uuid::now_v7().as_simple()));
            let mut guard = AsyncStreamGuard::new(temporary.clone());
            let fence = guard.fence();
            let stage_task = tokio::task::spawn_blocking(move || {
                write_file_atomic_stream_path(
                    &resolved,
                    &temporary,
                    Self::staged_mode(mode),
                    fence,
                    write,
                )
            });
            guard.track(&stage_task);
            let value = stage_task
                .await
                .map_err(std::io::Error::other)?
                .map_err(GatewayError::Io)?;
            // H03: a cancel that landed while the worker ran must surface
            // here (the worker already refused to commit when revoked).
            if guard.fence().is_revoked() {
                return Err(GatewayError::Io(super::staging::interrupted_error()));
            }
            guard.disarm();
            Ok(value)
        }
    }

    /// Hash-anchored read over an already-resolved path.
    ///
    /// Mechanism only: containment and handle-relative open are enforced,
    /// but permission checks happened at `resolve_read` time. Returns the
    /// file base hash plus the requested window of anchored lines.
    pub fn read_anchored(
        &self,
        target: &Utf8Path,
        offset: usize,
        limit: usize,
        max_bytes: Option<usize>,
    ) -> Result<AnchoredRead, GatewayError> {
        use std::io::Read as _;
        let file = self.open_read_resolved(target)?;
        let cap = max_bytes.map_or(u64::MAX, |value| value.saturating_add(1) as u64);
        let mut bytes = Vec::new();
        file.take(cap).read_to_end(&mut bytes)?;
        if let Some(value) = max_bytes
            && bytes.len() > value
        {
            return Err(GatewayError::Io(std::io::Error::other(format!(
                "file `{target}` exceeds {value} bytes"
            ))));
        }
        let text = String::from_utf8(bytes).map_err(|_| GatewayError::PatchNotUtf8 {
            path: target.to_string(),
        })?;
        let total_lines = files::split_lines(&text).len();
        let (base_sha256, window) = files::read_window(&text, offset, limit);
        Ok(AnchoredRead {
            base_sha256,
            lines: window,
            total_lines,
        })
    }

    /// Hash-anchored modify over an already-resolved path.
    ///
    /// Mechanism only: reads the current snapshot through the
    /// handle-relative open, verifies `expected_base` and every anchor,
    /// then commits atomically. Policy (permissions, approval, budgets,
    /// retry) is resolved by the caller before invoking this method.
    /// The read-verify-write sequence holds the process-wide per-file
    /// exclusion, so a concurrent in-process writer either lands fully
    /// before the read (then the base check sees it) or waits until after
    /// the commit (then its own base check sees this write): overlap can
    /// only surface as an explicit base mismatch, never a silent merge.
    /// Cross-process writers are outside this boundary and still rely on
    /// base mismatch to fail closed.
    ///
    /// G06: a content edit preserves the target's existing sanitized mode
    /// (executability survives a one-line patch); brand-new files keep the
    /// owner-only default. Setuid/setgid/sticky bits are never preserved.
    pub async fn apply_anchored_patch(
        &self,
        target: &Utf8Path,
        expected_base: Option<&str>,
        edits: Vec<files::PatchEdit>,
        limits: files::PatchLimits,
    ) -> Result<files::PatchOutcome, GatewayError> {
        use std::io::Read as _;
        // Q01: fail fast when the caller asks for more than the effective
        // patch ceiling instead of failing later at the commit re-check.
        if limits.max_result_bytes > PATCH_COMMIT_RECHECK_BYTES {
            return Err(files::AnchoredPatchError::ResultTooLarge {
                bytes: limits.max_result_bytes,
                limit: PATCH_COMMIT_RECHECK_BYTES,
            }
            .into());
        }
        let _guard = super::lock_patch_paths(&[target]).await;
        let current = {
            let file = self.open_read_resolved(target)?;
            let cap = limits.max_result_bytes.saturating_add(1) as u64;
            let mut bytes = Vec::new();
            file.take(cap).read_to_end(&mut bytes)?;
            if bytes.len() > limits.max_result_bytes {
                return Err(files::AnchoredPatchError::ResultTooLarge {
                    bytes: bytes.len(),
                    limit: limits.max_result_bytes,
                }
                .into());
            }
            String::from_utf8(bytes).map_err(|_| GatewayError::PatchNotUtf8 {
                path: target.to_string(),
            })?
        };
        // G06: capture the pre-edit mode while holding the exclusion, so a
        // concurrent chmod racing the commit cannot divert the inherited
        // mode silently; the commit-time re-check below still gates content.
        let preserved_mode = Self::existing_sanitized_mode(target);
        let outcome = files::apply_anchored_patch(&current, expected_base, &edits, limits)?;
        // Commit-time re-check. The lock above only excludes writers inside
        // this process, so a second qcg process or a direct workspace edit can
        // still land between the read and the replace. Re-reading the leaf
        // under the same exclusion refuses that instead of silently
        // overwriting it. The residual window is the rename itself, which no
        // portable primitive can close, so the base digest is what a caller
        // re-checks after a failure.
        self.require_unchanged_since(target, Some(&files::base_sha256(&current)))
            .await?;
        self.write_file_atomic_with_mode_under_guard(
            target,
            outcome.new_text.as_bytes(),
            preserved_mode,
            &_guard,
        )
        .await?;
        Ok(outcome)
    }

    /// Sanitized mode of the existing target, if it is a regular file
    /// (G06): content edits inherit executability (0755 stays executable)
    /// while setuid/setgid/sticky/world-writable bits are stripped by
    /// [`files::sanitize_mode_bits`]. Returns `None` for absent targets
    /// (callers fall back to the owner-only default) and for non-Unix
    /// platforms (no POSIX bits to inherit). Symlinks are never followed:
    /// a symlinked leaf is refused elsewhere, and here it reads as "no
    /// inheritable mode" instead of the link target's mode.
    fn existing_sanitized_mode(target: &Utf8Path) -> Option<u32> {
        #[cfg(unix)]
        {
            match std::fs::symlink_metadata(target.as_std_path()) {
                Ok(meta) if meta.file_type().is_file() => {
                    Some(files::sanitized_metadata_mode(&meta))
                }
                _ => None,
            }
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            None
        }
    }

    /// Content-edit write with the per-file exclusion already held that
    /// preserves the target's existing sanitized mode (G06). Repair and
    /// other edit-not-replace paths call this instead of the unconditional
    /// `write_file_atomic_*`: a 0755 script stays executable after its
    /// content is repaired, while brand-new files keep the owner-only
    /// default. Refused edits never reach this call, so content and mode
    /// stay jointly untouched on rejection (G06-03).
    pub async fn write_file_atomic_preserving_mode_under_guard(
        &self,
        target: &Utf8Path,
        bytes: &[u8],
        _guard: &super::PatchPathsGuard,
    ) -> Result<(), GatewayError> {
        let preserved = Self::existing_sanitized_mode(target);
        self.write_file_atomic_with_mode_under_guard(target, bytes, preserved, _guard)
            .await
    }

    /// Current content digest of a patch target, or `None` when it does not
    /// exist. Pairs with [`Self::require_unchanged_since`]: observe before
    /// patching, verify immediately before committing.
    pub async fn observe_patch_state(
        &self,
        target: &Utf8Path,
    ) -> Result<Option<String>, GatewayError> {
        use std::io::Read as _;
        let file = match self.open_read_resolved(target) {
            Ok(file) => file,
            Err(GatewayError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let mut bytes = Vec::new();
        file.take((PATCH_COMMIT_RECHECK_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(GatewayError::from)?;
        if bytes.len() > PATCH_COMMIT_RECHECK_BYTES {
            // Too large to hash within the commit bound: refuse rather than
            // claim a base this node never read.
            return Err(files::AnchoredPatchError::ResultTooLarge {
                bytes: bytes.len(),
                limit: PATCH_COMMIT_RECHECK_BYTES,
            }
            .into());
        }
        // The patch path hashes decoded text, so the observation must decode
        // the same way: a file that is no longer valid UTF-8 has drifted, not
        // silently hashed differently.
        let text = String::from_utf8(bytes).map_err(|_| GatewayError::PatchNotUtf8 {
            path: target.to_string(),
        })?;
        Ok(Some(files::base_sha256(&text)))
    }

    /// Refuses when `target` no longer matches the state a patch was computed
    /// from: `Some(digest)` requires that exact content, `None` requires the
    /// file to still be absent.
    ///
    /// The lock held by [`Self::apply_anchored_patch`] only excludes writers
    /// inside this process, so a second qcg process or a direct workspace edit
    /// can still land between the read and the replace; re-reading the leaf here
    /// surfaces that as a base mismatch instead of a silent overwrite. The
    /// residual window is the rename itself, which no portable primitive closes.
    pub async fn require_unchanged_since(
        &self,
        target: &Utf8Path,
        expected: Option<&str>,
    ) -> Result<(), GatewayError> {
        let observed = self.observe_patch_state(target).await?;
        if observed.as_deref() != expected {
            return Err(files::AnchoredPatchError::BaseMismatch {
                expected: expected.unwrap_or("missing").to_string(),
                actual: observed.unwrap_or_else(|| "missing".to_string()),
            }
            .into());
        }
        Ok(())
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
///
/// H03: `fence` is commit authority shared with the outer future. The
/// worker checks it before creating, after the producer, and immediately
/// before the replace: a cancelled outer future revokes the fence on drop,
/// and the worker refuses to commit instead of overwriting a newer writer
/// (abort cannot stop this started worker). On refusal the staging file is
/// reclaimed and `Interrupted` is returned.
#[cfg(not(unix))]
fn write_file_atomic_stream_path<T>(
    target: &Utf8Path,
    temporary: &Utf8Path,
    mode: u32,
    fence: super::staging::CommitFence,
    write: impl FnOnce(&mut std::fs::File) -> std::io::Result<T>,
) -> std::io::Result<T> {
    let mut staging = AsyncStreamGuard::new(temporary.to_path_buf());
    let result = (|| -> std::io::Result<T> {
        // H03 barrier 1: worker started but outer already gone.
        if fence.is_revoked() {
            return Err(super::staging::interrupted_error());
        }
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(temporary.as_std_path())?;
        let value = write(&mut file)?;
        // H03 barrier 2: producer ran while the outer was cancelled.
        if fence.is_revoked() {
            drop(file);
            let _ = std::fs::remove_file(temporary.as_std_path());
            return Err(super::staging::interrupted_error());
        }
        file.sync_all()?;
        // Non-Unix cannot carry POSIX modes; map the owner-write bit to the
        // read-only flag so an explicit 0o4xx mode is still honored instead
        // of silently ignored (E15).
        let mut permissions = file.metadata()?.permissions();
        permissions.set_readonly(mode & 0o200 == 0);
        std::fs::set_permissions(temporary.as_std_path(), permissions)?;
        drop(file);
        // H03 barrier 3: refuse the replace itself when revoked, so a stale
        // worker never overwrites the newer writer's committed result.
        if fence.is_revoked() {
            let _ = std::fs::remove_file(temporary.as_std_path());
            return Err(super::staging::interrupted_error());
        }
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

/// H03 Windows streaming barriers (runs on Windows CI): the real
/// non-Unix helper with barriers at worker-start/pre-create, producer, and
/// pre-replace. Cancelling the outer future at each barrier must never
/// commit afterwards, and a newer writer's result must survive (H03-01..03).
/// Unix behavior is covered by `h03_cancelled_stream_never_commits_stale_result`;
/// Unix stage/commit split is covered by the existing outer-drop test (H03-04).
#[cfg(all(test, windows))]
mod h03_windows_stream_tests {
    use super::*;
    use contract::Permissions;

    #[tokio::test]
    async fn windows_cancelled_stream_never_commits_stale_result() {
        let base = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("gateway-h03w-{}", uuid::Uuid::now_v7().as_simple())),
        )
        .expect("temporary directory path must be utf-8");
        struct TempGuard(Utf8PathBuf);
        impl Drop for TempGuard {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(self.0.as_std_path());
            }
        }
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(workspace.join("out")).expect("workspace");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        let target = gateway.resolve_write("out/data.txt").expect("target");
        gateway
            .write_file_atomic(&target, b"original")
            .await
            .expect("seed");
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
        let entered_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(entered_tx)));
        let release = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release_clone = release.clone();
        let gateway_clone = gateway.clone();
        let target_clone = target.clone();
        let stale = tokio::spawn(async move {
            gateway_clone
                .write_file_atomic_stream(&target_clone, None, move |file| -> std::io::Result<()> {
                    use std::io::Write as _;
                    file.write_all(b"stale-cancelled")?;
                    if let Some(tx) = entered_tx.lock().expect("lock").take() {
                        let _ = tx.send(());
                    }
                    while !release_clone.load(std::sync::atomic::Ordering::SeqCst) {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    file.write_all(b"-tail")?;
                    Ok(())
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), entered_rx)
            .await
            .expect("producer should enter")
            .expect("signal");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        stale.abort();
        let _ = stale.await;
        release.store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        gateway
            .write_file_atomic(&target, b"new-successful-writer")
            .await
            .expect("new writer");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            std::fs::read(workspace.join("out/data.txt")).expect("target"),
            b"new-successful-writer",
            "H03: cancelled Windows worker must not overwrite the newer result"
        );
    }
}

/// H03 commit-fence unit tests (platform-independent): the fence is the
/// commit authority shared by outer futures and blocking workers.
#[cfg(test)]
mod commit_fence_tests {
    use super::super::staging::CommitFence;

    #[test]
    fn fence_starts_armed_and_revokes_once() {
        let fence = CommitFence::new();
        assert!(!fence.is_revoked(), "a fresh fence must allow commit");
        fence.revoke();
        assert!(fence.is_revoked(), "revoke must refuse later commits");
        fence.revoke();
        assert!(fence.is_revoked(), "revoke must be idempotent");
    }

    #[test]
    fn cloned_fence_shares_revocation() {
        let fence = CommitFence::new();
        let worker = fence.clone();
        assert!(!worker.is_revoked());
        fence.revoke();
        assert!(
            worker.is_revoked(),
            "the worker clone must observe the outer revoke"
        );
    }
}

/// Platform-independent staging tests. The non-Unix guard tests below run
/// everywhere the guard compiles; the Unix outer-drop test proves an
/// aborted outer future leaves no `.part-*` behind on this platform
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
            "gateway-sweep-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(workspace.join("out")).expect("workspace should be created");
        let aged = workspace.join("out/.data.part-old");
        let fresh = workspace.join("out/.data.part-new");
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
            ".part-",
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
    use contract::Permissions;

    #[tokio::test]
    async fn h03_cancelled_stream_never_commits_stale_result() {
        // H03-01/H03-02 (Unix analogue): a stream whose outer future is
        // cancelled mid-producer must never commit afterwards, and a newer
        // writer's result must survive the stale worker finishing late.
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let base = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("gateway-h03-{}", uuid::Uuid::now_v7().as_simple())),
        )
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(workspace.join("out")).expect("workspace");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        let target = gateway.resolve_write("out/data.txt").expect("target");
        gateway
            .write_file_atomic(&target, b"original")
            .await
            .expect("seed");
        // Async-aware rendezvous: never block the executor thread (the
        // `#[tokio::test]` current-thread runtime would deadlock on a
        // `std::sync::Barrier`).
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
        let entered_tx = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
        let release = Arc::new(AtomicBool::new(false));
        let release_clone = release.clone();
        let gateway_clone = gateway.clone();
        let target_clone = target.clone();
        let stale = tokio::spawn(async move {
            gateway_clone
                .write_file_atomic_stream(&target_clone, None, move |file| -> std::io::Result<()> {
                    use std::io::Write as _;
                    file.write_all(b"stale-cancelled")?;
                    if let Some(tx) = entered_tx.lock().expect("entered lock").take() {
                        let _ = tx.send(());
                    }
                    // Producer blocks until the outer is cancelled below.
                    while !release_clone.load(Ordering::SeqCst) {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    file.write_all(b"-tail")?;
                    Ok(())
                })
                .await
        });
        // Wait until the producer is inside the worker, then cancel outer.
        tokio::time::timeout(std::time::Duration::from_secs(10), entered_rx)
            .await
            .expect("producer should enter")
            .expect("entered signal");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        stale.abort();
        let _ = stale.await;
        // The stale worker may still be running (abort never stops a started
        // blocking worker): let it finish, then run the newer writer.
        release.store(true, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        gateway
            .write_file_atomic(&target, b"new-successful-writer")
            .await
            .expect("new writer should succeed");
        // Give any late stale commit a chance to (incorrectly) land.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            std::fs::read(workspace.join("out/data.txt")).expect("target"),
            b"new-successful-writer",
            "H03-02: a cancelled worker must not overwrite the newer result"
        );
        let leaked: Vec<String> = std::fs::read_dir(workspace.join("out"))
            .expect("out dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("part"))
            .collect();
        assert!(leaked.is_empty(), "no staging may leak: {leaked:?}");
    }

    #[tokio::test]
    async fn aborted_outer_write_leaves_no_staging_and_keeps_target() {
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "gateway-outer-drop-{}",
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
            .filter(|name| name.contains("part"))
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
        let base = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("gateway-swap-{}", uuid::Uuid::now_v7().as_simple())),
        )
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
            "gateway-parallel-{}",
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
            .filter(|name| name.contains("part"))
            .collect();
        assert!(
            leaked.is_empty(),
            "parallel writes must not leak staging files: {leaked:?}"
        );
    }

    #[tokio::test]
    async fn concurrent_same_file_patches_fail_closed_on_base_drift() {
        // Two base-pinned patches race on one file: the process-wide
        // per-file exclusion serializes the read-verify-write sequences,
        // so the loser deterministically observes the winner's commit as
        // an explicit base mismatch instead of merging silently.
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "gateway-patch-race-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("test workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        permissions.fs_read.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        let seed = "alpha\nbeta\n";
        std::fs::write(workspace.join("notes.txt"), seed).expect("seed should be written");
        let target = gateway
            .resolve_write("notes.txt")
            .expect("target should resolve");
        let base_hash = files::base_sha256(seed);
        let annotated = files::annotate(seed);
        let limits = files::PatchLimits {
            max_edits: 8,
            max_patch_bytes: 4096,
            max_result_bytes: 65536,
        };
        let edit_for = |line: &str| {
            vec![files::PatchEdit {
                op: files::PatchOp::Replace,
                anchor_line: annotated[0].line_no,
                anchor_hash: annotated[0].hash.clone(),
                lines: vec![line.to_string()],
            }]
        };
        let edits_a = edit_for("winner-a");
        let edits_b = edit_for("winner-b");
        let first = {
            let gateway = gateway.clone();
            let target = target.clone();
            let base_hash = base_hash.clone();
            tokio::spawn(async move {
                gateway
                    .apply_anchored_patch(&target, Some(&base_hash), edits_a, limits)
                    .await
            })
        };
        let second = {
            let gateway = gateway.clone();
            let target = target.clone();
            tokio::spawn(async move {
                gateway
                    .apply_anchored_patch(&target, Some(&base_hash), edits_b, limits)
                    .await
            })
        };
        let (first, second) = tokio::join!(first, second);
        let first = first.expect("first patch task should not panic");
        let second = second.expect("second patch task should not panic");
        let succeeded = [&first, &second]
            .iter()
            .filter(|outcome| outcome.is_ok())
            .count();
        assert_eq!(
            succeeded, 1,
            "exactly one racy patch must win: {first:?} / {second:?}"
        );
        for outcome in [&first, &second] {
            if let Err(error) = outcome {
                assert!(
                    matches!(
                        error,
                        GatewayError::AnchoredPatch(files::AnchoredPatchError::BaseMismatch { .. })
                    ),
                    "the loser must fail on base drift, got: {error:?}"
                );
            }
        }
        let final_text = std::fs::read_to_string(&target).expect("patched file should be readable");
        assert!(
            final_text == "winner-a\nbeta\n" || final_text == "winner-b\nbeta\n",
            "the file must hold exactly one complete winner: {final_text:?}"
        );
    }

    #[tokio::test]
    async fn q01_patch_limits_above_ceiling_fail_fast() {
        // Q01: a caller asking for more than the effective patch ceiling
        // fails fast with the ceiling displayed, instead of failing later
        // at the commit re-check.
        let base = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("gateway-q01-{}", uuid::Uuid::now_v7().as_simple())),
        )
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(workspace.join("out")).expect("workspace");
        let mut permissions = contract::Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        let target = gateway.resolve_write("out/data.txt").expect("target");
        std::fs::write(workspace.join("out/data.txt"), b"hello").expect("seed");
        let anchor = files::anchor_for(1, "hello");
        let (_, hash) = files::parse_anchor(&anchor).expect("anchor");
        let over = files::PatchLimits {
            max_edits: 8,
            max_patch_bytes: 1024,
            max_result_bytes: policy::MAX_PATCH_TARGET_BYTES + 1,
        };
        let error = gateway
            .apply_anchored_patch(
                &target,
                Some(&files::base_sha256("hello")),
                vec![files::PatchEdit {
                    op: files::PatchOp::Replace,
                    anchor_line: 1,
                    anchor_hash: hash,
                    lines: vec!["hi".into()],
                }],
                over,
            )
            .await
            .expect_err("over-ceiling limits must fail fast");
        assert!(
            error
                .to_string()
                .contains(&policy::MAX_PATCH_TARGET_BYTES.to_string()),
            "the ceiling must be displayed: {error}"
        );
    }

    #[tokio::test]
    async fn commit_time_recheck_refuses_a_foreign_writer() {
        // The per-file exclusion only covers writers inside this process, so a
        // second qcg process or a direct workspace edit must surface at commit
        // time as a base mismatch instead of a silent overwrite.
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "gateway-patch-commit-recheck-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("test workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_read.push("workspace".into());
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(workspace, &permissions);
        let target = gateway
            .resolve_write("notes.txt")
            .expect("target should resolve");
        let mismatch = |error: GatewayError| {
            assert!(
                matches!(
                    error,
                    GatewayError::AnchoredPatch(files::AnchoredPatchError::BaseMismatch { .. })
                ),
                "must report base drift, got: {error:?}"
            );
        };

        std::fs::write(&target, "alpha\n").expect("seed should be written");
        let seed = files::base_sha256("alpha\n");
        gateway
            .require_unchanged_since(&target, Some(&seed))
            .await
            .expect("an unchanged file must pass");

        std::fs::write(&target, "foreign\n").expect("foreign write should land");
        mismatch(
            gateway
                .require_unchanged_since(&target, Some(&seed))
                .await
                .expect_err("a changed file must fail the re-check"),
        );
        assert_eq!(
            std::fs::read_to_string(&target).expect("file should be readable"),
            "foreign\n",
            "the re-check must not write anything itself"
        );

        // A target that disappeared after the observation is drift too, and an
        // absent target observed as absent still matches.
        std::fs::remove_file(&target).expect("file should be removed");
        mismatch(
            gateway
                .require_unchanged_since(&target, Some(&seed))
                .await
                .expect_err("a removed file must fail the re-check"),
        );
        gateway
            .require_unchanged_since(&target, None)
            .await
            .expect("an absent target observed as absent must pass");
    }

    #[tokio::test]
    async fn g02_repair_style_verify_then_preserving_write_serializes() {
        // G02-02: the llm.repair commit shape (verify the shown base under
        // the guard, then a mode-preserving write through `_under_guard`)
        // races an ordinary writer on the same target: the repair either
        // serializes first (write lands after) or observes the write and
        // refuses explicitly. Both sides never report success while losing
        // the intervening update.
        use super::*;
        use contract::Permissions;
        for _ in 0..20 {
            let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
                "gateway-g02-repair-{}",
                uuid::Uuid::now_v7().as_simple()
            )))
            .expect("temporary directory path must be utf-8");
            let _temp_guard = TempGuard(base.clone());
            let workspace = base.join("workspace");
            std::fs::create_dir_all(&workspace).expect("workspace should be created");
            let mut permissions = Permissions::default();
            permissions.fs_write.push("workspace".into());
            permissions.fs_read.push("workspace".into());
            let gateway = FsGateway::new(workspace.clone(), &permissions);
            std::fs::write(workspace.join("notes.txt"), "shown\n").expect("seed");
            let target = gateway.resolve_write("notes.txt").expect("target");
            let shown_base = files::base_sha256("shown\n");
            let gateway_repair = gateway.clone();
            let target_repair = target.clone();
            let repair_task = tokio::spawn(async move {
                // Repair shape: lock, re-read, verify shown base, commit
                // preserving the mode without re-locking.
                let guard = super::super::lock_patch_paths(&[target_repair.as_path()]).await;
                let current = {
                    use std::io::Read as _;
                    let file = gateway_repair.open_read_resolved(&target_repair)?;
                    let mut bytes = Vec::new();
                    file.take(65536).read_to_end(&mut bytes)?;
                    String::from_utf8(bytes).map_err(|_| GatewayError::PatchNotUtf8 {
                        path: target_repair.to_string(),
                    })?
                };
                if files::base_sha256(&current) != shown_base {
                    return Err(GatewayError::AnchoredPatch(
                        files::AnchoredPatchError::BaseMismatch {
                            expected: shown_base,
                            actual: files::base_sha256(&current),
                        },
                    ));
                }
                gateway_repair
                    .write_file_atomic_preserving_mode_under_guard(
                        &target_repair,
                        b"repaired\n",
                        &guard,
                    )
                    .await
            });
            let gateway_write = gateway.clone();
            let target_write = target.clone();
            let write_task = tokio::spawn(async move {
                tokio::task::yield_now().await;
                gateway_write
                    .write_file_atomic(&target_write, b"ordinary\n")
                    .await
            });
            let (repair_outcome, write_outcome) = tokio::join!(repair_task, write_task);
            let repair_outcome = repair_outcome.expect("repair task should not panic");
            write_outcome
                .expect("write task should not panic")
                .expect("the ordinary write must succeed");
            let final_text = std::fs::read_to_string(&target).expect("file should be readable");
            match repair_outcome {
                Ok(()) => {
                    // Repair serialized first; the ordinary write landed after.
                    assert_eq!(final_text, "ordinary\n", "serialized write must win last");
                }
                Err(error) => {
                    assert!(
                        matches!(
                            error,
                            GatewayError::AnchoredPatch(
                                files::AnchoredPatchError::BaseMismatch { .. }
                            )
                        ),
                        "a repair racing a committed write must refuse, got: {error:?}"
                    );
                    assert_eq!(final_text, "ordinary\n", "the committed write must survive");
                }
            }
        }
    }

    #[tokio::test]
    async fn g02_patch_vs_ordinary_write_serializes_without_loss() {
        // G02-01: the ordinary writer now holds the same per-file exclusion
        // as the patch, so a write racing a patch serializes instead of
        // being silently lost with both sides reporting success. Twenty
        // iterations race a pinned patch against an unconditional write on
        // one file: every round ends with exactly one complete winner and
        // the loser (when it is the patch) reports an explicit base
        // mismatch.
        use super::*;
        use contract::Permissions;
        for _ in 0..20 {
            let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
                "gateway-g02-race-{}",
                uuid::Uuid::now_v7().as_simple()
            )))
            .expect("temporary directory path must be utf-8");
            let _temp_guard = TempGuard(base.clone());
            let workspace = base.join("workspace");
            std::fs::create_dir_all(&workspace).expect("workspace should be created");
            let mut permissions = Permissions::default();
            permissions.fs_write.push("workspace".into());
            permissions.fs_read.push("workspace".into());
            let gateway = FsGateway::new(workspace.clone(), &permissions);
            std::fs::write(workspace.join("notes.txt"), "alpha\nbeta\n")
                .expect("seed should be written");
            let target = gateway
                .resolve_write("notes.txt")
                .expect("target should resolve");
            let base_hash = files::base_sha256("alpha\nbeta\n");
            let annotated = files::annotate("alpha\nbeta\n");
            let limits = files::PatchLimits {
                max_edits: 8,
                max_patch_bytes: 4096,
                max_result_bytes: 65536,
            };
            let patch_edits = vec![files::PatchEdit {
                op: files::PatchOp::Replace,
                anchor_line: annotated[0].line_no,
                anchor_hash: annotated[0].hash.clone(),
                lines: vec!["patched".to_string()],
            }];
            let gateway_patch = gateway.clone();
            let target_patch = target.clone();
            let base_hash_patch = base_hash.clone();
            let patch_task = tokio::spawn(async move {
                gateway_patch
                    .apply_anchored_patch(
                        &target_patch,
                        Some(&base_hash_patch),
                        patch_edits,
                        limits,
                    )
                    .await
            });
            let gateway_write = gateway.clone();
            let target_write = target.clone();
            let write_task = tokio::spawn(async move {
                // Small yield so the patch usually wins the read first and
                // the write lands inside its read-verify-commit window: the
                // exclusion must serialize it after the commit.
                tokio::task::yield_now().await;
                gateway_write
                    .write_file_atomic(&target_write, b"ordinary-winner\nbeta\n")
                    .await
            });
            let (patch_outcome, write_outcome) = tokio::join!(patch_task, write_task);
            let patch_outcome = patch_outcome.expect("patch task should not panic");
            write_outcome
                .expect("write task should not panic")
                .expect("the ordinary write must succeed");
            let final_text =
                std::fs::read_to_string(&target).expect("patched file should be readable");
            match patch_outcome {
                Ok(_) => {
                    // Patch won or serialized after the write with a fresh
                    // base? No: the patch pinned the stale base, so if the
                    // write committed inside its window the patch must have
                    // failed. A patch success means it serialized first and
                    // the write landed after: the file holds the write.
                    assert_eq!(
                        final_text, "ordinary-winner\nbeta\n",
                        "when the patch commits first the serialized write must win last"
                    );
                }
                Err(error) => {
                    assert!(
                        matches!(
                            error,
                            GatewayError::AnchoredPatch(
                                files::AnchoredPatchError::BaseMismatch { .. }
                            )
                        ),
                        "a patch racing a committed write must fail on base drift, got: {error:?}"
                    );
                    assert_eq!(
                        final_text, "ordinary-winner\nbeta\n",
                        "the committed write must survive when the patch loses"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn g02_guarded_writer_does_not_self_deadlock_and_keeps_parallelism() {
        // G02-04: a holder of the per-file guard commits through the
        // `_under_guard` variant without re-locking (tokio Mutex is not
        // reentrant), while unrelated targets still proceed in parallel.
        use super::*;
        use contract::Permissions;
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "gateway-g02-guard-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        permissions.fs_read.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        std::fs::write(workspace.join("a.txt"), "a\n").expect("seed a");
        std::fs::write(workspace.join("b.txt"), "b\n").expect("seed b");
        let target_a = gateway.resolve_write("a.txt").expect("target a");
        let target_b = gateway.resolve_write("b.txt").expect("target b");
        // Guarded commit on the same target must not self-deadlock.
        let guard = super::super::lock_patch_paths(&[target_a.as_path()]).await;
        gateway
            .write_file_atomic_with_mode_under_guard(&target_a, b"guarded\n", None, &guard)
            .await
            .expect("guarded write must not deadlock");
        drop(guard);
        assert_eq!(
            std::fs::read_to_string(&target_a).expect("a should read"),
            "guarded\n"
        );
        // Distinct targets proceed concurrently through the public locked
        // path: 8 parallel writes to 8 files all land exactly.
        let mut handles = Vec::new();
        for index in 0..8 {
            let gateway = gateway.clone();
            let target = gateway
                .resolve_write(&format!("p-{index}.txt"))
                .expect("parallel target should resolve");
            handles.push(tokio::spawn(async move {
                gateway
                    .write_file_atomic(&target, format!("payload-{index}\n").as_bytes())
                    .await
                    .expect("parallel write should succeed");
            }));
        }
        for handle in handles {
            handle.await.expect("parallel writer should not panic");
        }
        // Existing patch-vs-patch serialization still holds (regression).
        let target = gateway.resolve_write("b.txt").expect("target b");
        let _ = target_b;
        let base_hash = files::base_sha256("b\n");
        let annotated = files::annotate("b\n");
        let limits = files::PatchLimits {
            max_edits: 8,
            max_patch_bytes: 4096,
            max_result_bytes: 65536,
        };
        let edit_for = |line: &str| {
            vec![files::PatchEdit {
                op: files::PatchOp::Replace,
                anchor_line: annotated[0].line_no,
                anchor_hash: annotated[0].hash.clone(),
                lines: vec![line.to_string()],
            }]
        };
        let (first, second) = tokio::join!(
            gateway.apply_anchored_patch(&target, Some(&base_hash), edit_for("w1"), limits),
            gateway.apply_anchored_patch(&target, Some(&base_hash), edit_for("w2"), limits),
        );
        assert!(
            first.is_ok() ^ second.is_ok(),
            "exactly one racy patch must win: {first:?} / {second:?}"
        );
    }

    #[tokio::test]
    async fn g02_delete_recreate_keeps_presence_expectation() {
        // G02-03: deletions participate in the same exclusion, so a patch
        // that observed "present" cannot silently commit over a concurrent
        // delete (it fails on the absent re-check), and a patch that
        // observed "absent" still matches after the window.
        use super::*;
        use contract::Permissions;
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "gateway-g02-delete-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        permissions.fs_read.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        std::fs::write(workspace.join("notes.txt"), "alpha\n").expect("seed");
        let target = gateway.resolve_write("notes.txt").expect("target");
        // Observe present, delete underneath, then require-present must fail.
        let observed = gateway
            .observe_patch_state(&target)
            .await
            .expect("observe should succeed");
        assert!(observed.is_some());
        gateway
            .remove_file_resolved(&target)
            .await
            .expect("delete should succeed");
        let error = gateway
            .require_unchanged_since(&target, observed.as_deref())
            .await
            .expect_err("a deleted target must fail the present re-check");
        assert!(
            matches!(
                error,
                GatewayError::AnchoredPatch(files::AnchoredPatchError::BaseMismatch { .. })
            ),
            "delete must surface as base drift, got: {error:?}"
        );
        // Absent-observed still matches absent.
        gateway
            .require_unchanged_since(&target, None)
            .await
            .expect("absent observed as absent must pass");
        // Absent-observed plus a concurrent create must fail: the file the
        // patch assumed missing is now present.
        gateway
            .write_file_atomic(&target, b"created\n")
            .await
            .expect("concurrent create should succeed");
        let error = gateway
            .require_unchanged_since(&target, None)
            .await
            .expect_err("a created target must fail the absent re-check");
        assert!(
            matches!(
                error,
                GatewayError::AnchoredPatch(files::AnchoredPatchError::BaseMismatch { .. })
            ),
            "create must surface as base drift, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn g06_patch_preserves_executable_bit_and_runs() {
        // G06-01: a one-line patch of a 0755 script keeps executability and
        // the repaired script still executes directly.
        use super::*;
        use contract::Permissions;
        use std::os::unix::fs::PermissionsExt as _;
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "gateway-g06-exec-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        permissions.fs_read.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        let script = "#!/bin/sh\necho before\n";
        std::fs::write(workspace.join("run.sh"), script).expect("script should be written");
        std::fs::set_permissions(
            workspace.join("run.sh"),
            std::fs::Permissions::from_mode(0o755),
        )
        .expect("script should be executable");
        let target = gateway.resolve_write("run.sh").expect("target");
        let annotated = files::annotate(script);
        let base_hash = files::base_sha256(script);
        let limits = files::PatchLimits {
            max_edits: 8,
            max_patch_bytes: 4096,
            max_result_bytes: 65536,
        };
        gateway
            .apply_anchored_patch(
                &target,
                Some(&base_hash),
                vec![files::PatchEdit {
                    op: files::PatchOp::Replace,
                    anchor_line: annotated[1].line_no,
                    anchor_hash: annotated[1].hash.clone(),
                    lines: vec!["echo after".to_string()],
                }],
                limits,
            )
            .await
            .expect("patch should succeed");
        let mode = std::fs::metadata(&target)
            .expect("script should stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o755, "a patch must preserve 0755, got {mode:o}");
        let output = std::process::Command::new(&target)
            .output()
            .expect("patched script should execute directly");
        assert!(
            output.status.success(),
            "patched script must run: {output:?}"
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("after"),
            "patched content must run: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        // Guarded preserving write (the repair commit shape) also keeps it.
        let guard = super::super::lock_patch_paths(&[target.as_path()]).await;
        gateway
            .write_file_atomic_preserving_mode_under_guard(&target, b"#!/bin/sh\necho v2\n", &guard)
            .await
            .expect("preserving write should succeed");
        drop(guard);
        let mode = std::fs::metadata(&target)
            .expect("script should stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o755,
            "a repair-style write must preserve 0755, got {mode:o}"
        );
    }

    #[tokio::test]
    async fn g06_data_mode_and_special_bits_are_sanitized() {
        // G06-02: ordinary 0644 data keeps its mode; setuid/setgid/sticky
        // bits from a pre-existing file are never inherited; new files
        // stay owner-only.
        use super::*;
        use contract::Permissions;
        use std::os::unix::fs::PermissionsExt as _;
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "gateway-g06-mode-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        permissions.fs_read.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        std::fs::write(workspace.join("data.txt"), "a\n").expect("data seed");
        std::fs::set_permissions(
            workspace.join("data.txt"),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("data mode should set");
        let target = gateway.resolve_write("data.txt").expect("target");
        let base_hash = files::base_sha256("a\n");
        let annotated = files::annotate("a\n");
        gateway
            .apply_anchored_patch(
                &target,
                Some(&base_hash),
                vec![files::PatchEdit {
                    op: files::PatchOp::Replace,
                    anchor_line: annotated[0].line_no,
                    anchor_hash: annotated[0].hash.clone(),
                    lines: vec!["b".to_string()],
                }],
                files::PatchLimits {
                    max_edits: 8,
                    max_patch_bytes: 4096,
                    max_result_bytes: 65536,
                },
            )
            .await
            .expect("data patch should succeed");
        let mode = std::fs::metadata(&target)
            .expect("data should stat")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(
            mode & 0o777,
            0o644,
            "0644 data must keep its mode, got {mode:o}"
        );
        // Special bits are stripped on inherit, never preserved.
        std::fs::write(workspace.join("suid.sh"), "#!/bin/sh\necho x\n").expect("suid seed");
        std::fs::set_permissions(
            workspace.join("suid.sh"),
            std::fs::Permissions::from_mode(0o4755),
        )
        .expect("setuid mode should set");
        let suid = gateway.resolve_write("suid.sh").expect("suid target");
        let text = std::fs::read_to_string(&suid).expect("suid should read");
        let annotated = files::annotate(&text);
        let base_hash = files::base_sha256(&text);
        gateway
            .apply_anchored_patch(
                &suid,
                Some(&base_hash),
                vec![files::PatchEdit {
                    op: files::PatchOp::Replace,
                    anchor_line: annotated[1].line_no,
                    anchor_hash: annotated[1].hash.clone(),
                    lines: vec!["echo y".to_string()],
                }],
                files::PatchLimits {
                    max_edits: 8,
                    max_patch_bytes: 4096,
                    max_result_bytes: 65536,
                },
            )
            .await
            .expect("suid patch should succeed");
        let mode = std::fs::metadata(&suid)
            .expect("suid should stat")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(
            mode & 0o7000,
            0,
            "special bits must never survive, got {mode:o}"
        );
        assert_eq!(
            mode & 0o777,
            0o755,
            "permission bits inherit sanitized, got {mode:o}"
        );
        // Brand-new files stay owner-only.
        let fresh = gateway.resolve_write("fresh.txt").expect("fresh target");
        gateway
            .write_file_atomic(&fresh, b"new\n")
            .await
            .expect("fresh write should succeed");
        let mode = std::fs::metadata(&fresh)
            .expect("fresh should stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "new files must stay owner-only, got {mode:o}");
    }

    #[tokio::test]
    async fn g06_rejected_patch_leaves_content_and_mode_untouched() {
        // G06-03: a base-mismatched or anchor-rejected patch changes
        // neither bytes nor mode.
        use super::*;
        use contract::Permissions;
        use std::os::unix::fs::PermissionsExt as _;
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "gateway-g06-reject-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let _temp_guard = TempGuard(base.clone());
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        permissions.fs_read.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        std::fs::write(workspace.join("run.sh"), "#!/bin/sh\necho a\n").expect("seed");
        std::fs::set_permissions(
            workspace.join("run.sh"),
            std::fs::Permissions::from_mode(0o755),
        )
        .expect("mode should set");
        let target = gateway.resolve_write("run.sh").expect("target");
        let before_bytes = std::fs::read(&target).expect("bytes should read");
        let before_mode = std::fs::metadata(&target)
            .expect("stat should succeed")
            .permissions()
            .mode()
            & 0o7777;
        // Wrong base: refused before any write.
        let annotated = files::annotate("#!/bin/sh\necho a\n");
        let error = gateway
            .apply_anchored_patch(
                &target,
                Some(&files::base_sha256("stale\n")),
                vec![files::PatchEdit {
                    op: files::PatchOp::Replace,
                    anchor_line: annotated[0].line_no,
                    anchor_hash: annotated[0].hash.clone(),
                    lines: vec!["x".to_string()],
                }],
                files::PatchLimits {
                    max_edits: 8,
                    max_patch_bytes: 4096,
                    max_result_bytes: 65536,
                },
            )
            .await
            .expect_err("a stale base must be refused");
        assert!(
            matches!(
                error,
                GatewayError::AnchoredPatch(files::AnchoredPatchError::BaseMismatch { .. })
            ),
            "must report base drift, got: {error:?}"
        );
        assert_eq!(std::fs::read(&target).expect("bytes"), before_bytes);
        assert_eq!(
            std::fs::metadata(&target)
                .expect("stat")
                .permissions()
                .mode()
                & 0o7777,
            before_mode,
            "a refused patch must not touch the mode"
        );
    }
}
