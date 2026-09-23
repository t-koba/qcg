//! Package pack/unpack: archive inventory, fd-relative Unix extraction,
//! and mode sanitization.
//!
//! Responsibility split (C-5): inventory verification, path containment,
//! and `sanitize_restored_mode` / `apply_restored_mode` (mode-only helpers
//! shared via `qcg-fs::sanitize_mode_bits`) stay in this module; a future
//! split moves the mode helpers to `package_mode.rs` with no behavior change.
use camino::{Utf8Path, Utf8PathBuf};
use qcg_fs::read_bounded;
use qcg_policy::{is_safe_relative_path, portable_relative_path};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;

use crate::types::ServiceError;

/// `st_mode` file-type mask and symlink type bits for ZIP entries.
const UNIX_MODE_TYPE_MASK: u32 = 0o170000;
const UNIX_MODE_SYMLINK: u32 = 0o120000;
#[cfg(unix)]
const UNIX_MODE_DIR: u32 = 0o040000;

/// Explicit max only. `None` means no mechanistic limit.
#[derive(Debug, Clone, Copy, Default)]
pub struct PackageLimits {
    pub max_entries: Option<usize>,
    pub max_bytes: Option<u64>,
    pub max_metadata_bytes: Option<usize>,
    pub max_archive_bytes: Option<u64>,
}

/// Explicit max only. `None` means no mechanistic limit.
#[derive(Debug, Clone, Copy, Default)]
pub struct ArtifactZipLimits {
    pub max_bytes: Option<u64>,
    pub max_entries: Option<usize>,
}

/// Opens the package archive without following a terminal symlink: the
/// archive bytes define every mode and hash below, so a swapped link must
/// fail closed instead of feeding attacker-chosen bytes (E15). Unix opens
/// with `O_NOFOLLOW` (authoritative); the pre-probe only improves the error
/// message. Non-Unix probes before and after the open and fails closed on
/// any link; a residual pathname window remains and untrusted concurrent
/// writers are unsupported there (E15).
fn open_archive_nofollow(archive: &Utf8Path) -> Result<File, ServiceError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        match std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(archive)
        {
            Ok(file) => {
                if !file.metadata().map_err(ServiceError::Io)?.is_file() {
                    return Err(ServiceError::Invalid(format!(
                        "package archive `{archive}` is not a regular file"
                    )));
                }
                Ok(file)
            }
            Err(error) => {
                if std::fs::symlink_metadata(archive)
                    .is_ok_and(|meta| meta.file_type().is_symlink())
                {
                    return Err(ServiceError::Invalid(format!(
                        "package archive `{archive}` is a symbolic link; refusing to read"
                    )));
                }
                Err(ServiceError::Io(error))
            }
        }
    }
    #[cfg(not(unix))]
    {
        match std::fs::symlink_metadata(archive) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(ServiceError::Invalid(format!(
                    "package archive `{archive}` is a symbolic link; refusing to read"
                )));
            }
            Ok(_) => {}
            Err(error) => return Err(ServiceError::Io(error)),
        }
        let file = File::open(archive).map_err(ServiceError::Io)?;
        match std::fs::symlink_metadata(archive) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(ServiceError::Invalid(format!(
                    "package archive `{archive}` changed to a symlink during open; refusing to read"
                )));
            }
            Ok(_) => {}
            Err(error) => return Err(ServiceError::Io(error)),
        }
        if !file.metadata().map_err(ServiceError::Io)?.is_file() {
            return Err(ServiceError::Invalid(format!(
                "package archive `{archive}` is not a regular file"
            )));
        }
        Ok(file)
    }
}

/// Pinned unpack-target directory fd (Unix): opened once with
/// `O_DIRECTORY | O_NOFOLLOW` from the canonicalized target, so every
/// parent creation and file staging below runs relative to one directory
/// inode. A parent pathname swapped after resolution cannot redirect
/// `mkdirat`/staging/`renameat` (E15). Components are restricted to normal
/// names, so containment is structural: no absolute path is ever resolved
/// and `..` is refused.
#[cfg(unix)]
struct PinnedDir {
    fd: std::os::unix::io::RawFd,
}

#[cfg(unix)]
impl Drop for PinnedDir {
    fn drop(&mut self) {
        // SAFETY: `fd` is owned by this handle.
        unsafe {
            libc::close(self.fd);
        }
    }
}

#[cfg(unix)]
fn cstring_for_package(name: &str) -> Result<std::ffi::CString, ServiceError> {
    std::ffi::CString::new(name)
        .map_err(|_| ServiceError::Invalid("package path contains a NUL byte".into()))
}

#[cfg(unix)]
fn open_pinned_dir(path: &Utf8Path) -> Result<PinnedDir, ServiceError> {
    use std::os::unix::ffi::OsStrExt as _;
    // Pin without following (E15): refuse a symlink target before
    // canonicalization. `canonicalize` follows links, so a symlinked target
    // must fail here instead of pinning through it to outside. The
    // `O_NOFOLLOW` open below is authoritative; this pre-probe only improves
    // the error.
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(ServiceError::Invalid(format!(
                "package target `{path}` is a symbolic link; refusing to pin"
            )));
        }
        Ok(_) => {}
        Err(error) => return Err(ServiceError::Io(error)),
    }
    // Canonicalize once so the pin anchors the real target, not a path
    // that traverses a link. The target must already exist (callers
    // pre-create it); a missing target fails here instead of later with
    // the same refusal outcome.
    let canonical = dunce::canonicalize(path).map_err(ServiceError::Io)?;
    let owned = cstring_for_package(
        std::str::from_utf8(canonical.as_os_str().as_bytes())
            .map_err(|_| ServiceError::Invalid("package target path is not UTF-8".into()))?,
    )?;
    // SAFETY: `owned` is a valid NUL-terminated absolute path.
    let fd = unsafe {
        libc::open(
            owned.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(ServiceError::Io(std::io::Error::last_os_error()));
    }
    Ok(PinnedDir { fd })
}

/// `fstatat` without following a terminal symlink. `Ok(None)` is a missing
/// name; any other inspection failure propagates instead of guessing (E15).
#[cfg(unix)]
fn stat_at_nofollow(
    dir: &PinnedDir,
    name: &std::ffi::CString,
) -> Result<Option<libc::stat>, ServiceError> {
    let mut status: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `dir.fd` is an open directory fd and `name` is valid.
    let rc = unsafe {
        libc::fstatat(
            dir.fd,
            name.as_ptr(),
            &mut status,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc == 0 {
        return Ok(Some(status));
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::NotFound {
        return Ok(None);
    }
    Err(ServiceError::Io(error))
}

#[cfg(unix)]
fn stat_is_symlink(status: &libc::stat) -> bool {
    // `st_mode` is u16 on some Unix (macOS) and u32 on others (Linux):
    // widen through u64 so neither platform warns.
    (status.st_mode as u64 & u64::from(UNIX_MODE_TYPE_MASK)) == u64::from(UNIX_MODE_SYMLINK)
}

#[cfg(unix)]
fn stat_is_dir(status: &libc::stat) -> bool {
    // `st_mode` is u16 on some Unix (macOS) and u32 on others (Linux):
    // widen through u64 so neither platform warns.
    (status.st_mode as u64 & u64::from(UNIX_MODE_TYPE_MASK)) == u64::from(UNIX_MODE_DIR)
}

/// Opens (never follows) one child directory of a pinned directory,
/// creating it owner-only (0700, umask-proof via a post-open `fchmod`)
/// when requested. A symlink at the name is always refused, and a
/// non-directory collision fails closed (E15).
/// Mode-atomicity (E15): `mkdirat(0700)` is umask-filtered (`0700 & ~umask`
/// can only restrict, never widen), so no phantom window exposes a wider
/// mode; the post-open `fchmod(0700)` then enforces the exact owner-only
/// mode even under a hostile umask that narrowed creation.
#[cfg(unix)]
fn pinned_subdir(parent: &PinnedDir, name: &str, create: bool) -> Result<PinnedDir, ServiceError> {
    let owned = cstring_for_package(name)?;
    match stat_at_nofollow(parent, &owned)? {
        Some(status) if stat_is_symlink(&status) => Err(ServiceError::Invalid(format!(
            "unpack path traverses a symbolic link at `{name}`"
        ))),
        Some(status) if !stat_is_dir(&status) => Err(ServiceError::Invalid(format!(
            "unpack path `{name}` collides with an existing file"
        ))),
        Some(_) => open_child_dir(parent, &owned),
        None if create => {
            // SAFETY: `parent.fd` is an open directory fd; `owned` is valid.
            let rc = unsafe { libc::mkdirat(parent.fd, owned.as_ptr(), 0o700 as libc::mode_t) };
            if rc != 0 {
                return Err(ServiceError::Io(std::io::Error::last_os_error()));
            }
            let child = open_child_dir(parent, &owned)?;
            // mkdirat is umask-filtered: enforce the exact owner-only mode
            // on the handle so a hostile umask cannot widen or narrow it.
            // SAFETY: the child fd is valid and owned by `child`.
            if unsafe { libc::fchmod(child.fd, 0o700 as libc::mode_t) } != 0 {
                return Err(ServiceError::Io(std::io::Error::last_os_error()));
            }
            Ok(child)
        }
        None => Err(ServiceError::Invalid(format!(
            "unpack path `{name}` does not exist"
        ))),
    }
}

#[cfg(unix)]
fn open_child_dir(parent: &PinnedDir, name: &std::ffi::CString) -> Result<PinnedDir, ServiceError> {
    // SAFETY: `parent.fd` is an open directory fd and `name` is valid.
    let fd = unsafe {
        libc::openat(
            parent.fd,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(ServiceError::Io(std::io::Error::last_os_error()));
    }
    Ok(PinnedDir { fd })
}

/// Runs `op` with the fd of `relative`'s parent directory: walks parent
/// components under a pinned target root (creating missing levels
/// owner-only) and hands the leaf-parent fd to `op`. Intermediate fds are
/// closed as the walk descends; only the leaf parent stays open for `op`.
/// Only normal components are accepted: absolute paths and `..` are
/// refused structurally (E15).
#[cfg(unix)]
fn with_parent_fd<T>(
    root: &PinnedDir,
    relative: &Utf8Path,
    op: impl FnOnce(&PinnedDir, &str) -> Result<T, ServiceError>,
) -> Result<T, ServiceError> {
    let mut components = Vec::new();
    for component in relative.components() {
        match component {
            camino::Utf8Component::Normal(part) => components.push(part.to_string()),
            _ => {
                return Err(ServiceError::Invalid(format!(
                    "unpack path `{relative}` escapes the target directory"
                )));
            }
        }
    }
    let (leaf, parents) = match components.split_last() {
        Some(split) => split,
        None => {
            return Err(ServiceError::Invalid(format!(
                "unpack path `{relative}` is empty"
            )));
        }
    };
    let mut current: Option<PinnedDir> = None;
    for part in parents {
        let next = match current.as_ref() {
            Some(fd) => pinned_subdir(fd, part, true)?,
            None => pinned_subdir(root, part, true)?,
        };
        current = Some(next);
    }
    match current.as_ref() {
        Some(fd) => op(fd, leaf),
        None => op(root, leaf),
    }
}

/// Stages one file under a pinned parent fd with its final mode applied
/// pre-rename (E15): delegates to the single shared staging dance in
/// `qcg-fs` (`stage_file_at`) so pack and unpack cannot drift. Unix only.
#[cfg(unix)]
fn stage_file_fd(
    parent: &PinnedDir,
    file_name: &str,
    mode: u32,
    copy: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> Result<(), ServiceError> {
    qcg_fs::stage_file_at(parent.fd, file_name, mode, 3, copy).map_err(ServiceError::Io)
}

/// Creates or adopts one archive directory leaf under a pinned parent fd
/// with its final sanitized mode applied on the handle (E15). A symlink at
/// the leaf is never created through, chmodded through, or replaced.
#[cfg(unix)]
fn create_dir_leaf_fd(parent: &PinnedDir, file_name: &str, mode: u32) -> Result<(), ServiceError> {
    let mode = sanitize_restored_mode(mode);
    let owned = cstring_for_package(file_name)?;
    match stat_at_nofollow(parent, &owned)? {
        Some(status) if stat_is_symlink(&status) => Err(ServiceError::Invalid(format!(
            "package directory `{file_name}` collides with an existing symbolic link"
        ))),
        Some(status) if !stat_is_dir(&status) => Err(ServiceError::Invalid(format!(
            "package directory `{file_name}` collides with an existing file"
        ))),
        Some(_) => {
            let leaf = open_child_dir(parent, &owned)?;
            // SAFETY: the leaf fd is valid and owned by `leaf`.
            if unsafe { libc::fchmod(leaf.fd, mode as libc::mode_t) } != 0 {
                return Err(ServiceError::Io(std::io::Error::last_os_error()));
            }
            Ok(())
        }
        None => {
            // SAFETY: `parent.fd` is an open directory fd; `owned` is valid.
            if unsafe { libc::mkdirat(parent.fd, owned.as_ptr(), 0o700 as libc::mode_t) } != 0 {
                return Err(ServiceError::Io(std::io::Error::last_os_error()));
            }
            let leaf = open_child_dir(parent, &owned)?;
            // Final mode on the handle: explicit archive entries keep their
            // own sanitized mode, never the 0700 creation mode (E15).
            // SAFETY: the leaf fd is valid and owned by `leaf`.
            if unsafe { libc::fchmod(leaf.fd, mode as libc::mode_t) } != 0 {
                return Err(ServiceError::Io(std::io::Error::last_os_error()));
            }
            // SAFETY: `parent.fd` is an open directory fd.
            if unsafe { libc::fsync(parent.fd) } < 0 {
                return Err(ServiceError::Io(std::io::Error::last_os_error()));
            }
            Ok(())
        }
    }
}

/// Non-Unix file staging with pre-rename mode: the temp is created with
/// `create_new` (a residual symlink-plant window remains and is documented;
/// untrusted concurrent writers are unsupported on non-Unix), content is
/// written and synced, then the final mode mapping is applied to the
/// staging path BEFORE the rename. The leaf is re-probed immediately
/// before the rename and a symlink is refused (E15). No post-rename chmod
/// ever runs.
#[cfg(not(unix))]
fn stage_file_path(
    dest: &Utf8Path,
    mode: u32,
    copy: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> Result<(), ServiceError> {
    let mode = sanitize_restored_mode(mode);
    let file_name = dest.file_name().unwrap_or("file");
    let parent = dest.parent().ok_or_else(|| {
        ServiceError::Invalid(format!("package path `{dest}` has no parent directory"))
    })?;
    for _ in 0..100 {
        let staging = parent.join(format!(
            ".{file_name}.qcg-part-{}",
            uuid::Uuid::now_v7().as_simple()
        ));
        if std::fs::symlink_metadata(&staging).is_ok() {
            continue;
        }
        let mut file = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(ServiceError::Io(error)),
        };
        let outcome = copy(&mut file)
            .and_then(|()| file.sync_all())
            .map_err(ServiceError::Io);
        drop(file);
        if let Err(error) = outcome {
            let _ = std::fs::remove_file(&staging);
            return Err(error);
        }
        // Pre-rename mode on the staging path (never post-rename): the
        // owner-write bit maps onto the read-only flag (platform
        // limitation, E15-3). Non-Unix limitation: POSIX executability
        // cannot round-trip here, so a mode carrying exec bits degrades to
        // the read-only flag and the loss is warned, never silent. The
        // staging name was just created exclusively by this call, so the
        // probe-then-chmod below cannot reach a foreign path except through
        // the documented residual window.
        match std::fs::symlink_metadata(&staging) {
            Ok(meta) if meta.file_type().is_symlink() => {
                let _ = std::fs::remove_file(&staging);
                return Err(ServiceError::Invalid(format!(
                    "refusing to stage through symbolic link `{staging}`"
                )));
            }
            Ok(_) => {}
            Err(error) => {
                let _ = std::fs::remove_file(&staging);
                return Err(ServiceError::Io(error));
            }
        }
        let mut permissions = std::fs::metadata(&staging)
            .map_err(ServiceError::Io)?
            .permissions();
        if mode & 0o111 != 0 {
            tracing::warn!(path = %staging, mode = format!("{mode:o}"), "non-Unix staging cannot preserve POSIX executability; degrading to the read-only flag");
        }
        permissions.set_readonly(mode & 0o222 == 0);
        std::fs::set_permissions(&staging, permissions).map_err(ServiceError::Io)?;
        // Re-probe the leaf immediately before the rename; a symlink is
        // refused, never replaced (E15).
        match std::fs::symlink_metadata(dest) {
            Ok(meta) if meta.file_type().is_symlink() => {
                let _ = std::fs::remove_file(&staging);
                return Err(ServiceError::Invalid(format!(
                    "refusing to replace symbolic link `{dest}`"
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                let _ = std::fs::remove_file(&staging);
                return Err(ServiceError::Io(error));
            }
        }
        if let Err(error) = std::fs::rename(&staging, dest) {
            let _ = std::fs::remove_file(&staging);
            return Err(ServiceError::Io(error));
        }
        return Ok(());
    }
    Err(ServiceError::Invalid(
        "package staging collided repeatedly; refusing to overwrite".into(),
    ))
}

pub fn unpack_qcg(
    archive: &Utf8Path,
    target: &Utf8Path,
    limits: &PackageLimits,
) -> Result<(), ServiceError> {
    // Mode bits ride inside the archive bytes, whose integrity the install
    // layer verifies wholesale (SHA-256 or Ed25519 before staging), so
    // flipping an entry's executable bit without detection requires
    // defeating that outer verification. The inventory below pins content
    // hashes per path; modes are restored through the sanitizer, which
    // never adopts setuid/setgid/world-writable bits (E15).
    let file = open_archive_nofollow(archive)?;
    let mut archive = zip::ZipArchive::new(file)?;
    if let Some(limit) = limits.max_entries
        && archive.len() > limit
    {
        return Err(ServiceError::Invalid(format!(
            "archive contains too many entries: {} > {limit}",
            archive.len(),
        )));
    }
    let mut unpacked_bytes = 0_u64;
    let mut paths = BTreeSet::new();
    // Pin the target directory once (Unix): every parent creation and file
    // staging below runs relative to this fd (E15).
    #[cfg(unix)]
    let pinned_target = open_pinned_dir(target)?;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let Some(enclosed) = entry.enclosed_name() else {
            return Err(ServiceError::Invalid(format!(
                "archive contains an unsafe path: {}",
                entry.name()
            )));
        };
        let rel = Utf8PathBuf::from_path_buf(enclosed.to_path_buf())
            .map_err(|_| ServiceError::Invalid("archive path is not UTF-8".into()))?;
        if rel.as_str().is_empty() {
            continue;
        }
        if !paths.insert(rel.clone()) {
            return Err(ServiceError::Invalid(format!(
                "archive contains duplicate path `{rel}`"
            )));
        }
        if entry
            .unix_mode()
            .is_some_and(|mode| mode & UNIX_MODE_TYPE_MASK == UNIX_MODE_SYMLINK)
        {
            return Err(ServiceError::Invalid(format!(
                "archive contains a symbolic link `{rel}`"
            )));
        }
        unpacked_bytes = unpacked_bytes
            .checked_add(entry.size())
            .ok_or_else(|| ServiceError::Invalid("archive expanded size overflowed".into()))?;
        if let Some(limit) = limits.max_bytes
            && unpacked_bytes > limit
        {
            return Err(ServiceError::Invalid(format!(
                "archive expanded size exceeds {limit} bytes"
            )));
        }
        // Every archive entry must carry an explicit Unix mode; mode-less
        // entries are refused instead of inheriting the umask or falling
        // back to silent defaults (E15-8). The mode is sanitized on restore
        // so setuid/setgid/sticky never survive and world-writable is
        // cleared (E15-1).
        let Some(raw_mode) = entry.unix_mode() else {
            return Err(ServiceError::Invalid(format!(
                "archive entry `{rel}` is missing an explicit Unix mode"
            )));
        };
        let archived_mode = sanitize_restored_mode(raw_mode);
        if entry.is_dir() {
            // Directory leaves are created (or adopted) with their final
            // sanitized mode applied on a handle, never chmodded through a
            // pathname link (E15).
            #[cfg(unix)]
            with_parent_fd(&pinned_target, &rel, |parent, leaf| {
                create_dir_leaf_fd(parent, leaf, archived_mode)
            })?;
            #[cfg(not(unix))]
            {
                let out = target.join(&rel);
                if let Some(parent) = out.parent() {
                    create_parent_dirs(target, parent)?;
                }
                unpack_dir_leaf_path(&out, &rel, archived_mode)?;
            }
        } else {
            // File leaves are staged with the final mode applied BEFORE the
            // rename; no post-rename chmod ever runs (E15).
            let expected = entry.size();
            #[cfg(unix)]
            with_parent_fd(&pinned_target, &rel, |parent, leaf| {
                stage_file_fd(parent, leaf, archived_mode, |output| {
                    let copied = std::io::copy(&mut entry, output)?;
                    if copied != expected {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("archive entry size changed while unpacking `{rel}`"),
                        ));
                    }
                    Ok(())
                })
            })?;
            #[cfg(not(unix))]
            {
                let out = target.join(&rel);
                if let Some(parent) = out.parent() {
                    create_parent_dirs(target, parent)?;
                }
                // Fail closed when the file target is already a symlink,
                // symmetric with the directory path (E15-5). The staging
                // helper re-probes the leaf immediately before the rename
                // and refuses links there as well.
                match std::fs::symlink_metadata(&out) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(ServiceError::Invalid(format!(
                            "archive file `{rel}` collides with an existing symbolic link"
                        )));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(ServiceError::Io(error)),
                }
                stage_file_path(&out, archived_mode, |output| {
                    let copied = std::io::copy(&mut entry, output)?;
                    if copied != expected {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("archive entry size changed while unpacking `{rel}`"),
                        ));
                    }
                    Ok(())
                })?;
            }
        }
    }
    verify_package_inventory(target, limits)?;
    Ok(())
}

/// Non-Unix archive directory leaf: probe, create, re-probe, then apply the
/// final mode. The re-probe before the mode change refuses a link planted
/// in the create window; a residual pathname race remains and untrusted
/// concurrent writers are unsupported on non-Unix (E15).
#[cfg(not(unix))]
fn unpack_dir_leaf_path(out: &Utf8Path, rel: &Utf8PathBuf, mode: u32) -> Result<(), ServiceError> {
    // A pre-existing symlink or file at the directory path must never be
    // created through or chmodded through (E15).
    match std::fs::symlink_metadata(out) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(ServiceError::Invalid(format!(
                "archive directory `{rel}` collides with an existing symbolic link"
            )));
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(ServiceError::Invalid(format!(
                "archive directory `{rel}` collides with an existing file"
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_plain_dir(out)?;
        }
        Err(error) => return Err(ServiceError::Io(error)),
    }
    match std::fs::symlink_metadata(out) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(ServiceError::Invalid(format!(
                "archive directory `{rel}` changed to a symbolic link during unpack; refusing mode change"
            )));
        }
        Ok(_) => {}
        Err(error) => return Err(ServiceError::Io(error)),
    }
    apply_restored_mode(out, mode)?;
    Ok(())
}

/// Creates each missing component of `dir` under `target` with least-
/// privilege `0o700`, so implicit parents never land world-accessible and
/// never inherit the process umask (E15-2). Explicit archive entries keep
/// their own sanitized modes; only implicit parents use 0700.
/// Non-Unix path-based fallback for the fd-relative walk above: every level
/// is probed before creation and re-validated after (a symlink planted in
/// the probe/create window is refused at the re-probe instead of being
/// created through), and each created level is canonicalized back under the
/// target root. A residual pathname race remains by platform necessity;
/// untrusted concurrent writers are unsupported on non-Unix (E15).
#[cfg(not(unix))]
fn create_parent_dirs(target: &Utf8Path, dir: &Utf8Path) -> Result<(), ServiceError> {
    let rel = dir.strip_prefix(target).map_err(|_| {
        ServiceError::Invalid(format!("unpack path `{dir}` escapes the target directory"))
    })?;
    let canonical_root = dunce::canonicalize(target).ok();
    let mut current = target.to_path_buf();
    for component in rel.components() {
        let Some(part) = component.as_os_str().to_str() else {
            return Err(ServiceError::Invalid(format!(
                "unpack path component is not UTF-8 under `{target}`"
            )));
        };
        current.push(part);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(ServiceError::Invalid(format!(
                    "unpack path `{current}` traverses a symbolic link"
                )));
            }
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(ServiceError::Invalid(format!(
                    "unpack path `{current}` collides with an existing file"
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_plain_dir(&current)?;
                apply_restored_mode(&current, 0o700)?;
                // Re-validate immediately after creation: refuse a link
                // planted in the probe/create window (E15).
                match std::fs::symlink_metadata(&current) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(ServiceError::Invalid(format!(
                            "unpack path `{current}` changed to a symbolic link during creation; refusing"
                        )));
                    }
                    Ok(metadata) if metadata.is_dir() => {}
                    Ok(_) => {
                        return Err(ServiceError::Invalid(format!(
                            "unpack path `{current}` collides with an existing file"
                        )));
                    }
                    Err(error) => return Err(ServiceError::Io(error)),
                }
            }
            Err(error) => return Err(ServiceError::Io(error)),
        }
        // Containment re-check while a canonical root is available: a
        // created level that resolves outside the target fails closed.
        if let Some(root) = canonical_root.as_ref() {
            match dunce::canonicalize(&current) {
                Ok(canonical) if canonical.starts_with(root) => {}
                Ok(_) => {
                    return Err(ServiceError::Invalid(format!(
                        "unpack path `{current}` escapes the target directory"
                    )));
                }
                // An unscannable level fails closed instead of being
                // treated as contained (E15).
                Err(error) => return Err(ServiceError::Io(error)),
            }
        }
    }
    Ok(())
}

/// Ensures `path` is a plain directory without following a pre-existing
/// symlink: `create_dir_all` would silently accept one and later writes
/// would land outside the target (E15).
#[cfg(not(unix))]
fn ensure_plain_dir(path: &Utf8Path) -> Result<(), ServiceError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(ServiceError::Invalid(format!(
            "package path `{path}` collides with an existing symbolic link"
        ))),
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(ServiceError::Invalid(format!(
            "package path `{path}` collides with an existing file"
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => create_plain_dir(path),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

/// Creates one directory with an owner-only mode first, so the process
/// umask cannot expose a wider window before the caller applies the final
/// mode.
#[cfg(not(unix))]
fn create_plain_dir(path: &Utf8Path) -> Result<(), ServiceError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(path.as_std_path())?;
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir(path.as_std_path())?;
    }
    Ok(())
}

/// Strips dangerous bits from a restored mode: setuid, setgid, and sticky
/// are removed by the 0o777 mask, and the world-writable bit is cleared so a
/// 0o777 archive entry can never land world-writable (E15-1). Group bits
/// (including group-writable, e.g. 0775) and owner executability are
/// preserved by design: only other-write and special bits are dangers.
/// Staging itself is always owner-only; this sanitize applies at commit.
fn sanitize_restored_mode(mode: u32) -> u32 {
    qcg_fs::sanitize_mode_bits(mode)
}

/// Applies an explicit, sanitized permission mode. Callers must supply the
/// mode; there are no silent defaults (E15-8). Non-Unix targets map the
/// owner-write bit onto the read-only flag (platform limitation, E15-3).
/// Unix opens with `O_NOFOLLOW` and chmods the fd so a symlink swapped in
/// between extraction and chmod cannot redirect the mode change outside
/// the target (E15).
/// Applies an explicit, sanitized permission mode on non-Unix targets
/// (E15): there are no POSIX mode bits, so the owner-write bit maps onto
/// the read-only flag. Exec bits cannot round-trip; warn instead of
/// degrading silently (E15-3). Unix needs no path-based variant: every Unix
/// mode change runs fd-relative (`fchmod`/`fchmodat` on an `O_NOFOLLOW`
/// handle in `stage_file_fd` and the fd-relative directory walk), so a
/// symlink swapped in between extraction and chmod cannot redirect it.
/// A path-based Unix fallback would reintroduce exactly the race the
/// fd-relative path closes, hence only the non-Unix variant exists (E15).
#[cfg(not(unix))]
fn apply_restored_mode(path: &Utf8Path, mode: u32) -> Result<(), ServiceError> {
    let mode = sanitize_restored_mode(mode);
    if mode & 0o111 != 0 {
        tracing::warn!(path = %path, mode = format!("{mode:o}"), "non-Unix restore cannot preserve POSIX executability; degrading to the read-only flag");
    }
    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_readonly(mode & 0o222 == 0);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

/// Re-verifies an already-installed package against its own SBOM and
/// provenance inventory. Installers call this when a package satisfies the
/// request so a corrupted install is repaired instead of silently reused
/// (E14).
pub fn verify_installed_package(
    root: &Utf8Path,
    limits: &PackageLimits,
) -> Result<(), ServiceError> {
    verify_package_inventory(root, limits)
}

pub fn copy_dir_all(
    source: &Utf8Path,
    target: &Utf8Path,
    limits: &PackageLimits,
) -> Result<(), ServiceError> {
    let mut entry_count = 0_usize;
    let mut copied_bytes = 0_u64;
    // Pin the target once (Unix): destination parents and file staging run
    // relative to this fd (E15).
    #[cfg(unix)]
    let pinned_target = open_pinned_dir(target)?;
    for entry in qcg_fs::WalkDir::new(source) {
        let entry = entry.map_err(|error| {
            ServiceError::Invalid(format!("failed to walk `{source}`: {error}"))
        })?;
        let file_type = entry.file_type();
        if file_type.is_symlink() {
            return Err(ServiceError::Invalid(format!(
                "package source contains a symbolic link: {}",
                entry.path()
            )));
        }
        let path = entry.path().to_path_buf();
        let rel = path
            .strip_prefix(source)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        if rel.as_str().is_empty() {
            if !file_type.is_dir() {
                return Err(ServiceError::Invalid(format!(
                    "package source is not a directory: {path}"
                )));
            }
            continue;
        }
        entry_count = entry_count
            .checked_add(1)
            .ok_or_else(|| ServiceError::Invalid("package entry count overflowed".into()))?;
        if let Some(limit) = limits.max_entries
            && entry_count > limit
        {
            return Err(ServiceError::Invalid(format!(
                "package source contains too many entries: {entry_count} > {limit}"
            )));
        }
        if !is_safe_relative_path(&portable_relative_path(rel)) {
            return Err(ServiceError::Invalid(format!(
                "package source contains an unsafe path `{rel}`"
            )));
        }
        // Re-check for a symlink at use time: the WalkDir type was observed
        // earlier and the path could have been swapped since (E15-6).
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(ServiceError::Invalid(format!(
                    "package source contains a symbolic link: {path}"
                )));
            }
            Ok(_) => {}
            Err(error) => {
                return Err(ServiceError::Invalid(format!(
                    "failed to inspect package entry: {error}"
                )));
            }
        }
        // Open the source handle FIRST (O_NOFOLLOW at use time, so a
        // swapped-in symlink is refused instead of followed) and gate on
        // the handle's own metadata, never on a pre-open following stat:
        // length, kind, and mode all come from the opened file (E15).
        let mut input = open_source_file_nofollow(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::PermissionDenied {
                ServiceError::Invalid(format!("package source contains a symbolic link: {path}"))
            } else {
                ServiceError::Invalid(format!("failed to inspect package entry: {error}"))
            }
        })?;
        let source_metadata = input.metadata().map_err(|error| {
            ServiceError::Invalid(format!("failed to inspect package entry: {error}"))
        })?;
        if file_type.is_dir() {
            if !source_metadata.is_dir() {
                return Err(ServiceError::Invalid(format!(
                    "package source changed while being copied: {path}"
                )));
            }
            #[cfg(unix)]
            with_parent_fd(&pinned_target, rel, |parent, leaf| {
                create_dir_leaf_fd(parent, leaf, explicit_source_mode(&source_metadata, true))
            })?;
            #[cfg(not(unix))]
            {
                let dest = target.join(rel);
                ensure_plain_dir(&dest)?;
                apply_restored_mode(&dest, explicit_source_dir_mode(&source_metadata))?;
            }
            continue;
        }
        if !file_type.is_file() {
            return Err(ServiceError::Invalid(format!(
                "package source contains an unsupported entry: {path}"
            )));
        }
        if !source_metadata.is_file() {
            return Err(ServiceError::Invalid(format!(
                "package source changed while being copied: {path}"
            )));
        }
        let metadata = source_metadata;
        copied_bytes = copied_bytes
            .checked_add(metadata.len())
            .ok_or_else(|| ServiceError::Invalid("package source size overflowed".into()))?;
        if let Some(limit) = limits.max_bytes
            && copied_bytes > limit
        {
            return Err(ServiceError::Invalid(format!(
                "package source exceeds {limit} bytes"
            )));
        }
        let explicit_mode = explicit_source_mode(&metadata, false);
        // Handle-relative staging and replace with the final mode applied
        // BEFORE the rename, never post-rename (E15). The source is read
        // from the already-open handle, so content swapped between the
        // length gate above and the copy below is impossible: the gate and
        // the bytes share one inode.
        #[cfg(unix)]
        with_parent_fd(&pinned_target, rel, |parent, leaf| {
            stage_file_fd(parent, leaf, explicit_mode, |output| {
                let copied = std::io::copy(&mut input, output)?;
                if copied != metadata.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("package source changed while being copied: {path}"),
                    ));
                }
                Ok(())
            })
        })?;
        #[cfg(not(unix))]
        {
            let dest = target.join(rel);
            if let Some(parent) = dest.parent() {
                create_parent_dirs(target, parent)?;
            }
            // Fail closed on a symlinked destination, symmetric with the
            // directory path (E15-5); the staging helper re-probes the leaf
            // immediately before the rename and refuses links there too.
            match std::fs::symlink_metadata(&dest) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(ServiceError::Invalid(format!(
                        "package destination `{dest}` collides with an existing symbolic link"
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(ServiceError::Io(error)),
            }
            stage_file_path(&dest, explicit_mode, |output| {
                let copied = std::io::copy(&mut input, output)?;
                if copied != metadata.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("package source changed while being copied: {path}"),
                    ));
                }
                Ok(())
            })?;
        }
    }
    Ok(())
}

/// Derives the explicit source mode for a copied entry. Unix preserves the
/// source permission bits (sanitized on apply); non-Unix synthesizes from
/// the read-only flag so the mode stays explicit (E15-8, platform limit E15-3).
fn explicit_source_mode(metadata: &std::fs::Metadata, is_dir: bool) -> u32 {
    if is_dir {
        explicit_source_dir_mode(metadata)
    } else {
        explicit_source_file_mode(metadata)
    }
}

/// Explicit directory handling: the source directory bits round-trip so a
/// copied tree stays traversable exactly like its source (E15).
/// Non-Unix limitation (E15-3): Windows has no POSIX mode bits, so the mode
/// is synthesized from the read-only flag (0o555 when read-only, 0o755
/// otherwise). Exact group/other bits do not round-trip there.
fn explicit_source_dir_mode(metadata: &std::fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        metadata.permissions().mode() & 0o777
    }
    #[cfg(not(unix))]
    {
        if metadata.permissions().readonly() {
            0o555
        } else {
            0o755
        }
    }
}

/// Explicit file handling: on Unix the source file bits round-trip
/// (including owner executability) so scripts stay executable; the
/// sanitizer at apply time strips only the dangerous bits (E15).
/// Non-Unix limitation (E15-3): Windows exposes only the read-only flag,
/// so the mode degrades to 0o444 (read-only) or 0o644 (writable) and owner
/// executability is NOT preserved. Packaged scripts may lose their exec
/// bit there; callers restoring on non-Unix warn when exec bits are
/// dropped instead of degrading silently.
fn explicit_source_file_mode(metadata: &std::fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        metadata.permissions().mode() & 0o777
    }
    #[cfg(not(unix))]
    {
        if metadata.permissions().readonly() {
            0o444
        } else {
            0o644
        }
    }
}

/// Opens a source file without following a terminal symlink on Unix
/// (O_NOFOLLOW at use time, E15-6). Non-Unix pre-checks for a symlink.
fn open_source_file_nofollow(path: &Utf8Path) -> Result<File, std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
    }
    #[cfg(not(unix))]
    {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("refusing to open symbolic link `{path}`"),
            )),
            Ok(_) => std::fs::File::open(path),
            Err(error) => Err(error),
        }
    }
}

fn verify_package_inventory(root: &Utf8Path, limits: &PackageLimits) -> Result<(), ServiceError> {
    let sbom_path = root.join("QCG-SBOM.spdx.json");
    let provenance_path = root.join("QCG-PROVENANCE.intoto.json");
    let sbom_bytes = read_bounded(&sbom_path, limits.max_metadata_bytes).map_err(|error| {
        ServiceError::Invalid(format!("package is missing QCG-SBOM.spdx.json: {error}"))
    })?;
    let provenance_bytes =
        read_bounded(&provenance_path, limits.max_metadata_bytes).map_err(|error| {
            ServiceError::Invalid(format!(
                "package is missing QCG-PROVENANCE.intoto.json: {error}"
            ))
        })?;
    let sbom: serde_json::Value =
        serde_json::from_slice(&sbom_bytes).map_err(ServiceError::Json)?;
    let provenance: serde_json::Value =
        serde_json::from_slice(&provenance_bytes).map_err(ServiceError::Json)?;
    if sbom.get("spdxVersion").and_then(|value| value.as_str()) != Some("SPDX-2.3")
        || provenance.get("_type").and_then(|value| value.as_str())
            != Some("https://in-toto.io/Statement/v1")
    {
        return Err(ServiceError::Invalid(
            "package supply-chain metadata has an unsupported format".into(),
        ));
    }
    let mut expected: BTreeMap<String, (String, u32)> = BTreeMap::new();
    let files = sbom
        .get("files")
        .and_then(|value| value.as_array())
        .ok_or_else(|| ServiceError::Invalid("package SBOM files array is required".into()))?;
    for file in files {
        let path = file
            .get("fileName")
            .and_then(|value| value.as_str())
            .ok_or_else(|| ServiceError::Invalid("package SBOM fileName is required".into()))?;
        if !is_safe_relative_path(path) {
            return Err(ServiceError::Invalid(format!(
                "package SBOM contains unsafe path `{path}`"
            )));
        }
        let sha256 = file
            .pointer("/checksums/0/checksumValue")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                ServiceError::Invalid("package SBOM SHA256 checksum is required".into())
            })?;
        // The pack side records the sanitized permission bits per file
        // ("mode", decimal). Verification REQUIRES them (fail-closed, E15):
        // hashes always verify AND modes always verify. The older
        // absent-mode skip-compat was removed: an SBOM without an explicit
        // mode fails instead of verifying by hash alone, so a mode-stripped
        // package cannot downgrade to hash-only verification.
        let mode = match file.get("mode") {
            None => {
                return Err(ServiceError::Invalid(format!(
                    "package SBOM file `{path}` is missing the required mode"
                )));
            }
            Some(value) => match value.as_u64().and_then(|mode| u32::try_from(mode).ok()) {
                Some(mode) if mode <= 0o777 => mode,
                _ => {
                    return Err(ServiceError::Invalid(format!(
                        "package SBOM file `{path}` has an invalid mode"
                    )));
                }
            },
        };
        if expected
            .insert(path.to_string(), (sha256.to_string(), mode))
            .is_some()
        {
            return Err(ServiceError::Invalid(format!(
                "package SBOM contains duplicate path `{path}`"
            )));
        }
    }
    let mut actual = BTreeSet::new();
    let mut walked = 0_usize;
    // Single walk: each file is opened once (O_NOFOLLOW, fail-closed on a
    // swapped-in symlink) and its kind, mode, and hash all derive from the
    // opened handle — no probe-then-open-then-stat triple per file, and no
    // parent swap can redirect the bytes between validation and hashing
    // (E15).
    for entry in qcg_fs::WalkDir::new(root) {
        let entry = entry
            .map_err(|error| ServiceError::Invalid(format!("failed to walk package: {error}")))?;
        walked = walked.saturating_add(1);
        if let Some(limit) = limits.max_entries
            && walked > limit
        {
            return Err(ServiceError::Invalid(format!(
                "package contains more than {limit} entries"
            )));
        }
        if entry.file_type().is_symlink() {
            return Err(ServiceError::Invalid(format!(
                "package contains a symbolic link: {}",
                entry.path()
            )));
        }
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path().to_path_buf();
        let relative = portable_relative_path(
            path.strip_prefix(root)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?,
        );
        if matches!(
            relative.as_str(),
            "QCG-SBOM.spdx.json" | "QCG-PROVENANCE.intoto.json"
        ) {
            continue;
        }
        actual.insert(relative.clone());
        let (expected_sha256, expected_mode) = expected.get(&relative).ok_or_else(|| {
            ServiceError::Invalid(format!("package contains unlisted file `{relative}`"))
        })?;
        // The WalkDir observation predates the open: the O_NOFOLLOW open
        // below is authoritative, and a swapped-in symlink fails here
        // instead of being followed (E15).
        let mut handle = open_verified_file_nofollow(&path, &relative)?;
        let handle_metadata = handle.metadata().map_err(|error| {
            ServiceError::Invalid(format!(
                "failed to inspect package file `{relative}`: {error}"
            ))
        })?;
        if !handle_metadata.is_file() {
            return Err(ServiceError::Invalid(format!(
                "package file `{relative}` changed while being verified"
            )));
        }
        // Modes always verify (fail-closed): absent modes were rejected
        // above, so every entry has an expected mode (E15).
        let actual_mode = permission_bits(&handle_metadata);
        if &actual_mode != expected_mode {
            return Err(ServiceError::Invalid(format!(
                "package file `{relative}` mode mismatch: expected {expected_mode:o}, found {actual_mode:o}"
            )));
        }
        let digest = hash_opened_file(&mut handle)?;
        if &digest != expected_sha256 {
            return Err(ServiceError::Invalid(format!(
                "package file `{relative}` failed SBOM integrity verification"
            )));
        }
    }
    for missing in expected.keys() {
        if !actual.contains(missing) {
            return Err(ServiceError::Invalid(format!(
                "package SBOM lists missing file `{missing}`"
            )));
        }
    }
    Ok(())
}

/// Opens a package file for verification without following a terminal
/// symlink (E15). Unix opens with `O_NOFOLLOW` (authoritative); the
/// symlink probe below only classifies the error. Non-Unix probes before
/// the open and fails closed on any link, with a documented residual
/// window.
fn open_verified_file_nofollow(path: &Utf8Path, relative: &str) -> Result<File, ServiceError> {
    match open_source_file_nofollow(path) {
        Ok(file) => Ok(file),
        Err(error) => {
            if std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink()) {
                return Err(ServiceError::Invalid(format!(
                    "package contains a symbolic link: {relative}"
                )));
            }
            Err(ServiceError::Invalid(format!(
                "failed to inspect package file `{relative}`: {error}"
            )))
        }
    }
}

/// Sanitized mode bits from handle metadata for SBOM mode verification
/// (E15): delegates to the single shared helper so the verify path cannot
/// drift from the pack path. `sanitize_restored_mode` at the call site is
/// therefore subsumed (sanitize is idempotent) and removed there.
fn permission_bits(metadata: &std::fs::Metadata) -> u32 {
    qcg_fs::sanitized_metadata_mode(metadata)
}

/// SHA256 of an already-opened (already nofollow-verified) handle (E15):
/// delegates to the single shared streaming core so the verify path cannot
/// drift from the pack path. No byte cap here: the caller already bounded
/// the handle before this call.
fn hash_opened_file(file: &mut File) -> Result<String, ServiceError> {
    qcg_fs::hash_opened_file_sha256(file, None, |limit| {
        std::io::Error::other(format!("package file exceeds {limit} bytes while hashing"))
    })
    .map(|(hex, _)| hex)
    .map_err(ServiceError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    #[test]
    fn sanitize_restored_mode_clears_dangerous_bits() {
        // E15: setuid/setgid/sticky and world-writable never land.
        assert_eq!(sanitize_restored_mode(0o4755), 0o755);
        assert_eq!(sanitize_restored_mode(0o777), 0o775);
        assert_eq!(sanitize_restored_mode(0o755), 0o755);
        assert_eq!(sanitize_restored_mode(0o644), 0o644);
    }

    #[test]
    fn sbom_modes_verify_when_declared_and_reject_when_absent() {
        // E15 fail-closed: the manifest list verifies modes, not only
        // hashes. A declared mode that no longer matches fails closed, and
        // an absent mode (older packages) is REJECTED instead of verifying
        // by hash alone — backward skip-compat was removed.
        let root = temp_dir("sbom-modes");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let target = root.join("pkg");
        std::fs::create_dir_all(&target).expect("target should be created");
        let body = b"data";
        let sha256 = hex::encode(Sha256::digest(body));
        std::fs::write(target.join("data.txt"), body).expect("file should be written");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(
                target.join("data.txt"),
                std::fs::Permissions::from_mode(0o644),
            )
            .expect("mode should apply");
        }
        let sbom_with_mode = serde_json::json!({
            "spdxVersion": "SPDX-2.3",
            "files": [
                { "fileName": "data.txt", "checksums": [{ "checksumValue": sha256 }], "mode": 0o644 },
            ],
        });
        std::fs::write(
            target.join("QCG-SBOM.spdx.json"),
            serde_json::to_vec(&sbom_with_mode).expect("sbom should serialize"),
        )
        .expect("sbom should be written");
        std::fs::write(
            target.join("QCG-PROVENANCE.intoto.json"),
            br#"{"_type":"https://in-toto.io/Statement/v1"}"#,
        )
        .expect("provenance should be written");
        verify_installed_package(&target, &PackageLimits::default())
            .expect("a matching mode should verify");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(
                target.join("data.txt"),
                std::fs::Permissions::from_mode(0o600),
            )
            .expect("tampered mode should apply");
            let error = verify_installed_package(&target, &PackageLimits::default())
                .expect_err("a tampered mode must fail closed");
            assert!(
                error.to_string().contains("mode mismatch"),
                "the refusal must name the mode mismatch: {error}"
            );
            std::fs::set_permissions(
                target.join("data.txt"),
                std::fs::Permissions::from_mode(0o644),
            )
            .expect("mode should be restored");
        }
        // Absent mode: rejected fail-closed (skip-compat removed).
        let sbom_without_mode = serde_json::json!({
            "spdxVersion": "SPDX-2.3",
            "files": [
                { "fileName": "data.txt", "checksums": [{ "checksumValue": sha256 }] },
            ],
        });
        std::fs::write(
            target.join("QCG-SBOM.spdx.json"),
            serde_json::to_vec(&sbom_without_mode).expect("sbom should serialize"),
        )
        .expect("sbom should be written");
        let error = verify_installed_package(&target, &PackageLimits::default())
            .expect_err("an absent mode must fail closed");
        assert!(
            error.to_string().contains("missing the required mode"),
            "the refusal must name the missing mode: {error}"
        );
        // Malformed mode fails closed.
        let sbom_bad_mode = serde_json::json!({
            "spdxVersion": "SPDX-2.3",
            "files": [
                { "fileName": "data.txt", "checksums": [{ "checksumValue": sha256 }], "mode": 0o1777 },
            ],
        });
        std::fs::write(
            target.join("QCG-SBOM.spdx.json"),
            serde_json::to_vec(&sbom_bad_mode).expect("sbom should serialize"),
        )
        .expect("sbom should be written");
        let error = verify_installed_package(&target, &PackageLimits::default())
            .expect_err("an invalid mode must fail closed");
        assert!(
            error.to_string().contains("invalid mode"),
            "the refusal must name the invalid mode: {error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unpack_never_adopts_archive_ownership() {
        // E15: ownership fields are never honored (no chown exists on this
        // path). Unpacked files always belong to the unpacking user with
        // sanitized modes: a foreign-owner archive degrades to
        // current-user ownership instead of being honored, and setuid bits
        // never restore ownership-adjacent privilege.
        use std::io::Write as _;
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let root = temp_dir("unpack-ownership");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.qcg");
        let script = b"#!/bin/sh\n";
        let script_sha256 = hex::encode(Sha256::digest(script));
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            writer
                .start_file("bin/run.sh", options.unix_permissions(0o4755))
                .expect("script entry");
            writer.write_all(script).expect("script body");
            let sbom = serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [
                    { "fileName": "bin/run.sh", "checksums": [{ "checksumValue": script_sha256 }], "mode": 0o755 },
                ],
            });
            writer
                .start_file("QCG-SBOM.spdx.json", options.unix_permissions(0o644))
                .expect("sbom entry");
            writer
                .write_all(sbom.to_string().as_bytes())
                .expect("sbom body");
            writer
                .start_file(
                    "QCG-PROVENANCE.intoto.json",
                    options.unix_permissions(0o644),
                )
                .expect("provenance entry");
            writer
                .write_all(br#"{"_type":"https://in-toto.io/Statement/v1"}"#)
                .expect("provenance body");
            writer.finish().expect("archive should finish");
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target should be created");
        unpack_qcg(&archive, &target, &PackageLimits::default()).expect("unpack should succeed");
        let metadata = std::fs::metadata(target.join("bin/run.sh")).expect("script metadata");
        // SAFETY: process uid query cannot fail.
        let euid = unsafe { libc::getuid() };
        assert_eq!(
            metadata.uid(),
            euid,
            "unpacked files must belong to the unpacking user, never to an archive owner"
        );
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            0o755,
            "setuid bits must be masked even though the SBOM mode declares 0755"
        );
    }

    fn temp_dir(name: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-package-{name}-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path must be UTF-8")
    }

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04). A removal failure warns instead of being silently
    /// ignored (E01).
    struct TempGuard(Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(self.0.as_std_path()) {
                tracing::warn!(path = %self.0, %error, "test temp cleanup failed");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn copy_dir_all_preserves_ordinary_modes() {
        // Mode preservation must not depend on the umask.
        use std::os::unix::fs::PermissionsExt as _;
        let root = temp_dir("copy-modes");
        let source = root.join("source");
        let target = root.join("target");
        std::fs::create_dir_all(source.join("bin")).expect("source dirs");
        std::fs::write(source.join("bin/run.sh"), b"#!/bin/sh\n").expect("script");
        std::fs::set_permissions(
            source.join("bin/run.sh"),
            std::fs::Permissions::from_mode(0o755),
        )
        .expect("script mode");
        std::fs::write(source.join("data.txt"), b"data").expect("data");
        std::fs::set_permissions(
            source.join("data.txt"),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("data mode");
        std::fs::set_permissions(source.join("bin"), std::fs::Permissions::from_mode(0o750))
            .expect("dir mode");
        std::fs::create_dir_all(&target).expect("target");
        copy_dir_all(&source, &target, &PackageLimits::default()).expect("copy should succeed");
        let mode_of = |path: &Utf8Path| {
            std::fs::metadata(path)
                .expect("copied metadata")
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(mode_of(&target.join("bin/run.sh")), 0o755);
        assert_eq!(mode_of(&target.join("data.txt")), 0o644);
        assert_eq!(mode_of(&target.join("bin")), 0o750);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn unpack_refuses_entries_without_explicit_modes() {
        // E15-8: mode-less entries are refused instead of inheriting the
        // umask or falling back to silent defaults. The zip writer always
        // normalizes a default mode, so a truly mode-less entry must have
        // its central-directory external attributes cleared after writing;
        // otherwise `unix_mode()` still reports the normalized default.
        use std::io::Write as _;

        let root = temp_dir("unpack-no-modes");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.qcg");
        let body = b"data";
        let sha256 = hex::encode(Sha256::digest(body));
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            writer.start_file("data.txt", options).expect("data entry");
            writer.write_all(body).expect("data body");
            let sbom = serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [
                    { "fileName": "data.txt", "checksums": [{ "checksumValue": sha256 }], "mode": 0o644 },
                ],
            });
            writer
                .start_file("QCG-SBOM.spdx.json", options.unix_permissions(0o644))
                .expect("sbom entry");
            writer
                .write_all(sbom.to_string().as_bytes())
                .expect("sbom body");
            writer
                .start_file(
                    "QCG-PROVENANCE.intoto.json",
                    options.unix_permissions(0o644),
                )
                .expect("provenance entry");
            writer
                .write_all(br#"{"_type":"https://in-toto.io/Statement/v1"}"#)
                .expect("provenance body");
            writer.finish().expect("archive should finish");
        }
        // Strip the normalized mode from the data.txt central header so the
        // entry truly carries no Unix mode and `unix_mode()` returns None.
        {
            let mut bytes = std::fs::read(&archive).expect("archive should be readable");
            // Locate End of Central Directory (PK\x05\x06) from the tail.
            let eocd_sig = [0x50u8, 0x4b, 0x05, 0x06];
            let eocd_pos = bytes
                .windows(4)
                .rposition(|window| window == eocd_sig)
                .expect("archive should have an End of Central Directory");
            let central_size = u32::from_le_bytes(
                bytes[eocd_pos + 12..eocd_pos + 16]
                    .try_into()
                    .expect("size"),
            ) as usize;
            let central_offset = u32::from_le_bytes(
                bytes[eocd_pos + 16..eocd_pos + 20]
                    .try_into()
                    .expect("offset"),
            ) as usize;
            let central_end = central_offset + central_size;
            let mut offset = central_offset;
            let mut stripped = false;
            while offset + 46 <= central_end && offset + 46 <= bytes.len() {
                let is_central = bytes[offset] == 0x50
                    && bytes[offset + 1] == 0x4b
                    && bytes[offset + 2] == 0x01
                    && bytes[offset + 3] == 0x02;
                if !is_central {
                    break;
                }
                let name_len = u16::from_le_bytes(
                    bytes[offset + 28..offset + 30]
                        .try_into()
                        .expect("name len"),
                ) as usize;
                let extra_len = u16::from_le_bytes(
                    bytes[offset + 30..offset + 32]
                        .try_into()
                        .expect("extra len"),
                ) as usize;
                let comment_len = u16::from_le_bytes(
                    bytes[offset + 32..offset + 34]
                        .try_into()
                        .expect("comment len"),
                ) as usize;
                let name_start = offset + 46;
                let name_end = name_start + name_len;
                if name_end <= bytes.len() && &bytes[name_start..name_end] == b"data.txt" {
                    bytes[offset + 38..offset + 42].copy_from_slice(&[0, 0, 0, 0]);
                    stripped = true;
                }
                offset = name_end + extra_len + comment_len;
            }
            assert!(stripped, "data.txt central header should be found");
            std::fs::write(&archive, &bytes).expect("stripped archive should be writable");
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target");
        let error = unpack_qcg(&archive, &target, &PackageLimits::default())
            .expect_err("a mode-less entry must fail closed");
        assert!(
            error.to_string().contains("explicit Unix mode"),
            "the refusal must name the missing mode: {error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unpack_implicit_parents_get_least_privilege_0700() {
        // E15-2: implicit parents get 0700 (least privilege); explicit
        // archive entries keep their own sanitized modes.
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;
        use std::sync::Mutex;

        static UMASK_LOCK: Mutex<()> = Mutex::new(());
        let _guard = UMASK_LOCK.lock().expect("umask lock");
        let root = temp_dir("unpack-implicit-0700");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.qcg");
        let sha256 = |bytes: &[u8]| hex::encode(Sha256::digest(bytes));
        let tool = b"tool";
        let data = b"data";
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            // No directory entries: every implicit parent must get 0700
            // even with a hostile umask.
            writer
                .start_file("lib/deep/tool.sh", options.unix_permissions(0o755))
                .expect("tool entry");
            writer.write_all(tool).expect("tool body");
            writer
                .start_file("data.txt", options.unix_permissions(0o644))
                .expect("data entry");
            writer.write_all(data).expect("data body");
            let sbom = serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [
                    { "fileName": "lib/deep/tool.sh", "checksums": [{ "checksumValue": sha256(tool) }], "mode": 0o755 },
                    { "fileName": "data.txt", "checksums": [{ "checksumValue": sha256(data) }], "mode": 0o644 },
                ],
            });
            writer
                .start_file("QCG-SBOM.spdx.json", options.unix_permissions(0o644))
                .expect("sbom entry");
            writer
                .write_all(sbom.to_string().as_bytes())
                .expect("sbom body");
            writer
                .start_file(
                    "QCG-PROVENANCE.intoto.json",
                    options.unix_permissions(0o644),
                )
                .expect("provenance entry");
            writer
                .write_all(br#"{"_type":"https://in-toto.io/Statement/v1"}"#)
                .expect("provenance body");
            writer.finish().expect("archive should finish");
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target");
        // SAFETY: the UMASK_LOCK serializes the process-wide umask change
        // with every test in this module that inspects file modes.
        let previous = unsafe { libc::umask(0o077) };
        let result = unpack_qcg(&archive, &target, &PackageLimits::default());
        // SAFETY: restores the process umask captured above.
        unsafe {
            libc::umask(previous);
        }
        result.expect("unpack should succeed");
        let mode_of = |path: &Utf8Path| {
            std::fs::metadata(path)
                .expect("mode metadata")
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(mode_of(&target.join("lib/deep/tool.sh")), 0o755);
        assert_eq!(mode_of(&target.join("data.txt")), 0o644);
        assert_eq!(mode_of(&target.join("lib")), 0o700);
        assert_eq!(mode_of(&target.join("lib/deep")), 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn unpack_refuses_a_directory_colliding_with_a_symlink() {
        // E15: a pre-existing symlink where an archive directory entry
        // goes must never be chmodded through; the unpack fails closed
        // and the link target is untouched.
        use std::io::Write as _;

        let root = temp_dir("symlink-collision");
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.qcg");
        let body = b"data";
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            writer
                .add_directory("sub/", options.unix_permissions(0o755))
                .expect("directory entry");
            writer
                .start_file("sub/file.txt", options.unix_permissions(0o644))
                .expect("file entry");
            writer.write_all(body).expect("file body");
            let sha256 = hex::encode(Sha256::digest(body));
            let sbom = serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [
                    { "fileName": "sub/file.txt", "checksums": [{ "checksumValue": sha256 }], "mode": 0o644 },
                ],
            });
            writer
                .start_file("QCG-SBOM.spdx.json", options.unix_permissions(0o644))
                .expect("sbom entry");
            writer
                .write_all(sbom.to_string().as_bytes())
                .expect("sbom body");
            writer
                .start_file(
                    "QCG-PROVENANCE.intoto.json",
                    options.unix_permissions(0o644),
                )
                .expect("provenance entry");
            writer
                .write_all(br#"{"_type":"https://in-toto.io/Statement/v1"}"#)
                .expect("provenance body");
            writer.finish().expect("archive should finish");
        }
        let target = root.join("out");
        let outside = root.join("outside");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::create_dir_all(&outside).expect("outside");
        std::os::unix::fs::symlink(&outside, target.join("sub")).expect("planted symlink");
        let error = unpack_qcg(&archive, &target, &PackageLimits::default())
            .expect_err("a symlink collision must fail closed");
        assert!(
            error.to_string().contains("symbolic link"),
            "the collision must be named: {error}"
        );
        assert!(
            std::fs::read_dir(&outside)
                .expect("outside readable")
                .next()
                .is_none(),
            "the link target must stay untouched"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn unpack_refuses_a_file_colliding_with_a_symlink() {
        // E15-5: a pre-existing symlink where an archive file entry goes
        // must fail closed, symmetric with the directory path.
        use std::io::Write as _;

        let root = temp_dir("file-symlink-collision");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.qcg");
        let body = b"data";
        let sha256 = hex::encode(Sha256::digest(body));
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            writer
                .start_file("link.txt", options.unix_permissions(0o644))
                .expect("file entry");
            writer.write_all(body).expect("file body");
            let sbom = serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [
                    { "fileName": "link.txt", "checksums": [{ "checksumValue": sha256 }], "mode": 0o644 },
                ],
            });
            writer
                .start_file("QCG-SBOM.spdx.json", options.unix_permissions(0o644))
                .expect("sbom entry");
            writer
                .write_all(sbom.to_string().as_bytes())
                .expect("sbom body");
            writer
                .start_file(
                    "QCG-PROVENANCE.intoto.json",
                    options.unix_permissions(0o644),
                )
                .expect("provenance entry");
            writer
                .write_all(br#"{"_type":"https://in-toto.io/Statement/v1"}"#)
                .expect("provenance body");
            writer.finish().expect("archive should finish");
        }
        let target = root.join("out");
        let outside = root.join("outside.txt");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(&outside, b"outside").expect("outside file");
        std::os::unix::fs::symlink(&outside, target.join("link.txt")).expect("planted symlink");
        let error = unpack_qcg(&archive, &target, &PackageLimits::default())
            .expect_err("a file symlink collision must fail closed");
        assert!(
            error.to_string().contains("symbolic link"),
            "the collision must be named: {error}"
        );
        assert_eq!(
            std::fs::read(&outside).expect("outside readable"),
            b"outside",
            "the link target must stay untouched"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unpack_strips_world_writable_and_masks_dangerous_bits() {
        // E15-1: world-writable is stripped (0777 lands 0775) and setuid,
        // setgid, sticky are masked.
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;
        let root = temp_dir("modes-0777");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.qcg");
        let bytes = b"x";
        let sha256 = hex::encode(Sha256::digest(bytes));
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            writer
                .start_file("wide.sh", options.unix_permissions(0o777))
                .expect("wide entry");
            writer.write_all(bytes).expect("wide body");
            let sbom = serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [
                    { "fileName": "wide.sh", "checksums": [{ "checksumValue": sha256 }], "mode": 0o775 },
                ],
            });
            writer
                .start_file("QCG-SBOM.spdx.json", options.unix_permissions(0o644))
                .expect("sbom entry");
            writer
                .write_all(sbom.to_string().as_bytes())
                .expect("sbom body");
            writer
                .start_file(
                    "QCG-PROVENANCE.intoto.json",
                    options.unix_permissions(0o644),
                )
                .expect("provenance entry");
            writer
                .write_all(br#"{"_type":"https://in-toto.io/Statement/v1"}"#)
                .expect("provenance body");
            writer.finish().expect("archive should finish");
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target should be created");
        unpack_qcg(&archive, &target, &PackageLimits::default()).expect("unpack should succeed");
        let mode = std::fs::metadata(target.join("wide.sh"))
            .expect("wide metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o775, "world-writable must be stripped from 0777");
    }

    #[cfg(unix)]
    #[test]
    fn unpack_masks_the_full_special_bit_matrix() {
        // E15: setuid, setgid, sticky, and world-writable bits are all
        // masked; only the sanitized permission bits survive.
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let root = temp_dir("special-bits");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.qcg");
        let payload = b"x";
        let sha256 = hex::encode(Sha256::digest(payload));
        let mut files = Vec::new();
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            for (name, mode, expected) in [
                ("setuid", 0o4755_u32, 0o755_u32),
                ("setgid", 0o2755_u32, 0o755_u32),
                ("sticky", 0o1755_u32, 0o755_u32),
                ("all", 0o7777_u32, 0o775_u32),
                ("wide", 0o777_u32, 0o775_u32),
                ("plain-exec", 0o755_u32, 0o755_u32),
                ("plain-data", 0o640_u32, 0o640_u32),
            ] {
                writer
                    .start_file(name, options.unix_permissions(mode))
                    .expect("entry");
                writer.write_all(payload).expect("body");
                files.push((name, expected));
            }
            let sbom_files: Vec<_> = files
                .iter()
                .map(|(name, expected)| {
                    serde_json::json!({ "fileName": name, "checksums": [{ "checksumValue": sha256 }], "mode": expected })
                })
                .collect();
            let sbom = serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": sbom_files,
            });
            writer
                .start_file("QCG-SBOM.spdx.json", options.unix_permissions(0o644))
                .expect("sbom entry");
            writer
                .write_all(sbom.to_string().as_bytes())
                .expect("sbom body");
            writer
                .start_file(
                    "QCG-PROVENANCE.intoto.json",
                    options.unix_permissions(0o644),
                )
                .expect("provenance entry");
            writer
                .write_all(br#"{"_type":"https://in-toto.io/Statement/v1"}"#)
                .expect("provenance body");
            writer.finish().expect("archive should finish");
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target should be created");
        unpack_qcg(&archive, &target, &PackageLimits::default()).expect("unpack should succeed");
        for (name, expected) in files {
            let mode = std::fs::metadata(target.join(name))
                .expect("entry metadata")
                .permissions()
                .mode()
                & 0o7777;
            assert_eq!(mode, expected, "dangerous bits must be masked for `{name}`");
        }
    }

    #[cfg(unix)]
    #[test]
    fn unpack_restores_executable_bits_without_dangerous_modes() {
        // E15: 0755 scripts round-trip as executable, 0644 data stays
        // readable, and setuid bits from the archive are masked away.
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let root = temp_dir("modes");
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.qcg");
        let sha256 = |bytes: &[u8]| hex::encode(Sha256::digest(bytes));
        let script = b"#!/bin/sh\n";
        let data = b"data";
        let unsafe_bytes = b"unsafe";
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            writer
                .start_file("bin/run.sh", options.unix_permissions(0o755))
                .expect("script entry");
            writer.write_all(script).expect("script body");
            writer
                .start_file("data.txt", options.unix_permissions(0o644))
                .expect("data entry");
            writer.write_all(data).expect("data body");
            writer
                .start_file("unsafe", options.unix_permissions(0o4755))
                .expect("unsafe entry");
            writer.write_all(unsafe_bytes).expect("unsafe body");
            let sbom = serde_json::json!({
                "spdxVersion": "SPDX-2.3",
                "files": [
                    { "fileName": "bin/run.sh", "checksums": [{ "checksumValue": sha256(script) }], "mode": 0o755 },
                    { "fileName": "data.txt", "checksums": [{ "checksumValue": sha256(data) }], "mode": 0o644 },
                    { "fileName": "unsafe", "checksums": [{ "checksumValue": sha256(unsafe_bytes) }], "mode": 0o755 },
                ],
            });
            writer
                .start_file("QCG-SBOM.spdx.json", options.unix_permissions(0o644))
                .expect("sbom entry");
            writer
                .write_all(sbom.to_string().as_bytes())
                .expect("sbom body");
            writer
                .start_file(
                    "QCG-PROVENANCE.intoto.json",
                    options.unix_permissions(0o644),
                )
                .expect("provenance entry");
            writer
                .write_all(br#"{"_type":"https://in-toto.io/Statement/v1"}"#)
                .expect("provenance body");
            writer.finish().expect("archive should finish");
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target should be created");
        unpack_qcg(&archive, &target, &PackageLimits::default()).expect("unpack should succeed");
        let script_mode = std::fs::metadata(target.join("bin/run.sh"))
            .expect("script metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(script_mode, 0o755, "script must stay executable");
        let data_mode = std::fs::metadata(target.join("data.txt"))
            .expect("data metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(data_mode, 0o644, "data mode must round-trip");
        let unsafe_mode = std::fs::metadata(target.join("unsafe"))
            .expect("unsafe metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(unsafe_mode, 0o755, "setuid bits must be masked");
        let _ = std::fs::remove_dir_all(&root);
    }
}
