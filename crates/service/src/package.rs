//! Package pack/unpack: archive inventory, fd-relative Unix extraction,
//! and mode sanitization.
//!
//! Responsibility split (C-5): inventory verification, path containment,
//! and `sanitize_restored_mode` / `apply_restored_mode` (mode-only helpers
//! shared via `files::sanitize_mode_bits`) stay in this module; a future
//! split moves the mode helpers to `package_mode.rs` with no behavior change.
use camino::{Utf8Path, Utf8PathBuf};
use files::read_bounded;
use policy::{is_safe_relative_path, portable_relative_path};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;

use crate::types::ServiceError;

/// `st_mode` file-type mask and symlink type bits for ZIP entries.
const UNIX_MODE_TYPE_MASK: u32 = 0o170000;
const UNIX_MODE_SYMLINK: u32 = 0o120000;
#[cfg(unix)]
const UNIX_MODE_DIR: u32 = 0o040000;

/// Default entry-count bound for package archives (flood gate).
pub const DEFAULT_PACKAGE_MAX_ENTRIES: usize = 1000;
/// Default expanded-bytes bound for package archives (bomb gate, 10 MiB).
pub const DEFAULT_PACKAGE_MAX_BYTES: u64 = 10 * 1024 * 1024;

/// Bounded by default: `max_entries` (1000 entries) and `max_bytes` (10 MiB
/// expanded) fail closed without configuration. `max_archive_bytes` stays
/// explicit-only (an archive-byte cap cannot stop a bomb) and
/// `max_metadata_bytes` stays explicit-only (uncalibrated).
#[derive(Debug, Clone, Copy)]
pub struct PackageLimits {
    /// Raw ZIP entries (including duplicate names), checked before inventory allocation.
    pub max_entries: Option<usize>,
    pub max_bytes: Option<u64>,
    pub max_metadata_bytes: Option<usize>,
    pub max_archive_bytes: Option<u64>,
}

impl Default for PackageLimits {
    fn default() -> Self {
        Self {
            max_entries: Some(DEFAULT_PACKAGE_MAX_ENTRIES),
            max_bytes: Some(DEFAULT_PACKAGE_MAX_BYTES),
            max_metadata_bytes: None,
            max_archive_bytes: None,
        }
    }
}

/// Explicit max only. `None` means no mechanistic limit.
#[derive(Debug, Clone, Copy, Default)]
pub struct ArtifactZipLimits {
    pub max_bytes: Option<u64>,
    pub max_entries: Option<usize>,
}
/// Provisional dedicated-artifact profile (product policy, not customer data).
pub const DEDICATED_ARTIFACT_MAX_BYTES: u64 = 16 * 1024 * 1024;
pub const DEDICATED_ARTIFACT_MAX_ENTRIES: usize = 1000;
impl ArtifactZipLimits {
    /// Fill omitted caps for dedicated HTTP routes; explicit values win.
    pub fn effective(self) -> Self {
        Self {
            max_bytes: Some(self.max_bytes.unwrap_or(DEDICATED_ARTIFACT_MAX_BYTES)),
            max_entries: Some(self.max_entries.unwrap_or(DEDICATED_ARTIFACT_MAX_ENTRIES)),
        }
    }
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
        let base: &PinnedDir = current.as_ref().unwrap_or(root);
        current = Some(pinned_subdir(base, part, true)?);
    }
    op(current.as_ref().unwrap_or(root), leaf)
}

/// Stages one file under a pinned parent fd with its final mode applied
/// pre-rename (E15): delegates to the single shared staging dance in
/// `files` (`stage_file_at`) so pack and unpack cannot drift. Unix only.
#[cfg(unix)]
fn stage_file_fd(
    parent: &PinnedDir,
    file_name: &str,
    mode: u32,
    copy: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> Result<(), ServiceError> {
    files::stage_file_at(parent.fd, file_name, mode, 3, copy).map_err(ServiceError::Io)
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
            ".{file_name}.part-{}",
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

fn invalid_zip(error: rawzip::Error) -> ServiceError {
    ServiceError::Invalid(format!("invalid package ZIP: {error}"))
}

/// Validate all raw records without allocating an inventory. The EOCD/ZIP64
/// hint is a gate, not a trusted iterator length; duplicates count separately.
/// Extraction makes a second streaming pass with the same parser and handle.
fn validate_entry_count<R: rawzip::ReaderAt>(
    archive: &rawzip::ZipArchive<R>,
    buffer: &mut [u8],
    limit: Option<usize>,
) -> Result<(), ServiceError> {
    let check = |count: u64| {
        if let Some(limit) = limit
            && count > limit as u64
        {
            return Err(ServiceError::Invalid(format!(
                "archive contains too many entries: {count} > {limit}"
            )));
        }
        Ok(())
    };
    check(archive.entries_hint())?;
    let mut count = 0_u64;
    let mut entries = archive.entries(buffer);
    while entries.next_entry().map_err(invalid_zip)?.is_some() {
        count = count
            .checked_add(1)
            .ok_or_else(|| ServiceError::Invalid("archive entry count overflowed".into()))?;
        check(count)?;
    }
    if count != archive.entries_hint() {
        return Err(ServiceError::Invalid(format!(
            "archive entry count mismatch: {count} != {}",
            archive.entries_hint()
        )));
    }
    Ok(())
}

fn package_entry_path(bytes: &[u8]) -> Result<Utf8PathBuf, ServiceError> {
    let name = std::str::from_utf8(bytes)
        .map_err(|_| ServiceError::Invalid("archive path is not UTF-8".into()))?;
    let unsafe_path = || ServiceError::Invalid(format!("archive contains an unsafe path: {name}"));
    // Normalize both ZIP separators, but never sanitize away an escaping
    // parent or an absolute/drive prefix. rawzip's try_normalize deliberately
    // strips those, which is unsuitable for a fail-closed package reader.
    if name.starts_with(['/', '\\']) || name.contains(['\0', ':']) {
        return Err(unsafe_path());
    }
    let mut rel = Utf8PathBuf::new();
    for part in name.split(['/', '\\']) {
        match part {
            "" | "." => {}
            ".." => {
                if !rel.pop() {
                    return Err(unsafe_path());
                }
            }
            part => rel.push(part),
        }
    }
    Ok(rel)
}

/// Bound actual expanded bytes before writing them and validate CRC/size at
/// EOF, before the staging helper can rename the file. Verifying at EOF keeps
/// the configured stream-budget error authoritative for lying small headers.
fn copy_package_entry(
    input: &mut dyn std::io::Read,
    output: &mut dyn std::io::Write,
    expected: rawzip::ZipVerification,
    remaining: Option<u64>,
    max_bytes: Option<u64>,
) -> std::io::Result<()> {
    use std::io::Read as _;
    let mut input = input.take(remaining.map_or(u64::MAX, |n| n.saturating_add(1)));
    let mut crc = rawzip::Crc32::new();
    let mut copied = 0_u64;
    let mut buffer = [0; 8192];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(read as u64)
            .ok_or_else(|| std::io::Error::other("archive expanded size overflowed"))?;
        if remaining.is_some_and(|budget| copied > budget) {
            return Err(std::io::Error::other(format!(
                "archive expanded size exceeds {} bytes",
                max_bytes.unwrap_or(u64::MAX)
            )));
        }
        crc.update(&buffer[..read]);
        output.write_all(&buffer[..read])?;
    }
    expected
        .valid(rawzip::ZipVerification {
            crc: crc.checksum(),
            uncompressed_size: copied,
        })
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

pub fn unpack_package(
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
    // Fixed scratch space covers the largest legal central record (three
    // u16-length fields plus the fixed header), not an archive-size policy.
    let mut buffer = vec![0; 46 + 3 * usize::from(u16::MAX)];
    let archive = rawzip::ZipArchive::from_file(file, &mut buffer).map_err(invalid_zip)?;
    validate_entry_count(&archive, &mut buffer, limits.max_entries)?;
    let mut unpacked_bytes = 0_u64;
    let mut paths = BTreeSet::new();
    // Pin the target directory once (Unix): every parent creation and file
    // staging below runs relative to this fd (E15).
    #[cfg(unix)]
    let pinned_target = open_pinned_dir(target)?;
    let mut entries = archive.entries(&mut buffer);
    while let Some(header) = entries.next_entry().map_err(invalid_zip)? {
        let rel = package_entry_path(header.file_path().as_bytes())?;
        if rel.as_str().is_empty() {
            continue;
        }
        if !paths.insert(rel.clone()) {
            return Err(ServiceError::Invalid(format!(
                "archive contains duplicate path `{rel}`"
            )));
        }
        // Inspect the raw type bits too: a non-Unix creator must not hide a
        // symlink behind rawzip's synthesized DOS/default permissions.
        if header.external_attributes() >> 16 & UNIX_MODE_TYPE_MASK == UNIX_MODE_SYMLINK {
            return Err(ServiceError::Invalid(format!(
                "archive contains a symbolic link `{rel}`"
            )));
        }
        if header.flags().is_encrypted()
            || header.flags().has_strong_encryption()
            || header.flags().is_masked()
        {
            return Err(ServiceError::Invalid(
                "encrypted ZIP entries are unsupported".into(),
            ));
        }
        let method = header.compression_method();
        if !matches!(
            method,
            rawzip::CompressionMethod::STORE | rawzip::CompressionMethod::DEFLATE
        ) {
            return Err(ServiceError::Invalid(
                "unsupported ZIP compression method".into(),
            ));
        }
        let prior_bytes = unpacked_bytes;
        unpacked_bytes = unpacked_bytes
            .checked_add(header.uncompressed_size_hint())
            .ok_or_else(|| ServiceError::Invalid("archive expanded size overflowed".into()))?;
        if let Some(limit) = limits.max_bytes
            && unpacked_bytes > limit
        {
            return Err(ServiceError::Invalid(format!(
                "archive expanded size exceeds {limit} bytes"
            )));
        }
        // Every archive entry must carry a nonzero mode in the high word
        // of external attributes; creator-system metadata does not change
        // where this explicit mode is stored. Mode-less entries are refused
        // instead of inheriting the umask or falling back to silent defaults
        // (E15-8). The raw mode is sanitized on restore so
        // setuid/setgid/sticky never survive and world-writable is cleared
        // (E15-1).
        let raw_mode = header.external_attributes() >> 16;
        if raw_mode == 0 {
            return Err(ServiceError::Invalid(format!(
                "archive entry `{rel}` is missing an explicit Unix mode"
            )));
        }
        let archived_mode = sanitize_restored_mode(raw_mode);
        let local = archive.get_entry(header.wayfinder()).map_err(invalid_zip)?;
        let reader = local.reader();
        let verification = reader.claim_verifier();
        let mut entry: Box<dyn std::io::Read + '_> = match method {
            rawzip::CompressionMethod::STORE => Box::new(reader),
            rawzip::CompressionMethod::DEFLATE => {
                Box::new(flate2::read::DeflateDecoder::new(reader))
            }
            _ => unreachable!("method checked above"),
        };
        let remaining = limits
            .max_bytes
            .map(|limit| limit.saturating_sub(prior_bytes));
        if header.is_dir() {
            copy_package_entry(
                &mut entry,
                &mut std::io::sink(),
                verification,
                remaining,
                limits.max_bytes,
            )?;
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
            #[cfg(unix)]
            with_parent_fd(&pinned_target, &rel, |parent, leaf| {
                stage_file_fd(parent, leaf, archived_mode, |output| {
                    copy_package_entry(
                        &mut entry,
                        output,
                        verification,
                        remaining,
                        limits.max_bytes,
                    )
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
                    copy_package_entry(
                        &mut entry,
                        output,
                        verification,
                        remaining,
                        limits.max_bytes,
                    )
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
    files::sanitize_mode_bits(mode)
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

/// Computes a deterministic SHA-256 identity for a raw generator directory.
/// Every non-root entry contributes its portable relative path, kind, and
/// permission mode; regular-file bytes are streamed into the hash. Timestamps
/// and filesystem traversal order are excluded. Symlinks, special entries,
/// unsafe paths, concurrent changes, and configured entry/byte limit
/// violations fail closed. On Unix, change time is also checked so a same-size
/// in-place edit that restores its modification time is detected while the
/// tree is being scanned.
pub fn package_identity_sha256(
    root: &Utf8Path,
    limits: &PackageLimits,
) -> Result<String, ServiceError> {
    let initial = collect_identity_entries(root, limits)?;
    let mut digest = sha2::Sha256::new();
    use sha2::Digest as _;
    digest.update(b"qcg-package-identity-v1\0");
    let entry_count = u64::try_from(initial.len())
        .map_err(|_| ServiceError::Invalid("package entry count overflowed".into()))?;
    digest.update(entry_count.to_le_bytes());

    let mut total_bytes = 0_u64;
    for entry in &initial {
        let path_bytes = entry.relative.as_bytes();
        let path_len = u64::try_from(path_bytes.len())
            .map_err(|_| ServiceError::Invalid("package path length overflowed".into()))?;
        digest.update(path_len.to_le_bytes());
        digest.update(path_bytes);
        digest.update([if entry.is_dir { 1 } else { 0 }]);
        digest.update(explicit_source_mode(&entry.metadata, entry.is_dir).to_le_bytes());
        if entry.is_dir {
            continue;
        }

        digest.update(entry.metadata.len().to_le_bytes());
        let mut file = files::open_read_nofollow(&entry.path).map_err(|_error| {
            if std::fs::symlink_metadata(&entry.path)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                ServiceError::Invalid("package source contains a symbolic link".into())
            } else {
                ServiceError::Invalid("failed to open a package file".into())
            }
        })?;
        let opened = file
            .metadata()
            .map_err(|_| ServiceError::Invalid("failed to inspect a package file".into()))?;
        if !identity_metadata_matches(entry, &opened, false) {
            return Err(ServiceError::Invalid(
                "a package file changed while its identity was computed".into(),
            ));
        }

        let mut file_bytes = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            use std::io::Read as _;
            let read_len = file.read(&mut buffer).map_err(ServiceError::Io)?;
            if read_len == 0 {
                break;
            }
            let read_bytes = u64::try_from(read_len)
                .map_err(|_| ServiceError::Invalid("package byte count overflowed".into()))?;
            file_bytes = file_bytes
                .checked_add(read_bytes)
                .ok_or_else(|| ServiceError::Invalid("package byte count overflowed".into()))?;
            total_bytes = total_bytes
                .checked_add(read_bytes)
                .ok_or_else(|| ServiceError::Invalid("package byte count overflowed".into()))?;
            if let Some(limit) = limits.max_bytes
                && total_bytes > limit
            {
                return Err(ServiceError::Invalid(format!(
                    "package source exceeds {limit} bytes"
                )));
            }
            digest.update(&buffer[..read_len]);
        }
        let after = file.metadata().map_err(ServiceError::Io)?;
        if file_bytes != entry.metadata.len() || !identity_metadata_matches(entry, &after, false) {
            return Err(ServiceError::Invalid(
                "a package file changed while its identity was computed".into(),
            ));
        }
    }

    // A second inventory detects additions, removals, replacements, and mode
    // changes that raced the streaming pass. Its traversal order is likewise
    // discarded before comparison.
    let final_inventory = collect_identity_entries(root, limits)?;
    if initial.len() != final_inventory.len()
        || initial
            .iter()
            .zip(&final_inventory)
            .any(|(before, after)| !identity_entries_match(before, after))
    {
        return Err(ServiceError::Invalid(
            "package source changed while its identity was computed".into(),
        ));
    }
    Ok(hex::encode(digest.finalize()))
}

struct IdentityEntry {
    path: Utf8PathBuf,
    relative: String,
    is_dir: bool,
    metadata: std::fs::Metadata,
}

fn collect_identity_entries(
    root: &Utf8Path,
    limits: &PackageLimits,
) -> Result<Vec<IdentityEntry>, ServiceError> {
    let root_metadata = std::fs::symlink_metadata(root).map_err(ServiceError::Io)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(ServiceError::Invalid(
            "package source is not a plain directory".into(),
        ));
    }

    let mut entries = Vec::new();
    let mut total_bytes = 0_u64;
    for entry in files::WalkDir::new(root) {
        let entry =
            entry.map_err(|_| ServiceError::Invalid("failed to walk package source".into()))?;
        let relative_path = entry.path().strip_prefix(root).map_err(|_| {
            ServiceError::Invalid("package source yielded an invalid relative path".into())
        })?;
        if relative_path.as_str().is_empty() {
            if entry.file_type().is_symlink() || !entry.file_type().is_dir() {
                return Err(ServiceError::Invalid(
                    "package source is not a plain directory".into(),
                ));
            }
            continue;
        }
        let relative = portable_relative_path(relative_path);
        if !is_safe_relative_path(&relative) {
            return Err(ServiceError::Invalid(
                "package source contains an unsafe relative path".into(),
            ));
        }
        let file_type = entry.file_type();
        if file_type.is_symlink() {
            return Err(ServiceError::Invalid(
                "package source contains a symbolic link".into(),
            ));
        }
        let is_dir = file_type.is_dir();
        if !is_dir && !file_type.is_file() {
            return Err(ServiceError::Invalid(
                "package source contains an unsupported entry".into(),
            ));
        }
        let metadata = std::fs::symlink_metadata(entry.path()).map_err(ServiceError::Io)?;
        if metadata.file_type().is_symlink()
            || metadata.is_dir() != is_dir
            || (!is_dir && !metadata.is_file())
        {
            return Err(ServiceError::Invalid(
                "package source changed while being inspected".into(),
            ));
        }
        let count = entries
            .len()
            .checked_add(1)
            .ok_or_else(|| ServiceError::Invalid("package entry count overflowed".into()))?;
        if let Some(limit) = limits.max_entries
            && count > limit
        {
            return Err(ServiceError::Invalid(format!(
                "package source contains too many entries: {count} > {limit}"
            )));
        }
        let size = if is_dir { 0 } else { metadata.len() };
        if !is_dir {
            total_bytes = total_bytes
                .checked_add(size)
                .ok_or_else(|| ServiceError::Invalid("package source size overflowed".into()))?;
            if let Some(limit) = limits.max_bytes
                && total_bytes > limit
            {
                return Err(ServiceError::Invalid(format!(
                    "package source exceeds {limit} bytes"
                )));
            }
        }
        // Fail closed when mtime is unavailable so later comparisons have a
        // stable snapshot to compare against.
        metadata.modified().map_err(ServiceError::Io)?;
        entries.push(IdentityEntry {
            path: entry.path().to_path_buf(),
            relative,
            is_dir,
            metadata,
        });
    }
    entries.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok(entries)
}

fn metadata_identity_matches(
    is_dir: bool,
    left: &std::fs::Metadata,
    right: &std::fs::Metadata,
) -> bool {
    if left.is_dir() != is_dir
        || right.is_dir() != is_dir
        || (!is_dir && (!left.is_file() || !right.is_file()))
        || explicit_source_mode(left, is_dir) != explicit_source_mode(right, is_dir)
        || (!is_dir && left.len() != right.len())
        || left.modified().ok() != right.modified().ok()
    {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if left.dev() != right.dev()
            || left.ino() != right.ino()
            || left.ctime() != right.ctime()
            || left.ctime_nsec() != right.ctime_nsec()
        {
            return false;
        }
    }
    true
}

fn identity_metadata_matches(
    entry: &IdentityEntry,
    metadata: &std::fs::Metadata,
    is_dir: bool,
) -> bool {
    is_dir == entry.is_dir && metadata_identity_matches(is_dir, &entry.metadata, metadata)
}

fn identity_entries_match(left: &IdentityEntry, right: &IdentityEntry) -> bool {
    left.relative == right.relative
        && left.is_dir == right.is_dir
        && metadata_identity_matches(left.is_dir, &left.metadata, &right.metadata)
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
    for entry in files::WalkDir::new(source) {
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
        let mut input = open_source_entry_nofollow(&path).map_err(|error| {
            if std::fs::symlink_metadata(&path)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
            {
                ServiceError::Invalid(format!("package source contains a symbolic link: {path}"))
            } else {
                ServiceError::Invalid(format!("failed to open package entry `{rel}`: {error}"))
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
                apply_restored_mode(&dest, explicit_source_mode(&source_metadata, true))?;
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
/// source permission bits (sanitized on apply) so directories stay
/// traversable and files keep owner executability; the sanitizer at apply
/// time strips only the dangerous bits (E15).
/// Non-Unix limitation (E15-3): Windows has no POSIX mode bits, so the mode
/// is synthesized from the read-only flag: directories use 0o555/0o755 and
/// files degrade to 0o444/0o644, so packaged scripts may lose their exec bit
/// there and exact group/other bits do not round-trip. Callers restoring on
/// non-Unix warn when exec bits are dropped instead of degrading silently.
fn explicit_source_mode(metadata: &std::fs::Metadata, is_dir: bool) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = is_dir;
        metadata.permissions().mode() & 0o777
    }
    #[cfg(not(unix))]
    {
        match (is_dir, metadata.permissions().readonly()) {
            (true, true) => 0o555,
            (true, false) => 0o755,
            (false, true) => 0o444,
            (false, false) => 0o644,
        }
    }
}

/// Opens a source entry (file or directory) without following a terminal
/// symlink on Unix (O_NOFOLLOW at use time, E15-6). Non-Unix pre-checks
/// for a symlink. Windows needs FILE_FLAG_BACKUP_SEMANTICS to open a
/// directory so the copy path can check its kind and mode on the handle.
fn open_source_entry_nofollow(path: &Utf8Path) -> Result<File, std::io::Error> {
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
            Ok(_) => {
                let mut options = std::fs::OpenOptions::new();
                options.read(true);
                #[cfg(windows)]
                {
                    use std::os::windows::fs::OpenOptionsExt as _;
                    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
                    options.custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
                }
                options.open(path)
            }
            Err(error) => Err(error),
        }
    }
}

fn verify_package_inventory(root: &Utf8Path, limits: &PackageLimits) -> Result<(), ServiceError> {
    let sbom_path = root.join("SBOM.spdx.json");
    let provenance_path = root.join("PROVENANCE.intoto.json");
    let sbom_bytes = read_bounded(&sbom_path, limits.max_metadata_bytes).map_err(|error| {
        ServiceError::Invalid(format!("package is missing SBOM.spdx.json: {error}"))
    })?;
    let provenance_bytes =
        read_bounded(&provenance_path, limits.max_metadata_bytes).map_err(|error| {
            ServiceError::Invalid(format!(
                "package is missing PROVENANCE.intoto.json: {error}"
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
        // hashes always verify AND modes always verify. An SBOM without an
        // explicit mode fails instead of verifying by hash alone, so a
        // mode-stripped package cannot downgrade to hash-only verification.
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
    for entry in files::WalkDir::new(root) {
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
            "SBOM.spdx.json" | "PROVENANCE.intoto.json"
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
    match open_source_entry_nofollow(path) {
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
    files::sanitized_metadata_mode(metadata)
}

/// SHA256 of an already-opened (already nofollow-verified) handle (E15):
/// delegates to the single shared streaming core so the verify path cannot
/// drift from the pack path. No byte cap here: the caller already bounded
/// the handle before this call.
fn hash_opened_file(file: &mut File) -> Result<String, ServiceError> {
    files::hash_opened_file_sha256(file, None, |limit| {
        std::io::Error::other(format!("package file exceeds {limit} bytes while hashing"))
    })
    .map(|(hex, _)| hex)
    .map_err(ServiceError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    /// One entry of a [`test_sbom`] `files` list: the archived name, the
    /// SHA-256 of its bytes, and the Unix mode the archive declares for it.
    /// Both the digest and the mode are verified on unpack, so a test states
    /// all three.
    fn test_sbom_file(name: &str, sha256: &str, mode: u32) -> serde_json::Value {
        serde_json::json!({
            "fileName": name,
            "checksums": [{ "checksumValue": sha256 }],
            "mode": mode,
        })
    }

    /// The SPDX document every `.pkg` archive must carry, wrapping the
    /// per-entry values from [`test_sbom_file`].
    fn test_sbom(files: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({
            "spdxVersion": "SPDX-2.3",
            "files": files,
        })
    }

    /// Appends the two mandatory metadata entries - the SPDX SBOM and the
    /// in-toto provenance statement - to a package archive and closes it.
    /// Every unpackable archive carries exactly this pair, so a test states
    /// only its own payload entries and hands the SBOM over here. Takes the
    /// writer by value because closing an archive consumes it. Unix-only
    /// like its callers: every test archiving Unix modes is `#[cfg(unix)]`.
    #[cfg(unix)]
    fn finish_archive(
        mut writer: zip::ZipWriter<File>,
        options: zip::write::SimpleFileOptions,
        sbom: &serde_json::Value,
    ) -> File {
        use std::io::Write as _;
        writer
            .start_file("SBOM.spdx.json", options.unix_permissions(0o644))
            .expect("sbom entry");
        writer
            .write_all(sbom.to_string().as_bytes())
            .expect("sbom body");
        writer
            .start_file("PROVENANCE.intoto.json", options.unix_permissions(0o644))
            .expect("provenance entry");
        writer
            .write_all(br#"{"_type":"https://in-toto.io/Statement/v1"}"#)
            .expect("provenance body");
        writer.finish().expect("archive should finish")
    }

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
        // hashes. A declared mode that mismatches fails closed, and an
        // absent mode is REJECTED instead of verifying by hash alone.
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
        let sbom_with_mode = test_sbom(vec![test_sbom_file("data.txt", &sha256, 0o644)]);
        std::fs::write(
            target.join("SBOM.spdx.json"),
            serde_json::to_vec(&sbom_with_mode).expect("sbom should serialize"),
        )
        .expect("sbom should be written");
        std::fs::write(
            target.join("PROVENANCE.intoto.json"),
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
            target.join("SBOM.spdx.json"),
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
        let sbom_bad_mode = test_sbom(vec![test_sbom_file("data.txt", &sha256, 0o1777)]);
        std::fs::write(
            target.join("SBOM.spdx.json"),
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

    fn temp_dir(name: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("package-{name}-{}", uuid::Uuid::now_v7())),
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

    #[test]
    fn package_identity_is_independent_of_creation_order() {
        let root = temp_dir("identity-order");
        let _temp_guard = TempGuard(root.clone());
        let left = root.join("left");
        let right = root.join("right");
        std::fs::create_dir_all(left.join("nested")).expect("left dirs");
        std::fs::write(left.join("alpha.txt"), b"alpha").expect("left alpha");
        std::fs::write(left.join("nested/beta.txt"), b"beta").expect("left beta");

        std::fs::create_dir_all(right.join("nested")).expect("right dirs");
        std::fs::write(right.join("nested/beta.txt"), b"beta").expect("right beta");
        std::fs::write(right.join("alpha.txt"), b"alpha").expect("right alpha");

        let left_id =
            package_identity_sha256(&left, &PackageLimits::default()).expect("left identity");
        let right_id =
            package_identity_sha256(&right, &PackageLimits::default()).expect("right identity");
        assert_eq!(left_id, right_id);
    }

    #[test]
    fn package_identity_changes_with_content_and_path() {
        let root = temp_dir("identity-content-path");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root");
        let file = root.join("before.txt");
        std::fs::write(&file, b"before").expect("file");
        let original =
            package_identity_sha256(&root, &PackageLimits::default()).expect("original identity");

        std::fs::write(&file, b"after!").expect("edited content");
        let content_edit = package_identity_sha256(&root, &PackageLimits::default())
            .expect("content-edited identity");
        assert_ne!(original, content_edit);

        std::fs::rename(&file, root.join("after.txt")).expect("rename");
        let path_edit = package_identity_sha256(&root, &PackageLimits::default())
            .expect("path-edited identity");
        assert_ne!(content_edit, path_edit);
    }

    #[cfg(unix)]
    #[test]
    fn package_identity_detects_same_size_edits_with_restored_mtime() {
        use std::os::unix::fs::MetadataExt as _;

        let root = temp_dir("identity-change-time");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root");
        let file = root.join("payload");
        std::fs::write(&file, b"AAAA").expect("initial content");
        let entries =
            collect_identity_entries(&root, &PackageLimits::default()).expect("initial entries");
        let initial = entries.first().expect("file entry");
        let before = std::fs::metadata(&file).expect("initial metadata");
        let original_modified = before.modified().expect("initial mtime");
        let original_identity =
            package_identity_sha256(&root, &PackageLimits::default()).expect("initial identity");

        std::fs::write(&file, b"BBBB").expect("same-size edit");
        std::fs::File::open(&file)
            .expect("file handle")
            .set_times(std::fs::FileTimes::new().set_modified(original_modified))
            .expect("restore original mtime");
        let after = std::fs::metadata(&file).expect("edited metadata");
        assert_eq!(before.len(), after.len());
        assert_eq!(
            before.modified().expect("initial mtime"),
            after.modified().expect("restored mtime")
        );
        assert_ne!(
            (before.ctime(), before.ctime_nsec()),
            (after.ctime(), after.ctime_nsec()),
            "ctime must record the write"
        );
        assert!(
            !identity_metadata_matches(initial, &after, false),
            "restoring mtime must not hide an in-place content edit"
        );
        let edited_identity =
            package_identity_sha256(&root, &PackageLimits::default()).expect("edited identity");
        assert_ne!(original_identity, edited_identity);
    }

    #[cfg(unix)]
    #[test]
    fn package_identity_changes_with_permission_mode() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = temp_dir("identity-mode");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root");
        let file = root.join("tool");
        std::fs::write(&file, b"same bytes").expect("file");
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644))
            .expect("initial mode");
        let before = package_identity_sha256(&root, &PackageLimits::default())
            .expect("identity before mode edit");
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755))
            .expect("executable mode");
        let after = package_identity_sha256(&root, &PackageLimits::default())
            .expect("identity after mode edit");
        assert_ne!(before, after);
    }

    #[cfg(unix)]
    #[test]
    fn package_identity_refuses_symlinks() {
        use std::os::unix::fs::symlink;
        let root = temp_dir("identity-symlink");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root");
        std::fs::write(root.join("target.txt"), b"target").expect("target");
        symlink(root.join("target.txt"), root.join("link.txt")).expect("symlink");
        let error = package_identity_sha256(&root, &PackageLimits::default())
            .expect_err("a symlink must fail closed");
        let message = error.to_string();
        assert!(message.contains("symbolic link"), "{message}");
        assert!(
            !message.contains(root.as_str()),
            "package identity errors must not expose the absolute package path"
        );
    }

    #[test]
    fn package_identity_enforces_entry_and_byte_limits() {
        let root = temp_dir("identity-limits");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(root.join("nested")).expect("nested dir");
        std::fs::write(root.join("nested/data.txt"), b"1234").expect("data");

        let entry_limited = PackageLimits {
            max_entries: Some(1),
            ..PackageLimits::default()
        };
        let error = package_identity_sha256(&root, &entry_limited)
            .expect_err("directory and file exceed one entry");
        assert!(error.to_string().contains("too many entries"), "{error}");

        let byte_limited = PackageLimits {
            max_bytes: Some(3),
            ..PackageLimits::default()
        };
        let error = package_identity_sha256(&root, &byte_limited)
            .expect_err("four bytes exceed the configured limit");
        assert!(error.to_string().contains("exceeds 3 bytes"), "{error}");
    }

    #[test]
    fn copy_dir_all_preserves_nested_package_identity() {
        // Admission snapshots must copy directories too: on Windows a plain
        // File::open fails for directory entries even when files are readable.
        let root = temp_dir("copy-identity");
        let _temp_guard = TempGuard(root.clone());
        let source = root.join("source");
        let target = root.join("target");
        std::fs::create_dir_all(source.join("templates/nested")).expect("source dirs");
        std::fs::create_dir(source.join("empty")).expect("empty source dir");
        std::fs::write(source.join("qcg.toml"), b"manifest").expect("root file");
        std::fs::write(source.join("templates/nested/page.j2"), b"hello").expect("template");
        std::fs::create_dir(&target).expect("target");
        let expected =
            package_identity_sha256(&source, &PackageLimits::default()).expect("source identity");

        copy_dir_all(&source, &target, &PackageLimits::default()).expect("copy should succeed");
        assert!(target.join("empty").is_dir());
        assert_eq!(
            std::fs::read(target.join("templates/nested/page.j2")).expect("copied template"),
            b"hello"
        );
        crate::run_package::verify(&target, &expected).expect("snapshot identity must match");

        std::fs::write(target.join("templates/nested/page.j2"), b"other").expect("tamper");
        let error = crate::run_package::verify(&target, &expected)
            .expect_err("changed snapshot must fail closed");
        assert!(error.to_string().contains("identity mismatch"), "{error}");
    }

    #[test]
    fn run_package_snapshots_nested_http_fixtures_and_rejects_tampering() {
        // Exercise the full admission snapshot path, not only the source
        // opener: copy, destination identity verification, and contract load.
        let root = temp_dir("http-snapshots");
        let _temp_guard = TempGuard(root.clone());
        let fixtures = Utf8Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("service crate is under crates")
            .join("fixtures/generators");
        for (name, nested_file) in [
            ("file-input", "templates/summary.md.j2"),
            ("assets-demo", "ui/app.js"),
            ("agent-secret-result", "prompts/agent.j2"),
        ] {
            let source = fixtures.join(name);
            let run_dir = root.join(name);
            std::fs::create_dir_all(&run_dir).expect("fresh run directory");
            let expected = package_identity_sha256(&source, &PackageLimits::default())
                .expect("source package identity");
            let original = contract::Contract::load(&source).expect("source contract");
            let pinned = crate::run_package::copy(&source, &run_dir, &expected)
                .unwrap_or_else(|_| panic!("{name} snapshot copy failed"));
            assert_eq!(pinned.sha256, original.sha256, "{name}");
            crate::run_package::load_identity(&run_dir, Some(&expected), Some(&original.sha256))
                .unwrap_or_else(|_| panic!("{name} snapshot reload failed"));

            std::fs::write(pinned.root.join(nested_file), b"tampered")
                .expect("tamper with nested snapshot file");
            let error = crate::run_package::load_identity(
                &run_dir,
                Some(&expected),
                Some(&original.sha256),
            )
            .expect_err("tampered snapshot must refuse loading");
            assert!(
                error.to_string().contains("identity mismatch"),
                "{name} snapshot tampering must fail with an identity mismatch"
            );
            assert_eq!(
                package_identity_sha256(&source, &PackageLimits::default())
                    .expect("source identity after tampering with snapshot"),
                expected,
                "{name} snapshot must not share writable bytes with its source"
            );
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
        // stores a default mode in the high word, so this fixture clears
        // that word while retaining low-word DOS attributes to prove they
        // do not count as an explicit mode.
        use std::io::Write as _;

        let root = temp_dir("unpack-no-modes");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.pkg");
        let body = b"data";
        let sha256 = hex::encode(Sha256::digest(body));
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            writer.start_file("data.txt", options).expect("data entry");
            writer.write_all(body).expect("data body");
            let sbom = test_sbom(vec![test_sbom_file("data.txt", &sha256, 0o644)]);
            finish_archive(writer, options, &sbom);
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
                    // Keep a DOS attribute (read-only) while leaving the
                    // high-word mode at zero: low-word attributes are not an
                    // explicit mode.
                    bytes[offset + 38..offset + 42].copy_from_slice(&0x01_u32.to_le_bytes());
                    stripped = true;
                }
                offset = name_end + extra_len + comment_len;
            }
            assert!(stripped, "data.txt central header should be found");
            std::fs::write(&archive, &bytes).expect("stripped archive should be writable");
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target");
        let error = unpack_package(&archive, &target, &PackageLimits::default())
            .expect_err("a mode-less entry must fail closed");
        assert!(
            error.to_string().contains("explicit Unix mode"),
            "the refusal must name the missing mode: {error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unpack_uses_explicit_mode_when_creator_system_is_dos() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;

        let root = temp_dir("unpack-dos-creator-mode");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.pkg");
        let body = b"#!/bin/sh\necho package\n";
        let sha256 = hex::encode(Sha256::digest(body));
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            writer
                .start_file("bin/tool", options.unix_permissions(0o755))
                .expect("tool entry");
            writer.write_all(body).expect("tool body");
            let sbom = test_sbom(vec![test_sbom_file("bin/tool", &sha256, 0o755)]);
            finish_archive(writer, options, &sbom);
        }
        // Change only the creator-system byte of the payload's central
        // record from Unix to DOS/FAT. The external-attributes high word
        // remains the explicit 0o100755 mode.
        {
            let mut bytes = std::fs::read(&archive).expect("archive should be readable");
            let eocd_sig = [0x50u8, 0x4b, 0x05, 0x06];
            let eocd_pos = bytes
                .windows(4)
                .rposition(|window| window == eocd_sig)
                .expect("archive should have an End of Central Directory");
            let central_size = u32::from_le_bytes(
                bytes[eocd_pos + 12..eocd_pos + 16]
                    .try_into()
                    .expect("central size"),
            ) as usize;
            let central_offset = u32::from_le_bytes(
                bytes[eocd_pos + 16..eocd_pos + 20]
                    .try_into()
                    .expect("central offset"),
            ) as usize;
            let central_end = central_offset + central_size;
            let mut offset = central_offset;
            let mut changed = false;
            while offset + 46 <= central_end {
                assert_eq!(&bytes[offset..offset + 4], b"PK\x01\x02");
                let name_len = u16::from_le_bytes(
                    bytes[offset + 28..offset + 30]
                        .try_into()
                        .expect("name length"),
                ) as usize;
                let extra_len = u16::from_le_bytes(
                    bytes[offset + 30..offset + 32]
                        .try_into()
                        .expect("extra length"),
                ) as usize;
                let comment_len = u16::from_le_bytes(
                    bytes[offset + 32..offset + 34]
                        .try_into()
                        .expect("comment length"),
                ) as usize;
                let name_start = offset + 46;
                let name_end = name_start + name_len;
                if &bytes[name_start..name_end] == b"bin/tool" {
                    assert_eq!(bytes[offset + 5], rawzip::CreatorSystem::UNIX.as_u8());
                    let external_attributes = u32::from_le_bytes(
                        bytes[offset + 38..offset + 42]
                            .try_into()
                            .expect("external attributes"),
                    );
                    assert_eq!(external_attributes >> 16, 0o100755);
                    bytes[offset + 5] = rawzip::CreatorSystem::FAT.as_u8();
                    changed = true;
                    break;
                }
                offset = name_end + extra_len + comment_len;
            }
            assert!(changed, "tool central header should be found");
            std::fs::write(&archive, bytes).expect("mutated archive should be writable");
        }

        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target should be created");
        unpack_package(&archive, &target, &PackageLimits::default())
            .expect("explicit high-word mode should unpack regardless of creator system");
        let tool = target.join("bin/tool");
        assert_eq!(std::fs::read(&tool).expect("tool content"), body);
        assert_eq!(
            std::fs::metadata(tool)
                .expect("tool metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755
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
        let archive = root.join("pkg.pkg");
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
            let sbom = test_sbom(vec![
                test_sbom_file("lib/deep/tool.sh", &sha256(tool), 0o755),
                test_sbom_file("data.txt", &sha256(data), 0o644),
            ]);
            finish_archive(writer, options, &sbom);
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target");
        // SAFETY: the UMASK_LOCK serializes the process-wide umask change
        // with every test in this module that inspects file modes.
        let previous = unsafe { libc::umask(0o077) };
        let result = unpack_package(&archive, &target, &PackageLimits::default());
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
        let archive = root.join("pkg.pkg");
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
            let sbom = test_sbom(vec![test_sbom_file("sub/file.txt", &sha256, 0o644)]);
            finish_archive(writer, options, &sbom);
        }
        let target = root.join("out");
        let outside = root.join("outside");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::create_dir_all(&outside).expect("outside");
        std::os::unix::fs::symlink(&outside, target.join("sub")).expect("planted symlink");
        let error = unpack_package(&archive, &target, &PackageLimits::default())
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
        let archive = root.join("pkg.pkg");
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
            let sbom = test_sbom(vec![test_sbom_file("link.txt", &sha256, 0o644)]);
            finish_archive(writer, options, &sbom);
        }
        let target = root.join("out");
        let outside = root.join("outside.txt");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(&outside, b"outside").expect("outside file");
        std::os::unix::fs::symlink(&outside, target.join("link.txt")).expect("planted symlink");
        let error = unpack_package(&archive, &target, &PackageLimits::default())
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
    fn unpack_streaming_take_bounds_a_lying_declared_size() {
        // The declared-size gate trusts the header, so the entry stream
        // itself is bounded with `take`: a deflated entry whose central and
        // local uncompressed sizes are patched small still decompresses to
        // its real body. Under `max_bytes` the copy must fail mid-stream
        // with the expanded-size refusal (not the post-copy size-changed
        // error), and no committed output may be left behind.
        use std::io::Write as _;

        let root = temp_dir("unpack-lying-size");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.pkg");
        let body = vec![b'A'; 100];
        let sha256 = hex::encode(Sha256::digest(&body));
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            writer
                .start_file("evil.txt", options.unix_permissions(0o644))
                .expect("evil entry");
            writer.write_all(&body).expect("evil body");
            let sbom = test_sbom(vec![test_sbom_file("evil.txt", &sha256, 0o644)]);
            finish_archive(writer, options, &sbom);
        }
        // Patch only the uncompressed-size fields of evil.txt (central
        // header +24, local header +22) down to 1 byte, keeping the
        // compressed sizes intact so the deflate stream still yields the
        // full 100-byte body.
        {
            let mut bytes = std::fs::read(&archive).expect("archive should be readable");
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
            let mut local_offset = None;
            let mut patched = false;
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
                if name_end <= bytes.len() && &bytes[name_start..name_end] == b"evil.txt" {
                    bytes[offset + 24..offset + 28].copy_from_slice(&[1, 0, 0, 0]);
                    local_offset = Some(u32::from_le_bytes(
                        bytes[offset + 42..offset + 46]
                            .try_into()
                            .expect("local offset"),
                    ) as usize);
                    patched = true;
                }
                offset = name_end + extra_len + comment_len;
            }
            assert!(patched, "evil.txt central header should be found");
            let local = local_offset.expect("evil.txt local header should be found");
            assert!(
                bytes[local] == 0x50
                    && bytes[local + 1] == 0x4b
                    && bytes[local + 2] == 0x03
                    && bytes[local + 3] == 0x04,
                "evil.txt local header should be present"
            );
            bytes[local + 22..local + 26].copy_from_slice(&[1, 0, 0, 0]);
            std::fs::write(&archive, &bytes).expect("patched archive should be writable");
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target");
        let limits = PackageLimits {
            max_bytes: Some(10),
            ..PackageLimits::default()
        };
        let error = unpack_package(&archive, &target, &limits)
            .expect_err("a lying declared size must fail on the stream bound");
        assert!(
            error.to_string().contains("archive expanded size exceeds"),
            "the refusal must name the expanded-size bound: {error}"
        );
        assert!(
            !target.join("evil.txt").exists(),
            "the bounded stream must leave no committed output behind"
        );
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
        let archive = root.join("pkg.pkg");
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
                .map(|(name, expected)| test_sbom_file(name, &sha256, *expected))
                .collect();
            finish_archive(writer, options, &test_sbom(sbom_files));
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target should be created");
        unpack_package(&archive, &target, &PackageLimits::default())
            .expect("unpack should succeed");
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
    fn default_package_limits_pin_bounded_entry_and_byte_gates() {
        // Pin the bounded defaults: entry count (1000) and expanded bytes
        // (10 MiB) fail closed without configuration, while archive and
        // metadata bounds stay explicit-only. A regression to all-`None`
        // would silently reopen the bomb/flood shapes, so the default must
        // also reject a flood archive through the real unpack path.
        use std::io::Write as _;

        let limits = PackageLimits::default();
        assert_eq!(limits.max_entries, Some(DEFAULT_PACKAGE_MAX_ENTRIES));
        assert_eq!(limits.max_entries, Some(1000));
        assert_eq!(limits.max_bytes, Some(DEFAULT_PACKAGE_MAX_BYTES));
        assert_eq!(limits.max_bytes, Some(10 * 1024 * 1024));
        assert_eq!(limits.max_metadata_bytes, None);
        assert_eq!(limits.max_archive_bytes, None);

        let root = temp_dir("default-limits-flood");
        let _temp_guard = TempGuard(root.clone());
        std::fs::create_dir_all(&root).expect("root should be created");
        let archive = root.join("pkg.pkg");
        let body = b"x";
        let sha256 = hex::encode(Sha256::digest(body));
        let mut sbom_files = Vec::new();
        {
            let file = File::create(&archive).expect("archive should be created");
            let mut writer = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            for index in 0..=DEFAULT_PACKAGE_MAX_ENTRIES {
                let name = format!("flood-{index}.txt");
                writer
                    .start_file(name.as_str(), options.unix_permissions(0o644))
                    .expect("flood entry");
                writer.write_all(body).expect("flood body");
                sbom_files.push(test_sbom_file(name.as_str(), &sha256, 0o644));
            }
            finish_archive(writer, options, &test_sbom(sbom_files));
        }
        let target = root.join("out");
        std::fs::create_dir_all(&target).expect("target should be created");
        let error = unpack_package(&archive, &target, &PackageLimits::default())
            .expect_err("the default entry bound must reject the flood");
        assert!(
            error.to_string().contains("too many entries"),
            "the refusal must name the entry bound: {error}"
        );
    }
}
