//! Bounded synchronous file I/O: reads, hashes, and atomic writes with an
//! explicit optional cap. `None` means no mechanistic limit; the caller sets
//! a max only when wanted.

use camino::{Utf8Path, Utf8PathBuf};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read as _, Write};
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique-staging nonce shared by the Unix and non-Unix, sync write paths.
static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Reads a file, enforcing an explicit size cap only when set.
pub fn read_bounded(path: &Utf8Path, max_bytes: Option<usize>) -> std::io::Result<Vec<u8>> {
    let Some(max_bytes) = max_bytes else {
        return std::fs::read(path);
    };
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(std::io::Error::other(format!(
            "file `{path}` exceeds {max_bytes} bytes"
        )));
    }
    Ok(bytes)
}

/// Single-handle verify-and-use read (E06): opens with `O_NOFOLLOW` on Unix,
/// checks the opened handle is a regular file via `fstat`, then reads bytes
/// FROM THAT FD with the optional cap. The hash (when needed) must be
/// computed over THESE bytes, never by re-opening the path. On non-Unix the
/// leaf is probed with `symlink_metadata` first and refused when it is a
/// link, then the opened handle is re-checked; the check is best-effort
/// there and callers must treat the platform as fail-closed.
pub fn read_nofollow_bounded(
    path: &Utf8Path,
    max_bytes: Option<usize>,
) -> std::io::Result<Vec<u8>> {
    let file = open_read_nofollow(path)?;
    let mut bytes = Vec::new();
    match max_bytes {
        None => {
            use std::io::Read as _;
            let mut file = file;
            file.read_to_end(&mut bytes)?;
        }
        Some(limit) => {
            use std::io::Read as _;
            let file = file;
            file.take(limit.saturating_add(1) as u64)
                .read_to_end(&mut bytes)?;
            if bytes.len() > limit {
                return Err(std::io::Error::other(format!(
                    "file `{path}` exceeds {limit} bytes"
                )));
            }
        }
    }
    Ok(bytes)
}

/// Single-handle verify-and-use read+hash (E06): opens once with
/// `O_NOFOLLOW`, validates the handle via `fstat`, reads bytes FROM THAT FD,
/// hashes THOSE bytes, and returns both. Callers must compare the digest and
/// use THESE bytes, never re-open the path for a second hash or read.
/// Non-Unix is pre+post best-effort (see `open_read_nofollow`).
pub fn read_and_hash_nofollow(
    path: &Utf8Path,
    max_bytes: Option<usize>,
) -> std::io::Result<(Vec<u8>, String, u64)> {
    let bytes = read_nofollow_bounded(path, max_bytes)?;
    use sha2::{Digest as _, Sha256};
    let digest = hex::encode(Sha256::digest(&bytes));
    let count = bytes.len() as u64;
    Ok((bytes, digest, count))
}

/// Streams a file through SHA-256, enforcing an explicit size cap only when
/// set. Returns the hex digest and byte count. On Unix the file is opened
/// with `O_NOFOLLOW` and the handle is hashed, so a terminal symlink is
/// refused instead of followed (E06). On non-Unix the leaf is probed with
/// `symlink_metadata` first and refused when it is a link; the check is
/// best-effort there and callers must treat the platform as fail-closed.
/// SHA256 streaming core over an already-opened handle (E15): the single
/// home for handle hashing so pack (`package_cmd`), unpack-verify
/// (`package`), and path hashing cannot drift between three loops. Counts
/// bytes with checked arithmetic and fails closed past `max_bytes`
/// (`None` means unbounded; the caller must have bounded the handle
/// another way then). The limit error is built by `on_limit` so each
/// caller keeps its own path-aware message without string matching.
pub fn hash_opened_file_sha256(
    file: &mut File,
    max_bytes: Option<u64>,
    on_limit: impl FnOnce(u64) -> std::io::Error,
) -> std::io::Result<(String, u64)> {
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| std::io::Error::other("file byte count overflowed"))?;
        if let Some(limit) = max_bytes
            && total > limit
        {
            return Err(on_limit(limit));
        }
        digest.update(&buffer[..read]);
    }
    Ok((hex::encode(digest.finalize()), total))
}

pub fn hash_file_sha256(path: &Utf8Path, max_bytes: Option<u64>) -> std::io::Result<(String, u64)> {
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt as _;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("file `{path}` is not a regular file"),
            ));
        }
        file
    };
    #[cfg(not(unix))]
    let mut file = {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("file `{path}` is a symbolic link"),
                ));
            }
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("file `{path}` is not a regular file"),
                ));
            }
            Ok(_) => {}
            Err(error) => return Err(error),
        }
        let file = File::open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("file `{path}` is not a regular file"),
            ));
        }
        file
    };
    hash_opened_file_sha256(&mut file, max_bytes, |limit| {
        std::io::Error::other(format!("file `{path}` exceeds {limit} bytes"))
    })
}

/// Opens a file for reading without following a terminal symlink.
/// Unix opens with `O_NOFOLLOW` (authoritative) and checks the opened
/// handle is a regular file via `fstat`, so a symlink swapped in between
/// validation and open fails with `ELOOP` instead of being followed.
/// Non-Unix has no handle that can express `O_NOFOLLOW`: the leaf is
/// probed with `symlink_metadata` first and refused when it is a link,
/// then the opened handle is re-checked; the check is best-effort there
/// and callers must treat the platform as fail-closed.
pub fn open_read_nofollow(path: &Utf8Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("file `{path}` is not a regular file"),
            ));
        }
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("file `{path}` is a symbolic link"),
                ));
            }
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("file `{path}` is not a regular file"),
                ));
            }
            Ok(_) => {}
            Err(error) => return Err(error),
        }
        let file = File::open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("file `{path}` is not a regular file"),
            ));
        }
        Ok(file)
    }
}

/// Writes bytes while enforcing an explicit size cap only when set.
pub fn write_bounded<W: Write>(
    mut writer: W,
    bytes: &[u8],
    max_bytes: Option<usize>,
    resource: &str,
) -> std::io::Result<()> {
    if let Some(limit) = max_bytes
        && bytes.len() > limit
    {
        return Err(std::io::Error::other(format!(
            "{resource} exceeds {limit} bytes"
        )));
    }
    writer.write_all(bytes)
}

/// A single directory-tree entry produced by [`WalkDir`].
#[derive(Debug)]
pub struct WalkEntry {
    path: Utf8PathBuf,
    file_type: std::fs::FileType,
    depth: usize,
}

impl WalkEntry {
    /// Full path of the entry. The walk root itself is yielded at depth 0.
    pub fn path(&self) -> &Utf8Path {
        &self.path
    }

    /// Entry type observed without following symbolic links.
    pub fn file_type(&self) -> std::fs::FileType {
        self.file_type
    }

    /// Nesting depth relative to the walk root, which has depth 0.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Metadata of the entry, following symbolic links like [`std::fs::metadata`].
    pub fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        std::fs::metadata(&self.path)
    }
}

struct WalkFrame {
    child_depth: usize,
    read: std::fs::ReadDir,
}

/// Lazily walks a directory tree top-down without following symbolic links.
/// Only a small [`walkdir`](https://crates.io/crates/walkdir)-compatible
/// surface is provided: construction from a root, [`WalkDir::min_depth`],
/// and iteration over `Result<WalkEntry, io::Error>`. Sibling order follows
/// the operating system directory order and is not sorted.
pub struct WalkDir {
    root: Option<Result<WalkEntry, std::io::Error>>,
    stack: Vec<WalkFrame>,
    min_depth: usize,
}

impl WalkDir {
    /// Starts a walk at `root`. A missing root yields a single iteration
    /// error instead of failing eagerly.
    pub fn new(root: &Utf8Path) -> Self {
        let root = match std::fs::symlink_metadata(root) {
            Ok(metadata) => Ok(WalkEntry {
                path: root.to_path_buf(),
                file_type: metadata.file_type(),
                depth: 0,
            }),
            Err(error) => Err(error),
        };
        Self {
            root: Some(root),
            stack: Vec::new(),
            min_depth: 0,
        }
    }

    /// Skips entries shallower than `depth`. The root has depth 0, so
    /// `min_depth(1)` skips the root itself while still descending into it.
    pub fn min_depth(mut self, depth: usize) -> Self {
        self.min_depth = depth;
        self
    }

    fn descend(&mut self, entry: &WalkEntry) -> std::io::Result<()> {
        if entry.file_type.is_dir() {
            let read = std::fs::read_dir(&entry.path)?;
            self.stack.push(WalkFrame {
                child_depth: entry.depth + 1,
                read,
            });
        }
        Ok(())
    }
}

impl Iterator for WalkDir {
    type Item = Result<WalkEntry, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(root) = self.root.take() {
            match root {
                Err(error) => return Some(Err(error)),
                Ok(entry) => {
                    if let Err(error) = self.descend(&entry) {
                        return Some(Err(error));
                    }
                    if entry.depth >= self.min_depth {
                        return Some(Ok(entry));
                    }
                }
            }
        }
        loop {
            let mut frame = self.stack.pop()?;
            let dir_entry = match frame.read.next() {
                None => continue,
                Some(Err(error)) => {
                    self.stack.push(frame);
                    return Some(Err(error));
                }
                Some(Ok(dir_entry)) => dir_entry,
            };
            let path = match Utf8PathBuf::from_path_buf(dir_entry.path()) {
                Ok(path) => path,
                Err(path) => {
                    self.stack.push(frame);
                    return Some(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("path is not UTF-8: {}", path.display()),
                    )));
                }
            };
            let file_type = match dir_entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) => {
                    self.stack.push(frame);
                    return Some(Err(error));
                }
            };
            let entry = WalkEntry {
                path,
                file_type,
                depth: frame.child_depth,
            };
            self.stack.push(frame);
            if let Err(error) = self.descend(&entry) {
                return Some(Err(error));
            }
            if entry.depth >= self.min_depth {
                return Some(Ok(entry));
            }
        }
    }
}

/// Creates `path` atomically: content produced by `write` is staged in a
/// uniquely named sibling file with owner-only permissions, synced, and then
/// renamed over the destination. A failed attempt removes its staging file
/// instead of littering the directory; a panic unwinds through a Drop guard
/// that reclaims the staging file as well (E13). On Unix the staging create
/// and rename run handle-relative to an `O_NOFOLLOW` parent directory fd, so
/// a parent pathname swapped after resolution cannot redirect the write.
/// Sanitizes an archived or caller-supplied POSIX mode to the safe subset
/// (E15): keeps only the `0o777` permission bits and clears the
/// world-writable bit. Setuid, setgid, and sticky bits never survive;
/// group-write and owner executability are preserved by design. Single home
/// for every pack/unpack/stage path so the formula cannot drift between
/// them.
pub fn sanitize_mode_bits(mode: u32) -> u32 {
    (mode & 0o777) & !0o002
}

/// Sanitized mode bits straight from filesystem metadata (E15): the single
/// home for the metadata-to-mode step so pack, unpack-verify, and staging
/// cannot drift between duplicated `cfg` wrappers. Unix sanitizes the
/// `st_mode` permission bits; non-Unix maps the read-only flag (read-only
/// lands `0o444`, writable lands `0o644`) since there are no POSIX bits.
pub fn sanitized_metadata_mode(metadata: &std::fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        sanitize_mode_bits(metadata.permissions().mode())
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

/// Stages one file under an already-pinned parent directory fd and
/// publishes it with its final mode already applied (E15): the single home
/// for the pack (`package_cmd`) and unpack (`package`) staging dance so the
/// two cannot drift. The staging temp is created owner-only (0600,
/// `O_EXCL | O_NOFOLLOW`), content is written and synced, then the final
/// sanitized mode lands with `fchmod` on the handle BEFORE the `renameat`
/// publication — never a post-rename chmod, so a crash can never leave a
/// published file with a staging mode. A leaf symlink at the destination is
/// refused handle-relative before the rename, and the parent directory is
/// fsynced after it. Retries name collisions up to `attempts` times, then
/// fails closed instead of overwriting. Unix only: non-Unix staging stays
/// platform-specific in each caller with its documented residual window.
#[cfg(unix)]
pub fn stage_file_at(
    parent_fd: std::os::unix::io::RawFd,
    file_name: &str,
    mode: u32,
    attempts: u32,
    write: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use std::os::unix::io::{AsRawFd as _, FromRawFd as _};
    let mode = sanitize_mode_bits(mode);
    let cstring = |value: &str| {
        std::ffi::CString::new(value).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains a NUL byte")
        })
    };
    for _ in 0..attempts.max(1) {
        let staging = format!(".{file_name}.qcg-part-{}", uuid::Uuid::now_v7().as_simple());
        let staging_name = cstring(&staging)?;
        // SAFETY: `parent_fd` is an open directory fd.
        let raw = unsafe {
            libc::openat(
                parent_fd,
                staging_name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600 as libc::mode_t as libc::c_uint,
            )
        };
        if raw < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                continue;
            }
            return Err(error);
        }
        // SAFETY: `raw` is a freshly opened owned fd.
        let mut file = unsafe { File::from_raw_fd(raw) };
        let outcome = write(&mut file)
            .and_then(|()| {
                // SAFETY: the staging fd is valid and owned by `file`.
                let ret = unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) };
                if ret != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            })
            .and_then(|()| file.sync_all());
        drop(file);
        let cleanup = |result: std::io::Result<()>| {
            if result.is_err() {
                // SAFETY: `parent_fd` is an open directory fd.
                unsafe {
                    libc::unlinkat(parent_fd, staging_name.as_ptr(), 0);
                }
            }
            result
        };
        if let Err(error) = outcome {
            return cleanup(Err(error));
        }
        // Refuse a leaf symlink handle-relative before publishing (E15).
        let target = cstring(file_name)?;
        // SAFETY: fds and names are valid.
        let mut existing: libc::stat = unsafe { std::mem::zeroed() };
        let stat_rc = unsafe {
            libc::fstatat(
                parent_fd,
                target.as_ptr(),
                &mut existing,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if stat_rc == 0 {
            let is_link = (existing.st_mode as u32 & 0o170000) == 0o120000;
            if is_link {
                // SAFETY: `parent_fd` is an open directory fd.
                unsafe {
                    libc::unlinkat(parent_fd, staging_name.as_ptr(), 0);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("refusing to replace symbolic link `{file_name}`"),
                ));
            }
        } else {
            let stat_error = std::io::Error::last_os_error();
            if stat_error.kind() != std::io::ErrorKind::NotFound {
                // SAFETY: `parent_fd` is an open directory fd.
                unsafe {
                    libc::unlinkat(parent_fd, staging_name.as_ptr(), 0);
                }
                return Err(stat_error);
            }
        }
        // SAFETY: `parent_fd` is an open directory fd; both names are valid.
        if unsafe { libc::renameat(parent_fd, staging_name.as_ptr(), parent_fd, target.as_ptr()) }
            < 0
        {
            let error = std::io::Error::last_os_error();
            // SAFETY: `parent_fd` is an open directory fd.
            unsafe {
                libc::unlinkat(parent_fd, staging_name.as_ptr(), 0);
            }
            return Err(error);
        }
        // SAFETY: `parent_fd` is an open directory fd.
        if unsafe { libc::fsync(parent_fd) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(());
    }
    Err(std::io::Error::other(
        "package staging collided repeatedly; refusing to overwrite",
    ))
}

pub fn write_file_atomic(
    path: &Utf8Path,
    write: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        unix_atomic_write(path, write)
    }
    #[cfg(not(unix))]
    {
        write_file_atomic_path(path, write)
    }
}

#[cfg(unix)]
fn unix_atomic_write(
    path: &Utf8Path,
    write: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::io::{FromRawFd as _, RawFd};

    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other(format!("`{path}` has no parent directory")))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other(format!("`{path}` has no file name")))?;
    let cstring = |value: &str| {
        CString::new(value).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains a NUL byte")
        })
    };
    // Resolve OS-level symlinked prefixes once (macOS `/var` and `/tmp`),
    // then require the canonical tail to match the supplied components
    // exactly: a symlink anywhere inside the supplied path resolves to a
    // different name and is refused. The walk below then opens the
    // canonical parent from `/` with O_NOFOLLOW at every component, so a
    // swap after validation cannot redirect the staging open or the rename
    // (E13). Only absolute normal paths are accepted; callers pass trusted
    // internal locations.
    let canonical_parent = std::fs::canonicalize(parent.as_std_path())?;
    let canonical_parent = Utf8PathBuf::from_path_buf(canonical_parent)
        .map_err(|_| std::io::Error::other("canonical parent is not UTF-8"))?;
    let supplied: Vec<&str> = parent
        .components()
        .filter_map(|component| match component {
            camino::Utf8Component::Normal(part) => Some(part),
            _ => None,
        })
        .collect();
    let canonical: Vec<&str> = canonical_parent
        .components()
        .filter_map(|component| match component {
            camino::Utf8Component::Normal(part) => Some(part),
            _ => None,
        })
        .collect();
    if canonical.len() < supplied.len()
        || canonical[canonical.len() - supplied.len()..] != supplied[..]
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("parent `{parent}` contains a symbolic link"),
        ));
    }
    let mut dir: RawFd = {
        let root = cstring("/")?;
        // SAFETY: `root` is a valid NUL-terminated path.
        let opened = unsafe {
            libc::open(
                root.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if opened < 0 {
            return Err(std::io::Error::last_os_error());
        }
        opened
    };
    let mut parts = Vec::new();
    for component in canonical_parent.components() {
        match component {
            camino::Utf8Component::RootDir => {}
            camino::Utf8Component::Normal(part) => parts.push(part.to_string()),
            _ => {
                // SAFETY: `dir` is owned here.
                unsafe {
                    libc::close(dir);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("`{parent}` is not an absolute normal path"),
                ));
            }
        }
    }
    for part in &parts {
        let name = match cstring(part) {
            Ok(name) => name,
            Err(error) => {
                // SAFETY: `dir` is owned here.
                unsafe {
                    libc::close(dir);
                }
                return Err(error);
            }
        };
        // SAFETY: `dir` is an open directory fd and `name` is valid.
        let next = unsafe {
            libc::openat(
                dir,
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        // SAFETY: the previous descriptor is no longer needed.
        unsafe {
            libc::close(dir);
        }
        if next < 0 {
            return Err(std::io::Error::last_os_error());
        }
        dir = next;
    }
    let result = (|| -> std::io::Result<()> {
        let pid = std::process::id();
        for _ in 0..100 {
            let nonce = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
            let staging = cstring(&format!(".{file_name}.qcg-part-{pid}-{nonce}"))?;
            // Panic-safe staging ownership: a panic in `write` unwinds
            // through this guard and reclaims the staging file instead of
            // littering the directory (E13).
            struct StagingGuard {
                dir: libc::c_int,
                staging: std::ffi::CString,
                armed: bool,
            }
            impl Drop for StagingGuard {
                fn drop(&mut self) {
                    if self.armed {
                        // SAFETY: `dir` is an open directory fd and
                        // `staging` is valid for the guard's lifetime.
                        unsafe {
                            libc::unlinkat(self.dir, self.staging.as_ptr(), 0);
                        }
                    }
                }
            }
            // SAFETY: `dir` is an open directory fd and `staging` is valid.
            let fd = unsafe {
                libc::openat(
                    dir,
                    staging.as_ptr(),
                    libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW,
                    0o600 as libc::c_uint,
                )
            };
            if fd < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    continue;
                }
                return Err(error);
            }
            // SAFETY: the fd is freshly opened and owned here.
            let mut file = unsafe { File::from_raw_fd(fd) };
            let mut guard = StagingGuard {
                dir,
                staging: staging.clone(),
                armed: true,
            };
            let outcome = write(&mut file).and_then(|()| file.sync_all());
            drop(file);
            match outcome {
                Ok(()) => {
                    let target = cstring(file_name)?;
                    // Fail closed on a leaf symlink: refuse to replace it,
                    // symmetric with the gateway. Checked handle-relative
                    // with AT_SYMLINK_NOFOLLOW. A swap between this check
                    // and the rename below remains a residual window; the
                    // parent directory is service-owned for these internal
                    // paths, so only a writer with directory write access
                    // could plant it (E13).
                    // SAFETY: `dir` is an open directory fd; `target` is valid.
                    let mut existing: libc::stat = unsafe { std::mem::zeroed() };
                    let stat_rc = unsafe {
                        libc::fstatat(
                            dir,
                            target.as_ptr(),
                            &mut existing,
                            libc::AT_SYMLINK_NOFOLLOW,
                        )
                    };
                    if stat_rc == 0 {
                        // S_ISLNK test without pulling in extra helpers.
                        let is_link = (existing.st_mode as u32 & 0o170000) == 0o120000;
                        if is_link {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::PermissionDenied,
                                format!("refusing to replace symbolic link `{path}`"),
                            ));
                        }
                    } else {
                        let stat_error = std::io::Error::last_os_error();
                        if stat_error.kind() != std::io::ErrorKind::NotFound {
                            return Err(stat_error);
                        }
                    }
                    // SAFETY: `dir` is an open directory fd; both names are valid.
                    if unsafe { libc::renameat(dir, guard.staging.as_ptr(), dir, target.as_ptr()) }
                        < 0
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                    // Persist the directory entry; a lost fsync can lose the
                    // rename on crash, so surface it.
                    // SAFETY: `dir` is an open directory fd.
                    if unsafe { libc::fsync(dir) } < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    guard.armed = false;
                    return Ok(());
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
        Err(std::io::Error::other(format!(
            "failed to stage atomic write for `{path}`"
        )))
    })();
    // SAFETY: `dir` was opened above and is owned here.
    unsafe {
        libc::close(dir);
    }
    result
}

#[cfg(not(unix))]
fn write_file_atomic_path(
    path: &Utf8Path,
    write: impl FnOnce(&mut File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other(format!("`{path}` has no parent directory")))?;
    let pid = std::process::id();
    // Fail closed on a missing leaf (C-4): a default name would stage
    // under a guessed leaf instead of refusing.
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "atomic write path has no file name",
        )
    })?;
    for _ in 0..100 {
        let nonce = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
        let staging = parent.join(format!(".{file_name}.qcg-part-{pid}-{nonce}"));
        let mut options = File::options();
        options.write(true).create_new(true);
        let mut file = match options.open(&staging) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let outcome = write(&mut file).and_then(|()| file.sync_all());
        drop(file);
        match outcome {
            Ok(()) => {
                // Fail closed on a leaf symlink, symmetric with the gateway
                // and the Unix handle-relative check above.
                match std::fs::symlink_metadata(path) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        let refusal = std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            format!("refusing to replace symbolic link `{path}`"),
                        );
                        return Err(cleanup_staging(&staging, refusal));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(cleanup_staging(&staging, error)),
                }
                if let Err(error) = std::fs::rename(&staging, path) {
                    // The rename failed after the content was staged:
                    // reclaim the staging file instead of leaving it
                    // behind (D05). The destination is untouched.
                    return Err(cleanup_staging(&staging, error));
                }
                return Ok(());
            }
            Err(error) => return Err(cleanup_staging(&staging, error)),
        }
    }
    Err(std::io::Error::other(format!(
        "failed to stage atomic write for `{path}`"
    )))
}

/// Reclaims a staging file after a failed write. When the removal itself
/// fails, the leftover path and its error are attached to the returned
/// error so the remaining file stays observable instead of silently
/// disappearing (D05).
#[cfg(not(unix))]
fn cleanup_staging(staging: &Utf8Path, error: std::io::Error) -> std::io::Error {
    match std::fs::remove_file(staging) {
        Ok(()) => error,
        Err(cleanup_error) => std::io::Error::new(
            error.kind(),
            format!(
                "{error}; additionally failed to remove staging file `{staging}`: {cleanup_error}"
            ),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;

    fn temp_file(name: &str, bytes: &[u8]) -> Utf8PathBuf {
        let path = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-fs-test-{name}-{}", std::process::id())),
        )
        .expect("temporary path must be UTF-8");
        std::fs::write(&path, bytes).expect("fixture should be writable");
        path
    }

    #[test]
    fn read_bounded_honors_an_explicit_cap() {
        let path = temp_file("read", b"hello");
        assert_eq!(read_bounded(&path, None).expect("uncapped read"), b"hello");
        assert_eq!(read_bounded(&path, Some(5)).expect("exact cap"), b"hello");
        let error = read_bounded(&path, Some(4)).expect_err("over-cap read must fail");
        assert!(error.to_string().contains("exceeds"), "{error}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_bounded_reports_missing_files() {
        let missing = Utf8PathBuf::from_path_buf(std::env::temp_dir().join("qcg-fs-test-missing"))
            .expect("temporary path must be UTF-8");
        let _ = std::fs::remove_file(&missing);
        read_bounded(&missing, None).expect_err("missing file must fail");
    }

    #[test]
    fn hash_reports_digest_and_count_with_optional_cap() {
        let path = temp_file("hash", b"abc");
        let (hex, count) = hash_file_sha256(&path, None).expect("hash should succeed");
        assert_eq!(count, 3);
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let (hex_capped, count_capped) =
            hash_file_sha256(&path, Some(3)).expect("exact cap should pass");
        assert_eq!((hex_capped.as_str(), count_capped), (hex.as_str(), 3));
        let error = hash_file_sha256(&path, Some(2)).expect_err("over-cap hash must fail");
        assert!(error.to_string().contains("exceeds"), "{error}");
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn hash_refuses_a_symlink_leaf_without_following_it() {
        // E06: the hash opens with O_NOFOLLOW, so a symlink leaf is refused
        // even when its target holds hashable bytes.
        let dir = test_dir("hash-symlink");
        let target = dir.join("target.txt");
        std::fs::write(&target, b"abc").expect("target should be writable");
        let link = dir.join("link.txt");
        std::os::unix::fs::symlink(&target, &link).expect("symlink should be created");
        let error = hash_file_sha256(&link, None).expect_err("a symlink leaf must be refused");
        assert!(
            error.kind() == std::io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(libc::ELOOP),
            "the refusal must come from O_NOFOLLOW, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_bounded_enforces_the_cap() {
        let mut sink = Vec::new();
        write_bounded(&mut sink, b"hello", Some(5), "test").expect("exact cap should pass");
        assert_eq!(sink, b"hello");
        let mut sink = Vec::new();
        let error = write_bounded(&mut sink, b"hello!", Some(5), "test")
            .expect_err("over-cap write must fail");
        assert!(error.to_string().contains("exceeds"), "{error}");
        let mut sink = Vec::new();
        write_bounded(&mut sink, b"hello!", None, "test").expect("uncapped write should pass");
    }

    #[test]
    fn write_file_atomic_replaces_destination_content() {
        let dir = test_dir("atomic-ok");
        let path = dir.join("outputs.json");
        std::fs::write(&path, b"old").expect("fixture should be writable");
        write_file_atomic(&path, |file| {
            use std::io::Write as _;
            file.write_all(b"new")
        })
        .expect("atomic write should succeed");
        assert_eq!(
            std::fs::read(&path).expect("result should be readable"),
            b"new"
        );
        assert!(
            staging_files(&dir).is_empty(),
            "no staging files should remain"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_file_atomic_removes_staging_on_failure() {
        let dir = test_dir("atomic-fail");
        let path = dir.join("outputs.json");
        let error = write_file_atomic(&path, |_file| {
            Err(std::io::Error::other("synthetic write failure"))
        })
        .expect_err("failing write must fail");
        assert!(
            error.to_string().contains("synthetic write failure"),
            "{error}"
        );
        assert!(
            !path.exists(),
            "destination must not appear after a failed write"
        );
        assert!(
            staging_files(&dir).is_empty(),
            "failed staging files must be removed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_file_atomic_removes_staging_when_rename_fails() {
        // D05: a successful callback is not a successful write. When the
        // rename over the destination fails (here: the destination is a
        // directory), the staging file must be reclaimed and the
        // destination left untouched.
        let dir = test_dir("atomic-rename-fail");
        let path = dir.join("outputs.json");
        std::fs::create_dir(&path).expect("destination directory should be created");
        let error = write_file_atomic(&path, |file| {
            use std::io::Write as _;
            file.write_all(b"new")
        })
        .expect_err("rename over a directory must fail");
        assert!(!error.to_string().is_empty());
        assert!(path.is_dir(), "destination must be unchanged");
        assert!(
            staging_files(&dir).is_empty(),
            "a failed rename must not leave staging files"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn write_file_atomic_rejects_a_swapped_parent() {
        // A parent directory replaced by a symlink after the target path
        // was formed must not redirect the atomic write outside the
        // intended directory.
        let dir = test_dir("atomic-parent-swap");
        let outside = test_dir("atomic-parent-swap-outside");
        std::fs::create_dir_all(dir.join("sub")).expect("sub should be created");
        let path = dir.join("sub/outputs.json");
        std::fs::rename(dir.join("sub"), dir.join("sub-real")).expect("rename sub");
        std::os::unix::fs::symlink(&outside, dir.join("sub")).expect("symlink sub");
        write_file_atomic(&path, |file| {
            use std::io::Write as _;
            file.write_all(b"escape")
        })
        .expect_err("a swapped parent must not be followed");
        assert!(
            !outside.join("outputs.json").exists(),
            "the write must not escape the intended directory"
        );
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    #[test]
    fn write_file_atomic_rejects_parentless_paths() {
        let error = write_file_atomic(Utf8Path::new(""), |_file| Ok(()))
            .expect_err("parentless path must fail");
        assert!(error.to_string().contains("no parent directory"), "{error}");
    }

    #[test]
    fn write_file_atomic_refuses_a_leaf_symlink() {
        // E13-8: replacing a leaf symlink must fail closed, symmetric with
        // the gateway, and leave no staging residue.
        let dir = test_dir("atomic-leaf-symlink");
        let outside = test_dir("atomic-leaf-symlink-outside");
        let target_file = outside.join("secret.txt");
        std::fs::write(&target_file, b"secret").expect("outside file should be writable");
        let path = dir.join("outputs.json");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target_file, &path).expect("leaf symlink should be created");
        #[cfg(not(unix))]
        {
            // Without symlink privileges, exercise the same refusal path via
            // best-effort creation; skip when unsupported.
            if std::os::windows::fs::symlink_file(&target_file, &path).is_err() {
                let _ = std::fs::remove_dir_all(&dir);
                let _ = std::fs::remove_dir_all(&outside);
                return;
            }
        }
        let error = write_file_atomic(&path, |file| {
            use std::io::Write as _;
            file.write_all(b"new")
        })
        .expect_err("a leaf symlink must be refused");
        assert!(
            error.to_string().contains("symbolic link"),
            "the refusal must name the symlink: {error}"
        );
        assert_eq!(
            std::fs::read(&target_file).expect("outside file should be readable"),
            b"secret",
            "the link target must stay untouched"
        );
        assert!(
            staging_files(&dir).is_empty(),
            "a refused replace must not leave staging files"
        );
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&outside);
    }

    fn test_dir(name: &str) -> Utf8PathBuf {
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-fs-test-{name}-{}", std::process::id())),
        )
        .expect("temporary path must be UTF-8");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("test directory should be creatable");
        dir
    }

    fn staging_files(dir: &Utf8Path) -> Vec<Utf8PathBuf> {
        std::fs::read_dir(dir)
            .expect("test directory should be readable")
            .filter_map(|entry| entry.ok())
            .map(|entry| {
                Utf8PathBuf::from_path_buf(entry.path()).expect("temporary path must be UTF-8")
            })
            .filter(|path| {
                path.file_name().is_some_and(|name| {
                    (name.starts_with('.') && name.ends_with(".tmp")) || name.contains(".qcg-part-")
                })
            })
            .collect()
    }

    #[test]
    fn walk_dir_yields_tree_top_down_with_depths() {
        let dir = test_dir("walk-basic");
        std::fs::create_dir_all(dir.join("sub/nested")).expect("fixture dirs should be created");
        std::fs::write(dir.join("top.txt"), b"top").expect("fixture should be writable");
        std::fs::write(dir.join("sub/mid.txt"), b"mid").expect("fixture should be writable");
        std::fs::write(dir.join("sub/nested/deep.txt"), b"deep")
            .expect("fixture should be writable");

        let entries: Vec<WalkEntry> = WalkDir::new(&dir)
            .collect::<Result<_, _>>()
            .expect("walk should succeed");
        // Root plus two directories plus three files.
        assert_eq!(entries.len(), 6);
        assert_eq!(entries[0].depth(), 0);
        assert!(entries[0].file_type().is_dir());
        let mut by_path: Vec<(String, usize, bool)> = entries
            .iter()
            .map(|entry| {
                (
                    entry
                        .path()
                        .strip_prefix(&dir)
                        .expect("entry lives under root")
                        .as_str()
                        .replace(std::path::MAIN_SEPARATOR, "/"),
                    entry.depth(),
                    entry.file_type().is_dir(),
                )
            })
            .collect();
        by_path.sort();
        assert_eq!(
            by_path,
            vec![
                ("".to_string(), 0, true),
                ("sub".to_string(), 1, true),
                ("sub/mid.txt".to_string(), 2, false),
                ("sub/nested".to_string(), 2, true),
                ("sub/nested/deep.txt".to_string(), 3, false),
                ("top.txt".to_string(), 1, false),
            ]
        );
        // Parents precede their children.
        let position = |name: &str| by_path.iter().position(|entry| entry.0 == name);
        assert!(position("sub") < position("sub/mid.txt"));
        assert!(position("sub/nested") < position("sub/nested/deep.txt"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn walk_dir_min_depth_skips_root_but_descends() {
        let dir = test_dir("walk-min-depth");
        std::fs::write(dir.join("top.txt"), b"top").expect("fixture should be writable");

        let entries: Vec<WalkEntry> = WalkDir::new(&dir)
            .min_depth(1)
            .collect::<Result<_, _>>()
            .expect("walk should succeed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].depth(), 1);
        assert_eq!(
            entries[0]
                .path()
                .strip_prefix(&dir)
                .expect("entry lives under root")
                .as_str(),
            "top.txt"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn walk_dir_reports_missing_root_as_iteration_error() {
        let missing = test_dir("walk-missing");
        let _ = std::fs::remove_dir_all(&missing);
        let mut walk = WalkDir::new(&missing);
        assert!(
            walk.next()
                .expect("missing root must yield an error")
                .is_err()
        );
        assert!(walk.next().is_none(), "walk must end after the error");
    }

    #[cfg(unix)]
    #[test]
    fn walk_dir_never_follows_symbolic_links() {
        use std::os::unix::fs::symlink;

        let dir = test_dir("walk-symlink");
        std::fs::create_dir_all(dir.join("real")).expect("fixture dir should be created");
        std::fs::write(dir.join("real/file.txt"), b"data").expect("fixture should be writable");
        symlink(dir.join("real"), dir.join("linked")).expect("symlink should be created");
        symlink("nowhere-missing", dir.join("dangling")).expect("symlink should be created");

        let entries: Vec<WalkEntry> = WalkDir::new(&dir)
            .min_depth(1)
            .collect::<Result<_, _>>()
            .expect("walk should succeed");
        let names: Vec<String> = entries
            .iter()
            .map(|entry| {
                entry
                    .path()
                    .strip_prefix(&dir)
                    .expect("entry lives under root")
                    .as_str()
                    .to_string()
            })
            .collect();
        assert!(names.contains(&"linked".to_string()));
        assert!(names.contains(&"dangling".to_string()));
        // The linked tree is not descended into: no nested copy appears.
        assert!(
            !names.iter().any(|name| name.starts_with("linked/")),
            "symlinked directories must not be traversed: {names:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
