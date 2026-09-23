//! Handle-relative filesystem primitives used on Unix so no pathname
//! between validation and use can be replaced by a symlink (E13a).
//!
//! Workspace-scoped containment: `ParentHandle` below is workspace-scoped
//! (containment resolution plus staging plus mode commit on one open parent
//! fd), while `qcg-fs::unix_atomic_write` is for trusted internal paths
//! with no workspace containment. Do not merge them without a containment
//! review (E13).

/// Handle-relative filesystem primitives used on Unix so no pathname
/// between validation and use can be replaced by a symlink (E13a).
use camino::{Utf8Path, Utf8PathBuf};

use super::fs::create_dest_dir;
use std::ffi::CString;
use std::os::unix::io::{FromRawFd as _, RawFd};

fn cstr(value: &str) -> std::io::Result<CString> {
    CString::new(value).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains a NUL byte")
    })
}

fn components(path: &str) -> std::io::Result<Vec<&str>> {
    let mut parts = Vec::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." || part == ".." || part.contains('\\') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsafe relative path `{path}`"),
            ));
        }
        parts.push(part);
    }
    Ok(parts)
}

/// Opens the workspace root without following a symlink planted at the
/// root itself. Exposed for resolve-time parent validation walks (E13).
pub(crate) fn open_root(workspace: &Utf8Path) -> std::io::Result<RawFd> {
    let path = cstr(workspace.as_str())?;
    // SAFETY: `path` is a valid NUL-terminated string for this call.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(fd)
    }
}

/// Opens one child directory without following symlinks: `ELOOP` on a
/// symlinked component, `ENOTDIR` on a non-directory. Exposed for
/// resolve-time parent validation; the walk stops at the first missing
/// component (E13).
pub(crate) fn open_dir_no_follow(parent: RawFd, name: &str) -> std::io::Result<RawFd> {
    openat(
        parent,
        name,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        0,
    )
}

/// Closes an owned directory fd. Exposed so resolve-time validation
/// walks can release the handles they open (E13).
pub(crate) fn close_fd(fd: RawFd) {
    close(fd)
}

/// Reports whether the leaf of a workspace-relative path is a symlink,
/// inspected handle-relative without following anything (E13). A
/// missing leaf (or a parent that vanished concurrently) reads as
/// absent; any other inspection failure propagates so callers fail
/// closed.
pub(crate) fn leaf_is_symlink(workspace: &Utf8Path, relative: &str) -> std::io::Result<bool> {
    let (parent, leaf) = match open_parent(workspace, relative, false) {
        Ok(opened) => opened,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let result = is_symlink(parent, &leaf);
    close(parent);
    result
}

fn openat(
    parent: RawFd,
    name: &str,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> std::io::Result<RawFd> {
    let name = cstr(name)?;
    // SAFETY: `parent` is an open directory fd and `name` is valid.
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            flags | libc::O_CLOEXEC,
            mode as libc::c_uint,
        )
    };
    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(fd)
    }
}

fn close(fd: RawFd) {
    // SAFETY: the fd is owned by this module.
    unsafe {
        libc::close(fd);
    }
}

fn is_symlink(parent: RawFd, name: &str) -> std::io::Result<bool> {
    let name = cstr(name)?;
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `parent` is an open directory fd and `stat` is writable.
    let rc = unsafe { libc::fstatat(parent, name.as_ptr(), &mut stat, libc::AT_SYMLINK_NOFOLLOW) };
    if rc != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::NotFound {
            return Ok(false);
        }
        return Err(error);
    }
    Ok((stat.st_mode & libc::S_IFMT) == libc::S_IFLNK)
}

/// Identity of an opened parent directory (`st_dev`, `st_ino`).
/// Pinned at open and re-verified before mutating operations on the
/// same handle, mirroring `ParentHandle` commit-time verification.
fn parent_dev_ino(fd: RawFd) -> std::io::Result<(u64, u64)> {
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `fd` is an open fd owned by the caller.
    if unsafe { libc::fstat(fd, &mut stat) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((stat.st_dev as u64, stat.st_ino as u64))
}

/// Re-verifies the pinned parent identity before an unlink or reclaim.
/// A mismatch fails closed: the handle no longer denotes the validated
/// directory.
fn verify_parent_dev_ino(fd: RawFd, dev: u64, ino: u64) -> std::io::Result<()> {
    let (current_dev, current_ino) = parent_dev_ino(fd)?;
    if (current_dev, current_ino) != (dev, ino) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "parent directory was replaced during removal; refusing",
        ));
    }
    Ok(())
}

/// Walks `relative` from the workspace root without following symlinks,
/// optionally creating missing directories, and returns the parent fd
/// plus the leaf name.
fn open_parent(
    workspace: &Utf8Path,
    relative: &str,
    create: bool,
) -> std::io::Result<(RawFd, String)> {
    let parts = components(relative)?;
    let Some((leaf, parents)) = parts.split_last() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "empty path",
        ));
    };
    let mut dir = open_root(workspace)?;
    for part in parents {
        let next = match openat(
            dir,
            part,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
            0,
        ) {
            Ok(fd) => fd,
            Err(error) if create && error.kind() == std::io::ErrorKind::NotFound => {
                let name = match cstr(part) {
                    Ok(name) => name,
                    Err(error) => {
                        close(dir);
                        return Err(error);
                    }
                };
                // Created workspace directories use the same 0755
                // default the package layer applies, not 0777 (E15).
                if unsafe { libc::mkdirat(dir, name.as_ptr(), 0o755) } < 0 {
                    let error = std::io::Error::last_os_error();
                    // A concurrent writer creating the same missing
                    // parent races here with EEXIST: re-open instead of
                    // failing (E13).
                    if error.kind() != std::io::ErrorKind::AlreadyExists {
                        close(dir);
                        return Err(error);
                    }
                }
                match openat(
                    dir,
                    part,
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
                    0,
                ) {
                    Ok(fd) => {
                        // The mkdir mode above passes through the umask,
                        // so set the intended mode explicitly on the
                        // opened handle instead of trusting creation
                        // (E13). A concurrent chmod race here can only
                        // tighten or loosen a directory the writer owns;
                        // the leaf checks below still gate the write.
                        // SAFETY: `fd` is a freshly opened directory.
                        if unsafe { libc::fchmod(fd, 0o755) } < 0 {
                            let error = std::io::Error::last_os_error();
                            close(fd);
                            close(dir);
                            return Err(error);
                        }
                        fd
                    }
                    Err(error) => {
                        close(dir);
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                close(dir);
                return Err(error);
            }
        };
        close(dir);
        dir = next;
    }
    Ok((dir, (*leaf).to_string()))
}

pub(crate) fn open_read(workspace: &Utf8Path, relative: &str) -> std::io::Result<std::fs::File> {
    let (dir, leaf) = open_parent(workspace, relative, false)?;
    let fd = openat(dir, &leaf, libc::O_RDONLY | libc::O_NOFOLLOW, 0);
    close(dir);
    // SAFETY: the returned fd is a fresh owned descriptor.
    Ok(unsafe { std::fs::File::from_raw_fd(fd?) })
}

/// Unlinks the leaf of `resolved` relative to the workspace root with
/// `O_NOFOLLOW` at every component. A symlinked leaf is refused so a
/// swapped source can never redirect the removal (E13). The parent
/// directory identity (`dev`/`ino`) is pinned at open and re-verified
/// on the same handle before the unlink, mirroring the commit-time
/// check: the walk is handle-relative so no pathname sits between
/// validation and use, and the pinning closes any handle-reuse window.
/// A directory rename that moves the already-opened parent object
/// itself would keep the same identity but requires directory write
/// access the boundary does not grant to non-cooperative writers.
pub(crate) fn unlink(workspace: &Utf8Path, resolved: &Utf8Path) -> std::io::Result<()> {
    let relative = resolved.strip_prefix(workspace).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "target is outside the workspace",
        )
    })?;
    let (parent, leaf) = open_parent(workspace, relative.as_str(), false)?;
    let (dev, ino) = parent_dev_ino(parent)?;
    let result = (|| -> std::io::Result<()> {
        verify_parent_dev_ino(parent, dev, ino)?;
        if is_symlink(parent, &leaf)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "target is a symbolic link",
            ));
        }
        // Re-verify after the leaf check so a swap racing the check
        // cannot redirect the removal below.
        verify_parent_dev_ino(parent, dev, ino)?;
        let name = cstr(&leaf)?;
        // SAFETY: `parent` is an open directory fd and `name` is valid.
        if unsafe { libc::unlinkat(parent, name.as_ptr(), 0) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // Persist the removal; a lost directory fsync can lose it on
        // crash, so surface it (E13).
        // SAFETY: `parent` is an open directory fd.
        if unsafe { libc::fsync(parent) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    })();
    close(parent);
    result
}

/// Walks a workspace file or directory tree with `O_NOFOLLOW` at every
/// component, enforcing the byte and entry limits (E13).
pub(crate) fn bounded_tree_stats(
    workspace: &Utf8Path,
    relative: &str,
    limit: Option<usize>,
    count_limit: Option<usize>,
) -> std::io::Result<()> {
    if relative.is_empty() {
        let root = open_root(workspace)?;
        return walk_dir_contents(root, None, limit, count_limit);
    }
    let (parent, leaf) = open_parent(workspace, relative, false)?;
    let target = cstr(&leaf)?;
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `parent` is an open directory fd and `stat` is writable.
    if unsafe {
        libc::fstatat(
            parent,
            target.as_ptr(),
            &mut stat,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } < 0
    {
        let error = std::io::Error::last_os_error();
        close(parent);
        return Err(error);
    }
    let kind = stat.st_mode & libc::S_IFMT;
    if kind == libc::S_IFLNK {
        close(parent);
        return Err(symlink_error());
    }
    if kind == libc::S_IFREG {
        close(parent);
        let size = usize::try_from(stat.st_size)
            .map_err(|_| std::io::Error::other("file input size is invalid"))?;
        if limit.is_some_and(|limit| size > limit) {
            return Err(oversize(limit));
        }
        return Ok(());
    }
    if kind != libc::S_IFDIR {
        close(parent);
        return Err(not_file_or_dir());
    }
    let root = openat(
        parent,
        &leaf,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        0,
    );
    close(parent);
    walk_dir_contents(root?, None, limit, count_limit)
}

fn ensure_regular_file(fd: RawFd) -> std::io::Result<()> {
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `fd` is an open descriptor and `stat` is writable.
    if unsafe { libc::fstat(fd, &mut stat) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "file input must be a regular file",
        ));
    }
    Ok(())
}

fn symlink_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "file input must not contain symbolic links",
    )
}

fn not_file_or_dir() -> std::io::Error {
    std::io::Error::other("file input is not a file or directory")
}

fn oversize(limit: Option<usize>) -> std::io::Error {
    match limit {
        Some(limit) => std::io::Error::other(format!("file input exceeds {limit} bytes")),
        None => std::io::Error::other("file input exceeds the configured limit"),
    }
}

fn too_many_entries(count_limit: Option<usize>) -> std::io::Error {
    match count_limit {
        Some(limit) => {
            std::io::Error::other(format!("file input contains more than {limit} entries"))
        }
        None => std::io::Error::other("file input contains more entries than allowed"),
    }
}

/// Copies one regular file into `dest`, enforcing the cumulative byte
/// limit DURING the copy: `total` tracks all bytes copied by the
/// calling tree walk, and every chunk checks before writing, so a file
/// swapped for a larger one after the stat check cannot open a bloat
/// window (E13). Counter overflow fails closed instead of saturating.
/// The destination mode is set explicitly to owner-only after the copy
/// instead of inheriting the umask (E15).
fn copy_file(
    source: &mut std::fs::File,
    dest: &Utf8Path,
    limit: Option<usize>,
    total: &mut usize,
) -> std::io::Result<()> {
    use std::io::{Read as _, Write as _};
    if let Some(parent) = dest.parent() {
        create_dest_dir(parent)?;
    }
    // Never follow a pre-planted terminal symlink in the snapshot
    // destination: O_NOFOLLOW makes the open fail instead of writing
    // through the link (E13). Fresh destinations are created exclusive
    // at 0600 with O_NOFOLLOW so no umask window exists; pre-existing
    // destinations (retries) are truncated and re-secured immediately
    // after open via handle fchmod below.
    let open_fresh = || -> std::io::Result<std::fs::File> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .custom_flags(libc::O_NOFOLLOW)
                .mode(0o600)
                .open(dest)
        }
        #[cfg(not(unix))]
        {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dest)
        }
    };
    let mut output = match open_fresh() {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.custom_flags(libc::O_NOFOLLOW);
                options.mode(0o600);
            }
            options.open(dest)?
        }
        Err(error) => return Err(error),
    };
    // Immediately secure the opened handle to 0600 before any write:
    // a retry-truncated file may carry wider permissions from a
    // previous run, and fchmod on the handle (not the pathname) cannot
    // be redirected by a swap and leaves no old-perm window (E13/E15).
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd as _;
        // SAFETY: `output` is an open file descriptor owned here.
        if unsafe { libc::fchmod(output.as_raw_fd(), 0o600) } < 0 {
            let error = std::io::Error::last_os_error();
            drop(output);
            let _ = std::fs::remove_file(dest);
            return Err(error);
        }
    }
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        *total = total
            .checked_add(read)
            .ok_or_else(|| std::io::Error::other("file input size overflowed during snapshot"))?;
        if limit.is_some_and(|limit| *total > limit) {
            drop(output);
            // The partial destination must not survive a limit refusal;
            // reclaiming it is best-effort and documented here: the
            // limit error below is returned regardless (E13).
            let _ = std::fs::remove_file(dest);
            return Err(oversize(limit));
        }
        output.write_all(&buffer[..read])?;
    }
    // Re-assert owner-only mode on the handle before persisting; never
    // via the pathname which may have been swapped (E13/E15).
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd as _;
        // SAFETY: `output` is an open file descriptor owned here.
        if unsafe { libc::fchmod(output.as_raw_fd(), 0o600) } < 0 {
            let error = std::io::Error::last_os_error();
            drop(output);
            let _ = std::fs::remove_file(dest);
            return Err(error);
        }
    }
    output.sync_all()?;
    drop(output);
    // Explicit owner-only mode: snapshot bytes may carry sensitive
    // data, and creation must not depend on the process umask (E15).
    // Unix already secured the handle above with fchmod; non-Unix maps
    // the owner-write bit to the read-only flag below.
    #[cfg(not(unix))]
    {
        let mut permissions = std::fs::metadata(dest)?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(dest, permissions)?;
    }
    Ok(())
}

/// Shared nofollow traversal over an open directory. With `dest` set the
/// files are copied; without it only the byte and entry limits are
/// checked (E13). An omitted limit disables that bound by explicit
/// policy ("explicit max only"); callers that accept untrusted trees
/// always pass the runtime limits.
fn walk_dir_contents(
    root: RawFd,
    dest: Option<&Utf8Path>,
    limit: Option<usize>,
    count_limit: Option<usize>,
) -> std::io::Result<()> {
    if let Some(dest) = dest {
        // Snapshot destinations live outside the workspace handle tree:
        // create with the symlink-escape check on both sides (E13). A
        // planted link here would divert the copy outside the run meta
        // dir.
        create_dest_dir(dest)?;
    }
    // SAFETY: `root` is a fresh directory fd; fdopendir takes ownership.
    let root_dir = unsafe { libc::fdopendir(root) };
    if root_dir.is_null() {
        let error = std::io::Error::last_os_error();
        close(root);
        return Err(error);
    }
    let mut total = 0_usize;
    let mut entries = 0_usize;
    let mut stack: Vec<(*mut libc::DIR, Option<Utf8PathBuf>)> =
        vec![(root_dir, dest.map(Utf8Path::to_path_buf))];
    let close_stack = |stack: &mut Vec<(*mut libc::DIR, Option<Utf8PathBuf>)>| {
        while let Some((dir, _)) = stack.pop() {
            // SAFETY: every handle in the stack came from fdopendir.
            unsafe {
                libc::closedir(dir);
            }
        }
    };
    while let Some((dir, dir_dest)) = stack.last().cloned() {
        // SAFETY: `dir` is a live DIR handle owned by the stack.
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            // Guarded by the `stack.last()` above: the pop cannot fail.
            // Use let-else to fail closed without panicking.
            let Some((dir, _)) = stack.pop() else {
                return Err(std::io::Error::other("directory stack underflow"));
            };
            // SAFETY: the popped handle is no longer referenced.
            unsafe {
                libc::closedir(dir);
            }
            continue;
        }
        // SAFETY: `entry` points into the DIR handle's buffer.
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        entries = entries
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("file input entry count overflowed"))?;
        if count_limit.is_some_and(|limit| entries > limit) {
            close_stack(&mut stack);
            return Err(too_many_entries(count_limit));
        }
        // SAFETY: `dir` is a live DIR handle.
        let dir_fd = unsafe { libc::dirfd(dir) };
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: `dir_fd` is an open directory fd and `stat` is writable.
        if unsafe { libc::fstatat(dir_fd, name.as_ptr(), &mut stat, libc::AT_SYMLINK_NOFOLLOW) } < 0
        {
            let error = std::io::Error::last_os_error();
            close_stack(&mut stack);
            return Err(error);
        }
        let name_str = match name.to_str() {
            Ok(name) => name,
            Err(_) => {
                close_stack(&mut stack);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("path is not UTF-8: {:?}", name.to_bytes()),
                ));
            }
        };
        let kind = stat.st_mode & libc::S_IFMT;
        if kind == libc::S_IFLNK {
            close_stack(&mut stack);
            return Err(symlink_error());
        }
        if kind == libc::S_IFDIR {
            let sub_dest = match &dir_dest {
                Some(dir_dest) => {
                    let sub = dir_dest.join(name_str);
                    create_dest_dir(&sub)?;
                    Some(sub)
                }
                None => None,
            };
            match openat(
                dir_fd,
                name_str,
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
                0,
            ) {
                Ok(fd) => {
                    // SAFETY: `fd` is a fresh directory fd; fdopendir takes ownership.
                    let sub = unsafe { libc::fdopendir(fd) };
                    if sub.is_null() {
                        let error = std::io::Error::last_os_error();
                        close(fd);
                        close_stack(&mut stack);
                        return Err(error);
                    }
                    stack.push((sub, sub_dest));
                }
                Err(error) => {
                    close_stack(&mut stack);
                    return Err(error);
                }
            }
        } else if kind == libc::S_IFREG {
            // Stat size is an early gate only: the cumulative limit is
            // enforced again DURING the copy below, so a file swapped
            // for a larger one after this stat cannot bloat past the
            // limit (E13). Counter overflow fails closed (E13).
            let file_size = usize::try_from(stat.st_size)
                .map_err(|_| std::io::Error::other("file input size is invalid"))?;
            let advertised = total
                .checked_add(file_size)
                .ok_or_else(|| std::io::Error::other("file input size overflowed"))?;
            if limit.is_some_and(|limit| advertised > limit) {
                close_stack(&mut stack);
                return Err(oversize(limit));
            }
            if let Some(dir_dest) = &dir_dest {
                let fd = match openat(dir_fd, name_str, libc::O_RDONLY | libc::O_NOFOLLOW, 0) {
                    Ok(fd) => fd,
                    Err(error) => {
                        close_stack(&mut stack);
                        return Err(error);
                    }
                };
                // The leaf could have been swapped between fstatat and
                // openat: re-check the opened descriptor's type (E13).
                if let Err(error) = ensure_regular_file(fd) {
                    close(fd);
                    close_stack(&mut stack);
                    return Err(error);
                }
                // SAFETY: the fd is freshly opened and owned here.
                let mut source = unsafe { std::fs::File::from_raw_fd(fd) };
                // The actual limit travels into the copy: the pre-copy
                // stat above never gates the write by itself (E13).
                if let Err(error) =
                    copy_file(&mut source, &dir_dest.join(name_str), limit, &mut total)
                {
                    close_stack(&mut stack);
                    return Err(error);
                }
            } else {
                total = advertised;
            }
        }
    }
    Ok(())
}

/// Copies a workspace file or directory into `dest` with nofollow at
/// every component, applying the same limits as the tree check (E13).
pub(crate) fn snapshot_tree(
    workspace: &Utf8Path,
    relative: &str,
    dest: &Utf8Path,
    limit: Option<usize>,
    count_limit: Option<usize>,
) -> std::io::Result<()> {
    if relative.is_empty() {
        let root = open_root(workspace)?;
        return walk_dir_contents(root, Some(dest), limit, count_limit);
    }
    let (parent, leaf) = open_parent(workspace, relative, false)?;
    let target = cstr(&leaf)?;
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `parent` is an open directory fd and `stat` is writable.
    if unsafe {
        libc::fstatat(
            parent,
            target.as_ptr(),
            &mut stat,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } < 0
    {
        let error = std::io::Error::last_os_error();
        close(parent);
        return Err(error);
    }
    let kind = stat.st_mode & libc::S_IFMT;
    if kind == libc::S_IFLNK {
        close(parent);
        return Err(symlink_error());
    }
    if kind == libc::S_IFREG {
        let fd = openat(parent, &leaf, libc::O_RDONLY | libc::O_NOFOLLOW, 0);
        close(parent);
        let fd = fd?;
        // Re-check the opened descriptor against a swap between the
        // stat and the open (E13).
        ensure_regular_file(fd)?;
        // SAFETY: the fd is freshly opened and owned here.
        let mut source = unsafe { std::fs::File::from_raw_fd(fd) };
        // Single file: the cumulative total starts at zero and the
        // configured limit gates every chunk of the copy (E13).
        return copy_file(&mut source, dest, limit, &mut 0_usize);
    }
    if kind != libc::S_IFDIR {
        close(parent);
        return Err(not_file_or_dir());
    }
    let root = openat(
        parent,
        &leaf,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        0,
    );
    close(parent);
    walk_dir_contents(root?, Some(dest), limit, count_limit)
}

/// Pre-generates a unique staging leaf name so the async caller can
/// own the staging path across a `spawn_blocking` await: dropping the
/// outer future unlinks the known path even when the blocking task is
/// detached and still running (E13b).
pub(crate) fn staging_name_for(leaf: &str) -> String {
    format!(".{leaf}.qcg-part-{}", uuid::Uuid::now_v7().as_simple())
}

/// Owns one opened parent directory across resolve, stage, and commit
/// so the workspace tree is walked once, not three times (E13). The fd
/// is opened handle-relative (`O_NOFOLLOW` at every component); stage
/// and commit both re-check the leaf against this same handle, so no
/// pathname walk sits between validation and use. Moving the handle
/// across `spawn_blocking` calls keeps the same descriptor: the commit
/// re-validates the leaf on the still-open parent instead of
/// re-walking from the root.
pub(crate) struct ParentHandle {
    fd: RawFd,
    leaf: String,
    /// Identity of the parent directory at open (`st_dev`, `st_ino`).
    /// Re-verified before commit so a non-symlink directory replacement
    /// (rename swap) cannot redirect the staged rename outside the
    /// validated tree (E13).
    dev: u64,
    ino: u64,
}

// SAFETY: an owned directory fd plus an owned name carry no shared
// mutable state; every method issues syscalls on the fd.
unsafe impl Send for ParentHandle {}

impl ParentHandle {
    pub(crate) fn open(
        workspace: &Utf8Path,
        resolved: &Utf8Path,
        create: bool,
    ) -> std::io::Result<Self> {
        let relative = resolved.strip_prefix(workspace).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "target is outside the workspace",
            )
        })?;
        let (fd, leaf) = open_parent(workspace, relative.as_str(), create)?;
        let (dev, ino) = Self::fstat_dev_ino(fd)?;
        Ok(Self { fd, leaf, dev, ino })
    }

    fn fstat_dev_ino(fd: RawFd) -> std::io::Result<(u64, u64)> {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: `fd` is an open fd owned by the caller.
        if unsafe { libc::fstat(fd, &mut stat) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok((stat.st_dev as u64, stat.st_ino as u64))
    }

    fn verify_parent_identity(&self) -> std::io::Result<()> {
        let (dev, ino) = Self::fstat_dev_ino(self.fd)?;
        if (dev, ino) != (self.dev, self.ino) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "parent directory was replaced during staging; refusing commit",
            ));
        }
        Ok(())
    }

    fn refuse_symlink_leaf(&self) -> std::io::Result<()> {
        if is_symlink(self.fd, &self.leaf)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "target is a symbolic link",
            ));
        }
        Ok(())
    }

    /// Stages file contents without committing: writes through a
    /// `0o600` staging handle and fsyncs, regardless of the final mode.
    /// The caller applies the final mode at commit via
    /// [`Self::commit_with_mode`] so the staging window never exposes
    /// wider permissions than the target should carry (E15). The caller
    /// commits with [`Self::commit_with_mode`] after checking for
    /// cancellation, so an aborted outer future never commits (E13b).
    pub(crate) fn stage<T>(
        &self,
        staging: &str,
        _mode: u32,
        write: impl FnOnce(&mut std::fs::File) -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        self.refuse_symlink_leaf()?;
        let fd = openat(
            self.fd,
            staging,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW,
            // Staging is always owner-only; the final mode lands at
            // commit time (E15).
            0o600 as libc::mode_t,
        )?;
        // SAFETY: the fd is freshly opened and owned here.
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        let outcome = write(&mut file).and_then(|value| file.sync_all().map(|()| value));
        drop(file);
        match outcome {
            Ok(value) => Ok(value),
            Err(error) => {
                if let Ok(name) = cstr(staging) {
                    // SAFETY: the parent fd is open and `name` is valid.
                    if unsafe { libc::unlinkat(self.fd, name.as_ptr(), 0) } != 0 {
                        tracing::warn!(
                            staging = %staging,
                            error = %std::io::Error::last_os_error(),
                            "failed to reclaim staging file after a producer error"
                        );
                    }
                }
                Err(error)
            }
        }
    }

    /// Commits a file previously staged with [`Self::stage`] on this
    /// same parent handle: re-validates the leaf and the parent
    /// identity, applies the staged mode to the staging file itself
    /// (never through the target pathname), renames over the target,
    /// and fsyncs the parent (E13a). The rename is the atomic
    /// point-of-no-return: an abort before commit never commits (the
    /// staging guard reclaims), while an abort racing the rename lets
    /// the atomic rename complete exactly once (E13).
    pub(crate) fn commit_with_mode(&self, staging: &str, mode: u32) -> std::io::Result<()> {
        self.verify_parent_identity()?;
        self.refuse_symlink_leaf()?;
        let name = cstr(staging)?;
        // Apply the final mode to the staging file before the rename so
        // the staging window never exposes wider permissions than the
        // target should carry, regardless of umask (E15).
        // SAFETY: the parent fd is open and `name` is valid.
        if unsafe { libc::fchmodat(self.fd, name.as_ptr(), mode as libc::mode_t, 0) } < 0 {
            let error = std::io::Error::last_os_error();
            // SAFETY: the parent fd is open and `name` is valid.
            unsafe {
                libc::unlinkat(self.fd, name.as_ptr(), 0);
            }
            return Err(error);
        }
        // Re-validate after the chmod: a parent swap racing the chmod
        // must not redirect the rename (E13).
        self.verify_parent_identity()?;
        self.refuse_symlink_leaf()?;
        let target = cstr(&self.leaf)?;
        // SAFETY: the parent fd is open and both names are valid.
        if unsafe { libc::renameat(self.fd, name.as_ptr(), self.fd, target.as_ptr()) } < 0 {
            let error = std::io::Error::last_os_error();
            // SAFETY: the parent fd is open and `name` is valid.
            if unsafe { libc::unlinkat(self.fd, name.as_ptr(), 0) } != 0 {
                tracing::warn!(
                    staging = %staging,
                    error = %std::io::Error::last_os_error(),
                    "failed to reclaim staging file after a failed replace"
                );
            }
            return Err(error);
        }
        // Persist the replacement entry; a lost directory fsync can
        // lose the rename on crash, so surface it (E13).
        // SAFETY: the parent fd is an open directory fd.
        if unsafe { libc::fsync(self.fd) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for ParentHandle {
    fn drop(&mut self) {
        close(self.fd);
    }
}

/// Removes a named staging file, ignoring absence. Used by the async
/// owner to reclaim after abort (E13b). The parent identity is pinned
/// at open and re-verified before the reclaim, mirroring the unlink
/// path: a replaced parent fails closed with a warning and the startup
/// sweep retries. Reclaim failures are logged, never propagated: the
/// guard runs in `Drop`, and anything left behind carries a unique
/// staging name the startup sweep reaps by age.
pub(crate) fn remove_named_staging(workspace: &Utf8Path, resolved: &Utf8Path, staging: &str) {
    let Ok(relative) = resolved.strip_prefix(workspace) else {
        tracing::warn!("staging reclaim skipped: target escapes the workspace");
        return;
    };
    let Ok((parent, _)) = open_parent(workspace, relative.as_str(), false) else {
        return;
    };
    let Ok((dev, ino)) = parent_dev_ino(parent) else {
        close(parent);
        return;
    };
    if verify_parent_dev_ino(parent, dev, ino).is_err() {
        tracing::warn!(staging = %staging, "staging reclaim skipped: parent was replaced");
        close(parent);
        return;
    }
    if let Ok(name) = cstr(staging) {
        // SAFETY: `parent` is an open directory fd and `name` is valid.
        // The return is intentionally unchecked beyond a warning: a
        // concurrent sweep may have reaped the file first.
        if unsafe { libc::unlinkat(parent, name.as_ptr(), 0) } < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(staging = %staging, %error, "staging reclaim failed; startup sweep will retry");
            }
        }
    }
    close(parent);
}

/// Fd-based sweep of orphaned staging files on Unix (E13): every
/// directory is opened with `O_NOFOLLOW`, entries are inspected with
/// `fstatat(AT_SYMLINK_NOFOLLOW)`, and removals use `unlinkat` on the
/// open parent fd, so a child swapped for a symlink mid-sweep can
/// neither redirect enumeration outside the tree nor divert a removal.
/// Symlinks are removed as links, never followed or descended into.
pub(crate) fn sweep_staging_files(
    dir: &Utf8Path,
    fragment: &str,
    cutoff: std::time::SystemTime,
) -> std::io::Result<usize> {
    const MAX_ENTRIES: usize = 10_000;
    const MAX_DEPTH: usize = 32;
    let root = match open_root(dir) {
        Ok(fd) => fd,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    // SAFETY: `root` is a fresh directory fd; fdopendir takes ownership.
    let root_dir = unsafe { libc::fdopendir(root) };
    if root_dir.is_null() {
        let error = std::io::Error::last_os_error();
        close(root);
        return Err(error);
    }
    let mut removed = 0_usize;
    let mut budget = MAX_ENTRIES;
    let mut stack: Vec<(*mut libc::DIR, usize)> = vec![(root_dir, 0)];
    let close_stack = |stack: &mut Vec<(*mut libc::DIR, usize)>| {
        while let Some((dir, _)) = stack.pop() {
            // SAFETY: every handle in the stack came from fdopendir.
            unsafe {
                libc::closedir(dir);
            }
        }
    };
    let result = (|| -> std::io::Result<()> {
        while let Some((dir, depth)) = stack.last().copied() {
            // SAFETY: `dir` is a live DIR handle owned by the stack.
            let entry = unsafe { libc::readdir(dir) };
            if entry.is_null() {
                // Guarded by the `stack.last()` above: the pop cannot
                // fail. Use let-else to fail closed without panicking.
                let Some((dir, _)) = stack.pop() else {
                    return Err(std::io::Error::other("directory stack underflow"));
                };
                // SAFETY: the popped handle is no longer referenced.
                unsafe {
                    libc::closedir(dir);
                }
                continue;
            }
            if budget == 0 {
                return Err(std::io::Error::other(
                    "staging sweep exceeded the maximum entry budget",
                ));
            }
            budget -= 1;
            // SAFETY: `entry` points into the DIR handle's buffer.
            let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                continue;
            }
            // SAFETY: `dir` is a live DIR handle.
            let dir_fd = unsafe { libc::dirfd(dir) };
            let mut stat: libc::stat = unsafe { std::mem::zeroed() };
            // SAFETY: `dir_fd` is an open directory fd, `stat` writable.
            if unsafe { libc::fstatat(dir_fd, name.as_ptr(), &mut stat, libc::AT_SYMLINK_NOFOLLOW) }
                < 0
            {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::NotFound {
                    // Vanished concurrently; already gone.
                    continue;
                }
                return Err(error);
            }
            let kind = stat.st_mode & libc::S_IFMT;
            if kind == libc::S_IFDIR {
                if depth >= MAX_DEPTH {
                    return Err(std::io::Error::other(
                        "staging sweep exceeded the maximum directory depth",
                    ));
                }
                let name_str = std::str::from_utf8(name.to_bytes()).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "staging sweep found a non-UTF-8 entry",
                    )
                })?;
                match openat(
                    dir_fd,
                    name_str,
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
                    0,
                ) {
                    Ok(fd) => {
                        // SAFETY: fresh dir fd; fdopendir takes ownership.
                        let sub = unsafe { libc::fdopendir(fd) };
                        if sub.is_null() {
                            let error = std::io::Error::last_os_error();
                            close(fd);
                            return Err(error);
                        }
                        stack.push((sub, depth + 1));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
                continue;
            }
            let name_lossy = name.to_string_lossy();
            // Anchored staging match, matching the non-Unix path:
            // dot-prefixed fragments only (E13).
            if !(name_lossy.starts_with('.') && name_lossy.contains(fragment)) {
                continue;
            }
            // Full nanosecond comparison: truncating to seconds would
            // reap files up to 1 s younger than the TTL. An
            // unrepresentable mtime keeps the file (E13).
            let mtime = std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(
                stat.st_mtime.try_into().unwrap_or(u64::MAX),
                stat.st_mtime_nsec.try_into().unwrap_or(0),
            ));
            if mtime.is_none_or(|mtime| mtime > cutoff) {
                continue;
            }
            // SAFETY: `dir_fd` is an open directory fd, `name` valid.
            // unlinkat on a symlink removes the link itself.
            if unsafe { libc::unlinkat(dir_fd, name.as_ptr(), 0) } < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::NotFound {
                    continue;
                }
                return Err(error);
            }
            // SAFETY: `dir_fd` is an open directory fd.
            if unsafe { libc::fsync(dir_fd) } < 0 {
                return Err(std::io::Error::last_os_error());
            }
            removed = removed
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("staging sweep removal count overflowed"))?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => Ok(removed),
        Err(error) => {
            close_stack(&mut stack);
            Err(error)
        }
    }
}
