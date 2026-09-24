//! Registry install commit (E14): single commit phase for staged generator
//! closures, install locks, backups, and uninstall. Split from `install.rs`
//! with no behavior change (E14/C-5).

use anyhow::{Context, Result};

use camino::{Utf8Path, Utf8PathBuf};

use sha2::{Digest, Sha256};
use std::fs::OpenOptions;
use std::io::ErrorKind;

use std::sync::{Mutex, OnceLock};
use uuid::Uuid;

use super::plan::{confirm_stdin, ensure_safe_install_id, print_permission_summary};
use qcg_service::app_registry_with_providers as app_registry;

use super::install::path_exists;
use super::install_stage::{StagedInstall, load_verified_contract};

/// Single staging-cleanup guard (E14): owns every staging path that must
/// vanish unless committed. Single-path users push once and use
/// `armed`/`disarm`/`cleanup`; multi-path users push several. Drop reaps
/// whatever is still owned. The former two-type split (`CleanupPaths` plus
/// a single-path `CleanupPath`) is unified here so the reaping rule cannot
/// drift between them.
#[derive(Default)]
pub(crate) struct CleanupPaths {
    paths: Vec<Utf8PathBuf>,
}

impl CleanupPaths {
    pub(crate) fn new(path: Utf8PathBuf) -> Self {
        Self { paths: vec![path] }
    }

    pub(crate) fn push(&mut self, path: Utf8PathBuf) {
        self.paths.push(path);
    }

    fn cleanup(&mut self) -> Result<()> {
        for path in self.paths.drain(..) {
            remove_owned_path(&path)?;
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.paths.clear();
    }

    fn armed(&self) -> Result<&Utf8Path> {
        self.paths
            .last()
            .map(Utf8PathBuf::as_path)
            .ok_or_else(|| anyhow::anyhow!("install staging was disarmed before use"))
    }
}

impl Drop for CleanupPaths {
    fn drop(&mut self) {
        for path in self.paths.drain(..) {
            // Drop cannot return errors; surface them instead of squashing
            // silently so residue stays observable (E14-11).
            if let Err(error) = remove_owned_path(&path) {
                eprintln!("failed to remove install staging `{path}`: {error}");
            }
        }
    }
}

pub(crate) fn remove_owned_path(path: &Utf8Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_dir() {
        std::fs::remove_dir_all(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

pub(crate) fn finish_install(
    providers_path: Option<&Utf8Path>,
    staged: StagedInstall,
    generators_dir: &Utf8Path,
    yes: bool,
    force: bool,
    limits: &qcg_service::PackageLimits,
) -> Result<String> {
    // The staged contract was parsed once at stage time and threaded here
    // without re-opening or re-parsing the manifest (E14-1). Validation
    // below runs against these exact bytes.
    let staged_path = staged.path.clone();
    let contract = &staged.contract;
    app_registry(providers_path)?.validate_contract(contract)?;
    print_permission_summary(contract);
    if !yes {
        confirm_stdin("Install this generator?")?;
    }
    let id = contract.manifest.generator.id.clone();
    let expected_sha256 = contract.sha256.clone();
    ensure_safe_install_id(&id)?;
    std::fs::create_dir_all(generators_dir)?;
    let target = generators_dir.join(&id);
    let target_exists = match std::fs::symlink_metadata(&target) {
        Ok(_) => true,
        Err(error) if error.kind() == ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect existing generator `{target}`"));
        }
    };
    if target_exists && !force {
        anyhow::bail!("generator `{id}` already exists at `{target}`; pass --force to replace it");
    }
    if dunce::canonicalize(generators_dir)?.starts_with(dunce::canonicalize(&staged_path)?) {
        anyhow::bail!("install destination `{generators_dir}` is inside source `{staged_path}`");
    }
    let temporary = unique_directory_at(generators_dir, "qcg-install-temp")?;
    let mut temporary = CleanupPaths::new(temporary);
    // Single copy (E14): the staged private copy is copied ONCE into the
    // parent-temp dir for an atomic same-filesystem rename. No double copy:
    // staging already lives in private staging; this is the commit copy.
    qcg_service::package::copy_dir_all(&staged_path, temporary.armed()?, limits)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    // Single hash + compare (E14): the copied `qcg.toml` bytes are hashed
    // ONCE and compared to the staged digest (computed once at stage time
    // from the staged bytes). No re-parse of the manifest: byte equality
    // implies identical dependencies. The SBOM inventory verification below
    // is a SEPARATE required check (all files + modes, fail-closed E15),
    // not a duplicate manifest re-read: it must walk the copy for supply-
    // chain completeness. A mismatch fails closed with the partial copy
    // removed (E14-1, E14-9, E14-11). SBOM is REQUIRED (fail-closed E14):
    // a copy without one fails here, never falling back to hash-only.
    let copied_temp = temporary.armed()?.to_path_buf();
    let copy_check = (|| -> Result<()> {
        let manifest_path = copied_temp.join("qcg.toml");
        let bytes = qcg_fs::read_bounded(&manifest_path, limits.max_metadata_bytes)
            .with_context(|| format!("copied generator is missing qcg.toml in `{copied_temp}`"))?;
        let actual = hex::encode(Sha256::digest(&bytes));
        if actual != expected_sha256 {
            anyhow::bail!(
                "copied generator manifest does not match the verified closure for `{id}`"
            );
        }
        // Dependency set is covered by the byte equality above (identical
        // manifest bytes imply identical dependencies): the hash comparison
        // IS the dependency-equality check, so no re-parse is needed (E14).
        if !path_exists(&copied_temp.join("QCG-SBOM.spdx.json"))? {
            anyhow::bail!(
                "copied generator in `{copied_temp}` is missing QCG-SBOM.spdx.json; refusing to commit without supply-chain metadata"
            );
        }
        qcg_service::package::verify_installed_package(&copied_temp, limits)
            .map_err(|error| anyhow::anyhow!(error.to_string()))
            .context("copied generator failed inventory verification")?;
        Ok(())
    })();
    if let Err(error) = copy_check {
        match remove_owned_path(&copied_temp) {
            Ok(()) => return Err(error),
            Err(cleanup_error) => {
                return Err(anyhow::anyhow!(
                    "{error:#}; additionally failed to remove partial copy: {cleanup_error}"
                ));
            }
        }
    }
    commit_install(temporary.armed()?, &target, target_exists)?;
    temporary.disarm();
    Ok(id)
}

/// Renames the staged install into place. Without an existing target
/// the rename must not replace anything: `RENAME_NOREPLACE` closes the
/// check-then-rename race on Linux, and other platforms run the
/// existence probe in [`check_no_replace_target`] with the residual
/// window documented there (E14). Durability: the parent directory is
/// fsynced after a successful rename on every platform (Linux included)
/// so the commit survives a crash (E14 parity).
#[cfg(target_os = "linux")]
pub(crate) fn rename_no_replace(
    temporary: &Utf8Path,
    target: &Utf8Path,
    replace_existing: bool,
) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
    if replace_existing {
        std::fs::rename(temporary, target)?;
        fsync_parent_dir(target)?;
        return Ok(());
    }
    let from = CString::new(temporary.as_std_path().as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "temporary path contains a NUL byte",
        )
    })?;
    let to = CString::new(target.as_std_path().as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "target path contains a NUL byte",
        )
    })?;
    // SAFETY: both paths are valid NUL-terminated strings for this call.
    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    fsync_parent_dir(target)?;
    Ok(())
}

/// Fsyncs the parent directory of `target` so a completed rename is
/// durable across a crash (E14). Best-effort on platforms where opening
/// the parent fails: the rename itself already succeeded, so an fsync
/// open failure is surfaced fail-closed (the caller fails, the target
/// stands, and a rerun converges).
pub(crate) fn fsync_parent_dir(target: &Utf8Path) -> std::io::Result<()> {
    let Some(parent) = target.parent() else {
        return Ok(());
    };
    // Windows cannot open a directory with `File::open`
    // (`ERROR_ACCESS_DENIED`); NTFS journals the rename itself, so the
    // entry flush is Unix-only (same convention as journal and
    // idempotency persistence).
    #[cfg(not(unix))]
    let _ = parent;
    #[cfg(unix)]
    {
        let dir = std::fs::File::open(parent)?;
        dir.sync_all()?;
    }
    Ok(())
}

/// Pre-rename existence probe shared by the non-Linux fallback. Returns an
/// `AlreadyExists` error when the target is present so a plain rename never
/// silently replaces a concurrent install (E14). Testable on every
/// platform; the residual check-then-rename window on non-Linux is
/// documented on [`rename_no_replace`]. Linux uses `RENAME_NOREPLACE` and
/// unit tests instead, so the helper is compiled out there.
#[cfg(any(test, not(target_os = "linux")))]
pub(crate) fn check_no_replace_target(
    target: &Utf8Path,
    replace_existing: bool,
) -> std::io::Result<()> {
    if replace_existing {
        return Ok(());
    }
    match std::fs::symlink_metadata(target) {
        Ok(_) => Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("install target `{target}` already exists"),
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Non-Linux fallback: no `RENAME_NOREPLACE` exists, so the pre-check plus
/// a handle-pinned re-check narrow the window best-effort (E14-4). The
/// temporary must be a real directory (O_NOFOLLOW `symlink_metadata`), the
/// target is re-probed just before rename, and the parent is fsynced after
/// rename for durability (same `fsync_parent_dir` as Linux, fail-closed).
/// Hard-link-based NOREPLACE is infeasible for directories (hard links to
/// directories are prohibited), so an atomic no-overwrite rename does not
/// exist on this platform: the residual window below is truly unavoidable
/// without OS support. Mitigation: `commit_install` holds the per-id file
/// lock (plus the in-process shard mutex) across the whole probe+rename
/// window, so two concurrent same-id installs serialize — the second blocks
/// on the lock, then observes the committed target and fails closed (or
/// replaces under `--force` with serialization). Residual window remains
/// only for lock-bypassing actors (manual filesystem meddling): two such
/// actors can both observe NotFound and race; the loser fails on rename.
/// Concurrent same-id installs on non-Linux without going through the CLI
/// locks are operator error (serialize them).
#[cfg(not(target_os = "linux"))]
pub(crate) fn rename_no_replace(
    temporary: &Utf8Path,
    target: &Utf8Path,
    replace_existing: bool,
) -> std::io::Result<()> {
    check_no_replace_target(target, replace_existing)?;
    match std::fs::symlink_metadata(temporary) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("install staging `{temporary}` is not a directory"),
            ));
        }
        Err(error) => return Err(error),
    }
    check_no_replace_target(target, replace_existing)?;
    std::fs::rename(temporary, target)?;
    fsync_parent_dir(target)?;
    Ok(())
}

/// In-process serialization sharded by install id (E14-5). Commits touch
/// only `generators_dir/<id>` plus their own temp staging, which is
/// per-id state: unrelated ids use different shards and different lock
/// files, so they install in parallel. The same id serializes in-process
/// here and across processes via the per-id file lock below, closing the
/// fresh-install rename race (two NotFound observers) and the `--force`
/// last-win window. Commits also hold the global lock in shared mode (see
/// `global_commit_rwlock`): many installs may hold it together, but an
/// uninstall holding it exclusive blocks new commits during its atomic
/// dependents re-check plus removal.
pub(crate) fn replace_mutex_for(id: &str) -> &'static Mutex<()> {
    static SHARDS: OnceLock<[Mutex<()>; 16]> = OnceLock::new();
    let shards = SHARDS.get_or_init(|| {
        [
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
            Mutex::new(()),
        ]
    });
    // Stable shard from the id hash; collisions only over-serialize.
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    &shards[(hash as usize) % shards.len()]
}

/// Per-id cross-process lock file name. The id is sanitized so path
/// separators can never escape the parent, with a hash suffix to keep
/// distinct ids distinct after sanitization.
pub(crate) fn per_id_lock_name(id: &str) -> String {
    let mut safe = String::with_capacity(id.len());
    for c in id.chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
            safe.push(c);
        } else {
            safe.push('_');
        }
    }
    if safe.is_empty() {
        safe.push('_');
    }
    let hash = hex::encode(Sha256::digest(id.as_bytes()));
    format!(".qcg-install-{safe}-{}.lock", &hash[..8])
}

/// Global cross-process lock for operations that mutate or depend on
/// global state: uninstall's dependents re-check plus removal (the
/// dependents graph spans all ids, so a concurrent install of a different
/// id could add a new dependent between check and removal). Commits hold
/// this lock in shared mode (many readers), uninstall holds it exclusive
/// (single writer), so unrelated installs proceed in parallel but never
/// interleave with an uninstall's atomic check-plus-remove.
pub(crate) fn global_lock_path(parent: &Utf8Path) -> Utf8PathBuf {
    parent.join(".qcg-install-global.lock")
}

/// In-process global lock matching the file lock above: commits hold a
/// read guard, uninstall holds a write guard across its atomic section.
pub(crate) fn global_commit_rwlock() -> &'static std::sync::RwLock<()> {
    static GLOBAL_RWLOCK: OnceLock<std::sync::RwLock<()>> = OnceLock::new();
    GLOBAL_RWLOCK.get_or_init(|| std::sync::RwLock::new(()))
}

pub(crate) fn acquire_file_lock(lock_path: &Utf8Path, target: &Utf8Path) -> Result<std::fs::File> {
    // Open with no-follow semantics so check and use cannot diverge (E14f
    // SENSITIVE): Unix opens with O_NOFOLLOW at use time; a planted symlink
    // fails the open instead of redirecting the lock outside the install
    // tree. Non-Unix does a pre-check plus a post-open re-check (no
    // O_NOFOLLOW exists there). Inspection failures other than absence
    // propagate fail-closed instead of proceeding to open.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        // Pre-check for a clear symlink refusal message; the O_NOFOLLOW
        // open below is authoritative (a swap between check and open fails
        // the open, never follows).
        match std::fs::symlink_metadata(lock_path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                anyhow::bail!("refusing to lock through symbolic link `{lock_path}`")
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect install lock `{lock_path}`"));
            }
        }
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(lock_path)
            .with_context(|| format!("failed to open install lock `{lock_path}`"))?;
        // Post-open verification: the opened handle must not be a symlink
        // (O_NOFOLLOW already guarantees this; the re-check closes any
        // platform gap best-effort).
        // Fail closed on inspection errors (E14/G-1): an unscannable
        // lock path proves nothing about symlinks, so proceeding would be
        // fail-open. Only NotFound is impossible here (the path was just
        // opened/created above) and still refuses like any other error.
        if std::fs::symlink_metadata(lock_path)
            .map(|meta| meta.file_type().is_symlink())
            .with_context(|| format!("failed to inspect install lock `{lock_path}`"))?
        {
            anyhow::bail!("refusing to lock through symbolic link `{lock_path}`");
        }
        // Blocking exclusive lock: holders of the same lock file serialize (E14).
        lock_file
            .lock()
            .with_context(|| format!("failed to lock install `{target}`"))?;
        Ok(lock_file)
    }
    #[cfg(not(unix))]
    {
        match std::fs::symlink_metadata(lock_path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                anyhow::bail!("refusing to lock through symbolic link `{lock_path}`")
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect install lock `{lock_path}`"));
            }
        }
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(lock_path)
            .with_context(|| format!("failed to open install lock `{lock_path}`"))?;
        // Post-open re-check (no O_NOFOLLOW on this platform): a symlink
        // swapped in between pre-check and open fails closed here instead of
        // locking through it.
        // Fail closed on inspection errors (E14/G-1): an unscannable
        // lock path proves nothing about symlinks, so proceeding would be
        // fail-open. Only NotFound is impossible here (the path was just
        // opened/created above) and still refuses like any other error.
        if std::fs::symlink_metadata(lock_path)
            .map(|meta| meta.file_type().is_symlink())
            .with_context(|| format!("failed to inspect install lock `{lock_path}`"))?
        {
            anyhow::bail!("refusing to lock through symbolic link `{lock_path}`");
        }
        // Blocking exclusive lock: holders of the same lock file serialize (E14).
        lock_file
            .lock()
            .with_context(|| format!("failed to lock install `{target}`"))?;
        Ok(lock_file)
    }
}

pub(crate) fn acquire_file_lock_shared(
    lock_path: &Utf8Path,
    target: &Utf8Path,
) -> Result<std::fs::File> {
    // Shared variant of the lock above for commits holding the global lock
    // as readers: many installs may share it, one uninstall excludes all.
    // Same no-follow semantics as the exclusive variant (E14f SENSITIVE).
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        match std::fs::symlink_metadata(lock_path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                anyhow::bail!("refusing to lock through symbolic link `{lock_path}`")
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect install lock `{lock_path}`"));
            }
        }
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(lock_path)
            .with_context(|| format!("failed to open install lock `{lock_path}`"))?;
        // Fail closed on inspection errors (E14/G-1): an unscannable
        // lock path proves nothing about symlinks, so proceeding would be
        // fail-open. Only NotFound is impossible here (the path was just
        // opened/created above) and still refuses like any other error.
        if std::fs::symlink_metadata(lock_path)
            .map(|meta| meta.file_type().is_symlink())
            .with_context(|| format!("failed to inspect install lock `{lock_path}`"))?
        {
            anyhow::bail!("refusing to lock through symbolic link `{lock_path}`");
        }
        // Blocking shared lock: parallel installs proceed together (E14).
        lock_file
            .lock_shared()
            .with_context(|| format!("failed to lock install `{target}`"))?;
        Ok(lock_file)
    }
    #[cfg(not(unix))]
    {
        match std::fs::symlink_metadata(lock_path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                anyhow::bail!("refusing to lock through symbolic link `{lock_path}`")
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect install lock `{lock_path}`"));
            }
        }
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(lock_path)
            .with_context(|| format!("failed to open install lock `{lock_path}`"))?;
        // Fail closed on inspection errors (E14/G-1): an unscannable
        // lock path proves nothing about symlinks, so proceeding would be
        // fail-open. Only NotFound is impossible here (the path was just
        // opened/created above) and still refuses like any other error.
        if std::fs::symlink_metadata(lock_path)
            .map(|meta| meta.file_type().is_symlink())
            .with_context(|| format!("failed to inspect install lock `{lock_path}`"))?
        {
            anyhow::bail!("refusing to lock through symbolic link `{lock_path}`");
        }
        // Blocking shared lock: parallel installs proceed together (E14).
        lock_file
            .lock_shared()
            .with_context(|| format!("failed to lock install `{target}`"))?;
        Ok(lock_file)
    }
}

pub(crate) fn commit_install(
    temporary: &Utf8Path,
    target: &Utf8Path,
    replace_existing: bool,
) -> Result<()> {
    // Recovery before locking (F14): if a previous commit died between
    // target->backup and new-target publish, `target` is missing with a
    // backup sibling intact. Restore the newest backup for this id before
    // any sweep or commit can delete the sole old version.
    recover_interrupted_backup(target);
    // Per-id serialization in-process plus per-id file lock across
    // processes, held for the whole backup+rename window. Fresh installs
    // race too (two NotFound observers both renaming): without the lock
    // they last-win and both report success with a divergent closure (E14).
    // Unrelated ids use different shards and lock files and proceed in
    // parallel; only the same id serializes. The global lock is held
    // shared so an uninstall holding it exclusive blocks new commits
    // during its atomic check-plus-remove, while parallel commits share it.
    let id = target
        .file_name()
        .context("install target must have a file name")?;
    let parent = target
        .parent()
        .context("install target must have a parent directory")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create install parent `{parent}`"))?;
    // Global before per-id ordering prevents deadlock with uninstall,
    // which takes global exclusive then per-id.
    let _global_read = global_commit_rwlock()
        .read()
        .map_err(|_| anyhow::anyhow!("install global lock was poisoned for `{target}`"))?;
    let _process_guard = replace_mutex_for(id)
        .lock()
        .map_err(|_| anyhow::anyhow!("install replace lock was poisoned for `{target}`"))?;
    // Cross-process locks, held for the whole commit.
    let global_path = global_lock_path(parent);
    let _global_file_guard = acquire_file_lock_shared(&global_path, target)?;
    let lock_path = parent.join(per_id_lock_name(id));
    let _file_guard = acquire_file_lock(&lock_path, target)?;
    let backup = if replace_existing {
        let backup = loop {
            let candidate = unique_nonexistent_path(parent, "qcg-install-backup")?;
            match std::fs::rename(target, &candidate) {
                Ok(()) => break candidate,
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to move existing generator `{target}`"));
                }
            }
        };
        // Backup ownership marker (F14): rename preserves the old
        // directory mtime, so a fresh backup would otherwise look
        // 1h-stale to a concurrent sweep. The sidecar carries the target
        // id, commit phase, owner pid, and creation time; its own mtime
        // is the freshness signal. Recovery runs before sweep, so a
        // missing target with a backup sibling restores instead of
        // deleting.
        if let Err(error) = write_backup_marker(&backup, id, target) {
            let _ = std::fs::rename(&backup, target);
            return Err(error)
                .with_context(|| format!("failed to mark install backup for `{target}`"));
        }
        // Durability: fsync the parent after moving the existing target to
        // backup, before replacing (E14 backup→fsync→replace→fsync). A crash
        // between backup and replace leaves `target` missing with `backup`
        // intact; the next boot's stale sweep keeps foreign backups for 1h
        // (never reaps fresh foreign staging), and rerunning the install
        // converges (fresh target installed, orphan backup reaped later).
        // Boot-time recovery: if `target` is missing but a
        // `.qcg-install-backup-*` sibling exists, the previous commit was
        // interrupted after backup — rerun the install (or manually rename
        // the newest backup back to `target`); the backup is retained on
        // replace failure below for manual restore, never auto-deleted on
        // failure paths.
        if let Err(error) = fsync_parent_dir(target) {
            // Best-effort rollback: try to restore the backup before
            // reporting; if restore also fails, retain the backup path for
            // manual recovery (same fail-closed retention as below).
            let _ = std::fs::rename(&backup, target);
            return Err(anyhow::Error::from(error).context(format!(
                "failed to durably move existing generator `{target}`"
            )));
        }
        Some(CleanupPaths::new(backup))
    } else {
        None
    };

    if !replace_existing {
        match std::fs::symlink_metadata(target) {
            Ok(_) => anyhow::bail!("install target `{target}` appeared during staging"),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect install target `{target}`"));
            }
        }
    }

    if let Err(error) = rename_no_replace(temporary, target, replace_existing) {
        let Some(mut backup) = backup else {
            return Err(error).with_context(|| format!("failed to commit generator `{target}`"));
        };
        let backup_path = backup.armed()?.to_owned();
        match std::fs::rename(&backup_path, target) {
            Ok(()) => {
                backup.disarm();
                return Err(error)
                    .with_context(|| format!("failed to commit generator `{target}`"));
            }
            Err(restore_error) => {
                // Retain the backup: deleting it would lose the previous
                // version with nothing installed. Report its path so the
                // operator can restore manually (E14).
                backup.disarm();
                return Err(anyhow::anyhow!(
                    "failed to commit generator `{target}`: {error}; failed to restore existing generator: {restore_error}; previous version retained at `{backup_path}`"
                ));
            }
        }
    }

    if let Some(mut backup) = backup {
        backup
            .cleanup()
            .with_context(|| format!("installed `{target}` but failed to remove backup"))?;
    }
    Ok(())
}

pub(crate) fn uninstall(id: &str, generators_dir: &Utf8Path, yes: bool) -> Result<()> {
    ensure_safe_install_id(id)?;
    let target = generators_dir.join(id);
    // Never follow a planted symlink: `remove_dir_all` through a link
    // would delete outside the generators dir (E14).
    match std::fs::symlink_metadata(&target) {
        Ok(meta) if meta.file_type().is_symlink() => {
            anyhow::bail!("refusing to uninstall through symbolic link `{target}`")
        }
        Ok(_) => {}
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect `{target}`"));
        }
    }
    if !path_exists(&target.join("qcg.toml"))? {
        anyhow::bail!("generator `{id}` is not installed under `{generators_dir}`");
    }
    // Hold the install locks across the dependents re-check plus removal
    // so the check-then-remove is atomic: a concurrent install adding a
    // new dependent cannot slip between them. Global exclusive blocks new
    // commits (which hold global shared), per-id exclusive serializes with
    // same-id commits. Ordering is global before per-id, matching commits.
    std::fs::create_dir_all(generators_dir)
        .with_context(|| format!("failed to create install parent `{generators_dir}`"))?;
    let _global_write = global_commit_rwlock()
        .write()
        .map_err(|_| anyhow::anyhow!("install global lock was poisoned for uninstall `{id}`"))?;
    let _process_guard = replace_mutex_for(id)
        .lock()
        .map_err(|_| anyhow::anyhow!("install replace lock was poisoned for uninstall `{id}`"))?;
    let _global_file_guard = acquire_file_lock(&global_lock_path(generators_dir), &target)?;
    let _per_id_file_guard =
        acquire_file_lock(&generators_dir.join(per_id_lock_name(id)), &target)?;
    // Re-check under the locks: fail closed when any installed generator
    // depends on `id`, using verified loads so a tampered dependent cannot
    // hide behind unverified bytes. There is no force flag, so refusal is
    // unconditional (E14-9).
    let dependents = installed_dependents(generators_dir, id)?;
    if !dependents.is_empty() {
        anyhow::bail!(
            "generator `{id}` is required by installed generator(s) {} under `{generators_dir}`; uninstall them first",
            dependents.join(", ")
        );
    }
    // Re-verify the target is still a real directory under the locks: a
    // swap between the pre-check and here fails closed instead of deleting
    // through a planted link.
    match std::fs::symlink_metadata(&target) {
        Ok(meta) if meta.file_type().is_symlink() => {
            anyhow::bail!("refusing to uninstall through symbolic link `{target}`")
        }
        Ok(meta) if !meta.file_type().is_dir() => {
            anyhow::bail!("generator `{id}` is not installed under `{generators_dir}`")
        }
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {
            anyhow::bail!("generator `{id}` is not installed under `{generators_dir}`")
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect `{target}`"));
        }
    }
    if !yes {
        confirm_stdin(&format!("Uninstall generator `{id}`?"))?;
    }
    std::fs::remove_dir_all(&target)
        .with_context(|| format!("failed to remove generator `{target}`"))?;
    Ok(())
}

/// Lists installed generators whose manifest dependencies include `id`.
/// Unreadable manifests fail closed (propagated) so a dependent cannot hide
/// behind an IO error (E14-11).
pub(crate) fn installed_dependents(generators_dir: &Utf8Path, id: &str) -> Result<Vec<String>> {
    let mut dependents = Vec::new();
    let entries = match std::fs::read_dir(generators_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(dependents),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to scan generators in `{generators_dir}`"));
        }
    };
    for entry in entries {
        let entry = entry.with_context(|| format!("failed to read entry in `{generators_dir}`"))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to inspect entry in `{generators_dir}`"))?;
        if !file_type.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            // Non-UTF8 generator names are skipped fail-closed, never lossy-matched (E14).
            continue;
        };
        if name == id || name.starts_with('.') {
            continue;
        }
        let candidate = generators_dir.join(&name);
        if !path_exists(&candidate.join("qcg.toml"))? {
            continue;
        }
        // Verified load, never unverified bytes: the satisfy path verifies
        // before reuse, so the dependents scan must verify too, otherwise a
        // tampered dependent could hide behind unverified bytes (E14).
        // Any parse, validation, or inventory failure propagates
        // fail-closed rather than silently assuming no dependency.
        let manifest = load_verified_contract(&candidate, &qcg_service::PackageLimits::default())
            .with_context(|| format!("failed to inspect installed generator `{name}`"))?
            .manifest;
        if manifest.dependencies.contains_key(id) {
            dependents.push(name);
        }
    }
    dependents.sort();
    Ok(dependents)
}

pub(crate) fn unique_stage_dir_in(parent: &Utf8Path) -> Result<Utf8PathBuf> {
    unique_directory_at(parent, "qcg-install-stage")
}

pub(crate) fn unique_directory_at(parent: &Utf8Path, prefix: &str) -> Result<Utf8PathBuf> {
    let pid = std::process::id();
    loop {
        let path = parent.join(format!(".{prefix}-{pid}-{}", Uuid::now_v7()));
        // Owner-only staging dir: Unix creates with 0700 atomically so no
        // umask window exposes staged content between creation and chmod.
        // Non-Unix has no directory mode bits; uniqueness is the isolation.
        #[cfg(unix)]
        let created = {
            use std::os::unix::fs::DirBuilderExt as _;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .map_err(|error| (error.kind(), error))
        };
        #[cfg(not(unix))]
        let created: Result<(), (std::io::ErrorKind, std::io::Error)> =
            std::fs::create_dir(&path).map_err(|error| (error.kind(), error));
        match created {
            Ok(()) => return Ok(path),
            Err((ErrorKind::AlreadyExists, _)) => continue,
            Err((_, error)) => {
                return Err(error)
                    .with_context(|| format!("failed to create temporary directory `{path}`"));
            }
        }
    }
}

pub(crate) fn unique_nonexistent_path(parent: &Utf8Path, prefix: &str) -> Result<Utf8PathBuf> {
    let pid = std::process::id();
    loop {
        let path = parent.join(format!(".{prefix}-{pid}-{}", Uuid::now_v7()));
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(path),
            Ok(_) => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect temporary path `{path}`"));
            }
        }
    }
}

/// Sidecar marker that distinguishes a recovery backup from disposable
/// scratch (F14). Written immediately after target->backup rename so the
/// fresh backup has a fresh mtime even though the renamed directory keeps
/// its old mtime.
pub(crate) const BACKUP_MARKER_FILE: &str = ".qcg-backup.json";

/// Best-effort recovery for a commit interrupted between target->backup
/// and new-target publish (F14-02/F14-04). If `target` is missing but a
/// backup sibling with a marker for this id exists, the newest backup is
/// renamed back. Never deletes: on failure the backup and its location
/// remain for manual recovery.
pub(crate) fn recover_interrupted_backup(target: &Utf8Path) {
    if std::fs::symlink_metadata(target).is_ok() {
        return;
    }
    let Some(parent) = target.parent() else {
        return;
    };
    let Some(id) = target.file_name() else {
        return;
    };
    let candidates = backup_candidates_for(parent, id);
    // Newest marker mtime first so the latest pre-commit state wins.
    let mut newest: Option<(Utf8PathBuf, std::time::SystemTime)> = None;
    for candidate in candidates {
        let marker = candidate.join(BACKUP_MARKER_FILE);
        let mtime = std::fs::symlink_metadata(&marker)
            .and_then(|meta| meta.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let is_newer = newest.as_ref().is_none_or(|(_, t)| mtime > *t);
        if is_newer {
            newest = Some((candidate, mtime));
        }
    }
    if let Some((backup, _)) = newest
        && std::fs::rename(&backup, target).is_ok()
    {
        let _ = fsync_parent_dir(target);
    }
}

/// Lists backup siblings whose marker names this target id (F14). Marker
/// parsing is best-effort: unmarked directories are scratch, not recovery
/// backups, and are left to the age-gated scratch path.
pub(crate) fn backup_candidates_for(parent: &Utf8Path, id: &str) -> Vec<Utf8PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(parent) else {
        return out;
    };
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if !name.starts_with(".qcg-install-backup-") {
            continue;
        }
        let path = match Utf8PathBuf::from_path_buf(entry.path()) {
            Ok(path) => path,
            Err(_) => continue,
        };
        if backup_marker_id(&path).as_deref() == Some(id) {
            out.push(path);
        }
    }
    out
}

fn backup_marker_id(backup: &Utf8Path) -> Option<String> {
    let bytes = std::fs::read(backup.join(BACKUP_MARKER_FILE)).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// Freshness of a backup (F14-01): the max of the directory mtime and the
/// marker mtime. Rename preserves the old directory mtime, so a just-made
/// backup would otherwise look hours old.
pub(crate) fn backup_freshness(backup: &Utf8Path) -> Option<std::time::SystemTime> {
    let dir_mtime = std::fs::symlink_metadata(backup)
        .and_then(|meta| meta.modified())
        .ok();
    let marker_mtime = std::fs::symlink_metadata(backup.join(BACKUP_MARKER_FILE))
        .and_then(|meta| meta.modified())
        .ok();
    match (dir_mtime, marker_mtime) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn write_backup_marker(backup: &Utf8Path, id: &str, target: &Utf8Path) -> Result<()> {
    let marker = serde_json::json!({
        "id": id,
        "target": target.as_str(),
        "phase": "backup-created",
        "pid": std::process::id(),
        "created_at": chrono::Utc::now().to_rfc3339(),
    });
    let bytes = serde_json::to_vec_pretty(&marker).context("backup marker is not serializable")?;
    std::fs::write(backup.join(BACKUP_MARKER_FILE), &bytes)
        .with_context(|| format!("failed to write backup marker for `{target}`"))?;
    let _ = fsync_parent_dir(target);
    Ok(())
}
