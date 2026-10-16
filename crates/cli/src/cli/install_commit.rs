//! Registry install commit (E14): single commit phase for staged generator
//! closures, install locks, backups, and uninstall. Split from `install.rs`
//! with no behavior change (E14/C-5).

use anyhow::{Context, Result};

use camino::{Utf8Path, Utf8PathBuf};

use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};

use std::sync::{Mutex, OnceLock};
use uuid::Uuid;

use super::plan::{confirm_stdin, ensure_safe_install_id, print_permission_summary};
use service::app_registry_with_providers as app_registry;

use super::install::path_exists;
use super::install_stage::{InstallConfig, Selected, StagedInstall, load_verified_contract};

/// Single staging-cleanup guard (E14): owns every staging path that must
/// vanish unless committed. Single-path users push once and use
/// `armed`/`disarm`; multi-path users push several. Drop reaps
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

/// Prepared on the destination filesystem, with all copies, prompts and
/// copied-package verification complete. Drop removes unpublished copies.
pub(crate) struct PreparedInstall {
    temporary: CleanupPaths,
    target: Utf8PathBuf,
    replace_existing: bool,
    manifest: contract::Manifest,
}

#[cfg(test)]
pub(crate) fn finish_install(
    providers_path: Option<&Utf8Path>,
    staged: StagedInstall,
    generators_dir: &Utf8Path,
    yes: bool,
    force: bool,
    limits: &service::PackageLimits,
) -> Result<String> {
    let id = staged.contract.manifest.generator.id.clone();
    finish_closure(
        InstallConfig {
            providers_path,
            generators_dir,
            yes,
            force,
            limits,
        },
        vec![staged],
        &Selected::new(),
    )?;
    Ok(id)
}

pub(crate) fn finish_closure(
    config: InstallConfig<'_>,
    stages: Vec<StagedInstall>,
    selected: &Selected,
) -> Result<()> {
    let prepared = stages
        .into_iter()
        .map(|stage| prepare_install(config, stage))
        .collect::<Result<Vec<_>>>()?;
    commit_prepared_closure(config, prepared, selected)
}

pub(crate) fn prepare_install(
    config: InstallConfig<'_>,
    staged: StagedInstall,
) -> Result<PreparedInstall> {
    let InstallConfig {
        providers_path,
        generators_dir,
        yes,
        force,
        limits,
    } = config;
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
    let temporary = unique_directory_at(generators_dir, "install-temp")?;
    let temporary = CleanupPaths::new(temporary);
    // Single copy (E14): the staged private copy is copied ONCE into the
    // parent-temp dir for an atomic same-filesystem rename. No double copy:
    // staging already lives in private staging; this is the commit copy.
    service::package::copy_dir_all(&staged_path, temporary.armed()?, limits)
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
        let manifest_path = copied_temp.join(contract::MANIFEST_FILE);
        let bytes = files::read_bounded(&manifest_path, limits.max_metadata_bytes)
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
        if !path_exists(&copied_temp.join("SBOM.spdx.json"))? {
            anyhow::bail!(
                "copied generator in `{copied_temp}` is missing SBOM.spdx.json; refusing to commit without supply-chain metadata"
            );
        }
        service::package::verify_installed_package(&copied_temp, limits)
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
    Ok(PreparedInstall {
        temporary,
        target,
        replace_existing: target_exists,
        manifest: contract.manifest.clone(),
    })
}

/// Publication holds shared global locks, then every distinct shard in
/// ascending index order, then every closure ID file lock in ID order. Reused
/// nodes stay protected through revalidation and all leaf-to-parent commits.
/// Copies, prompts and copied inventory checks finish before this scope.
pub(crate) fn commit_prepared_closure(
    config: InstallConfig<'_>,
    prepared: Vec<PreparedInstall>,
    selected: &Selected,
) -> Result<()> {
    let _global_read = global_commit_rwlock()
        .read()
        .map_err(|_| anyhow::anyhow!("install global lock was poisoned"))?;
    let _global_file = acquire_file_lock_shared(
        &global_lock_path(config.generators_dir),
        config.generators_dir,
    )?;
    let ids = prepared
        .iter()
        .map(|package| package.manifest.generator.id.as_str())
        .chain(selected.keys().map(String::as_str));
    let _id_guards = lock_closure_ids(config.generators_dir, ids)?;

    // Include reused nodes, not just packages that happened to need staging.
    // A same-version replacement may change dependencies too: validate the
    // current manifests' edges against the complete selected set below.
    let mut manifests = std::collections::BTreeMap::new();
    for package in &prepared {
        let id = package.manifest.generator.id.clone();
        if manifests
            .insert(id.clone(), package.manifest.clone())
            .is_some()
        {
            anyhow::bail!("duplicate prepared generator `{id}`");
        }
    }
    for (id, selection) in selected {
        let version = &selection.version;
        if !manifests.contains_key(id) {
            let manifest = load_verified_contract(&config.generators_dir.join(id), config.limits)
                .with_context(|| {
                    format!(
                        "selected dependency `{id}@{version}` changed or disappeared; retry install"
                    )
                })?
                .manifest;
            manifests.insert(id.clone(), manifest);
        }
        let manifest = &manifests[id];
        if manifest.generator.id != *id
            || semver::Version::parse(&manifest.generator.version)? != *version
            || manifest.dependencies != selection.dependencies
        {
            anyhow::bail!(
                "selected dependency `{id}@{version}` changed identity, version or dependencies; retry install"
            );
        }
    }
    for (id, manifest) in &manifests {
        for (dependency, requirement) in &manifest.dependencies {
            let requirement = semver::VersionReq::parse(requirement)?;
            let Some(dependency_manifest) = manifests.get(dependency) else {
                anyhow::bail!(
                    "dependency closure changed for `{id}`: `{dependency}` was not selected; retry install"
                );
            };
            if !requirement.matches(&semver::Version::parse(
                &dependency_manifest.generator.version,
            )?) {
                anyhow::bail!(
                    "dependency closure changed for `{id}`: `{dependency}` no longer satisfies `{requirement}`; retry install"
                );
            }
        }
    }
    #[cfg(test)]
    AFTER_CLOSURE_REVALIDATION.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    // Leaf order from staging, parent last. Failures may leave standalone
    // dependencies, but never publish the parent after a failed dependency.
    for mut package in prepared {
        commit_install_locked(
            package.temporary.armed()?,
            &package.target,
            package.replace_existing,
            config.limits,
        )?;
        package.temporary.disarm();
    }
    Ok(())
}

/// Renames the staged install into place. Without an existing target
/// the rename must not replace anything: `RENAME_NOREPLACE` closes the
/// check-then-rename race on Linux, and other platforms run the
/// existence probe in [`check_no_replace_target`] with the residual
/// window documented there (E14). Reports rename only: callers must sync
/// separately so a sync error cannot be confused with rename failure.
#[cfg(target_os = "linux")]
pub(crate) fn rename_no_replace(temporary: &Utf8Path, target: &Utf8Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
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
    Ok(())
}

/// Flushes the parent directory on Unix, surfacing open/sync failures.
/// Other platforms currently have no directory flush here: success there
/// does not promise directory-entry durability under power loss.
pub(crate) fn fsync_parent_dir(target: &Utf8Path) -> std::io::Result<()> {
    let Some(parent) = target.parent() else {
        return Ok(());
    };
    // Windows cannot open a directory with `File::open`
    // (`ERROR_ACCESS_DENIED`); no directory-entry durability is claimed
    // on platforms without this flush.
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
/// a directory-kind re-check narrow the window best-effort (E14-4). The
/// temporary must be a real directory (O_NOFOLLOW `symlink_metadata`), the
/// target is re-probed just before rename. Callers flush the parent separately.
/// Hard-link-based NOREPLACE is infeasible for directories (hard links to
/// directories are prohibited), so an atomic no-overwrite rename does not
/// exist on this platform: the residual window below is truly unavoidable
/// without OS support. Mitigation: `commit_install_locked` holds the per-id file
/// lock (plus the in-process shard mutex) across the whole probe+rename
/// window, so two concurrent same-id installs serialize — the second blocks
/// on the lock, then observes the committed target and fails closed (or
/// replaces under `--force` with serialization). Residual window remains
/// only for lock-bypassing actors (manual filesystem meddling): two such
/// actors can both observe NotFound and race; a plain rename can replace
/// an empty directory planted by such an actor.
/// Concurrent same-id installs on non-Linux without going through the CLI
/// locks are operator error (serialize them).
#[cfg(not(target_os = "linux"))]
pub(crate) fn rename_no_replace(temporary: &Utf8Path, target: &Utf8Path) -> std::io::Result<()> {
    check_no_replace_target(target, false)?;
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
    check_no_replace_target(target, false)?;
    std::fs::rename(temporary, target)?;
    Ok(())
}

/// Stable shard index, not ID ordering: ID-ordered acquisition can invert
/// shard order, and colliding IDs must never recursively lock the same mutex.
pub(crate) fn replace_shard_index(id: &str) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) % 16
}

fn replace_shards() -> &'static [Mutex<()>; 16] {
    static SHARDS: OnceLock<[Mutex<()>; 16]> = OnceLock::new();
    SHARDS.get_or_init(|| std::array::from_fn(|_| Mutex::new(())))
}

pub(crate) fn replace_mutex_for(id: &str) -> &'static Mutex<()> {
    &replace_shards()[replace_shard_index(id)]
}

/// The two independent total orders used by publication. Sorting IDs alone
/// is insufficient: e.g. `a`/`q` collide on shard 12, while `b` uses shard 5.
pub(crate) fn closure_lock_order<'a>(
    ids: impl IntoIterator<Item = &'a str>,
) -> (BTreeSet<usize>, BTreeSet<&'a str>) {
    let ids: BTreeSet<_> = ids.into_iter().collect();
    let shards = ids.iter().map(|id| replace_shard_index(id)).collect();
    (shards, ids)
}

/// Caller already holds both global locks. Retain all guards to completion;
/// partial acquisition errors release everything acquired so far via RAII.
fn lock_closure_ids<'a>(
    parent: &Utf8Path,
    ids: impl IntoIterator<Item = &'a str>,
) -> Result<(Vec<std::sync::MutexGuard<'static, ()>>, Vec<std::fs::File>)> {
    let (shards, ids) = closure_lock_order(ids);
    let mut process_guards = Vec::with_capacity(shards.len());
    for index in shards {
        process_guards.push(
            replace_shards()[index]
                .lock()
                .map_err(|_| anyhow::anyhow!("install replace shard {index} was poisoned"))?,
        );
    }
    let mut file_guards = Vec::with_capacity(ids.len());
    for id in ids {
        file_guards.push(acquire_file_lock(
            &parent.join(per_id_lock_name(id)),
            &parent.join(id),
        )?);
    }
    Ok((process_guards, file_guards))
}

#[cfg(test)]
thread_local! {
    // Thread-local so unrelated production-path tests cannot steal a hook.
    static AFTER_CLOSURE_REVALIDATION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn with_closure_publication_hook<T>(
    hook: impl FnOnce() + 'static,
    publish: impl FnOnce() -> T,
) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            AFTER_CLOSURE_REVALIDATION.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
    AFTER_CLOSURE_REVALIDATION.with(|slot| {
        assert!(slot.borrow_mut().replace(Box::new(hook)).is_none());
    });
    let _reset = Reset;
    publish()
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
    format!(".install-{safe}-{}.lock", &hash[..8])
}

/// Global cross-process publication lock: shared for closure publication,
/// exclusive for uninstall's dependents recheck/removal. Per-ID locks keep
/// selected dependencies stable. Preparation never holds this lock.
pub(crate) fn global_lock_path(parent: &Utf8Path) -> Utf8PathBuf {
    parent.join(".install-global.lock")
}

/// In-process global lock matching the file lock above. Always acquired
/// before the global file lock, then distinct shards by ascending index,
/// then per-ID file locks by ascending ID. Never upgrade or reacquire global.
pub(crate) fn global_commit_rwlock() -> &'static std::sync::RwLock<()> {
    static GLOBAL_RWLOCK: OnceLock<std::sync::RwLock<()>> = OnceLock::new();
    GLOBAL_RWLOCK.get_or_init(|| std::sync::RwLock::new(()))
}

fn open_install_lock(lock_path: &Utf8Path) -> Result<std::fs::File> {
    match std::fs::symlink_metadata(lock_path) {
        Ok(meta) if !meta.file_type().is_file() => {
            anyhow::bail!("refusing to lock through non-regular file `{lock_path}`")
        }
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("failed to inspect install lock"),
    }
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(lock_path)
        .with_context(|| format!("failed to open install lock `{lock_path}`"))?;
    if !file.metadata()?.is_file() || !std::fs::symlink_metadata(lock_path)?.file_type().is_file() {
        anyhow::bail!("refusing to lock through non-regular file `{lock_path}`");
    }
    Ok(file)
}

pub(crate) fn acquire_file_lock(lock_path: &Utf8Path, target: &Utf8Path) -> Result<std::fs::File> {
    let file = open_install_lock(lock_path)?;
    file.lock()
        .with_context(|| format!("failed to lock install `{target}`"))?;
    Ok(file)
}

pub(crate) fn acquire_file_lock_shared(
    lock_path: &Utf8Path,
    target: &Utf8Path,
) -> Result<std::fs::File> {
    let file = open_install_lock(lock_path)?;
    file.lock_shared()
        .with_context(|| format!("failed to lock install `{target}`"))?;
    Ok(file)
}

/// Maintenance never waits for an active commit and retains the returned
/// handle through recovery or deletion. Opening or locking errors propagate.
pub(crate) fn try_acquire_file_lock(lock_path: &Utf8Path) -> Result<Option<std::fs::File>> {
    let file = open_install_lock(lock_path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

#[cfg(test)]
pub(crate) fn commit_install(
    temporary: &Utf8Path,
    target: &Utf8Path,
    replace_existing: bool,
) -> Result<()> {
    let parent = target
        .parent()
        .context("install target must have a parent directory")?;
    std::fs::create_dir_all(parent)?;
    let _global_read = global_commit_rwlock()
        .read()
        .map_err(|_| anyhow::anyhow!("install global lock was poisoned for `{target}`"))?;
    let _global_file = acquire_file_lock_shared(&global_lock_path(parent), target)?;
    let id = target
        .file_name()
        .context("install target must have a file name")?;
    let _id_guards = lock_closure_ids(parent, [id])?;
    commit_install_locked(
        temporary,
        target,
        replace_existing,
        &service::PackageLimits::default(),
    )
}

/// Caller holds both global locks AND all closure shard/per-ID locks. Never
/// reacquire them here. Per-ID locks also exclude maintenance, which never
/// acquires global after per-ID and only restores/deletes owned backups.
fn commit_install_locked(
    temporary: &Utf8Path,
    target: &Utf8Path,
    replace_existing: bool,
    limits: &service::PackageLimits,
) -> Result<()> {
    let id = target
        .file_name()
        .context("install target must have a file name")?;
    ensure_safe_install_id(id)?;
    let parent = target
        .parent()
        .context("install target must have a parent directory")?;
    recover_interrupted_backup(target, limits)?;
    // No second generation while an earlier recovery record is pending.
    let pending = backup_records_for(parent, id)?;
    if !pending.is_empty() {
        verify_backup_identity(target, id, limits).context(
            "recovery cleanup pending; verify/repair the current target before retrying",
        )?;
        fsync_parent_dir(target)?;
        for record in pending {
            remove_backup_record(&record, limits)?;
        }
    }
    if replace_existing {
        match std::fs::symlink_metadata(target) {
            Ok(metadata) if metadata.file_type().is_dir() => {}
            Ok(metadata) if metadata.file_type().is_symlink() => {
                anyhow::bail!("refusing to replace symbolic-link generator target `{target}`");
            }
            Ok(_) => {
                anyhow::bail!("refusing to replace non-directory generator target `{target}`");
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                anyhow::bail!(
                    "existing generator target `{target}` disappeared before replacement"
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to revalidate existing generator target `{target}`")
                });
            }
        }
    }
    let backup = if replace_existing {
        let path = unique_nonexistent_path(parent, "install-backup")?;
        let mut record = write_backup_marker(&path, id)?;
        // Bytes AND directory entry flushed before destructive rename.
        backup_probe("marker-durable")?;
        if let Err(error) = rename_no_replace(target, &path) {
            // Preparation is not ownership: retain any colliding directory.
            return Err(error).context("failed to move existing generator to backup");
        }
        // A crash here is indistinguishable from a pre-rename collision.
        // Never adopt the directory from preparation metadata alone.
        backup_probe("backup-renamed")?;
        backup_probe("backup-sync")?;
        fsync_parent_dir(target)
            .context("backup rename durability uncertain; ambiguous recovery state retained")?;
        confirm_backup_ownership(&mut record)?;
        backup_probe("backup-owned")?;
        Some(record)
    } else {
        None
    };
    // --force must not overwrite an unexpected target after moving the old one.
    let publish =
        backup_probe("publish-rename").and_then(|()| Ok(rename_no_replace(temporary, target)?));
    if let Err(error) = publish {
        if let Some(record) = &backup {
            return rollback_error(error, record, target, limits);
        }
        return Err(error).with_context(|| format!("failed to commit generator `{target}`"));
    }
    // Rename succeeded: sync failure must NEVER enter ordinary rollback.
    backup_probe("publish-sync")?;
    fsync_parent_dir(target)
        .context("published generator but failed to sync parent; recovery backup retained")?;
    if let Some(record) = backup {
        verify_backup_identity(target, id, limits)
            .context("published target failed verification; recovery backup retained")?;
        remove_backup_record(&record, limits)
            .context("installed generator but recovery cleanup pending; retry install")?;
    }
    Ok(())
}

fn rollback_error(
    error: anyhow::Error,
    record: &BackupRecord,
    target: &Utf8Path,
    limits: &service::PackageLimits,
) -> Result<()> {
    match restore_backup(record, target, limits) {
        Ok(()) => Err(error).with_context(|| {
            format!("failed to commit generator `{target}`; old package restored")
        }),
        Err(restore_error) => anyhow::bail!(
            "failed to commit generator `{target}`: {error:#}; failed to restore existing generator: {restore_error:#}; inspect recovery record `{}`",
            record.sidecar
        ),
    }
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
    if !path_exists(&target.join(contract::MANIFEST_FILE))? {
        anyhow::bail!("generator `{id}` is not installed under `{generators_dir}`");
    }
    if !yes {
        confirm_stdin(&format!("Uninstall generator `{id}`?"))?;
    }
    // Hold the install locks across the dependents re-check plus removal
    // so the check-then-remove is atomic: a concurrent install adding a
    // new dependent cannot slip between them. Global exclusive blocks new
    // closure publication; per-id exclusive also excludes maintenance.
    // Ordering is global process, global file, shard, per-id file.
    std::fs::create_dir_all(generators_dir)
        .with_context(|| format!("failed to create install parent `{generators_dir}`"))?;
    let _global_write = global_commit_rwlock()
        .write()
        .map_err(|_| anyhow::anyhow!("install global lock was poisoned for uninstall `{id}`"))?;
    let _global_file_guard = acquire_file_lock(&global_lock_path(generators_dir), &target)?;
    let _process_guard = replace_mutex_for(id)
        .lock()
        .map_err(|_| anyhow::anyhow!("install replace lock was poisoned for uninstall `{id}`"))?;
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
    // Remove owned recovery copies while holding the same per-id lock.
    // Otherwise later maintenance could resurrect a successfully uninstalled id.
    // Do this before removing the target so cleanup failure leaves it installed.
    for record in backup_records_for(generators_dir, id)? {
        remove_backup_record(&record, &service::PackageLimits::default())
            .context("failed to clean recovery record before uninstall; target retained")?;
    }
    std::fs::remove_dir_all(&target)
        .with_context(|| format!("failed to remove generator `{target}`"))?;
    fsync_parent_dir(&target)?;
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
        if !path_exists(&candidate.join(contract::MANIFEST_FILE))? {
            continue;
        }
        // Verified load, never unverified bytes: the satisfy path verifies
        // before reuse, so the dependents scan must verify too, otherwise a
        // tampered dependent could hide behind unverified bytes (E14).
        // Any parse, validation, or inventory failure propagates
        // fail-closed rather than silently assuming no dependency.
        let manifest = load_verified_contract(&candidate, &service::PackageLimits::default())
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
    unique_directory_at(parent, "install-stage")
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

/// External metadata never changes authenticated package contents.
const BACKUP_RECORD_LIMIT: usize = 4096;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupMetadata {
    schema: u32,
    id: String,
    backup: String,
    phase: BackupPhase,
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
enum BackupPhase {
    Prepared,
    Owned,
    Restoring,
    Cleaning,
}

pub(crate) struct BackupRecord {
    pub(crate) backup: Utf8PathBuf,
    pub(crate) sidecar: Utf8PathBuf,
    pub(crate) id: String,
    phase: BackupPhase,
}

fn valid_backup_name(name: &str) -> bool {
    name.strip_prefix(".install-backup-").is_some_and(|nonce| {
        !nonce.is_empty()
            && nonce
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    })
}

pub(crate) fn backup_sidecar_path(backup: &Utf8Path) -> Utf8PathBuf {
    backup.with_file_name(format!("{}.json", backup.file_name().unwrap_or_default()))
}

/// Prepared/restoring/cleaning records report ambiguity, never directory ownership.
/// With a verified target, missing backups allow sidecar-only cleanup.
/// Non-directories, symlinks and invalid/legacy metadata prove no ownership.
/// Restoring/cleaning records also recognize occupied non-directory paths to block cleanup.
/// All paths derive from basenames under the scanned parent.
fn read_backup_record(parent: &Utf8Path, name: &str) -> Option<BackupRecord> {
    let basename = name.strip_suffix(".json")?;
    if !valid_backup_name(basename) {
        return None;
    }
    let sidecar = parent.join(name);
    if !std::fs::symlink_metadata(&sidecar)
        .ok()?
        .file_type()
        .is_file()
    {
        return None;
    }
    let bytes = files::read_nofollow_bounded(&sidecar, Some(BACKUP_RECORD_LIMIT)).ok()?;
    let data: BackupMetadata = serde_json::from_slice(&bytes).ok()?;
    if data.schema != 2 || data.backup != basename || !policy::is_safe_path_component(&data.id) {
        return None;
    }
    let backup = parent.join(basename);
    match std::fs::symlink_metadata(&backup) {
        Ok(meta)
            if meta.file_type().is_dir()
                || matches!(data.phase, BackupPhase::Restoring | BackupPhase::Cleaning) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        _ => return None,
    }
    Some(BackupRecord {
        backup,
        sidecar,
        id: data.id,
        phase: data.phase,
    })
}

pub(crate) fn backup_records(parent: &Utf8Path) -> Result<Vec<BackupRecord>> {
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).context("failed to scan recovery records"),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str()
            && let Some(record) = read_backup_record(parent, name)
        {
            out.push(record);
        }
    }
    Ok(out)
}

fn backup_records_for(parent: &Utf8Path, id: &str) -> Result<Vec<BackupRecord>> {
    Ok(backup_records(parent)?
        .into_iter()
        .filter(|record| record.id == id)
        .collect())
}

pub(crate) fn backup_freshness(record: &BackupRecord) -> Option<std::time::SystemTime> {
    std::fs::symlink_metadata(&record.sidecar)
        .ok()?
        .modified()
        .ok()
}

pub(crate) fn write_backup_marker(backup: &Utf8Path, id: &str) -> Result<BackupRecord> {
    anyhow::ensure!(policy::is_safe_path_component(id), "unsafe recovery ID");
    let basename = backup.file_name().context("backup must have a basename")?;
    anyhow::ensure!(valid_backup_name(basename), "unsafe backup basename");
    let sidecar = backup_sidecar_path(backup);
    let data = BackupMetadata {
        schema: 2,
        id: id.to_string(),
        backup: basename.to_string(),
        phase: BackupPhase::Prepared,
    };
    let bytes = serde_json::to_vec(&data)?;
    anyhow::ensure!(
        bytes.len() <= BACKUP_RECORD_LIMIT,
        "oversized recovery record"
    );
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(&sidecar)
        .context("failed to create exclusive recovery sidecar")?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    backup_probe("marker-file-synced")?;
    fsync_parent_dir(&sidecar)?;
    backup_probe("marker-parent-synced")?;
    Ok(BackupRecord {
        backup: backup.to_path_buf(),
        sidecar,
        id: id.to_string(),
        phase: BackupPhase::Prepared,
    })
}

/// Only the live commit that successfully renamed AND synced the old target
/// may promote preparation to ownership. Recovery must never call this.
fn confirm_backup_ownership(record: &mut BackupRecord) -> Result<()> {
    let current = current_backup_record(record)?;
    anyhow::ensure!(
        current.phase == BackupPhase::Prepared,
        "backup already owned"
    );
    backup_probe("ownership-write")?;
    persist_backup_phase(record, BackupPhase::Owned)
}

/// Caller has validated the transition and holds the per-ID lock. A failed
/// write/sync is ambiguous: no subsequent rename may run on that error path.
fn persist_backup_phase(record: &mut BackupRecord, phase: BackupPhase) -> Result<()> {
    current_backup_record(record)?;
    let data = BackupMetadata {
        schema: 2,
        id: record.id.clone(),
        backup: record
            .backup
            .file_name()
            .context("backup has no basename")?
            .to_string(),
        phase,
    };
    let bytes = serde_json::to_vec(&data)?;
    anyhow::ensure!(
        bytes.len() <= BACKUP_RECORD_LIMIT,
        "oversized recovery record"
    );
    files::write_sidecar_atomic(&record.sidecar, &bytes)
        .context("failed to persist backup phase; retained for inspection")?;
    record.phase = phase;
    Ok(())
}

#[cfg(test)]
pub(crate) fn write_owned_backup_marker(backup: &Utf8Path, id: &str) -> Result<BackupRecord> {
    // Fixture for a commit that already renamed and synced its backup.
    let mut record = write_backup_marker(backup, id)?;
    fsync_parent_dir(backup)?;
    confirm_backup_ownership(&mut record)?;
    Ok(record)
}

fn current_backup_record(record: &BackupRecord) -> Result<BackupRecord> {
    let parent = record.sidecar.parent().context("sidecar has no parent")?;
    let current = read_backup_record(parent, record.sidecar.file_name().unwrap_or_default())
        .context("recovery ownership changed; retained for inspection")?;
    anyhow::ensure!(
        current.id == record.id && current.backup == record.backup && current.phase == record.phase,
        "recovery ownership changed"
    );
    Ok(current)
}

fn require_owned_backup(record: &BackupRecord) -> Result<()> {
    anyhow::ensure!(
        record.phase != BackupPhase::Restoring,
        "ambiguous restoring record `{}`; retained untouched for inspection",
        record.sidecar
    );
    anyhow::ensure!(
        record.phase != BackupPhase::Cleaning,
        "ambiguous cleaning record `{}`; retained untouched for inspection",
        record.sidecar
    );
    anyhow::ensure!(
        record.phase == BackupPhase::Owned,
        "ambiguous recovery directory `{}`: preparation does not prove ownership; retained untouched for inspection",
        record.backup
    );
    Ok(())
}

pub(crate) fn verify_backup_identity(
    root: &Utf8Path,
    id: &str,
    limits: &service::PackageLimits,
) -> Result<()> {
    anyhow::ensure!(
        std::fs::symlink_metadata(root)?.file_type().is_dir(),
        "not a real package directory: `{root}`"
    );
    let contract = load_verified_contract(root, limits)?;
    anyhow::ensure!(
        contract.manifest.generator.id == id,
        "recovery package manifest ID differs from `{id}`"
    );
    Ok(())
}

/// Caller holds global -> shard -> per-ID locks. Only Owned may remove a
/// backup, after durably relinquishing ownership as Cleaning. Other phases
/// require absence and retire only the sidecar; Restoring/Cleaning also require
/// target verification here (including for uninstall). Flush
/// between cleanup steps. No recovery backup has a Drop guard.
pub(crate) fn remove_backup_record(
    record: &BackupRecord,
    limits: &service::PackageLimits,
) -> Result<()> {
    let mut current = current_backup_record(record)?;
    if matches!(
        current.phase,
        BackupPhase::Restoring | BackupPhase::Cleaning
    ) {
        let phase = if current.phase == BackupPhase::Cleaning {
            backup_probe("cleanup-backup")?;
            "cleaning"
        } else {
            "restoring"
        };
        // Ownership was relinquished before mutation. On retry only an absent
        // basename and verified target permit sidecar-only retirement.
        match std::fs::symlink_metadata(&current.backup) {
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => anyhow::bail!(
                "ambiguous {phase} record `{}`: backup path exists; retained untouched for inspection",
                current.sidecar
            ),
        }
        let target = current
            .sidecar
            .parent()
            .context("sidecar has no parent")?
            .join(&current.id);
        verify_backup_identity(&target, &current.id, limits)
            .with_context(|| format!("{phase} record requires a verified current target"))?;
        fsync_parent_dir(&target)?;
        return retire_vacated_backup_record(&current);
    }
    backup_probe("cleanup-backup")?;
    if current.phase == BackupPhase::Owned {
        // Probe pre-mutation failures before relinquishing ownership. Once
        // Cleaning is durable, an error/crash must never reauthorize this path.
        backup_probe("cleaning-write")?;
        persist_backup_phase(&mut current, BackupPhase::Cleaning)?;
        backup_probe("cleaning-durable")?;
        remove_owned_path(&record.backup)?;
    } else {
        // A missing prepared backup permits deleting only the sidecar, never
        // anything that appears at the chosen basename, even after this check.
        match std::fs::symlink_metadata(&record.backup) {
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => require_owned_backup(&current)?,
        }
    }
    backup_probe("cleanup-backup-sync")?;
    fsync_parent_dir(&record.backup)?;
    backup_probe("cleanup-sidecar")?;
    std::fs::remove_file(&record.sidecar)?;
    backup_probe("cleanup-sidecar-sync")?;
    fsync_parent_dir(&record.sidecar)?;
    Ok(())
}

/// Validate and retire a vacated backup's sidecar without touching its old
/// basename: that path became vacant at rename/removal and may have been reused.
fn retire_vacated_backup_record(record: &BackupRecord) -> Result<()> {
    let parent = record.sidecar.parent().context("sidecar has no parent")?;
    let backup_parent = record.backup.parent().context("backup has no parent")?;
    let backup_name = record
        .backup
        .file_name()
        .context("backup has no basename")?;
    anyhow::ensure!(
        parent == backup_parent && valid_backup_name(backup_name),
        "recovery record paths changed"
    );
    let expected_sidecar = format!("{backup_name}.json");
    anyhow::ensure!(
        record.sidecar.file_name() == Some(expected_sidecar.as_str()),
        "recovery record paths changed"
    );
    let metadata = std::fs::symlink_metadata(&record.sidecar)?;
    anyhow::ensure!(
        metadata.file_type().is_file(),
        "recovery sidecar is not a regular file"
    );
    let bytes = files::read_nofollow_bounded(&record.sidecar, Some(BACKUP_RECORD_LIMIT))?;
    let current: BackupMetadata = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        current.schema == 2
            && current.backup == backup_name
            && policy::is_safe_path_component(&current.id)
            && current.id == record.id
            && current.phase == record.phase
            && matches!(
                current.phase,
                BackupPhase::Restoring | BackupPhase::Cleaning
            ),
        "recovery ownership changed; retained for inspection"
    );

    backup_probe("cleanup-sidecar")?;
    std::fs::remove_file(&record.sidecar)?;
    backup_probe("cleanup-sidecar-sync")?;
    fsync_parent_dir(&record.sidecar)?;
    Ok(())
}

fn restore_backup(
    record: &BackupRecord,
    target: &Utf8Path,
    limits: &service::PackageLimits,
) -> Result<()> {
    let mut current = current_backup_record(record)?;
    require_owned_backup(&current)?;
    verify_backup_identity(&current.backup, &current.id, limits)?;
    backup_probe("restoring-write")?;
    persist_backup_phase(&mut current, BackupPhase::Restoring)?;
    // Probes model interruptions, not a syscall's known no-move result. Never
    // revert the phase after a probe or after a successful rename.
    backup_probe("restore-rename")?;
    if let Err(error) = rename_no_replace(&current.backup, target) {
        // A target conflict is a definite no-move result on both supported
        // rename paths. Other errors (notably I/O) remain ambiguous.
        if error.kind() == ErrorKind::AlreadyExists {
            persist_backup_phase(&mut current, BackupPhase::Owned)
                .context("restore did not move backup but failed to persist ownership")?;
        }
        return Err(error).context("failed to rename recovery backup");
    }
    backup_probe("restore-sync")?;
    fsync_parent_dir(target)?;
    retire_vacated_backup_record(&current)
}

/// Restore exactly one unambiguous owned, inventory-verified package.
/// Unknown/legacy backups are retained; invalid packages are actionable errors.
pub(crate) fn recover_interrupted_backup(
    target: &Utf8Path,
    limits: &service::PackageLimits,
) -> Result<()> {
    match std::fs::symlink_metadata(target) {
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("failed to inspect recovery target"),
    }
    let parent = target.parent().context("recovery target has no parent")?;
    let id = target
        .file_name()
        .context("recovery target has no basename")?;
    ensure_safe_install_id(id)?;
    let mut candidates = Vec::new();
    for record in backup_records_for(parent, id)? {
        if matches!(record.phase, BackupPhase::Restoring | BackupPhase::Cleaning) {
            // Even an absent backup cannot resolve a missing target.
            require_owned_backup(&record)?;
        }
        match std::fs::symlink_metadata(&record.backup) {
            Ok(_) => {
                require_owned_backup(&record)?;
                candidates.push(record);
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("failed to inspect recovery backup"),
        }
    }
    anyhow::ensure!(
        candidates.len() <= 1,
        "multiple recovery backups for `{id}`; retained untouched; inspect sibling .install-backup-* records and resolve ambiguity before retrying"
    );
    if let Some(record) = candidates.pop() {
        restore_backup(&record, target, limits).with_context(|| {
            format!(
                "recovery failed; inspect retained record `{}`",
                record.sidecar
            )
        })?;
    }
    Ok(())
}

// Thread-local probes keep fault tests independent of parallel tests.
#[cfg(test)]
thread_local! {
    static BACKUP_PROBE: std::cell::RefCell<Option<BackupProbe>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
type BackupProbe = Box<dyn FnMut(&str) -> Result<()>>;

#[cfg(test)]
pub(crate) fn with_backup_probe<T>(
    probe: impl FnMut(&str) -> Result<()> + 'static,
    action: impl FnOnce() -> T,
) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            BACKUP_PROBE.with(|slot| *slot.borrow_mut() = None);
        }
    }
    BACKUP_PROBE.with(|slot| *slot.borrow_mut() = Some(Box::new(probe)));
    let _reset = Reset;
    action()
}

fn backup_probe(_point: &str) -> Result<()> {
    #[cfg(test)]
    BACKUP_PROBE.with(|slot| {
        if let Some(probe) = slot.borrow_mut().as_mut() {
            probe(_point)
        } else {
            Ok(())
        }
    })?;
    Ok(())
}

/// Maintenance uses the same order as closure publication, never waits, and
/// never obtains a global lock after a shard/per-ID lock.
pub(crate) fn with_backup_maintenance_lock(
    parent: &Utf8Path,
    id: &str,
    action: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let _global = match global_commit_rwlock().try_read() {
        Ok(guard) => guard,
        Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
        Err(error) => anyhow::bail!("install global lock unavailable: {error}"),
    };
    let global_file = open_install_lock(&global_lock_path(parent))?;
    match global_file.try_lock_shared() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(()),
        Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
    }
    let _shard = match replace_mutex_for(id).try_lock() {
        Ok(guard) => guard,
        Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
        Err(error) => anyhow::bail!("install shard lock unavailable: {error}"),
    };
    let Some(_per_id) = try_acquire_file_lock(&parent.join(per_id_lock_name(id)))? else {
        return Ok(());
    };
    action()
}
