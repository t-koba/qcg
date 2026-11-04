//! Linux cgroup-v2 crash-recovery owner for trusted-host processes.
//!
//! `trusted_host` workloads are trusted; this is whole-tree cleanup, not a
//! hostile-code sandbox. Containers remain responsible for untrusted code.
//! Configuration is explicit and optional (`QCG_HOST_CGROUP_PARENT`), default
//! absent: without it launches keep their live process-group behavior and no
//! crash-cleanup claim is made. When enabled but invalid, binding fails
//! before any host launch.
//!
//! One [`policy::HostCgroupScope`] is bound per run execution beneath the
//! authorized delegated parent, durably recorded in
//! `meta/host-cgroup.json` before any host command or MCP stdio startup
//! (MCP connection can precede its `operation_started` event). Children
//! self-place via a `pre_exec` write to the run root's `cgroup.procs`
//! before payload execution, with parent-death protection across the
//! fork-to-placement window: no post-spawn PID/PGID migration
//! is the containment boundary. Live `ProcessTreeGuard` cancellation is kept
//! where compatible. Reconciliation runs under the per-run execution lease
//! before `Started` replay/refusal, recovery/resume, deletion, and
//! retention GC, and bounded normal return retires only this run's tree.
//! Cleanup kills the whole run root (`cgroup.kill`), confirms
//! `cgroup.events` reports `populated 0`, removes nested cgroups
//! bottom-up, and preserves the binding on any timeout, error, or identity
//! ambiguity. Absent PIDs or directory deletion are never cleanup proof,
//! and parents, siblings, unrelated runs, or foreign hosts are never
//! targeted.

use camino::{Utf8Path, Utf8PathBuf};
#[cfg(target_os = "linux")]
use policy::is_strictly_inside;
use policy::{
    HOST_CGROUP_BINDING_FILE, HOST_CGROUP_BINDING_VERSION, HostCgroupBinding, HostCgroupConfig,
    HostCgroupScope, run_root_name,
};
use std::io::Read;
#[cfg(target_os = "linux")]
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::time::Instant;

// Fixed identity-record schema bound, not a history/retention limit.
const MAX_BINDING_BYTES: u64 = 16 * 1024;

/// Host-cgroup failure. All variants fail closed: the binding and journal
/// are preserved and no sibling, parent, or foreign cgroup is touched.
#[derive(Debug, thiserror::Error)]
pub enum HostCgroupError {
    #[error("{0}")]
    Config(String),
    #[error("{0}")]
    Bind(String),
    #[error("{0}")]
    Identity(String),
    #[error("{0}")]
    Cleanup(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl HostCgroupError {
    fn config(detail: impl Into<String>) -> Self {
        Self::Config(detail.into())
    }
    #[cfg(target_os = "linux")]
    fn bind(detail: impl Into<String>) -> Self {
        Self::Bind(detail.into())
    }
    fn identity(detail: impl Into<String>) -> Self {
        Self::Identity(detail.into())
    }
    fn cleanup(detail: impl Into<String>) -> Self {
        Self::Cleanup(detail.into())
    }
}

#[cfg(target_os = "linux")]
fn cleanup_timing() -> Result<[std::time::Duration; 2], HostCgroupError> {
    policy::host_cgroup_cleanup_timing(|name| std::env::var(name).ok())
        .map_err(HostCgroupError::config)
}

/// Outcome of [`reconcile_host_cgroup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostCgroupReconcile {
    /// No binding file: nothing to clean, no claim made.
    NotBound,
    /// Binding already marked cleaned: idempotent no-op.
    AlreadyCleaned,
    /// Subtree was killed, confirmed empty, and removed.
    Cleaned,
}

/// Reads the optional delegation configuration. Absent or blank means
/// disabled (no claim); present-but-invalid is an error.
pub fn host_cgroup_config_from_env() -> Result<HostCgroupConfig, HostCgroupError> {
    HostCgroupConfig::from_env().map_err(HostCgroupError::config)
}

/// Whether the current host can enforce a cgroup boundary at all.
#[cfg(target_os = "linux")]
pub fn host_cgroup_supported() -> bool {
    Path::new(policy::CGROUP2_MOUNT).is_dir()
        && Path::new("/sys/fs/cgroup/cgroup.controllers").is_file()
        && std::fs::read_to_string("/proc/self/cgroup")
            .map(|contents| contents.lines().any(|line| line.starts_with("0::")))
            .unwrap_or(false)
}

/// Non-Linux hosts never support the Linux-only boundary.
#[cfg(not(target_os = "linux"))]
pub fn host_cgroup_supported() -> bool {
    false
}

/// Whether integration tests requiring a writable delegation should run.
/// Unconfigured hosts skip; configured but unusable delegation panics so
/// CI can never silently skip its required containment coverage.
pub fn host_cgroup_tests_enabled() -> bool {
    #[cfg(not(target_os = "linux"))]
    {
        return false;
    }
    #[cfg(target_os = "linux")]
    {
        let config = host_cgroup_config_from_env().expect("invalid configured cgroup delegation");
        let Some(parent) = config.parent() else {
            return false;
        };
        validate_delegated_parent(parent).expect("configured cgroup delegation is unusable");
        true
    }
}

/// Validates that `parent` is a usable delegated cgroup-v2 parent for the
/// current process. Fails before any host launch when invalid. No
/// sudo/chown is ever performed: the parent must already be delegated.
#[cfg(target_os = "linux")]
pub fn validate_delegated_parent(parent: &Path) -> Result<PathBuf, HostCgroupError> {
    if !host_cgroup_supported() {
        return Err(HostCgroupError::config(
            "host cgroup recovery requires Linux cgroup v2",
        ));
    }
    policy::validate_parent_path(parent).map_err(HostCgroupError::config)?;
    let canonical = parent.canonicalize().map_err(|error| {
        HostCgroupError::config(format!("delegated parent is not usable: {error}"))
    })?;
    if !policy::is_inside_cgroup_mount(&canonical) {
        return Err(HostCgroupError::config(
            "delegated parent canonical path escapes /sys/fs/cgroup",
        ));
    }
    // A path beneath the conventional mount is not by itself proof of v2.
    use std::os::fd::AsRawFd;
    let directory = std::fs::File::open(&canonical)?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: the descriptor is live and stat points to writable storage.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: fstatfs succeeded and initialized stat.
    if unsafe { stat.assume_init() }.f_type != libc::CGROUP2_SUPER_MAGIC {
        return Err(HostCgroupError::config("delegated parent is not cgroup v2"));
    }
    validate_cgroup_files(&canonical)?;
    Ok(canonical)
}

// No resource controller is enabled or required: in particular,
// cgroup.subtree_control need not be writable. The parent is never killed.
#[cfg(target_os = "linux")]
fn validate_cgroup_files(root: &Path) -> Result<(), HostCgroupError> {
    open_cgroup_file(root, "cgroup.procs", true)?;
    open_cgroup_file(root, "cgroup.events", false)?;
    read_occupancy(root)?;
    if std::fs::read_to_string(root.join("cgroup.type"))?.trim() != "domain" {
        return Err(HostCgroupError::config(
            "host cgroup recovery requires domain cgroups",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_run_cgroup_files(root: &Path) -> Result<(), HostCgroupError> {
    validate_cgroup_files(root)?;
    // Only per-run roots need kill permission; never open the parent's kill file.
    open_cgroup_file(root, "cgroup.kill", true)
}

#[cfg(target_os = "linux")]
fn open_cgroup_file(root: &Path, leaf: &str, write: bool) -> Result<(), HostCgroupError> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(!write)
        .write(write)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join(leaf))
        .map_err(|error| HostCgroupError::config(format!("unusable {leaf}: {error}")))?;
    if !file.metadata()?.is_file() {
        return Err(HostCgroupError::config(format!(
            "{leaf} is not a regular cgroup file"
        )));
    }
    Ok(())
}

/// Non-Linux validation always fails closed when enabled.
#[cfg(not(target_os = "linux"))]
pub fn validate_delegated_parent(parent: &Path) -> Result<PathBuf, HostCgroupError> {
    let _ = parent;
    Err(HostCgroupError::config(
        "host cgroup recovery is Linux cgroup-v2 only",
    ))
}

/// Current kernel boot id for foreign-boot refusal.
#[cfg(target_os = "linux")]
pub fn current_boot_id() -> Result<String, HostCgroupError> {
    let raw = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map_err(|error| HostCgroupError::identity(format!("cannot read boot id: {error}")))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        return Err(HostCgroupError::identity("boot id is empty"));
    }
    Ok(trimmed)
}

#[cfg(not(target_os = "linux"))]
pub fn current_boot_id() -> Result<String, HostCgroupError> {
    Err(HostCgroupError::identity("boot identity is Linux-only"))
}

/// Canonical binding path inside the run metadata directory.
pub fn binding_path(meta_dir: &Utf8Path) -> Utf8PathBuf {
    meta_dir.join(HOST_CGROUP_BINDING_FILE)
}

/// Loads the persisted binding when present. Missing means never bound.
pub fn load_host_binding(
    meta_dir: &Utf8Path,
) -> Result<Option<HostCgroupBinding>, HostCgroupError> {
    let path = binding_path(meta_dir);
    // Also refuse reparse/symlink records on non-Unix hosts; Unix's open below
    // is authoritative against leaf replacement between this check and open.
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            return Err(HostCgroupError::identity(
                "host-cgroup binding is not a regular file",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        _ => {}
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !file.metadata()?.is_file() || file.metadata()?.len() > MAX_BINDING_BYTES {
        return Err(HostCgroupError::identity(
            "host-cgroup binding is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BINDING_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BINDING_BYTES {
        return Err(HostCgroupError::identity(
            "host-cgroup binding exceeds schema bound",
        ));
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}

/// Binds one unique per-run-execution root under the authorized parent and
/// durably records it before any host launch. Returns `None` when disabled.
pub fn bind_host_cgroup(
    config: &HostCgroupConfig,
    meta_dir: &Utf8Path,
    run_id: &str,
) -> Result<Option<HostCgroupScope>, HostCgroupError> {
    let Some(parent) = config.parent() else {
        return Ok(None);
    };
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (meta_dir, run_id);
        return Err(HostCgroupError::config(
            "host cgroup recovery is Linux cgroup-v2 only",
        ));
    }
    #[cfg(target_os = "linux")]
    {
        if let Some(binding) = load_host_binding(meta_dir)?
            && !binding.cleaned
        {
            return Err(HostCgroupError::bind(
                "unretired host binding; reconcile under execution lease first",
            ));
        }
        cleanup_timing()?;
        let canonical_parent = validate_delegated_parent(parent)?;
        let boot_id = current_boot_id()?;
        let nonce = uuid::Uuid::now_v7().as_simple().to_string();
        let name = run_root_name(run_id, &nonce);
        let run_root = canonical_parent.join(&name);
        // Guard against parent/sibling confusion before creation.
        if !is_strictly_inside(&canonical_parent, &run_root) {
            return Err(HostCgroupError::bind("run root escapes delegated parent"));
        }
        std::fs::create_dir(&run_root).map_err(|error| {
            HostCgroupError::bind(format!("cannot create per-run cgroup root: {error}"))
        })?;
        let canonical_root = run_root.canonicalize().map_err(|error| {
            // Preserve the created directory for diagnosis; binding is not recorded.
            HostCgroupError::bind(format!("cannot canonicalize per-run cgroup root: {error}"))
        })?;
        if !is_strictly_inside(&canonical_parent, &canonical_root) {
            return Err(HostCgroupError::bind(
                "per-run cgroup root canonical path escapes delegated parent",
            ));
        }
        validate_delegated_parent(&canonical_root)?;
        validate_run_cgroup_files(&canonical_root)?;
        // Empty leaf: prove kill works, without touching the owner/parent.
        std::fs::write(canonical_root.join("cgroup.kill"), "1")?;
        if read_occupancy(&canonical_root)? {
            return Err(HostCgroupError::bind(
                "new run cgroup unexpectedly occupied",
            ));
        }
        let binding = HostCgroupBinding {
            version: HOST_CGROUP_BINDING_VERSION,
            run_id: run_id.to_string(),
            nonce: nonce.clone(),
            delegated_parent: canonical_parent.clone(),
            run_root: canonical_root.clone(),
            boot_id: boot_id.clone(),
            cleaned: false,
            empty_confirmed: false,
        };
        write_binding_atomically(meta_dir, &binding)?;
        Ok(Some(HostCgroupScope {
            delegated_parent: canonical_parent,
            run_root: canonical_root,
            run_id: run_id.to_string(),
            nonce,
            boot_id,
        }))
    }
}

#[cfg(target_os = "linux")]
fn write_binding_atomically(
    meta_dir: &Utf8Path,
    binding: &HostCgroupBinding,
) -> Result<(), HostCgroupError> {
    std::fs::create_dir_all(meta_dir.as_std_path())?;
    let path = binding_path(meta_dir);
    let contents = serde_json::to_vec_pretty(binding)?;
    if contents.len() as u64 > MAX_BINDING_BYTES {
        return Err(HostCgroupError::identity(
            "host-cgroup binding exceeds schema bound",
        ));
    }
    let tmp = meta_dir.join(format!(
        ".{HOST_CGROUP_BINDING_FILE}.tmp-{}",
        uuid::Uuid::now_v7().as_simple()
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)?;
    file.write_all(&contents)?;
    file.sync_all()?;
    std::fs::rename(tmp.as_std_path(), path.as_std_path())?;
    std::fs::File::open(meta_dir)?.sync_all()?;
    Ok(())
}

/// Reconciles the persisted binding under the caller's exclusive execution
/// lease. Still uses the persisted parent when the current configuration
/// was removed; a changed nonempty parent is refused; any mismatch, foreign boot, or ambiguity
/// preserves the binding and refuses instead of targeting another cgroup.
pub fn reconcile_host_cgroup(
    meta_dir: &Utf8Path,
    current: &HostCgroupConfig,
    expected_run_id: &str,
) -> Result<HostCgroupReconcile, HostCgroupError> {
    let Some(binding) = load_host_binding(meta_dir)? else {
        let _ = current;
        return Ok(HostCgroupReconcile::NotBound);
    };
    validate_binding(&binding, current, expected_run_id)?;
    if binding.cleaned {
        return Ok(HostCgroupReconcile::AlreadyCleaned);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = current;
        return Err(HostCgroupError::identity(
            "persisted host-cgroup binding requires Linux to reconcile; refusing",
        ));
    }
    #[cfg(target_os = "linux")]
    {
        // The persisted parent must still be a valid delegation for this
        // process; otherwise reconciliation cannot prove ownership.
        let canonical_parent = validate_delegated_parent(&binding.delegated_parent)?;
        if canonical_parent != binding.delegated_parent {
            return Err(HostCgroupError::identity(
                "host-cgroup delegated parent identity changed; refusing",
            ));
        }
        retire_binding(meta_dir, binding)?;
        Ok(HostCgroupReconcile::Cleaned)
    }
}

/// Retires one run's tree after bounded normal execution. Only this run's
/// root is targeted; parents and siblings are never touched.
pub fn retire_host_scope(meta: &Utf8Path, scope: &HostCgroupScope) -> Result<(), HostCgroupError> {
    let binding = load_host_binding(meta)?
        .ok_or_else(|| HostCgroupError::identity("missing host binding"))?;
    validate_binding(&binding, &HostCgroupConfig::disabled(), &scope.run_id)?;
    if binding.scope() != *scope {
        return Err(HostCgroupError::identity(
            "host binding changed during execution",
        ));
    }
    retire_binding(meta, binding)
}

fn validate_binding(
    binding: &HostCgroupBinding,
    current: &HostCgroupConfig,
    expected: &str,
) -> Result<(), HostCgroupError> {
    validate_binding_record(binding, expected)?;
    // Check even cleaned records: their persisted parent must be canonical,
    // not a lexical alias that could redirect a future binding or retirement.
    if binding.delegated_parent.canonicalize()? != binding.delegated_parent {
        return Err(HostCgroupError::identity(
            "delegated parent is not canonical; refusing",
        ));
    }
    if let Some(parent) = current.parent()
        && parent.canonicalize()? != binding.delegated_parent
    {
        return Err(HostCgroupError::identity(
            "configured delegated parent changed; refusing",
        ));
    }
    Ok(())
}

fn validate_binding_record(
    binding: &HostCgroupBinding,
    expected: &str,
) -> Result<(), HostCgroupError> {
    if binding.version != HOST_CGROUP_BINDING_VERSION
        || binding.run_id != expected
        || binding.nonce.len() != 32
        || !binding
            .nonce
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        || policy::validate_parent_path(&binding.delegated_parent).is_err()
        || binding.delegated_parent.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
        || binding.run_root
            != binding
                .delegated_parent
                .join(run_root_name(expected, &binding.nonce))
    {
        return Err(HostCgroupError::identity(
            "host-cgroup binding run/nonce/root/parent identity mismatch; refusing",
        ));
    }
    if current_boot_id()? != binding.boot_id {
        return Err(HostCgroupError::identity(
            "host-cgroup binding is from a different boot; refusing",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn retire_binding(meta: &Utf8Path, mut binding: HostCgroupBinding) -> Result<(), HostCgroupError> {
    match std::fs::symlink_metadata(&binding.run_root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && binding.empty_confirmed => {}
        Err(error) => return Err(error.into()),
        Ok(_) => {
            kill_and_confirm(&binding.scope())?;
            binding.empty_confirmed = true;
            write_binding_atomically(meta, &binding)?;
            remove_nested_cgroups(&binding.run_root)?;
            std::fs::remove_dir(&binding.run_root)?;
        }
    }
    binding.cleaned = true;
    write_binding_atomically(meta, &binding)
}

#[cfg(not(target_os = "linux"))]
fn retire_binding(_meta: &Utf8Path, _binding: HostCgroupBinding) -> Result<(), HostCgroupError> {
    Err(HostCgroupError::config(
        "host cgroup recovery requires Linux",
    ))
}

/// Kills the complete per-run subtree, confirms `populated 0`, and removes
/// nested cgroups bottom-up. Preserves evidence on any failure.
#[cfg(target_os = "linux")]
pub fn cleanup_scope(scope: &HostCgroupScope) -> Result<(), HostCgroupError> {
    kill_and_confirm(scope)?;
    remove_nested_cgroups(&scope.run_root)?;
    std::fs::remove_dir(&scope.run_root)?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn kill_and_confirm(scope: &HostCgroupScope) -> Result<(), HostCgroupError> {
    validate_binding(
        &HostCgroupBinding {
            version: HOST_CGROUP_BINDING_VERSION,
            run_id: scope.run_id.clone(),
            nonce: scope.nonce.clone(),
            delegated_parent: scope.delegated_parent.clone(),
            run_root: scope.run_root.clone(),
            boot_id: scope.boot_id.clone(),
            cleaned: false,
            empty_confirmed: false,
        },
        &HostCgroupConfig::disabled(),
        &scope.run_id,
    )?;
    if scope.run_root.canonicalize()? != scope.run_root {
        return Err(HostCgroupError::identity("run root identity changed"));
    }
    validate_delegated_parent(&scope.run_root)?;
    validate_run_cgroup_files(&scope.run_root)?;
    let [timeout, poll] = cleanup_timing()?;
    // Kill the whole subtree.
    std::fs::write(scope.run_root.join("cgroup.kill"), "1")
        .map_err(|error| HostCgroupError::cleanup(format!("cannot write cgroup.kill: {error}")))?;
    // Confirm emptiness via `cgroup.events` (`populated 0`), not via absent
    // PIDs or directory deletion.
    let deadline = Instant::now() + timeout;
    loop {
        if !read_occupancy(&scope.run_root)? {
            break;
        }
        if Instant::now() >= deadline {
            return Err(HostCgroupError::cleanup(
                "timed out waiting for cgroup.events populated 0; binding preserved",
            ));
        }
        std::thread::sleep(poll.min(deadline.saturating_duration_since(Instant::now())));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn cleanup_scope(_scope: &HostCgroupScope) -> Result<(), HostCgroupError> {
    Err(HostCgroupError::cleanup(
        "host cgroup cleanup is Linux cgroup-v2 only",
    ))
}

/// Removes nested child cgroups deepest-first without following symlinks.
#[cfg(target_os = "linux")]
fn remove_nested_cgroups(root: &Path) -> Result<(), HostCgroupError> {
    let mut descendants: Vec<PathBuf> = Vec::new();
    collect_child_cgroups(root, root, &mut descendants)?;
    // Deepest first so parents are empty when removed.
    descendants.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for child in descendants {
        if !is_strictly_inside(root, &child) {
            return Err(HostCgroupError::cleanup(
                "nested cgroup escapes per-run root; refusing",
            ));
        }
        match std::fs::remove_dir(&child) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(HostCgroupError::cleanup(format!(
                    "cannot remove nested cgroup {}: {error}",
                    child.display()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn collect_child_cgroups(
    root: &Path,
    current: &Path,
    out: &mut Vec<PathBuf>,
) -> Result<(), HostCgroupError> {
    let entries = std::fs::read_dir(current).map_err(|error| {
        HostCgroupError::cleanup(format!("cannot list cgroup subtree: {error}"))
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            HostCgroupError::cleanup(format!("cannot read cgroup entry: {error}"))
        })?;
        let file_type = entry.file_type().map_err(|error| {
            HostCgroupError::cleanup(format!("cannot stat cgroup entry: {error}"))
        })?;
        // Never follow symlinks inside the cgroup filesystem.
        if file_type.is_symlink() {
            continue;
        }
        if !file_type.is_dir() {
            continue;
        }
        let path = entry.path();
        if !is_strictly_inside(root, &path) {
            return Err(HostCgroupError::cleanup(
                "nested cgroup escapes per-run root; refusing",
            ));
        }
        // Only descend into real cgroups (those with `cgroup.procs`).
        if !path.join("cgroup.procs").is_file() {
            continue;
        }
        collect_child_cgroups(root, &path, out)?;
        out.push(path);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn read_occupancy(root: &Path) -> Result<bool, HostCgroupError> {
    let events = std::fs::read_to_string(root.join("cgroup.events"))?;
    match events
        .lines()
        .find_map(|line| line.strip_prefix("populated "))
    {
        Some("0") => Ok(false),
        Some("1") => Ok(true),
        _ => Err(HostCgroupError::config(
            "missing/invalid cgroup.events populated field",
        )),
    }
}

/// Applies race-free self-placement to a not-yet-spawned std command.
/// The child writes its own pid into the run root before payload execution,
/// so a qcg death between fork and exec cannot leave it outside. Must be
/// called before `spawn`; post-spawn migration is never the boundary.
#[cfg(target_os = "linux")]
pub fn apply_scope_to_std_command(
    command: &mut std::process::Command,
    scope: &HostCgroupScope,
) -> Result<(), HostCgroupError> {
    policy::host_cgroup_launch::apply_scope(command, scope).map_err(HostCgroupError::Io)
}

/// Non-Linux launches with a scope fail closed.
#[cfg(not(target_os = "linux"))]
pub fn apply_scope_to_std_command(
    _command: &mut std::process::Command,
    _scope: &HostCgroupScope,
) -> Result<(), HostCgroupError> {
    Err(HostCgroupError::config(
        "host cgroup launch is Linux cgroup-v2 only",
    ))
}

/// Applies the scope to a Tokio command before spawn. `None` is a no-op.
pub fn apply_scope_to_tokio_command(
    command: &mut tokio::process::Command,
    scope: Option<&HostCgroupScope>,
) -> Result<(), HostCgroupError> {
    let Some(scope) = scope else {
        return Ok(());
    };
    #[cfg(target_os = "linux")]
    {
        apply_scope_to_std_command(command.as_std_mut(), scope)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = command;
        Err(HostCgroupError::config(
            "host cgroup launch is Linux cgroup-v2 only",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_config_has_no_binding() {
        let config = HostCgroupConfig::disabled();
        assert!(!config.is_enabled());
        let dir = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-host-disabled-{}", uuid::Uuid::now_v7())),
        )
        .unwrap();
        std::fs::create_dir_all(dir.as_std_path()).unwrap();
        let scope = bind_host_cgroup(&config, &dir, "run").expect("disabled bind should succeed");
        assert!(scope.is_none());
        assert!(load_host_binding(&dir).unwrap().is_none());
        assert!(matches!(
            reconcile_host_cgroup(&dir, &config, "run").unwrap(),
            HostCgroupReconcile::NotBound
        ));
        let _ = std::fs::remove_dir_all(dir.as_std_path());
    }

    #[test]
    fn invalid_parent_fails_before_launch() {
        for value in ["/tmp/qcg", "relative", "/sys/fs/cgroup"] {
            let error =
                HostCgroupConfig::from_parent(Some(std::path::PathBuf::from(value))).unwrap_err();
            assert!(error.contains(policy::HOST_CGROUP_PARENT_ENV), "{error}");
        }
        // Non-absolute and outside-mount parents are rejected without touching
        // the filesystem, so this runs in the read-only sandbox.
        let config = HostCgroupConfig::resolve(|key| {
            (key == policy::HOST_CGROUP_PARENT_ENV).then(|| "/tmp/qcg".to_string())
        })
        .unwrap_err();
        assert!(config.contains(policy::HOST_CGROUP_PARENT_ENV));
    }

    #[test]
    fn scope_authorization_never_targets_parent_or_sibling() {
        let scope = HostCgroupScope {
            delegated_parent: std::path::PathBuf::from("/sys/fs/cgroup/qcg.slice"),
            run_root: std::path::PathBuf::from("/sys/fs/cgroup/qcg.slice/qcg-run-nonce"),
            run_id: "run".into(),
            nonce: "nonce".into(),
            boot_id: "boot".into(),
        };
        assert!(scope.authorizes(&scope.run_root));
        assert!(!scope.authorizes(&scope.delegated_parent));
        assert!(!scope.authorizes(&std::path::PathBuf::from(
            "/sys/fs/cgroup/other/qcg-run-nonce"
        )));
        assert!(!scope.authorizes(&std::path::PathBuf::from("/sys/fs/cgroup")));
    }
}

#[cfg(all(test, target_os = "linux"))]
mod identity_tests {
    use super::*;
    fn fixture(meta: &Utf8Path) -> HostCgroupBinding {
        let parent = PathBuf::from("/sys/fs/cgroup/qcg-test");
        let nonce = "a".repeat(32);
        let binding = HostCgroupBinding {
            version: HOST_CGROUP_BINDING_VERSION,
            run_id: "run".into(),
            nonce: nonce.clone(),
            run_root: parent.join(run_root_name("run", &nonce)),
            delegated_parent: parent,
            boot_id: current_boot_id().unwrap(),
            cleaned: true,
            empty_confirmed: true,
        };
        write_binding_atomically(meta, &binding).unwrap();
        binding
    }
    #[test]
    fn host_cgroup_identity_checks_precede_any_cleanup() {
        let meta = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-identity-{}", uuid::Uuid::now_v7())),
        )
        .unwrap();
        let valid = fixture(&meta);
        let disabled = HostCgroupConfig::disabled();
        validate_binding_record(&valid, "run").unwrap();
        assert!(reconcile_host_cgroup(&meta, &disabled, "another-run").is_err());
        let changed = HostCgroupConfig::from_parent(Some("/sys/fs/cgroup/other".into())).unwrap();
        assert!(reconcile_host_cgroup(&meta, &changed, "run").is_err());
        for field in [
            "run_id", "sibling", "nonce", "nonhex", "boot", "parent", "version",
        ] {
            let mut binding = valid.clone();
            match field {
                "run_id" => binding.run_id = "sibling-run".into(),
                "sibling" => {
                    binding.run_root = binding
                        .delegated_parent
                        .join(run_root_name("sibling-run", &binding.nonce))
                }
                "nonce" => binding.nonce = "bad".into(),
                "nonhex" => binding.nonce = "z".repeat(32),
                "boot" => binding.boot_id = "foreign".into(),
                "parent" => binding.delegated_parent.push("../qcg-test"),
                _ => binding.version += 1,
            }
            write_binding_atomically(&meta, &binding).unwrap();
            assert!(
                matches!(
                    reconcile_host_cgroup(&meta, &disabled, "run"),
                    Err(HostCgroupError::Identity(_))
                ),
                "{field}"
            );
            assert_eq!(load_host_binding(&meta).unwrap().unwrap(), binding);
        }
        std::fs::remove_dir_all(meta).unwrap();
    }
    #[test]
    fn host_cgroup_sidecar_is_bounded_and_nofollow() {
        let meta = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-sidecar-{}", uuid::Uuid::now_v7())),
        )
        .unwrap();
        fixture(&meta);
        let path = binding_path(&meta);
        let target = meta.join("target");
        std::fs::rename(&path, &target).unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(load_host_binding(&meta).is_err());
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, vec![b' '; 16 * 1024 + 1]).unwrap();
        assert!(load_host_binding(&meta).is_err());
        std::fs::write(&path, b"{invalid json").unwrap();
        assert!(matches!(
            load_host_binding(&meta),
            Err(HostCgroupError::Json(_))
        ));
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(load_host_binding(&meta).is_err());
        assert!(target.exists());
        std::fs::remove_dir_all(meta).unwrap();
    }
    #[test]
    fn host_cgroup_updates_replace_atomically_and_preserve_evidence_on_error() {
        let meta = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-atomic-{}", uuid::Uuid::now_v7())),
        )
        .unwrap();
        let mut binding = fixture(&meta);
        let mut original = std::fs::File::open(binding_path(&meta)).unwrap();
        binding.cleaned = false;
        write_binding_atomically(&meta, &binding).unwrap();
        let mut previous = String::new();
        original.read_to_string(&mut previous).unwrap();
        assert!(
            serde_json::from_str::<HostCgroupBinding>(&previous)
                .unwrap()
                .cleaned
        );
        assert!(!load_host_binding(&meta).unwrap().unwrap().cleaned);
        let before = std::fs::read(binding_path(&meta)).unwrap();
        binding.run_id = "x".repeat(MAX_BINDING_BYTES as usize);
        assert!(write_binding_atomically(&meta, &binding).is_err());
        assert_eq!(std::fs::read(binding_path(&meta)).unwrap(), before);
        std::fs::remove_dir_all(meta).unwrap();
    }

    #[test]
    fn host_cgroup_only_run_roots_require_kill() {
        // This exercises the actual file/occupancy preflight on ordinary files,
        // not kernel support or delegation (covered by the opt-in test below).
        let root = std::env::temp_dir().join(format!("qcg-preflight-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        for (name, contents) in [
            ("cgroup.procs", ""),
            ("cgroup.type", "domain\n"),
            ("cgroup.events", "populated 0\n"),
        ] {
            std::fs::write(root.join(name), contents).unwrap();
        }
        validate_cgroup_files(&root).unwrap(); // no kill or subtree_control file
        assert!(
            validate_run_cgroup_files(&root)
                .unwrap_err()
                .to_string()
                .contains("cgroup.kill")
        );
        // O_NOFOLLOW would reject this if parent validation opened cgroup.kill.
        std::os::unix::fs::symlink(root.join("cgroup.procs"), root.join("cgroup.kill")).unwrap();
        validate_cgroup_files(&root).unwrap();
        assert!(validate_run_cgroup_files(&root).is_err());
        std::fs::remove_file(root.join("cgroup.kill")).unwrap();
        std::fs::write(root.join("cgroup.kill"), "").unwrap();
        validate_run_cgroup_files(&root).unwrap();
        for leaf in ["cgroup.kill", "cgroup.events"] {
            let path = root.join(leaf);
            let original = std::fs::read(&path).unwrap();
            std::fs::remove_file(&path).unwrap();
            assert!(
                validate_run_cgroup_files(&root)
                    .unwrap_err()
                    .to_string()
                    .contains(leaf)
            );
            std::os::unix::fs::symlink(root.join("cgroup.procs"), &path).unwrap();
            assert!(validate_run_cgroup_files(&root).is_err());
            std::fs::remove_file(&path).unwrap();
            std::fs::write(path, original).unwrap();
        }
        std::fs::write(root.join("cgroup.events"), "frozen 0\n").unwrap();
        assert!(validate_run_cgroup_files(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn host_cgroup_persisted_parent_is_canonical_and_survives_unset_config() {
        if !host_cgroup_tests_enabled() {
            eprintln!(
                "SKIP canonical persisted-parent reconciliation: no writable cgroup delegation"
            );
            return;
        }
        let meta = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-canonical-{}", uuid::Uuid::now_v7())),
        )
        .unwrap();
        let config = host_cgroup_config_from_env().unwrap();
        let scope = bind_host_cgroup(&config, &meta, "run").unwrap().unwrap();
        let binding = load_host_binding(&meta).unwrap().unwrap();
        // The run root is another existing cgroup beneath the same parent,
        // not a nonexistent path that would fail for an unrelated IO reason.
        let changed = HostCgroupConfig::from_parent(Some(scope.run_root.clone())).unwrap();
        assert!(matches!(
            reconcile_host_cgroup(&meta, &changed, "run"),
            Err(HostCgroupError::Identity(_))
        ));
        assert_eq!(load_host_binding(&meta).unwrap().unwrap(), binding);
        let mut tampered = binding.clone();
        tampered.delegated_parent.push("../");
        tampered
            .delegated_parent
            .push(scope.delegated_parent.file_name().unwrap());
        tampered.run_root = tampered
            .delegated_parent
            .join(run_root_name("run", &binding.nonce));
        write_binding_atomically(&meta, &tampered).unwrap();
        assert!(reconcile_host_cgroup(&meta, &config, "run").is_err());
        write_binding_atomically(&meta, &binding).unwrap();
        let alias = HostCgroupConfig::from_parent(Some(scope.delegated_parent.join("."))).unwrap();
        validate_binding(&binding, &alias, "run").unwrap();
        assert_eq!(
            reconcile_host_cgroup(&meta, &HostCgroupConfig::disabled(), "run").unwrap(),
            HostCgroupReconcile::Cleaned
        );
        assert_eq!(
            reconcile_host_cgroup(&meta, &config, "run").unwrap(),
            HostCgroupReconcile::AlreadyCleaned
        );
        std::fs::remove_dir_all(meta).unwrap();
    }
}
