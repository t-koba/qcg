//! Linux cgroup-v2 delegation policy for trusted-host crash recovery.
//!
//! `trusted_host` workloads are trusted, not sandboxed against hostile code;
//! containers remain responsible for untrusted code. This module holds only
//! data, path checks and cleanup timing policy; the shared Linux launch
//! mechanism lives in `host_cgroup_launch`. Filesystem delegation
//! checks and kill/empty/remove live in the engine's host-cgroup owner.
//!
//! Configuration is explicit and optional with no default: the deployment sets
//! `QCG_HOST_CGROUP_PARENT` to an already-delegated cgroup directory. Absent
//! means no crash-cleanup claim; present-but-invalid fails before any host
//! process launch.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Environment variable naming the delegated cgroup parent.
pub const HOST_CGROUP_PARENT_ENV: &str = "QCG_HOST_CGROUP_PARENT";

/// Binding file retained in the run metadata directory.
pub const HOST_CGROUP_BINDING_FILE: &str = "host-cgroup.json";

/// Current binding schema version.
pub const HOST_CGROUP_BINDING_VERSION: u32 = 2;

/// Canonical cgroup-v2 mount point checked for prefix safety.
pub const CGROUP2_MOUNT: &str = "/sys/fs/cgroup";

/// Optional delegated-parent configuration. `None` is the default: existing
/// live process-group cancellation is preserved but no crash-cleanup claim
/// is made and the `Started`-operation gates stay authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HostCgroupConfig {
    parent: Option<PathBuf>,
}

impl HostCgroupConfig {
    /// Disabled configuration (the default).
    pub fn disabled() -> Self {
        Self { parent: None }
    }

    /// Explicit delegated parent. Path-prefix validation only; delegation
    /// and permission checks run at bind time before any host launch.
    pub fn from_parent(parent: Option<PathBuf>) -> Result<Self, String> {
        let Some(parent) = parent else {
            return Ok(Self::disabled());
        };
        validate_parent_path(&parent)?;
        Ok(Self {
            parent: Some(parent),
        })
    }

    /// Reads `QCG_HOST_CGROUP_PARENT`. Empty or unset means disabled.
    pub fn from_env() -> Result<Self, String> {
        let raw = std::env::var_os(HOST_CGROUP_PARENT_ENV)
            .map(|v| {
                v.into_string()
                    .map_err(|_| format!("{HOST_CGROUP_PARENT_ENV} must be UTF-8"))
            })
            .transpose()?;
        Self::resolve(|_| raw.clone())
    }

    /// Pure lookup seam for startup validation and tests; no later env reads.
    pub fn resolve(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, String> {
        let raw = lookup(HOST_CGROUP_PARENT_ENV)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let Some(raw) = raw else {
            return Ok(Self::disabled());
        };
        Self::from_parent(Some(PathBuf::from(raw)))
    }

    /// Delegated parent when enabled.
    pub fn parent(&self) -> Option<&Path> {
        self.parent.as_deref()
    }

    /// Whether crash recovery is enabled.
    pub fn is_enabled(&self) -> bool {
        self.parent.is_some()
    }
}

/// Cleanup confirmation timeout and polling interval, in milliseconds.
/// Defaults are deployment policy, not cgroup protocol limits.
pub fn host_cgroup_cleanup_timing(
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<[std::time::Duration; 2], String> {
    let mut values = [10_000, 50];
    for (index, name) in [
        "QCG_HOST_CGROUP_CLEANUP_TIMEOUT_MS",
        "QCG_HOST_CGROUP_POLL_MS",
    ]
    .iter()
    .enumerate()
    {
        if let Some(raw) = lookup(name).filter(|s| !s.trim().is_empty()) {
            values[index] = raw
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| format!("{name} must be a positive integer in milliseconds"))?;
        }
    }
    let durations = values.map(std::time::Duration::from_millis);
    if std::time::Instant::now()
        .checked_add(durations[0])
        .is_none()
    {
        return Err("host cgroup cleanup timeout exceeds the monotonic clock range".into());
    }
    Ok(durations)
}

/// Pure path-prefix check: absolute and beneath the cgroup-v2 mount.
/// Delegation, writability, and topology are proved at bind time.
pub fn validate_parent_path(parent: &Path) -> Result<(), String> {
    if !parent.is_absolute() {
        return Err(format!(
            "{HOST_CGROUP_PARENT_ENV} must be an absolute path under {CGROUP2_MOUNT}"
        ));
    }
    if !is_inside_cgroup_mount(parent) {
        return Err(format!(
            "{HOST_CGROUP_PARENT_ENV} must be under {CGROUP2_MOUNT}"
        ));
    }
    if parent == Path::new(CGROUP2_MOUNT) {
        return Err(format!(
            "{HOST_CGROUP_PARENT_ENV} must be a delegated subtree, not {CGROUP2_MOUNT} itself"
        ));
    }
    Ok(())
}

/// Whether `path` is strictly beneath the cgroup-v2 mount.
pub fn is_inside_cgroup_mount(path: &Path) -> bool {
    let mount = Path::new(CGROUP2_MOUNT);
    path != mount && path.starts_with(mount)
}

/// Whether `child` is strictly beneath `parent`.
pub fn is_strictly_inside(parent: &Path, child: &Path) -> bool {
    child != parent && child.starts_with(parent)
}

/// Sanitizes one run-id path component: alphanumerics, `-`, `_`, `.` stay,
/// everything else becomes `_`. Empty results become `_`.
pub fn sanitize_run_component(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|next| {
            if next.is_ascii_alphanumeric() || next == '-' || next == '_' || next == '.' {
                next
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "_".to_string()
    } else {
        cleaned
    }
}

/// Per-run-execution directory name. Unique per execution via the nonce.
pub fn run_root_name(run_id: &str, nonce: &str) -> String {
    format!("qcg-{}-{nonce}", sanitize_run_component(run_id))
}

/// Run-scoped crash-recovery capability. One value is created per run
/// execution, threaded through `RunContext`, `CmdGateway`, and each MCP
/// stdio connection, and durably bound before any host launch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostCgroupScope {
    /// Authorized delegated parent (canonicalized at bind time).
    pub delegated_parent: PathBuf,
    /// Unique per-run-execution root beneath the parent.
    pub run_root: PathBuf,
    /// Owning run id.
    pub run_id: String,
    /// Unique per-execution nonce.
    pub nonce: String,
    /// Kernel boot id at bind time (foreign-boot refusal).
    pub boot_id: String,
}

impl HostCgroupScope {
    /// Whether `candidate` is strictly inside this scope's delegated parent.
    pub fn authorizes(&self, candidate: &Path) -> bool {
        candidate == self.run_root || is_strictly_inside(&self.run_root, candidate)
    }
}

/// Durable binding retained in `meta/host-cgroup.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostCgroupBinding {
    /// Schema version (`HOST_CGROUP_BINDING_VERSION`).
    pub version: u32,
    /// Owning run id.
    pub run_id: String,
    /// Per-execution nonce naming the run root.
    pub nonce: String,
    /// Canonical delegated parent at bind time.
    pub delegated_parent: PathBuf,
    /// Canonical per-run-execution root.
    pub run_root: PathBuf,
    /// Kernel boot id at bind time.
    pub boot_id: String,
    /// Intent persisted before mkdir; never authorizes killing or removing a candidate.
    /// Legacy v1 bindings are already Bound, not Prepared.
    #[serde(default)]
    pub prepared: bool,
    /// Whether the subtree was retired (or a never-launched Prepared root was absent).
    #[serde(default)]
    pub cleaned: bool,
    /// Durable empty confirmation before directory retirement, or Prepared absence.
    #[serde(default)]
    pub empty_confirmed: bool,
}

impl HostCgroupBinding {
    /// Bound scope view; `None` while Prepared (intent, never authority).
    pub fn scope(&self) -> Option<HostCgroupScope> {
        if self.prepared {
            return None;
        }
        Some(HostCgroupScope {
            delegated_parent: self.delegated_parent.clone(),
            run_root: self.run_root.clone(),
            run_id: self.run_id.clone(),
            nonce: self.nonce.clone(),
            boot_id: self.boot_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_by_default_and_on_empty() {
        assert!(!HostCgroupConfig::disabled().is_enabled());
        assert!(!HostCgroupConfig::resolve(|_| None).unwrap().is_enabled());
        assert!(
            !HostCgroupConfig::resolve(
                |key| (key == HOST_CGROUP_PARENT_ENV).then(|| "   ".to_string())
            )
            .unwrap()
            .is_enabled()
        );
    }

    #[test]
    fn rejects_relative_outside_and_mount_itself() {
        for value in ["relative/dir", "/tmp/qcg", "/sys/fs/cgroup"] {
            let error = HostCgroupConfig::from_parent(Some(PathBuf::from(value))).unwrap_err();
            assert!(error.contains(HOST_CGROUP_PARENT_ENV), "{error}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn accepts_delegated_subtree() {
        let config =
            HostCgroupConfig::from_parent(Some(PathBuf::from("/sys/fs/cgroup/qcg.slice"))).unwrap();
        assert!(config.is_enabled());
        assert_eq!(
            config.parent().unwrap(),
            Path::new("/sys/fs/cgroup/qcg.slice")
        );
    }

    #[cfg(windows)]
    #[test]
    fn rejects_linux_mount_path_as_nonabsolute() {
        let error = HostCgroupConfig::from_parent(Some(PathBuf::from("/sys/fs/cgroup/qcg.slice")))
            .unwrap_err();
        assert!(error.contains(HOST_CGROUP_PARENT_ENV), "{error}");
    }

    #[test]
    fn sanitize_and_naming() {
        assert_eq!(sanitize_run_component(""), "_");
        assert_eq!(sanitize_run_component("a/b:c"), "a_b_c");
        assert_eq!(run_root_name("run 1", "n"), "qcg-run_1-n");
        assert!(is_inside_cgroup_mount(Path::new("/sys/fs/cgroup/a")));
        assert!(!is_inside_cgroup_mount(Path::new("/sys/fs/cgroup")));
        assert!(is_strictly_inside(
            Path::new("/sys/fs/cgroup/a"),
            Path::new("/sys/fs/cgroup/a/b")
        ));
        assert!(!is_strictly_inside(
            Path::new("/sys/fs/cgroup/a"),
            Path::new("/sys/fs/cgroup/a")
        ));
    }
}

#[cfg(test)]
mod timing_tests {
    #[test]
    fn host_cgroup_timing_defaults_and_invalid_config() {
        use super::host_cgroup_cleanup_timing;
        assert_eq!(
            host_cgroup_cleanup_timing(|_| None)
                .unwrap()
                .map(|d| d.as_millis()),
            [10_000, 50]
        );
        assert_eq!(
            host_cgroup_cleanup_timing(|_| Some("25".into()))
                .unwrap()
                .map(|d| d.as_millis()),
            [25, 25]
        );
        for value in ["0", "-1", "oops"] {
            assert!(host_cgroup_cleanup_timing(|_| Some(value.into())).is_err());
        }
    }
}
