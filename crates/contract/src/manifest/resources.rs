use policy::DEFAULT_MAX_TOTAL_STEPS;

use crate::ContractError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceDef {
    #[serde(rename = "type")]
    pub kind: ResourceKind,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub pin_sha256: Option<String>,
    #[serde(default)]
    pub cache_ttl_seconds: Option<u64>,
    #[serde(default)]
    pub trust: Trust,
    #[serde(default)]
    pub llm_visible: bool,
    /// Type-specific bounded settings for built-in resource loaders.
    #[serde(default)]
    pub params: serde_json::Map<String, Value>,
}

/// Selector policy for a `run_ref` resource. Exactly one selector is
/// required; the engine/service resolves it to one concrete run and pins
/// the artifact revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunRefSelector {
    #[serde(default)]
    pub run_id: Option<String>,
    /// Newest successful run of this generator id.
    #[serde(default)]
    pub latest_success: Option<String>,
    /// Newest terminal run of this generator id.
    #[serde(default)]
    pub latest_terminal: Option<String>,
}

impl RunRefSelector {
    /// The single selected strategy with its operand, or `None` when zero or
    /// more than one selector is set.
    pub fn single(&self) -> Option<(&'static str, &str)> {
        let mut found = None;
        for (kind, value) in [
            ("run_id", self.run_id.as_deref()),
            ("latest_success", self.latest_success.as_deref()),
            ("latest_terminal", self.latest_terminal.as_deref()),
        ] {
            if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
                if found.is_some() {
                    return None;
                }
                found = Some((kind, value));
            }
        }
        found
    }
}

/// Parsed `[resources.<name>.params]` for `run_ref`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunRefParams {
    pub selector: RunRefSelector,
    /// Declared artifact path in the source run's output manifest.
    pub artifact: String,
    /// Explicit byte bound for the copied artifact.
    pub max_bytes: usize,
    #[serde(default)]
    pub require_sha256: Option<String>,
}

impl ResourceDef {
    /// Parses and validates `run_ref` params. Errors name the resource.
    pub fn run_ref_params(&self, name: &str) -> Result<RunRefParams, ContractError> {
        if self.kind != ResourceKind::RunRef {
            return Err(ContractError::Invalid(format!(
                "resource `{name}` is not a run_ref resource"
            )));
        }
        let params: RunRefParams = serde_json::from_value(Value::Object(self.params.clone()))
            .map_err(|error| {
                ContractError::Invalid(format!(
                    "resource `{name}` type `run_ref` has invalid params: {error}"
                ))
            })?;
        if params.selector.single().is_none() {
            return Err(ContractError::Invalid(format!(
                "resource `{name}` type `run_ref` requires exactly one selector: run_id, latest_success, or latest_terminal"
            )));
        }
        if params.artifact.trim().is_empty()
            || !policy::is_safe_relative_path(params.artifact.trim())
        {
            return Err(ContractError::Invalid(format!(
                "resource `{name}` type `run_ref` artifact must be a safe relative path"
            )));
        }
        if params.max_bytes == 0 {
            return Err(ContractError::Invalid(format!(
                "resource `{name}` type `run_ref` max_bytes must be greater than zero"
            )));
        }
        if let Some(sha) = &params.require_sha256
            && (sha.len() != 64 || !sha.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(ContractError::Invalid(format!(
                "resource `{name}` type `run_ref` require_sha256 must be 64 hexadecimal characters"
            )));
        }
        Ok(params)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    File,
    Dir,
    Skill,
    SkillLibrary,
    Url,
    Openapi,
    Exec,
    /// Immutable, hash-pinned snapshot of another run's declared artifact.
    /// The selector is policy; the mechanism resolves it once, copies the
    /// bytes into this run's workspace, and pins the revision so resume and
    /// replay never depend on the source run surviving retention.
    RunRef,
}

impl ResourceKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::File => "file",
            Self::Dir => "dir",
            Self::Skill => "skill",
            Self::SkillLibrary => "skill_library",
            Self::Url => "url",
            Self::Openapi => "openapi",
            Self::Exec => "exec",
            Self::RunRef => "run_ref",
        }
    }
}

impl std::fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Trust {
    Trusted,
    #[default]
    Untrusted,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Permissions {
    pub fs_read: Vec<String>,
    pub fs_write: Vec<String>,
    pub network: Vec<String>,
    /// Opt-in SSRF hardening for the HTTP fetch path. When true, every
    /// hop resolves ALL A/AAAA records and denies non-global IPs
    /// (loopback, private, link-local, multicast, unspecified, reserved);
    /// the connection is pinned to the validated addresses. Default
    /// false so existing wildcard deployments keep working.
    #[serde(default)]
    pub network_deny_private_ips: bool,
    pub commands: Vec<CommandPermission>,
    pub containers: ContainerPermission,
    pub side_effects: SideEffects,
    /// How far one approval reaches. `invocation` (default) authorizes a
    /// single call; `content` reuses the approval for identical content in
    /// later calls (Q1). The serde default keeps the documented
    /// manifest-parse-time default: an explicit `[permissions]` block may
    /// omit the key and still resolve to `invocation`.
    #[serde(default)]
    pub side_effects_scope: SideEffectScope,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SideEffectScope {
    /// Approval binds one invocation: a later call re-confirms even for
    /// identical content.
    #[default]
    Invocation,
    /// Approval binds the exact operation content; the same content in a
    /// later invocation reuses it.
    Content,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommandPermission {
    pub bin: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub purpose: String,
    #[serde(default)]
    pub isolation: Option<CommandIsolation>,
    #[serde(default)]
    pub image: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CommandIsolation {
    Container,
    TrustedHost,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolDef {
    pub kind: String,
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default = "default_tool_network")]
    pub network: ToolNetwork,
    #[serde(default = "default_tool_workspace")]
    pub workspace: ToolWorkspace,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_output_limit_bytes")]
    pub output_limit_bytes: usize,
    #[serde(default)]
    pub resolution: ToolResolution,
    #[serde(default)]
    pub backends: ToolBackends,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolBackends {
    #[serde(default)]
    pub host: Option<HostToolBackend>,
    #[serde(default)]
    pub bundled: Option<BundledToolBackend>,
    #[serde(default)]
    pub container: Option<ContainerToolBackend>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HostToolBackend {
    pub bin: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BundledToolBackend {
    pub bin: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContainerToolBackend {
    pub image: String,
    #[serde(default = "default_container_mount")]
    pub mount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ToolResolution {
    #[serde(default)]
    pub allowed_backends: Vec<ToolBackendKind>,
    #[serde(default)]
    pub preferred_backends: Vec<ToolBackendKind>,
    #[serde(default)]
    pub fallback: ToolFallback,
}

impl Default for ToolResolution {
    fn default() -> Self {
        Self {
            allowed_backends: Vec::new(),
            preferred_backends: Vec::new(),
            fallback: ToolFallback::Explicit,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolBackendKind {
    Host,
    Bundled,
    Container,
}

impl std::fmt::Display for ToolBackendKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Host => "host",
            Self::Bundled => "bundled",
            Self::Container => "container",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolFallback {
    #[default]
    Explicit,
    None,
}

/// Network reach for a command-backed tool.
///
/// The set is closed at `none`: no tool backend ever granted network access,
/// so a `permissioned` value could only be a declaration that silently never
/// took effect. It is refused at contract load instead of being accepted and
/// rejected again at execution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolNetwork {
    #[default]
    None,
}

fn default_tool_network() -> ToolNetwork {
    ToolNetwork::None
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolWorkspace {
    ReadOnly,
    Writable,
    None,
}

fn default_tool_workspace() -> ToolWorkspace {
    ToolWorkspace::ReadOnly
}

pub(crate) fn default_timeout_seconds() -> u64 {
    30
}

fn default_output_limit_bytes() -> usize {
    1024 * 1024
}

pub(crate) fn default_template_fuel() -> u64 {
    1_000_000
}

pub(crate) fn default_max_total_steps() -> usize {
    DEFAULT_MAX_TOTAL_STEPS
}

fn default_container_mount() -> String {
    "/work".into()
}

/// Network reach declared for a container workload.
///
/// `none` (the default) keeps the runner's fully isolated network mode.
/// `destination` names exactly one canonical host and selects the runner
/// network dedicated to it; the deployment owns that network's egress
/// rules. It is validated against the existing `permissions.network`
/// allowlist at contract load, and a wildcard entry never qualifies.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContainerNetwork {
    #[default]
    None,
    Destination {
        host: String,
    },
}

impl ContainerNetwork {
    /// The declared destination host, if any.
    pub fn destination_host(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Destination { host } => Some(host),
        }
    }
}

/// Resource bounds declared for one container workload.
///
/// Every field defaults to the hardened runner values used before bounds
/// became configurable: 256 pids, a 64 MiB noexec/nosuid `/tmp`, and no
/// memory or CPU limit. The contract owns these values; the container plan
/// only renders a validated declaration into runner argv.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContainerResources {
    /// Maximum process ids in one container. Must be positive and fit the
    /// runner's signed 32-bit `--pids-limit` flag.
    #[serde(default = "default_container_pids_limit")]
    pub pids_limit: u32,
    /// `/tmp` tmpfs size in canonical `<digits><b|k|m|g>` form (e.g. `64m`).
    #[serde(default = "default_container_tmpfs_size")]
    pub tmpfs_size: String,
    /// Memory ceiling in the same size grammar. `None` means no `--memory`.
    #[serde(default)]
    pub memory_limit: Option<String>,
    /// CPU ceiling in cores, `<digits>[.<1-3 digits>]`. `None` means no
    /// `--cpus` flag.
    #[serde(default)]
    pub cpu_limit: Option<String>,
}

pub const DEFAULT_CONTAINER_PIDS_LIMIT: u32 = 256;
pub const DEFAULT_CONTAINER_TMPFS_SIZE: &str = "64m";

fn default_container_pids_limit() -> u32 {
    DEFAULT_CONTAINER_PIDS_LIMIT
}

fn default_container_tmpfs_size() -> String {
    DEFAULT_CONTAINER_TMPFS_SIZE.to_string()
}

impl Default for ContainerResources {
    fn default() -> Self {
        Self {
            pids_limit: DEFAULT_CONTAINER_PIDS_LIMIT,
            tmpfs_size: DEFAULT_CONTAINER_TMPFS_SIZE.to_string(),
            memory_limit: None,
            cpu_limit: None,
        }
    }
}

impl ContainerResources {
    /// True when every bound equals the runner defaults. Managed-instance
    /// families have no matching runner argv, so only a default declaration
    /// is valid for them.
    pub fn is_default(&self) -> bool {
        self.pids_limit == DEFAULT_CONTAINER_PIDS_LIMIT
            && self.tmpfs_size == DEFAULT_CONTAINER_TMPFS_SIZE
            && self.memory_limit.is_none()
            && self.cpu_limit.is_none()
    }

    /// Validates every bound with the closed grammar the runner argv uses.
    /// Called at contract load and again at the gateway before any daemon
    /// call; a rejected value never reaches a runner invocation.
    pub fn validate(&self) -> Result<(), String> {
        if self.pids_limit == 0 || self.pids_limit > i32::MAX as u32 {
            return Err(format!(
                "container resources.pids_limit must be between 1 and {}",
                i32::MAX
            ));
        }
        parse_container_size("resources.tmpfs_size", &self.tmpfs_size)?;
        if let Some(memory) = &self.memory_limit {
            parse_container_size("resources.memory_limit", memory)?;
        }
        if let Some(cpu) = &self.cpu_limit {
            parse_container_cpu("resources.cpu_limit", cpu)?;
        }
        Ok(())
    }
}

/// Parses a Docker-compatible size into bytes. The grammar is deliberately
/// closed: argv must never carry a sign, whitespace, an alternate unit, or a
/// value larger than the runner's signed-byte flags accept.
fn parse_container_size(field: &str, value: &str) -> Result<u64, String> {
    let invalid =
        || format!("container {field} must be a positive size with a b/k/m/g unit (e.g. 64m)");
    let (digits, multiplier) = if let Some(rest) = value.strip_suffix('b') {
        (rest, 1_u64)
    } else if let Some(rest) = value.strip_suffix('k') {
        (rest, 1024_u64)
    } else if let Some(rest) = value.strip_suffix('m') {
        (rest, 1024_u64 * 1024)
    } else if let Some(rest) = value.strip_suffix('g') {
        (rest, 1024_u64 * 1024 * 1024)
    } else {
        return Err(invalid());
    };
    if digits.is_empty()
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid());
    }
    let count: u64 = digits
        .parse()
        .map_err(|_| format!("container {field} size overflows"))?;
    let bytes = count
        .checked_mul(multiplier)
        .ok_or_else(|| format!("container {field} size overflows"))?;
    if bytes == 0 || bytes > i64::MAX as u64 {
        return Err(format!("container {field} size is out of range"));
    }
    Ok(bytes)
}

/// Parses a Docker-compatible CPU count into millicpus. Three fractional
/// digits is the runner's precision; anything else is refused rather than
/// rounded into a different bound.
fn parse_container_cpu(field: &str, value: &str) -> Result<u64, String> {
    let invalid = || {
        format!(
            "container {field} must be a positive CPU count with at most three fractional digits (e.g. 1.5)"
        )
    };
    let (whole, fraction) = match value.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (value, None),
    };
    let fractional = if let Some(fraction) = fraction {
        if fraction.is_empty()
            || fraction.len() > 3
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(invalid());
        }
        let padded = format!("{fraction}{}", "0".repeat(3 - fraction.len()));
        padded
            .parse::<u64>()
            .map_err(|_| format!("container {field} CPU count overflows"))?
    } else {
        0
    };
    if whole.is_empty()
        || (whole.len() > 1 && whole.starts_with('0'))
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid());
    }
    let whole: u64 = whole
        .parse()
        .map_err(|_| format!("container {field} CPU count overflows"))?;
    let millis = whole
        .checked_mul(1000)
        .and_then(|millis| millis.checked_add(fractional))
        .filter(|millis| *millis != 0 && *millis <= i64::MAX as u64)
        .ok_or_else(|| format!("container {field} CPU count is out of range"))?;
    Ok(millis)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContainerPermission {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub runtime: Option<ContainerRuntime>,
    #[serde(default)]
    pub images: Vec<String>,
    #[serde(default)]
    pub on_missing: Option<String>,
    /// Destination-scoped runner network. Empty means fully isolated.
    #[serde(default)]
    pub network: ContainerNetwork,
    /// Resource bounds applied to Docker-family runner invocations.
    #[serde(default)]
    pub resources: ContainerResources,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContainerRuntime {
    Docker,
    Podman,
    DockerRunsc,
    Incus,
    Lxd,
}

impl ContainerRuntime {
    /// Stable display name shared by plans, logs, and journal audit fields.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Docker => "docker",
            Self::Podman => "podman",
            Self::DockerRunsc => "docker+runsc",
            Self::Incus => "incus",
            Self::Lxd => "lxd",
        }
    }
}

/// Validates a container image reference for the declared runtime.
///
/// Docker-compatible runtimes require a `name@sha256:` digest pin, enforced
/// by the daemon itself. Incus-like runtimes require
/// `<remote>:<path>@sha256:<fingerprint>`; execution launches by
/// fingerprint so exactly the pinned bits run.
pub fn validate_container_image(runtime: &ContainerRuntime, image: &str) -> Result<(), String> {
    match runtime {
        ContainerRuntime::Docker | ContainerRuntime::Podman | ContainerRuntime::DockerRunsc => {
            if image.contains("@sha256:") {
                Ok(())
            } else {
                Err("image must be pinned by digest (`name@sha256:<hex>`)".into())
            }
        }
        ContainerRuntime::Incus | ContainerRuntime::Lxd => match image.split_once("@sha256:") {
            Some((reference, fingerprint))
                if !reference.is_empty()
                    && !reference.contains(char::is_whitespace)
                    && fingerprint.len() == 64
                    && fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
            {
                Ok(())
            }
            _ => Err("image must have form `<remote>:<path>@sha256:<fingerprint>`".into()),
        },
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SideEffects {
    #[default]
    None,
    Confirm,
    DryRunFirst,
    Allowed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    #[serde(default)]
    pub env: Option<String>,
    #[serde(default)]
    pub file_env: Option<String>,
}

impl SecretRef {
    pub fn source_env_name(&self) -> Option<&str> {
        self.env.as_deref().or(self.file_env.as_deref())
    }

    pub fn source_label(&self) -> Option<String> {
        self.env
            .as_deref()
            .map(|name| format!("env:{name}"))
            .or_else(|| {
                self.file_env
                    .as_deref()
                    .map(|name| format!("file_env:{name}"))
            })
    }
}

/// Per-node execution retry policy. Retry counts and waits are declared by
/// the generator; the engine only executes them. No serde defaults (Q1):
/// every field is required in the manifest and omission fails closed at
/// parse time; contract-time validation still rejects out-of-range values
/// (never silently corrected at runtime). `Default` below stays for
/// programmatic construction only, never for wire compat.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Total attempts including the first. 1 means no retry.
    pub max_attempts: u32,
    /// Fixed wait between attempts in milliseconds. 0 means no wait.
    pub backoff_ms: u64,
    /// Per-attempt execution timeout in seconds. `None` means no timeout.
    pub timeout_secs: Option<u64>,
    /// What a retry may do after an indeterminate outcome (started but
    /// finished unknown: timeout, disconnect, killed process). `fail`
    /// refuses automatic replay; `repeat` re-executes the same invocation
    /// and records the acknowledged double-apply risk.
    /// Clean failures (remote-declared errors, validation) always retry
    /// within `max_attempts`; successes never retry.
    pub on_indeterminate: RetryOnIndeterminate,
}

/// Retry policy for indeterminate operation outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum RetryOnIndeterminate {
    /// Refuse automatic replay of indeterminate operations (default).
    #[default]
    Fail,
    /// Re-execute the same invocation, acknowledging possible double-apply.
    /// The choice is journaled with the attempt.
    Repeat,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff_ms: 0,
            timeout_secs: None,
            on_indeterminate: RetryOnIndeterminate::Fail,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_side_effects_scope_defaults_to_invocation() {
        // Q1: the documented manifest-parse-time default. An explicit
        // `[permissions]` block may omit the key and still resolve to
        // `invocation`; every minted ConfirmSpec then carries it explicitly.
        let parsed: Permissions = toml::from_str(
            r#"
fs_read = []
fs_write = []
network = []
commands = []
side_effects = "allowed"
[containers]
enabled = false
"#,
        )
        .expect("permissions without a scope key should parse");
        assert_eq!(parsed.side_effects_scope, SideEffectScope::Invocation);
    }

    #[test]
    fn container_image_rules_follow_the_declared_runtime() {
        let digest =
            "example/tool@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        for runtime in [
            ContainerRuntime::Docker,
            ContainerRuntime::Podman,
            ContainerRuntime::DockerRunsc,
        ] {
            assert!(validate_container_image(&runtime, digest).is_ok());
            assert!(validate_container_image(&runtime, "example/tool:latest").is_err());
        }
        let fp = "images:alpine/3.20@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        for runtime in [ContainerRuntime::Incus, ContainerRuntime::Lxd] {
            assert!(validate_container_image(&runtime, fp).is_ok());
            assert!(validate_container_image(&runtime, "images:alpine/3.20").is_err());
            assert!(validate_container_image(&runtime, digest).is_ok());
        }
    }

    #[test]
    fn container_runtime_display_names_are_stable() {
        assert_eq!(ContainerRuntime::Docker.display_name(), "docker");
        assert_eq!(ContainerRuntime::Podman.display_name(), "podman");
        assert_eq!(ContainerRuntime::DockerRunsc.display_name(), "docker+runsc");
        assert_eq!(ContainerRuntime::Incus.display_name(), "incus");
        assert_eq!(ContainerRuntime::Lxd.display_name(), "lxd");
    }
}
