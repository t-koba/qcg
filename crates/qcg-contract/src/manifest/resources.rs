use qcg_policy::DEFAULT_MAX_TOTAL_STEPS;
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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    File,
    Dir,
    Skill,
    Url,
    Openapi,
    Exec,
}

impl ResourceKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::File => "file",
            Self::Dir => "dir",
            Self::Skill => "skill",
            Self::Url => "url",
            Self::Openapi => "openapi",
            Self::Exec => "exec",
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
    #[serde(default)]
    pub fs_read: Vec<String>,
    #[serde(default)]
    pub fs_write: Vec<String>,
    #[serde(default)]
    pub network: Vec<String>,
    #[serde(default)]
    pub commands: Vec<CommandPermission>,
    #[serde(default)]
    pub containers: ContainerPermission,
    #[serde(default)]
    pub side_effects: SideEffects,
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
    #[serde(default)]
    pub version_command: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolNetwork {
    None,
    Permissioned,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContainerRuntime {
    Docker,
    Podman,
    DockerRunsc,
    Incus,
    Lxd,
    Lxc,
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
            Self::Lxc => "lxc",
        }
    }
}

/// Validates a container image reference for the declared runtime.
///
/// Docker-compatible runtimes require a `name@sha256:` digest pin, enforced
/// by the daemon itself. Incus-like runtimes require
/// `<remote>:<path>@sha256:<fingerprint>`; execution launches by
/// fingerprint so exactly the pinned bits run. Legacy LXC addresses
/// `dist:release` series verified through the signed download index, which
/// is weaker than a digest pin and documented as such.
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
        ContainerRuntime::Lxc => match image.split_once(':') {
            Some((dist, release))
                if !dist.is_empty()
                    && !release.is_empty()
                    && !image.contains('@')
                    && !image.contains('/')
                    && dist.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'.' | b'-' | b'_')
                    })
                    && release.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_')
                    }) =>
            {
                Ok(())
            }
            _ => Err("image must have form `<dist>:<release>` (for example `alpine:3.20`)".into()),
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
/// the generator; the engine only executes them.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Total attempts including the first. Defaults to 1 (no retry).
    #[serde(default = "default_retry_attempts")]
    pub max_attempts: u32,
    /// Fixed wait between attempts in milliseconds. Defaults to 0.
    #[serde(default)]
    pub backoff_ms: u64,
    /// Per-attempt execution timeout in seconds. Omitted means no timeout.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_retry_attempts(),
            backoff_ms: 0,
            timeout_secs: None,
        }
    }
}

fn default_retry_attempts() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(validate_container_image(&ContainerRuntime::Lxc, "alpine:3.20").is_ok());
        assert!(validate_container_image(&ContainerRuntime::Lxc, "ubuntu:jammy").is_ok());
        assert!(validate_container_image(&ContainerRuntime::Lxc, digest).is_err());
        assert!(validate_container_image(&ContainerRuntime::Lxc, "Alpine:3.20").is_err());
        assert!(validate_container_image(&ContainerRuntime::Lxc, "alpine").is_err());
    }

    #[test]
    fn container_runtime_display_names_are_stable() {
        assert_eq!(ContainerRuntime::Docker.display_name(), "docker");
        assert_eq!(ContainerRuntime::Podman.display_name(), "podman");
        assert_eq!(ContainerRuntime::DockerRunsc.display_name(), "docker+runsc");
        assert_eq!(ContainerRuntime::Incus.display_name(), "incus");
        assert_eq!(ContainerRuntime::Lxd.display_name(), "lxd");
        assert_eq!(ContainerRuntime::Lxc.display_name(), "lxc");
    }
}
