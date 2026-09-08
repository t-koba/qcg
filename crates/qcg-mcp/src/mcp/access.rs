use std::collections::BTreeSet;

use super::error::McpError;
use super::profile::McpProfile;
use super::transport::McpTransport;

#[derive(Debug, Clone)]
pub struct McpAccess {
    pub network_hosts: BTreeSet<String>,
    pub commands: Vec<McpCommandAccess>,
    pub workspace: std::path::PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpCommandAccess {
    pub argv: Vec<String>,
    pub isolation: McpCommandIsolation,
    pub image: Option<String>,
    pub runtime: Option<McpContainerRuntime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpCommandIsolation {
    Container,
    TrustedHost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpContainerRuntime {
    Docker,
    Podman,
    DockerRunsc,
    Incus,
    Lxd,
    Lxc,
}

impl McpContainerRuntime {
    /// Maps to the shared backend descriptor. Availability is probed
    /// separately so missing clients keep the existing skip/error behavior.
    pub fn backend(&self) -> qcg_container::Backend {
        use qcg_container::Backend;
        match self {
            Self::Docker => Backend::Docker {
                binary: "docker".into(),
                runtime_flag: None,
            },
            Self::Podman => Backend::Docker {
                binary: "podman".into(),
                runtime_flag: None,
            },
            Self::DockerRunsc => Backend::Docker {
                binary: "docker".into(),
                runtime_flag: Some("runsc".into()),
            },
            Self::Incus => Backend::Incus {
                binary: "incus".into(),
            },
            Self::Lxd => Backend::Incus {
                binary: "lxc".into(),
            },
            Self::Lxc => Backend::Lxc,
        }
    }
}

impl McpCommandAccess {
    pub fn trusted_host(argv: Vec<String>) -> Self {
        Self {
            argv,
            isolation: McpCommandIsolation::TrustedHost,
            image: None,
            runtime: None,
        }
    }
}

impl McpAccess {
    pub(crate) fn validate(&self, profile: &McpProfile) -> Result<(), McpError> {
        match profile.transport() {
            McpTransport::StreamableHttp => {
                for host in profile.allowed_hosts() {
                    if !self.network_hosts.contains(host) {
                        return Err(McpError::Configuration(format!(
                            "MCP server `{}` requires permissions.network entry `{host}`",
                            profile.id()
                        )));
                    }
                }
            }
            McpTransport::Stdio => {
                if !self
                    .commands
                    .iter()
                    .any(|allowed| allowed.argv == profile.command())
                {
                    return Err(McpError::Configuration(format!(
                        "MCP server `{}` command is not allowed by permissions.commands",
                        profile.id()
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Resolves a declared MCP container runtime to an available backend.
/// `None` preserves the existing missing-client behavior; no technology is
/// ever substituted silently.
pub(crate) fn mcp_container_backend(
    runtime: &McpContainerRuntime,
) -> Option<qcg_container::Backend> {
    let backend = runtime.backend();
    let binaries = match &backend {
        qcg_container::Backend::Docker { binary, .. } => vec![binary.as_str()],
        qcg_container::Backend::Incus { binary } => vec![binary.as_str()],
        qcg_container::Backend::Lxc => vec!["lxc-create"],
    };
    binaries
        .iter()
        .all(|binary| qcg_container::binary_available(binary))
        .then_some(backend)
}
