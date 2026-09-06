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

pub(crate) fn mcp_container_runtime_command(
    runtime: &McpContainerRuntime,
) -> Option<(&'static str, Vec<&'static str>)> {
    let path = std::env::var_os("PATH")?;
    let (binary, args) = match runtime {
        McpContainerRuntime::Docker => ("docker", vec!["run"]),
        McpContainerRuntime::Podman => ("podman", vec!["run"]),
        McpContainerRuntime::DockerRunsc => ("docker", vec!["run", "--runtime", "runsc"]),
    };
    std::env::split_paths(&path)
        .any(|directory| directory.join(binary).is_file())
        .then_some((binary, args))
}
