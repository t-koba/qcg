use qcg_contract::ContainerRuntime;

/// Resolved container backend: a concrete client binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    Docker {
        binary: String,
        runtime_flag: Option<String>,
    },
    Incus {
        binary: String,
    },
    Lxc,
}

impl Backend {
    /// Stable display name for logs, plans, and journal audit fields.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Docker {
                binary,
                runtime_flag,
            } => {
                if runtime_flag.is_some() {
                    "docker+runsc"
                } else if binary == "podman" {
                    "podman"
                } else {
                    "docker"
                }
            }
            Self::Incus { binary } => {
                if binary == "incus" {
                    "incus"
                } else {
                    "lxd"
                }
            }
            Self::Lxc => "lxc",
        }
    }
}

/// Pure mapping from declared runtime to backend descriptor. No filesystem
/// or environment access, so unit tests cover every runtime.
pub fn backend_for(runtime: &ContainerRuntime) -> Backend {
    match runtime {
        ContainerRuntime::Docker => Backend::Docker {
            binary: "docker".into(),
            runtime_flag: None,
        },
        ContainerRuntime::Podman => Backend::Docker {
            binary: "podman".into(),
            runtime_flag: None,
        },
        ContainerRuntime::DockerRunsc => Backend::Docker {
            binary: "docker".into(),
            runtime_flag: Some("runsc".into()),
        },
        ContainerRuntime::Incus => Backend::Incus {
            binary: "incus".into(),
        },
        ContainerRuntime::Lxd => Backend::Incus {
            binary: "lxc".into(),
        },
        ContainerRuntime::Lxc => Backend::Lxc,
    }
}

/// Binaries that must exist for each backend. The legacy toolset is
/// reported as a single `lxc-create` probe; the remaining tools ship
/// together and their absence surfaces as an explicit stage failure.
fn required_binaries(backend: &Backend) -> Vec<&'static str> {
    match backend {
        Backend::Docker { binary, .. } => {
            if binary == "podman" {
                vec!["podman"]
            } else {
                vec!["docker"]
            }
        }
        Backend::Incus { binary } => {
            if binary == "incus" {
                vec!["incus"]
            } else {
                vec!["lxc"]
            }
        }
        Backend::Lxc => vec!["lxc-create"],
    }
}

pub fn binary_available(binary: &str) -> bool {
    if binary.contains('/') || binary.contains('\\') {
        return std::path::Path::new(binary).is_file();
    }
    std::env::var_os("PATH")
        .is_some_and(|paths| std::env::split_paths(&paths).any(|dir| dir.join(binary).is_file()))
}

/// Resolves the declared runtime to a backend whose client exists.
/// Returns `None` when the client is missing so callers keep the
/// contract `on_missing` skip/error semantics. Never substitutes a
/// different technology silently.
pub fn resolve_backend(runtime: &ContainerRuntime) -> Option<Backend> {
    let backend = backend_for(runtime);
    required_binaries(&backend)
        .iter()
        .all(|binary| binary_available(binary))
        .then_some(backend)
}
