//! Pure command-line and configuration builders for every container
//! family. These functions perform no I/O, so unit tests pin the exact
//! argv vectors and config text without mocks or daemons.

use std::path::Path;

use super::backend::Backend;
use super::error::ContainerError;

/// Host-to-guest bind mount shared by all families.
#[derive(Debug, Clone)]
pub struct Mount<'a> {
    pub host: &'a Path,
    pub guest: &'a str,
    pub readonly: bool,
}

/// Inputs for [`docker_run_argv`], bundled so the builder signature stays
/// reviewable as options grow.
#[derive(Debug, Clone)]
pub struct DockerRunSpec<'a> {
    pub binary: &'a str,
    pub runtime_flag: Option<&'a str>,
    /// Unique effect name (see [`effect_name`]) and its owner labels.
    pub name: &'a str,
    pub labels: &'a [(String, String)],
    pub mounts: &'a [Mount<'a>],
    pub workdir: Option<&'a str>,
    pub env_names: &'a [String],
    pub image: &'a str,
    pub workload: &'a [String],
    pub stdin_pipe: bool,
}

/// Builds `docker run` argv in one canonical order. The `-v` triple form is
/// exactly equivalent to `--mount type=bind,src,dst` and keeps a single code
/// path for commands, tool backends, and long-lived MCP servers.
pub fn docker_run_argv(spec: &DockerRunSpec<'_>) -> Vec<String> {
    let binary = spec.binary;
    let mut argv = vec![binary.to_string(), "run".to_string()];
    if let Some(flag) = spec.runtime_flag {
        argv.push("--runtime".into());
        argv.push(flag.to_string());
    }
    argv.push("--rm".into());
    if spec.stdin_pipe {
        argv.push("-i".into());
    }
    argv.extend(["--name".into(), spec.name.to_string()]);
    for (key, value) in spec.labels {
        argv.extend(["--label".into(), format!("{key}={value}")]);
    }
    argv.extend([
        "--network".into(),
        "none".into(),
        "--read-only".into(),
        "--cap-drop".into(),
        "ALL".into(),
        "--security-opt".into(),
        "no-new-privileges".into(),
        "--pids-limit".into(),
        "256".into(),
        "--tmpfs".into(),
        "/tmp:rw,noexec,nosuid,size=64m".into(),
    ]);
    for mount in spec.mounts {
        argv.push("-v".into());
        argv.push(format!(
            "{}:{}:{}",
            mount.host.to_string_lossy(),
            mount.guest,
            if mount.readonly { "ro" } else { "rw" }
        ));
    }
    if let Some(workdir) = spec.workdir {
        argv.push("--workdir".into());
        argv.push(workdir.to_string());
    }
    for name in spec.env_names {
        argv.push("--env".into());
        argv.push(name.clone());
    }
    argv.push(spec.image.to_string());
    argv.extend(spec.workload.iter().cloned());
    argv
}

/// Unique effect name valid for every backend: `qcg-` plus 128 bits from the
/// OS RNG as 32 lowercase hex digits. The token is the proof of ownership:
/// an instance with this exact name exists only because we created it.
pub fn effect_name() -> Result<String, ContainerError> {
    let mut token = [0_u8; 16];
    getrandom::fill(&mut token).map_err(|error| ContainerError::StageFailed {
        stage: "name",
        instance: String::new(),
        detail: format!("OS randomness unavailable: {error}"),
    })?;
    Ok(format!(
        "qcg-{}",
        token.map(|byte| format!("{byte:02x}")).concat()
    ))
}

/// Whether `name` has the exact shape [`effect_name`] produces.
pub fn is_effect_name(name: &str) -> bool {
    name.strip_prefix("qcg-").is_some_and(|token| {
        token.len() == 32
            && token
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// Stop-then-delete argv (with stage names) for one named instance.
pub fn teardown_argvs(backend: &Backend, name: &str) -> [(Vec<String>, &'static str); 2] {
    match backend {
        Backend::Docker { binary, .. } => [
            (vec![binary.clone(), "kill".into(), name.into()], "kill"),
            (
                vec![binary.clone(), "rm".into(), "-f".into(), name.into()],
                "rm",
            ),
        ],
        Backend::Incus { binary } => [
            (incus_stop_argv(binary, name), "stop"),
            (incus_delete_argv(binary, name), "delete"),
        ],
    }
}

/// Lists instances whose name matches `name`; the caller compares exactly.
pub fn presence_argv(backend: &Backend, name: &str) -> Vec<String> {
    let strings = |parts: &[&str]| {
        parts
            .iter()
            .map(|part| part.to_string())
            .collect::<Vec<_>>()
    };
    match backend {
        Backend::Docker { binary, .. } => {
            let mut argv = vec![binary.clone()];
            argv.extend(strings(&["ps", "-a", "--filter"]));
            argv.push(format!("name={name}"));
            argv.extend(strings(&["--format", "{{.Names}}"]));
            argv
        }
        Backend::Incus { binary } => {
            let mut argv = vec![binary.clone(), "list".into(), name.into()];
            argv.extend(strings(&["--format", "csv", "-c", "n"]));
            argv
        }
    }
}

/// Splits a pinned `<ref>@sha256:<fingerprint>` image reference. Docker
/// passes the whole string through; Incus/LXD launch by fingerprint.
pub fn split_pinned_image(image: &str) -> Option<(&str, &str)> {
    let (reference, fingerprint) = image.split_once("@sha256:")?;
    if reference.is_empty() || reference.contains(char::is_whitespace) {
        return None;
    }
    if fingerprint.len() != 64 || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some((reference, fingerprint))
}

fn invalid_image(image: &str, backend: &str, reason: &str) -> ContainerError {
    ContainerError::InvalidImage {
        image: image.to_string(),
        backend: backend.to_string(),
        reason: reason.to_string(),
    }
}

/// Defense-in-depth image check mirroring the contract rules, applied again
/// immediately before any daemon operation.
pub fn validate_image_for_backend(
    backend: &super::backend::Backend,
    image: &str,
) -> Result<(), ContainerError> {
    use super::backend::Backend;
    match backend {
        Backend::Docker { .. } => {
            if image.contains("@sha256:") {
                Ok(())
            } else {
                Err(invalid_image(
                    image,
                    backend.display_name(),
                    "image must be pinned by digest (`name@sha256:<hex>`)",
                ))
            }
        }
        Backend::Incus { .. } => {
            if split_pinned_image(image).is_some() {
                Ok(())
            } else {
                Err(invalid_image(
                    image,
                    backend.display_name(),
                    "image must have form `<remote>:<path>@sha256:<fingerprint>`",
                ))
            }
        }
    }
}

/// Validates an absolute guest mount destination shared by managed families.
pub fn validate_guest_path(guest: &str) -> Result<(), ContainerError> {
    if !guest.starts_with('/') || guest.contains('\0') || guest.split('/').any(|part| part == "..")
    {
        return Err(ContainerError::InvalidImage {
            image: guest.to_string(),
            backend: "mount".to_string(),
            reason: "guest mount destination must be absolute without `..`".to_string(),
        });
    }
    Ok(())
}

/// Storage pool discovery following the daemon's own default profile.
pub fn incus_pool_argv(binary: &str) -> Vec<String> {
    vec![
        binary.into(),
        "profile".into(),
        "device".into(),
        "get".into(),
        "default".into(),
        "root".into(),
        "pool".into(),
    ]
}

pub fn incus_init_argv(
    binary: &str,
    pool: &str,
    image_fingerprint: &str,
    name: &str,
    labels: &[(String, String)],
) -> Vec<String> {
    let mut argv = vec![binary.into(), "init".into(), "--no-profiles".into()];
    for (key, value) in labels {
        argv.extend(["-c".into(), format!("user.{key}={value}")]);
    }
    argv.extend([
        "-s".into(),
        pool.into(),
        image_fingerprint.into(),
        name.into(),
    ]);
    argv
}

pub fn incus_device_add_argv(
    binary: &str,
    name: &str,
    device: &str,
    host: &Path,
    guest: &str,
    readonly: bool,
) -> Vec<String> {
    let mut argv = vec![
        binary.into(),
        "config".into(),
        "device".into(),
        "add".into(),
        name.into(),
        device.into(),
        "disk".into(),
        format!("source={}", host.to_string_lossy()),
        format!("path={guest}"),
    ];
    if readonly {
        argv.push("readonly=true".into());
    }
    argv
}

pub fn incus_secure_argv(binary: &str, name: &str) -> Vec<String> {
    vec![
        binary.into(),
        "config".into(),
        "set".into(),
        name.into(),
        "security.nesting=false".into(),
        "security.privileged=false".into(),
    ]
}

pub fn incus_start_argv(binary: &str, name: &str) -> Vec<String> {
    vec![binary.into(), "start".into(), name.into()]
}

pub fn incus_probe_argv(binary: &str, name: &str) -> Vec<String> {
    vec![
        binary.into(),
        "exec".into(),
        name.into(),
        "-T".into(),
        "--".into(),
        "true".into(),
    ]
}

/// One-shot workload argv with explicit workdir and non-interactive mode.
pub fn incus_exec_argv(
    binary: &str,
    name: &str,
    workdir: Option<&str>,
    env: &[(String, String)],
    workload: &[String],
) -> Vec<String> {
    let mut argv = vec![binary.into(), "exec".into(), name.into()];
    if let Some(workdir) = workdir {
        argv.push("--cwd".into());
        argv.push(workdir.to_string());
    }
    argv.push("-T".into());
    for (key, value) in env {
        argv.push("--env".into());
        argv.push(format!("{key}={value}"));
    }
    argv.push("--".into());
    argv.extend(workload.iter().cloned());
    argv
}

pub fn incus_stop_argv(binary: &str, name: &str) -> Vec<String> {
    vec![binary.into(), "stop".into(), "-f".into(), name.into()]
}

pub fn incus_delete_argv(binary: &str, name: &str) -> Vec<String> {
    vec![binary.into(), "delete".into(), "-f".into(), name.into()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effect_names_are_distinct_valid_and_recognized() {
        let names: Vec<String> = (0..64).map(|_| effect_name().unwrap()).collect();
        for name in &names {
            assert_eq!(name.len(), 36, "{name}");
            assert!(is_effect_name(name), "{name}");
        }
        let unique: std::collections::BTreeSet<&String> = names.iter().collect();
        assert_eq!(unique.len(), names.len(), "names must be distinct");
        for foreign in [
            "qcg-",
            "qcg-ABCDEF0123456789abcdef0123456789",
            "ctr0123",
            "prod-db",
        ] {
            assert!(!is_effect_name(foreign), "{foreign}");
        }
    }
}
