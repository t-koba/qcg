//! Pure command-line and configuration builders for every container
//! family. These functions perform no I/O, so unit tests pin the exact
//! argv vectors and config text without mocks or daemons.

use std::path::Path;

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
    pub cidfile: &'a Path,
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
    argv.push("--cidfile".into());
    argv.push(spec.cidfile.to_string_lossy().into_owned());
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

/// Unique instance name valid for Incus, LXD, and legacy LXC: lowercase
/// `qcg` prefix plus hex, no hyphen (LXC identifiers are alphanumeric).
pub fn instance_name() -> String {
    let hex = uuid::Uuid::now_v7().as_simple().to_string();
    format!("qcg{}", &hex[..16])
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
        Backend::Lxc => {
            if parse_lxc_image(image).is_some() {
                Ok(())
            } else {
                Err(invalid_image(
                    image,
                    backend.display_name(),
                    "image must have form `<dist>:<release>` (for example `alpine:3.20`)",
                ))
            }
        }
    }
}

/// Parses a legacy LXC download-template image reference `dist:release`.
pub fn parse_lxc_image(image: &str) -> Option<(String, String)> {
    if image.contains('@') || image.contains('/') || image.contains(char::is_whitespace) {
        return None;
    }
    let (dist, release) = image.split_once(':')?;
    if dist.is_empty()
        || release.is_empty()
        || !dist.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || byte == b'.'
                || byte == b'-'
                || byte == b'_'
        })
        || !release.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-' || byte == b'_'
        })
    {
        return None;
    }
    Some((dist.to_string(), release.to_string()))
}

/// Maps host architecture names to LXC download-template arch names.
pub fn map_lxc_arch(arch: &str) -> Option<&'static str> {
    match arch {
        "x86_64" => Some("amd64"),
        "aarch64" => Some("arm64"),
        "x86" => Some("i386"),
        "arm" => Some("armhf"),
        "riscv64" => Some("riscv64"),
        _ => None,
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

/// Minimal PATH set explicitly inside legacy LXC workloads, which start
/// with a cleared environment.
pub const LXC_MINIMAL_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Capabilities dropped from legacy LXC workloads. Every entry is a real
/// Linux capability name; an unknown entry would fail container start
/// closed instead of silently weakening isolation.
pub const LXC_CAP_DROP: &str = "sys_module sys_rawio sys_boot sys_time audit_control audit_write mac_admin mac_override syslog wake_alarm";

/// Generates a legacy LXC container configuration enforcing network
/// isolation, workspace-only bind mounts, dropped capabilities, and
/// no-new-privileges. Guests are written relative to the container rootfs.
pub fn lxc_config_text(mounts: &[Mount<'_>]) -> Result<String, ContainerError> {
    let mut text = String::from("lxc.net.0.type = none\n");
    for mount in mounts {
        validate_guest_path(mount.guest)?;
        let relative = mount.guest.trim_start_matches('/');
        let options = if mount.readonly {
            "bind,ro,create=dir"
        } else {
            "bind,create=dir"
        };
        text.push_str(&format!(
            "lxc.mount.entry = {} {} none {} 0 0\n",
            mount.host.to_string_lossy(),
            relative,
            options
        ));
    }
    text.push_str(&format!("lxc.cap.drop = {LXC_CAP_DROP}\n"));
    text.push_str("lxc.no_new_privs = 1\n");
    Ok(text)
}

pub fn lxc_create_argv(
    name: &str,
    config_path: &Path,
    dist: &str,
    release: &str,
    arch: &str,
) -> Vec<String> {
    vec![
        "lxc-create".into(),
        "-n".into(),
        name.into(),
        "-f".into(),
        config_path.to_string_lossy().into_owned(),
        "-t".into(),
        "download".into(),
        "--".into(),
        "-d".into(),
        dist.into(),
        "-r".into(),
        release.into(),
        "-a".into(),
        arch.into(),
    ]
}

pub fn lxc_start_argv(name: &str) -> Vec<String> {
    vec!["lxc-start".into(), "-n".into(), name.into(), "-d".into()]
}

pub fn lxc_probe_argv(name: &str) -> Vec<String> {
    vec![
        "lxc-attach".into(),
        "-n".into(),
        name.into(),
        "--clear-env".into(),
        "--".into(),
        "/bin/true".into(),
    ]
}

/// One-shot workload argv. `lxc-attach` has no `--cwd` flag, so the workload
/// runs through `/bin/sh` which changes to the workdir and `exec`s the real
/// command, preserving PID, stdio, signals, and exit status.
pub fn lxc_exec_argv(
    name: &str,
    path_env: &str,
    workdir: &str,
    workload: &[String],
) -> Vec<String> {
    let mut argv = vec![
        "lxc-attach".into(),
        "-n".into(),
        name.into(),
        "--clear-env".into(),
        "-v".into(),
        format!("PATH={path_env}"),
        "--".into(),
        "/bin/sh".into(),
        "-c".into(),
        "cd \"$1\" && shift && exec \"$@\"".into(),
        "qcg-sh".into(),
        workdir.into(),
    ];
    argv.extend(workload.iter().cloned());
    argv
}

/// Long-lived server spawn argv without the workdir wrapper: servers do not
/// depend on a working directory, so the extra shell is omitted.
pub fn lxc_server_argv(
    name: &str,
    path_env: &str,
    env: &[(String, String)],
    workload: &[String],
) -> Vec<String> {
    let mut argv = vec![
        "lxc-attach".into(),
        "-n".into(),
        name.into(),
        "--clear-env".into(),
        "-v".into(),
        format!("PATH={path_env}"),
    ];
    for (key, value) in env {
        argv.push("-v".into());
        argv.push(format!("{key}={value}"));
    }
    argv.push("--".into());
    argv.extend(workload.iter().cloned());
    argv
}

pub fn lxc_stop_argv(name: &str) -> Vec<String> {
    vec![
        "lxc-stop".into(),
        "-n".into(),
        name.into(),
        "-k".into(),
        "-W".into(),
    ]
}

pub fn lxc_destroy_argv(name: &str) -> Vec<String> {
    vec!["lxc-destroy".into(), "-n".into(), name.into(), "-f".into()]
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
) -> Vec<String> {
    vec![
        binary.into(),
        "init".into(),
        "--no-profiles".into(),
        "-s".into(),
        pool.into(),
        image_fingerprint.into(),
        name.into(),
    ]
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
