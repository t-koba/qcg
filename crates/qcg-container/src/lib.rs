//! Technology-neutral container backend abstraction.
//!
//! Docker-compatible runtimes (`docker`, `podman`, `docker --runtime runsc`)
//! execute one-shot `run` commands. Incus-like runtimes (`incus`, LXD `lxc`)
//! and the legacy `lxc-*` toolset manage instances through an explicit
//! create/configure/start/enter/stop/delete lifecycle. Every family shares
//! one lifecycle contract: provisioned instances are always torn down,
//! including when the awaiting future is dropped by timeout or abort.

mod backend;
mod error;
mod plans;
mod session;

pub use backend::{Backend, backend_for, binary_available, resolve_backend};
pub use error::ContainerError;
pub use plans::{
    DockerRunSpec, LXC_CAP_DROP, LXC_MINIMAL_PATH, Mount, docker_run_argv, incus_delete_argv,
    incus_device_add_argv, incus_exec_argv, incus_init_argv, incus_pool_argv, incus_probe_argv,
    incus_secure_argv, incus_start_argv, incus_stop_argv, instance_name, lxc_config_text,
    lxc_create_argv, lxc_destroy_argv, lxc_exec_argv, lxc_probe_argv, lxc_server_argv,
    lxc_start_argv, lxc_stop_argv, map_lxc_arch, parse_lxc_image, split_pinned_image,
    validate_guest_path, validate_image_for_backend,
};
pub use session::{
    ADMIN_TIMEOUT, CREATE_TIMEOUT, InstanceId, PROBE_INTERVAL, Provision, START_TIMEOUT,
    STOP_TIMEOUT, Session, SessionGuard, await_outstanding_cleanups, provision, teardown,
    teardown_sync,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn workload() -> Vec<String> {
        vec!["echo".into(), "confirmed".into()]
    }

    #[test]
    fn backend_mapping_covers_every_declared_runtime() {
        use qcg_contract::ContainerRuntime;
        assert_eq!(
            backend_for(&ContainerRuntime::Docker),
            Backend::Docker {
                binary: "docker".into(),
                runtime_flag: None
            }
        );
        assert_eq!(
            backend_for(&ContainerRuntime::Podman),
            Backend::Docker {
                binary: "podman".into(),
                runtime_flag: None
            }
        );
        assert_eq!(
            backend_for(&ContainerRuntime::DockerRunsc),
            Backend::Docker {
                binary: "docker".into(),
                runtime_flag: Some("runsc".into())
            }
        );
        assert_eq!(
            backend_for(&ContainerRuntime::Incus),
            Backend::Incus {
                binary: "incus".into()
            }
        );
        assert_eq!(
            backend_for(&ContainerRuntime::Lxd),
            Backend::Incus {
                binary: "lxc".into()
            }
        );
        assert_eq!(backend_for(&ContainerRuntime::Lxc), Backend::Lxc);
    }

    #[test]
    fn display_names_distinguish_lxd_from_lxc() {
        use qcg_contract::ContainerRuntime;
        assert_eq!(backend_for(&ContainerRuntime::Lxd).display_name(), "lxd");
        assert_eq!(backend_for(&ContainerRuntime::Lxc).display_name(), "lxc");
        assert_eq!(
            backend_for(&ContainerRuntime::DockerRunsc).display_name(),
            "docker+runsc"
        );
    }

    #[test]
    fn docker_run_argv_matches_the_hardened_shape() {
        let cidfile = Path::new("/tmp/.qcg-container-abc.cid");
        let mounts = [Mount {
            host: Path::new("/runs/r1/workspace"),
            guest: "/work",
            readonly: false,
        }];
        let argv = docker_run_argv(&DockerRunSpec {
            binary: "docker",
            runtime_flag: None,
            cidfile,
            mounts: &mounts,
            workdir: Some("/work"),
            env_names: &[],
            image: "example/tool@sha256:abc",
            workload: &workload(),
            stdin_pipe: true,
        });
        assert_eq!(
            argv,
            vec![
                "docker",
                "run",
                "--rm",
                "-i",
                "--cidfile",
                "/tmp/.qcg-container-abc.cid",
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                "256",
                "--tmpfs",
                "/tmp:rw,noexec,nosuid,size=64m",
                "-v",
                "/runs/r1/workspace:/work:rw",
                "--workdir",
                "/work",
                "example/tool@sha256:abc",
                "echo",
                "confirmed",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn docker_run_argv_marks_readonly_mounts_and_server_env() {
        let cidfile = Path::new("/tmp/x.cid");
        let mounts = [Mount {
            host: Path::new("/runs/r1/workspace"),
            guest: "/src",
            readonly: true,
        }];
        let argv = docker_run_argv(&DockerRunSpec {
            binary: "podman",
            runtime_flag: None,
            cidfile,
            mounts: &mounts,
            workdir: None,
            env_names: &["API_TOKEN".to_string()],
            image: "example/tool@sha256:abc",
            workload: &workload(),
            stdin_pipe: true,
        });
        assert!(argv.starts_with(&["podman".to_string(), "run".to_string()]));
        assert!(argv.contains(&"/runs/r1/workspace:/src:ro".to_string()));
        assert!(argv.contains(&"--env".to_string()));
        assert!(argv.contains(&"API_TOKEN".to_string()));
        assert!(!argv.contains(&"--workdir".to_string()));
    }

    #[test]
    fn docker_runsc_adds_the_runtime_flag() {
        let argv = docker_run_argv(&DockerRunSpec {
            binary: "docker",
            runtime_flag: Some("runsc"),
            cidfile: Path::new("/tmp/x.cid"),
            mounts: &[],
            workdir: None,
            env_names: &[],
            image: "img@sha256:abc",
            workload: &workload(),
            stdin_pipe: false,
        });
        assert_eq!(&argv[0..4], &["docker", "run", "--runtime", "runsc"]);
        assert!(!argv.contains(&"-i".to_string()));
    }

    #[test]
    fn pinned_image_split_accepts_fingerprints_only() {
        let (reference, fingerprint) =
            split_pinned_image("images:alpine/3.20@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
                .expect("pinned incus image should split");
        assert_eq!(reference, "images:alpine/3.20");
        assert_eq!(
            fingerprint,
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        assert!(split_pinned_image("images:alpine/3.20").is_none());
        assert!(split_pinned_image("images:alpine/3.20@sha256:short").is_none());
        assert!(split_pinned_image("images:alpine/3.20@sha256:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_none());
    }

    #[test]
    fn lxc_image_parse_accepts_dist_release_only() {
        assert_eq!(
            parse_lxc_image("alpine:3.20"),
            Some(("alpine".into(), "3.20".into()))
        );
        assert_eq!(
            parse_lxc_image("ubuntu:jammy"),
            Some(("ubuntu".into(), "jammy".into()))
        );
        assert!(parse_lxc_image("alpine").is_none());
        assert!(parse_lxc_image("Alpine:3.20").is_none());
        assert!(parse_lxc_image("alpine:3.20@sha256:abc").is_none());
        assert!(parse_lxc_image("a/b:1").is_none());
        assert!(parse_lxc_image(":3.20").is_none());
        assert!(parse_lxc_image("alpine:").is_none());
    }

    #[test]
    fn lxc_arch_mapping_covers_common_hosts_and_rejects_unknown() {
        assert_eq!(map_lxc_arch("x86_64"), Some("amd64"));
        assert_eq!(map_lxc_arch("aarch64"), Some("arm64"));
        assert_eq!(map_lxc_arch("mips"), None);
    }

    #[test]
    fn lxc_config_enforces_network_empty_mounts_and_caps() {
        let mounts = [
            Mount {
                host: Path::new("/runs/r1/workspace"),
                guest: "/work",
                readonly: false,
            },
            Mount {
                host: Path::new("/runs/r1/workspace"),
                guest: "/src",
                readonly: true,
            },
        ];
        let config = lxc_config_text(&mounts).expect("valid mounts should render");
        assert!(config.contains("lxc.net =\n"));
        assert!(config.contains("lxc.net.0.type = empty\n"));
        assert!(!config.contains("type = none\n"));
        assert!(
            config.contains("lxc.mount.entry = /runs/r1/workspace work none bind,create=dir 0 0\n")
        );
        assert!(
            config
                .contains("lxc.mount.entry = /runs/r1/workspace src none bind,ro,create=dir 0 0\n")
        );
        assert!(config.contains("lxc.cap.drop = "));
        assert!(config.contains("sys_module"));
        assert!(config.contains("lxc.no_new_privs = 1\n"));
    }

    #[test]
    fn lxc_config_rejects_absolute_escape_and_symlink_shapes() {
        let bad = [Mount {
            host: Path::new("/runs/r1/workspace"),
            guest: "/../escape",
            readonly: true,
        }];
        assert!(lxc_config_text(&bad).is_err());
    }

    #[test]
    fn lxc_lifecycle_argv_uses_verified_flags() {
        let name = "qcg0123456789abcdef";
        assert_eq!(
            lxc_create_argv(name, Path::new("/tmp/qcg.conf"), "alpine", "3.20", "amd64"),
            vec![
                "lxc-create",
                "-n",
                name,
                "-f",
                "/tmp/qcg.conf",
                "-t",
                "download",
                "--",
                "-d",
                "alpine",
                "-r",
                "3.20",
                "-a",
                "amd64",
            ]
        );
        assert_eq!(lxc_start_argv(name), vec!["lxc-start", "-n", name, "-d"]);
        assert_eq!(
            lxc_stop_argv(name),
            vec!["lxc-stop", "-n", name, "-k", "-W"]
        );
        assert_eq!(
            lxc_destroy_argv(name),
            vec!["lxc-destroy", "-n", name, "-f"]
        );
    }

    #[test]
    fn lxc_exec_wraps_the_workdir_without_changing_the_workload() {
        let argv = lxc_exec_argv(
            "qcg0123456789abcdef",
            LXC_MINIMAL_PATH,
            "/work",
            &workload(),
        );
        assert_eq!(
            &argv[..9],
            &[
                "lxc-attach",
                "-n",
                "qcg0123456789abcdef",
                "--clear-env",
                "-v",
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "--",
                "/bin/sh",
                "-c",
            ]
        );
        assert!(argv.contains(&"echo".to_string()));
        assert!(argv.contains(&"confirmed".to_string()));
    }

    #[test]
    fn incus_lifecycle_argv_is_fully_pinned() {
        let name = "qcg0123456789abcdef";
        assert_eq!(
            incus_init_argv("incus", "default", "abc123", name),
            vec![
                "incus",
                "init",
                "--no-profiles",
                "-s",
                "default",
                "abc123",
                name
            ]
        );
        assert_eq!(
            incus_device_add_argv(
                "incus",
                name,
                "qcgwork0",
                Path::new("/runs/r1/workspace"),
                "/work",
                false
            ),
            vec![
                "incus",
                "config",
                "device",
                "add",
                name,
                "qcgwork0",
                "disk",
                "source=/runs/r1/workspace",
                "path=/work",
            ]
        );
        assert_eq!(
            incus_device_add_argv("lxc", name, "qcgwork0", Path::new("/ws"), "/src", true)[9],
            "readonly=true"
        );
        assert_eq!(
            incus_exec_argv("incus", name, Some("/work"), &[], &workload()),
            vec![
                "incus",
                "exec",
                name,
                "--cwd",
                "/work",
                "-T",
                "--",
                "echo",
                "confirmed"
            ]
        );
        assert_eq!(
            incus_stop_argv("incus", name),
            vec!["incus", "stop", "-f", name]
        );
        assert_eq!(
            incus_delete_argv("incus", name),
            vec!["incus", "delete", "-f", name]
        );
    }

    #[test]
    fn instance_names_fit_every_backend() {
        for _ in 0..100 {
            let name = instance_name();
            assert!(name.starts_with("qcg"));
            assert_eq!(name.len(), 19);
            assert!(
                name.bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()),
                "name must be alphanumeric without hyphens for LXC: {name}"
            );
        }
    }

    #[test]
    fn image_validation_mirrors_the_contract_rules() {
        use qcg_contract::ContainerRuntime;
        let digest =
            "example/tool@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(
            validate_image_for_backend(&backend_for(&ContainerRuntime::Docker), digest).is_ok()
        );
        assert!(
            validate_image_for_backend(
                &backend_for(&ContainerRuntime::Docker),
                "example/tool:latest"
            )
            .is_err()
        );
        let fp = "images:alpine/3.20@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(validate_image_for_backend(&backend_for(&ContainerRuntime::Incus), fp).is_ok());
        assert!(validate_image_for_backend(&backend_for(&ContainerRuntime::Lxd), fp).is_ok());
        assert!(
            validate_image_for_backend(
                &backend_for(&ContainerRuntime::Incus),
                "images:alpine/3.20"
            )
            .is_err()
        );
        assert!(
            validate_image_for_backend(&backend_for(&ContainerRuntime::Lxc), "alpine:3.20").is_ok()
        );
        assert!(validate_image_for_backend(&backend_for(&ContainerRuntime::Lxc), digest).is_err());
    }
}
