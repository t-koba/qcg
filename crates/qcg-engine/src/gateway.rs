mod command;
mod error;
mod fs;
mod http;
mod process;

pub use command::*;
pub use error::*;
pub use fs::*;
pub use http::*;
#[cfg(test)]
pub(crate) use process::*;

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use qcg_contract::{CommandIsolation, CommandPermission, Permissions};
    use std::collections::BTreeMap;
    #[cfg(unix)]
    use std::time::Duration;
    #[cfg(unix)]
    use tokio_util::sync::CancellationToken;

    fn temp_workspace() -> Utf8PathBuf {
        let path = Utf8PathBuf::from_path_buf(std::env::temp_dir().join("qcg-gateway-test"))
            .expect("temporary directory path must be utf-8");
        std::fs::create_dir_all(&path).expect("test workspace should be created");
        path
    }

    #[test]
    fn denies_workspace_write_without_permission() {
        let mut permissions = Permissions::default();
        permissions.fs_write.clear();
        let gateway = FsGateway::new(temp_workspace(), &permissions);
        assert!(matches!(
            gateway.resolve_write("out.txt"),
            Err(GatewayError::FsWriteDenied)
        ));
    }

    #[test]
    fn denies_path_escape_even_with_workspace_permission() {
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(temp_workspace(), &permissions);
        assert!(matches!(
            gateway.resolve_write("../out.txt"),
            Err(GatewayError::PathDenied { .. })
        ));
    }

    #[test]
    fn denies_read_without_read_permission() {
        let mut permissions = Permissions::default();
        permissions.fs_read.clear();
        let gateway = FsGateway::new(temp_workspace(), &permissions);
        assert!(matches!(
            gateway.resolve_read("out.txt"),
            Err(GatewayError::FsReadDenied)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn denies_symlink_read_escape() {
        let workspace = temp_workspace();
        let outside = workspace
            .parent()
            .expect("test workspace has parent")
            .join("qcg-gateway-outside.txt");
        std::fs::write(&outside, "outside").expect("outside file should be written");
        let link = workspace.join("outside-link");
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&outside, &link).expect("symlink should be created");
        let mut permissions = Permissions::default();
        permissions.fs_read.push("workspace".into());
        let gateway = FsGateway::new(workspace, &permissions);
        assert!(matches!(
            gateway.resolve_read("outside-link"),
            Err(GatewayError::PathDenied { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn denies_symlink_write_escape() {
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-gateway-write-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("test workspace should be created");
        let outside = base.join("outside.txt");
        std::fs::write(&outside, "outside").expect("outside file should be written");
        let link = workspace.join("out.txt");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(workspace, &permissions);
        assert!(matches!(
            gateway.resolve_write("out.txt"),
            Err(GatewayError::PathDenied { .. })
        ));
        assert_eq!(
            std::fs::read_to_string(&outside).expect("outside file should be readable"),
            "outside"
        );
        std::fs::remove_dir_all(base).expect("test workspace should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn denies_parent_symlink_write_without_external_side_effect() {
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-gateway-parent-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("test workspace should be created");
        let external = base.join("external");
        std::fs::create_dir_all(&external).expect("external dir should be created");
        let link = workspace.join("linked");
        std::os::unix::fs::symlink(&external, &link).expect("symlink should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(workspace, &permissions);
        assert!(matches!(
            gateway.resolve_write("linked/newdir/out.txt"),
            Err(GatewayError::PathDenied { .. })
        ));
        assert!(
            !external.join("newdir").exists(),
            "rejected write must not create directories outside the workspace"
        );
        std::fs::remove_dir_all(base).expect("test workspace should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn atomic_write_replaces_terminal_symlink_target_safely() {
        let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-gateway-atomic-{}",
            uuid::Uuid::now_v7().as_simple()
        )))
        .expect("temporary directory path must be utf-8");
        let workspace = base.join("workspace");
        std::fs::create_dir_all(&workspace).expect("test workspace should be created");
        let mut permissions = Permissions::default();
        permissions.fs_write.push("workspace".into());
        let gateway = FsGateway::new(workspace.clone(), &permissions);
        let resolved = gateway
            .resolve_write("nested/out.txt")
            .expect("nested write should resolve");
        gateway
            .write_file_atomic(&resolved, b"hello")
            .await
            .expect("atomic write should succeed");
        assert_eq!(
            std::fs::read_to_string(&resolved).expect("written file should be readable"),
            "hello"
        );
        // A terminal symlink must be denied even when the atomic path is used.
        let outside = base.join("outside.txt");
        std::fs::write(&outside, "outside").expect("outside file should be written");
        let link = workspace.join("linked.txt");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink should be created");
        assert!(matches!(
            gateway.resolve_write("linked.txt"),
            Err(GatewayError::PathDenied { .. })
        ));
        assert_eq!(
            std::fs::read_to_string(&outside).expect("outside file should be readable"),
            "outside"
        );
        std::fs::remove_dir_all(base).expect("test workspace should be removed");
    }

    #[test]
    fn permits_declared_command_shape() {
        let permission = CommandPermission {
            bin: "cc".into(),
            args: vec!["-o".into(), "*".into(), "*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        };
        let args = vec!["-o".into(), "hello".into(), "main.c".into()];
        assert!(args_allowed(&permission, &args));
        let denied = vec!["-shared".into(), "main.c".into()];
        assert!(!args_allowed(&permission, &denied));
    }

    #[test]
    fn command_plan_uses_the_same_runtime_limits_as_execution() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "date".into(),
            args: vec![],
            purpose: "show time".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let plan = CmdGateway::new(permissions, temp_workspace())
            .with_bounds(CommandBounds {
                timeout_seconds: 17,
                input_limit_bytes: None,
                output_limit_bytes: Some(4096),
            })
            .command_plan(&["date".into()])
            .expect("declared command should have a plan");
        assert_eq!(plan["timeout_seconds"], 17);
        assert_eq!(plan["output_limit_bytes"], 4096);
    }

    #[tokio::test]
    async fn workspace_relative_command_resolves_against_the_declared_workspace() {
        let workspace = temp_workspace().join(format!(
            "relative-command-{}",
            uuid::Uuid::now_v7().as_simple()
        ));
        std::fs::create_dir_all(&workspace).expect("test workspace should be created");
        let executable_name = if cfg!(windows) {
            "local-command.exe"
        } else {
            "local-command"
        };
        let executable = workspace.join(executable_name);
        std::fs::copy(
            std::env::current_exe().expect("current test executable should resolve"),
            &executable,
        )
        .expect("test executable should be copied into the workspace");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))
                .expect("test executable should be executable");
        }
        let bin = format!("./{executable_name}");
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: bin.clone(),
            args: vec!["--list".into()],
            purpose: "verify workspace-relative execution".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });

        let output = CmdGateway::new(permissions, workspace.clone())
            .run_with_limits(&[bin, "--list".into()], 30, Some(1024 * 1024))
            .await
            .expect("workspace-relative executable should run");

        assert_eq!(output.status, 0);
        assert!(output.stdout.contains("workspace_relative_command"));
        std::fs::remove_dir_all(workspace).expect("test workspace should be removed");
    }

    #[test]
    fn workspace_relative_command_rejects_parent_traversal() {
        let error = resolve_command_program(&temp_workspace(), "./../outside")
            .expect_err("parent traversal must be rejected before filesystem resolution");
        assert!(matches!(error, GatewayError::CommandPathDenied { .. }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_stops_a_running_command() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "sh".into(),
            args: vec!["-c".into(), "sleep 30".into()],
            purpose: "cancellation test".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let cancellation = CancellationToken::new();
        let gateway =
            CmdGateway::new(permissions, temp_workspace()).with_cancellation(cancellation.clone());
        let task = tokio::spawn(async move {
            gateway
                .run(&["sh".into(), "-c".into(), "sleep 30".into()])
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("command cancellation should not wait for the timeout")
            .expect("command task should join");
        assert!(matches!(result, Err(GatewayError::Canceled)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_output_limit_stops_stdout_overflow_during_read() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let result = gateway
            .run_trusted_process(
                &["sh".into(), "-c".into(), "head -c 4097 /dev/zero".into()],
                5,
                Some(4096),
            )
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandOutputTooLarge { bin }) if bin == "sh"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_output_limit_stops_stderr_overflow_during_read() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let result = gateway
            .run_trusted_process(
                &[
                    "sh".into(),
                    "-c".into(),
                    "head -c 4097 /dev/zero >&2".into(),
                ],
                5,
                Some(4096),
            )
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandOutputTooLarge { bin }) if bin == "sh"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_output_limit_is_shared_between_stdout_and_stderr() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let result = gateway
            .run_trusted_process(
                &[
                    "sh".into(),
                    "-c".into(),
                    "head -c 3000 /dev/zero; head -c 3000 /dev/zero >&2".into(),
                ],
                5,
                Some(4096),
            )
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandOutputTooLarge { bin }) if bin == "sh"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_output_limit_is_observed_after_one_stream_eof() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let started = tokio::time::Instant::now();
        let result = gateway
            .run_trusted_process(
                &[
                    "sh".into(),
                    "-c".into(),
                    "printf done; exec 1>&-; head -c 8192 /dev/zero >&2".into(),
                ],
                5,
                Some(4096),
            )
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandOutputTooLarge { bin }) if bin == "sh"
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn command_stdin_limit_is_separate_from_output_limit() {
        let gateway =
            CmdGateway::new(Permissions::default(), temp_workspace()).with_bounds(CommandBounds {
                timeout_seconds: 30,
                input_limit_bytes: Some(4),
                output_limit_bytes: Some(4096),
            });
        let result = gateway
            .run_trusted_process_with_stdin(&["cat".into()], 5, Some(4096), Some(b"12345"))
            .await;
        assert!(matches!(
            result,
            Err(GatewayError::CommandInputTooLarge { bin }) if bin == "cat"
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_environment_contains_only_explicitly_forwarded_values() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let output = gateway
            .run_trusted_process(&["env".into()], 5, Some(4096))
            .await
            .expect("environment inspection should run");
        assert_eq!(output.status, 0);

        let environment = output
            .stdout
            .lines()
            .filter_map(|line| line.split_once('=').map(|(name, _)| name))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            environment,
            std::collections::BTreeSet::from(["PATH", "TMPDIR"]),
            "child processes must not inherit provider credentials or other parent state"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_kills_the_command_process_group() {
        let workspace = temp_workspace().join(format!("process-group-{}", std::process::id()));
        std::fs::create_dir_all(&workspace).expect("test workspace should be created");
        let script = "sleep 30 & child=$!; printf '%s' \"$child\" > child.pid; wait";
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "sh".into(),
            args: vec!["-c".into(), script.into()],
            purpose: "process group cancellation test".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let cancellation = CancellationToken::new();
        let gateway =
            CmdGateway::new(permissions, workspace.clone()).with_cancellation(cancellation.clone());
        let task = tokio::spawn(async move {
            gateway
                .run(&["sh".into(), "-c".into(), script.into()])
                .await
        });
        let child_pid = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(source) = tokio::fs::read_to_string(workspace.join("child.pid")).await
                    && let Ok(pid) = source.parse::<i32>()
                {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("grandchild pid should be recorded");
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("process group cancellation should be prompt")
            .expect("command task should join");
        assert!(matches!(result, Err(GatewayError::Canceled)));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                // SAFETY: signal 0 only probes whether the recorded process still exists.
                let exists = unsafe { libc::kill(child_pid, 0) } == 0
                    || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
                if !exists {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("grandchild process should be reaped");
    }

    #[test]
    fn wildcard_command_args_reject_path_escape_shapes() {
        let permission = CommandPermission {
            bin: "test".into(),
            args: vec!["-f".into(), "*".into()],
            purpose: "probe file".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        };
        for denied in [
            "../secret",
            "/tmp/secret",
            r"..\secret",
            "ok/../secret",
            "bad\0arg",
        ] {
            assert!(
                !args_allowed(&permission, &["-f".into(), denied.into()]),
                "wildcard should reject {denied:?}"
            );
        }
        assert!(args_allowed(
            &permission,
            &["-f".into(), "configs/qpx.yaml".into()]
        ));
    }

    #[test]
    fn suffix_wildcard_command_args_reject_path_escape_shapes() {
        let permission = CommandPermission {
            bin: "cc".into(),
            args: vec!["*.c".into()],
            purpose: "compile source".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        };
        assert!(args_allowed(&permission, &["main.c".into()]));
        assert!(args_allowed(&permission, &["src/main.c".into()]));
        assert!(!args_allowed(&permission, &["../main.c".into()]));
        assert!(!args_allowed(&permission, &[r"..\main.c".into()]));
    }

    #[test]
    fn command_denial_reports_allowed_declarations() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "cc".into(),
            args: vec!["-o".into(), "*".into(), "*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        let denied = gateway
            .command_plan(&["cc".into(), "-shared".into(), "main.c".into()])
            .expect_err("undeclared command arguments should be denied");
        match denied {
            GatewayError::CommandArgsDenied {
                bin,
                actual,
                allowed,
            } => {
                assert_eq!(bin, "cc");
                assert_eq!(actual, vec!["-shared", "main.c"]);
                assert_eq!(allowed.len(), 1);
                assert_eq!(allowed[0].purpose, "compile");
            }
            other => panic!("unexpected gateway error: {other}"),
        }
    }

    #[test]
    fn command_plan_uses_the_first_matching_permission_for_the_bin() {
        let mut permissions = Permissions::default();
        permissions.commands.extend([
            CommandPermission {
                bin: "cc".into(),
                args: vec!["-shared".into(), "*.c".into()],
                purpose: "build a shared library".into(),
                isolation: Some(CommandIsolation::TrustedHost),
                image: None,
            },
            CommandPermission {
                bin: "cc".into(),
                args: vec!["-o".into(), "*".into(), "*.c".into()],
                purpose: "build an executable".into(),
                isolation: Some(CommandIsolation::TrustedHost),
                image: None,
            },
        ]);
        let plan = CmdGateway::new(permissions, temp_workspace())
            .command_plan(&["cc".into(), "-o".into(), "app".into(), "main.c".into()])
            .expect("a later matching permission should be accepted");
        assert_eq!(plan["permission"]["purpose"], "build an executable");
    }

    #[test]
    fn denies_undeclared_network_host() {
        let permissions = Permissions::default();
        assert!(matches!(
            ensure_url_allowed(&permissions, "https://example.com/"),
            Err(GatewayError::NetworkDenied { .. })
        ));
    }

    #[test]
    fn permits_declared_network_host() {
        let mut permissions = Permissions::default();
        permissions.network.push("example.com".into());
        assert!(ensure_url_allowed(&permissions, "https://example.com/path").is_ok());
    }

    #[test]
    fn sensitive_query_parameters_are_removed_from_public_urls() {
        let sensitive = BTreeMap::from([("api_key".to_string(), "secret-value".to_string())]);
        let public = redact_query_parameters(
            "https://example.com/search?engine=google&api_key=secret-value&q=qcg",
            &sensitive,
        )
        .expect("URL should be redacted");
        assert_eq!(public, "https://example.com/search?engine=google&q=qcg");
        assert!(!public.contains("secret-value"));
    }

    macro_rules! fs_write_denied_path_case {
        ($name:ident, $path:expr) => {
            #[test]
            fn $name() {
                let mut permissions = Permissions::default();
                permissions.fs_write.push("workspace".into());
                let gateway = FsGateway::new(temp_workspace(), &permissions);
                assert!(
                    matches!(
                        gateway.resolve_write($path),
                        Err(GatewayError::PathDenied { .. })
                    ),
                    "write path should be denied: {:?}",
                    $path
                );
            }
        };
    }

    macro_rules! fs_read_denied_path_case {
        ($name:ident, $path:expr) => {
            #[test]
            fn $name() {
                let mut permissions = Permissions::default();
                permissions.fs_read.push("workspace".into());
                let gateway = FsGateway::new(temp_workspace(), &permissions);
                assert!(
                    matches!(
                        gateway.resolve_read($path),
                        Err(GatewayError::PathDenied { .. })
                    ),
                    "read path should be denied: {:?}",
                    $path
                );
            }
        };
    }

    macro_rules! wildcard_arg_denied_case {
        ($name:ident, $pattern:expr, $actual:expr) => {
            #[test]
            fn $name() {
                let permission = CommandPermission {
                    bin: "tool".into(),
                    args: vec![$pattern.into()],
                    purpose: "test wildcard".into(),
                    isolation: Some(CommandIsolation::TrustedHost),
                    image: None,
                };
                assert!(
                    !args_allowed(&permission, &[$actual.into()]),
                    "pattern {:?} should deny {:?}",
                    $pattern,
                    $actual
                );
            }
        };
    }

    macro_rules! wildcard_arg_allowed_case {
        ($name:ident, $pattern:expr, $actual:expr) => {
            #[test]
            fn $name() {
                let permission = CommandPermission {
                    bin: "tool".into(),
                    args: vec![$pattern.into()],
                    purpose: "test wildcard".into(),
                    isolation: Some(CommandIsolation::TrustedHost),
                    image: None,
                };
                assert!(
                    args_allowed(&permission, &[$actual.into()]),
                    "pattern {:?} should allow {:?}",
                    $pattern,
                    $actual
                );
            }
        };
    }

    macro_rules! url_allowed_case {
        ($name:ident, $allow:expr, $url:expr) => {
            #[test]
            fn $name() {
                let mut permissions = Permissions::default();
                permissions.network.push($allow.into());
                assert!(
                    ensure_url_allowed(&permissions, $url).is_ok(),
                    "allow {:?} should permit {:?}",
                    $allow,
                    $url
                );
            }
        };
    }

    macro_rules! url_denied_case {
        ($name:ident, $allow:expr, $url:expr, $error:pat) => {
            #[test]
            fn $name() {
                let mut permissions = Permissions::default();
                permissions.network.push($allow.into());
                assert!(
                    matches!(ensure_url_allowed(&permissions, $url), Err($error)),
                    "allow {:?} should deny {:?}",
                    $allow,
                    $url
                );
            }
        };
    }

    fs_write_denied_path_case!(fs_write_denies_parent_escape, "../secret.txt");
    fs_write_denied_path_case!(fs_write_denies_nested_parent_escape, "a/../../secret.txt");
    fs_write_denied_path_case!(fs_write_denies_absolute_unix_path, "/tmp/secret.txt");
    fs_write_denied_path_case!(fs_write_denies_backslash_parent_escape, r"..\secret.txt");
    fs_write_denied_path_case!(fs_write_denies_backslash_separator, r"dir\secret.txt");
    fs_write_denied_path_case!(fs_write_denies_nul_byte, "dir\0secret.txt");
    fs_write_denied_path_case!(fs_write_denies_current_dir_component, "./secret.txt");
    fs_write_denied_path_case!(
        fs_write_denies_current_dir_then_parent_dir,
        "dir/./../secret.txt"
    );
    fs_write_denied_path_case!(fs_write_denies_embedded_parent_dir, "dir/../secret.txt");
    fs_write_denied_path_case!(fs_write_denies_parent_only, "..");
    fs_write_denied_path_case!(fs_write_denies_dir_parent_suffix, "dir/..");
    fs_write_denied_path_case!(fs_write_denies_trailing_parent, "dir/sub/..");
    fs_write_denied_path_case!(
        fs_write_denies_deep_parent_escape,
        "dir/sub/../../secret.txt"
    );
    fs_write_denied_path_case!(fs_write_denies_unc_shape, r"\\server\share\secret.txt");
    fs_write_denied_path_case!(fs_write_denies_windows_device_shape, r"\\?\C:\secret.txt");
    fs_write_denied_path_case!(fs_write_denies_windows_drive_backslash, r"C:\secret.txt");
    fs_write_denied_path_case!(
        fs_write_denies_forward_then_backslash,
        r"dir/sub\secret.txt"
    );
    fs_write_denied_path_case!(
        fs_write_denies_parent_after_normalized_leaf,
        "dir/.../../secret.txt"
    );
    fs_write_denied_path_case!(
        fs_write_denies_repeated_parent_components,
        "../../secret.txt"
    );
    fs_write_denied_path_case!(fs_write_denies_dot_parent_combo, "./../secret.txt");

    fs_read_denied_path_case!(fs_read_denies_parent_escape, "../secret.txt");
    fs_read_denied_path_case!(fs_read_denies_nested_parent_escape, "a/../../secret.txt");
    fs_read_denied_path_case!(fs_read_denies_absolute_unix_path, "/tmp/secret.txt");
    fs_read_denied_path_case!(fs_read_denies_backslash_parent_escape, r"..\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_backslash_separator, r"dir\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_nul_byte, "dir\0secret.txt");
    fs_read_denied_path_case!(fs_read_denies_current_dir_component, "./secret.txt");
    fs_read_denied_path_case!(
        fs_read_denies_current_dir_then_parent_dir,
        "dir/./../secret.txt"
    );
    fs_read_denied_path_case!(fs_read_denies_embedded_parent_dir, "dir/../secret.txt");
    fs_read_denied_path_case!(fs_read_denies_parent_only, "..");
    fs_read_denied_path_case!(fs_read_denies_dir_parent_suffix, "dir/..");
    fs_read_denied_path_case!(fs_read_denies_trailing_parent, "dir/sub/..");
    fs_read_denied_path_case!(
        fs_read_denies_deep_parent_escape,
        "dir/sub/../../secret.txt"
    );
    fs_read_denied_path_case!(fs_read_denies_unc_shape, r"\\server\share\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_windows_device_shape, r"\\?\C:\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_windows_drive_backslash, r"C:\secret.txt");
    fs_read_denied_path_case!(fs_read_denies_forward_then_backslash, r"dir/sub\secret.txt");
    fs_read_denied_path_case!(
        fs_read_denies_parent_after_normalized_leaf,
        "dir/.../../secret.txt"
    );
    fs_read_denied_path_case!(
        fs_read_denies_repeated_parent_components,
        "../../secret.txt"
    );
    fs_read_denied_path_case!(fs_read_denies_dot_parent_combo, "./../secret.txt");

    wildcard_arg_denied_case!(wildcard_star_denies_empty_arg, "*", "");
    wildcard_arg_denied_case!(wildcard_star_denies_parent_escape, "*", "../secret");
    wildcard_arg_denied_case!(
        wildcard_star_denies_nested_parent_escape,
        "*",
        "ok/../secret"
    );
    wildcard_arg_denied_case!(wildcard_star_denies_absolute_path, "*", "/tmp/secret");
    wildcard_arg_denied_case!(wildcard_star_denies_backslash_parent, "*", r"..\secret");
    wildcard_arg_denied_case!(wildcard_star_denies_backslash_separator, "*", r"dir\secret");
    wildcard_arg_denied_case!(wildcard_star_denies_nul_byte, "*", "bad\0arg");
    wildcard_arg_denied_case!(wildcard_suffix_denies_parent_escape, "*.c", "../main.c");
    wildcard_arg_denied_case!(
        wildcard_suffix_denies_nested_parent_escape,
        "*.c",
        "src/../main.c"
    );
    wildcard_arg_denied_case!(wildcard_suffix_denies_absolute_path, "*.c", "/tmp/main.c");
    wildcard_arg_denied_case!(wildcard_suffix_denies_backslash_parent, "*.c", r"..\main.c");
    wildcard_arg_denied_case!(
        wildcard_suffix_denies_backslash_separator,
        "*.c",
        r"src\main.c"
    );
    wildcard_arg_denied_case!(wildcard_suffix_denies_nul_byte, "*.c", "main\0.c");
    wildcard_arg_denied_case!(wildcard_prefix_denies_parent_escape, "src/*", "../main.c");
    wildcard_arg_denied_case!(
        wildcard_prefix_denies_nested_parent_escape,
        "src/*",
        "src/../main.c"
    );
    wildcard_arg_denied_case!(wildcard_prefix_denies_absolute_path, "src/*", "/tmp/main.c");
    wildcard_arg_denied_case!(
        wildcard_prefix_denies_backslash_separator,
        "src/*",
        r"src\main.c"
    );
    wildcard_arg_denied_case!(wildcard_prefix_denies_nul_byte, "src/*", "src/main\0.c");
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_parent_escape,
        "--file=*",
        "--file=../secret"
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_empty_value,
        "--file=*",
        "--file="
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_absolute_path,
        "--file=*",
        "--file=/tmp/secret"
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_nested_parent_escape,
        "--file=*",
        "--file=ok/../secret"
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_backslash,
        "--file=*",
        r"--file=..\secret"
    );
    wildcard_arg_denied_case!(
        wildcard_flag_value_denies_nul_byte,
        "--file=*",
        "--file=bad\0arg"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_parent_escape,
        "*-out",
        "../build-out"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_nested_parent,
        "*-out",
        "build/../out"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_absolute_path,
        "*-out",
        "/tmp/build-out"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_backslash,
        "*-out",
        r"build\app-out"
    );
    wildcard_arg_denied_case!(
        wildcard_output_suffix_denies_nul_byte,
        "*-out",
        "build\0-out"
    );
    wildcard_arg_denied_case!(
        wildcard_json_suffix_denies_parent_escape,
        "*.json",
        "../data.json"
    );
    wildcard_arg_denied_case!(
        wildcard_json_suffix_denies_nested_parent,
        "*.json",
        "data/../data.json"
    );
    wildcard_arg_denied_case!(
        wildcard_json_suffix_denies_absolute_path,
        "*.json",
        "/tmp/data.json"
    );
    wildcard_arg_denied_case!(
        wildcard_json_suffix_denies_backslash,
        "*.json",
        r"data\data.json"
    );

    wildcard_arg_allowed_case!(wildcard_star_allows_filename, "*", "config.yaml");
    wildcard_arg_allowed_case!(
        wildcard_star_allows_nested_relative_path,
        "*",
        "configs/qpx.yaml"
    );
    wildcard_arg_allowed_case!(wildcard_suffix_allows_leaf_c_file, "*.c", "main.c");
    wildcard_arg_allowed_case!(wildcard_suffix_allows_nested_c_file, "*.c", "src/main.c");
    wildcard_arg_allowed_case!(wildcard_prefix_allows_nested_file, "src/*", "src/main.c");
    wildcard_arg_allowed_case!(
        wildcard_prefix_allows_deep_nested_file,
        "src/*",
        "src/bin/main.c"
    );
    wildcard_arg_allowed_case!(
        wildcard_flag_value_allows_relative_file,
        "--file=*",
        "--file=config.yaml"
    );
    wildcard_arg_allowed_case!(wildcard_output_suffix_allows_leaf, "*-out", "build-out");
    wildcard_arg_allowed_case!(wildcard_json_suffix_allows_leaf, "*.json", "data.json");
    wildcard_arg_allowed_case!(
        wildcard_json_suffix_allows_nested_file,
        "*.json",
        "data/input.json"
    );

    url_allowed_case!(
        url_allows_exact_host_https,
        "example.com",
        "https://example.com/path"
    );
    url_allowed_case!(
        url_allows_full_url_host_https,
        "https://example.com/base",
        "https://example.com/other"
    );
    url_allowed_case!(url_allows_wildcard_https, "*", "https://example.net/path");
    url_allowed_case!(
        url_allows_host_with_port_by_host,
        "127.0.0.1",
        "http://127.0.0.1:8080/path"
    );
    url_allowed_case!(
        url_allows_localhost_with_port_by_host,
        "localhost",
        "http://localhost:3000/path"
    );
    url_allowed_case!(
        url_allows_subdomain_when_exact,
        "api.example.com",
        "https://api.example.com/v1"
    );
    url_allowed_case!(
        url_allows_case_normalized_host,
        "example.com",
        "https://EXAMPLE.com/path"
    );

    url_denied_case!(
        url_denies_different_host,
        "example.com",
        "https://other.example.com/path",
        GatewayError::NetworkDenied { .. }
    );
    url_denied_case!(
        url_denies_parent_domain,
        "api.example.com",
        "https://example.com/path",
        GatewayError::NetworkDenied { .. }
    );
    url_denied_case!(
        url_denies_sibling_subdomain,
        "api.example.com",
        "https://cdn.example.com/path",
        GatewayError::NetworkDenied { .. }
    );
    url_denied_case!(
        url_denies_file_scheme,
        "*",
        "file:///tmp/secret",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_ftp_scheme,
        "*",
        "ftp://example.com/file",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_missing_host,
        "*",
        "https://",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_relative_url,
        "*",
        "/relative/path",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_malformed_url,
        "*",
        "not a url",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_javascript_scheme,
        "*",
        "javascript:alert(1)",
        GatewayError::UnsupportedUrl { .. }
    );
    url_denied_case!(
        url_denies_data_scheme,
        "*",
        "data:text/plain,secret",
        GatewayError::UnsupportedUrl { .. }
    );

    #[test]
    fn command_plan_records_the_declared_container_runtime() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "tool".into(),
            args: vec![],
            purpose: "container test".into(),
            isolation: Some(CommandIsolation::Container),
            image: Some("example/tool@sha256:abc".into()),
        });
        permissions.containers.enabled = true;
        permissions.containers.runtime = Some(qcg_contract::ContainerRuntime::Incus);
        let plan = CmdGateway::new(permissions, temp_workspace())
            .command_plan(&["tool".into()])
            .expect("declared container command should have a plan");
        assert_eq!(plan["runtime"], "incus");
    }

    #[tokio::test]
    async fn container_workload_without_a_declared_runtime_fails_closed() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        let mounts: Vec<(Utf8PathBuf, String, bool)> = vec![];
        let workload = vec!["echo".to_string(), "hi".to_string()];
        let error = gateway
            .run_container_workload(
                ContainerWorkload {
                    image: "example/tool@sha256:abc",
                    mounts: &mounts,
                    workdir: Some("/work"),
                    workload_argv: &workload,
                    stdin: None,
                },
                5,
                Some(1024),
            )
            .await
            .expect_err("missing runtime must fail without touching a daemon");
        assert!(
            matches!(error, GatewayError::ContainerRuntimeMissing { .. }),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn command_plan_denies_empty_argv() {
        let gateway = CmdGateway::new(Permissions::default(), temp_workspace());
        assert!(matches!(
            gateway.command_plan(&[]),
            Err(GatewayError::EmptyCommand)
        ));
    }

    #[test]
    fn command_plan_denies_undeclared_bin_with_allowed_summary() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "cc".into(),
            args: vec!["*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        let error = gateway
            .command_plan(&["sh".into(), "-c".into(), "echo no".into()])
            .expect_err("undeclared bin should be denied");
        match error {
            GatewayError::CommandDenied { bin, allowed } => {
                assert_eq!(bin, "sh");
                assert_eq!(allowed.len(), 1);
                assert_eq!(allowed[0].bin, "cc");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn command_plan_denies_extra_args_for_empty_pattern() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "date".into(),
            args: vec![],
            purpose: "print date".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        assert!(matches!(
            gateway.command_plan(&["date".into(), "-u".into()]),
            Err(GatewayError::CommandArgsDenied { .. })
        ));
    }

    #[test]
    fn command_plan_denies_missing_arg_for_declared_pattern() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "cc".into(),
            args: vec!["*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        assert!(matches!(
            gateway.command_plan(&["cc".into()]),
            Err(GatewayError::CommandArgsDenied { .. })
        ));
    }

    #[test]
    fn command_plan_denies_too_many_args_for_declared_pattern() {
        let mut permissions = Permissions::default();
        permissions.commands.push(CommandPermission {
            bin: "cc".into(),
            args: vec!["*.c".into()],
            purpose: "compile".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        });
        let gateway = CmdGateway::new(permissions, temp_workspace());
        assert!(matches!(
            gateway.command_plan(&["cc".into(), "main.c".into(), "extra.c".into()]),
            Err(GatewayError::CommandArgsDenied { .. })
        ));
    }
}
