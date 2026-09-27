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
    // Best-effort pre-clean documented here: a leftover link from a prior run is removed when present (E13).
    if std::fs::remove_file(&link).is_err() && link.exists() {
        panic!("stale test symlink should be removable");
    }
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

#[cfg(unix)]
#[tokio::test]
async fn failed_streaming_write_reclaims_staging_and_keeps_target() {
    // E13: a producer failure must remove the staged file, keep the
    // committed target, and leave no `.qcg-part-*` artifact behind.
    let base = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
        "qcg-gateway-stream-{}",
        uuid::Uuid::now_v7().as_simple()
    )))
    .expect("temporary directory path must be utf-8");
    let workspace = base.join("workspace");
    std::fs::create_dir_all(workspace.join("out")).expect("test workspace should be created");
    let mut permissions = Permissions::default();
    permissions.fs_write.push("workspace".into());
    let gateway = FsGateway::new(workspace.clone(), &permissions);
    let target = gateway
        .resolve_write("out/data.txt")
        .expect("target should resolve");
    gateway
        .write_file_atomic(&target, b"original")
        .await
        .expect("first write should succeed");
    let error = gateway
        .write_file_atomic_stream(&target, None, |file| -> std::io::Result<()> {
            use std::io::Write as _;
            file.write_all(b"partial")?;
            Err(std::io::Error::other("producer failed"))
        })
        .await
        .expect_err("a producer error must fail the write");
    assert!(
        error.to_string().contains("producer failed"),
        "the producer error must surface: {error}"
    );
    assert_eq!(
        std::fs::read(workspace.join("out/data.txt")).expect("target readable"),
        b"original",
        "a failed streaming write must not replace the target"
    );
    let leaked: Vec<String> = std::fs::read_dir(workspace.join("out"))
        .expect("out dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("qcg-part"))
        .collect();
    assert!(
        leaked.is_empty(),
        "a failed streaming write must not leak staging files: {leaked:?}"
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
fn sensitive_query_values_refuse_typos_and_bad_urls() {
    // E09: a mistyped name or unparseable URL fails closed so the value
    // cannot slip into the journal in the clear.
    use super::http::sensitive_query_values;
    let values = sensitive_query_values(
        "https://example.com/search?engine=google&api_key=secret-value",
        &["api_key".to_string()],
    )
    .expect("declared values should extract");
    assert_eq!(
        values.get("api_key").map(String::as_str),
        Some("secret-value")
    );
    assert!(
        sensitive_query_values(
            "https://example.com/search?engine=google",
            &["api_key".to_string()]
        )
        .is_err()
    );
    assert!(sensitive_query_values(":::/not a url", &["api_key".to_string()]).is_err());
    assert!(sensitive_query_values("https://example.com/plain", &[]).is_ok());
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

#[test]
fn fs_write_denies_path_escape_shapes() {
    // Every traversal shape the workspace resolver must refuse for
    // writes, in one table instead of one test per shape.
    let paths = [
        "../secret.txt",
        "a/../../secret.txt",
        "/tmp/secret.txt",
        r"..\secret.txt",
        r"dir\secret.txt",
        "dir\0secret.txt",
        "./secret.txt",
        "dir/./../secret.txt",
        "dir/../secret.txt",
        "..",
        "dir/..",
        "dir/sub/..",
        "dir/sub/../../secret.txt",
        r"\\server\share\secret.txt",
        r"\\?\C:\secret.txt",
        r"C:\secret.txt",
        r"dir/sub\secret.txt",
        "dir/.../../secret.txt",
        "../../secret.txt",
        "./../secret.txt",
    ];
    let mut permissions = Permissions::default();
    permissions.fs_write.push("workspace".into());
    let gateway = FsGateway::new(temp_workspace(), &permissions);
    let mut diverged = Vec::new();
    for path in paths {
        if !matches!(
            gateway.resolve_write(path),
            Err(GatewayError::PathDenied { .. })
        ) {
            diverged.push(path);
        }
    }
    assert!(
        diverged.is_empty(),
        "write paths must be denied: {diverged:?}"
    );
}

#[test]
fn fs_read_denies_path_escape_shapes() {
    // Read-side mirror of the write table: the same shapes must fail
    // through the shared resolver.
    let paths = [
        "../secret.txt",
        "a/../../secret.txt",
        "/tmp/secret.txt",
        r"..\secret.txt",
        r"dir\secret.txt",
        "dir\0secret.txt",
        "./secret.txt",
        "dir/./../secret.txt",
        "dir/../secret.txt",
        "..",
        "dir/..",
        "dir/sub/..",
        "dir/sub/../../secret.txt",
        r"\\server\share\secret.txt",
        r"\\?\C:\secret.txt",
        r"C:\secret.txt",
        r"dir/sub\secret.txt",
        "dir/.../../secret.txt",
        "../../secret.txt",
        "./../secret.txt",
    ];
    let mut permissions = Permissions::default();
    permissions.fs_read.push("workspace".into());
    let gateway = FsGateway::new(temp_workspace(), &permissions);
    let mut diverged = Vec::new();
    for path in paths {
        if !matches!(
            gateway.resolve_read(path),
            Err(GatewayError::PathDenied { .. })
        ) {
            diverged.push(path);
        }
    }
    assert!(
        diverged.is_empty(),
        "read paths must be denied: {diverged:?}"
    );
}

#[test]
fn wildcard_args_deny_unsafe_shapes_across_patterns() {
    // Every unsafe value shape must be denied under every glob
    // pattern kind: the check inspects the value, not the pattern.
    let cases: &[(&str, &[&str])] = &[
        (
            "*",
            &[
                "",
                "../secret",
                "ok/../secret",
                "/tmp/secret",
                r"..\secret",
                r"dir\secret",
                "bad\0arg",
            ],
        ),
        (
            "*.c",
            &[
                "../main.c",
                "src/../main.c",
                "/tmp/main.c",
                r"..\main.c",
                r"src\main.c",
                "main\0.c",
            ],
        ),
        (
            "src/*",
            &[
                "../main.c",
                "src/../main.c",
                "/tmp/main.c",
                r"src\main.c",
                "src/main\0.c",
            ],
        ),
        (
            "--file=*",
            &[
                "--file=../secret",
                "--file=",
                "--file=/tmp/secret",
                "--file=ok/../secret",
                r"--file=..\secret",
                "--file=bad\0arg",
            ],
        ),
        (
            "*-out",
            &[
                "../build-out",
                "build/../out",
                "/tmp/build-out",
                r"build\app-out",
                "build\0-out",
            ],
        ),
        (
            "*.json",
            &[
                "../data.json",
                "data/../data.json",
                "/tmp/data.json",
                r"data\data.json",
            ],
        ),
    ];
    let mut diverged = Vec::new();
    for (pattern, actuals) in cases {
        let permission = CommandPermission {
            bin: "tool".into(),
            args: vec![(*pattern).into()],
            purpose: "test wildcard".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        };
        for actual in *actuals {
            if args_allowed(&permission, &[(*actual).into()]) {
                diverged.push((*pattern, *actual));
            }
        }
    }
    assert!(
        diverged.is_empty(),
        "wildcard patterns must deny unsafe values: {diverged:?}"
    );
}

#[test]
fn wildcard_args_allow_safe_shapes_across_patterns() {
    let cases: &[(&str, &[&str])] = &[
        ("*", &["config.yaml", "configs/qpx.yaml"]),
        ("*.c", &["main.c", "src/main.c"]),
        ("src/*", &["src/main.c", "src/bin/main.c"]),
        ("--file=*", &["--file=config.yaml"]),
        ("*-out", &["build-out"]),
        ("*.json", &["data.json", "data/input.json"]),
    ];
    let mut diverged = Vec::new();
    for (pattern, actuals) in cases {
        let permission = CommandPermission {
            bin: "tool".into(),
            args: vec![(*pattern).into()],
            purpose: "test wildcard".into(),
            isolation: Some(CommandIsolation::TrustedHost),
            image: None,
        };
        for actual in *actuals {
            if !args_allowed(&permission, &[(*actual).into()]) {
                diverged.push((*pattern, *actual));
            }
        }
    }
    assert!(
        diverged.is_empty(),
        "wildcard patterns must allow safe values: {diverged:?}"
    );
}

#[test]
fn urls_allow_declared_hosts() {
    let cases: &[(&str, &str)] = &[
        ("example.com", "https://example.com/path"),
        ("https://example.com/base", "https://example.com/other"),
        ("*", "https://example.net/path"),
        ("127.0.0.1", "http://127.0.0.1:8080/path"),
        ("localhost", "http://localhost:3000/path"),
        ("api.example.com", "https://api.example.com/v1"),
        ("example.com", "https://EXAMPLE.com/path"),
    ];
    for (allow, url) in cases {
        let mut permissions = Permissions::default();
        permissions.network.push((*allow).into());
        assert!(
            ensure_url_allowed(&permissions, url).is_ok(),
            "allow {allow:?} should permit {url:?}"
        );
    }
}

#[test]
fn urls_deny_undeclared_hosts_and_unsupported_schemes() {
    // Host mismatches report NetworkDenied; non-http(s) schemes and
    // unparsable URLs report UnsupportedUrl.
    let denied: &[(&str, &str)] = &[
        ("example.com", "https://other.example.com/path"),
        ("api.example.com", "https://example.com/path"),
        ("api.example.com", "https://cdn.example.com/path"),
    ];
    for (allow, url) in denied {
        let mut permissions = Permissions::default();
        permissions.network.push((*allow).into());
        assert!(
            matches!(
                ensure_url_allowed(&permissions, url),
                Err(GatewayError::NetworkDenied { .. })
            ),
            "allow {allow:?} should deny {url:?}"
        );
    }
    let unsupported: &[&str] = &[
        "file:///tmp/secret",
        "ftp://example.com/file",
        "https://",
        "/relative/path",
        "not a url",
        "javascript:alert(1)",
        "data:text/plain,secret",
    ];
    for url in unsupported {
        let mut permissions = Permissions::default();
        permissions.network.push("*".into());
        assert!(
            matches!(
                ensure_url_allowed(&permissions, url),
                Err(GatewayError::UnsupportedUrl { .. })
            ),
            "unsupported URL should be refused: {url:?}"
        );
    }
}

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
fn command_plan_denies_arg_count_mismatches() {
    // Extra args for an empty pattern, a missing arg, and too many
    // args all fail on the same declared-arity check.
    let mut permissions = Permissions::default();
    permissions.commands.push(CommandPermission {
        bin: "date".into(),
        args: vec![],
        purpose: "print date".into(),
        isolation: Some(CommandIsolation::TrustedHost),
        image: None,
    });
    permissions.commands.push(CommandPermission {
        bin: "cc".into(),
        args: vec!["*.c".into()],
        purpose: "compile".into(),
        isolation: Some(CommandIsolation::TrustedHost),
        image: None,
    });
    let gateway = CmdGateway::new(permissions, temp_workspace());
    assert!(matches!(
        gateway.command_plan(&["date".into(), "-u".into()]),
        Err(GatewayError::CommandArgsDenied { .. })
    ));
    assert!(matches!(
        gateway.command_plan(&["cc".into()]),
        Err(GatewayError::CommandArgsDenied { .. })
    ));
    assert!(matches!(
        gateway.command_plan(&["cc".into(), "main.c".into(), "extra.c".into()]),
        Err(GatewayError::CommandArgsDenied { .. })
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn parent_swap_after_resolution_cannot_escape_the_workspace() {
    // E13a: validate first, then replace the parent with a symlink to
    // an outside directory. Both write and read must fail instead of
    // reaching the outside target.
    let root = Utf8PathBuf::from_path_buf(
        std::env::temp_dir().join(format!("qcg-gateway-swap-{}", uuid::Uuid::now_v7())),
    )
    .expect("temporary path must be UTF-8");
    let workspace = root.join("workspace");
    let outside = root.join("outside");
    std::fs::create_dir_all(workspace.join("sub")).expect("workspace sub");
    std::fs::create_dir_all(outside.as_std_path()).expect("outside dir");
    std::fs::write(workspace.join("sub/secret.txt"), b"inside").expect("secret");
    std::fs::write(workspace.join("sub/removable.txt"), b"remove").expect("removable");
    let mut permissions = Permissions::default();
    permissions.fs_read.push("workspace".into());
    permissions.fs_write.push("workspace".into());
    let gateway = FsGateway::new(workspace.clone(), &permissions);
    let write_target = gateway
        .resolve_write("sub/file.txt")
        .expect("write should resolve before the swap");
    let read_target = gateway
        .resolve_read("sub/secret.txt")
        .expect("read should resolve before the swap");
    let remove_target = gateway
        .resolve_read("sub/removable.txt")
        .expect("remove should resolve before the swap");
    std::fs::rename(workspace.join("sub"), workspace.join("sub-real")).expect("rename");
    std::os::unix::fs::symlink(&outside, workspace.join("sub")).expect("symlink");
    let write_error = gateway
        .write_file_atomic(&write_target, b"escape")
        .await
        .expect_err("write through a swapped parent must fail");
    assert!(
        !outside.join("file.txt").exists(),
        "write must not escape the workspace: {write_error}"
    );
    gateway
        .open_read_resolved(&read_target)
        .expect_err("read through a swapped parent must fail");
    assert!(
        !outside.join("secret.txt").exists(),
        "read must not escape the workspace"
    );
    gateway
        .remove_file_resolved(&remove_target)
        .await
        .expect_err("remove through a swapped parent must fail");
    assert!(
        workspace.join("sub-real/removable.txt").exists(),
        "remove must not touch the real target"
    );
    assert!(
        !outside.join("removable.txt").exists(),
        "remove must not follow the swapped parent"
    );
    // Best-effort test cleanup documented here: temp removal cannot propagate from test tails (E13).
}

#[cfg(unix)]
#[test]
fn snapshot_tree_copies_handle_relative_and_rejects_symlinks() {
    // E13: consumers that must parse a tree by path read a private
    // handle-relative snapshot, which never follows symlinks.
    let root = Utf8PathBuf::from_path_buf(
        std::env::temp_dir().join(format!("qcg-tree-snapshot-{}", uuid::Uuid::now_v7())),
    )
    .expect("temporary path must be UTF-8");
    let workspace = root.join("workspace");
    let dest = root.join("snapshot");
    std::fs::create_dir_all(workspace.join("out/sub")).expect("workspace tree");
    std::fs::write(workspace.join("out/a.txt"), b"alpha").expect("file a");
    std::fs::write(workspace.join("out/sub/b.txt"), b"beta").expect("file b");
    let mut permissions = Permissions::default();
    permissions.fs_read.push("workspace".into());
    permissions.fs_write.push("workspace".into());
    let gateway = FsGateway::new(workspace.clone(), &permissions);
    let tree = gateway.resolve_read("out").expect("tree should resolve");
    gateway
        .snapshot_tree_to(&tree, &dest, Some(100), Some(10))
        .expect("snapshot should copy");
    assert_eq!(
        std::fs::read(dest.join("a.txt")).expect("copied a"),
        b"alpha"
    );
    assert_eq!(
        std::fs::read(dest.join("sub/b.txt")).expect("copied b"),
        b"beta"
    );
    gateway
        .snapshot_tree_to(&tree, &root.join("over-limit"), Some(2), Some(10))
        .expect_err("byte limit must fail");
    std::os::unix::fs::symlink(workspace.join("out/a.txt"), workspace.join("out/link"))
        .expect("symlink");
    gateway
        .snapshot_tree_to(&tree, &root.join("with-link"), Some(100), Some(10))
        .expect_err("symlinked entry must fail");
    // Best-effort test cleanup documented here: temp removal cannot propagate from test tails (E13).
}

#[cfg(unix)]
#[test]
fn bounded_tree_stats_rejects_symlinks_limits_and_swapped_parents() {
    // E13: the workspace tree check walks handle-relative and never
    // follows symlinks, so it cannot be redirected after resolution.
    let root = Utf8PathBuf::from_path_buf(
        std::env::temp_dir().join(format!("qcg-tree-stats-{}", uuid::Uuid::now_v7())),
    )
    .expect("temporary path must be UTF-8");
    let workspace = root.join("workspace");
    let outside = root.join("outside");
    std::fs::create_dir_all(workspace.join("out/sub")).expect("workspace tree");
    std::fs::create_dir_all(outside.as_std_path()).expect("outside dir");
    std::fs::write(workspace.join("out/a.txt"), b"0123456789").expect("file a");
    std::fs::write(workspace.join("out/sub/b.txt"), b"01234").expect("file b");
    let mut permissions = Permissions::default();
    permissions.fs_read.push("workspace".into());
    permissions.fs_write.push("workspace".into());
    let gateway = FsGateway::new(workspace.clone(), &permissions);
    let tree = gateway.resolve_read("out").expect("tree should resolve");
    gateway
        .bounded_tree_stats(&tree, Some(100), Some(10))
        .expect("tree within limits");
    gateway
        .bounded_tree_stats(&tree, Some(5), Some(10))
        .expect_err("oversize tree must fail");
    gateway
        .bounded_tree_stats(&tree, Some(100), Some(1))
        .expect_err("entry limit must fail");
    std::os::unix::fs::symlink(workspace.join("out/a.txt"), workspace.join("out/link"))
        .expect("symlink");
    gateway
        .bounded_tree_stats(&tree, Some(100), Some(10))
        .expect_err("symlinked entry must fail");
    std::fs::remove_file(workspace.join("out/link")).expect("remove symlink");
    std::fs::rename(workspace.join("out"), workspace.join("out-real")).expect("rename");
    std::os::unix::fs::symlink(&outside, workspace.join("out")).expect("swap symlink");
    gateway
        .bounded_tree_stats(&tree, Some(100), Some(10))
        .expect_err("a swapped parent must not be followed");
    // Best-effort test cleanup documented here: temp removal cannot propagate from test tails (E13).
}

#[cfg(unix)]
#[test]
fn snapshot_tree_rejects_symlinked_destination_parents() {
    // E13: a planted symlink in the snapshot destination tree would
    // divert the copy outside the run meta dir, so symlinked
    // destination parents are refused before anything is created.
    let root = Utf8PathBuf::from_path_buf(
        std::env::temp_dir().join(format!("qcg-tree-dest-link-{}", uuid::Uuid::now_v7())),
    )
    .expect("temporary path must be UTF-8");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(workspace.join("out")).expect("workspace tree");
    std::fs::write(workspace.join("out/a.txt"), b"alpha").expect("file a");
    let mut permissions = Permissions::default();
    permissions.fs_read.push("workspace".into());
    permissions.fs_write.push("workspace".into());
    let gateway = FsGateway::new(workspace.clone(), &permissions);
    let tree = gateway.resolve_read("out").expect("tree should resolve");
    let outside = root.join("outside");
    std::fs::create_dir_all(&outside).expect("outside dir");
    let linked_parent = root.join("linked");
    std::os::unix::fs::symlink(&outside, &linked_parent).expect("symlinked parent");
    gateway
        .snapshot_tree_to(&tree, &linked_parent.join("snap"), Some(100), Some(10))
        .expect_err("a symlinked destination parent must fail");
    assert!(
        !outside.join("snap").exists(),
        "refusing the symlinked destination must divert nothing outside"
    );
    // Best-effort test cleanup documented here: temp removal cannot propagate from test tails (E13).
}

#[cfg(unix)]
#[test]
fn snapshot_tree_copies_with_explicit_modes() {
    // E15: snapshot copies set explicit modes instead of inheriting the
    // process umask.
    use std::os::unix::fs::PermissionsExt as _;
    let root = Utf8PathBuf::from_path_buf(
        std::env::temp_dir().join(format!("qcg-tree-modes-{}", uuid::Uuid::now_v7())),
    )
    .expect("temporary path must be UTF-8");
    let workspace = root.join("workspace");
    let dest = root.join("snapshot");
    std::fs::create_dir_all(workspace.join("out")).expect("workspace tree");
    std::fs::write(workspace.join("out/a.txt"), b"alpha").expect("file a");
    let mut permissions = Permissions::default();
    permissions.fs_read.push("workspace".into());
    permissions.fs_write.push("workspace".into());
    let gateway = FsGateway::new(workspace.clone(), &permissions);
    let tree = gateway.resolve_read("out").expect("tree should resolve");
    gateway
        .snapshot_tree_to(&tree, &dest, Some(100), Some(10))
        .expect("snapshot should copy");
    assert_eq!(
        std::fs::metadata(dest.join("a.txt"))
            .expect("copied file should stat")
            .permissions()
            .mode()
            & 0o777,
        0o600,
        "snapshot copies must carry the explicit owner-only mode"
    );
    // Best-effort test cleanup documented here: temp removal cannot propagate from test tails (E13).
}
