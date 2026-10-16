//! Unix process/pipe regressions. Every fixture uses its own absolute path;
//! no PATH or other process-wide environment changes are needed.
use super::*;
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt;

struct Client {
    root: PathBuf,
    binary: String,
}

impl Client {
    fn new(mode: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("qcg-cleanup-stderr-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let binary = root.join("client");
        let script = format!(
            r#"#!/bin/sh
root='{}'
printf '%s\n' "$1" >> "$root/log"
if [ '{mode}' = home ]; then printf '%s\n' "$HOME" >> "$root/home-seen"; fi
case "{mode}:$1" in
  misleading-pool:delete|misleading-pool:rm)
    echo 'Failed deleting instance owned-id: storage pool default not found' >&2
    exit 1 ;;
  misleading-project:delete|misleading-project:rm)
    printf 'Failed deleting owned-id\nError: project default does not exist\n' >&2
    exit 1 ;;
  running:*)
    echo "$$" >> "$root/direct-pids"
    exec /bin/sleep 8 ;;
  absent:*)
    echo 'Error: no such instance owned-id' >&2
    exit 1 ;;
  held-stop:stop|held-stop:kill|held-success:stop|held-success:kill|held-delete:delete|held-delete:rm)
    /bin/sleep 8 >&2 &
    echo "$!" >> "$root/pids"
    echo 'Error: no such instance owned-id' >&2
    echo "$1" >> "$root/exited"
    if [ '{mode}' = held-success ]; then exit 0; fi
    exit 1 ;;
esac
exit 0
"#,
            root.display()
        );
        // Concurrent test forks can briefly inherit a writable executable
        // descriptor even with CLOEXEC, causing ETXTBSY in another thread.
        // Open the target only in a writer process and wait for it to exit;
        // this process never holds a writable descriptor for the executable.
        let mut writer = std::process::Command::new("/bin/sh")
            .args(["-c", "/bin/cat > \"$1\"", "write-client"])
            .arg(&binary)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        writer
            .stdin
            .take()
            .unwrap()
            .write_all(script.as_bytes())
            .unwrap();
        assert!(writer.wait().unwrap().success());
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            root,
            binary: binary.to_str().unwrap().into(),
        }
    }

    fn session(&self, docker: bool) -> Session {
        Session {
            timeouts: policy::ContainerTimeouts::from_seconds([1; 4]).unwrap(),
            backend: if docker {
                Backend::Docker {
                    binary: self.binary.clone(),
                    runtime_flag: None,
                }
            } else {
                Backend::Incus {
                    binary: self.binary.clone(),
                }
            },
            id: if docker {
                let cidfile = self.root.join("cid");
                std::fs::write(&cidfile, "owned-id").unwrap();
                InstanceId::CidFile(cidfile)
            } else {
                InstanceId::Name("owned-id".into())
            },
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // The production code owns only the daemon client, not arbitrary
        // descendants. Kill the fixture's sleepers rather than leaving them
        // around after the timeout assertions (they also self-expire).
        if let Ok(pids) = std::fs::read_to_string(self.root.join("pids")) {
            for pid in pids.lines() {
                let _ = std::process::Command::new("/bin/kill")
                    .args(["-KILL", pid])
                    .status();
            }
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

async fn check_inherited_stderr(sync: bool) {
    for docker in [false, true] {
        for mode in ["held-stop", "held-success", "held-delete"] {
            let client = Client::new(mode);
            let session = client.session(docker);
            let start = std::time::Instant::now();
            let result = if sync {
                teardown_sync(&session)
            } else {
                teardown(&session).await
            };
            let elapsed = start.elapsed();
            assert!(
                elapsed + Duration::from_millis(50) >= session.timeouts.stop(),
                "{mode}: EOF must be awaited, got {elapsed:?}: {result:?}"
            );
            assert!(elapsed < Duration::from_secs(3), "{mode}: {elapsed:?}");
            let final_stage = if docker { "rm" } else { "delete" };
            assert_eq!(
                std::fs::read_to_string(client.root.join("log")).unwrap(),
                format!("{}\n{final_stage}\n", if docker { "kill" } else { "stop" }),
                "delete/rm must follow even a timed-out stop/kill"
            );
            let exited = std::fs::read_to_string(client.root.join("exited")).unwrap();
            assert!(exited.contains(if mode == "held-delete" {
                final_stage
            } else if docker {
                "kill"
            } else {
                "stop"
            }));
            if mode == "held-delete" {
                assert!(
                    matches!(
                        result,
                        Err(ContainerError::StageTimedOut { stage, timeout_secs: 1, .. })
                            if stage == final_stage
                    ),
                    "incomplete absence report must not prove absence: {result:?}"
                );
            } else {
                result.expect("final cleanup must still succeed");
            }
            if let InstanceId::CidFile(path) = &session.id {
                assert_eq!(path.exists(), mode == "held-delete");
            }
        }
    }
}

#[tokio::test]
async fn inherited_stderr_cannot_extend_async_cleanup_budget() {
    check_inherited_stderr(false).await;
}

#[tokio::test]
async fn inherited_stderr_cannot_extend_sync_cleanup_budget() {
    // Also exercise synchronous cleanup called from an existing runtime.
    check_inherited_stderr(true).await;
}

async fn check_unrelated_absence_phrases_fail(sync: bool) {
    for mode in ["misleading-pool", "misleading-project"] {
        for docker in [false, true] {
            let client = Client::new(mode);
            let session = client.session(docker);
            let result = if sync {
                teardown_sync(&session)
            } else {
                teardown(&session).await
            };
            let final_stage = if docker { "rm" } else { "delete" };
            assert!(
                matches!(result, Err(ContainerError::StageFailed { stage, .. }) if stage == final_stage),
                "{mode}, docker={docker}, sync={sync}: unrelated absence phrase must fail: {result:?}"
            );
            assert_eq!(
                std::fs::read_to_string(client.root.join("log")).unwrap(),
                format!("{}\n{final_stage}\n", if docker { "kill" } else { "stop" })
            );
            if let InstanceId::CidFile(path) = session.id {
                assert!(
                    path.exists(),
                    "failed rm must retain Docker cidfile evidence"
                );
            }
        }
    }
}

#[tokio::test]
async fn unrelated_absence_phrases_fail_teardown_and_retain_cidfiles() {
    check_unrelated_absence_phrases_fail(false).await;
    check_unrelated_absence_phrases_fail(true).await;
}

#[tokio::test]
async fn supported_absence_reports_still_complete_teardown() {
    for sync in [false, true] {
        for docker in [false, true] {
            let client = Client::new("absent");
            let session = client.session(docker);
            let result = if sync {
                teardown_sync(&session)
            } else {
                teardown(&session).await
            };
            result.expect("supported explicit absence report is success");
            if let InstanceId::CidFile(path) = session.id {
                assert!(!path.exists(), "confirmed absence may remove cidfile");
            }
        }
    }
}

#[tokio::test]
async fn teardown_clients_restore_home_for_awaited_and_sync_fallbacks() {
    const CHILD: &str = "QCG_TEARDOWN_HOME_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let root = std::env::temp_dir().join(format!("qcg-teardown-home-{}", uuid::Uuid::now_v7()));
        let home = root.join("home");
        std::fs::create_dir_all(&home).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "session::cleanup_stderr_tests::teardown_clients_restore_home_for_awaited_and_sync_fallbacks",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("HOME", &home)
            .output()
            .unwrap();
        let _ = std::fs::remove_dir_all(&root);
        assert!(
            output.status.success(),
            "isolated HOME child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let home = std::env::var("HOME").expect("isolated test HOME");
    let awaited = Client::new("home");
    teardown(&awaited.session(false)).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(awaited.root.join("home-seen")).unwrap(),
        format!("{home}\n{home}\n")
    );

    let sync = Client::new("home");
    teardown_sync(&sync.session(false)).unwrap();
    assert_eq!(
        std::fs::read_to_string(sync.root.join("home-seen")).unwrap(),
        format!("{home}\n{home}\n"),
        "synchronous worker runtime must restore HOME"
    );

    let fallback = Client::new("home");
    let _no_worker = NoCommandWorker::enter();
    teardown_sync(&fallback.session(false)).unwrap();
    assert_eq!(
        std::fs::read_to_string(fallback.root.join("home-seen")).unwrap(),
        format!("{home}\n{home}\n"),
        "thread-spawn fallback must restore HOME"
    );
}

async fn check_large_stderr(sync: bool) {
    for exit in [0, 1] {
        // 2 MiB exceeds pipe capacity. A discarded client-error prefix
        // must not turn the retained absence-looking suffix into proof.
        let script = format!(
            "echo 'socket error' >&2; /bin/dd if=/dev/zero bs=4096 count=512 1>&2 2>/dev/null; echo 'no such instance owned-id' >&2; exit {exit}"
        );
        let timeout = Duration::from_secs(2);
        let start = std::time::Instant::now();
        let result = if sync {
            run_sync_bounded("/bin/sh", &["-c", &script], timeout, "owned-id", "delete")
        } else {
            run_admin_timed(&["/bin/sh", "-c", &script], timeout, "owned-id", "delete").await
        };
        assert!(start.elapsed() < Duration::from_secs(3));
        if exit == 0 {
            result.expect("large stderr must not stall a healthy client");
        } else {
            let Err(ContainerError::StageFailed { detail, .. }) = result else {
                panic!("truncated output must not prove absence: {result:?}");
            };
            assert!(detail.contains("[stderr truncated]"), "{detail}");
            assert!(detail.ends_with("no such instance owned-id"));
            assert!(detail.len() < 1024, "diagnostics must remain bounded");
        }
    }
    for (script, absent) in [
        ("echo 'no such instance owned-id' >&2; exit 1", true),
        ("printf '\\377no such instance owned-id' >&2; exit 1", false),
    ] {
        let result = if sync {
            run_sync_bounded(
                "/bin/sh",
                &["-c", script],
                Duration::from_secs(2),
                "owned-id",
                "rm",
            )
        } else {
            run_admin_timed(
                &["/bin/sh", "-c", script],
                Duration::from_secs(2),
                "owned-id",
                "rm",
            )
            .await
        };
        assert_eq!(
            matches!(result, Err(ContainerError::InstanceAbsent { .. })),
            absent,
            "{result:?}"
        );
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn large_stderr_is_drained_concurrently_async() {
    check_large_stderr(false).await;
}

#[tokio::test]
async fn large_stderr_is_drained_concurrently_sync() {
    check_large_stderr(true).await;
}

// Injection is thread-local, so other concurrently running cleanup tests
// still exercise the real worker. The outer spawn is failed separately.
struct NoCommandWorker;

impl NoCommandWorker {
    fn enter() -> Self {
        assert!(!FAIL_TEARDOWN_WORKER_SPAWN.replace(true));
        Self
    }
}

impl Drop for NoCommandWorker {
    fn drop(&mut self) {
        FAIL_TEARDOWN_WORKER_SPAWN.set(false);
    }
}

#[tokio::test]
async fn inline_cleanup_attempts_both_commands_when_command_worker_cannot_spawn() {
    let _no_worker = NoCommandWorker::enter();
    for docker in [false, true] {
        for mode in ["success", "absent", "running"] {
            let client = Client::new(mode);
            let session = client.session(docker);
            let cidfile = match &session.id {
                InstanceId::CidFile(path) => Some(path.clone()),
                _ => None,
            };
            let config = client.root.join("config");
            std::fs::write(&config, "owned config").unwrap();
            let start = std::time::Instant::now();
            spawn_cleanup_with(session, Some(config.clone()), |_| false);
            assert!(start.elapsed() < Duration::from_secs(3), "{mode}");
            assert_eq!(
                std::fs::read_to_string(client.root.join("log"))
                    .unwrap_or_else(|error| panic!("{mode} docker={docker}: {error}")),
                if docker {
                    "kill\nrm\n"
                } else {
                    "stop\ndelete\n"
                },
                "{mode}: stop/kill and delete/rm must actually be attempted"
            );
            assert!(!config.exists());
            if let Some(path) = cidfile {
                assert_eq!(path.exists(), mode != "success", "{mode}: fail closed");
            }
        }
    }
    // In particular, stderr discarded by this fallback is not absence proof.
    let result = run_sync_bounded(
        "/bin/sh",
        &["-c", "echo 'no such instance owned-id' >&2; exit 1"],
        Duration::from_secs(1),
        "owned-id",
        "delete",
    );
    assert!(matches!(result, Err(ContainerError::StageFailed { .. })));
}

#[cfg(target_os = "linux")]
#[test]
fn sync_timeout_reaps_live_direct_child_before_runtime_is_destroyed() {
    // Plain sync test: there is no surrounding Tokio runtime that could
    // accidentally reap the child on behalf of the temporary worker runtime.
    for no_worker in [false, true] {
        let _injection = no_worker.then(NoCommandWorker::enter);
        for _ in 0..5 {
            let client = Client::new("running");
            let start = std::time::Instant::now();
            let result = run_sync_bounded(
                &client.binary,
                &["delete"],
                Duration::from_millis(100),
                "owned-id",
                "delete",
            );
            assert!(matches!(result, Err(ContainerError::StageTimedOut { .. })));
            assert!(start.elapsed() < Duration::from_secs(1));
            let pid = std::fs::read_to_string(client.root.join("direct-pids")).unwrap();
            assert!(
                !std::path::Path::new("/proc").join(pid.trim()).exists(),
                "direct child {pid} is still live or a zombie (no_worker={no_worker})"
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn retained_reaping_responsibility_survives_temporary_runtime_destruction() {
    // Exercise the exceptional handoff without needing an uninterruptible
    // OS process: deliberately retain a still-running client, then kill it
    // only after its spawning runtime no longer exists.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let child = runtime.block_on(async {
        tokio::process::Command::new("/bin/sleep")
            .arg("8")
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    });
    let pid = child.id().unwrap();
    retain_for_reaping(ReapChild::Async(child));
    drop(runtime);
    assert!(reap_pending_children() >= 1);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        runtime
            .block_on(await_outstanding_cleanups(Duration::ZERO))
            .outstanding
            >= 1,
        "a retained live client must not be reported as a clean shutdown"
    );
    assert!(
        std::process::Command::new("/bin/kill")
            .args(["-KILL", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
    runtime.block_on(await_outstanding_cleanups(Duration::from_secs(1)));
    assert!(
        !std::path::Path::new("/proc").join(pid.to_string()).exists(),
        "shutdown join must reap the retained client after its runtime was destroyed"
    );
}

#[tokio::test]
async fn inline_cleanup_stays_outstanding_until_fallback_finishes() {
    let Some(daemon) = super::daemon_policy_test_support::Daemon::isolated(
        "session::cleanup_stderr_tests::inline_cleanup_stays_outstanding_until_fallback_finishes",
    ) else {
        return;
    };
    use std::sync::{Arc, Barrier};

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let config = daemon.root.join("inline-config");
    std::fs::write(&config, "owned config").unwrap();
    let session = Session {
        timeouts: policy::ContainerTimeouts::default(),
        backend: Backend::Incus {
            binary: daemon.root.join("missing-client").to_str().unwrap().into(),
        },
        id: InstanceId::Name("owned-id".into()),
    };
    let worker = {
        let entered = entered.clone();
        let release = release.clone();
        let config = config.clone();
        std::thread::spawn(move || {
            INLINE_CLEANUP_HOOK.with_borrow_mut(|hook| {
                *hook = Some(Box::new(move || {
                    entered.wait();
                    release.wait();
                }));
            });
            spawn_cleanup_with(session, Some(config), |_| false);
        })
    };
    entered.wait();
    let blocked = await_outstanding_cleanups(Duration::ZERO).await;
    let config_still_owned = config.exists();
    release.wait();
    worker.join().unwrap();
    let finished = await_outstanding_cleanups(Duration::ZERO).await;

    assert_eq!(
        blocked.outstanding, 1,
        "inline fallback must remain counted"
    );
    assert_eq!(blocked.failed, 0);
    assert!(config_still_owned);
    assert!(!config.exists(), "config cleanup must precede completion");
    assert_eq!(finished.outstanding, 0);
    assert_eq!(
        finished.failed, 1,
        "failure reporting must precede completion"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn canceled_admin_retains_reaping_ownership_after_runtime_destruction() {
    let Some(_daemon) = super::daemon_policy_test_support::Daemon::isolated(
        "session::cleanup_stderr_tests::canceled_admin_retains_reaping_ownership_after_runtime_destruction",
    ) else {
        return;
    };
    // Isolate the global registry so no other cleanup can reap this child
    // between cancellation and the shutdown-accounting observation.
    let client = Client::new("running");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let binary = client.binary.clone();
        let cleanup = tokio::spawn(async move {
            run_admin_timed(
                &[&binary, "delete"],
                Duration::from_secs(10),
                "owned-id",
                "delete",
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !client.root.join("direct-pids").exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("cleanup must have a live direct child before abort");
        cleanup.abort();
        assert!(cleanup.await.unwrap_err().is_cancelled());
    });
    drop(runtime);

    let pid: u32 = std::fs::read_to_string(client.root.join("direct-pids"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    {
        let pending = PENDING_REAPS.lock().unwrap();
        assert_eq!(OUTSTANDING_CLEANUPS.load(Ordering::Acquire), 0);
        assert_eq!(pending.len(), 1, "canceled child must remain outstanding");
        let ReapChild::Async(child) = &pending[0] else {
            panic!("canceled admin must transfer its async child");
        };
        assert_eq!(child.id(), Some(pid));
    }
    let proc_path = std::path::Path::new("/proc").join(pid.to_string());
    assert!(proc_path.exists(), "retained child has not yet been reaped");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let joined = runtime.block_on(await_outstanding_cleanups(Duration::from_secs(1)));
    assert_eq!(joined.outstanding, 0);
    assert_eq!(joined.failed, 0);
    assert!(PENDING_REAPS.lock().unwrap().is_empty());
    assert!(
        !proc_path.exists(),
        "later shutdown join must reap the child"
    );
}

#[cfg(target_os = "linux")]
fn check_async_child_handoff(probe: bool) {
    // Each caller runs in an isolated process: registry observations must not
    // race unrelated cleanups, including those from earlier test runtimes.
    for end in ["abort", "cancel", "timeout"] {
        let client = Client::new("running");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let pid = runtime.block_on(async {
            let argv = vec![client.binary.clone(), "probe-or-init".into()];
            let cancel = CancellationToken::new();
            let task_cancel = cancel.clone();
            let timeout = if end == "timeout" {
                Duration::from_millis(250)
            } else {
                Duration::from_secs(10)
            };
            let task = tokio::spawn(async move {
                if probe {
                    wait_ready(&argv, timeout, &task_cancel, "owned-id").await
                } else {
                    run_admin(&argv, timeout, &task_cancel, "owned-id", "init")
                        .await
                        .map(|_| ())
                }
            });
            let pid = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if let Ok(pid) = std::fs::read_to_string(client.root.join("direct-pids"))
                        && let Ok(pid) = pid.trim().parse::<u32>()
                    {
                        break pid;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("must observe a direct child before ending its future");
            let start = std::time::Instant::now();
            match end {
                "abort" => {
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                }
                "cancel" => {
                    cancel.cancel();
                    assert!(matches!(task.await.unwrap(), Err(ContainerError::Canceled)));
                }
                _ => assert!(matches!(
                    task.await.unwrap(),
                    Err(ContainerError::StageTimedOut { stage, .. })
                        if stage == if probe { "wait-ready" } else { "init" }
                )),
            }
            assert!(start.elapsed() < Duration::from_secs(1), "{end}");
            pid
        });
        drop(runtime);
        {
            let pending = PENDING_REAPS.lock().unwrap();
            assert_eq!(
                pending.len(),
                1,
                "{end}: child must retain an explicit owner"
            );
            let ReapChild::Async(child) = &pending[0] else {
                panic!("{end}: expected an async direct CLI child");
            };
            assert_eq!(child.id(), Some(pid));
        }
        let proc_path = std::path::Path::new("/proc").join(pid.to_string());
        assert!(
            proc_path.exists(),
            "{end}: child must not be implicitly reaped"
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let joined = runtime.block_on(await_outstanding_cleanups(Duration::from_secs(1)));
        assert_eq!(
            joined,
            CleanupWaitOutcome {
                outstanding: 0,
                failed: 0
            }
        );
        assert!(PENDING_REAPS.lock().unwrap().is_empty());
        assert!(
            !proc_path.exists(),
            "{end}: shutdown join must reap the child"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn provision_admin_abort_cancel_and_timeout_retain_joinable_children() {
    let Some(_daemon) = super::daemon_policy_test_support::Daemon::isolated(
        "session::cleanup_stderr_tests::provision_admin_abort_cancel_and_timeout_retain_joinable_children",
    ) else {
        return;
    };
    check_async_child_handoff(false);
}

#[cfg(target_os = "linux")]
#[test]
fn readiness_abort_cancel_and_timeout_retain_joinable_children() {
    let Some(_daemon) = super::daemon_policy_test_support::Daemon::isolated(
        "session::cleanup_stderr_tests::readiness_abort_cancel_and_timeout_retain_joinable_children",
    ) else {
        return;
    };
    check_async_child_handoff(true);
}

#[tokio::test]
async fn provision_admin_drains_both_pipes_and_preserves_output_and_errors() {
    let Some(_daemon) = super::daemon_policy_test_support::Daemon::isolated(
        "session::cleanup_stderr_tests::provision_admin_drains_both_pipes_and_preserves_output_and_errors",
    ) else {
        return;
    };
    let cancel = CancellationToken::new();
    // More than a pipe buffer on each stream, interleaved: waiting before
    // draining either stream, or draining them sequentially, would deadlock.
    let script = "i=0; while [ $i -lt 4096 ]; do printf '%064d' 0; printf '%064d' 1 >&2; i=$((i+1)); done; echo output-end; echo error-end >&2";
    for failure in [false, true] {
        let argv = vec![
            "/bin/sh".into(),
            "-c".into(),
            format!("{script}; exit {}", if failure { 7 } else { 0 }),
        ];
        let result = run_admin(&argv, Duration::from_secs(5), &cancel, "owned-id", "init").await;
        if failure {
            let Err(ContainerError::StageFailed { stage, detail, .. }) = result else {
                panic!("must preserve a nonzero admin exit: {result:?}");
            };
            assert_eq!(stage, "init");
            assert!(detail.contains('7'));
            assert!(detail.ends_with("error-end"));
        } else {
            let output = result.unwrap();
            assert_eq!(
                output.stdout,
                format!("{}output-end", "0".repeat(64 * 4096))
            );
        }
    }
}

#[tokio::test]
async fn readiness_retries_on_cadence_without_extending_global_deadline() {
    let client = Client::new("success");
    let cancel = CancellationToken::new();
    let script = format!(
        "echo probe >> '{}'; exit 1",
        client.root.join("probes").display()
    );
    let argv = vec!["/bin/sh".into(), "-c".into(), script];
    for (timeout, expected_probes) in [(120, 1), (1120, 3)] {
        std::fs::write(client.root.join("probes"), "").unwrap();
        let start = std::time::Instant::now();
        let result = wait_ready(&argv, Duration::from_millis(timeout), &cancel, "owned-id").await;
        assert!(matches!(
            result,
            Err(ContainerError::StageTimedOut {
                stage: "wait-ready",
                ..
            })
        ));
        assert!(start.elapsed() >= Duration::from_millis(timeout));
        assert!(start.elapsed() < Duration::from_millis(timeout + 250));
        assert_eq!(
            std::fs::read_to_string(client.root.join("probes"))
                .unwrap()
                .lines()
                .count(),
            expected_probes
        );
    }
}
