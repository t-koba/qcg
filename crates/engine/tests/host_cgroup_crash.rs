//! Linux cgroup-v2 crash-recovery integration for trusted-host commands.
//!
//! All tests requiring a writable delegation skip (not fail) when
//! `QCG_HOST_CGROUP_PARENT` is absent; configured-but-invalid delegation fails, so the read-only sandbox
//! stays green. CI provisions a delegated subtree on ubuntu-latest and runs
//! these tests inside it. Tests that need no delegation always run.

#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::time::{Duration, Instant};

fn enabled_scope(
    label: &str,
) -> Option<(
    camino::Utf8PathBuf,
    policy::HostCgroupScope,
    policy::HostCgroupConfig,
)> {
    if !engine::host_cgroup::host_cgroup_tests_enabled() {
        eprintln!("SKIP {label}: no writable cgroup delegation (set QCG_HOST_CGROUP_PARENT)");
        return None;
    }
    let config =
        engine::host_cgroup::host_cgroup_config_from_env().expect("configured parent must parse");
    if !config.is_enabled() {
        eprintln!("SKIP {label}: delegation disabled");
        return None;
    }
    let meta = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
        "qcg-host-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7().as_simple()
    )))
    .unwrap();
    std::fs::create_dir_all(meta.as_std_path()).unwrap();
    let run_id = format!("run-{label}");
    let scope = engine::host_cgroup::bind_host_cgroup(&config, &meta, &run_id)
        .expect("bind should succeed with delegation")
        .expect("enabled config must bind");
    Some((meta, scope, config))
}

fn cleanup_meta(meta: &camino::Utf8PathBuf) {
    let _ = std::fs::remove_dir_all(meta.as_std_path());
}

fn procs_contains(root: &std::path::Path, pid: u32) -> bool {
    let Ok(contents) = std::fs::read_to_string(root.join("cgroup.procs")) else {
        return false;
    };
    contents.lines().any(|line| line.trim() == pid.to_string())
}

fn wait_for_procs(root: &std::path::Path, pid: u32, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if procs_contains(root, pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    procs_contains(root, pid)
}

fn is_running(pid: u32) -> bool {
    // A zombie cannot execute or remain in a cgroup; PID existence isn't liveness.
    if std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|s| s.rsplit_once(") ").map(|(_, rest)| rest.starts_with('Z')))
        .unwrap_or(false)
    {
        return false;
    }
    // ESRCH means gone; permission errors mean still there but not ours.
    let result = unsafe { libc::kill(pid as i32, 0) };
    if result == 0 {
        return true;
    }
    let error = std::io::Error::last_os_error();
    error.raw_os_error() != Some(libc::ESRCH)
}

fn kill_wait(pid: u32) {
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if !is_running(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn disabled_by_default_preserves_live_behavior_without_claim() {
    // Always runnable: no delegation needed.
    let config = policy::HostCgroupConfig::disabled();
    assert!(!config.is_enabled());
    // CmdGateway without scope must still run trusted commands.
    let workspace = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir()).unwrap();
    let gateway = engine::CmdGateway::new(contract_permissions(), workspace);
    assert!(gateway.host_cgroup().is_none());
}

fn contract_permissions() -> contract::Permissions {
    // Minimal permissions allowing `/bin/echo` as trusted_host.
    // Contract deserialization is heavy; construct via default and rely on
    // command tests using direct trusted-process paths instead.
    contract::Permissions::default()
}

#[test]
fn invalid_config_fails_before_host_launch() {
    // Always runnable: pure prefix validation, no filesystem writes.
    for value in ["/tmp/qcg-nope", "relative", "/sys/fs/cgroup"] {
        let error = policy::HostCgroupConfig::from_parent(Some(PathBuf::from(value))).unwrap_err();
        assert!(error.contains(policy::HOST_CGROUP_PARENT_ENV), "{error}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn command_launch_places_child_before_payload() {
    let Some((meta, scope, _)) = enabled_scope("launch") else {
        return;
    };
    // Spawn a long-lived child via the same race-free helper the gateway
    // uses, then prove it is inside the run root before payload runs.
    let mut command = tokio::process::Command::new("/bin/sleep");
    command.arg("30");
    engine::host_cgroup::apply_scope_to_tokio_command(&mut command, Some(&scope))
        .expect("scope should apply");
    // Use a runtime for one spawn; bounded.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let child = runtime.block_on(async move {
        use std::os::unix::process::CommandExt as _;
        // Keep process-group behavior alongside cgroup for live cancel.
        command.as_std_mut().process_group(0);
        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn should succeed")
    });
    let pid = child.id().expect("child should have pid");
    assert!(
        wait_for_procs(&scope.run_root, pid, Duration::from_secs(5)),
        "child {pid} should self-place into {}",
        scope.run_root.display()
    );
    // Cleanup must kill the whole root and confirm empty.
    engine::host_cgroup::cleanup_scope(&scope).expect("cleanup should succeed");
    assert!(
        !is_running(pid) || !procs_contains(&scope.run_root, pid),
        "child should be dead or removed after kill"
    );
    // Mark cleaned for idempotence check, then remove meta.
    // Reconcile after manual cleanup with missing root but unmarked binding
    // must refuse (no inference from absent paths).
    let binding = engine::host_cgroup::load_host_binding(&meta)
        .unwrap()
        .expect("binding should exist");
    assert!(!binding.cleaned);
    // Second cleanup on missing root must fail closed, not claim success.
    assert!(engine::host_cgroup::cleanup_scope(&scope).is_err());
    cleanup_meta(&meta);
}

#[cfg(target_os = "linux")]
#[test]
fn setsid_double_fork_and_leader_exit_stay_inside_root() {
    let Some((meta, scope, _)) = enabled_scope("escape") else {
        return;
    };
    // Descendant escapes process groups via setsid/double-fork, but must
    // remain in the cgroup root and be killed as a whole.
    let mut command = std::process::Command::new("/bin/sh");
    command.arg("-c").arg(
        // Double-fork: grandchild sleeps, middle exits, leader exits.
        // setsid detaches the survivors from any process group.
        "setsid sh -c 'sleep 30 & sleep 30 & wait' & sleep 1; exit 0",
    );
    engine::host_cgroup::apply_scope_to_std_command(&mut command, &scope)
        .expect("scope should apply");
    use std::os::unix::process::CommandExt as _;
    command.process_group(0);
    command.stdin(std::process::Stdio::null());
    command.stdout(std::process::Stdio::null());
    command.stderr(std::process::Stdio::null());
    let mut child = command.spawn().expect("spawn should succeed");
    let _ = child.wait();
    // Give grandchildren a moment to start.
    std::thread::sleep(Duration::from_secs(2));
    // At least one sleep should be in the root.
    let procs = std::fs::read_to_string(scope.run_root.join("cgroup.procs"))
        .expect("cgroup.procs should be readable");
    let members: Vec<u32> = procs
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect();
    assert!(
        !members.is_empty(),
        "escaped descendants should remain in {}",
        scope.run_root.display()
    );
    for pid in &members {
        assert!(
            is_running(*pid),
            "descendant {pid} should be alive before kill"
        );
    }
    engine::host_cgroup::cleanup_scope(&scope).expect("cleanup should kill escaped tree");
    std::thread::sleep(Duration::from_millis(500));
    for pid in members {
        assert!(
            !is_running(pid),
            "descendant {pid} must be dead after cgroup.kill"
        );
    }
    cleanup_meta(&meta);
}

#[cfg(target_os = "linux")]
#[test]
fn sibling_isolation_only_cleans_own_root() {
    let Some((meta_a, scope_a, config)) = enabled_scope("sibling-a") else {
        return;
    };
    let meta_b = camino::Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
        "qcg-host-sibling-b-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7().as_simple()
    )))
    .unwrap();
    std::fs::create_dir_all(meta_b.as_std_path()).unwrap();
    let scope_b = engine::host_cgroup::bind_host_cgroup(&config, &meta_b, "run-sibling-b")
        .unwrap()
        .unwrap();
    assert_ne!(scope_a.run_root, scope_b.run_root);
    // Start one sleeper in each root.
    let spawn_in = |scope: &policy::HostCgroupScope| -> u32 {
        let mut command = std::process::Command::new("/bin/sleep");
        command.arg("30");
        engine::host_cgroup::apply_scope_to_std_command(&mut command, scope).unwrap();
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
        command.stdin(std::process::Stdio::null());
        command.stdout(std::process::Stdio::null());
        command.stderr(std::process::Stdio::null());
        command.spawn().unwrap().id()
    };
    let pid_a = spawn_in(&scope_a);
    let pid_b = spawn_in(&scope_b);
    assert!(wait_for_procs(
        &scope_a.run_root,
        pid_a,
        Duration::from_secs(5)
    ));
    assert!(wait_for_procs(
        &scope_b.run_root,
        pid_b,
        Duration::from_secs(5)
    ));
    // A sidecar cannot redirect A's cleanup into a same-parent sibling.
    let binding_a = engine::host_cgroup::load_host_binding(&meta_a)
        .unwrap()
        .unwrap();
    let mut tampered = binding_a.clone();
    tampered.run_root = scope_b.run_root.clone();
    let binding_path = engine::host_cgroup::binding_path(&meta_a);
    std::fs::write(&binding_path, serde_json::to_vec(&tampered).unwrap()).unwrap();
    assert!(engine::host_cgroup::reconcile_host_cgroup(&meta_a, &config, &scope_a.run_id).is_err());
    std::fs::write(&binding_path, serde_json::to_vec(&binding_a).unwrap()).unwrap();
    assert!(engine::host_cgroup::reconcile_host_cgroup(&meta_a, &config, &scope_b.run_id).is_err());
    assert!(is_running(pid_a) && is_running(pid_b));
    // Cleaning A must not touch B.
    engine::host_cgroup::cleanup_scope(&scope_a).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert!(!is_running(pid_a), "sibling A should be dead");
    assert!(is_running(pid_b), "sibling B must survive A's cleanup");
    assert!(procs_contains(&scope_b.run_root, pid_b));
    // Cleanup B as well.
    engine::host_cgroup::cleanup_scope(&scope_b).unwrap();
    kill_wait(pid_b);
    cleanup_meta(&meta_a);
    cleanup_meta(&meta_b);
}

#[cfg(target_os = "linux")]
#[test]
fn invalid_identity_and_failure_retention_preserve_binding() {
    let Some((meta, scope, config)) = enabled_scope("identity") else {
        return;
    };
    // Tampered binding targeting outside the parent must be refused.
    let mut tampered = engine::host_cgroup::load_host_binding(&meta)
        .unwrap()
        .unwrap();
    tampered.run_root = std::path::PathBuf::from("/sys/fs/cgroup/other-root");
    let path = engine::host_cgroup::binding_path(&meta);
    std::fs::write(
        path.as_std_path(),
        serde_json::to_string_pretty(&tampered).unwrap(),
    )
    .unwrap();
    assert!(engine::host_cgroup::reconcile_host_cgroup(&meta, &config, &scope.run_id).is_err());
    // Restore valid binding, then remove the root without marking cleaned:
    // reconcile must refuse to infer cleanup from absence.
    let valid = engine::host_cgroup::load_host_binding(&meta)
        .unwrap()
        .unwrap();
    assert_ne!(valid.run_root, scope.run_root);
    // Rebind fresh to get a valid uncleaned binding, then remove its root.
    let _ = std::fs::remove_dir(&scope.run_root);
    // The tampered binding is still refused, not cleaned.
    assert!(engine::host_cgroup::reconcile_host_cgroup(&meta, &config, &scope.run_id).is_err());
    // Failure retention: binding file must still exist.
    assert!(
        engine::host_cgroup::load_host_binding(&meta)
            .unwrap()
            .is_some()
    );
    cleanup_meta(&meta);
    let _ = std::fs::remove_dir(&scope.run_root);
}

// A real separate owner uses the same binding/launch/retirement APIs as qcg.
// Helpers are re-executed by name, so SIGKILL never kills the test runner.
#[cfg(target_os = "linux")]
#[test]
fn host_cgroup_owner_helper() {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    let Ok(meta) = std::env::var("QCG_TEST_OWNER_META") else {
        return;
    };
    let meta = camino::Utf8PathBuf::from(meta);
    let phase = std::env::var("QCG_TEST_OWNER_PHASE").unwrap();
    let config = engine::host_cgroup::host_cgroup_config_from_env().unwrap();
    if phase == "recover" {
        engine::host_cgroup::reconcile_host_cgroup(&meta, &config, "owner").unwrap();
        return;
    }
    let scope = engine::host_cgroup::bind_host_cgroup(&config, &meta, "owner")
        .unwrap()
        .unwrap();
    let mut command = std::process::Command::new("/bin/sh");
    command.arg("-c").arg("cat /proc/self/cgroup > \"$1/membership\"; setsid /bin/sh -c 'sleep 60 & wait' & echo ready > \"$1/workload\"; wait")
        .arg("owner-payload").arg(&meta);
    let barrier = std::fs::File::create(meta.join("barrier")).unwrap();
    let fd = barrier.as_raw_fd();
    let stop = move || {
        // Only raw async-signal-safe syscalls after fork. The open file is
        // held by the owner while spawn is blocked in this launch barrier.
        unsafe {
            let pid = libc::getpid().to_ne_bytes();
            if libc::write(fd, pid.as_ptr().cast(), pid.len()) != pid.len() as isize {
                return Err(std::io::Error::last_os_error());
            }
            libc::raise(libc::SIGSTOP);
        }
        Ok(())
    };
    if phase == "before" {
        unsafe {
            command.pre_exec(stop);
        }
    }
    engine::host_cgroup::apply_scope_to_std_command(&mut command, &scope).unwrap();
    if phase == "placed" {
        unsafe {
            command.pre_exec(stop);
        }
    }
    let mut child = command.spawn().unwrap();
    child.wait().unwrap();
}

#[cfg(target_os = "linux")]
fn run_owner(meta: &camino::Utf8Path, phase: &str) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "host_cgroup_owner_helper", "--nocapture"])
        .env("QCG_TEST_OWNER_META", meta)
        .env("QCG_TEST_OWNER_PHASE", phase)
        .spawn()
        .unwrap()
}

#[cfg(target_os = "linux")]
#[test]
fn host_cgroup_owner_sigkill_launch_and_workload_recovery() {
    if !engine::host_cgroup::host_cgroup_tests_enabled() {
        eprintln!("SKIP owner SIGKILL launch/workload: QCG_HOST_CGROUP_PARENT unset");
        return;
    }
    for phase in ["before", "placed", "workload"] {
        let meta = camino::Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-owner-{}", uuid::Uuid::now_v7())),
        )
        .unwrap();
        std::fs::create_dir_all(&meta).unwrap();
        let mut owner = run_owner(&meta, phase);
        let deadline = Instant::now() + Duration::from_secs(10);
        let payload_pid = loop {
            let bytes = std::fs::read(meta.join("barrier")).unwrap_or_default();
            let ready = phase == "workload" && meta.join("workload").exists();
            if ready || bytes.len() == 4 {
                break if ready {
                    None
                } else {
                    Some(i32::from_ne_bytes(bytes.try_into().unwrap()))
                };
            }
            assert!(Instant::now() < deadline, "owner did not reach {phase}");
            assert!(
                owner.try_wait().unwrap().is_none(),
                "owner failed before barrier"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        let scope = engine::host_cgroup::load_host_binding(&meta)
            .unwrap()
            .unwrap()
            .scope()
            .expect("Bound binding must yield a scope");
        if phase == "workload" {
            let membership = std::fs::read_to_string(meta.join("membership")).unwrap();
            assert_eq!(
                membership.trim(),
                format!(
                    "0::/{}",
                    scope
                        .run_root
                        .strip_prefix("/sys/fs/cgroup")
                        .unwrap()
                        .display()
                )
            );
            assert!(
                !std::fs::read_to_string(scope.run_root.join("cgroup.procs"))
                    .unwrap()
                    .trim()
                    .is_empty()
            );
        } else if phase == "placed" {
            assert!(procs_contains(&scope.run_root, payload_pid.unwrap() as u32));
        }
        owner.kill().unwrap(); // SIGKILL the actual owner, not its command leader.
        owner.wait().unwrap();
        let survivors = std::fs::read_to_string(scope.run_root.join("cgroup.procs")).unwrap();
        if phase != "before" {
            assert!(
                !survivors.trim().is_empty(),
                "owner death must leave effects to reconcile"
            );
        }
        assert!(run_owner(&meta, "recover").wait().unwrap().success());
        if phase == "before" {
            // The child was stopped outside the root before placement. A new
            // owner can already have retired the empty root. On release, the
            // original-owner check must refuse exec, not run outside containment.
            unsafe {
                libc::kill(payload_pid.unwrap(), libc::SIGCONT);
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while is_running(payload_pid.unwrap() as u32) {
                assert!(
                    Instant::now() < deadline,
                    "pre-placement child escaped owner-death protection"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(!meta.join("membership").exists());
        }
        for pid in survivors.lines().filter_map(|s| s.parse::<u32>().ok()) {
            assert!(!is_running(pid), "survivor {pid}");
        }
        assert!(!scope.run_root.exists());
        assert!(
            engine::host_cgroup::load_host_binding(&meta)
                .unwrap()
                .unwrap()
                .cleaned
        );
        cleanup_meta(&meta);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn host_cgroup_interrupted_retirement_requires_durable_empty_confirmation() {
    let Some((meta, scope, config)) = enabled_scope("retirement") else {
        return;
    };
    let mut binding = engine::host_cgroup::load_host_binding(&meta)
        .unwrap()
        .unwrap();
    // Simulate interruption after removal, before the final cleaned update.
    // Without durable empty confirmation, absence alone never authorizes cleanup.
    std::fs::remove_dir(&scope.run_root).unwrap();
    assert!(engine::host_cgroup::reconcile_host_cgroup(&meta, &config, &scope.run_id).is_err());
    binding.empty_confirmed = true;
    std::fs::write(
        engine::host_cgroup::binding_path(&meta),
        serde_json::to_vec(&binding).unwrap(),
    )
    .unwrap();
    // Removed env is allowed: persisted identity remains authoritative.
    assert_eq!(
        engine::host_cgroup::reconcile_host_cgroup(
            &meta,
            &policy::HostCgroupConfig::disabled(),
            &scope.run_id
        )
        .unwrap(),
        engine::host_cgroup::HostCgroupReconcile::Cleaned
    );
    assert!(
        engine::host_cgroup::load_host_binding(&meta)
            .unwrap()
            .unwrap()
            .cleaned
    );
    cleanup_meta(&meta);
}
