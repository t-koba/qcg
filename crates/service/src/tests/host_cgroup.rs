//! Host-cgroup reconciliation before `Started` gates: recovery, deletion,
//! and retention GC all reconcile under exclusive ownership first.

use crate::summaries::run_meta_dir;
use camino::Utf8PathBuf;
use serde_json::json;

fn temp_meta(label: &str) -> (Utf8PathBuf, Utf8PathBuf) {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
        "qcg-svc-host-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7().as_simple()
    )))
    .unwrap();
    let meta = run_meta_dir(&root);
    std::fs::create_dir_all(meta.as_std_path()).unwrap();
    (root, meta)
}

fn write_started_journal(meta: &camino::Utf8Path, run_id: &str) {
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), run_id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
                "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
            }),
        )
        .unwrap();
    journal
        .event(
            "operation_started",
            json!({
                "node": "n", "kind": "command", "target": "echo",
                "operation_id": "op-1", "operation_digest": "d",
                "invocation_id": "call-1", "attempt": 1, "on_indeterminate": "fail",
            }),
        )
        .unwrap();
}

#[test]
fn disabled_reconcile_is_not_bound_and_started_gate_still_blocks() {
    let (_root, meta) = temp_meta("disabled");
    let config = policy::HostCgroupConfig::disabled();
    let outcome =
        engine::host_cgroup::reconcile_host_cgroup(&meta, &config, "run-recovery").unwrap();
    assert!(matches!(
        outcome,
        engine::host_cgroup::HostCgroupReconcile::NotBound
    ));
    write_started_journal(&meta, "run-disabled");
    let state = engine::RunState::fold_journal(&meta.join("journal.jsonl")).unwrap();
    assert!(state.ensure_no_inflight_operations().is_err());
    assert!(state.ensure_no_fail_closed_inflight_operations().is_err());
    let _ = std::fs::remove_dir_all(_root.as_std_path());
}

#[test]
fn tampered_binding_is_refused_before_started_gate() {
    let (_root, meta) = temp_meta("tampered");
    write_started_journal(&meta, "run-tampered");
    // Persist a binding that escapes its parent: reconcile must refuse
    // without touching any cgroup, preserving the journal.
    let binding = policy::HostCgroupBinding {
        version: policy::HOST_CGROUP_BINDING_VERSION,
        run_id: "run-tampered".into(),
        nonce: "a".repeat(32),
        delegated_parent: std::path::PathBuf::from("/sys/fs/cgroup/qcg.slice"),
        run_root: std::path::PathBuf::from("/sys/fs/cgroup/other/qcg-run-n"),
        boot_id: "boot".into(),
        prepared: false,
        cleaned: false,
        empty_confirmed: false,
    };
    let config = policy::HostCgroupConfig::disabled();
    for field in ["outside-parent", "same-parent-sibling", "run-id"] {
        let mut tampered = binding.clone();
        match field {
            "same-parent-sibling" => {
                tampered.run_root = tampered
                    .delegated_parent
                    .join(policy::run_root_name("sibling", &tampered.nonce));
            }
            "run-id" => {
                // Keep the root consistent with the record, but not the caller.
                tampered.run_id = "sibling".into();
                tampered.run_root = tampered
                    .delegated_parent
                    .join(policy::run_root_name(&tampered.run_id, &tampered.nonce));
            }
            _ => {}
        }
        std::fs::write(
            engine::host_cgroup::binding_path(&meta),
            serde_json::to_vec_pretty(&tampered).unwrap(),
        )
        .unwrap();
        let error =
            engine::host_cgroup::reconcile_host_cgroup(&meta, &config, "run-tampered").unwrap_err();
        assert!(
            matches!(error, engine::host_cgroup::HostCgroupError::Identity(_)),
            "{field}: {error}"
        );
        assert_eq!(
            engine::host_cgroup::load_host_binding(&meta)
                .unwrap()
                .unwrap(),
            tampered
        );
    }
    // The Started gate still blocks re-entry/deletion/GC independently.
    let state = engine::RunState::fold_journal(&meta.join("journal.jsonl")).unwrap();
    assert!(state.ensure_no_inflight_operations().is_err());
    // Binding is preserved for operator diagnosis.
    assert!(
        engine::host_cgroup::load_host_binding(&meta)
            .unwrap()
            .is_some()
    );
    let _ = std::fs::remove_dir_all(_root.as_std_path());
}

#[test]
fn foreign_boot_is_refused_and_preserves_binding() {
    let (_root, meta) = temp_meta("foreign");
    let binding = policy::HostCgroupBinding {
        version: policy::HOST_CGROUP_BINDING_VERSION,
        run_id: "run-foreign".into(),
        nonce: "a".repeat(32),
        delegated_parent: std::path::PathBuf::from("/sys/fs/cgroup/qcg.slice"),
        run_root: std::path::PathBuf::from(format!(
            "/sys/fs/cgroup/qcg.slice/qcg-run-foreign-{}",
            "a".repeat(32)
        )),
        boot_id: "definitely-not-the-current-boot".into(),
        prepared: false,
        cleaned: false,
        empty_confirmed: false,
    };
    std::fs::write(
        engine::host_cgroup::binding_path(&meta).as_std_path(),
        serde_json::to_string_pretty(&binding).unwrap(),
    )
    .unwrap();
    let config = policy::HostCgroupConfig::disabled();
    // On Linux this fails on boot mismatch; on other OS it fails as
    // Linux-only. Either way it refuses rather than targeting another cgroup.
    assert!(engine::host_cgroup::reconcile_host_cgroup(&meta, &config, "run-foreign").is_err());
    assert!(
        engine::host_cgroup::load_host_binding(&meta)
            .unwrap()
            .is_some()
    );
    let _ = std::fs::remove_dir_all(_root.as_std_path());
}

#[cfg(target_os = "linux")]
#[test]
fn recovery_cleans_and_delete_gate_passes_only_after() {
    if !engine::host_cgroup::host_cgroup_tests_enabled() {
        eprintln!("SKIP recovery: no writable delegation");
        return;
    }
    let (_root, meta) = temp_meta("recovery");
    let config = engine::host_cgroup::host_cgroup_config_from_env().unwrap();
    let scope = engine::host_cgroup::bind_host_cgroup(&config, &meta, "run-recovery")
        .unwrap()
        .unwrap();
    // One survivor in the root.
    let mut command = std::process::Command::new("/bin/sleep");
    command.arg("30");
    engine::host_cgroup::apply_scope_to_std_command(&mut command, &scope).unwrap();
    use std::os::unix::process::CommandExt as _;
    command.process_group(0);
    command.stdin(std::process::Stdio::null());
    command.stdout(std::process::Stdio::null());
    command.stderr(std::process::Stdio::null());
    let mut child = command.spawn().unwrap();
    let pid = child.id();
    let start = std::time::Instant::now();
    let mut placed = false;
    while start.elapsed() < std::time::Duration::from_secs(5) {
        if let Ok(contents) = std::fs::read_to_string(scope.run_root.join("cgroup.procs"))
            && contents.lines().any(|line| line.trim() == pid.to_string())
        {
            placed = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(placed, "child should be in run root");
    // Recovery must kill and mark cleaned; second call is idempotent.
    let outcome =
        engine::host_cgroup::reconcile_host_cgroup(&meta, &config, "run-recovery").unwrap();
    assert!(matches!(
        outcome,
        engine::host_cgroup::HostCgroupReconcile::Cleaned
    ));
    assert!(matches!(
        engine::host_cgroup::reconcile_host_cgroup(&meta, &config, "run-recovery").unwrap(),
        engine::host_cgroup::HostCgroupReconcile::AlreadyCleaned
    ));
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(_root.as_std_path());
    let _ = std::fs::remove_dir(&scope.run_root);
}

#[test]
fn host_cgroup_incomplete_admission_never_wipes_binding_evidence() {
    for journal in [None, Some(""), Some("partial")] {
        let (root, meta) = temp_meta("admission");
        let path = engine::host_cgroup::binding_path(&meta);
        std::fs::write(&path, b"ambiguous-binding").unwrap();
        if let Some(journal) = journal {
            std::fs::write(meta.join("journal.jsonl"), journal).unwrap();
        }
        let _admission = crate::run_dirs::try_lock_run_admission(&root)
            .unwrap()
            .unwrap();
        assert!(crate::run_dirs::try_adopt_run_dir_with_snapshot(&root, "run").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"ambiguous-binding");
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn host_cgroup_replay_requires_execution_ownership_before_reading_binding() {
    use crate::tests::support::{TempGuard, temp_run_dir, test_service};
    let root = temp_run_dir("host-replay-lock");
    let _guard = TempGuard(root.clone());
    let runs = root.join("runs");
    let service = test_service(vec![], runs.clone());
    let dir = runs.join("source");
    let meta = run_meta_dir(&dir);
    std::fs::create_dir_all(&meta).unwrap();
    write_started_journal(&meta, "source");
    let path = engine::host_cgroup::binding_path(&meta);
    std::fs::write(&path, b"must not read before lease").unwrap();
    let execution = crate::run_dirs::try_lock_run_execution(&dir)
        .unwrap()
        .unwrap();
    let error = service.reconcile_replay_source("source").await.unwrap_err();
    assert!(error.to_string().contains("busy"), "{error}");
    assert_eq!(std::fs::read(&path).unwrap(), b"must not read before lease");
    drop(execution);
    // Now the malformed binding (rather than a live owner's tree) is examined.
    let error = service.reconcile_replay_source("source").await.unwrap_err();
    assert!(!error.to_string().contains("busy"), "{error}");
    assert!(path.exists());
}

#[cfg(target_os = "linux")]
fn wait_for_child_placement(scope: &policy::HostCgroupScope, pid: u32) {
    let start = std::time::Instant::now();
    while start.elapsed() < std::time::Duration::from_secs(5) {
        if std::fs::read_to_string(scope.run_root.join("cgroup.procs"))
            .is_ok_and(|contents| contents.lines().any(|line| line.trim() == pid.to_string()))
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!("child {pid} did not enter {}", scope.run_root.display());
}

#[cfg(target_os = "linux")]
fn wait_for_child_exit(child: &mut std::process::Child) {
    let start = std::time::Instant::now();
    while start.elapsed() < std::time::Duration::from_secs(5) {
        if child.try_wait().unwrap().is_some() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let _ = child.kill();
    panic!("cgroup cleanup did not terminate the child");
}

// Synthetic Prepared evidence exercises the service's refusal boundary; the
// engine tests separately crash during real creation before Bound publication.
#[cfg(target_os = "linux")]
fn mark_prepared(meta: &camino::Utf8Path) -> Vec<u8> {
    let path = engine::host_cgroup::binding_path(meta);
    let bound = std::fs::read(&path).unwrap();
    let mut binding = engine::host_cgroup::load_host_binding(meta)
        .unwrap()
        .unwrap();
    binding.prepared = true;
    std::fs::write(path, serde_json::to_vec(&binding).unwrap()).unwrap();
    bound
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn delete_reconciles_host_tree_before_retaining_started_journal() {
    use crate::tests::support::{TempGuard, temp_run_dir, test_service};
    if !engine::host_cgroup::host_cgroup_tests_enabled() {
        eprintln!("SKIP delete reconciliation: no writable delegation");
        return;
    }
    let root = temp_run_dir("host-cgroup-delete");
    let _guard = TempGuard(root.clone());
    let runs = root.join("runs");
    let service = test_service(vec![], runs.clone());
    let id = "delete-started";
    let run_dir = runs.join(id);
    let meta = run_meta_dir(&run_dir);
    std::fs::create_dir_all(&meta).unwrap();
    write_started_journal(&meta, id);

    let config = engine::host_cgroup::host_cgroup_config_from_env().unwrap();
    let scope = engine::host_cgroup::bind_host_cgroup(&config, &meta, id)
        .unwrap()
        .unwrap();
    let mut command = std::process::Command::new("/bin/sleep");
    command.arg("30");
    engine::host_cgroup::apply_scope_to_std_command(&mut command, &scope).unwrap();
    let mut child = command.spawn().unwrap();
    wait_for_child_placement(&scope, child.id());

    let bound = mark_prepared(&meta);
    let path = engine::host_cgroup::binding_path(&meta);
    let prepared = std::fs::read(&path).unwrap();
    let error = service.delete_run(id).await.unwrap_err();
    assert!(error.to_string().contains("ownership unproven"), "{error}");
    assert_eq!(std::fs::read(&path).unwrap(), prepared);
    assert!(child.try_wait().unwrap().is_none());
    assert!(scope.run_root.is_dir() && run_dir.exists());
    // Restore the test's known Bound record to exercise ordinary retirement.
    std::fs::write(&path, bound).unwrap();

    // Deletion reconciles external processes first, then preserves the
    // unresolved operation journal under the existing Started policy.
    assert!(service.delete_run(id).await.is_err());
    wait_for_child_exit(&mut child);
    assert!(run_dir.exists(), "Started evidence must remain");
    assert!(
        engine::host_cgroup::load_host_binding(&meta)
            .unwrap()
            .unwrap()
            .cleaned,
        "cgroup must be retired before the Started gate refuses deletion"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn retention_gc_reconciles_host_tree_before_removing_terminal_run() {
    if !engine::host_cgroup::host_cgroup_tests_enabled() {
        eprintln!("SKIP retention reconciliation: no writable delegation");
        return;
    }
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
        "qcg-svc-host-gc-{}-{}",
        std::process::id(),
        uuid::Uuid::now_v7().as_simple()
    )))
    .unwrap();
    let _guard = crate::tests::support::TempGuard(root.clone());
    let runs = root.join("runs");
    let id = "finished-host";
    let run_dir = runs.join(id);
    let meta = run_meta_dir(&run_dir);
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
                "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
            }),
        )
        .unwrap();
    journal
        .event("run_finished", json!({"status": "success"}))
        .unwrap();

    let config = engine::host_cgroup::host_cgroup_config_from_env().unwrap();
    let scope = engine::host_cgroup::bind_host_cgroup(&config, &meta, id)
        .unwrap()
        .unwrap();
    let mut command = std::process::Command::new("/bin/sleep");
    command.arg("30");
    engine::host_cgroup::apply_scope_to_std_command(&mut command, &scope).unwrap();
    let mut child = command.spawn().unwrap();
    wait_for_child_placement(&scope, child.id());

    let bound = mark_prepared(&meta);
    let path = engine::host_cgroup::binding_path(&meta);
    let prepared = std::fs::read(&path).unwrap();
    let selected = crate::summaries::gc_run_directories(&runs, 0, 0, true, 100).unwrap();
    assert!(
        selected.is_empty(),
        "an ambiguous Prepared run must not be selected"
    );
    assert_eq!(std::fs::read(&path).unwrap(), prepared);
    assert!(child.try_wait().unwrap().is_none());
    assert!(scope.run_root.is_dir() && run_dir.exists());
    std::fs::write(&path, bound).unwrap();

    let deleted = crate::summaries::gc_run_directories(&runs, 0, 0, true, 100).unwrap();
    assert_eq!(deleted, vec![run_dir.clone()]);
    wait_for_child_exit(&mut child);
    assert!(
        !run_dir.exists(),
        "terminal run should be collected after cleanup"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn retention_gc_preview_leaves_selected_and_retained_host_trees_untouched() {
    if !engine::host_cgroup::host_cgroup_tests_enabled() {
        eprintln!("SKIP retention preview: no writable delegation");
        return;
    }
    // Also clean up on assertion failure: dropping Child alone does not kill it.
    struct ScopedChild {
        scope: policy::HostCgroupScope,
        child: Option<std::process::Child>,
    }
    impl Drop for ScopedChild {
        fn drop(&mut self) {
            if self.scope.run_root.exists() {
                let _ = engine::host_cgroup::cleanup_scope(&self.scope);
            }
            if let Some(child) = &mut self.child {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    let runs = crate::tests::support::temp_run_dir("host-cgroup-gc-preview");
    let _guard = crate::tests::support::TempGuard(runs.clone());
    let config = engine::host_cgroup::host_cgroup_config_from_env().unwrap();
    let mut children = Vec::new();
    let mut bindings = Vec::new();
    // Separate success/failed budgets make selection deterministic without
    // depending on journal timestamp resolution or directory iteration order.
    for (id, status) in [("selected", "success"), ("retained", "failed")] {
        let meta = run_meta_dir(&runs.join(id));
        std::fs::create_dir_all(&meta).unwrap();
        let journal =
            engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
        journal
            .event(
                "run_started",
                json!({
                    "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
                    "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
                    "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
                }),
            )
            .unwrap();
        journal
            .event("run_finished", json!({"status": status}))
            .unwrap();
        let scope = engine::host_cgroup::bind_host_cgroup(&config, &meta, id)
            .unwrap()
            .unwrap();
        let mut scoped = ScopedChild { scope, child: None };
        let mut command = std::process::Command::new("/bin/sleep");
        command.arg("30");
        engine::host_cgroup::apply_scope_to_std_command(&mut command, &scoped.scope).unwrap();
        scoped.child = Some(command.spawn().unwrap());
        wait_for_child_placement(&scoped.scope, scoped.child.as_ref().unwrap().id());
        let binding_path = engine::host_cgroup::binding_path(&meta);
        bindings.push((binding_path.clone(), std::fs::read(binding_path).unwrap()));
        children.push(scoped);
    }

    let selected = crate::summaries::gc_run_directories(&runs, 0, 1, false, 100).unwrap();
    assert_eq!(selected, vec![runs.join("selected")]);
    for (scoped, (path, before)) in children.iter_mut().zip(&bindings) {
        assert!(scoped.child.as_mut().unwrap().try_wait().unwrap().is_none());
        assert!(scoped.scope.run_root.is_dir(), "preview removed a cgroup");
        assert!(path.parent().unwrap().is_dir(), "preview removed a run");
        assert_eq!(std::fs::read(path).unwrap(), *before);
    }

    // Explicit teardown: kill the whole scope, remove its cgroups, and reap
    // the child before TempGuard removes the run metadata and bindings.
    for scoped in &mut children {
        engine::host_cgroup::cleanup_scope(&scoped.scope).unwrap();
        wait_for_child_exit(scoped.child.as_mut().unwrap());
    }
}
