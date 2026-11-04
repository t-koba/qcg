use super::support::*;
use crate::*;
use camino::Utf8Path;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn gc_service(runs: &Utf8Path, interval_secs: u64) -> LocalService {
    LocalService::with_generator_roots_policy_and_store_mode(
        Vec::new(),
        runs.to_owned(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy {
            gc_keep: 0,
            gc_keep_failed: 0,
            gc_interval_secs: interval_secs,
            ..ServiceDeploymentPolicy::default()
        },
    )
    .unwrap()
}

fn write_finished_run(runs: &Utf8Path, id: &str) {
    let meta = runs.join(id).join("meta");
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
}

/// Yields until `target` passes have started; panics loudly instead of
/// hanging if the production path stalls.
async fn wait_for_passes(counter: &AtomicUsize, target: usize) {
    for _ in 0..100_000 {
        if counter.load(Ordering::SeqCst) >= target {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for {target} retention passes");
}

/// Yields until the collector's blocking work deletes `path`; the pass
/// counter increments at sweep entry, so filesystem assertions need this extra
/// settle wait.
async fn wait_for_deletion(path: &Utf8Path) {
    for _ in 0..100_000 {
        if !path.exists() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for {path} to be collected");
}

/// Lets an already-due immediate sweep run: with the fix the schedule is
/// parked on its timer and nothing runs here.
async fn settle() {
    for _ in 0..100_000 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn retention_gc_runs_startup_sweep_then_waits_full_interval() {
    let root = temp_run_dir("retention-gc-schedule");
    let _guard = TempGuard(root.clone());
    let runs = root.join("runs");
    let service = gc_service(&runs, 60);
    write_finished_run(&runs, "probe-startup");
    let _test_lock = crate::retention_gc_test_hooks::TEST_LOCK.lock().await;
    let passes = &crate::retention_gc_test_hooks::PASSES;
    passes.store(0, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let task = service.start_retention_gc(shutdown.clone()).unwrap();
    // The shutdown-selectable startup sweep runs immediately, without
    // advancing time, and does real work.
    wait_for_passes(passes, 1).await;
    wait_for_deletion(&runs.join("probe-startup")).await;
    assert!(
        !runs.join("probe-startup").exists(),
        "startup sweep deletes over-retention terminal runs"
    );
    // Let the task park on its tick before moving virtual time, so the
    // interval base stays at startup.
    settle().await;
    // The consumed immediate tick means no second sweep before the
    // configured interval; without it this observes two startup sweeps.
    tokio::time::advance(Duration::from_secs(59)).await;
    settle().await;
    assert_eq!(
        passes.load(Ordering::SeqCst),
        1,
        "no second retention sweep before gc_interval_secs"
    );
    // The next sweep fires once the interval elapses.
    write_finished_run(&runs, "probe-periodic");
    tokio::time::advance(Duration::from_secs(1)).await;
    wait_for_passes(passes, 2).await;
    wait_for_deletion(&runs.join("probe-periodic")).await;
    assert!(
        !runs.join("probe-periodic").exists(),
        "interval sweep deletes over-retention terminal runs"
    );
    shutdown.cancel();
    task.await
        .expect("gc task joins")
        .expect("clean shutdown ends the schedule as Ok");
}

#[tokio::test(start_paused = true)]
async fn retention_gc_persistent_failure_ends_task_with_error() {
    let _test_lock = crate::retention_gc_test_hooks::TEST_LOCK.lock().await;
    crate::retention_gc_test_hooks::PASSES.store(0, Ordering::SeqCst);
    let root = temp_run_dir("retention-gc-error");
    let _guard = TempGuard(root.clone());
    let runs = root.join("runs");
    // Two entries exceed the scan cap of one, so every sweep attempt fails
    // and the bounded retries exhaust.
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        Vec::new(),
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy {
            max_directory_scan_entries: 1,
            ..ServiceDeploymentPolicy::default()
        },
    )
    .unwrap();
    std::fs::write(runs.join("first-entry"), b"one").unwrap();
    std::fs::write(runs.join("second-entry"), b"two").unwrap();
    let shutdown = CancellationToken::new();
    let task = service.start_retention_gc(shutdown.clone()).unwrap();
    // Three attempts, 200 ms apart: advance past the whole retry budget.
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        task.await.expect("gc task joins").is_err(),
        "exhausted retention retries end the task with Err"
    );
}

#[tokio::test]
async fn started_operation_retains_even_terminal_evidence_without_blocking_other_runs() {
    let runs = temp_run_dir("gc-inflight-evidence");
    let _guard = TempGuard(runs.clone());
    write_finished_run(&runs, "unrelated");
    for id in ["unresolved", "finished-indeterminate"] {
        let meta = runs.join(id).join("meta");
        std::fs::create_dir_all(&meta).unwrap();
        let journal =
            engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
        journal.event("run_started", json!({
            "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
            "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
            "retention_days": 0,
        })).unwrap();
        journal.event("operation_started", json!({
            "operation_id": "opaque-operation", "operation_digest": "digest",
            "invocation_id": "invocation", "node": "external", "kind": "command", "target": "fixture",
        })).unwrap();
        if id == "finished-indeterminate" {
            journal
                .event(
                    "operation_finished",
                    json!({
                        "operation_id": "opaque-operation", "status": "indeterminate",
                    }),
                )
                .unwrap();
        }
        journal
            .event("run_finished", json!({"status": "success"}))
            .unwrap();
    }
    // A `repeat` Started operation is still evidence for deletion/GC even
    // though re-entry lets it reach the repeat guard.
    {
        let meta = runs.join("repeat-unresolved").join("meta");
        std::fs::create_dir_all(&meta).unwrap();
        let journal = engine::JournalWriter::create(
            &meta.join("journal.jsonl"),
            "repeat-unresolved",
            false,
            None,
        )
        .unwrap();
        journal.event("run_started", json!({
            "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
            "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
            "retention_days": 0,
        })).unwrap();
        journal.event("operation_started", json!({
            "operation_id": "opaque-repeat", "operation_digest": "digest",
            "invocation_id": "invocation", "node": "external", "kind": "command", "target": "fixture",
            "on_indeterminate": "repeat",
        })).unwrap();
        journal
            .event("run_finished", json!({"status": "success"}))
            .unwrap();
    }
    let journal = runs.join("unresolved/meta/journal.jsonl");
    let before = std::fs::read(&journal).unwrap();
    // Legacy Started events without a policy field fold fail-closed.
    let legacy = crate::summaries::fold_run_state(&runs.join("unresolved")).unwrap();
    assert_eq!(
        legacy.operation_records["opaque-operation"].on_indeterminate,
        contract::RetryOnIndeterminate::Fail
    );
    assert!(legacy.ensure_no_inflight_operations().is_err());
    assert!(legacy.ensure_no_fail_closed_inflight_operations().is_err());
    // Repeat Started passes the re-entry gate but still blocks retention.
    let repeated = crate::summaries::fold_run_state(&runs.join("repeat-unresolved")).unwrap();
    assert_eq!(
        repeated.operation_records["opaque-repeat"].on_indeterminate,
        contract::RetryOnIndeterminate::Repeat
    );
    assert!(repeated.ensure_no_fail_closed_inflight_operations().is_ok());
    assert!(repeated.ensure_no_inflight_operations().is_err());
    let service = gc_service(&runs, 0);
    for id in ["unresolved", "repeat-unresolved"] {
        let refused = service.delete_run(id).await.unwrap_err().to_string();
        assert!(
            refused.contains(if id == "unresolved" {
                "opaque-operation"
            } else {
                "opaque-repeat"
            }) && refused.contains("remains Started"),
            "{refused}"
        );
    }
    // Unknown policy values also fold fail-closed.
    {
        let meta = runs.join("unknown-policy").join("meta");
        std::fs::create_dir_all(&meta).unwrap();
        let journal = engine::JournalWriter::create(
            &meta.join("journal.jsonl"),
            "unknown-policy",
            false,
            None,
        )
        .unwrap();
        journal.event("run_started", json!({
            "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
            "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
            "retention_days": 0,
        })).unwrap();
        journal.event("operation_started", json!({
            "operation_id": "opaque-unknown", "operation_digest": "digest",
            "invocation_id": "invocation", "node": "external", "kind": "command", "target": "fixture",
            "on_indeterminate": "sometimes",
        })).unwrap();
        journal
            .event("run_finished", json!({"status": "success"}))
            .unwrap();
        let folded = crate::summaries::fold_run_state(&runs.join("unknown-policy")).unwrap();
        assert_eq!(
            folded.operation_records["opaque-unknown"].on_indeterminate,
            contract::RetryOnIndeterminate::Fail
        );
        assert!(folded.ensure_no_fail_closed_inflight_operations().is_err());
        std::fs::remove_dir_all(runs.join("unknown-policy")).unwrap();
    }
    for delete in [false, true] {
        let selected = gc_run_directories(&runs, 0, 0, delete, 100).unwrap();
        assert_eq!(selected.len(), 2);
        assert!(
            !selected
                .iter()
                .any(|path| path.file_name() == Some("unresolved")
                    || path.file_name() == Some("repeat-unresolved"))
        );
        assert_eq!(std::fs::read(&journal).unwrap(), before);
        assert!(runs.join("repeat-unresolved").exists());
    }
    assert!(!runs.join("unrelated").exists());
    assert!(!runs.join("finished-indeterminate").exists());
}
