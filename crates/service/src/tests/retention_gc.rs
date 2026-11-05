use super::support::*;
use crate::*;
use camino::Utf8Path;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn gc_service(runs: &Utf8Path, interval_secs: u64) -> LocalService {
    gc_service_with_policy(
        runs,
        ServiceDeploymentPolicy {
            gc_keep: 0,
            gc_keep_failed: 0,
            gc_interval_secs: interval_secs,
            ..ServiceDeploymentPolicy::default()
        },
    )
}

fn gc_service_with_policy(runs: &Utf8Path, policy: ServiceDeploymentPolicy) -> LocalService {
    LocalService::with_generator_roots_policy_and_store_mode(
        Vec::new(),
        runs.to_owned(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        policy,
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
async fn retention_gc_configured_exhaustion_sleeps_only_between_attempts() {
    let _test_lock = crate::retention_gc_test_hooks::TEST_LOCK.lock().await;
    let attempts = &crate::retention_gc_test_hooks::ATTEMPTS;
    // Cover no retries, multiple retries, immediate retries, and unchanged defaults.
    for (budget, delay_ms) in [(1, 4000), (4, 1234), (2, 0), (3, 200)] {
        attempts.store(0, Ordering::SeqCst);
        let root = temp_run_dir("retention-gc-error");
        let _guard = TempGuard(root.clone());
        let runs = root.join("runs");
        let service = gc_service_with_policy(
            &runs,
            ServiceDeploymentPolicy {
                gc_retry_attempts: budget,
                gc_retry_delay_ms: delay_ms,
                max_directory_scan_entries: 1,
                ..ServiceDeploymentPolicy::default()
            },
        );
        // Two entries exceed the scan cap, so every real sweep fails.
        std::fs::write(runs.join("first-entry"), b"one").unwrap();
        std::fs::write(runs.join("second-entry"), b"two").unwrap();
        let shutdown = CancellationToken::new();
        let started = tokio::time::Instant::now();
        let task = service.start_retention_gc(shutdown).unwrap();
        for completed in 1..=budget {
            wait_for_passes(attempts, completed as usize).await;
            if completed < budget && delay_ms != 0 {
                assert_eq!(attempts.load(Ordering::SeqCst), completed as usize);
                tokio::time::advance(Duration::from_millis(u64::from(delay_ms - 1))).await;
                assert_eq!(attempts.load(Ordering::SeqCst), completed as usize);
                tokio::time::advance(Duration::from_millis(1)).await;
            }
        }
        // A final sleep would leave this task unfinished and add to elapsed time.
        assert!(
            task.is_finished(),
            "exhaustion must return without a final sleep"
        );
        let error = task
            .await
            .unwrap()
            .expect_err("exhaustion must stay visible");
        assert!(error.to_string().contains("more than 1 entries"), "{error}");
        assert_eq!(attempts.load(Ordering::SeqCst), budget as usize);
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(u64::from(budget - 1) * u64::from(delay_ms))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn retention_gc_recovers_after_configured_delay() {
    let _test_lock = crate::retention_gc_test_hooks::TEST_LOCK.lock().await;
    let attempts = &crate::retention_gc_test_hooks::ATTEMPTS;
    attempts.store(0, Ordering::SeqCst);
    let root = temp_run_dir("retention-gc-recovery");
    let _guard = TempGuard(root.clone());
    let runs = root.join("runs");
    let service = gc_service_with_policy(
        &runs,
        ServiceDeploymentPolicy {
            gc_retry_attempts: 5,
            gc_retry_delay_ms: 2345,
            max_directory_scan_entries: 1,
            ..ServiceDeploymentPolicy::default()
        },
    );
    std::fs::write(runs.join("first-entry"), b"one").unwrap();
    std::fs::write(runs.join("second-entry"), b"two").unwrap();
    let shutdown = CancellationToken::new();
    let task = service.start_retention_gc(shutdown.clone()).unwrap();
    wait_for_passes(attempts, 1).await;
    std::fs::remove_file(runs.join("first-entry")).unwrap();
    std::fs::remove_file(runs.join("second-entry")).unwrap();
    tokio::time::advance(Duration::from_millis(2344)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    wait_for_passes(attempts, 2).await;
    // Successful recovery parks on the sweep interval, not another retry.
    tokio::time::advance(Duration::from_millis(2345)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(!task.is_finished());
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn retention_gc_contention_skips_without_retrying() {
    let _test_lock = crate::retention_gc_test_hooks::TEST_LOCK.lock().await;
    let attempts = &crate::retention_gc_test_hooks::ATTEMPTS;
    attempts.store(0, Ordering::SeqCst);
    let root = temp_run_dir("retention-gc-contention");
    let _guard = TempGuard(root.clone());
    let service = gc_service_with_policy(
        &root,
        ServiceDeploymentPolicy {
            gc_retry_attempts: 5,
            gc_retry_delay_ms: 1000,
            ..ServiceDeploymentPolicy::default()
        },
    );
    let _maintenance = crate::run_dirs::try_lock_store_maintenance(&root)
        .unwrap()
        .unwrap();
    let shutdown = CancellationToken::new();
    let task = service.start_retention_gc(shutdown.clone()).unwrap();
    wait_for_passes(attempts, 1).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[test]
fn retention_gc_rejects_zero_attempt_policy_before_creating_store() {
    let root = temp_run_dir("retention-gc-invalid");
    let _guard = TempGuard(root.clone());
    let runs = root.join("runs");
    let error = LocalService::with_generator_roots_policy_and_store_mode(
        Vec::new(),
        runs.clone(),
        None,
        1,
        1,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy {
            gc_retry_attempts: 0,
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("gc_retry_attempts"));
    assert!(!runs.exists());
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
