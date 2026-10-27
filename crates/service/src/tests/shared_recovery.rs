use crate::tests::support::{TempGuard, temp_run_dir, wait_for_snapshot};
use crate::*;
use api::{RunStatus, StartRun};
use camino::{Utf8Path, Utf8PathBuf};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn shared_service(runs: &Utf8Path) -> LocalService {
    shared_service_with_policy(runs, ServiceDeploymentPolicy::default())
}

fn shared_service_with_policy(runs: &Utf8Path, policy: ServiceDeploymentPolicy) -> LocalService {
    LocalService::with_generator_roots_policy_and_store_mode(
        vec![Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators")],
        runs.to_owned(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        policy,
    )
    .expect("shared service should initialize")
}

async fn waiting_run(service: &LocalService) -> String {
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_snapshot(service, &id, RunStatus::Waiting).await;
    id
}

#[tokio::test]
async fn corrupt_entry_does_not_block_shared_adoption_or_hide_limits() {
    let root = temp_run_dir("shared-corrupt-entry");
    let _guard = TempGuard(root.clone());
    let runs = root.join("runs");
    let owner = shared_service(&runs);
    let peer = shared_service(&runs);
    assert_eq!(
        peer.shared_recovery_status(),
        SharedRecoveryStatus::Starting
    );
    let healthy = waiting_run(&owner).await;
    // The peer exists before either entry, so success must adopt the run,
    // not just leave an already-rehydrated record observable.
    let corrupt_meta = crate::summaries::run_meta_dir(&runs.join("corrupt"));
    std::fs::create_dir_all(&corrupt_meta).unwrap();
    std::fs::write(corrupt_meta.join("journal.jsonl"), b"not json\n").unwrap();
    assert!(
        crate::summaries::rehydrate_runs(&runs, 10, 100).is_err(),
        "startup must still refuse a corrupt journal"
    );
    peer.refresh_shared_runs()
        .await
        .expect("isolated entry failure is not a global failure");
    assert_eq!(
        peer.shared_recovery_status(),
        SharedRecoveryStatus::Degraded
    );
    let records = peer.inner.runs.read().await;
    assert_eq!(records[&healthy].state, RunStatus::Waiting);
    assert!(!records.contains_key("corrupt"));
    drop(records);
    assert!(
        crate::summaries::refresh_runs(&runs, 10, 1).is_err(),
        "corrupt entries must consume the scan budget"
    );
    assert!(
        crate::summaries::refresh_runs(&runs, 0, 100).is_err(),
        "the non-terminal run cap must remain a global error"
    );
    // A valid journal with a broken package still consumes the run cap.
    let package_meta = crate::summaries::run_meta_dir(&runs.join("missing-package"));
    std::fs::create_dir_all(&package_meta).unwrap();
    std::fs::copy(
        crate::summaries::run_meta_dir(&runs.join(&healthy)).join("journal.jsonl"),
        package_meta.join("journal.jsonl"),
    )
    .unwrap();
    assert!(crate::summaries::refresh_runs(&runs, 1, 100).is_err());
    std::fs::remove_dir_all(runs.join("corrupt")).unwrap();
    std::fs::remove_dir_all(runs.join("missing-package")).unwrap();
    peer.refresh_shared_runs().await.unwrap();
    assert_eq!(peer.shared_recovery_status(), SharedRecoveryStatus::Healthy);
}

#[tokio::test]
async fn tracked_probe_and_fold_failures_preserve_memory_and_refresh_peers() {
    let root = temp_run_dir("shared-tracked-corrupt");
    let _guard = TempGuard(root.clone());
    let runs = root.join("runs");
    let owner = shared_service(&runs);
    let bad = waiting_run(&owner).await;
    let healthy = waiting_run(&owner).await;
    let peer = shared_service(&runs);
    let journal = crate::summaries::run_meta_dir(&runs.join(&bad)).join("journal.jsonl");
    let original = std::fs::read_to_string(&journal).unwrap();
    let mut values: Vec<serde_json::Value> = original
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    values[0]["t"] = serde_json::json!("unknown_event");
    let bad_fold = values
        .iter()
        .map(|event| format!("{event}\n"))
        .collect::<String>();
    let original_question = peer.inner.runs.read().await[&bad]
        .question
        .clone()
        .unwrap()
        .id;
    for corrupt in ["not json\n", bad_fold.as_str()] {
        std::fs::write(&journal, corrupt).unwrap();
        if corrupt == bad_fold {
            assert!(crate::summaries::has_remote_cancel_request(&runs.join(&bad)).is_ok());
            assert!(crate::summaries::fold_run_state(&runs.join(&bad)).is_err());
        }
        // Make the healthy memory stale to prove its journal is still folded.
        {
            let mut records = peer.inner.runs.write().await;
            records.get_mut(&healthy).unwrap().state = RunStatus::Queued;
            records.get_mut(&healthy).unwrap().question = None;
        }
        peer.refresh_shared_runs().await.unwrap();
        assert_eq!(
            peer.shared_recovery_status(),
            SharedRecoveryStatus::Degraded
        );
        let records = peer.inner.runs.read().await;
        assert_eq!(records[&bad].state, RunStatus::Waiting);
        assert_eq!(
            records[&bad].question.as_ref().unwrap().id,
            original_question
        );
        assert!(!records[&bad].cancellation.is_cancelled());
        assert_eq!(records[&healthy].state, RunStatus::Waiting);
        assert!(records[&healthy].question.is_some());
    }
    std::fs::write(&journal, original).unwrap();
    peer.refresh_shared_runs().await.unwrap();
    assert_eq!(peer.shared_recovery_status(), SharedRecoveryStatus::Healthy);
}

async fn settle_tasks() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn global_failures_retry_past_ten_recover_and_cancel_during_backoff() {
    let root = temp_run_dir("shared-retry");
    let _guard = TempGuard(root.clone());
    let runs = root.join("runs");
    let service = shared_service_with_policy(
        &runs,
        ServiceDeploymentPolicy {
            max_directory_scan_entries: 1,
            ..ServiceDeploymentPolicy::default()
        },
    );
    // Exceeding the configured global scan cap is a store-wide error, not
    // an isolated run failure. Use entries rather than renaming the locked
    // run directory so this test is portable to Windows.
    let excess = runs.join("excess-entry");
    std::fs::write(runs.join("first-entry"), b"one").unwrap();
    std::fs::write(&excess, b"two").unwrap();
    let stop = CancellationToken::new();
    let worker = service.start_shared_store_recovery(stop.clone()).unwrap();
    settle_tasks().await;
    let interval = service.recovery_interval();
    for failure in 1..=12 {
        assert_eq!(
            service.shared_recovery_status(),
            SharedRecoveryStatus::Degraded
        );
        assert!(
            !worker.is_finished(),
            "ordinary failures must never retire the worker"
        );
        tokio::time::advance(super::recovery_retry_delay(interval, failure)).await;
        settle_tasks().await;
    }
    std::fs::remove_file(&excess).unwrap();
    tokio::time::advance(super::recovery_retry_delay(interval, 13)).await;
    settle_tasks().await;
    assert_eq!(
        service.shared_recovery_status(),
        SharedRecoveryStatus::Healthy
    );
    assert!(!worker.is_finished());
    // Fail again, then cancel without advancing the retry timer.
    std::fs::write(&excess, b"two").unwrap();
    tokio::time::advance(interval).await;
    settle_tasks().await;
    assert_eq!(
        service.shared_recovery_status(),
        SharedRecoveryStatus::Degraded
    );
    let now = tokio::time::Instant::now();
    stop.cancel();
    tokio::time::timeout(Duration::from_millis(100), worker)
        .await
        .expect("shutdown must not wait out backoff")
        .unwrap()
        .unwrap();
    assert_eq!(tokio::time::Instant::now(), now);
}

#[test]
fn recovery_retry_delay_is_exponential_and_bounded() {
    let interval = Duration::from_secs(5);
    assert_eq!(super::recovery_retry_delay(interval, 1), interval * 2);
    assert_eq!(super::recovery_retry_delay(interval, 2), interval * 4);
    assert_eq!(super::recovery_retry_delay(interval, 6), interval * 64);
    assert_eq!(
        super::recovery_retry_delay(interval, u32::MAX),
        interval * 64
    );
}

#[tokio::test(start_paused = true)]
async fn aborting_supervisor_does_not_detach_shared_worker() {
    let root = temp_run_dir("shared-supervisor-abort");
    let _guard = TempGuard(root.clone());
    let service = shared_service(&root.join("runs"));
    let handle = service
        .start_shared_store_recovery(CancellationToken::new())
        .unwrap();
    settle_tasks().await;
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    settle_tasks().await;
    assert_eq!(
        std::sync::Arc::strong_count(&service.inner),
        1,
        "no worker may retain the service/store after the supervisor aborts"
    );
}

#[tokio::test(start_paused = true)]
async fn supervisor_restarts_panicked_and_early_exit_workers() {
    for panic_first in [true, false] {
        let root = temp_run_dir("shared-restart");
        let _guard = TempGuard(root.clone());
        let service = shared_service(&root.join("runs"));
        let stop = CancellationToken::new();
        let worker_service = service.clone();
        let worker_stop = stop.clone();
        let mut attempts = 0;
        let handle = tokio::spawn(async move {
            worker_service
                .supervise_shared_recovery(worker_stop, move |worker, stop| {
                    attempts += 1;
                    let first = attempts == 1;
                    async move {
                        if first {
                            assert!(!panic_first, "injected worker panic");
                            return;
                        }
                        worker.shared_store_recovery_worker(stop).await;
                    }
                })
                .await
        });
        settle_tasks().await;
        assert_eq!(
            service.shared_recovery_status(),
            SharedRecoveryStatus::Restarting
        );
        tokio::time::advance(super::recovery_retry_delay(service.recovery_interval(), 1)).await;
        settle_tasks().await;
        assert_eq!(
            service.shared_recovery_status(),
            SharedRecoveryStatus::Healthy
        );
        stop.cancel();
        handle.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn spawn_projection_preserves_record_fields_and_shared_handles() {
    let root = temp_run_dir("spawn-projection");
    let _guard = TempGuard(root.clone());
    let mut record = crate::tests::support::synthetic_hitl_record(&root, "source");
    record
        .inputs
        .insert("input".into(), serde_json::json!({"x": 1}));
    record
        .answers
        .insert("question".into(), serde_json::json!("yes"));
    record.confirmations.insert("confirmation".into(), true);
    let spawn = crate::lifecycle::SpawnRun::from_record("resume".into(), &record);
    assert_eq!(spawn.run_id, "resume");
    assert_eq!(spawn.contract.root, record.contract.root);
    assert_eq!(spawn.contract.sha256, record.contract.sha256);
    assert_eq!(spawn.inputs, record.inputs);
    assert_eq!(spawn.run_dir, record.run_dir);
    assert_eq!(spawn.answers, record.answers);
    assert_eq!(spawn.confirmations, record.confirmations);
    assert!(spawn.journal_snapshot.is_none());
    assert!(spawn.events.same_channel(&record.events));
    assert!(std::sync::Arc::ptr_eq(&spawn.task, &record.task));
    assert!(!record.cancellation.is_cancelled());
    spawn.cancellation.cancel();
    assert!(record.cancellation.is_cancelled());
}
