use super::*;
use crate::tests::support::*;
use std::time::Duration;

// Use a real writer and command cancellation, not an invented terminal journal.
#[cfg(unix)]
#[tokio::test]
async fn retention_gc_skips_interrupt_handoff_between_execution_leases() {
    let root = temp_run_dir("gc-interrupt-handoff");
    let _temp = TempGuard(root.clone());
    let generator = root.join("slow");
    std::fs::create_dir_all(&generator).unwrap();
    std::fs::write(generator.join(contract::MANIFEST_FILE), r#"
[generator]
id = "slow"
name = "Retention handoff probe"
version = "0.1.0"
[[flow]]
id = "slow"
type = "command"
[flow.params]
command = ["sh", "-c", "echo started > started; sleep 30"]
[permissions]
fs_read = []
fs_write = ["workspace"]
network = []
side_effects = "allowed"
side_effects_scope = "invocation"
commands = [{ bin = "sh", args = ["-c", "echo started > started; sleep 30"], purpose = "handoff probe", isolation = "trusted_host" }]
[permissions.containers]
enabled = false
[runtime]
command_timeout_seconds = 60
[retention]
days = 0
"#).unwrap();
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        root.join("runs"),
        None,
        1,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        crate::ServiceDeploymentPolicy {
            preemption_mode: policy::PreemptionMode::Interrupt,
            ..Default::default()
        },
    )
    .unwrap();
    let id = service
        .start_run(api::StartRun {
            generator_id: "slow".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let run_dir = service.inner.runs_dir.join(&id);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !run_workspace_dir(&run_dir).join("started").exists() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel();
    let preempt = {
        let service = service.clone();
        let id = id.clone();
        tokio::spawn(async move {
            service
                .preempt_run_inner(&id, Some((reached_tx, proceed_rx)))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), reached_rx)
        .await
        .unwrap()
        .unwrap();
    let before = crate::summaries::read_journal_events(&run_dir).unwrap();
    assert_eq!(before.last().unwrap()["t"], "run_canceled");
    assert_eq!(
        crate::summaries::run_selector_info(&run_dir)
            .unwrap()
            .status,
        "canceled"
    );
    assert_eq!(
        before
            .iter()
            .filter(|event| event["t"] == "run_queued")
            .count(),
        1
    );
    let execution = crate::run_dirs::try_lock_run_execution(&run_dir)
        .unwrap()
        .unwrap();
    drop(execution); // Prove this is the unowned gap, not execution-lock protection.
    let maintenance = crate::run_dirs::try_lock_store_maintenance(&service.inner.runs_dir)
        .unwrap()
        .unwrap();
    assert!(
        crate::summaries::gc_run_directories(
            &service.inner.runs_dir,
            0,
            0,
            true,
            policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES
        )
        .unwrap()
        .is_empty()
    );
    drop(maintenance);
    assert!(run_dir.is_dir());
    assert_eq!(
        crate::summaries::read_journal_events(&run_dir).unwrap(),
        before
    );
    proceed_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), preempt)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = crate::summaries::read_journal_events(&run_dir).unwrap();
    assert_eq!(
        after
            .iter()
            .filter(|event| event["t"] == "run_queued")
            .count(),
        2
    );
    assert!(!after.iter().any(|event| event["t"] == "run_canceled"));
    engine::RunState::fold_values(&after).unwrap();
    // An interrupted external effect remains indeterminate; the fix must not
    // weaken existing fail-closed command replay behavior merely to requeue.
    assert_eq!(
        wait_for_terminal_snapshot(&service, &id).await.state,
        RunStatus::Failed
    );
    service.shutdown_active_runs().await.unwrap();
}

#[tokio::test]
async fn gc_first_preemption_cannot_recreate_deleted_metadata() {
    let root = temp_run_dir("gc-first-preemption");
    let _temp = TempGuard(root.clone());
    let id = "victim";
    let mut record = synthetic_hitl_record(&root, id);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    record.owner_id = service.inner.owner_id.clone();
    record.state = RunStatus::Running; // Stale memory, as when a writer exits.
    let run_dir = record.run_dir.clone();
    let journal_path = crate::summaries::run_meta_dir(&run_dir).join("journal.jsonl");
    std::fs::remove_file(&journal_path).unwrap();
    let journal = engine::JournalWriter::create(&journal_path, id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "ask-user@0.1.0",
                "generator_path": record.contract.root,
                "contract_sha256": record.contract_sha256,
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
                "retention_days": 0,
            }),
        )
        .unwrap();
    journal
        .event(
            "run_finished",
            json!({"status":"success", "failures":[], "metrics":{}}),
        )
        .unwrap();
    service.inner.runs.write().await.insert(id.into(), record);

    let maintenance = crate::run_dirs::try_lock_store_maintenance(&service.inner.runs_dir)
        .unwrap()
        .unwrap();
    let deleted = crate::summaries::gc_run_directories(
        &service.inner.runs_dir,
        0,
        0,
        true,
        policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
    )
    .unwrap();
    drop(maintenance);
    assert_eq!(deleted, vec![run_dir.clone()]);
    assert!(!run_dir.exists());

    // The map can lag the terminal journal. A preemptor that was selected
    // before GC must treat the deleted directory as settled, not recreate it.
    service.preempt_run(id).await.unwrap();
    assert!(!run_dir.exists());
    let runs = service.inner.runs.read().await;
    assert!(!runs[id].preempted);
    assert!(!runs[id].cancellation.is_cancelled());
}

#[tokio::test]
async fn retained_gc_retires_a_deleted_record_even_if_memory_lags() {
    let root = temp_run_dir("gc-stale-memory-record");
    let _temp = TempGuard(root.clone());
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        root.join("runs"),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        crate::types::ServiceDeploymentPolicy {
            gc_keep: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let id = service
        .start_run(api::StartRun {
            generator_id: "hello-template".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_for_terminal_snapshot(&service, &id).await;
    let run_dir = service.inner.runs_dir.join(&id);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(lease) = crate::run_dirs::try_lock_run_execution(&run_dir).unwrap() {
                drop(lease);
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    service.inner.runs.write().await.get_mut(&id).unwrap().state = RunStatus::Running;

    service.collect_retained_runs().await.unwrap();
    assert!(!run_dir.exists());
    assert!(!service.inner.runs.read().await.contains_key(&id));
}
