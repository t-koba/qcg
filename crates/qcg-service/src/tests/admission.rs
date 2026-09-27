use super::support::*;
use crate::*;
use camino::Utf8PathBuf;
use qcg_api::{AnswerPayload, ApiError, ForkRun, ForkStatePatch, RunStatus, StartRun};
use qcg_policy::DEFAULT_MAX_TRACKED_RUNS;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(unix)]
use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn queued_snapshots_expose_admission_order() {
    let root = temp_run_dir("queue-visibility");
    let _ = std::fs::remove_dir_all(&root);
    write_generator_package(&root.join("generators"), "queue-gen");
    let service = test_service(vec![root.join("generators")], root.join("runs"));
    let contract = service
        .load_generator("queue-gen")
        .expect("fixture generator should load");
    // Insert out of order to prove positions follow admission time.
    let first_at = chrono::Utc::now();
    let second_at = first_at + chrono::Duration::seconds(5);
    for (run_id, at, state) in [
        ("q-second", Some(second_at), RunStatus::Queued),
        ("q-first", Some(first_at), RunStatus::Queued),
        ("q-running", None, RunStatus::Running),
    ] {
        let run_dir = root.join("runs").join(run_id);
        prepare_api_run_directory(&run_dir).expect("run directory should prepare");
        let (events, _) = broadcast::channel(512);
        let record = RunRecord {
            contract: contract.clone(),
            contract_sha256: contract.sha256.clone(),
            inputs: BTreeMap::new(),
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            priority: 0,
            parent_run_id: None,
            preempted: false,
            state,
            run_dir: run_dir.clone(),
            artifacts: None,
            question: None,
            confirm: None,
            events,
            cancellation: CancellationToken::new(),
            task: Arc::new(Mutex::new(None)),
            queued_at: at,
            owner_id: String::new(),
            ephemeral: false,
        };
        write_run_event(
            &record,
            "run_queued",
            json!({
                "run_id": run_id,
                "generator": "queue-gen@0.1.0",
                "generator_path": contract.root,
                "contract_sha256": contract.sha256,
                "inputs": {},
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
            }),
        )
        .expect("queued event should append");
        service
            .inner
            .runs
            .write()
            .await
            .insert(run_id.into(), record);
    }
    let first = service
        .snapshot("q-first".into())
        .await
        .expect("first snapshot should load");
    assert_eq!(first.state, RunStatus::Queued);
    assert_eq!(first.queue_position, Some(1));
    // Display follows the durable journal instant, not the memory value,
    // so every process reports identical admission order.
    let first_journal_at =
        crate::summaries::read_last_queued_at(&root.join("runs").join("q-first"))
            .expect("journal should carry admission time")
            .to_rfc3339();
    assert_eq!(first.queued_at, Some(first_journal_at));
    let second = service
        .snapshot("q-second".into())
        .await
        .expect("second snapshot should load");
    assert_eq!(second.queue_position, Some(2));
    let second_journal_at =
        crate::summaries::read_last_queued_at(&root.join("runs").join("q-second"))
            .expect("journal should carry admission time")
            .to_rfc3339();
    assert_eq!(second.queued_at, Some(second_journal_at));
    let running = service
        .snapshot("q-running".into())
        .await
        .expect("running snapshot should load");
    assert_eq!(running.queue_position, None);
    assert_eq!(running.queued_at, None);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn concurrent_start_runs_have_unique_ids_and_independent_outputs() {
    let root = temp_run_dir("concurrent-start");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "concurrent"
name = "Concurrent"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "marker"
required = true
type = "string"

[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "{{ inputs.marker }}"
"#,
    )
    .expect("generator manifest should be written");
    let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        10,
        qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let results = futures_util::future::join_all((0..10).map(|index| {
        let service = service.clone();
        let marker = format!("marker-{index}");
        async move {
            let id = service
                .start_run(StartRun {
                    generator_id: "generator".into(),
                    inputs: BTreeMap::from([(String::from("marker"), json!(marker.clone()))]),
                    ..Default::default()
                })
                .await?;
            Ok::<_, ApiError>((id, marker))
        }
    }))
    .await;
    let runs_with_markers: Vec<(String, String)> = results
        .into_iter()
        .map(|result| result.expect("concurrent run should start"))
        .collect();
    let ids: Vec<String> = runs_with_markers.iter().map(|(id, _)| id.clone()).collect();
    assert_eq!(ids.len(), BTreeSet::<String>::from_iter(ids.clone()).len());
    let snapshots = futures_util::future::join_all(
        runs_with_markers
            .iter()
            .map(|(id, _)| wait_for_terminal_snapshot(&service, id)),
    )
    .await;
    for ((id, marker), snapshot) in runs_with_markers.iter().zip(snapshots) {
        assert_eq!(snapshot.run_id, *id);
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let run_dir = service
            .run_dir_for(id)
            .await
            .expect("run directory should exist");
        assert_eq!(run_dir, runs.join(id));
        assert_eq!(
            std::fs::read_to_string(run_workspace_dir(&run_dir).join("result.txt"))
                .expect("run artifact should be readable"),
            *marker
        );

        // Merged public view: observation records live in audit.jsonl.
        let events = crate::summaries::read_events_with_audit(&run_dir)
            .expect("journal should be valid JSONL");
        assert!(!events.is_empty(), "journal should contain run events");
        let seqs: Vec<u64> = events
            .iter()
            .map(|event| {
                event
                    .get("seq")
                    .and_then(Value::as_u64)
                    .expect("journal event should have a sequence")
            })
            .collect();
        assert_eq!(
            seqs,
            (1..=events.len() as u64).collect::<Vec<_>>(),
            "run journal sequence should be contiguous for {id}"
        );
        assert_eq!(
            snapshot.seq,
            *seqs.last().expect("journal should have a last seq")
        );
        let started = events
            .iter()
            .find(|event| event.get("t").and_then(Value::as_str) == Some("run_started"))
            .expect("journal should start the run");
        assert_eq!(
            started.get("run_id").and_then(Value::as_str),
            Some(id.as_str())
        );
        assert_eq!(
            started
                .get("inputs")
                .and_then(|inputs| inputs.get("marker"))
                .and_then(Value::as_str),
            Some(marker.as_str())
        );
        let terminal_events: Vec<&Value> = events
            .iter()
            .filter(|event| {
                matches!(
                    event.get("t").and_then(Value::as_str),
                    Some("run_finished")
                        | Some("run_error")
                        | Some("run_canceled")
                        | Some("run_interrupted")
                )
            })
            .collect();
        assert_eq!(
            terminal_events.len(),
            1,
            "run should have one terminal event"
        );
        assert_eq!(
            terminal_events[0].get("t").and_then(Value::as_str),
            Some("run_finished")
        );
        assert!(
            read_run_events(&run_dir)
                .expect("typed journal events should parse")
                .iter()
                .all(|event| event.run_id == *id),
            "all events should remain scoped to {id}"
        );
    }

    let listed = service
        .list_run_items()
        .await
        .expect("runs should be listable");
    assert_eq!(
        listed
            .iter()
            .filter(|snapshot| ids.contains(&snapshot.run_id))
            .count(),
        10
    );
}

#[cfg(unix)]
#[tokio::test]
async fn run_limits_bound_active_and_tracked_runs() {
    let root = temp_run_dir("max-active-runs");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "max-active"
name = "Max Active"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 1"], purpose = "active run limit test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 1"]"#,
        )
        .expect("generator manifest should be written");
    let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs,
        None,
        1,
        2,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let fork_source_id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("fork source should start");
    assert_eq!(
        wait_for_terminal_snapshot(&service, &fork_source_id)
            .await
            .state,
        RunStatus::Succeeded
    );
    let fork_source_dir = service
        .run_dir_for(&fork_source_id)
        .await
        .expect("fork source directory should exist");
    let fork_seq = read_journal_events(&fork_source_dir)
        .expect("fork source journal should be readable")
        .into_iter()
        .find(|event| event.get("t").and_then(Value::as_str) == Some("step_finished"))
        .and_then(|event| event.get("seq").and_then(Value::as_u64))
        .expect("fork source should contain a stable step checkpoint");
    let first_id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("first run should start");
    let second_id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("second run should enter the durable queue");
    let capacity_error = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect_err("tracked run capacity should reject another queued run");
    assert!(
        capacity_error
            .to_string()
            .contains("run capacity is exhausted")
    );
    let fork_capacity_error = service
        .fork_run(
            &fork_source_id,
            ForkRun {
                at_seq: fork_seq,
                state_patch: ForkStatePatch::default(),
                ..Default::default()
            },
        )
        .await
        .expect_err("fork must share the tracked run capacity");
    assert!(
        fork_capacity_error
            .to_string()
            .contains("run capacity is exhausted")
    );
    assert_eq!(
        service
            .snapshot(second_id.clone())
            .await
            .expect("queued run should be visible")
            .state,
        RunStatus::Queued
    );
    assert_eq!(
        wait_for_terminal_snapshot(&service, &first_id).await.state,
        RunStatus::Succeeded
    );
    assert_eq!(
        wait_for_terminal_snapshot(&service, &second_id).await.state,
        RunStatus::Succeeded
    );
    let replacement_id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("terminal runs should be evicted when capacity is reused");
    assert_eq!(
        wait_for_terminal_snapshot(&service, &replacement_id)
            .await
            .state,
        RunStatus::Succeeded
    );
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn runs_directory_is_exclusive_between_services() {
    let root = temp_run_dir("runs-directory-lock");
    let _ = std::fs::remove_dir_all(&root);
    let generators = root.join("generators");
    let runs = root.join("runs");
    let first = test_service(vec![generators.clone()], runs.clone());
    let error = LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![generators.clone()],
        runs.clone(),
        None,
        qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
        qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect_err("a second service must not share the runs directory");
    assert!(
        error.to_string().contains("already owned"),
        "lock failure should explain ownership conflict, got {error}"
    );
    drop(first);
    wait_for_runs_store_release(&runs).await;
    let second = test_service(vec![generators], runs);
    drop(second);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn shared_filesystem_store_allows_multiple_services_and_serializes_each_run() {
    let root = temp_run_dir("shared-runs-directory");
    let _ = std::fs::remove_dir_all(&root);
    let generators = root.join("generators");
    let runs = root.join("runs");
    let first = LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![generators.clone()],
        runs.clone(),
        None,
        1,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("first shared service should initialize");
    let second = LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        runs,
        None,
        1,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("second shared service should initialize");

    let run_dir = root.join("shared-run");
    let first_lease = try_lock_run_execution(&run_dir)
        .expect("first lease attempt should succeed")
        .expect("first process should own the run");
    assert!(
        try_lock_run_execution(&run_dir)
            .expect("second lease attempt should be valid")
            .is_none(),
        "a run must have only one execution owner"
    );
    drop(first_lease);
    assert!(
        try_lock_run_execution(&run_dir)
            .expect("released lease should be reusable")
            .is_some()
    );
    drop(first);
    drop(second);
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(unix)]
#[tokio::test]
async fn direct_runs_reject_the_same_active_output_directory() {
    let root = temp_run_dir("direct-output-lock");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let output = root.join("output");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "direct-output-lock"
name = "Direct Output Lock"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 1"], purpose = "direct output lock test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 1"]"#,
        )
        .expect("generator manifest should be written");
    let service = test_service(vec![root.clone()], root.join("runs"));
    let direct_run = || DirectRun {
        generator_path: generator.clone(),
        inputs: BTreeMap::new(),
        output_dir: output.clone(),
        json_events: false,
        interactive: false,
        answers: BTreeMap::new(),
        confirmations: BTreeMap::new(),
        llm_seed_override: None,
    };
    let first = tokio::spawn({
        let service = service.clone();
        let run = direct_run();
        async move { service.run_generator_path(run).await }
    });
    let lock_path = direct_run_meta_dir(&output).join(".run.lock");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Ok(probe) = OpenOptions::new().read(true).write(true).open(&lock_path) {
                match probe.try_lock() {
                    Err(std::fs::TryLockError::WouldBlock) => break,
                    Err(std::fs::TryLockError::Error(error)) => {
                        panic!("output lock probe failed: {error}")
                    }
                    Ok(()) => {}
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first direct run should acquire its output lock");
    let error = service
        .run_generator_path(direct_run())
        .await
        .expect_err("a second direct run must not share an active output directory");
    assert!(error.to_string().contains("already active"));
    first
        .await
        .expect("first direct run task should not panic")
        .expect("first direct run should succeed");
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn retrying_admission_onto_a_live_run_keeps_its_record() {
    // E03: a retry bound to the same run id must reuse the live record
    // (task handle, token, channel) instead of replacing it.
    let root = temp_run_dir("adopt-live-run");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "adopt-live"
name = "Adopt Live"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "adoption test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("generator manifest should be written");
    let service = test_service(vec![root], runs);
    let request = || StartRun {
        generator_id: "generator".into(),
        inputs: BTreeMap::new(),
        answers: BTreeMap::from([("q".to_string(), serde_json::json!("a"))]),
        confirmations: BTreeMap::from([("c".to_string(), true)]),
        ..Default::default()
    };
    let reserved = "generator-retry-1".to_string();
    let id = service
        .start_run_with_id(request(), Some(reserved.clone()))
        .await
        .expect("first start should win");
    assert_eq!(id, reserved);
    wait_for_snapshot(&service, &id, RunStatus::Running).await;
    let before = service
        .inner
        .runs
        .read()
        .await
        .get(&id)
        .expect("live record should exist")
        .task
        .clone();
    let retried = service
        .start_run_with_id(request(), Some(reserved))
        .await
        .expect("retry admission should converge");
    assert_eq!(retried, id);
    let after_record = service
        .inner
        .runs
        .read()
        .await
        .get(&id)
        .expect("live record should still exist")
        .clone();
    assert!(
        std::sync::Arc::ptr_eq(&before, &after_record.task),
        "the live task handle must survive a retry admission"
    );
    let before_record = service
        .inner
        .runs
        .read()
        .await
        .get(&id)
        .expect("live record should still exist")
        .clone();
    assert!(
        before_record.events.same_channel(&after_record.events),
        "the live broadcast channel must survive a retry admission"
    );
    // The admitted answer and approval sets must survive as well: a
    // retry reuses the record instead of re-initializing it from the
    // (possibly empty) retry request state (E03).
    assert_eq!(
        after_record.answers.get("q"),
        Some(&serde_json::json!("a")),
        "the admitted answers must survive a retry admission"
    );
    assert_eq!(
        after_record.confirmations.get("c"),
        Some(&true),
        "the admitted confirmations must survive a retry admission"
    );
    // The cancellation token has no identity accessor; its survival is
    // proven functionally below when cancel through the preserved
    // record still settles the run (E03).
    service
        .cancel(id.clone())
        .await
        .expect("cancel should succeed");
    assert_eq!(
        service
            .snapshot(id)
            .await
            .expect("snapshot should exist")
            .state,
        RunStatus::Canceled,
        "the preserved record must still control the run"
    );
}

#[tokio::test]
async fn concurrent_admission_of_one_run_id_fails_closed() {
    // E03: while one admission is preparing a reserved run id, a second
    // caller must fail closed instead of wiping the live prepare.
    let runs = temp_run_dir("admission-lock");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let run_id = "ask-user-admission-lock-1".to_string();
    let held = crate::run_dirs::try_lock_run_admission(&runs.join(&run_id))
        .expect("admission lock should open")
        .expect("admission lock should acquire");
    let request = || StartRun {
        generator_id: "ask-user".into(),
        inputs: BTreeMap::new(),
        ..Default::default()
    };
    let error = service
        .start_run_with_id(request(), Some(run_id.clone()))
        .await
        .expect_err("a concurrent admission must fail closed");
    assert!(
        matches!(error, ApiError::Conflict { .. }),
        "admission contention must be a conflict: {error}"
    );
    drop(held);
    let id = service
        .start_run_with_id(request(), Some(run_id.clone()))
        .await
        .expect("retry after the lock is released should succeed");
    assert_eq!(id, run_id);
    wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    drop(service);
}

#[tokio::test]
async fn a_lease_loser_keeps_the_live_task_handle() {
    // E03: spawn_engine_run must not clear a task handle stored by a
    // concurrent winner when its own lease attempt loses, or the winner
    // becomes an orphan and a second execution can start.
    let root = temp_run_dir("lease-loser");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let contract = service.load_generator("ask-user").expect("contract");
    let run_dir = service.inner.runs_dir.join("ask-user-lease-loser-1");
    crate::run_dirs::prepare_api_run_directory(&run_dir).expect("run directory");
    let held_execution = crate::run_dirs::try_lock_run_execution(&run_dir)
        .expect("execution lock")
        .expect("execution lease should be free");
    let task = Arc::new(Mutex::new(Some(tokio::spawn(async {}))));
    let (events, _) = broadcast::channel(8);
    service
        .clone()
        .spawn_engine_run(crate::lifecycle::SpawnRun {
            run_id: "ask-user-lease-loser-1".into(),
            contract,
            inputs: BTreeMap::new(),
            run_dir,
            events,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            // No admission snapshot on this direct spawn path: the
            // spawn reads the journal once (E03).
            journal_snapshot: None,
            cancellation: CancellationToken::new(),
            task: Arc::clone(&task),
        })
        .await;
    assert!(
        task.lock().expect("task lock").is_some(),
        "the losing spawn must not orphan the winner's handle"
    );
    drop(held_execution);
    if let Some(handle) = task.lock().expect("task lock").take() {
        handle.abort();
    }
    drop(service);
}

#[tokio::test]
async fn unknown_admission_time_sorts_last() {
    let root = temp_run_dir("queue-unknown-last");
    let _ = std::fs::remove_dir_all(&root);
    write_generator_package(&root.join("generators"), "queue-gen");
    let service = test_service(vec![root.join("generators")], root.join("runs"));
    let contract = service
        .load_generator("queue-gen")
        .expect("fixture generator should load");
    let first_at = chrono::Utc::now();
    for (run_id, at) in [("q-first", Some(first_at)), ("q-unknown", None)] {
        let run_dir = root.join("runs").join(run_id);
        prepare_api_run_directory(&run_dir).expect("run directory should prepare");
        let (events, _) = broadcast::channel(512);
        let record = RunRecord {
            contract: contract.clone(),
            contract_sha256: contract.sha256.clone(),
            inputs: BTreeMap::new(),
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            priority: 0,
            parent_run_id: None,
            preempted: false,
            state: RunStatus::Queued,
            run_dir: run_dir.clone(),
            artifacts: None,
            question: None,
            confirm: None,
            events,
            cancellation: CancellationToken::new(),
            task: Arc::new(Mutex::new(None)),
            queued_at: at,
            owner_id: String::new(),
            ephemeral: false,
        };
        write_run_event(
            &record,
            "run_queued",
            json!({
                "run_id": run_id,
                "generator": "queue-gen@0.1.0",
                "generator_path": contract.root,
                "contract_sha256": contract.sha256,
                "inputs": {},
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
            }),
        )
        .expect("queued event should append");
        service
            .inner
            .runs
            .write()
            .await
            .insert(run_id.into(), record);
    }
    let first = service
        .snapshot("q-first".into())
        .await
        .expect("first snapshot should load");
    assert_eq!(first.queue_position, Some(1));
    let unknown = service
        .snapshot("q-unknown".into())
        .await
        .expect("unknown snapshot should load");
    assert_eq!(
        unknown.queue_position,
        Some(2),
        "runs without admission time must sort after dated equals"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn store_modes_are_mutually_exclusive_on_one_runs_directory() {
    // E12c: shared peers coexist under a shared lock, an exclusive
    // owner is refused while any shared peer remains, and shared peers
    // are refused while the exclusive owner holds the store.
    let runs = temp_run_dir("store-lock-modes");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let shared = || {
        LocalQcgService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
    };
    let first = shared().expect("first shared service should initialize");
    let second = shared().expect("second shared service should initialize");
    let exclusive_error = try_service(vec![generators.clone()], runs.clone())
        .expect_err("exclusive must be refused while shared peers hold the store");
    assert!(
        exclusive_error.to_string().contains("already owned"),
        "refusal must name ownership conflict: {exclusive_error}"
    );
    drop(second);
    assert!(
        try_service(vec![generators.clone()], runs.clone()).is_err(),
        "exclusive must be refused while any shared peer remains"
    );
    drop(first);
    wait_for_runs_store_release(&runs).await;
    let exclusive = test_service(vec![generators.clone()], runs.clone());
    let shared_error =
        shared().expect_err("shared must be refused while exclusive holds the store");
    assert!(
        shared_error.to_string().contains("exclusively owned"),
        "refusal must name exclusive ownership: {shared_error}"
    );
    drop(exclusive);
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn duplicate_start_preserves_the_live_task_channel_and_token() {
    // E03: a duplicate admission for a live run converges onto the
    // registered record after proving identity: the engine task, the
    // broadcast channel, and the cancellation token stay the originals,
    // and the live executor (not a replacement) still serves answers.
    // A mismatched identity is refused instead of reusing the record.
    let root = temp_run_dir("live-reuse-identity");
    let _ = std::fs::remove_dir_all(&root);
    let _temp_guard = TempGuard(root.clone());
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "generator"
name = "Live Reuse"
version = "1.0.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "topic"
type = "string"
required = true

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
    )
    .expect("generator manifest should be written");
    let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let run_id = format!("generator-{}", uuid::Uuid::now_v7());
    let request = || StartRun {
        generator_id: "generator".into(),
        inputs: BTreeMap::from([("topic".into(), json!("gardening"))]),
        ..Default::default()
    };
    let first = service
        .start_run_with_id(request(), Some(run_id.clone()))
        .await
        .expect("first start should admit");
    assert_eq!(first, run_id);
    let waiting = wait_for_snapshot(&service, &run_id, RunStatus::Waiting).await;
    let question_id = waiting.question.expect("run should ask").id;
    let (task_before, events_before, answers_before) = {
        let runs = service.inner.runs.read().await;
        let record = runs.get(&run_id).expect("record should exist");
        (
            Arc::clone(&record.task),
            record.events.clone(),
            record.answers.clone(),
        )
    };
    assert!(
        task_before.lock().expect("task slot should lock").is_some(),
        "the live run must hold its engine task"
    );
    let second = service
        .start_run_with_id(request(), Some(run_id.clone()))
        .await
        .expect("identical retry must converge");
    assert_eq!(second, run_id);
    {
        let runs = service.inner.runs.read().await;
        let record = runs.get(&run_id).expect("record should exist");
        assert!(
            Arc::ptr_eq(&task_before, &record.task),
            "the engine task slot must survive a duplicate start"
        );
        assert!(
            events_before.same_channel(&record.events),
            "the broadcast channel must survive a duplicate start"
        );
        assert_eq!(
            record.answers, answers_before,
            "accepted answers must survive a duplicate start"
        );
        assert!(
            record.task.lock().expect("task slot should lock").is_some(),
            "the engine task must still run after a duplicate start"
        );
        // Convergence preserves live state (E03): the retry must not
        // re-initialize answers or reset Waiting to Queued.
        assert_eq!(
            record.state,
            RunStatus::Waiting,
            "a duplicate retry must preserve live Waiting state"
        );
    }
    // Journal convergence (E03): the retry reuses the adopted fold and
    // never appends a second `run_queued`; answers are not
    // re-initialized on disk either. One admission event total.
    {
        let run_dir = service
            .run_dir_for(&run_id)
            .await
            .expect("run directory should resolve");
        let values = crate::summaries::read_journal_events(&run_dir).expect("journal should read");
        let queued = values
            .iter()
            .filter(|event| event.get("t").and_then(Value::as_str) == Some("run_queued"))
            .count();
        assert_eq!(
            queued, 1,
            "a duplicate retry must not journal a second admission"
        );
        let folded = qcg_engine::RunState::fold_values(&values).expect("journal should fold");
        assert!(
            folded.pending.is_some(),
            "the journal fold must preserve the pending question across retry"
        );
    }
    // Lease convergence (E03): a Waiting run holds no live execution
    // lease (its engine finished after issuing the question), so the
    // retry must not start a second execution that grabs it. A free
    // lease here proves no second engine was spawned on retry.
    {
        let run_dir = service
            .run_dir_for(&run_id)
            .await
            .expect("run directory should resolve");
        let lease =
            crate::run_dirs::try_lock_run_execution(&run_dir).expect("lease probe should not fail");
        assert!(
            lease.is_some(),
            "a duplicate retry must not start a second execution that steals the lease"
        );
    }
    // The live executor still serves the original question: answering
    // completes the run instead of wedging on a replaced record.
    service
        .answer(
            run_id.clone(),
            question_id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("yes"))]),
            },
        )
        .await
        .expect("the live run should accept its answer");
    let terminal = wait_for_terminal_snapshot(&service, &run_id).await;
    assert_eq!(terminal.state, RunStatus::Succeeded);
    // A mismatched identity must not reuse the live record.
    let mut other = request();
    other.inputs = BTreeMap::from([("topic".into(), json!("weaving"))]);
    let error = service
        .start_run_with_id(other, Some(run_id.clone()))
        .await
        .expect_err("different inputs must not reuse a live run");
    assert!(
        error.to_string().contains("different inputs"),
        "the refusal must name the mismatch: {error}"
    );
}

#[tokio::test]
async fn start_run_persists_canonical_default_inputs() {
    // A10: admission resolves defaults once and persists the canonical
    // inputs to the journal, the memory record, and the engine.
    let root = temp_run_dir("canonical-inputs");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "generator"
name = "Canonical Inputs"
version = "0.1.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "main"

[[inputs.stages.fields]]
id = "name"
type = "string"
default = "world"

[[flow]]
id = "out"
type = "write"

[flow.params]
content = "hello {{ inputs.name }}"
output_file = "out.txt"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
    )
    .expect("generator manifest should be written");
    let service = test_service(vec![root.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    let journal = read_journal_string(&service, id.clone()).await;
    assert!(
        journal.contains("\"name\":\"world\"") || journal.contains("\"name\": \"world\""),
        "canonical default input must reach the journal, got: {journal}"
    );
    let run_dir = service
        .run_dir_for(&id)
        .await
        .expect("run dir should resolve");
    let state = crate::summaries::fold_run_state(&run_dir).expect("fold should succeed");
    assert_eq!(
        state.inputs.as_ref().and_then(|inputs| inputs.get("name")),
        Some(&json!("world")),
        "folded inputs must carry the resolved default"
    );
    let _ = std::fs::remove_dir_all(&root);
}
