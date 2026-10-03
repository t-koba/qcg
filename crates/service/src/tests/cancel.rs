use super::support::*;
use crate::*;
use api::ConfirmDecision;
use api::ConfirmationDecision;
use api::{AnswerPayload, ApiError, ForkRun, RunStatus, StartRun};
use camino::Utf8PathBuf;
use policy::DEFAULT_MAX_TRACKED_RUNS;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
#[tokio::test]
async fn cancel_stops_a_running_service_command_before_following_nodes() {
    let root = temp_run_dir("service-cancel");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
            generator.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "cancelable"
name = "Cancelable"
version = "0.1.0"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "cancellation test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]

[[flow]]
id = "must_not_run"
type = "write"
needs = ["sleep"]
artifact = { label = "Unexpected", required = false }
[flow.params]
output_file = "must-not-exist.txt"
content = "unexpected"
"#,
        )
        .expect("generator manifest should be written");
    let service = test_service(vec![root], runs);
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("cancelable run should start");
    wait_for_snapshot(&service, &id, RunStatus::Running).await;
    service
        .cancel(id.clone())
        .await
        .expect("cancel should succeed");
    let run_dir = service
        .run_dir_for(&id)
        .await
        .expect("run directory should exist");
    let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
        .expect("journal should be complete when cancel returns");
    assert!(journal.contains("\"t\":\"run_canceled\""));
    assert_eq!(
        service
            .snapshot(id)
            .await
            .expect("canceled snapshot should be available")
            .state,
        RunStatus::Canceled
    );
    assert!(
        !run_workspace_dir(&run_dir)
            .join("must-not-exist.txt")
            .exists()
    );
    assert!(
        !crate::run_dirs::has_pending_cancel_control(&run_dir),
        "settled cancellation must consume its mailbox control files"
    );
}

#[tokio::test]
async fn shutting_down_service_refuses_new_admissions() {
    // E05: the shutdown gate covers internal callers, not just the
    // HTTP middleware, so a resumer or peer cannot admit work after the
    // shutdown snapshot.
    let root = temp_run_dir("shutdown-admission");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service = test_service(vec![generators.clone()], runs.clone());
    assert!(!service.is_shutting_down());
    service.mark_shutting_down();
    assert!(service.is_shutting_down());
    let error = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect_err("start must be refused during shutdown");
    assert!(
        error.to_string().contains("shutting down"),
        "refusal must name shutdown: {error}"
    );
    let fork = service
        .fork_run(
            "missing-run",
            ForkRun {
                at_seq: 1,
                ..Default::default()
            },
        )
        .await;
    assert!(
        fork.is_err(),
        "fork must be refused before the source is consulted"
    );
    for (name, result) in [
        (
            "cancel",
            service.cancel("missing-run".to_string()).await.map(|_| ()),
        ),
        ("delete", service.delete_run("missing-run").await),
    ] {
        let error = result.expect_err(&format!("{name} must be refused during shutdown"));
        assert!(
            matches!(error, ApiError::Unavailable { .. }),
            "{name} must report shutdown unavailability: {error}"
        );
    }
    // An in-flight answer is refused at the lock-held re-check even
    // though its run predates the shutdown snapshot.
    let answer = service
        .answer(
            "missing-run".to_string(),
            "question".to_string(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("brief"))]),
            },
        )
        .await
        .expect_err("answer must be refused during shutdown");
    assert!(
        matches!(answer, ApiError::Unavailable { .. }),
        "answer must report shutdown unavailability: {answer}"
    );
    // Confirmations go through the same lock-held gate.
    let confirm = service
        .confirm(
            "missing-run".to_string(),
            "confirm-1".to_string(),
            ConfirmDecision {
                decision: ConfirmationDecision::Approve,
            },
        )
        .await
        .expect_err("confirm must be refused during shutdown");
    assert!(
        matches!(confirm, ApiError::Unavailable { .. }),
        "confirm must report shutdown unavailability: {confirm}"
    );
    // A spawn raced past the admission gate is refused at the engine
    // entry itself instead of starting doomed work (E05).
    let contract = contract::Contract::load(generators.join("ask-user"))
        .expect("fixture contract should load");
    let run_dir = runs.join("shutdown-spawn-1");
    let (events, _) = broadcast::channel(8);
    let task = Arc::new(Mutex::new(None));
    service
        .clone()
        .spawn_engine_run(crate::lifecycle::SpawnRun {
            run_id: "shutdown-spawn-1".into(),
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
        task.lock().expect("task slot").is_none(),
        "no engine task may start during shutdown"
    );
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn completion_racing_with_cancel_commits_exactly_one_terminal_state() {
    let root = temp_run_dir("completion-cancel-race");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "completion-cancel-race"
name = "Completion Cancel Race"
version = "0.1.0"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = false }
[flow.params]
output_file = "result.txt"
content = "complete"
"#,
    )
    .expect("generator manifest should be written");
    let service = test_service(vec![root.clone()], runs);

    for _ in 0..32 {
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("run should start");
        tokio::task::yield_now().await;
        service
            .cancel(id.clone())
            .await
            .expect("cancel should settle the run");
        let snapshot = service
            .snapshot(id.clone())
            .await
            .expect("settled snapshot should be available");
        assert!(matches!(
            snapshot.state,
            RunStatus::Succeeded | RunStatus::Canceled
        ));
        let run_dir = service
            .run_dir_for(&id)
            .await
            .expect("run directory should exist");
        let events = read_journal_events(&run_dir).expect("journal should be valid JSONL");
        let terminal_count = events
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
            .count();
        assert_eq!(terminal_count, 1);
    }
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(unix)]
#[tokio::test]
async fn cancel_racing_with_an_answer_has_one_canceled_terminal_state() {
    let root = temp_run_dir("cancel-answer-race");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
            generator.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "cancel-answer-race"
name = "Cancel Answer Race"
version = "0.1.0"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 30"]

[permissions]
fs_read = []
network = []
side_effects_scope = "invocation"
fs_write = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "hold resumed run", isolation = "trusted_host" }]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
    let service = test_service(vec![root], runs);
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question = snapshot
        .question
        .expect("run should be waiting for an answer");
    let (cancel_result, answer_result) = tokio::join!(
        service.cancel(id.clone()),
        service.answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("yes"))]),
            },
        )
    );
    cancel_result.expect("cancel should succeed");
    if let Err(error) = answer_result {
        assert!(
            matches!(
                error,
                api::ApiError::Invalid { .. } | api::ApiError::Conflict { .. }
            ),
            "a canceled or terminal question must reject the answer: {error}"
        );
    }
    let snapshot = wait_for_snapshot(&service, &id, RunStatus::Canceled).await;
    assert_eq!(snapshot.state, RunStatus::Canceled);
    let journal = read_journal_string(&service, id).await;
    let events: Vec<Value> = journal
        .lines()
        .map(|line| serde_json::from_str(line).expect("journal event"))
        .collect();
    assert_eq!(
        events
            .iter()
            .filter(|event| event["t"] == "run_canceled")
            .count(),
        1
    );
    assert!(
        !events
            .iter()
            .any(|event| event["t"] == "run_finished" && event["status"] == "success")
    );
}

#[tokio::test]
async fn shutdown_converges_when_executor_ignores_cancellation() {
    let root = temp_run_dir("shutdown-deadline");
    let _ = std::fs::remove_dir_all(&root);
    write_generator_package(&root.join("generators"), "queue-gen");
    let service = test_service(vec![root.join("generators")], root.join("runs"));
    let contract = service
        .load_generator("queue-gen")
        .expect("fixture generator should load");
    let run_id = "stuck-run";
    let run_dir = root.join("runs").join(run_id);
    prepare_api_run_directory(&run_dir).expect("run directory should prepare");
    let (events, _) = broadcast::channel(512);
    // An executor that never observes cancellation, as with a wedged
    // container runtime client.
    let stuck = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });
    let record = RunRecord {
        contract: contract.clone(),
        contract_sha256: contract.sha256.clone(),
        inputs: BTreeMap::new(),
        answers: BTreeMap::new(),
        confirmations: BTreeMap::new(),
        priority: 0,
        parent_run_id: None,
        preempted: false,
        state: RunStatus::Running,
        run_dir: run_dir.clone(),
        artifacts: None,
        question: None,
        confirm: None,
        events,
        cancellation: CancellationToken::new(),
        task: Arc::new(Mutex::new(Some(stuck))),
        queued_at: None,
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
            "schema_version": api::JOURNAL_SCHEMA_VERSION,
        }),
    )
    .expect("queued event should append");
    service
        .inner
        .runs
        .write()
        .await
        .insert(run_id.to_string(), record);
    let started = std::time::Instant::now();
    service
        .shutdown_active_runs()
        .await
        .expect("shutdown must converge despite the stuck executor");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(25),
        "shutdown deadline must bound the wait"
    );
    let journal = read_journal_string(&service, run_id.to_string()).await;
    assert!(
        journal.contains("run_interrupted"),
        "abandoned work must be marked interrupted for restart triage"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn cancel_mailbox_reaches_disk_only_peer_owner() {
    // A02: a peer tracking no local record still delivers cancellation
    // through the durable mailbox.
    let root = temp_run_dir("cancel-mailbox");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "generator"
name = "Cancel Mailbox"
version = "0.1.0"

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
    let owner = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("shared owner should initialize");
    let id = owner
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
    // Simulate a peer process sharing the runs directory but tracking
    // nothing in memory: dropping its memory record must not drop the
    // durable cancel signal.
    let peer = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("peer should initialize");
    peer.inner.runs.write().await.remove(&id);
    peer.cancel(id.clone())
        .await
        .expect("disk-only peer cancel should succeed");
    let run_dir = owner
        .run_dir_for(&id)
        .await
        .expect("run dir should resolve");
    assert!(
        crate::summaries::has_remote_cancel_request(&run_dir).expect("cancel check should succeed"),
        "mailbox cancel must be observable by the owner"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn cancel_acceptance_is_not_settlement_until_terminal_is_journaled() {
    // A02: a mailbox cancel observed by a non-owning peer reports
    // `CancelRequested`, never `Canceled`. Only a journaled terminal
    // outcome settles the display.
    let root = temp_run_dir("cancel-acceptance");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "generator"
name = "Cancel Acceptance"
version = "0.1.0"

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
    let owner = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("shared owner should initialize");
    let id = owner
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
    // A non-owning peer shares the directory but tracks nothing.
    let peer = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("peer should initialize");
    let run_dir = peer.run_dir_for(&id).await.expect("run dir should resolve");
    crate::run_dirs::request_remote_cancel(&run_dir, &id, "peer-test")
        .expect("mailbox write should succeed");
    // No terminal outcome exists: nothing settled.
    let folded = crate::summaries::fold_run_state(&run_dir).expect("fold should succeed");
    assert!(
        folded.terminal.is_none(),
        "mailbox acceptance must not journal a terminal outcome by itself"
    );
    peer.refresh_shared_runs()
        .await
        .expect("refresh should succeed");
    assert_eq!(
        peer.snapshot(id.clone())
            .await
            .expect("snapshot should be available")
            .state,
        RunStatus::CancelRequested,
        "mailbox acceptance must display as requested, not canceled"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn concurrent_cancel_drains_journal_each_request_once() {
    // E01: N published requests drained twice concurrently journal
    // each operation exactly once; a requester-less control journals
    // once as `unknown` instead of losing a legitimate cancel over a
    // cosmetic field.
    let root = temp_run_dir("cancel-drain-once");
    let _ = std::fs::remove_dir_all(&root);
    let _temp_guard = TempGuard(root.clone());
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "generator"
name = "Cancel Drain"
version = "0.1.0"

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
    let owner = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("owner should initialize");
    let id = owner
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
    let run_dir = owner
        .run_dir_for(&id)
        .await
        .expect("run dir should resolve");
    let mut ops = Vec::new();
    for _ in 0..3 {
        ops.push(
            crate::run_dirs::request_remote_cancel(&run_dir, &id, "tester")
                .expect("publish should succeed"),
        );
    }
    // A requester-less control: malformed, never journaled.
    let ghost = "ghost-no-requester";
    std::fs::write(
        crate::run_dirs::control_dir(&run_dir)
            .join(format!("cancel-{ghost}.json"))
            .as_std_path(),
        serde_json::to_vec(&serde_json::json!({
            "op": "cancel",
            "operation_id": ghost,
            "run_id": id,
        }))
        .expect("ghost should serialize"),
    )
    .expect("ghost should write");
    let (first, second) = tokio::join!(
        owner.drain_cancel_controls(&id, &run_dir),
        owner.drain_cancel_controls(&id, &run_dir),
    );
    first.expect("first drain should succeed");
    second.expect("second drain should succeed");
    let events = crate::summaries::read_events_from_meta(&crate::run_meta_dir(&run_dir))
        .expect("events should read");
    for op in &ops {
        let count = events
            .iter()
            .filter(|event| {
                event.get("t").and_then(Value::as_str) == Some("user_cancel_requested")
                    && event.get("operation_id").and_then(Value::as_str) == Some(op.as_str())
            })
            .count();
        assert_eq!(count, 1, "operation `{op}` must journal exactly once");
    }
    let ghost_events: Vec<_> = events
        .iter()
        .filter(|event| {
            event.get("t").and_then(Value::as_str) == Some("user_cancel_requested")
                && event.get("operation_id").and_then(Value::as_str) == Some(ghost)
        })
        .collect();
    assert_eq!(
        ghost_events.len(),
        1,
        "a requester-less control must journal exactly once, not vanish"
    );
    assert_eq!(
        ghost_events[0].get("requester").and_then(Value::as_str),
        Some("unknown"),
        "a missing requester journals as `unknown`"
    );
    assert!(
        !crate::run_dirs::control_dir(&run_dir)
            .join(format!("cancel-{ghost}.json"))
            .exists(),
        "a requester-less control must be consumed"
    );
}

#[tokio::test]
async fn cancel_racing_shutdown_settles_interrupted() {
    // Q3/lifecycle 625: a cancel racing shutdown settles Interrupted,
    // never Canceled. Direct branch test through the real service with
    // temp dirs: a queued cancel drained while the shutdown token is
    // cancelled journals `run_interrupted`.
    let root = temp_run_dir("cancel-race-shutdown");
    let _guard = TempGuard(root.clone());
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        runs,
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let run_dir = service
        .run_dir_for(&id)
        .await
        .expect("run directory should resolve");
    crate::run_dirs::request_remote_cancel(&run_dir, &id, "tester").expect("cancel should publish");
    service.mark_shutting_down();
    service
        .settle_queued_cancel(&id, &run_dir, "cancellation requested")
        .await;
    let snapshot = service
        .snapshot(id.clone())
        .await
        .expect("snapshot should exist");
    assert_eq!(
        snapshot.state,
        RunStatus::Interrupted,
        "a cancel racing shutdown must settle Interrupted"
    );
    let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
        .expect("journal should read");
    assert!(
        journal.contains("\"t\":\"run_interrupted\""),
        "the race must journal interruption, not cancellation"
    );
}

#[tokio::test]
async fn shutdown_leaves_never_tracked_queued_orphan_for_next_boot() {
    // Q3: the shutdown disk-orphan pass must NOT interrupt never-tracked
    // Queued orphans (leave them for next boot per docs/operations 447
    // exception); resume picks them up via a disk scan for untracked
    // Queued runs. Proves both the exception and its negation in one
    // test: Queued stays, Waiting settles.
    let root = temp_run_dir("shutdown-orphan-exception");
    let _guard = TempGuard(root.clone());
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let contract =
        contract::Contract::load(generators.join("ask-user")).expect("contract should load");
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators.clone()],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    // Never-tracked Queued orphan: a completed admission this process
    // never tracked (crash between journal write and registration).
    // Written directly to disk after construction so no memory record
    // exists.
    let queued_id = format!("orphan-queued-{}", uuid::Uuid::now_v7());
    let queued_dir = runs.join(&queued_id);
    std::fs::create_dir_all(crate::summaries::run_meta_dir(&queued_dir))
        .expect("orphan meta should be created");
    {
        let writer = engine::JournalWriter::create(
            &crate::summaries::run_meta_dir(&queued_dir).join("journal.jsonl"),
            &queued_id,
            false,
            None,
        )
        .expect("orphan writer should be created");
        writer
            .event(
                "run_queued",
                json!({
                    "generator": "ask-user@0.1.0",
                    "generator_path": generators.join("ask-user"),
                    "contract_sha256": contract.sha256,
                    "inputs": {},
                    "answers": {},
                    "confirmations": {},

                    "schema_version": api::JOURNAL_SCHEMA_VERSION,
                    "retention_days": 0,
                    "priority": 0,
                    "parent_run_id": null,
                    "effective_max_total_steps": 64,
                    "effective_policy_origin": "test",
                }),
            )
            .expect("orphan queued event should append");
    }
    // Disk-only Waiting orphan: queued plus a pending question (started
    // elsewhere, never tracked here). Shutdown must interrupt it.
    let waiting_id = format!("orphan-waiting-{}", uuid::Uuid::now_v7());
    let waiting_dir = runs.join(&waiting_id);
    std::fs::create_dir_all(crate::summaries::run_meta_dir(&waiting_dir))
        .expect("waiting meta should be created");
    {
        let writer = engine::JournalWriter::create(
            &crate::summaries::run_meta_dir(&waiting_dir).join("journal.jsonl"),
            &waiting_id,
            false,
            None,
        )
        .expect("waiting writer should be created");
        writer
            .event(
                "run_queued",
                json!({
                    "generator": "ask-user@0.1.0",
                    "generator_path": generators.join("ask-user"),
                    "contract_sha256": contract.sha256,
                    "inputs": {},
                    "answers": {},
                    "confirmations": {},

                    "schema_version": api::JOURNAL_SCHEMA_VERSION,
                    "retention_days": 0,
                    "priority": 0,
                    "parent_run_id": null,
                    "effective_max_total_steps": 64,
                    "effective_policy_origin": "test",
                }),
            )
            .expect("waiting queued should append");
        writer
            .event(
                "run_waiting",
                json!({
                    "question_id": "q1",
                    "question": {"id": "q1", "title": "Question", "fields": []},
                }),
            )
            .expect("waiting marker should append");
    }
    service
        .shutdown_active_runs()
        .await
        .expect("shutdown should succeed");
    let queued_journal =
        std::fs::read_to_string(crate::summaries::run_meta_dir(&queued_dir).join("journal.jsonl"))
            .expect("queued journal should read");
    assert!(
        !queued_journal.contains("run_interrupted"),
        "a never-tracked Queued orphan must survive shutdown for next-boot resume"
    );
    let waiting_journal =
        std::fs::read_to_string(crate::summaries::run_meta_dir(&waiting_dir).join("journal.jsonl"))
            .expect("waiting journal should read");
    assert!(
        waiting_journal.contains("run_interrupted"),
        "a disk-only Waiting orphan must settle as Interrupted (negation)"
    );
    // Next boot resumes the exception orphan via the disk scan.
    drop(service);
    wait_for_runs_store_release(&runs).await;
    let rebooted = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        runs,
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("rebooted service should initialize");
    rebooted.resume_recovered_runs().await;
    // The orphan is adopted into memory (Queued with no live task yet
    // spawns promptly; at minimum it is tracked again).
    let tracked = rebooted.inner.runs.read().await.contains_key(&queued_id);
    assert!(
        tracked,
        "the exception orphan must resume on the next boot via the disk scan"
    );
}

#[tokio::test]
async fn accepted_cancel_converges_to_terminal_without_a_second_cancel() {
    // B10: continuing the acceptance scenario, recovery adopts the
    // task-less accepted run and the common finalizer journals the
    // terminal outcome. No second cancel is ever issued.
    let root = temp_run_dir("cancel-converge");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "generator"
name = "Cancel Converge"
version = "0.1.0"

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
    let owner = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("shared owner should initialize");
    let id = owner
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
    let peer = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("peer should initialize");
    let run_dir = peer.run_dir_for(&id).await.expect("run dir should resolve");
    crate::run_dirs::request_remote_cancel(&run_dir, &id, "peer-test")
        .expect("mailbox write should succeed");
    peer.refresh_shared_runs()
        .await
        .expect("refresh should succeed");
    assert_eq!(
        peer.snapshot(id.clone())
            .await
            .expect("snapshot should be available")
            .state,
        RunStatus::CancelRequested,
        "acceptance must display before settlement"
    );
    // Recovery adopts the accepted run; the spawned task settles it.
    peer.resume_recovered_runs().await;
    let terminal = wait_for_terminal_snapshot(&peer, &id).await;
    assert_eq!(
        terminal.state,
        RunStatus::Canceled,
        "accepted cancel must converge to canceled"
    );
    // Exactly one terminal outcome is journaled: acceptance never
    // counted as settlement, settlement never duplicated it.
    let folded = crate::summaries::fold_run_state(&run_dir).expect("fold should succeed");
    assert!(
        folded.terminal.is_some(),
        "convergence must journal a terminal outcome"
    );
    let events = crate::summaries::read_events_from_meta(&crate::run_meta_dir(&run_dir))
        .expect("events should read");
    let terminals = events
        .iter()
        .filter(|event| {
            matches!(
                event.get("t").and_then(serde_json::Value::as_str),
                Some("run_canceled")
                    | Some("run_finished")
                    | Some("run_error")
                    | Some("run_interrupted")
            )
        })
        .count();
    assert_eq!(terminals, 1, "exactly one terminal event must exist");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn shutdown_active_runs_settles_real_runs_as_interrupted() {
    // E05/Q3: shutdown_active_runs through real service state converges
    // live AND queued runs to Interrupted with journaled terminal
    // events. The queued run proves the all-tracked-non-terminal scope
    // (not just the executing run).
    let root = temp_run_dir("shutdown-active-runs");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
            generator.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "slow-shutdown"
name = "Slow Shutdown"
version = "0.1.0"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "shutdown test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("generator manifest should be written");
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs,
        None,
        1,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("slow run should start");
    wait_for_snapshot(&service, &id, RunStatus::Running).await;
    // A second admission while the single slot is busy stays Queued.
    let queued = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("second run should admit");
    wait_for_snapshot(&service, &queued, RunStatus::Queued).await;
    service.mark_shutting_down();
    service
        .shutdown_active_runs()
        .await
        .expect("shutdown settlement should succeed");
    let snapshot = service
        .snapshot(id.clone())
        .await
        .expect("settled snapshot should exist");
    assert_eq!(
        snapshot.state,
        RunStatus::Interrupted,
        "a live run must settle as Interrupted through shutdown"
    );
    let run_dir = service
        .run_dir_for(&id)
        .await
        .expect("run directory should exist");
    let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
        .expect("journal should be readable");
    assert!(
        journal.contains("\"t\":\"run_interrupted\""),
        "shutdown must journal a terminal interruption"
    );
    let queued_snapshot = service
        .snapshot(queued.clone())
        .await
        .expect("queued snapshot should exist");
    assert_eq!(
        queued_snapshot.state,
        RunStatus::Interrupted,
        "a queued run must also settle as Interrupted through shutdown"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn missing_workspace_blocks_resume_without_repeating_side_effects() {
    let root = temp_run_dir("missing-workspace-resume");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
            generator.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "missing-workspace-resume"
name = "Missing Workspace Resume"
version = "0.1.0"

[[flow]]
id = "write_before"
type = "write"

[flow.params]
output_file = "before.txt"
content = "before"

[[flow]]
id = "effect"
type = "command"

[flow.params]
command = ["echo", "effect"]

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_read = []
network = []
side_effects_scope = "invocation"
fs_write = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "echo", args = ["effect"], purpose = "resume safety proof", isolation = "trusted_host" }]

[permissions.containers]
enabled = false"#,
        )
        .expect("generator manifest should be written");
    let service = test_service(vec![root], runs);
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question = snapshot
        .question
        .expect("run should wait after the side effect");
    let run_dir = service
        .run_dir_for(&id)
        .await
        .expect("run directory should exist");
    std::fs::remove_dir_all(run_workspace_dir(&run_dir))
        .expect("workspace should be removed for the recovery test");
    service
        .answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("yes"))]),
            },
        )
        .await
        .expect("answer should trigger a guarded resume");
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(snapshot.state, RunStatus::Failed);
    let journal = read_journal_string(&service, id).await;
    assert_eq!(
        journal
            .lines()
            .filter(|line| {
                line.contains("\"t\":\"side_effect\"")
                    && line.contains("\"node\":\"effect\"")
                    && line.contains("\"decision\":\"allowed\"")
            })
            .count(),
        1,
        "resume failure must not repeat a completed side effect"
    );
    // The resume failure names the missing workspace output. The
    // assertion pins the cause (the deleted `before.txt` blocking a
    // safe resume), not the engine's full sentence, so wording owned
    // by the resume path can evolve without breaking this settlement
    // assertion.
    assert!(
        journal.contains("before.txt") && journal.contains("cannot safely resume"),
        "resume failure must report the missing workspace output blocking resume"
    );
}

#[tokio::test]
async fn adopt_then_cancel_reaches_the_run_and_sse_continues_to_terminal() {
    // E03: an adopted (rebuilt) run still honors cancel, and a live SSE
    // subscriber through the real subscribe path continues to the
    // terminal event instead of hanging.
    use futures_util::StreamExt as _;
    let root = temp_run_dir("adopt-cancel-sse");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service = test_service(vec![generators.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    // Simulate a crash and adoption: drop the owner, rebuild on the same
    // directory, and resume the orphaned run.
    drop(service);
    wait_for_runs_store_release(&runs).await;
    let adopted = test_service(vec![generators], runs);
    adopted.resume_recovered_runs().await;
    // A live subscriber through the real subscribe path must continue
    // past the adoption to the terminal event.
    let mut events = adopted
        .subscribe(id.clone())
        .await
        .expect("subscribe should succeed");
    adopted
        .cancel(id.clone())
        .await
        .expect("adopted cancel must reach the run");
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let event = events.next().await.expect("SSE stream must continue");
            if api::is_terminal_event_kind(event.kind.as_str()) {
                break event;
            }
        }
    })
    .await
    .expect("subscriber should reach a terminal event after adopt-then-cancel");
    assert_eq!(terminal.kind, "run_canceled");
    assert_eq!(
        adopted
            .snapshot(id)
            .await
            .expect("snapshot should exist")
            .state,
        RunStatus::Canceled
    );
}

#[tokio::test]
async fn g01_forced_cancel_settles_without_self_conflict() {
    // G01-01: a mock executor that ignores cancellation is force-aborted
    // after the deadline; with no peer owner the cancel must settle
    // `run_canceled` exactly once instead of misreporting our own held
    // execution lease as a peer conflict (409). Before the fix the abort
    // path re-acquired the lease through a second open, and `flock`
    // reported our own first fd as `WouldBlock`.
    let root = temp_run_dir("g01-forced-cancel");
    let _guard = TempGuard(root.clone());
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service = test_service(vec![generators], runs.clone());
    let contract = service
        .load_generator("ask-user")
        .expect("fixture generator should load");
    let run_id = "g01-stuck-run";
    let run_dir = runs.join(run_id);
    prepare_api_run_directory(&run_dir).expect("run directory should prepare");
    let (events, _) = broadcast::channel(512);
    // An executor that never observes cancellation (wedged loop).
    let stuck = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });
    let record = RunRecord {
        contract: contract.clone(),
        contract_sha256: contract.sha256.clone(),
        inputs: BTreeMap::new(),
        answers: BTreeMap::new(),
        confirmations: BTreeMap::new(),
        priority: 0,
        parent_run_id: None,
        preempted: false,
        state: RunStatus::Running,
        run_dir: run_dir.clone(),
        artifacts: None,
        question: None,
        confirm: None,
        events,
        cancellation: CancellationToken::new(),
        task: Arc::new(Mutex::new(Some(stuck))),
        queued_at: None,
        owner_id: String::new(),
        ephemeral: false,
    };
    write_run_event(
        &record,
        "run_queued",
        json!({
            "run_id": run_id,
            "generator": "ask-user@0.1.0",
            "generator_path": contract.root,
            "contract_sha256": contract.sha256,
            "inputs": {},
            "schema_version": api::JOURNAL_SCHEMA_VERSION,
        }),
    )
    .expect("queued event should append");
    service
        .inner
        .runs
        .write()
        .await
        .insert(run_id.to_string(), record);
    // The stuck task forces the 5 s abort path; the fix settles under the
    // already-held lease instead of conflicting with itself.
    service
        .cancel(run_id.to_string())
        .await
        .expect("forced cancel with no peer must settle, not conflict");
    let journal = read_journal_string(&service, run_id.to_string()).await;
    assert_eq!(
        journal.matches("\"t\":\"run_canceled\"").count(),
        1,
        "forced cancel must journal exactly one run_canceled: {journal}"
    );
    assert_eq!(
        service
            .snapshot(run_id.to_string())
            .await
            .expect("snapshot should exist")
            .state,
        RunStatus::Canceled,
        "forced cancel must settle as Canceled"
    );
}

#[tokio::test]
async fn g01_peer_execution_lease_blocks_false_settlement() {
    // G01-02: when a real peer holds the execution lease, a task-less
    // cancel must report the peer conflict (cancel signaled, settlement
    // elsewhere) instead of faking a local stop. The durable mailbox
    // already carries the request to the owner.
    let root = temp_run_dir("g01-peer-lease");
    let _guard = TempGuard(root.clone());
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service = test_service(vec![generators.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    // Simulate a restart: the rebooted service tracks the run with no live
    // task, while a peer process holds execution.
    drop(service);
    wait_for_runs_store_release(&runs).await;
    let peer = test_service(vec![generators], runs.clone());
    // No resume: the rehydrated record carries no live task, and no engine
    // holds the lease yet, so the externally held peer lease below is the
    // sole owner. (Resuming would spawn an engine that takes the lease
    // itself and the test could no longer stage a foreign owner.)
    let run_dir = peer.run_dir_for(&id).await.expect("run dir should exist");
    let _peer_lease = crate::run_dirs::try_lock_run_execution(&run_dir)
        .expect("peer lease should lock")
        .expect("peer must own execution for this test");
    let error = peer
        .cancel(id.clone())
        .await
        .expect_err("a peer-owned execution must not settle locally");
    assert!(
        matches!(error, ApiError::Conflict { .. }),
        "peer ownership must surface as Conflict, got: {error}"
    );
    assert!(
        error.to_string().contains("executing elsewhere"),
        "the conflict must name the peer owner: {error}"
    );
    // No terminal outcome was faked locally: the journal holds no
    // run_canceled/run_interrupted from this caller.
    let journal = read_journal_string(&peer, id).await;
    assert!(
        !journal.contains("\"t\":\"run_canceled\"")
            && !journal.contains("\"t\":\"run_interrupted\""),
        "a peer-owned cancel must not journal a local terminal: {journal}"
    );
}
