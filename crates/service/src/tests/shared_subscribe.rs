use super::support::*;
use crate::*;
use api::{AnswerPayload, RunStatus, StartRun};
use camino::Utf8PathBuf;
use futures_util::StreamExt as _;
use policy::DEFAULT_MAX_TRACKED_RUNS;
use serde_json::json;
use std::collections::BTreeMap;

#[tokio::test]
async fn concurrent_subscribe_and_refresh_serialize_without_deadlock_or_loss() {
    // E12: concurrent subscribes plus concurrent shared-store refreshes
    // on real temp dirs complete without deadlock and observe identical
    // history. Subscribe is a read-only observation (it deliberately
    // does not join the store lock; per-run authority stays with the
    // execution lease plus the journal lock), so this proves the paths
    // interleave safely rather than serializing on one lock.
    use futures_util::StreamExt as _;
    let run_id = format!("concurrent-{}", uuid::Uuid::now_v7());
    let root = temp_run_dir("concurrent-subscribe-refresh");
    let _guard = TempGuard(root.clone());
    let generators_dir = root.join("generators");
    let generator_dir = generators_dir.join("demo");
    let runs_dir = root.join("runs");
    let run_dir = runs_dir.join(&run_id);
    std::fs::create_dir_all(&generator_dir).expect("generator dir should be created");
    std::fs::write(
        generator_dir.join(contract::MANIFEST_FILE),
        r#"[generator]
id = "demo"
name = "Demo"
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
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "done""#,
    )
    .expect("generator manifest should be written");
    let contract = contract::Contract::load(&generator_dir).expect("contract should load");
    std::fs::create_dir_all(crate::summaries::run_meta_dir(&run_dir))
        .expect("meta dir should be created");
    let writer = engine::JournalWriter::create(
        &crate::summaries::run_meta_dir(&run_dir).join("journal.jsonl"),
        &run_id,
        false,
        None,
    )
    .expect("journal writer should be created");
    writer
        .event(
            "run_queued",
            json!({
                "generator": "demo@0.1.0",
                "generator_path": generator_dir,
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
        .expect("queued event should append");
    writer
        .event(
            "run_started",
            json!({
                "generator": "demo",
                "generator_path": generator_dir,
                "contract_sha256": contract.sha256,
                "inputs": {},
                "resource_hashes": [],

                "schema_version": api::JOURNAL_SCHEMA_VERSION,
            }),
        )
        .expect("started event should append");
    drop(writer);
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators_dir],
        runs_dir,
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    // Two subscribes plus two refreshes run concurrently; the timeout
    // proves no deadlock, and the identical histories prove no loss.
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let first = {
            let service = service.clone();
            let run_id = run_id.clone();
            tokio::spawn(async move {
                let mut stream = service
                    .subscribe_with_cursor(run_id, 0)
                    .await
                    .expect("first subscribe should succeed");
                let mut kinds = Vec::new();
                for _ in 0..2 {
                    let event = stream.next().await.expect("history should replay");
                    kinds.push((event.seq, event.kind.clone()));
                }
                kinds
            })
        };
        let second = {
            let service = service.clone();
            let run_id = run_id.clone();
            tokio::spawn(async move {
                let mut stream = service
                    .subscribe_with_cursor(run_id, 0)
                    .await
                    .expect("second subscribe should succeed");
                let mut kinds = Vec::new();
                for _ in 0..2 {
                    let event = stream.next().await.expect("history should replay");
                    kinds.push((event.seq, event.kind.clone()));
                }
                kinds
            })
        };
        let refresh_first = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .refresh_shared_runs()
                    .await
                    .expect("first refresh should succeed");
            })
        };
        let refresh_second = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .refresh_shared_runs()
                    .await
                    .expect("second refresh should succeed");
            })
        };
        let (first, second, refresh_first, refresh_second) =
            tokio::join!(first, second, refresh_first, refresh_second);
        (
            first.expect("first subscribe task should finish"),
            second.expect("second subscribe task should finish"),
            refresh_first.expect("first refresh task should finish"),
            refresh_second.expect("second refresh task should finish"),
        )
    })
    .await
    .expect("concurrent subscribes and refreshes must complete without deadlock");
    let (first, second, (), ()) = outcome;
    assert_eq!(
        first,
        vec![
            (1, "run_queued".to_string()),
            (2, "run_started".to_string())
        ],
        "first subscriber must observe the full history"
    );
    assert_eq!(
        first, second,
        "concurrent subscribers must observe identical history with no lost events"
    );
    // Corrupt-journal isolation (E12): a corrupt peer journal never
    // corrupts another run's snapshot. The corrupt run fails closed on
    // its own, while the healthy run still replays its identical
    // history above. Queue-position scans skip (never fail on) corrupt
    // peers, so positions stay exact among orderable runs.
    {
        let corrupt_id = format!("corrupt-{run_id}");
        let corrupt_dir = root.join("runs").join(&corrupt_id);
        std::fs::create_dir_all(crate::summaries::run_meta_dir(&corrupt_dir))
            .expect("corrupt meta dir should be created");
        std::fs::write(
            crate::summaries::run_meta_dir(&corrupt_dir).join("journal.jsonl"),
            b"{ not json\n",
        )
        .expect("corrupt journal should be written");
        let corrupt_snapshot = service.snapshot(corrupt_id.clone()).await;
        assert!(
            corrupt_snapshot.is_err(),
            "a corrupt journal must fail its own snapshot closed"
        );
        let healthy = service
            .snapshot(run_id.clone())
            .await
            .expect("healthy snapshot must survive a corrupt peer");
        assert_eq!(
            healthy.seq, 2,
            "a corrupt peer must not shift the healthy run's history"
        );
    }
}

#[tokio::test]
async fn shared_peer_answer_is_adopted_without_duplicate_execution() {
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = temp_run_dir("shared-peer-answer");
    let _ = std::fs::remove_dir_all(&runs);
    let make_service = || {
        LocalService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("shared service should initialize")
    };
    let owner = make_service();
    let id = owner
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    let question = wait_for_snapshot(&owner, &id, RunStatus::Waiting)
        .await
        .question
        .expect("run should be waiting");
    // A peer attached to the same store observes the waiting run and
    // answers it; the durable journal carries the decision over.
    let peer = make_service();
    let peer_snapshot = peer
        .snapshot(id.clone())
        .await
        .expect("peer should observe the waiting run");
    assert_eq!(peer_snapshot.state, RunStatus::Waiting);
    peer.answer(
        id.clone(),
        question.id.clone(),
        AnswerPayload {
            values: BTreeMap::from([("answer".into(), json!("brief"))]),
        },
    )
    .await
    .expect("peer answer should be accepted");
    let terminal = wait_for_terminal_snapshot(&peer, &id).await;
    assert_eq!(terminal.state, RunStatus::Succeeded);
    let journal = read_journal_string(&peer, id.clone()).await;
    assert!(
        journal.contains("\"t\":\"user_answered\""),
        "peer decision must be journaled for the owner to adopt"
    );
    assert_eq!(
        journal.matches("\"t\":\"run_finished\"").count(),
        1,
        "takeover must settle the run exactly once"
    );
    drop(owner);
    drop(peer);
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn concurrent_conflicting_answers_journal_once() {
    let runs = temp_run_dir("answer-race");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    let question = wait_for_snapshot(&service, &id, RunStatus::Waiting)
        .await
        .question
        .expect("run should be waiting");
    let answer = |value: &str| {
        service.answer(
            id.clone(),
            question.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!(value))]),
            },
        )
    };
    let (first, second) = tokio::join!(answer("brief"), answer("detailed"));
    let succeeded = [&first, &second].iter().filter(|r| r.is_ok()).count();
    assert_eq!(succeeded, 1, "exactly one conflicting answer must win");
    for result in [&first, &second] {
        if let Err(error) = result {
            assert!(
                error.to_string().contains("different values"),
                "loser must see a conflict, got {error}"
            );
        }
    }
    let journal = read_journal_string(&service, id.clone()).await;
    assert_eq!(
        journal.matches("\"t\":\"user_answered\"").count(),
        1,
        "conflicting answers must not both persist with last-wins"
    );
    drop(service);
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn shared_subscribers_follow_the_durable_journal() {
    // E12b: a shared-store subscriber must observe peer progress through
    // the durable journal instead of a local broadcast it does not own.
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = temp_run_dir("sse-ownership");
    let _ = std::fs::remove_dir_all(&runs);
    let make_service = || {
        LocalService::with_generator_roots_policy_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("shared service should initialize")
    };
    let owner = make_service();
    let id = owner
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    let waiting = wait_for_snapshot(&owner, &id, RunStatus::Waiting).await;
    let question = waiting.question.expect("run should have a question");
    let peer = make_service();
    let mut stream = peer.subscribe(id.clone()).await.expect("subscription");
    // Let the peer's journal poller attach before the answer lands.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    owner
        .answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("brief"))]),
            },
        )
        .await
        .expect("answer should be accepted");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut saw_finished = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(event)) if event.kind == "run_finished" => {
                saw_finished = true;
                break;
            }
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => break,
        }
    }
    assert!(
        saw_finished,
        "a shared subscriber must observe durable terminal settlement"
    );
    drop(owner);
    drop(peer);
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn exclusive_tail_ends_with_lagged_on_owner_handoff() {
    // E12: an Exclusive live tail pinned at attach ends with a `lagged`
    // marker when the memory owner changes mid-tail, so the client
    // resubscribes and re-resolves from journal truth instead of
    // stalling on the previous owner's channel.
    use futures_util::StreamExt as _;
    let root = temp_run_dir("owner-handoff-tail");
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
    let history_len = service
        .snapshot(id.clone())
        .await
        .expect("snapshot should exist")
        .seq;
    assert!(history_len >= 1, "waiting history must exist");
    let mut stream = service
        .subscribe(id.clone())
        .await
        .expect("subscribe should succeed");
    // Drain exactly the history prefix so the tail pends live. The live
    // tail observes the owner hand-off before waiting for the next
    // broadcast, so changing the owner now yields a lagged marker on
    // the very next poll without needing a live event.
    for _ in 0..history_len {
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("history should arrive promptly")
            .expect("history must not end");
        assert_ne!(event.kind, "lagged", "history must not contain markers");
    }
    // Simulate an owner hand-off mid-tail: a peer takes over. No
    // intermediate live-pend probe here: cancelling a live `next()`
    // mid-poll would drop the unfold state and stall the tail (E12).
    {
        let mut runs = service.inner.runs.write().await;
        let record = runs.get_mut(&id).expect("record should exist");
        record.owner_id = "peer-takeover".to_string();
    }
    let marker = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .expect("hand-off marker should arrive promptly")
        .expect("stream must not end without a marker");
    assert_eq!(
        marker.kind, "lagged",
        "an owner hand-off must end the pinned tail with a lagged marker"
    );
    // Resubscribe continuation: the journal replay still serves history
    // from the marker position. The marker carries the last delivered
    // seq (history end), so resubscribing from 0 replays the full
    // history and proves the client can continue without loss.
    let mut resumed = service
        .subscribe_with_cursor(id.clone(), 0)
        .await
        .expect("resubscribe should succeed");
    let replayed = tokio::time::timeout(std::time::Duration::from_secs(5), resumed.next())
        .await
        .expect("resubscribed replay should arrive")
        .expect("resubscribed stream must not end");
    assert_eq!(
        replayed.seq, 1,
        "resubscribe from start must replay history after a hand-off"
    );
}

#[tokio::test]
async fn shared_tail_continues_across_owner_claim_change() {
    // E12 shared-mode hand-off: shared subscribers follow the durable
    // journal (~250ms poll), so an execution-owner claim change
    // mid-tail never stalls them. Simulate the change via direct
    // owner-claim manipulation on the same runs directory and assert
    // the shared tail still reaches the terminal event plus resubscribe
    // continuation.
    use futures_util::StreamExt as _;
    let root = temp_run_dir("shared-handoff");
    let _guard = TempGuard(root.clone());
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("shared service should initialize");
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
    let mut stream = service
        .subscribe(id.clone())
        .await
        .expect("shared subscribe should succeed");
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .expect("shared history should arrive")
        .expect("stream must not end");
    assert!(first.seq >= 1, "shared history must replay");
    // Owner-claim hand-off mid-tail: whoever holds the execution lease
    // rewrites the advisory claim; shared refresh adopts it.
    {
        let lease =
            crate::run_dirs::try_lock_run_execution(&run_dir).expect("lease probe should not fail");
        if let Some(lease) = lease {
            crate::run_dirs::claim_run_execution_owner(&lease, "peer-takeover")
                .expect("owner claim should write");
        }
    }
    service
        .refresh_shared_runs()
        .await
        .expect("shared refresh should adopt the new claim");
    // Answer through the service; the shared journal poll (not a pinned
    // owner channel) must still deliver the terminal event.
    let question = service
        .snapshot(id.clone())
        .await
        .expect("snapshot should exist")
        .question
        .expect("run should expose its question");
    service
        .answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("brief"))]),
            },
        )
        .await
        .expect("answer should be accepted");
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let event = stream.next().await.expect("shared stream must continue");
            if api::is_terminal_event_kind(event.kind.as_str()) {
                break event;
            }
        }
    })
    .await
    .expect("shared tail must reach terminal across the owner change");
    assert_eq!(terminal.kind, "run_finished");
}

#[tokio::test]
async fn live_owner_refresh_merges_peer_terminal_and_answers() {
    // E12: a live owner (Running with a live task) still merges durable
    // terminal settlement and HITL answers read-only, never appending
    // while its writer is live. Two real services share one runs
    // directory (two-process view).
    use crate::RunStoreMode;
    let root = temp_run_dir("two-process-view");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service_a = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators.clone()],
        runs.clone(),
        None,
        4,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("first shared service should initialize");
    let service_b = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        runs.clone(),
        None,
        4,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("second shared service should initialize");
    // Live-owner terminal merge: A runs slow (live task), B cancels
    // through the durable mailbox, A refreshes and converges without
    // journaling itself.
    let slow_root = temp_run_dir("two-process-slow");
    let _ = std::fs::remove_dir_all(&slow_root);
    let slow_gen = slow_root.join("generator");
    std::fs::create_dir_all(&slow_gen).expect("slow generator should be created");
    std::fs::write(
            slow_gen.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "slow-live"
name = "Slow Live"
version = "0.1.0"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "live merge test", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]"#,
        )
        .expect("slow manifest should be written");
    let slow_runs = slow_root.join("runs");
    let slow_a = LocalService::with_generator_roots_policy_and_store_mode(
        vec![slow_root.clone()],
        slow_runs.clone(),
        None,
        2,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("slow service A should initialize");
    let slow_b = LocalService::with_generator_roots_policy_and_store_mode(
        vec![slow_root.clone()],
        slow_runs.clone(),
        None,
        2,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("slow service B should initialize");
    let running = slow_a
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("slow run should start");
    wait_for_snapshot(&slow_a, &running, RunStatus::Running).await;
    // B requests cancel through the durable mailbox (non-owner path).
    let run_dir_b = slow_b
        .run_dir_for(&running)
        .await
        .expect("B should resolve the run directory");
    crate::run_dirs::request_remote_cancel(&run_dir_b, &running, "peer-b")
        .expect("peer cancel should publish");
    // A refreshes while its engine task is still live: the cancel token
    // must be prodded and the run must not stay stale Running.
    slow_a
        .refresh_shared_runs()
        .await
        .expect("live refresh should succeed");
    {
        let runs = slow_a.inner.runs.read().await;
        let record = runs.get(&running).expect("live record should still exist");
        assert!(
            record.cancellation.is_cancelled(),
            "a live owner must observe a peer cancel through refresh"
        );
    }
    slow_a
        .cancel(running.clone())
        .await
        .expect("owner cancel should settle");
    assert_eq!(
        slow_a
            .snapshot(running.clone())
            .await
            .expect("snapshot should exist")
            .state,
        RunStatus::Canceled,
        "a peer-cancelled live run must converge to Canceled"
    );
    // HITL answer merge: A parks Waiting, B answers, A refreshes and
    // converges without stale Waiting.
    let waiting = service_a
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    let question = wait_for_snapshot(&service_a, &waiting, RunStatus::Waiting)
        .await
        .question
        .expect("run should expose its question");
    // B answers through the durable journal (real answer path on B).
    // B must first observe the run: refresh to import the rehydrated
    // record, then answer.
    service_b
        .refresh_shared_runs()
        .await
        .expect("B should import the waiting run");
    service_b
        .answer(
            waiting.clone(),
            question.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("brief"))]),
            },
        )
        .await
        .expect("peer answer should be accepted");
    // A refreshes: the durable answer must converge into memory instead
    // of staying stale Waiting.
    for _ in 0..100 {
        service_a
            .refresh_shared_runs()
            .await
            .expect("refresh should succeed");
        let snapshot = service_a
            .snapshot(waiting.clone())
            .await
            .expect("snapshot should exist");
        if snapshot.state != RunStatus::Waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let converged = service_a
        .snapshot(waiting.clone())
        .await
        .expect("snapshot should exist");
    assert!(
        converged.state != RunStatus::Waiting,
        "A must converge past stale Waiting after B answers, got {:?}",
        converged.state
    );
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&slow_root);
}

#[tokio::test]
async fn shared_poller_serves_many_subscribers_with_one_task() {
    // E12: N subscribers through the real subscribe path share one
    // underlying poll task (reference-counted map).
    use crate::RunStoreMode;
    use futures_util::StreamExt as _;
    let root = temp_run_dir("shared-poller");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
            generator.join(contract::MANIFEST_FILE),
            r#"
[generator]
id = "slow-poller"
name = "Slow Poller"
version = "0.1.0"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "poller test", isolation = "trusted_host" }]


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
        runs.clone(),
        None,
        4,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::SharedFilesystem,
        ServiceDeploymentPolicy::default(),
    )
    .expect("shared service should initialize");
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("slow run should start");
    wait_for_snapshot(&service, &id, RunStatus::Running).await;
    let mut first = service
        .subscribe(id.clone())
        .await
        .expect("first subscribe should succeed");
    let mut second = service
        .subscribe(id.clone())
        .await
        .expect("second subscribe should succeed");
    let mut third = service
        .subscribe(id.clone())
        .await
        .expect("third subscribe should succeed");
    // One poller serves all three subscribers.
    {
        let pollers = service
            .inner
            .journal_pollers
            .lock()
            .expect("poller map should lock");
        assert_eq!(
            pollers.len(),
            1,
            "one shared poller must serve N subscribers, got {}",
            pollers.len()
        );
    }
    // All three observe live progress: cancel settles terminally and
    // every stream continues to the terminal event instead of hanging.
    service
        .cancel(id.clone())
        .await
        .expect("cancel should settle");
    for (index, stream) in [&mut first, &mut second, &mut third]
        .into_iter()
        .enumerate()
    {
        let terminal = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let event = stream.next().await.expect("shared stream must continue");
                if api::is_terminal_event_kind(event.kind.as_str()) {
                    break event;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("subscriber {index} should reach a terminal event"));
        assert_eq!(terminal.kind, "run_canceled");
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn losing_hitl_race_answers_without_deadlocking_the_run_map() {
    // B01: a peer that loses the journal precondition race must report
    // the rejection promptly instead of deadlocking the runs map against
    // itself (write guard held across a lock-taking classify await).
    // Two services share one runs directory so neither peer's memory
    // fast path can decide the race; only the durable precondition can.
    let root = temp_run_dir("hitl-race-no-deadlock");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "generator"
name = "Race"
version = "0.1.0"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes", "no"]

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
    let make_service = || {
        LocalService::with_generator_roots_policy_and_store_mode(
            vec![root.clone()],
            runs.clone(),
            None,
            policy::DEFAULT_MAX_ACTIVE_RUNS,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
            ServiceDeploymentPolicy::default(),
        )
        .expect("shared service should initialize")
    };
    let owner = make_service();
    let peer = make_service();
    let id = owner
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let question = wait_for_snapshot(&owner, &id, RunStatus::Waiting)
        .await
        .question
        .expect("run should be waiting");
    // A second run proves the map stays responsive while the loser is
    // being classified.
    let probe = owner
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("probe run should start");
    // The peer observes the waiting generation before the owner wins,
    // so its memory checks pass later and only the journal precondition
    // can reject it.
    peer.refresh_shared_runs()
        .await
        .expect("refresh should succeed");
    owner
        .answer(
            id.clone(),
            question.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("yes"))]),
            },
        )
        .await
        .expect("winning answer should be accepted");
    let (loser, probe_snapshot) = tokio::join!(
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            peer.answer(
                id.clone(),
                question.id.clone(),
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("no"))]),
                },
            )
        ),
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            peer.snapshot(probe.clone()),
        ),
    );
    let loser = loser
        .expect("losing answer must respond, not deadlock")
        .expect_err("losing answer must be rejected");
    // Either accurate classification proves prompt rejection without
    // deadlock: a finished run reports terminal, a still-settling run
    // reports the conflicting acceptance.
    assert!(
        loser.to_string().contains("different values")
            || loser.to_string().contains("already terminal"),
        "loser should see the conflict, got: {loser}"
    );
    probe_snapshot
        .expect("unrelated snapshot must stay responsive")
        .expect("probe snapshot should exist");
    let _ = std::fs::remove_dir_all(&root);
}
