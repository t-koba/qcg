use camino::Utf8PathBuf;
use futures_util::StreamExt as _;
use qcg_api::RunStatus;
use qcg_contract::Contract;
use qcg_engine::JournalWriter;
use qcg_service::LocalQcgService;
use qcg_service::run_meta_dir;
use serde_json::json;

/// Removes the temp root on drop so a failed assertion cannot leak test
/// directories (E04).
struct TempGuard(Utf8PathBuf);
impl Drop for TempGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0.as_std_path());
    }
}

/// Test-only service construction through the policy constructor (E04).
/// Integration tests are external crates, so the unit-test-only
/// `LocalQcgService::new` is unavailable; every test service is built here
/// with explicit defaults instead of a legacy constructor.
fn test_service(
    generators_dir: Utf8PathBuf,
    runs_dir: Utf8PathBuf,
    providers_path: Option<Utf8PathBuf>,
) -> Result<LocalQcgService, qcg_service::ServiceError> {
    LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![generators_dir],
        runs_dir,
        providers_path,
        qcg_policy::DEFAULT_MAX_ACTIVE_RUNS,
        qcg_policy::DEFAULT_MAX_TRACKED_RUNS,
        qcg_service::RunStoreMode::Exclusive,
        qcg_service::ServiceDeploymentPolicy::default(),
    )
}

#[tokio::test]
async fn subscribe_replays_history_and_resumes_an_orphaned_run() {
    let run_id = format!("history-{}", std::process::id());
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join("qcg-service-subscribe-test")
        .join(&run_id);
    let generators_dir = root.join("generators");
    let generator_dir = generators_dir.join("demo");
    let runs_dir = root.join("runs");
    let run_dir = runs_dir.join(&run_id);
    std::fs::create_dir_all(&generator_dir).unwrap();
    std::fs::write(
        generator_dir.join("qcg.toml"),
        r#"[generator]
id = "demo"
name = "Demo"
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
[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "resumed""#,
    )
    .unwrap();
    let contract = Contract::load(&generator_dir).unwrap();
    std::fs::create_dir_all(run_meta_dir(&run_dir)).unwrap();
    // A realistic orphaned journal starts with the durable admission event:
    // execution reuses the journaled ceiling and refuses to re-resolve one
    // (E04), so a journal without `run_queued` must fail closed, never run.
    let writer = JournalWriter::create(
        &run_meta_dir(&run_dir).join("journal.jsonl"),
        &run_id,
        false,
        None,
    )
    .unwrap();
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
                "qcg": "0.1.0",
                "schema_version": 1,
                "retention_days": 0,
                "priority": 0,
                "parent_run_id": null,
                "effective_max_total_steps": 64,
                "effective_policy_origin": "test",
            }),
        )
        .unwrap();
    writer
        .event(
            "run_started",
            json!({
                "generator": "demo",
                "generator_path": generator_dir,
                "contract_sha256": contract.sha256,
                "inputs": {},
                "resource_hashes": [],
                "qcg": "0.1.0",
                "schema_version": 1,
            }),
        )
        .unwrap();
    drop(writer);

    let service = test_service(generators_dir, runs_dir, None).expect("service should initialize");
    assert_eq!(
        service.snapshot(run_id.clone()).await.unwrap().state,
        RunStatus::Queued
    );
    let mut events = service.subscribe(run_id.clone()).await.unwrap();
    let admitted = events.next().await.unwrap();
    assert_eq!(admitted.kind, "run_queued");
    let first = events.next().await.unwrap();
    assert_eq!(first.kind, "run_started");
    let started = first
        .data
        .run_started()
        .expect("run_started data should be typed");
    assert_eq!(started.generator, "demo");
    assert_eq!(started.contract_sha256, contract.sha256);
    service.resume_recovered_runs().await;
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let event = events.next().await.expect("resumed event stream closed");
            match event.kind.as_str() {
                "run_finished" => break event,
                "run_error" | "run_canceled" => {
                    panic!("resumed run terminated unexpectedly: {event:?}")
                }
                _ => {}
            }
        }
    })
    .await
    .expect("resumed run should finish");
    assert_eq!(terminal.kind, "run_finished");
    assert_eq!(
        service.snapshot(run_id).await.unwrap().state,
        RunStatus::Succeeded
    );
    assert_eq!(
        std::fs::read_to_string(run_dir.join("workspace/result.txt")).unwrap(),
        "resumed"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn subscribe_with_future_cursor_is_clamped_not_skipped() {
    // E12: a cursor ahead of known history clamps to the history end so
    // events the client never saw are not skipped forever.
    let run_id = format!("cursor-{}", uuid::Uuid::now_v7());
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join("qcg-service-subscribe-test")
        .join(&run_id);
    let _guard = TempGuard(root.clone());
    let runs_dir = root.join("runs");
    let run_dir = runs_dir.join(&run_id);
    let generator_dir = root.join("generators").join("demo");
    std::fs::create_dir_all(&generator_dir).unwrap();
    std::fs::write(
        generator_dir.join("qcg.toml"),
        r#"[generator]
id = "demo"
name = "Demo"
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
[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "resumed""#,
    )
    .unwrap();
    std::fs::create_dir_all(run_meta_dir(&run_dir)).unwrap();
    JournalWriter::create(
        &run_meta_dir(&run_dir).join("journal.jsonl"),
        &run_id,
        false,
        None,
    )
    .unwrap()
    .event(
        "run_started",
        json!({
            "generator": "demo",
            "generator_path": generator_dir,
            "contract_sha256": "abc",
            "inputs": {},
            "resource_hashes": [],
            "qcg": "0.1.0",
            "schema_version": 1,
        }),
    )
    .unwrap();
    let service = test_service(root.join("generators"), runs_dir, None).unwrap();
    let mut stream = service
        .subscribe_with_cursor(run_id, 999_999)
        .await
        .expect("future cursor must clamp, not fail");
    let first = stream
        .next()
        .await
        .expect("clamped stream must replay history");
    assert_eq!(first.seq, 1, "clamped cursor must not skip unseen events");
}

#[tokio::test]
async fn subscribe_with_garbage_cursor_replays_from_start() {
    // E12 cursor-failure policy: `Last-Event-ID` is client-controlled
    // (`run_detail` is FOREIGN). Empty, missing, or garbage cursors fail
    // toward REPLAY (never skip): the HTTP layer maps them to 0, and 0
    // replays the full history here. A corrupt cursor can only duplicate
    // (filtered by seq), never lose real events.
    let run_id = format!("garbage-{}", uuid::Uuid::now_v7());
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join("qcg-service-subscribe-test")
        .join(&run_id);
    let _guard = TempGuard(root.clone());
    let runs_dir = root.join("runs");
    let run_dir = runs_dir.join(&run_id);
    let generator_dir = root.join("generators").join("demo");
    std::fs::create_dir_all(&generator_dir).unwrap();
    std::fs::write(
        generator_dir.join("qcg.toml"),
        r#"[generator]
id = "demo"
name = "Demo"
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
[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "resumed""#,
    )
    .unwrap();
    std::fs::create_dir_all(run_meta_dir(&run_dir)).unwrap();
    JournalWriter::create(
        &run_meta_dir(&run_dir).join("journal.jsonl"),
        &run_id,
        false,
        None,
    )
    .unwrap()
    .event(
        "run_started",
        json!({
            "generator": "demo",
            "generator_path": generator_dir,
            "contract_sha256": "abc",
            "inputs": {},
            "resource_hashes": [],
            "qcg": "0.1.0",
            "schema_version": 1,
        }),
    )
    .unwrap();
    let service = test_service(root.join("generators"), runs_dir, None).unwrap();
    // The HTTP layer maps empty/garbage to 0; prove 0 replays from start.
    for garbage in ["", "not-a-seq", "99999999999999999999999", "-1"] {
        let parsed: u64 = garbage.parse().unwrap_or(0);
        assert_eq!(parsed, 0, "garbage `{garbage}` must fail toward replay (0)");
        let mut stream = service
            .subscribe_with_cursor(run_id.clone(), parsed)
            .await
            .expect("garbage cursor must replay, not fail");
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("garbage replay should arrive promptly")
            .expect("garbage cursor stream must not end");
        assert_eq!(
            first.seq, 1,
            "a garbage cursor must replay from history start, never skip it"
        );
    }
}

#[tokio::test]
async fn subscribe_boundary_cursor_replays_exactly_once() {
    // E12: a cursor of exactly history_last_seq replays nothing old and
    // misses nothing new; history_last_seq + 1 (no such event yet) behaves
    // the same instead of skipping the next real event.
    let run_id = format!("boundary-{}", uuid::Uuid::now_v7());
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join("qcg-service-subscribe-test")
        .join(&run_id);
    let _guard = TempGuard(root.clone());
    let runs_dir = root.join("runs");
    let run_dir = runs_dir.join(&run_id);
    let generator_dir = root.join("generators").join("demo");
    std::fs::create_dir_all(&generator_dir).unwrap();
    std::fs::write(
        generator_dir.join("qcg.toml"),
        r#"[generator]
id = "demo"
name = "Demo"
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
[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "resumed""#,
    )
    .unwrap();
    std::fs::create_dir_all(run_meta_dir(&run_dir)).unwrap();
    JournalWriter::create(
        &run_meta_dir(&run_dir).join("journal.jsonl"),
        &run_id,
        false,
        None,
    )
    .unwrap()
    .event(
        "run_started",
        json!({
            "generator": "demo",
            "generator_path": generator_dir,
            "contract_sha256": "abc",
            "inputs": {},
            "resource_hashes": [],
            "qcg": "0.1.0",
            "schema_version": 1,
        }),
    )
    .unwrap();
    let service = test_service(root.join("generators"), runs_dir, None).unwrap();
    // history_last_seq is 1 here: cursor 0 replays it at once, while
    // cursors 1 and 2 replay nothing and pend on the live tail instead of
    // skipping the next real event.
    let mut replay = service
        .subscribe_with_cursor(run_id.clone(), 0)
        .await
        .expect("cursor 0 should replay history");
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), replay.next())
        .await
        .expect("history replay should arrive promptly")
        .expect("history must not end");
    assert_eq!(
        first.seq, 1,
        "cursor 0 must replay the single history event"
    );
    // Cursor 1 is exactly the history end: nothing to replay, pend on
    // the live tail.
    let mut live = service
        .subscribe_with_cursor(run_id.clone(), 1)
        .await
        .expect("boundary subscribe should succeed");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), live.next())
            .await
            .is_err(),
        "cursor 1 must replay nothing and pend on live tail"
    );
    // Cursor 2 is beyond the history end (no such event yet): fail closed
    // toward replay so the unseen event is never skipped.
    let mut future = service
        .subscribe_with_cursor(run_id.clone(), 2)
        .await
        .expect("future subscribe should succeed");
    let replayed = tokio::time::timeout(std::time::Duration::from_secs(5), future.next())
        .await
        .expect("future cursor replay should arrive promptly")
        .expect("future cursor stream must not end");
    assert_eq!(
        replayed.seq, 1,
        "a future cursor must replay from history, never skip it"
    );
}

#[tokio::test]
async fn subscribe_to_a_settled_run_ends_without_hanging() {
    // E12: subscribing to an already-terminal run must return history and
    // end, never pend on the live broadcast.
    let run_id = format!("settled-{}", std::process::id());
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join("qcg-service-subscribe-settled-test")
        .join(&run_id);
    let generators_dir = root.join("generators");
    let generator_dir = generators_dir.join("demo");
    let runs_dir = root.join("runs");
    let run_dir = runs_dir.join(&run_id);
    let _temp_guard = TempGuard(root.clone());
    std::fs::create_dir_all(&generator_dir).unwrap();
    std::fs::write(
        generator_dir.join("qcg.toml"),
        r#"[generator]
id = "demo"
name = "Demo"
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
[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "done""#,
    )
    .unwrap();
    let contract = Contract::load(&generator_dir).unwrap();
    std::fs::create_dir_all(run_meta_dir(&run_dir)).unwrap();
    let writer = JournalWriter::create(
        &run_meta_dir(&run_dir).join("journal.jsonl"),
        &run_id,
        false,
        None,
    )
    .unwrap();
    writer
        .event(
            "run_started",
            json!({
                "generator": "demo",
                "generator_path": generator_dir,
                "contract_sha256": contract.sha256,
                "inputs": {},
                "resource_hashes": [],
                "qcg": "0.1.0",
                "schema_version": 1,
            }),
        )
        .unwrap();
    writer
        .event("run_finished", json!({"status": "success"}))
        .unwrap();
    drop(writer);

    let service = test_service(generators_dir, runs_dir, None).expect("service should initialize");
    let mut events = service.subscribe(run_id.clone()).await.unwrap();
    let kinds = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut kinds = Vec::new();
        while let Some(event) = events.next().await {
            kinds.push(event.kind.clone());
        }
        kinds
    })
    .await
    .expect("a settled run must end its stream, not hang");
    assert!(
        kinds.iter().any(|kind| kind == "run_finished"),
        "history must include the terminal event: {kinds:?}"
    );
}

#[tokio::test]
async fn hitl_two_streams_continue_to_terminal_through_subscribe() {
    // E12: two live streams through the real subscribe path (not raw
    // broadcast) both continue past an HITL answer to the terminal event.
    use qcg_api::{AnswerPayload, StartRun};
    use std::collections::BTreeMap;

    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("qcg-subscribe-hitl-{}", uuid::Uuid::now_v7()));
    let _temp_guard = TempGuard(root.clone());
    let generators_dir =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs_dir = root.join("runs");
    let service = test_service(generators_dir, runs_dir, None).expect("service should initialize");
    let run_id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    let mut first = service
        .subscribe(run_id.clone())
        .await
        .expect("first subscribe should succeed");
    let mut second = service
        .subscribe(run_id.clone())
        .await
        .expect("second subscribe should succeed");
    // Wait for Waiting through a snapshot (real service state, no mocks).
    for _ in 0..200 {
        if service
            .snapshot(run_id.clone())
            .await
            .is_ok_and(|snapshot| snapshot.state == qcg_api::RunStatus::Waiting)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let question = service
        .snapshot(run_id.clone())
        .await
        .expect("snapshot should exist")
        .question
        .expect("run should expose its question");
    service
        .answer(
            run_id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), serde_json::json!("brief"))]),
            },
        )
        .await
        .expect("answer should be accepted");
    for (index, stream) in [&mut first, &mut second].into_iter().enumerate() {
        let terminal = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let event = stream.next().await.expect("HITL stream must continue");
                if qcg_api::is_terminal_event_kind(event.kind.as_str()) {
                    break event;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("stream {index} should reach a terminal event"));
        assert_eq!(terminal.kind, "run_finished");
    }
}

#[tokio::test]
async fn broadcast_overflow_continues_through_resubscribe() {
    // E12: a broadcast overflow through the real subscribe path ends with a
    // `lagged` marker carrying the last delivered seq (never delivered +
    // skipped), and a resubscribe from that seq continues through the
    // journal replay to the terminal event.
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("qcg-subscribe-overflow-{}", uuid::Uuid::now_v7()));
    let _temp_guard = TempGuard(root.clone());
    let generators_dir = root.join("generators");
    let generator_dir = generators_dir.join("flood");
    let runs_dir = root.join("runs");
    std::fs::create_dir_all(&generator_dir).unwrap();
    // A real generator flooding >512 live events: 600 foreach writes emit
    // well over the live channel capacity, so a non-reading subscriber
    // deterministically lags through the real broadcast (no raw channel).
    std::fs::write(
        generator_dir.join("qcg.toml"),
        r#"[generator]
id = "flood"
name = "Flood"
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
id = "items"
required = true
type = "list"
item_type = "integer"

[[blocks.item]]
id = "write_item"
type = "write"

[blocks.item.params]
content = "item={{ item }}"
output_file = "items/{{ item }}.txt"

[[flow]]
id = "flood"
type = "foreach"

[flow.params]
items = "inputs.items"
max_iterations = 700
parallel = 8
subflow = "item"

[[flow]]
id = "done"
type = "write"
needs = ["flood"]
artifact = { label = "Done", required = true }

[flow.params]
content = "done"
output_file = "done.txt""#,
    )
    .unwrap();
    let service = test_service(generators_dir, runs_dir, None).expect("service should initialize");
    let items: Vec<serde_json::Value> = (0..600).map(|index| serde_json::json!(index)).collect();
    let run_id = service
        .start_run(qcg_api::StartRun {
            generator_id: "flood".into(),
            inputs: std::collections::BTreeMap::from([(
                "items".into(),
                serde_json::Value::Array(items),
            )]),
            ..Default::default()
        })
        .await
        .expect("flooding run should start");
    // Subscribe early, then do not read while the engine floods: the live
    // broadcast must lag.
    let mut lagging = service
        .subscribe(run_id.clone())
        .await
        .expect("subscribe should succeed");
    // Wait for terminal through snapshots (not through the lagging stream),
    // so the stream's receiver stays unread while 600+ events flood it.
    for _ in 0..400 {
        if service
            .snapshot(run_id.clone())
            .await
            .is_ok_and(|snapshot| snapshot.state.is_terminal())
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    // Drain the lagging stream through the real path: it must end with a
    // `lagged` marker (not a fabricated skip) or already hold the terminal
    // via history on resubscribe. Either way the client continues to the
    // terminal event.
    let mut saw_lagged = false;
    let mut saw_terminal = false;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(event) = lagging.next().await {
            if event.kind.as_str() == "lagged" {
                saw_lagged = true;
                break;
            }
            if qcg_api::is_terminal_event_kind(event.kind.as_str()) {
                saw_terminal = true;
                break;
            }
        }
    })
    .await;
    if saw_lagged {
        // The marker itself was delivered above carrying the last delivered
        // seq; resubscribe through the real path and require the terminal
        // event via journal replay.
        let mut resumed = service
            .subscribe(run_id.clone())
            .await
            .expect("resubscribe should succeed");
        let terminal = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let event = resumed.next().await.expect("resumed stream must continue");
                if qcg_api::is_terminal_event_kind(event.kind.as_str()) {
                    break event;
                }
            }
        })
        .await
        .expect("resumed subscribe should reach a terminal event");
        assert!(
            qcg_api::is_terminal_event_kind(terminal.kind.as_str()),
            "resumed stream must end terminally"
        );
    } else {
        assert!(
            saw_terminal,
            "a flooding subscribe must end with lagged-then-terminal or direct terminal"
        );
    }
}

#[tokio::test]
async fn subscribe_serves_history_while_execution_lease_is_held() {
    // E12: subscribe is a read-only observation that joins neither the
    // store lock nor the per-run execution lease. Holding the execution
    // lease (exclusive writer contention, as a live peer would) must not
    // block history delivery.
    let run_id = format!("lease-contended-{}", std::process::id());
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join("qcg-service-subscribe-test")
        .join(&run_id);
    let generators_dir = root.join("generators");
    let generator_dir = generators_dir.join("demo");
    let runs_dir = root.join("runs");
    let run_dir = runs_dir.join(&run_id);
    std::fs::create_dir_all(&generator_dir).unwrap();
    std::fs::write(
        generator_dir.join("qcg.toml"),
        r#"[generator]
id = "demo"
name = "Demo"
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
[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "resumed""#,
    )
    .unwrap();
    let contract = Contract::load(&generator_dir).unwrap();
    std::fs::create_dir_all(run_meta_dir(&run_dir)).unwrap();
    let writer = JournalWriter::create(
        &run_meta_dir(&run_dir).join("journal.jsonl"),
        &run_id,
        false,
        None,
    )
    .unwrap();
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
                "qcg": "0.1.0",
                "schema_version": 1,
                "retention_days": 0,
                "priority": 0,
                "parent_run_id": null,
                "effective_max_total_steps": 64,
                "effective_policy_origin": "test",
            }),
        )
        .unwrap();
    drop(writer);

    let service = test_service(generators_dir, runs_dir, None).expect("service should initialize");
    // Exclusive writer contention: a peer holds the run execution lease
    // (`<meta>/execution.lock`, same file the engine locks). Subscribe
    // joins neither this lease nor the store lock, so history still serves.
    let meta = run_meta_dir(&run_dir);
    std::fs::create_dir_all(&meta).unwrap();
    let lease = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(meta.join("execution.lock"))
        .unwrap();
    lease.try_lock().expect("execution lease should lock");
    let mut events = service.subscribe(run_id.clone()).await.unwrap();
    let admitted = tokio::time::timeout(std::time::Duration::from_secs(5), events.next())
        .await
        .expect("history must arrive despite the held lease")
        .expect("stream must yield");
    assert_eq!(admitted.kind, "run_queued");
    let _ = std::fs::remove_dir_all(root);
}
