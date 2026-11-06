//! Cross-process barriers exercise the real orchestration and service admission.
use super::*;
use camino::Utf8PathBuf;
use std::{
    fs,
    process::{Child, Command},
    time::{Duration, Instant},
};

const KEY: &str = "execution-gate-test";
const CHILD: &str = "server::idempotency::execution_gate_tests::gate_process";

struct Fixture(Utf8PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Process(Child);
impl Process {
    fn success(&mut self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status.success();
            }
            assert!(Instant::now() < deadline, "child process timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn spawn(runs: &Utf8PathBuf, mode: &str) -> Process {
    Process(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD, "--nocapture"])
            .env("QCG_GATE_TEST_RUNS", runs.as_str())
            .env("QCG_GATE_TEST_MODE", mode)
            .spawn()
            .unwrap(),
    )
}
fn wait_file(runs: &Utf8PathBuf, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !runs.join(name).exists() {
        assert!(Instant::now() < deadline, "barrier timed out: {name}");
        std::thread::sleep(Duration::from_millis(10));
    }
}
async fn barrier(runs: &Utf8PathBuf, name: &str) {
    fs::write(runs.join(name), b"").unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !runs.join(format!("{name}-go")).exists() {
        assert!(Instant::now() < deadline, "child barrier timed out: {name}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn pending(runs: &Utf8PathBuf) -> Utf8PathBuf {
    runs.join("idempotency").join(format!(
        "{}.pending.json",
        hex::encode(Sha256::digest(KEY.as_bytes()))
    ))
}
fn age_claim(runs: &Utf8PathBuf) -> Vec<u8> {
    // Controlled clock seam, never a product mutation: only the timestamp changes.
    let original = fs::read(pending(runs)).unwrap();
    let mut record: serde_json::Value = serde_json::from_slice(&original).unwrap();
    record["created_at_unix"] = 1.into();
    fs::write(pending(runs), serde_json::to_vec(&record).unwrap()).unwrap();
    original
}

fn shared_service(runs: &Utf8PathBuf) -> service::LocalService {
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned();
    service::LocalService::with_generator_roots_policy_and_store_mode(
        vec![workspace.join("fixtures/generators")],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        service::RunStoreMode::SharedFilesystem,
        service::ServiceDeploymentPolicy::default(),
    )
    .unwrap()
}

fn answer_payload() -> api::AnswerPayload {
    api::AnswerPayload {
        values: std::collections::BTreeMap::from([(
            "answer".into(),
            serde_json::json!("detailed"),
        )]),
    }
}
fn confirm_decision() -> api::ConfirmDecision {
    api::ConfirmDecision {
        decision: api::ConfirmationDecision::Approve,
    }
}
fn interaction_body(scope: &str, interaction: &str) -> Vec<u8> {
    // Match run_detail's digest: interaction id, NUL, then the typed JSON body.
    let mut body = interaction.as_bytes().to_vec();
    body.push(0);
    body.extend(if scope == "answer" {
        serde_json::to_vec(&answer_payload()).unwrap()
    } else {
        serde_json::to_vec(&confirm_decision()).unwrap()
    });
    body
}

#[tokio::test]
async fn gate_process() {
    let Ok(root) = std::env::var("QCG_GATE_TEST_RUNS") else {
        return;
    };
    let runs = Utf8PathBuf::from(root);
    let mode = std::env::var("QCG_GATE_TEST_MODE").unwrap();
    if mode == "probe" {
        // This process cannot replace, release or commit the expired same-key
        // claim: it cannot obtain the capability required by those APIs.
        let before = fs::read(pending(&runs)).unwrap();
        assert!(ExecutionGate::try_acquire(&runs, KEY).unwrap().is_none());
        // A separate shard can both claim and release: no metadata lock was
        // held while the same-key process was waiting at a barrier.
        let other = (0..)
            .map(|n| format!("other-{n}"))
            .find(|k| Sha256::digest(k.as_bytes())[0] != Sha256::digest(KEY.as_bytes())[0])
            .unwrap();
        let gate = ExecutionGate::try_acquire(&runs, &other).unwrap().unwrap();
        let (owner, generation) = match durable::claim_durable_pending_gated(
            &gate,
            &other,
            "digest",
            None,
            policy::IDEMPOTENCY_TTL,
            &Default::default(),
        )
        .unwrap()
        {
            ClaimOutcome::Owner {
                owner, generation, ..
            } => (owner, generation),
            other => panic!("unexpected: {other:?}"),
        };
        durable::release_durable_pending_gated(&gate, &other, &owner, generation).unwrap();
        assert_eq!(fs::read(pending(&runs)).unwrap(), before);
        return;
    }
    let service = shared_service(&runs);
    let state = Arc::new(crate::test_state(
        service,
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    if let Some(scope @ ("answer" | "confirm")) = mode.strip_suffix("-crash") {
        let (id, interaction): (String, String) =
            serde_json::from_slice(&fs::read(runs.join("interaction-target")).unwrap()).unwrap();
        let body = interaction_body(scope, &interaction);
        let mut headers = HeaderMap::new();
        headers.insert(policy::IDEMPOTENCY_HEADER, KEY.parse().unwrap());
        with_idempotency(IdempotentCall {
            state: &state,
            headers: &headers,
            scope,
            target: &id,
            body: &body,
            digest_context: None,
            created: false,
            reserved_run_id: Some(id.clone()),
            execute: |context| {
                let id = &id;
                let state = &state;
                let runs = &runs;
                async move {
                    assert_eq!(context.reserved_run_id.as_ref(), Some(id));
                    if scope == "answer" {
                        state
                            .service
                            .answer(id.clone(), interaction, answer_payload())
                            .await?;
                    } else {
                        state
                            .service
                            .confirm(id.clone(), interaction, confirm_decision())
                            .await?;
                    }
                    // The journal append has completed, but Ready cannot be stored
                    // until this closure returns. The parent holds execution.lock.
                    barrier(runs, "interaction-accepted").await;
                    Ok(id.clone())
                }
            },
        })
        .await
        .unwrap();
        panic!("parent must kill request before Ready");
    }
    if mode.starts_with("cancel-") {
        let id = fs::read_to_string(runs.join("cancel-target")).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(policy::IDEMPOTENCY_HEADER, KEY.parse().unwrap());
        if mode == "cancel-crash" {
            with_idempotency(IdempotentCall {
                state: &state,
                headers: &headers,
                scope: "cancel",
                target: &id,
                body: &[],
                digest_context: None,
                created: false,
                reserved_run_id: Some(id.clone()),
                execute: |context| {
                    let id = &id;
                    let state = &state;
                    let runs = &runs;
                    async move {
                        assert_eq!(context.reserved_run_id.as_ref(), Some(id));
                        let operation_id = context.operation_id.unwrap();
                        let accepted = state
                            .service
                            .cancel_with_operation_id(id.clone(), operation_id.clone())
                            .await;
                        assert!(matches!(accepted, Err(api::ApiError::Conflict { .. })));

                        fs::write(runs.join("cancel-operation"), operation_id).unwrap();
                        barrier(runs, "cancel-accepted").await;
                        Ok(id.clone())
                    }
                },
            })
            .await
            .unwrap();
            panic!("parent must kill request before Ready");
        }
        let response = crate::server::run_detail::cancel_run(
            axum::extract::State(state),
            headers,
            axum::extract::Path(id),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        return;
    }
    let reserved = state
        .service
        .reserve_start_run_id("hello-template")
        .unwrap();
    let request = api::StartRun {
        generator_id: "hello-template".into(),
        inputs: std::collections::BTreeMap::from([("name".into(), serde_json::json!("gate"))]),
        ..Default::default()
    };
    let mut headers = HeaderMap::new();
    headers.insert(policy::IDEMPOTENCY_HEADER, KEY.parse().unwrap());
    if mode == "retry" {
        fs::write(runs.join("retry-start"), b"").unwrap();
    }
    let result = with_idempotency(IdempotentCall {
        state: &state,
        headers: &headers,
        scope: "start_run",
        target: "",
        body: b"gate",
        digest_context: None,
        created: true,
        reserved_run_id: Some(reserved),
        execute: |context| async {
            let reserved = context.reserved_run_id;
            assert_ne!(
                mode, "retry",
                "same-key successor must replay, never execute"
            );
            barrier(&runs, "before").await;
            let id = state.service.start_run_with_id(request, reserved).await?;
            fs::write(runs.join("accepted-run"), &id).unwrap();
            // Paused inside keyed execution, after durable service acceptance
            // but before returning to Ready publication.
            barrier(&runs, "accepted").await;
            Ok(id)
        },
    })
    .await;
    let response = result.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::CREATED);
    if mode == "retry" {
        let id = fs::read_to_string(runs.join("accepted-run")).unwrap();
        assert_eq!(
            response.headers()[axum::http::header::LOCATION],
            format!("/api/runs/{id}")
        );
        assert_eq!(state.service.list_run_items().await.unwrap().len(), 1);
    }
}

#[test]
fn expired_owner_is_fenced_before_and_after_service_acceptance() {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-gate-{}", uuid::Uuid::now_v7())),
    );
    let runs = &fixture.0;
    let mut owner = spawn(runs, "owner");
    wait_file(runs, "before");
    // Age only while the owner is paused and before starting concurrent reads.
    // Rewriting a live claim with fs::write briefly exposes an empty file to
    // the retry's read-only preflight. The claim stays expired in both phases.
    age_claim(runs);
    let aged = fs::read(pending(runs)).unwrap();
    let mut retry = spawn(runs, "retry");
    wait_file(runs, "retry-start");
    for phase in ["before", "accepted"] {
        wait_file(runs, phase);
        // Expiry is not ownership: the gate-aware holder remains current
        // before and after acceptance, and the same-key retry must wait.
        assert!(spawn(runs, "probe").success());
        assert_eq!(fs::read(pending(runs)).unwrap(), aged);
        assert!(retry.0.try_wait().unwrap().is_none());
        fs::write(runs.join(format!("{phase}-go")), b"").unwrap();
    }
    assert!(owner.success());
    assert!(retry.success());
    let ready = durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
        .unwrap()
        .unwrap();
    assert_eq!(
        ready.run_id,
        fs::read_to_string(runs.join("accepted-run")).unwrap()
    );
    assert!(!pending(runs).exists());
    // Gate leaves survive Ready release. An already-open
    // handle must still arbitrate with fresh opens (no unlink/inode ABA).
    let shard = Sha256::digest(KEY.as_bytes())[0];
    let path = runs
        .join("idempotency")
        .join(format!(".execution-{shard:02x}.lock"));
    let old_handle = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let gate = ExecutionGate::try_acquire(runs, KEY).unwrap().unwrap();
    assert!(matches!(
        old_handle.try_lock(),
        Err(fs::TryLockError::WouldBlock)
    ));
    drop(gate);
    old_handle.try_lock().unwrap();
    assert!(ExecutionGate::try_acquire(runs, KEY).unwrap().is_none());
    drop(old_handle);
    assert!(ExecutionGate::try_acquire(runs, KEY).unwrap().is_some());
}

#[test]
fn process_death_releases_gate_but_preserves_pending_evidence() {
    for phase in ["before", "accepted"] {
        let fixture = Fixture(
            Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .unwrap()
                .join(format!("qcg-gate-death-{}", uuid::Uuid::now_v7())),
        );
        let runs = &fixture.0;
        let mut owner = spawn(runs, "owner");
        wait_file(runs, "before");
        if phase == "accepted" {
            fs::write(runs.join("before-go"), b"").unwrap();
            wait_file(runs, "accepted");
        }
        age_claim(runs);
        let before = fs::read(pending(runs)).unwrap();
        assert!(ExecutionGate::try_acquire(runs, KEY).unwrap().is_none());
        owner.0.kill().unwrap();
        owner.0.wait().unwrap();
        let gate = ExecutionGate::try_acquire(runs, KEY).unwrap().unwrap();
        assert_eq!(fs::read(pending(runs)).unwrap(), before);
        // Preserve existing same-key adoption only; this is NOT evidence that
        // a death after acceptance can be safely replayed for every route.
        let old: serde_json::Value = serde_json::from_slice(&before).unwrap();
        match durable::claim_durable_pending_gated(
            &gate,
            KEY,
            &request_digest("start_run", "", b"gate", None),
            None,
            policy::IDEMPOTENCY_TTL,
            &Default::default(),
        )
        .unwrap()
        {
            ClaimOutcome::Owner {
                run_id, generation, ..
            } => {
                assert_eq!(run_id.as_deref(), old["run_id"].as_str());
                assert!(generation > old["generation"].as_u64().unwrap());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

#[tokio::test]
async fn live_conflict_is_reported_without_waiting_for_the_gate() {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-gate-conflict-{}", uuid::Uuid::now_v7())),
    );
    let gate = ExecutionGate::try_acquire(&fixture.0, KEY)
        .unwrap()
        .unwrap();
    durable::claim_durable_pending_gated(
        &gate,
        KEY,
        "original",
        None,
        policy::IDEMPOTENCY_TTL,
        &Default::default(),
    )
    .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        acquire_execution_gate(fixture.0.clone(), KEY.into(), "different".into()),
    )
    .await
    .unwrap()
    .err()
    .unwrap();
    assert_eq!(error.problem.status, 409);
    // Dropping a waiter cannot leave a blocking lock acquisition alive.
    {
        let wait = acquire_execution_gate(fixture.0.clone(), KEY.into(), "original".into());
        tokio::pin!(wait);
        assert!(futures_util::poll!(&mut wait).is_pending());
    }
    drop(gate);
}

#[tokio::test]
async fn cancel_process_crash_before_ready_reuses_mailbox_identity() {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-cancel-crash-{}", uuid::Uuid::now_v7())),
    );
    let runs = &fixture.0;
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let owner = service::LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        service::RunStoreMode::SharedFilesystem,
        service::ServiceDeploymentPolicy::default(),
    )
    .unwrap();
    let id = owner
        .start_run(api::StartRun {
            generator_id: "ask-user".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while owner.snapshot(id.clone()).await.unwrap().state != api::RunStatus::Waiting {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let run_dir = owner.run_dir_for(&id).await.unwrap();
    let lease = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(run_dir.join("meta/execution.lock"))
        .unwrap();
    loop {
        match lease.try_lock() {
            Ok(()) => break,
            Err(fs::TryLockError::WouldBlock) => tokio::task::yield_now().await,
            Err(error) => panic!("lease failed: {error:?}"),
        }
        assert!(Instant::now() < deadline);
    }
    fs::write(runs.join("cancel-target"), &id).unwrap();
    // This single-threaded runtime stays blocked at the process barriers, so
    // the owner cannot drain the mailbox before we inspect and kill the request.
    let mut request = spawn(runs, "cancel-crash");
    wait_file(runs, "cancel-accepted");
    let operation = fs::read_to_string(runs.join("cancel-operation")).unwrap();
    let claim: serde_json::Value =
        serde_json::from_slice(&fs::read(pending(runs)).unwrap()).unwrap();
    assert_eq!(claim["operation_id"], operation);
    let mailbox = run_dir
        .join("meta/control")
        .join(format!("cancel-{operation}.json"));
    let original = fs::read(&mailbox).unwrap();
    request.0.kill().unwrap();
    request.0.wait().unwrap();
    assert_eq!(fs::read(&mailbox).unwrap(), original);
    age_claim(runs);
    drop(lease);
    let mut retry = spawn(runs, "cancel-retry");
    assert!(retry.success());
    assert!(
        !mailbox.exists(),
        "successful settlement consumes the accepted mailbox"
    );
    assert!(!pending(runs).exists());
    assert!(
        durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
            .unwrap()
            .is_some()
    );
    owner
        .cancel_with_operation_id(id.clone(), operation.clone())
        .await
        .unwrap();
    owner
        .cancel_with_operation_id(id.clone(), operation.clone())
        .await
        .unwrap();
    let journal = fs::read_to_string(run_dir.join("meta/journal.jsonl")).unwrap();
    let accepted: Vec<serde_json::Value> = journal
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|event: &serde_json::Value| event["t"] == "user_cancel_requested")
        .collect();
    assert_eq!(
        accepted.len(),
        1,
        "one logical cancel must journal once: {journal}"
    );
    assert_eq!(accepted[0]["operation_id"], operation);
    assert!(
        !mailbox.exists(),
        "terminal retry must not leave phantom control"
    );
}

#[tokio::test]
async fn direct_and_unkeyed_cancel_keep_fresh_operation_ids() {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-cancel-fresh-{}", uuid::Uuid::now_v7())),
    );
    let service = crate::test_service(
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators"),
        fixture.0.clone(),
        None,
    )
    .unwrap();
    let state = Arc::new(crate::test_state(
        service,
        fixture.0.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let id = state
        .service
        .start_run(api::StartRun {
            generator_id: "ask-user".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while state.service.snapshot(id.clone()).await.unwrap().state != api::RunStatus::Waiting {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let run_dir = state.service.run_dir_for(&id).await.unwrap();
    let lease = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(run_dir.join("meta/execution.lock"))
        .unwrap();
    loop {
        match lease.try_lock() {
            Ok(()) => break,
            Err(fs::TryLockError::WouldBlock) => tokio::task::yield_now().await,
            Err(error) => panic!("lease failed: {error:?}"),
        }
        assert!(Instant::now() < deadline);
    }
    for _ in 0..2 {
        assert!(matches!(
            state.service.cancel(id.clone()).await,
            Err(api::ApiError::Conflict { .. })
        ));
        let error = crate::server::run_detail::cancel_run(
            axum::extract::State(state.clone()),
            HeaderMap::new(),
            axum::extract::Path(id.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(error.problem.status, 409);
    }
    let controls: Vec<serde_json::Value> = fs::read_dir(run_dir.join("meta/control"))
        .unwrap()
        .map(|entry| serde_json::from_slice(&fs::read(entry.unwrap().path()).unwrap()).unwrap())
        .collect();
    let ids: std::collections::BTreeSet<_> = controls
        .iter()
        .map(|control| control["operation_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids.len(),
        4,
        "direct and unkeyed calls must each use fresh IDs"
    );
    assert!(ids.iter().all(|id| uuid::Uuid::parse_str(id).is_ok()));
    assert!(state.idempotency.lock().await.is_empty());
    drop(lease);
    state.service.cancel(id.clone()).await.unwrap();
    state.service.cancel(id.clone()).await.unwrap();
    assert_eq!(
        fs::read_dir(run_dir.join("meta/control")).unwrap().count(),
        0
    );
}

#[tokio::test]
async fn answer_process_crash_after_acceptance_retries_to_ready() {
    interaction_process_crash("answer").await;
}

#[tokio::test]
async fn confirm_process_crash_after_acceptance_retries_to_ready() {
    interaction_process_crash("confirm").await;
}

// Process termination on the shared filesystem only, not a power-loss test.
async fn interaction_process_crash(scope: &str) {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-{scope}-crash-{}", uuid::Uuid::now_v7())),
    );
    let runs = &fixture.0;
    let owner = shared_service(runs);
    let id = owner
        .start_run(api::StartRun {
            generator_id: if scope == "answer" {
                "ask-user"
            } else {
                "side-effect-confirm"
            }
            .into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let interaction = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = owner.snapshot(id.clone()).await.unwrap();
            if scope == "answer" && snapshot.state == api::RunStatus::Waiting {
                break snapshot.question.unwrap().id;
            }
            if scope == "confirm" && snapshot.state == api::RunStatus::Confirming {
                break snapshot.confirm.unwrap().id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let run_dir = owner.run_dir_for(&id).await.unwrap();
    let lease = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(run_dir.join("meta/execution.lock"))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match lease.try_lock() {
            Ok(()) => break,
            Err(fs::TryLockError::WouldBlock) => tokio::task::yield_now().await,
            Err(error) => panic!("lease failed: {error:?}"),
        }
        assert!(Instant::now() < deadline, "execution lease timed out");
    }
    drop(owner);
    fs::write(
        runs.join("interaction-target"),
        serde_json::to_vec(&(&id, &interaction)).unwrap(),
    )
    .unwrap();
    let accepted_events = || {
        fs::read_to_string(run_dir.join("meta/journal.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|event| {
                event["t"]
                    == if scope == "answer" {
                        "user_answered"
                    } else {
                        "user_confirmed"
                    }
            })
            .collect::<Vec<_>>()
    };
    let mut request = spawn(runs, &format!("{scope}-crash"));
    wait_file(runs, "interaction-accepted");
    let original = fs::read(pending(runs)).unwrap();
    let claim: serde_json::Value = serde_json::from_slice(&original).unwrap();
    assert_eq!(claim["run_id"], id);
    assert_eq!(
        claim["digest"],
        request_digest(scope, &id, &interaction_body(scope, &interaction), None)
    );
    assert!(
        durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
            .unwrap()
            .is_none()
    );
    let accepted = accepted_events();
    assert_eq!(accepted.len(), 1);
    if scope == "answer" {
        assert_eq!(accepted[0]["question_id"], interaction);
        assert_eq!(
            accepted[0]["values"],
            serde_json::json!(answer_payload().values)
        );
    } else {
        assert_eq!(accepted[0]["confirmation_id"], interaction);
        assert_eq!(accepted[0]["approved"], true);
    }
    request.0.kill().unwrap();
    request.0.wait().unwrap();
    assert_eq!(
        fs::read(pending(runs)).unwrap(),
        original,
        "process death must preserve Pending"
    );
    assert_eq!(accepted_events(), accepted);
    assert!(
        durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
            .unwrap()
            .is_none()
    );
    // Controlled lease-expiry seam from the stable-key claim tests: only
    // created_at changes, not the digest, target, owner or reserved run.
    age_claim(runs);
    let state = Arc::new(crate::test_state(
        shared_service(runs),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let mut headers = HeaderMap::new();
    headers.insert(policy::IDEMPOTENCY_HEADER, KEY.parse().unwrap());
    let response = tokio::time::timeout(Duration::from_secs(5), async {
        let path = axum::extract::Path((id.clone(), interaction.clone()));
        if scope == "answer" {
            crate::server::run_detail::answer_run(
                axum::extract::State(state.clone()),
                headers,
                path,
                axum::Json(answer_payload()),
            )
            .await
        } else {
            crate::server::run_detail::confirm_run(
                axum::extract::State(state.clone()),
                headers,
                path,
                axum::Json(confirm_decision()),
            )
            .await
        }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let snapshot: api::RunSnapshot = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(snapshot.run_id, id);
    assert!(!pending(runs).exists());
    let ready = durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
        .unwrap()
        .unwrap();
    assert_eq!(ready.run_id, id);
    assert_eq!(ready.digest, claim["digest"].as_str().unwrap());
    assert_eq!(
        accepted_events(),
        accepted,
        "retry must preserve the single original acceptance"
    );
    // Do not launch the approved command when releasing the test-owned lease.
    state.service.mark_shutting_down();
    drop(lease);
}
