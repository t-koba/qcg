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
fn pending_for(runs: &Utf8PathBuf, key: &str) -> Utf8PathBuf {
    runs.join("idempotency").join(format!(
        "{}.pending.json",
        hex::encode(Sha256::digest(key.as_bytes()))
    ))
}
fn pending(runs: &Utf8PathBuf) -> Utf8PathBuf {
    pending_for(runs, KEY)
}
fn age_claim_for(runs: &Utf8PathBuf, key: &str) -> Vec<u8> {
    // Controlled clock seam, never a product mutation: only the timestamp changes.
    let path = pending_for(runs, key);
    let original = fs::read(&path).unwrap();
    let mut record: serde_json::Value = serde_json::from_slice(&original).unwrap();
    record["created_at_unix"] = 1.into();
    fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
    original
}
fn age_claim(runs: &Utf8PathBuf) -> Vec<u8> {
    age_claim_for(runs, KEY)
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
fn confirm_decision(deny: bool) -> api::ConfirmDecision {
    api::ConfirmDecision {
        decision: if deny {
            api::ConfirmationDecision::Deny
        } else {
            api::ConfirmationDecision::Approve
        },
    }
}
fn interaction_body(scope: &str, interaction: &str, deny: bool) -> Vec<u8> {
    // Match run_detail's digest: interaction id, NUL, then the typed JSON body.
    let mut body = interaction.as_bytes().to_vec();
    body.push(0);
    body.extend(if scope == "answer" {
        serde_json::to_vec(&answer_payload()).unwrap()
    } else {
        serde_json::to_vec(&confirm_decision(deny)).unwrap()
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
    if let Some(scope @ ("start_run" | "fork_run")) = mode
        .strip_suffix("-pre-admission")
        .or_else(|| mode.strip_suffix("-post-acceptance"))
    {
        let body = fs::read(runs.join("creation-request")).unwrap();
        let target = fs::read_to_string(runs.join("creation-target")).unwrap();
        let package = fs::read_to_string(runs.join("creation-package")).unwrap();
        let reserved = if scope == "start_run" {
            let request: api::StartRun = serde_json::from_slice(&body).unwrap();
            state
                .service
                .reserve_start_run_id(&request.generator_id)
                .unwrap()
        } else {
            state.service.reserve_fork_run_id(&target).await.unwrap()
        };
        let mut headers = HeaderMap::new();
        headers.insert(policy::IDEMPOTENCY_HEADER, KEY.parse().unwrap());
        with_idempotency(IdempotentCall {
            state: &state,
            headers: &headers,
            scope,
            target: &target,
            body: &body,
            digest_context: (scope == "start_run").then_some(package.as_bytes()),
            created: true,
            reserved_run_id: Some(reserved.clone()),
            execute: |context| async {
                assert_eq!(context.reserved_run_id.as_ref(), Some(&reserved));
                assert!(!runs.join(&reserved).exists());
                if mode.ends_with("-pre-admission") {
                    // The gate and Pending reservation exist, but neither service
                    // admission API has been called. The parent kills us here.
                    barrier(&runs, "creation-before-admission").await;
                }
                let id = if scope == "start_run" {
                    state
                        .service
                        .start_run_with_package_identity(
                            serde_json::from_slice(&body).unwrap(),
                            context.reserved_run_id,
                            &package,
                        )
                        .await
                } else {
                    state
                        .service
                        .fork_run_with_id(
                            &target,
                            serde_json::from_slice(&body).unwrap(),
                            context.reserved_run_id,
                        )
                        .await
                }?;
                assert_eq!(id, reserved);
                if mode.ends_with("-post-acceptance") {
                    assert!(runs.join(&reserved).exists());
                    // Service admission returned the reserved run, but this
                    // closure has not returned, so Ready cannot be published.
                    barrier(&runs, "creation-after-acceptance").await;
                }
                Ok(id)
            },
        })
        .await
        .unwrap();
        panic!("parent must kill request at the creation barrier");
    }
    let denial_prefix = mode
        .strip_prefix("deny-")
        .and_then(|mode| mode.strip_suffix("-crash"))
        .map(|prefix| prefix.parse::<usize>().unwrap());
    if let Some(scope @ ("answer" | "confirm")) = denial_prefix
        .map(|_| "confirm")
        .or_else(|| mode.strip_suffix("-crash"))
    {
        let (id, interaction): (String, String) =
            serde_json::from_slice(&fs::read(runs.join("interaction-target")).unwrap()).unwrap();
        let body = interaction_body(scope, &interaction, denial_prefix.is_some());
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
                    } else if let Some(prefix @ 0..=1) = denial_prefix {
                        // Materialize the exact durable service prefix at a
                        // deterministic crash boundary, inside a real Pending
                        // operation/gate. No timing race or power-loss claim.
                        let path = runs.join(id).join("meta/journal.jsonl");
                        let observed = engine::RunState::fold_journal(&path).unwrap();
                        let Some(engine::Interaction::Confirmation { confirm }) =
                            observed.settled_pending()
                        else {
                            panic!("missing prompt")
                        };
                        assert_eq!(confirm.id, interaction);
                        let mut events = vec![(
                            "user_confirmed",
                            serde_json::json!({
                                "confirmation_id": interaction, "approved": false,
                            }),
                        )];
                        if prefix == 1 {
                            events.push((
                                "side_effect",
                                serde_json::json!({
                                    "kind": confirm.kind, "target": confirm.target,
                                    "decision": "denied_by_user",
                                }),
                            ));
                        }
                        engine::JournalWriter::append_events_if(
                            &path,
                            id,
                            events,
                            engine::JournalLimits::default(),
                            None,
                            |state| {
                                assert_eq!(state.last_seq, observed.last_seq);
                                Ok(())
                            },
                        )
                        .unwrap();
                    } else {
                        state
                            .service
                            .confirm(
                                id.clone(),
                                interaction,
                                confirm_decision(denial_prefix.is_some()),
                            )
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
async fn aborted_interaction_requests_after_acceptance_retry_to_ready() {
    for scope in ["answer", "confirm"] {
        aborted_interaction_request_after_acceptance(scope).await;
    }
}

async fn aborted_interaction_request_after_acceptance(scope: &str) {
    let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("qcg-{scope}-abort-{}", uuid::Uuid::now_v7()));
    let _fixture = Fixture(runs.clone());
    let state = Arc::new(crate::test_state(
        shared_service(&runs),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let id = state
        .service
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
            let snapshot = state.service.snapshot(id.clone()).await.unwrap();
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
    let key = format!("{scope}-abort-after-acceptance");
    let mut headers = HeaderMap::new();
    headers.insert(policy::IDEMPOTENCY_HEADER, key.parse().unwrap());
    let body = interaction_body(scope, &interaction, scope == "confirm");
    let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
    let (_resume_tx, resume_rx) = tokio::sync::oneshot::channel::<()>();
    let owner_state = state.clone();
    let owner_headers = headers.clone();
    let owner_id = id.clone();
    let owner_interaction = interaction.clone();
    let owner_body = body.clone();
    let owner_scope = scope.to_owned();
    let request = tokio::spawn(async move {
        with_idempotency(IdempotentCall {
            state: &owner_state,
            headers: &owner_headers,
            scope: &owner_scope,
            target: &owner_id,
            body: &owner_body,
            digest_context: None,
            created: false,
            reserved_run_id: Some(owner_id.clone()),
            execute: |context| {
                let state = &owner_state;
                let id = &owner_id;
                let interaction = &owner_interaction;
                let scope = &owner_scope;
                async move {
                    assert_eq!(context.reserved_run_id.as_ref(), Some(id));
                    if scope == "answer" {
                        state
                            .service
                            .answer(id.clone(), interaction.clone(), answer_payload())
                            .await?;
                    } else {
                        state
                            .service
                            .confirm(id.clone(), interaction.clone(), confirm_decision(true))
                            .await?;
                    }
                    accepted.send(()).unwrap();
                    let _ = resume_rx.await;
                    Ok(id.clone())
                }
            },
        })
        .await
    });
    accepted_rx.await.unwrap();
    let pending_path = pending_for(&runs, &key);
    let original = fs::read(&pending_path).expect("accepted request should retain Pending");
    assert!(
        durable::load_durable_ready_result(&runs, &key, policy::IDEMPOTENCY_TTL)
            .unwrap()
            .is_none(),
        "the request is paused before Ready publication"
    );
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !state.idempotency.lock().await.contains_key(&key) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("aborted request must withdraw its in-memory waiter");
    assert_eq!(fs::read(&pending_path).unwrap(), original);
    let released = ExecutionGate::try_acquire(&runs, &key).unwrap();
    assert!(released.is_some(), "aborted request must release its gate");
    drop(released);
    age_claim_for(&runs, &key);

    let retry = with_idempotency(IdempotentCall {
        state: &state,
        headers: &headers,
        scope,
        target: &id,
        body: &body,
        digest_context: None,
        created: false,
        reserved_run_id: Some(id.clone()),
        execute: |context| {
            let state = &state;
            let id = &id;
            let interaction = &interaction;
            async move {
                assert_eq!(context.reserved_run_id.as_ref(), Some(id));
                if scope == "answer" {
                    state
                        .service
                        .answer(id.clone(), interaction.clone(), answer_payload())
                        .await?;
                } else {
                    state
                        .service
                        .confirm(id.clone(), interaction.clone(), confirm_decision(true))
                        .await?;
                }
                Ok(id.clone())
            }
        },
    })
    .await
    .unwrap();
    assert_eq!(retry.status(), axum::http::StatusCode::OK);
    let ready = durable::load_durable_ready_result(&runs, &key, policy::IDEMPOTENCY_TTL)
        .unwrap()
        .unwrap();
    assert_eq!(ready.run_id, id);
    let journal = runs.join(&ready.run_id).join("meta/journal.jsonl");
    let events = engine::read_journal_values(&journal, engine::JournalLimits::default())
        .unwrap()
        .events;
    let accepted_event = if scope == "answer" {
        "user_answered"
    } else {
        "user_confirmed"
    };
    assert_eq!(
        events
            .iter()
            .filter(|event| event["t"] == accepted_event)
            .count(),
        1,
        "retry after request cancellation must reuse the accepted interaction"
    );
}

#[tokio::test]
async fn answer_process_crash_after_acceptance_retries_to_ready() {
    interaction_process_crash("answer", None).await;
}

#[tokio::test]
async fn confirm_process_crash_after_acceptance_retries_to_ready() {
    interaction_process_crash("confirm", None).await;
}

// Process termination on the shared filesystem only, not a power-loss test.
#[tokio::test]
async fn denied_confirmation_prefix_process_crash_retries_to_ready() {
    // false only, false + side_effect, and fully settled before Ready.
    for prefix in 0..=2 {
        interaction_process_crash("confirm", Some(prefix)).await;
    }
}

#[tokio::test]
async fn terminal_answer_process_crash_before_ready_retries_to_ready() {
    interaction_process_crash_with_lease("answer", None, false).await;
}

#[tokio::test]
async fn terminal_approved_confirmation_process_crash_before_ready_retries_to_ready() {
    interaction_process_crash_with_lease("confirm", None, false).await;
}

async fn interaction_process_crash(scope: &str, denial_prefix: Option<usize>) {
    interaction_process_crash_with_lease(scope, denial_prefix, true).await;
}

async fn interaction_process_crash_with_lease(
    scope: &str,
    denial_prefix: Option<usize>,
    hold_execution_lease: bool,
) {
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
    let lease = if hold_execution_lease {
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
        Some(lease)
    } else {
        None
    };
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
    let mode = denial_prefix.map_or_else(
        || format!("{scope}-crash"),
        |prefix| format!("deny-{prefix}-crash"),
    );
    let mut request = spawn(runs, &mode);
    wait_file(runs, "interaction-accepted");
    let original = fs::read(pending(runs)).unwrap();
    let claim: serde_json::Value = serde_json::from_slice(&original).unwrap();
    assert_eq!(claim["run_id"], id);
    assert_eq!(
        claim["digest"],
        request_digest(
            scope,
            &id,
            &interaction_body(scope, &interaction, denial_prefix.is_some()),
            None
        )
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
        assert_eq!(accepted[0]["approved"], denial_prefix.is_none());
    }
    if !hold_execution_lease {
        let journal = run_dir.join("meta/journal.jsonl");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if engine::RunState::fold_journal(&journal)
                    .unwrap()
                    .terminal
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("accepted operation must finish before the request process is killed");
        assert!(
            durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
                .unwrap()
                .is_none(),
            "terminal run must still have no Ready while the request is blocked"
        );
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
    if denial_prefix.is_some() {
        lease.as_ref().unwrap().unlock().unwrap();
    }
    if !hold_execution_lease {
        let _maintenance = service::try_lock_store_maintenance(runs).unwrap().unwrap();
        let deleted = _maintenance.gc_run_directories(0, 0, 100).unwrap();
        assert!(
            !deleted.contains(&run_dir),
            "accepted Pending pins the terminal journal before restart"
        );
        assert!(run_dir.join("meta/journal.jsonl").is_file());
    }
    let aged_pending = fs::read(pending(runs)).unwrap();
    let state = Arc::new(crate::test_state(
        shared_service(runs),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    if denial_prefix.is_some() {
        state.service.resume_recovered_runs().await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = state.service.snapshot(id.clone()).await.unwrap();
                if snapshot.state == api::RunStatus::Failed {
                    assert!(snapshot.confirm.is_none());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("restart recovery must settle denial without a retry");

        assert_eq!(
            fs::read(pending(runs)).unwrap(),
            aged_pending,
            "service recovery must retain Pending until Ready commits"
        );
        assert!(
            durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
                .unwrap()
                .is_none()
        );
    }
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
                axum::Json(confirm_decision(denial_prefix.is_some())),
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
    if denial_prefix.is_some() {
        assert_eq!(snapshot.state, api::RunStatus::Failed);
        assert!(snapshot.confirm.is_none());
        assert!(!run_dir.join("workspace/after.txt").exists());
        let path = run_dir.join("meta/journal.jsonl");
        let settled = fs::read(&path).unwrap();
        let events = engine::read_journal_values(&path, engine::JournalLimits::default())
            .unwrap()
            .events;
        assert_eq!(
            events
                .iter()
                .filter(|e| e["decision"] == "denied_by_user")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e["t"] == "run_finished" && e["status"] == "failed")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e["t"] == "confirm_request")
                .count(),
            1
        );
        assert!(
            !events
                .iter()
                .any(|e| e["decision"] == "approved_by_user" || e["t"] == "command_started")
        );
        // In the fully settled prefix case, recovery omitted the terminal
        // run from its map. Durable replay must still work.
        state
            .service
            .confirm(id.clone(), interaction.clone(), confirm_decision(true))
            .await
            .unwrap();
        state.service.resume_recovered_runs().await;
        assert_eq!(fs::read(&path).unwrap(), settled);
    } else if !hold_execution_lease {
        assert_eq!(snapshot.state, api::RunStatus::Succeeded);
        if scope == "confirm" {
            assert!(run_dir.join("workspace/after.txt").exists());
            let events = engine::read_journal_values(
                &run_dir.join("meta/journal.jsonl"),
                engine::JournalLimits::default(),
            )
            .unwrap()
            .events;
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event["t"] == "operation_started")
                    .count(),
                1,
                "terminal replay must not duplicate operation execution"
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event["t"] == "operation_finished")
                    .count(),
                1
            );
        }
        let journal = run_dir.join("meta/journal.jsonl");
        let settled = fs::read(&journal).unwrap();
        let conflict = if scope == "answer" {
            state
                .service
                .answer(
                    id.clone(),
                    interaction.clone(),
                    api::AnswerPayload {
                        values: std::collections::BTreeMap::from([(
                            "answer".into(),
                            serde_json::json!("different"),
                        )]),
                    },
                )
                .await
        } else {
            state
                .service
                .confirm(id.clone(), interaction.clone(), confirm_decision(true))
                .await
        };
        assert!(matches!(conflict, Err(api::ApiError::Conflict { .. })));
        assert_eq!(fs::read(&journal).unwrap(), settled);
        let missing = if scope == "answer" {
            state
                .service
                .answer(
                    id.clone(),
                    format!("{interaction}-missing"),
                    answer_payload(),
                )
                .await
        } else {
            state
                .service
                .confirm(
                    id.clone(),
                    format!("{interaction}-missing"),
                    confirm_decision(false),
                )
                .await
        };
        assert!(matches!(missing, Err(api::ApiError::Invalid { .. })));
        assert_eq!(fs::read(&journal).unwrap(), settled);
    }
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

#[tokio::test]
async fn start_process_crash_before_admission_retries_reserved_run_to_ready() {
    creation_process_crash("start_run").await;
}

#[tokio::test]
async fn fork_process_crash_before_admission_retries_reserved_run_to_ready() {
    creation_process_crash("fork_run").await;
}

#[tokio::test]
async fn start_process_crash_after_acceptance_adopts_reserved_run_to_ready() {
    creation_process_crash_at("start_run", true).await;
}

#[tokio::test]
async fn fork_process_crash_after_acceptance_adopts_reserved_run_to_ready() {
    creation_process_crash_at("fork_run", true).await;
}

// Process termination on the shared filesystem only, not power-loss durability
// or evidence for general Pending retirement.
async fn creation_process_crash(scope: &str) {
    creation_process_crash_at(scope, false).await;
}

async fn creation_process_crash_at(scope: &str, accepted: bool) {
    let phase = if accepted {
        "post-acceptance"
    } else {
        "pre-admission"
    };
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-{scope}-{phase}-{}", uuid::Uuid::now_v7())),
    );
    let runs = &fixture.0;
    let owner = shared_service(runs);
    let request = api::StartRun {
        generator_id: "hello-template".into(),
        inputs: std::collections::BTreeMap::from([("name".into(), serde_json::json!("gate"))]),
        ..Default::default()
    };
    let (target, body, package) = if scope == "start_run" {
        let package = owner
            .describe(&request.generator_id)
            .await
            .unwrap()
            .package_sha256;
        (
            String::new(),
            serde_json::to_vec(&request).unwrap(),
            package,
        )
    } else {
        let source = owner.start_run(request).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !owner
                .snapshot(source.clone())
                .await
                .unwrap()
                .state
                .is_terminal()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let journal = fs::read_to_string(
            owner
                .run_dir_for(&source)
                .await
                .unwrap()
                .join("meta/journal.jsonl"),
        )
        .unwrap();
        let at_seq = journal
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|event| event["t"] == "run_started")
            .unwrap()["seq"]
            .as_u64()
            .unwrap();
        let request = api::ForkRun {
            at_seq,
            state_patch: api::ForkStatePatch {
                inputs: std::collections::BTreeMap::from([(
                    "name".into(),
                    serde_json::json!("forked-gate"),
                )]),
                ..Default::default()
            },
            ..Default::default()
        };
        (source, serde_json::to_vec(&request).unwrap(), String::new())
    };
    // In particular, release the fork source's service before spawning a peer.
    owner.mark_shutting_down();
    drop(owner);
    fs::write(runs.join("creation-request"), &body).unwrap();
    fs::write(runs.join("creation-target"), &target).unwrap();
    fs::write(runs.join("creation-package"), &package).unwrap();
    let source_journal = (scope == "fork_run")
        .then(|| fs::read(runs.join(&target).join("meta/journal.jsonl")).unwrap());
    let mut child = spawn(runs, &format!("{scope}-{phase}"));
    wait_file(
        runs,
        if accepted {
            "creation-after-acceptance"
        } else {
            "creation-before-admission"
        },
    );
    let original = fs::read(pending(runs)).unwrap();
    let claim: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let reserved = claim["run_id"].as_str().unwrap().to_owned();
    let digest = request_digest(
        scope,
        &target,
        &body,
        (scope == "start_run").then_some(package.as_bytes()),
    );
    assert_eq!(claim["digest"], digest);
    let admissions = || {
        let journal = fs::read_to_string(runs.join(&reserved).join("meta/journal.jsonl")).unwrap();
        let events: Vec<serde_json::Value> = journal
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        // A fork copies source history. Count only its own admission, after
        // the fork marker, not the source's queued/started events.
        let first = if scope == "fork_run" {
            events
                .iter()
                .rposition(|event| event["t"] == "run_forked")
                .unwrap()
                + 1
        } else {
            0
        };
        events
            .into_iter()
            .skip(first)
            .filter(|event| event["t"] == "run_queued")
            .collect::<Vec<_>>()
    };
    let original_admissions = accepted.then(|| {
        let events = admissions();
        assert_eq!(
            events.len(),
            1,
            "service durably admitted the reserved run once"
        );
        events
    });
    if accepted {
        assert!(runs.join(&reserved).exists(), "reserved run was admitted");
    } else {
        assert!(
            !runs.join(&reserved).exists(),
            "reserved run is not admitted"
        );
    }
    assert!(
        durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
            .unwrap()
            .is_none()
    );
    // No retry or concurrent claim read runs during this timestamp-only seam.
    // Age is not authority to steal the still-busy execution gate.
    assert_eq!(age_claim(runs), original);
    let aged = fs::read(pending(runs)).unwrap();
    let mut expected = claim.clone();
    expected["created_at_unix"] = 1.into();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&aged).unwrap(),
        expected
    );
    assert!(ExecutionGate::try_acquire(runs, KEY).unwrap().is_none());
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    assert_eq!(
        fs::read(pending(runs)).unwrap(),
        aged,
        "death retains Pending evidence"
    );
    if accepted {
        assert!(
            runs.join(&reserved).exists(),
            "death retains the admitted run"
        );
    } else {
        assert!(!runs.join(&reserved).exists());
    }
    assert!(
        durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
            .unwrap()
            .is_none()
    );
    let state = Arc::new(crate::test_state(
        shared_service(runs),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let before = state.service.list_run_items().await.unwrap();
    if accepted {
        assert_eq!(before.len(), 1 + usize::from(scope == "fork_run"));
        assert_eq!(
            before.iter().filter(|item| item.run_id == reserved).count(),
            1
        );
        if scope == "fork_run" {
            assert_eq!(
                before.iter().filter(|item| item.run_id == target).count(),
                1
            );
        }
    } else {
        assert_eq!(before.len(), usize::from(scope == "fork_run"));
        if scope == "fork_run" {
            assert_eq!(before[0].run_id, target);
        }
    }
    assert_eq!(
        fs::read(pending(runs)).unwrap(),
        aged,
        "restart retains Pending until retry"
    );
    let retry = || async {
        let mut headers = HeaderMap::new();
        headers.insert(policy::IDEMPOTENCY_HEADER, KEY.parse().unwrap());
        if scope == "start_run" {
            headers.insert("x-qcg-package-sha256", package.parse().unwrap());
            crate::server::runs::start_run(
                axum::extract::State(state.clone()),
                headers,
                axum::Json(serde_json::from_slice(&body).unwrap()),
            )
            .await
        } else {
            crate::server::runs::fork_run(
                axum::extract::State(state.clone()),
                headers,
                axum::extract::Path(target.clone()),
                axum::Json(serde_json::from_slice(&body).unwrap()),
            )
            .await
        }
    };
    let mut publication = None;
    for _ in 0..2 {
        let response = tokio::time::timeout(Duration::from_secs(10), retry())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::CREATED);
        assert_eq!(
            response.headers()[axum::http::header::LOCATION],
            format!("/api/runs/{reserved}")
        );
        let snapshot: api::RunSnapshot = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(snapshot.run_id, reserved);
        // Check the committed Ready mapping before checking Pending retirement.
        let ready = durable::load_durable_ready_result(runs, KEY, policy::IDEMPOTENCY_TTL)
            .unwrap()
            .unwrap();
        assert_eq!(ready.run_id, reserved);
        assert_eq!(ready.digest, digest);
        assert!(ready.generation > claim["generation"].as_u64().unwrap());
        if let Some(previous) = &publication {
            assert_eq!(
                &ready.publication_id, previous,
                "replay must not republish Ready"
            );
        }
        publication = Some(ready.publication_id);
        assert!(!pending(runs).exists());
        let mappings: Vec<serde_json::Value> = fs::read_dir(runs.join("idempotency"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .map(|path| serde_json::from_slice(&fs::read(path).unwrap()).unwrap())
            .collect();
        assert_eq!(mappings.len(), 1, "exactly one Ready mapping");
        assert_eq!(mappings[0]["key"], KEY);
        assert_eq!(mappings[0]["run_id"], reserved);
        let after = state.service.list_run_items().await.unwrap();
        if accepted {
            assert_eq!(
                after.len(),
                before.len(),
                "retry must adopt, not admit a new run"
            );
            assert_eq!(
                admissions(),
                *original_admissions.as_ref().unwrap(),
                "retry must preserve the single original admission"
            );
        } else {
            assert_eq!(after.len(), before.len() + 1, "exactly one new admission");
        }
        assert_eq!(
            after.iter().filter(|item| item.run_id == reserved).count(),
            1
        );
        if scope == "fork_run" {
            assert_eq!(after.iter().filter(|item| item.run_id == target).count(), 1);
            assert_eq!(
                fs::read(runs.join(&target).join("meta/journal.jsonl")).unwrap(),
                *source_journal.as_ref().unwrap(),
                "fork retry leaves the source journal intact"
            );
        }
    }
}

// One path-scoped barrier; other concurrently running fixtures are untouched.
type SnapshotBarrier = (
    Utf8PathBuf,
    Arc<tokio::sync::Notify>,
    Arc<tokio::sync::Notify>,
);
static SNAPSHOT_BARRIER: std::sync::Mutex<Option<SnapshotBarrier>> = std::sync::Mutex::new(None);
pub(super) async fn snapshot_barrier(runs: &Utf8PathBuf) {
    let notifications = SNAPSHOT_BARRIER
        .lock()
        .unwrap()
        .as_ref()
        .filter(|(path, ..)| path == runs)
        .map(|(_, reached, resume)| (reached.clone(), resume.clone()));
    if let Some((reached, resume)) = notifications {
        reached.notify_one();
        resume.notified().await;
    }
}

#[tokio::test]
async fn initial_and_replay_snapshot_admission_survive_physical_ready_expiry() {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-snapshot-retention-{}", uuid::Uuid::now_v7())),
    );
    let runs = &fixture.0;
    let state = Arc::new(crate::test_state(
        shared_service(runs),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let mut headers = HeaderMap::new();
    for replay in [false, true] {
        let key = format!("{KEY}-{replay}");
        headers.insert(policy::IDEMPOTENCY_HEADER, key.parse().unwrap());
        let id = state
            .service
            .start_run(api::StartRun {
                generator_id: "hello-template".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !state
                .service
                .snapshot(id.clone())
                .await
                .unwrap()
                .state
                .is_terminal()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let ready = runs.join("idempotency").join(format!(
            "{}.json",
            hex::encode(Sha256::digest(key.as_bytes()))
        ));
        if replay {
            drop(
                crate::server::run_detail::cancel_run(
                    axum::extract::State(state.clone()),
                    headers.clone(),
                    axum::extract::Path(id.clone()),
                )
                .await
                .unwrap(),
            );
        }
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        *SNAPSHOT_BARRIER.lock().unwrap() = Some((runs.clone(), reached.clone(), resume.clone()));
        let request = {
            let state = state.clone();
            let headers = headers.clone();
            let id = id.clone();
            tokio::spawn(async move {
                crate::server::run_detail::cancel_run(
                    axum::extract::State(state),
                    headers,
                    axum::extract::Path(id),
                )
                .await
            })
        };
        tokio::time::timeout(Duration::from_secs(5), reached.notified())
            .await
            .unwrap();
        assert!(
            ready.is_file(),
            "Ready must be committed before snapshot materialization"
        );
        // Controlled expiry-worker seam, under its actual metadata lock.
        let metadata = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(runs.join("idempotency/.idempotency.lock"))
            .unwrap();
        metadata.lock().unwrap();
        fs::remove_file(&ready).unwrap();
        metadata.unlock().unwrap();
        {
            let _maintenance = service::try_lock_store_maintenance(runs).unwrap().unwrap();
            let deleted = _maintenance.gc_run_directories(0, 0, 100).unwrap();
            assert!(
                !deleted.contains(&runs.join(&id)),
                "snapshot must hold admission beyond Ready retirement"
            );
        }
        resume.notify_one();
        let response = tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        drop(response);
        *SNAPSHOT_BARRIER.lock().unwrap() = None;
        let _maintenance = service::try_lock_store_maintenance(runs).unwrap().unwrap();
        assert!(
            _maintenance
                .gc_run_directories(0, 0, 100)
                .unwrap()
                .contains(&runs.join(&id))
        );
    }
}

#[tokio::test]
async fn stale_ready_is_revalidated_after_admission_wait() {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-stale-retention-{}", uuid::Uuid::now_v7())),
    );
    let runs = &fixture.0;
    let state = Arc::new(crate::test_state(
        shared_service(runs),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let id = state
        .service
        .start_run(api::StartRun {
            generator_id: "hello-template".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let admission = state.service.lock_idempotency_snapshot(&id).await.unwrap();
    // No canonical Ready exists: a stale cached/observed identity cannot serve.
    let record = durable::DurableIdempotencyRecord {
        key: KEY.into(),
        digest: "d".into(),
        run_id: id,
        generation: 1,
        created_at_unix: durable::now_unix().unwrap(),
        publication_id: None,
    };
    let request = {
        let state = state.clone();
        tokio::spawn(async move { replay_ready(&state, record, false, "d").await })
    };
    tokio::task::yield_now().await;
    assert!(!request.is_finished());
    drop(admission);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .problem
            .status
            == 500
    );
}

#[tokio::test]
async fn mutation_claim_fails_fast_during_maintenance_and_rechecks_deleted_target() {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-target-retention-{}", uuid::Uuid::now_v7())),
    );
    let runs = &fixture.0;
    fs::create_dir_all(runs.join("target/meta")).unwrap();
    fs::write(runs.join("target/meta/journal.jsonl"), b"journal").unwrap();
    let gate = ExecutionGate::try_acquire(runs, KEY).unwrap().unwrap();
    let maintenance = service::try_lock_store_maintenance(runs).unwrap().unwrap();
    // Holding metadata too proves the busy path never waits on it under the shard.
    let metadata = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(runs.join("idempotency/.idempotency.lock"))
        .unwrap();
    metadata.lock().unwrap();
    let claim = || {
        durable::claim_durable_pending_for_target_gated(
            &gate,
            KEY,
            "d",
            Some("target".into()),
            policy::IDEMPOTENCY_TTL,
            &Default::default(),
            true,
        )
    };
    assert_eq!(claim().unwrap_err().problem.status, 503);
    assert!(!pending(runs).exists());
    fs::remove_dir_all(runs.join("target")).unwrap();
    metadata.unlock().unwrap();
    drop(maintenance);
    assert_eq!(claim().unwrap_err().problem.status, 404);
    assert!(
        !pending(runs).exists(),
        "deleted target cannot receive dangling Pending"
    );
}

#[tokio::test]
async fn ready_replay_distinguishes_loss_expiry_and_canonical_replacement() {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-ready-loss-{}", uuid::Uuid::now_v7())),
    );
    let runs = &fixture.0;
    let state = Arc::new(crate::test_state(
        shared_service(runs),
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let record = durable::DurableIdempotencyRecord {
        key: KEY.into(),
        digest: request_digest("cancel_run", "target", b"", None),
        run_id: "target".into(),
        generation: 1,
        created_at_unix: durable::now_unix().unwrap(),
        publication_id: Some("publication".into()),
    };
    let mut headers = HeaderMap::new();
    headers.insert(policy::IDEMPOTENCY_HEADER, KEY.parse().unwrap());
    state
        .idempotency
        .lock()
        .await
        .insert(KEY.into(), IdempotencyEntry::ready(record.clone()));
    let called = std::sync::atomic::AtomicBool::new(false);
    let error = with_idempotency(IdempotentCall {
        state: &state,
        headers: &headers,
        scope: "cancel_run",
        target: "target",
        body: b"",
        digest_context: None,
        created: false,
        reserved_run_id: Some("target".into()),
        execute: |_| async {
            called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok("target".into())
        },
    })
    .await
    .unwrap_err();
    assert_eq!(error.problem.status, 500);
    assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
    assert!(state.idempotency.lock().await.contains_key(KEY));
    assert!(!pending(runs).exists());
    let mut expired = record.clone();
    expired.created_at_unix = 0;
    assert!(
        replay_ready(&state, expired.clone(), false, "anything")
            .await
            .unwrap()
            .is_none()
    );
    let ready = runs.join("idempotency").join(format!(
        "{}.json",
        hex::encode(Sha256::digest(KEY.as_bytes()))
    ));
    fs::create_dir_all(ready.parent().unwrap()).unwrap();
    // An actually different canonical publication permits re-admission, even
    // when the replacement is already expired under this peer's frozen TTL.
    expired.publication_id = Some("replacement".into());
    fs::write(&ready, serde_json::to_vec(&expired).unwrap()).unwrap();
    assert!(
        replay_ready(&state, record.clone(), false, "anything")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        replay_ready(&state, expired.clone(), false, "anything")
            .await
            .unwrap()
            .is_none()
    );
    fs::write(&ready, serde_json::to_vec(&record).unwrap()).unwrap();
    assert_eq!(
        replay_ready(&state, record, false, "different")
            .await
            .unwrap_err()
            .problem
            .status,
        409
    );
    assert!(
        replay_ready(&state, expired, false, "anything")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn start_lookup_checks_live_memory_pending_digest_before_disk() {
    let fixture = Fixture(
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-pending-lookup-{}", uuid::Uuid::now_v7())),
    );
    let state = Arc::new(crate::test_state(
        shared_service(&fixture.0),
        fixture.0.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
    let digest = request_digest("start_run", "", b"same", None);
    let mut headers = HeaderMap::new();
    headers.insert(policy::IDEMPOTENCY_HEADER, KEY.parse().unwrap());
    let (completed, _) = tokio::sync::watch::channel(false);
    state.idempotency.lock().await.insert(
        KEY.into(),
        IdempotencyEntry::Pending {
            digest,
            owner_id: uuid::Uuid::now_v7(),
            created_at: Instant::now(),
            completed,
        },
    );
    for body in [b"same".as_slice(), b"different".as_slice()] {
        let call = IdempotentCall {
            state: &state,
            headers: &headers,
            scope: "start_run",
            target: "",
            body,
            digest_context: None,
            created: true,
            reserved_run_id: None,
            execute: |_| async { panic!("lookup never executes") },
        };
        let result = lookup_start_idempotency(&call).await;
        if body == b"same" {
            assert!(matches!(result, Ok(StartIdempotencyLookup::Pending)));
        } else {
            assert_eq!(result.err().unwrap().problem.status, 409);
        }
    }
}

#[cfg(unix)]
#[test]
fn mutation_claim_rejects_linked_and_non_directory_parents_before_publication() {
    use std::os::unix::fs::symlink;
    for component in ["target", "target/meta", "target/meta/journal.jsonl"] {
        for linked in [false, true] {
            let fixture = Fixture(
                Utf8PathBuf::from_path_buf(std::env::temp_dir())
                    .unwrap()
                    .join(format!("qcg-target-path-{}", uuid::Uuid::now_v7())),
            );
            let runs = &fixture.0;
            fs::create_dir_all(runs.join("target/meta")).unwrap();
            fs::write(runs.join("target/meta/journal.jsonl"), b"journal").unwrap();
            let path = runs.join(component);
            let original = runs.join("original");
            fs::rename(&path, &original).unwrap();
            if linked {
                symlink(&original, &path).unwrap();
            } else if component.ends_with("jsonl") {
                fs::create_dir(&path).unwrap();
            } else {
                fs::write(&path, b"not a directory").unwrap();
            }
            let gate = ExecutionGate::try_acquire(runs, KEY).unwrap().unwrap();
            let result = durable::claim_durable_pending_for_target_gated(
                &gate,
                KEY,
                "d",
                Some("target".into()),
                policy::IDEMPOTENCY_TTL,
                &Default::default(),
                true,
            );
            assert!(result.is_err(), "{component}, linked={linked}");
            assert!(!pending(runs).exists());
        }
    }
}
