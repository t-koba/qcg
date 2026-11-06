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
    let workspace = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned();
    let service = service::LocalService::with_generator_roots_policy_and_store_mode(
        vec![workspace.join("fixtures/generators")],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        service::RunStoreMode::SharedFilesystem,
        service::ServiceDeploymentPolicy::default(),
    )
    .unwrap();
    let state = Arc::new(crate::test_state(
        service,
        runs.clone(),
        None,
        tokio_util::sync::CancellationToken::new(),
    ));
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
        execute: |reserved| async {
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
    assert_eq!(result.unwrap().status(), axum::http::StatusCode::CREATED);
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
    let mut retry = spawn(runs, "retry");
    wait_file(runs, "retry-start");
    for phase in ["before", "accepted"] {
        wait_file(runs, phase);
        // Age the exact claim past its 60s timestamp while the owner holds
        // its gate, then resume the call without restoring: the gate-aware
        // holder must remain current and publish Ready. A gate-aware
        // successor cannot replace it while the gate is held (see probe).
        age_claim(runs);
        assert!(spawn(runs, "probe").success());
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
