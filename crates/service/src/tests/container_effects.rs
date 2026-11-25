//! Fail-closed recovery of name-keyed container effects. A name-keyed
//! observation cannot prove ownership, so recovery retains the intent and
//! surfaces a durable refusal rather than deleting by name.

use crate::effects::reconcile_run_effects;
use crate::summaries::{fold_run_state, read_journal_events, run_meta_dir};
use api::{RunStatus, StartRun};
use camino::Utf8PathBuf;
use serde_json::{Value, json};
use std::time::Duration;

use super::support::{TempGuard, temp_run_dir, wait_for_snapshot};

use super::execution::daemon_policy_test_support::Daemon;

const T: &str = "tests::container_effects::";
const MARKER: &str = "qcg has not proved that the effect stopped";

fn timeouts() -> policy::ContainerTimeouts {
    policy::ContainerTimeouts::from_seconds([1; 5]).unwrap()
}

fn name() -> String {
    container::effect_name().unwrap()
}

/// A run directory whose journal holds one open intent per name (docker).
fn run_with_intents(daemon: &Daemon, names: &[&str]) -> Utf8PathBuf {
    let dir = Utf8PathBuf::from_path_buf(daemon.root.join("runs/run-x")).unwrap();
    let meta = run_meta_dir(&dir);
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), "run-x", false, None).unwrap();
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
    for name in names {
        let backend = container::Backend::Docker {
            binary: "docker".into(),
            runtime_flag: None,
        };
        journal
            .event("effect_intent", json!({"effect": name, "backend": backend}))
            .unwrap();
    }
    dir
}

fn events(dir: &Utf8PathBuf) -> Vec<Value> {
    read_journal_events(dir).unwrap()
}

fn refusal(dir: &Utf8PathBuf) -> String {
    events(dir)
        .iter()
        .find(|event| {
            event["t"] == "run_finished"
                && event["status"] == "failed"
                && event["reason"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains(MARKER))
        })
        .expect("the refusal must be a durable terminal failure detail")
        .to_string()
}

fn assert_no_effect_closure(dir: &Utf8PathBuf) {
    assert!(
        events(dir)
            .iter()
            .all(|event| event["t"] != "effect_closed"),
        "recovery must never journal effect_closed"
    );
}

fn assert_no_deletion(daemon: &Daemon) {
    let log = daemon.log();
    assert!(
        !log.contains(" kill ")
            && !log.contains(" stop ")
            && !log.contains(" rm ")
            && !log.contains(" delete "),
        "a name match cannot prove ownership: {log}"
    );
}

async fn join_run_task(service: &crate::LocalService, id: &str) {
    let task = {
        let runs = service.inner.runs.read().await;
        runs[id].task.lock().unwrap().take()
    };
    if let Some(task) = task {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("service task must finish within the bounded test window")
            .expect("service task must not panic");
    }
}

fn svc(runs: Utf8PathBuf) -> crate::LocalService {
    crate::LocalService::with_generator_roots_policy_and_store_mode(
        vec![],
        runs,
        None,
        1,
        8,
        crate::RunStoreMode::Exclusive,
        crate::ServiceDeploymentPolicy::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn crash_after_create_never_deletes_by_name_and_refuses_replay_and_deletion() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}crash_after_create_never_deletes_by_name_and_refuses_replay_and_deletion"
    )) else {
        return;
    };
    let (ours, foreign) = (name(), name());
    let dir = run_with_intents(&daemon, &[&ours]);
    let similar = format!("{ours}-sidecar");
    daemon.set_instances(&[&ours, &foreign, &similar]);

    let state = fold_run_state(&dir).unwrap();
    assert!(state.operation_records.is_empty());
    let replay = state
        .ensure_no_fail_closed_inflight_operations()
        .expect_err("an open effect blocks re-entry");
    let deletion = state
        .ensure_no_inflight_operations()
        .expect_err("an open effect blocks deletion");
    for detail in [&replay, &deletion] {
        assert!(detail.contains("container effect"), "{detail}");
        assert!(detail.contains(MARKER), "{detail}");
    }

    let error = reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .expect_err("a name-keyed observation is never a settlement");
    assert!(error.to_string().contains(MARKER), "{error}");
    assert!(
        error.to_string().contains("was observed"),
        "the observation must be recorded for diagnostics: {error}"
    );
    assert_eq!(
        daemon.instances(),
        [ours.as_str(), foreign.as_str(), similar.as_str()]
    );
    assert_no_effect_closure(&dir);
    assert_no_deletion(&daemon);
    let state = fold_run_state(&dir).unwrap();
    assert!(state.effects.contains_key(&ours));
    assert!(state.ensure_no_fail_closed_inflight_operations().is_err());
    assert!(state.ensure_no_inflight_operations().is_err());

    // The refusal itself is persisted only by the callers under the lease;
    // when it is, the durable detail carries the same marker and is not
    // overwritten by a second settlement.
    crate::run_dirs::settle_unresolved_effect(&dir, &error.to_string()).unwrap();
    crate::run_dirs::settle_unresolved_effect(&dir, "must not overwrite").unwrap();
    let detail = refusal(&dir);
    assert!(detail.contains(MARKER), "{detail}");
    assert!(!detail.contains("must not overwrite"), "{detail}");
    assert_eq!(
        events(&dir)
            .iter()
            .filter(|event| event["t"] == "run_finished")
            .count(),
        1
    );

    let again = reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .expect_err("the intent is still open");
    assert!(again.to_string().contains(MARKER), "{again}");
    assert!(fold_run_state(&dir).unwrap().effects.contains_key(&ours));
    assert_no_effect_closure(&dir);
    assert_no_deletion(&daemon);
}

#[tokio::test]
async fn crash_before_create_never_certifies_absence() {
    let Some(daemon) = Daemon::isolated(&format!("{T}crash_before_create_never_certifies_absence"))
    else {
        return;
    };
    let name = name();
    let dir = run_with_intents(&daemon, &[&name]);
    let error = reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .expect_err("a missing entry does not settle the intent");
    assert!(
        error
            .to_string()
            .contains("no instance with the journaled name was observed"),
        "{error}"
    );
    assert!(error.to_string().contains(MARKER), "{error}");
    assert!(fold_run_state(&dir).unwrap().effects.contains_key(&name));
    assert_no_effect_closure(&dir);
    assert!(daemon.instances().is_empty());
    let probes = daemon
        .log()
        .lines()
        .filter(|line| line.contains(" ps "))
        .count();
    assert_eq!(probes, 1, "one bounded probe, never an absence certificate");
    assert_no_deletion(&daemon);
}

#[tokio::test]
async fn a_create_completing_after_the_probe_is_not_claimed() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}a_create_completing_after_the_probe_is_not_claimed"
    )) else {
        return;
    };
    let ours = name();
    let dir = run_with_intents(&daemon, &[&ours]);
    daemon.mode("late");
    daemon.create_late(&ours);
    let error = reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .expect_err("a late create is not a settled effect");
    assert!(error.to_string().contains(MARKER), "{error}");
    assert_eq!(daemon.instances(), [ours.as_str()]);
    assert_no_effect_closure(&dir);
    assert_no_deletion(&daemon);
}

#[tokio::test]
async fn an_unreachable_backend_and_a_healthy_backend_both_remain_refused() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}an_unreachable_backend_and_a_healthy_backend_both_remain_refused"
    )) else {
        return;
    };
    let (first, second) = (name(), name());
    let dir = run_with_intents(&daemon, &[&first, &second]);
    daemon.set_instances(&[&first]);
    daemon.mode("broken");
    let error = reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .expect_err("an unreadable backend is retained");
    assert!(error.to_string().contains(MARKER), "{error}");
    assert_eq!(fold_run_state(&dir).unwrap().effects.len(), 2);
    daemon.mode("ok");
    let error = reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .expect_err("a healthy probe is still not a name-keyed settlement");
    assert!(error.to_string().contains(MARKER), "{error}");
    assert_eq!(fold_run_state(&dir).unwrap().effects.len(), 2);
    assert_eq!(daemon.instances(), [first.as_str()]);
    assert_no_effect_closure(&dir);
    assert_no_deletion(&daemon);
}

#[tokio::test]
async fn refusal_is_durable_and_observable_and_stops_retry() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}refusal_is_durable_and_observable_and_stops_retry"
    )) else {
        return;
    };
    for store_mode in [
        crate::RunStoreMode::Exclusive,
        crate::RunStoreMode::SharedFilesystem,
    ] {
        let root = temp_run_dir("effect-reconciliation-refusal");
        let _guard = TempGuard(root.clone());
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let runs = root.join("runs");
        let service = crate::LocalService::with_generator_roots_policy_and_store_mode(
            vec![generators],
            runs,
            None,
            1,
            8,
            store_mode,
            crate::ServiceDeploymentPolicy {
                queued_resumer_secs: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let retries = &service.inner.reconciliation_retries;
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        join_run_task(&service, &id).await;

        let dir = service.run_dir_for(&id).await.unwrap();
        let journal = run_meta_dir(&dir).join("journal.jsonl");
        let effect = name();
        engine::JournalWriter::append_single_event(
            &journal,
            &id,
            "effect_intent",
            json!({
                "effect": &effect,
                "backend": container::Backend::Docker {
                    binary: "docker".into(),
                    runtime_flag: None,
                }
            }),
            engine::JournalLimits::default(),
            None,
        )
        .unwrap();
        daemon.set_instances(&[&effect]);

        service.resume_recovered_runs().await;
        join_run_task(&service, &id).await;
        let failed = wait_for_snapshot(&service, &id, RunStatus::Failed).await;
        assert_eq!(failed.state, RunStatus::Failed);
        let detail = refusal(&dir);
        assert!(detail.contains(MARKER), "{detail}");
        assert!(detail.contains(&effect), "{detail}");
        assert!(fold_run_state(&dir).unwrap().effects.contains_key(&effect));
        assert!(daemon.instances().contains(&effect));
        assert_no_effect_closure(&dir);
        assert_no_deletion(&daemon);
        assert!(retries.lock().unwrap().is_empty());

        // A healthy suspended run stays intact and the permanent refusal is
        // never revisited by the production resumer.
        let healthy = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        wait_for_snapshot(&service, &healthy, RunStatus::Waiting).await;
        join_run_task(&service, &healthy).await;
        let calls = daemon.log();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let resumer = service.start_queued_resumer(shutdown.clone());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(retries.lock().unwrap().is_empty());
        assert_eq!(
            daemon.log(),
            calls,
            "a permanent refusal must not be retried"
        );
        shutdown.cancel();
        resumer.await.unwrap().unwrap();
        assert_eq!(
            service.snapshot(healthy).await.unwrap().state,
            RunStatus::Waiting
        );
        assert_no_effect_closure(&dir);
    }
}

#[tokio::test]
async fn startup_recovery_of_a_terminal_run_preserves_terminal_and_never_settles_by_name() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}startup_recovery_of_a_terminal_run_preserves_terminal_and_never_settles_by_name"
    )) else {
        return;
    };
    let ours = name();
    let dir = run_with_intents(&daemon, &[&ours]);
    let meta = run_meta_dir(&dir);
    engine::JournalWriter::append_single_event(
        &meta.join("journal.jsonl"),
        "run-x",
        "run_canceled",
        json!({"reason": {"code": "canceled", "message": "test"}}),
        engine::JournalLimits::default(),
        None,
    )
    .unwrap();
    daemon.set_instances(&[&ours]);
    let runs = dir.parent().unwrap().to_owned();
    let service = svc(runs.clone());
    let lease = crate::run_dirs::try_lock_run_execution(&dir)
        .unwrap()
        .unwrap();
    service.resume_recovered_runs().await;
    assert_eq!(
        daemon.instances(),
        [ours.as_str()],
        "a live owner's run is not touched"
    );
    drop(lease);
    drop(service);
    let before = events(&dir);
    let service = svc(runs);
    service.resume_recovered_runs().await;
    assert_eq!(
        events(&dir),
        before,
        "a prior terminal event is never overwritten"
    );
    assert_eq!(daemon.instances(), [ours.as_str()]);
    assert!(fold_run_state(&dir).unwrap().effects.contains_key(&ours));
    assert_no_effect_closure(&dir);
    assert_no_deletion(&daemon);
}

#[tokio::test]
async fn open_effect_blocks_direct_replay_gate_without_a_fail_closed_started_operation() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}open_effect_blocks_direct_replay_gate_without_a_fail_closed_started_operation"
    )) else {
        return;
    };
    let ours = name();
    let dir = run_with_intents(&daemon, &[&ours]);
    let state = fold_run_state(&dir).unwrap();
    assert!(state.operation_records.is_empty());
    let detail = state
        .ensure_no_fail_closed_inflight_operations()
        .expect_err("direct qcg run re-entry must refuse");
    assert!(detail.contains("container effect"), "{detail}");
    assert!(detail.contains(MARKER), "{detail}");
    assert!(detail.contains("before replay"), "{detail}");
}
