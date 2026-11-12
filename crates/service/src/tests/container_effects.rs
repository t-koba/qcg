//! Reclaiming orphaned container effects by journaled name.

use crate::effects::reconcile_run_effects;
use crate::summaries::{fold_run_state, run_meta_dir};
use camino::Utf8PathBuf;
use serde_json::json;

use super::execution::daemon_policy_test_support::Daemon;

const T: &str = "tests::container_effects::";

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

fn outcomes(dir: &Utf8PathBuf) -> Vec<String> {
    std::fs::read_to_string(run_meta_dir(dir).join("journal.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|event| event["t"] == "effect_closed")
        .map(|event| event["outcome"].as_str().unwrap().to_string())
        .collect()
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
async fn crash_after_create_removes_only_the_journaled_instance_and_is_idempotent() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}crash_after_create_removes_only_the_journaled_instance_and_is_idempotent"
    )) else {
        return;
    };
    let (ours, foreign) = (name(), name());
    let dir = run_with_intents(&daemon, &[&ours]);
    let similar = format!("{ours}-sidecar");
    daemon.set_instances(&[&ours, &foreign, &similar]);
    assert!(
        fold_run_state(&dir)
            .unwrap()
            .ensure_no_inflight_operations()
            .is_err()
    );
    reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .unwrap();
    assert_eq!(daemon.instances(), [foreign.as_str(), similar.as_str()]);
    assert_eq!(outcomes(&dir), ["removed"]);
    assert!(
        fold_run_state(&dir)
            .unwrap()
            .ensure_no_inflight_operations()
            .is_ok()
    );
    let calls = daemon.log().lines().count();
    reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .unwrap();
    assert_eq!(
        daemon.log().lines().count(),
        calls,
        "a closed intent is never revisited"
    );
}

#[tokio::test]
async fn crash_before_create_is_confirmed_absent_and_closed() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}crash_before_create_is_confirmed_absent_and_closed"
    )) else {
        return;
    };
    let dir = run_with_intents(&daemon, &[&name()]);
    reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .unwrap();
    assert_eq!(outcomes(&dir), ["absent"]);
}

#[tokio::test]
async fn a_create_completing_after_the_first_check_is_reclaimed() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}a_create_completing_after_the_first_check_is_reclaimed"
    )) else {
        return;
    };
    let ours = name();
    let dir = run_with_intents(&daemon, &[&ours]);
    daemon.mode("late");
    daemon.create_late(&ours);
    reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .unwrap();
    assert!(daemon.instances().is_empty());
    assert_eq!(outcomes(&dir), ["removed"]);
}

#[tokio::test]
async fn an_unreachable_backend_keeps_the_intent_open_for_the_next_recovery() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}an_unreachable_backend_keeps_the_intent_open_for_the_next_recovery"
    )) else {
        return;
    };
    let (first, second) = (name(), name());
    let dir = run_with_intents(&daemon, &[&first, &second]);
    daemon.set_instances(&[&first]);
    daemon.mode("broken");
    let error = reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not yet reclaimed"), "{error}");
    assert_eq!(fold_run_state(&dir).unwrap().effects.len(), 2);
    daemon.mode("ok");
    reconcile_run_effects(&dir, "run-x", timeouts())
        .await
        .unwrap();
    assert!(fold_run_state(&dir).unwrap().effects.is_empty());
    assert!(daemon.instances().is_empty());
}

#[tokio::test]
async fn startup_recovery_reconciles_dead_runs_and_skips_leased_ones() {
    let Some(daemon) = Daemon::isolated(&format!(
        "{T}startup_recovery_reconciles_dead_runs_and_skips_leased_ones"
    )) else {
        return;
    };
    let ours = name();
    let dir = run_with_intents(&daemon, &[&ours]);
    // Terminal run: construction still reports its open effect.
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
    let service = svc(runs);
    service.resume_recovered_runs().await;
    assert!(daemon.instances().is_empty());
    assert_eq!(outcomes(&dir), ["removed"]);
}
