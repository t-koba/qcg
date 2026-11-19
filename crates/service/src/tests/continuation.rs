use super::support::*;
use crate::ContinuationRefusal;
use api::{RunStatus, StartRun};
use camino::Utf8PathBuf;
use serde_json::json;
use std::collections::BTreeMap;

fn source_generator(root: &Utf8PathBuf, id: &str, manifest: &str) {
    let dir = root.join("generators").join(id);
    std::fs::create_dir_all(&dir).expect("generator directory");
    std::fs::write(dir.join(contract::MANIFEST_FILE), manifest).expect("manifest");
}

const CONTINUE_MANIFEST: &str = r#"
[generator]
id = "continue-source"
name = "Continue Source"
version = "0.1.0"
description = "Read-only checks over two file inputs."

[runtime]
file_input_limit_bytes = 4096

[[inputs.stages]]
id = "files"

[[inputs.stages.fields]]
id = "good"
required = true
type = "file"

[[inputs.stages.fields]]
id = "bad"
required = true
type = "file"

[[flow]]
id = "check_good"
type = "check.format"

[flow.params]
content = "json"
source = "{{ inputs.good }}"

[[flow]]
id = "check_bad"
type = "check.format"

[flow.params]
content = "json"
source = "{{ inputs.bad }}"

[permissions]
fs_read = ["workspace"]
fs_write = []
network = []
commands = []
side_effects = "none"

[permissions.containers]
enabled = false
"#;

fn continuation_service(root: &Utf8PathBuf) -> crate::LocalService {
    crate::LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.join("generators")],
        root.join("runs"),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        crate::RunStoreMode::Exclusive,
        crate::ServiceDeploymentPolicy {
            elapsed_budget_mode: policy::ElapsedBudgetMode::ActiveExecution,
            ..Default::default()
        },
    )
    .expect("continuation test service")
}

async fn failed_source(service: &crate::LocalService, root: &Utf8PathBuf) -> String {
    source_generator(root, "continue-source", CONTINUE_MANIFEST);
    let id = service
        .start_run(StartRun {
            generator_id: "continue-source".into(),
            inputs: BTreeMap::from([
                ("good".into(), json!({"name": "good.json", "text": "{}"})),
                ("bad".into(), json!({"name": "bad.json", "text": "{"})),
            ]),
            ..Default::default()
        })
        .await
        .expect("failed source should be admitted");
    wait_for_snapshot(service, &id, RunStatus::Failed).await;
    id
}

#[tokio::test]
#[cfg(unix)]
async fn continuation_refuses_non_failed_and_unsealed_sources_with_fixed_reasons() {
    let root = temp_run_dir("continuation-refusal");
    let service = continuation_service(&root);

    source_generator(
        &root,
        "continue-success",
        &CONTINUE_MANIFEST.replace(
            "source = \"{{ inputs.bad }}\"",
            "source = \"{{ inputs.good }}\"",
        ),
    );
    let success = service
        .start_run(StartRun {
            generator_id: "continue-success".into(),
            inputs: BTreeMap::from([
                ("good".into(), json!({"name": "good.json", "text": "{}"})),
                ("bad".into(), json!({"name": "bad.json", "text": "{}"})),
            ]),
            ..Default::default()
        })
        .await
        .expect("successful source should be admitted");
    wait_for_snapshot(&service, &success, RunStatus::Succeeded).await;
    let refusal = service.continue_run(&success).await.unwrap_err();
    assert_eq!(refusal, ContinuationRefusal::NotFailed);
    assert_eq!(refusal.reason(), "source run is not terminally failed");

    let source_id = failed_source(&service, &root).await;
    let source_dir = service.run_dir_for(&source_id).await.expect("source dir");
    let before = std::fs::read(source_dir.join("meta/journal.jsonl")).expect("journal");
    std::fs::remove_file(source_dir.join("meta/.clean_shutdown")).expect("marker");
    let refusal = service.continue_run(&source_id).await.unwrap_err();
    assert_eq!(
        refusal,
        ContinuationRefusal::Planning(engine::ContinuationRefusal::Unsealed)
    );
    assert_eq!(refusal.reason(), "source journal is not cleanly sealed");
    assert_eq!(
        std::fs::read(source_dir.join("meta/journal.jsonl")).expect("journal"),
        before,
        "refusal must not modify the source"
    );
}

/// A real service-owned failed run (whose journal has a pre-start elapsed
/// ledger record) must continue into a linked child that reuses every
/// successful source outcome and reruns only the failed node, with the source
/// journal left byte-identical.
#[tokio::test]
#[cfg(unix)]
async fn continuation_creates_linked_child_preserving_successful_outcomes() {
    let root = temp_run_dir("continuation-history");
    let service = continuation_service(&root);
    let source_id = failed_source(&service, &root).await;
    let source_dir = service.run_dir_for(&source_id).await.expect("source dir");
    let source_before = std::fs::read(source_dir.join("meta/journal.jsonl")).expect("journal");

    let child_id = service
        .continue_run(&source_id)
        .await
        .expect("service-owned failed run continues");
    assert_ne!(child_id, source_id);

    let child_dir = service.run_dir_for(&child_id).await.expect("child dir");
    let journal = std::fs::read_to_string(child_dir.join("meta/journal.jsonl")).expect("journal");
    let events = journal
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("event"))
        .collect::<Vec<_>>();

    let fork = events
        .iter()
        .find(|event| event["t"] == "run_forked")
        .expect("run_forked");
    assert_eq!(fork["source_run_id"], source_id);
    assert_eq!(fork["source_contract_sha256"], events[0]["contract_sha256"]);

    let overlay = events
        .iter()
        .find(|event| event["t"] == "child_reset_overlay")
        .expect("child_reset_overlay");
    let candidate = &overlay["overlay"]["candidate"];
    assert_eq!(candidate["preserved_outcomes"], json!(["check_good"]));
    assert_eq!(candidate["stale_outcomes"], json!([]));
    assert_eq!(candidate["retry_roots"], json!(["check_bad"]));
    assert_eq!(
        candidate["reset_nodes"]
            .as_object()
            .expect("reset nodes")
            .keys()
            .collect::<Vec<_>>(),
        vec!["check_bad"]
    );
    let budget = &candidate["inherited_budget"];
    assert_eq!(budget["steps_executed"], 2);
    assert_eq!(budget["steps_succeeded"], 1);
    assert_eq!(budget["steps_failed"], 1);
    assert_eq!(budget["budget_charged"], 2);

    // The child is registered as a descendant and keeps the source private.
    let fork_seq = fork["seq"].as_u64().expect("seq");
    let child_queued = events
        .iter()
        .find(|event| event["t"] == "run_queued" && event["seq"].as_u64() > Some(fork_seq))
        .expect("child run_queued");
    assert_eq!(child_queued["parent_run_id"], source_id);
    assert_eq!(
        std::fs::read(source_dir.join("meta/journal.jsonl")).expect("journal"),
        source_before,
        "continuation must not modify the source run"
    );

    // The child executes only the reset node; the preserved success is reused.
    wait_for_snapshot(&service, &child_id, RunStatus::Failed).await;
    let child_journal =
        std::fs::read_to_string(child_dir.join("meta/journal.jsonl")).expect("journal");
    let child_events = child_journal
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("event"))
        .collect::<Vec<_>>();
    let admission_seq = child_queued["seq"].as_u64().expect("seq");
    let rerun = child_events
        .iter()
        .filter(|event| {
            event["t"] == "step_started"
                && event["seq"].as_u64().is_some_and(|seq| seq > admission_seq)
        })
        .map(|event| event["node"].as_str().unwrap_or("").to_owned())
        .collect::<Vec<_>>();
    assert_eq!(rerun, vec!["check_bad"]);

    // Every refusal category stays fixed and redacted.
    for refusal in [
        ContinuationRefusal::Source,
        ContinuationRefusal::InFlight,
        ContinuationRefusal::Manifest,
        ContinuationRefusal::Admission,
        ContinuationRefusal::Planning(engine::ContinuationRefusal::History),
    ] {
        let reason = refusal.reason();
        assert!(!reason.is_empty());
        assert!(!reason.contains('/'));
        assert!(!reason.contains(&source_id));
    }
}

/// The product default is WallClock elapsed-budget mode. A failed run created
/// under that default must be continuable: the child inherits the durable
/// `started_at` anchor so `wall_elapsed` charges the source's consumed window,
/// and it still preserves every successful source outcome.
#[tokio::test]
#[cfg(unix)]
async fn continuation_creates_child_from_default_wallclock_source() {
    let root = temp_run_dir("continuation-wallclock");
    source_generator(&root, "continue-source", CONTINUE_MANIFEST);
    let service = crate::LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.join("generators")],
        root.join("runs"),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        crate::RunStoreMode::Exclusive,
        crate::ServiceDeploymentPolicy::default(),
    )
    .expect("default-policy service");
    let source_id = service
        .start_run(StartRun {
            generator_id: "continue-source".into(),
            inputs: BTreeMap::from([
                ("good".into(), json!({"name": "good.json", "text": "{}"})),
                ("bad".into(), json!({"name": "bad.json", "text": "{"})),
            ]),
            ..Default::default()
        })
        .await
        .expect("wallclock source admitted");
    wait_for_snapshot(&service, &source_id, RunStatus::Failed).await;

    let source_dir = service.run_dir_for(&source_id).await.expect("source dir");
    let source_journal =
        std::fs::read_to_string(source_dir.join("meta/journal.jsonl")).expect("journal");
    let source_events = source_journal
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("event"))
        .collect::<Vec<_>>();
    let source_started_at = source_events
        .iter()
        .find(|event| event["t"] == "run_queued")
        .expect("source queue")["ts"]
        .clone();

    let child_id = service
        .continue_run(&source_id)
        .await
        .expect("default WallClock source continues");
    let child_dir = service.run_dir_for(&child_id).await.expect("child dir");
    let journal = std::fs::read_to_string(child_dir.join("meta/journal.jsonl")).expect("journal");
    let events = journal
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("event"))
        .collect::<Vec<_>>();
    let overlay = events
        .iter()
        .find(|event| event["t"] == "child_reset_overlay")
        .expect("child_reset_overlay");
    let candidate = &overlay["overlay"]["candidate"];
    let budget = &candidate["inherited_budget"];
    assert_eq!(budget["elapsed_budget_mode"], "wall_clock");
    assert!(budget["elapsed"].is_null());
    assert_eq!(budget["started_at"], source_started_at);
    assert_eq!(candidate["preserved_outcomes"], json!(["check_good"]));
    assert_eq!(
        candidate["reset_nodes"]
            .as_object()
            .expect("reset nodes")
            .keys()
            .collect::<Vec<_>>(),
        vec!["check_bad"]
    );

    wait_for_snapshot(&service, &child_id, RunStatus::Failed).await;
    let child_journal =
        std::fs::read_to_string(child_dir.join("meta/journal.jsonl")).expect("journal");
    let admission_seq = events
        .iter()
        .find(|event| event["t"] == "run_queued" && event["parent_run_id"] == source_id)
        .expect("child queue")["seq"]
        .as_u64()
        .expect("seq");
    let rerun = child_journal
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("event"))
        .filter(|event| {
            event["t"] == "step_started"
                && event["seq"].as_u64().is_some_and(|seq| seq > admission_seq)
        })
        .map(|event| event["node"].as_str().unwrap_or("").to_owned())
        .collect::<Vec<_>>();
    assert_eq!(rerun, vec!["check_bad"]);
    assert_eq!(
        std::fs::read_to_string(source_dir.join("meta/journal.jsonl")).expect("journal"),
        source_journal,
        "continuation must not modify the source"
    );
}
