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

/// A service-owned ActiveExecution run currently has a pre-start elapsed
/// ledger record that the private source-history proof does not admit. The
/// continuation path must fail closed here; widening that proof is a separate
/// safety decision and is deliberately not done in this change.
#[tokio::test]
#[cfg(unix)]
async fn continuation_fails_closed_on_unproven_service_history() {
    let root = temp_run_dir("continuation-history");
    let service = continuation_service(&root);
    let source_id = failed_source(&service, &root).await;
    let refusal = service.continue_run(&source_id).await.unwrap_err();
    assert_eq!(
        refusal,
        ContinuationRefusal::Planning(engine::ContinuationRefusal::History)
    );
    for refusal in [
        ContinuationRefusal::Source,
        ContinuationRefusal::InFlight,
        ContinuationRefusal::Manifest,
        ContinuationRefusal::Admission,
        refusal,
    ] {
        let reason = refusal.reason();
        assert!(!reason.is_empty());
        assert!(!reason.contains('/'));
        assert!(!reason.contains(&source_id));
    }
}
