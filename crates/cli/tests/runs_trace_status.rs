use camino::Utf8PathBuf;
use serde_json::{Value, json};
use std::process::Command;

fn seed_run(root: &Utf8PathBuf, id: &str, status: &str) {
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event("run_finished", json!({ "status": status, "metrics": {} }))
        .unwrap();
    drop(journal);
}

fn seed_unsettled_run(root: &Utf8PathBuf, id: &str) {
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    drop(journal);
}

fn seed_failed_step_run(root: &Utf8PathBuf, id: &str) {
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "step_finished",
            json!({
                "node": "build",
                "status": "failed",
                "reason": { "code": "execution_failed", "message": "boom" },
            }),
        )
        .unwrap();
    journal
        .event("run_finished", json!({ "status": "failed", "metrics": {} }))
        .unwrap();
    drop(journal);
}

fn trace(root: &Utf8PathBuf, id: &str) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .args(["runs", "trace", id, "--runs-dir"])
        .arg(root.join("runs"))
        .env_remove("PROVIDERS")
        .env("AUTO_GC", "false")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn span_code(payload: &Value, name: &str) -> u64 {
    let spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .unwrap();
    spans
        .iter()
        .find(|span| span["name"] == name)
        .unwrap_or_else(|| panic!("span `{name}` missing from trace"))["status"]["code"]
        .as_u64()
        .unwrap()
}

#[test]
fn failed_run_exports_error_on_root_and_terminal_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-failed-{}", uuid::Uuid::now_v7()));
    let id = "failed-run";
    seed_run(&root, id, "failed");

    let payload = trace(&root, id);
    assert_eq!(span_code(&payload, &format!("run {id}")), 2, "root span");
    assert_eq!(
        span_code(&payload, "event run_finished"),
        2,
        "terminal span"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn successful_run_exports_ok_on_root_and_terminal_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-success-{}", uuid::Uuid::now_v7()));
    let id = "success-run";
    seed_run(&root, id, "success");

    let payload = trace(&root, id);
    assert_eq!(span_code(&payload, &format!("run {id}")), 1, "root span");
    assert_eq!(
        span_code(&payload, "event run_finished"),
        1,
        "terminal span"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn unsettled_run_exports_unset_on_root_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-unsettled-{}", uuid::Uuid::now_v7()));
    let id = "unsettled-run";
    seed_unsettled_run(&root, id);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, &format!("run {id}")),
        0,
        "root span must not claim OK before the journal settles"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_step_exports_error_on_event_and_step_spans() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-step-failed-{}", uuid::Uuid::now_v7()));
    let id = "step-failed-run";
    seed_failed_step_run(&root, id);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "event step_finished"),
        2,
        "failed step event span"
    );
    assert_eq!(span_code(&payload, "step build"), 2, "step span");

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_hook_exports_error_on_its_step_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-trace-hook-failed-{}", uuid::Uuid::now_v7()));
    let id = "hook-failed-run";
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "step_started",
            json!({ "node": "notify", "type": "hook", "attempt": 1 }),
        )
        .unwrap();
    journal
        .event(
            "hook_failed",
            json!({ "hook": "notify", "policy": "warn", "error": "boom" }),
        )
        .unwrap();
    journal
        .event(
            "run_finished",
            json!({ "status": "success", "metrics": {} }),
        )
        .unwrap();
    drop(journal);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "event hook_failed"),
        2,
        "hook event span"
    );
    assert_eq!(span_code(&payload, "step notify"), 2, "hook step span");

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovered_step_uses_its_last_outcome_for_the_step_span() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "runs-trace-step-recovered-{}",
            uuid::Uuid::now_v7()
        ));
    let id = "step-recovered-run";
    let meta = root.join("runs").join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0",
                "generator_path": "demo",
                "contract_sha256": "test",
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock",
                "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "step_finished",
            json!({ "node": "build", "status": "failed" }),
        )
        .unwrap();
    journal
        .event(
            "step_finished",
            json!({ "node": "build", "status": "success" }),
        )
        .unwrap();
    journal
        .event(
            "run_finished",
            json!({ "status": "success", "metrics": {} }),
        )
        .unwrap();
    drop(journal);

    let payload = trace(&root, id);
    assert_eq!(
        span_code(&payload, "step build"),
        1,
        "last step outcome wins"
    );

    std::fs::remove_dir_all(root).unwrap();
}
