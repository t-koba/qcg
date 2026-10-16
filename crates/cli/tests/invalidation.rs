use camino::Utf8PathBuf;
use serde_json::{Value, json};
use std::process::{Command, Output};

fn cli(runs: &Utf8PathBuf, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_qcg"))
        .arg("runs")
        .args(args)
        .arg("--runs-dir")
        .arg(runs)
        .env("AUTO_GC", "false")
        .output()
        .unwrap()
}

#[test]
fn invalidation_cli_round_trip_conflict_status_delete_and_gc_lock() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("invalidation-cli-{}", uuid::Uuid::now_v7()));
    let runs = root.join("runs");
    let meta = runs.join("success/meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), "success", false, None).unwrap();
    journal.event("run_started", json!({"generator": "demo@1.0.0", "generator_path": "demo",
        "contract_sha256": "test", "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
        "elapsed_budget_mode": "wall_clock", "retention_days": 10 })).unwrap();
    journal
        .event("run_finished", json!({"status": "success"}))
        .unwrap();
    drop(journal);
    let args = [
        "invalidate",
        "success",
        "--reason",
        "wrong output",
        "--actor-label",
        "reviewer",
        "--json",
    ];
    let first = cli(&runs, &args);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let record: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(record["reason"], "wrong output");
    assert_eq!(record["actor_label"], "reviewer");
    assert_eq!(cli(&runs, &args).stdout, first.stdout);
    assert!(
        !cli(
            &runs,
            &[
                "invalidate",
                "success",
                "--reason",
                "other",
                "--actor-label",
                "reviewer"
            ]
        )
        .status
        .success()
    );
    assert!(
        !cli(&runs, &["invalidate", "success", "--reason", "ok"])
            .status
            .success()
    );
    let show = cli(&runs, &["show", "success", "--json"]);
    assert!(
        show.status.success(),
        "{}",
        String::from_utf8_lossy(&show.stderr)
    );
    let summary: Value = serde_json::from_slice(&show.stdout).unwrap();
    assert_eq!(summary["invalidation"], record);
    assert_eq!(summary["status"], "success");
    let lock = service::try_lock_store_maintenance(&runs).unwrap().unwrap();
    let gc = cli(&runs, &["gc", "--keep", "0", "--delete"]);
    assert!(!gc.status.success());
    assert!(String::from_utf8_lossy(&gc.stderr).contains("maintenance is busy"));
    assert!(runs.join("success").exists());
    assert!(!cli(&runs, &["delete", "success"]).status.success());
    drop(lock);
    assert!(
        cli(&runs, &["gc", "--keep", "0", "--delete"])
            .status
            .success()
    );
    assert!(!runs.join("success").exists());
    assert!(!cli(&runs, &args).status.success());
    assert!(!runs.join("success").exists());
    std::fs::remove_dir_all(root).unwrap();
}
