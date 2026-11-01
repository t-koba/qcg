use camino::Utf8PathBuf;
use serde_json::{Value, json};
use std::process::Command;

fn write_cost_run(root: &Utf8PathBuf) -> Utf8PathBuf {
    let runs = root.join("runs");
    let meta = runs.join("cost-run/meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), "cost-run", false, None)
            .unwrap();
    journal
        .event(
            "run_started",
            json!({"generator": "demo@1.0.0", "generator_path": "demo",
                "contract_sha256": "test", "inputs": {}, "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock", "retention_days": 10}),
        )
        .unwrap();
    journal
        .event(
            "llm_call",
            json!({"node": "generate", "provider": "provider", "model": "model",
                "seed": 1_u64, "max_tokens": 16,
                "tokens": {"input": 100, "output": 20, "cached_input": 0},
                "cost_microusd": 42_u64}),
        )
        .unwrap();
    journal
        .event("run_finished", json!({"status": "success"}))
        .unwrap();
    drop(journal);
    runs
}

fn costs(runs: &Utf8PathBuf, by_model: bool) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qcg"));
    command
        .args(["runs", "costs", "--runs-dir"])
        .arg(runs)
        .arg("--json")
        .env_remove("PROVIDERS")
        .env("AUTO_GC", "false");
    if by_model {
        command.arg("--by-model");
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn by_model_json_carries_the_breakdown_and_only_with_the_flag() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-costs-json-{}", uuid::Uuid::now_v7()));
    let runs = write_cost_run(&root);

    let value = costs(&runs, true);
    let models = value["models"].as_array().expect("models array");
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["provider"], "provider");
    assert_eq!(models[0]["model"], "model");
    assert_eq!(models[0]["llm_calls"], 1);
    assert_eq!(models[0]["tokens_input"], 100);
    assert_eq!(models[0]["tokens_output"], 20);
    assert_eq!(models[0]["cost_microusd"], 42);
    assert!(models[0]["input_rate"].is_null());
    assert_eq!(value["total_cost_microusd"], 42);

    let plain = costs(&runs, false);
    assert!(plain.get("models").is_none());

    std::fs::remove_dir_all(root).unwrap();
}
