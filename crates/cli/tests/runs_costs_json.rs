use camino::Utf8PathBuf;
use serde_json::{Value, json};
use std::process::Command;

fn write_cost_run(
    root: &Utf8PathBuf,
    name: &str,
    cost_microusd: u64,
    generator_path: &str,
    search: bool,
    route_failed: bool,
) -> Utf8PathBuf {
    let runs = root.join("runs");
    let meta = runs.join(name).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), name, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({"generator": "demo@1.0.0", "generator_path": generator_path,
                "contract_sha256": "test", "inputs": {}, "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock", "retention_days": 10}),
        )
        .unwrap();
    let mut call = json!({"node": "generate", "provider": "provider", "model": "model",
        "seed": 1_u64, "max_tokens": 16,
        "tokens": {"input": 100, "output": 20, "cached_input": 0},
        "cost_microusd": cost_microusd});
    if search {
        call["web_search"] = json!({"calls": null, "cost_microusd": null});
    }
    journal.event("llm_call", call).unwrap();
    if route_failed {
        journal
            .event(
                "llm_route_failed",
                json!({"provider": "provider", "model": "model", "attempt": 1,
                    "kind": "network",
                    "web_search": {"calls": null, "cost_microusd": null}}),
            )
            .unwrap();
    }
    journal
        .event("run_finished", json!({"status": "success"}))
        .unwrap();
    drop(journal);
    runs
}

fn write_contract(dir: &Utf8PathBuf, input: f64, output: f64) -> Utf8PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(
        dir.join(contract::MANIFEST_FILE),
        format!(
            r#"
[generator]
id = "pricing-fixture"
name = "Pricing Fixture"
version = "0.1.0"

[llm]
max_tokens = 1024
[llm.model]
provider = "provider"
model = "model"
input_cost_per_million_usd = {input}
output_cost_per_million_usd = {output}

[[flow]]
id = "emit"
type = "write"
params = {{ content = "x", output_file = "x.txt" }}
"#
        ),
    )
    .unwrap();
    dir.clone()
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
    let runs = write_cost_run(&root, "cost-run", 42, "demo", false, false);

    let value = costs(&runs, true);
    let models = value["models"].as_array().expect("models array");
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["provider"], "provider");
    assert_eq!(models[0]["model"], "model");
    assert_eq!(models[0]["llm_calls"], 1);
    assert_eq!(models[0]["tokens_input"], 100);
    assert_eq!(models[0]["tokens_output"], 20);
    assert_eq!(models[0]["cost_microusd"], 42);
    assert!(models[0]["base_input_rate"].is_null());
    assert_eq!(value["total_cost_microusd"], 42);

    let plain = costs(&runs, false);
    assert!(plain.get("models").is_none());

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn by_model_text_header_names_the_contract_base_rates() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-costs-text-{}", uuid::Uuid::now_v7()));
    let runs = write_cost_run(&root, "cost-run", 42, "demo", false, false);

    let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .args(["runs", "costs", "--runs-dir"])
        .arg(&runs)
        .arg("--by-model")
        .env_remove("PROVIDERS")
        .env("AUTO_GC", "false")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("base(contract input/read/write/output USD per 1M)"),
        "base-rate header missing: {stdout}"
    );
    assert!(
        stdout.contains("model\tprovider\t100/20/0/0\t1\tn/a"),
        "by-model row missing: {stdout}"
    );

    let plain = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .args(["runs", "costs", "--runs-dir"])
        .arg(&runs)
        .env_remove("PROVIDERS")
        .env("AUTO_GC", "false")
        .output()
        .unwrap();
    assert!(
        plain.status.success(),
        "{}",
        String::from_utf8_lossy(&plain.stderr)
    );
    let plain = String::from_utf8(plain.stdout).unwrap();
    assert!(
        !plain.contains("base(contract"),
        "base-rate header must require --by-model: {plain}"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn unpriced_is_flagged_without_by_model() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-costs-unpriced-{}", uuid::Uuid::now_v7()));
    let runs = write_cost_run(&root, "unpriced-run", 0, "missing-contract", false, false);

    let plain = costs(&runs, false);
    assert_eq!(plain["runs"][0]["unpriced"], true);
    assert_eq!(plain["runs"][0]["cost_microusd"], 0);

    let with_models = costs(&runs, true);
    assert_eq!(with_models["runs"][0]["unpriced"], true);
    assert_eq!(with_models["models"][0]["unpriced_calls"], 1);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn by_model_rates_require_one_base_row() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-costs-mixed-{}", uuid::Uuid::now_v7()));
    let contract_a = write_contract(&root.join("contract-a"), 2.0, 10.0);
    let contract_b = write_contract(&root.join("contract-b"), 5.0, 20.0);
    write_cost_run(&root, "run-a", 10, contract_a.as_str(), false, false);
    let runs = write_cost_run(&root, "run-b", 20, contract_b.as_str(), false, false);

    let mixed = costs(&runs, true);
    let models = mixed["models"].as_array().unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["tokens_input"], 200);
    assert_eq!(models[0]["cost_microusd"], 30);
    assert!(models[0]["base_input_rate"].is_null());
    assert!(models[0]["base_output_rate"].is_null());

    let single_root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-costs-single-{}", uuid::Uuid::now_v7()));
    let contract = write_contract(&single_root.join("contract"), 2.0, 10.0);
    let single_runs = write_cost_run(&single_root, "run", 10, contract.as_str(), false, false);
    let single = costs(&single_runs, true);
    assert_eq!(single["models"][0]["base_input_rate"], 2.0);
    assert_eq!(single["models"][0]["base_output_rate"], 10.0);

    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(single_root).unwrap();
}

#[test]
fn unmetered_search_is_flagged_in_both_modes() {
    for (search, route_failed) in [(true, false), (false, true)] {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!(
                "runs-costs-search-{}-{}",
                search,
                uuid::Uuid::now_v7()
            ));
        let runs = write_cost_run(&root, "run", 42, "demo", search, route_failed);

        let plain = costs(&runs, false);
        assert_eq!(
            plain["runs"][0]["unpriced"], true,
            "search={search}, route_failed={route_failed}"
        );

        let with_models = costs(&runs, true);
        assert_eq!(with_models["runs"][0]["unpriced"], true);
        if search {
            assert_eq!(with_models["models"][0]["unpriced_calls"], 1);
        }

        std::fs::remove_dir_all(root).unwrap();
    }
}
