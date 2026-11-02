use camino::Utf8PathBuf;
use serde_json::Value;
use std::process::Command;

fn write_contract(dir: &Utf8PathBuf) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(
        dir.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "replay-fixture"
name = "Replay Fixture"
version = "0.1.0"

[llm]
max_tokens = 1024
[llm.model]
provider = "provider"
model = "model"
input_cost_per_million_usd = 2.0
output_cost_per_million_usd = 10.0

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false

[[flow]]
id = "emit"
type = "write"
params = { content = "x", output_file = "x.txt" }
"#,
    )
    .unwrap();
}

fn seed_run(runs: &Utf8PathBuf, id: &str, generator_path: &Utf8PathBuf, pinned: bool) {
    let run = runs.join(id);
    let meta = run.join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let contract = contract::Contract::load(generator_path).unwrap();
    let package_sha256 = pinned.then(|| {
        let package = run.join("generator-package");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::copy(
            generator_path.join(contract::MANIFEST_FILE),
            package.join(contract::MANIFEST_FILE),
        )
        .unwrap();
        service::package::package_identity_sha256(
            generator_path,
            &service::package::PackageLimits::default(),
        )
        .unwrap()
    });
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            serde_json::json!({
                "generator": "replay-fixture@0.1.0",
                "generator_path": generator_path.as_str(),
                "contract_sha256": contract.sha256,
                "package_sha256": package_sha256,
                "inputs": {},
                "resource_hashes": [],
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock"
            }),
        )
        .unwrap();
    journal
        .event("run_finished", serde_json::json!({"status": "success"}))
        .unwrap();
}

fn replay(runs: &Utf8PathBuf, id: &str) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .args(["runs", "replay", id, "--runs-dir"])
        .arg(runs)
        .arg("--json")
        .env_remove("PROVIDERS")
        .env("AUTO_GC", "false")
        .env("RUST_LOG", "off")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn replay_defaults_to_the_pinned_package_when_the_source_is_gone() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("qcg-replay-pinned-{}", uuid::Uuid::now_v7()));
    let runs = root.join("runs");
    let source = root.join("source");
    write_contract(&source);
    seed_run(&runs, "pinned", &source, true);
    let package = runs.join("pinned").join("generator-package");
    std::fs::remove_dir_all(&source).unwrap();

    let report = replay(&runs, "pinned");
    assert_eq!(report["generator_path"], package.as_str());
    assert!(Utf8PathBuf::from(report["replay_dir"].as_str().unwrap()).exists());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn replay_keeps_the_recorded_path_for_legacy_runs() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("qcg-replay-legacy-{}", uuid::Uuid::now_v7()));
    let runs = root.join("runs");
    let source = root.join("source");
    write_contract(&source);
    seed_run(&runs, "legacy", &source, false);

    let report = replay(&runs, "legacy");
    assert_eq!(report["generator_path"], source.as_str());

    let _ = std::fs::remove_dir_all(root);
}
