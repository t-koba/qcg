use camino::Utf8PathBuf;
use serde_json::json;
use std::{io::Read as _, process::Command};

#[test]
fn bundle_export_keeps_cli_unbounded_defaults() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("bundle-cli-{}", uuid::Uuid::now_v7()));
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
    // This source exceeds the HTTP provisional default, but CLI remains unbounded.
    let audit = vec![b'x'; 16 * 1024 * 1024 + 1];
    std::fs::write(meta.join("audit.jsonl"), &audit).unwrap();
    let archive = root.join("bundle.zip");
    let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .args(["runs", "export", "success", "--runs-dir"])
        .arg(&runs)
        .arg("--output")
        .arg(&archive)
        .env_remove("PROVIDERS")
        .env("AUTO_GC", "false")
        .env("MAX_ARTIFACT_BYTES", "1")
        .env("MAX_ARTIFACT_ENTRIES", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut zip = zip::ZipArchive::new(std::fs::File::open(&archive).unwrap()).unwrap();
    let mut entry = zip.by_name("audit.jsonl").unwrap();
    assert_eq!(entry.size(), audit.len() as u64);
    let mut actual = Vec::new();
    entry.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, audit);
    drop(entry);
    drop(zip);
    std::fs::remove_dir_all(root).unwrap();
}
