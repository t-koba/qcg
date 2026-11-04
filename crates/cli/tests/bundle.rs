use camino::Utf8PathBuf;
use serde_json::json;
use std::{io::Read as _, process::Command};

fn bundle_fixture() -> (Utf8PathBuf, Utf8PathBuf, Utf8PathBuf) {
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
    (root, runs, meta)
}

fn export(runs: &camino::Utf8Path, destination: &camino::Utf8Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_qcg"))
        .args(["runs", "export", "success", "--runs-dir"])
        .arg(runs)
        .arg("--output")
        .arg(destination)
        .env_remove("PROVIDERS")
        .env("AUTO_GC", "false")
        .env("MAX_ARTIFACT_BYTES", "1")
        .env("MAX_ARTIFACT_ENTRIES", "1")
        .output()
        .unwrap()
}

#[test]
fn bundle_export_keeps_cli_unbounded_defaults() {
    let (root, runs, meta) = bundle_fixture();
    // This source exceeds the HTTP provisional default, but CLI remains unbounded.
    let audit = vec![b'x'; 16 * 1024 * 1024 + 1];
    std::fs::write(meta.join("audit.jsonl"), &audit).unwrap();
    let archive = root.join("bundle.zip");
    let output = export(&runs, &archive);
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

#[test]
fn bundle_export_refuses_source_aliases_without_modifying_sources() {
    use sha2::{Digest as _, Sha256};
    let (root, runs, meta) = bundle_fixture();
    let artifact = runs.join("success/workspace/result.txt");
    std::fs::create_dir_all(artifact.parent().unwrap()).unwrap();
    std::fs::write(&artifact, b"result").unwrap();
    std::fs::write(meta.join("audit.jsonl"), b"audit bytes\n").unwrap();
    std::fs::write(
        meta.join("outputs.json"),
        serde_json::to_vec(&json!({"artifacts": [{
            "path": "result.txt", "sha256": hex::encode(Sha256::digest(b"result")),
            "bytes": 6, "label": "result", "required": true
        }]}))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        meta.join("invalidation.json"),
        serde_json::to_vec(&json!({
            "run_id": "success", "reason": "test", "actor_label": "test",
            "invalidated_at": "2026-11-04T00:00:00Z"
        }))
        .unwrap(),
    )
    .unwrap();
    let sources = [
        meta.join("journal.jsonl"),
        meta.join("audit.jsonl"),
        meta.join("outputs.json"),
        meta.join("invalidation.json"),
        artifact,
    ];
    let before: Vec<_> = sources.iter().map(|p| std::fs::read(p).unwrap()).collect();
    let mut destinations = sources.to_vec();
    destinations.push(meta.join("../meta/journal.jsonl"));
    // Hard links are available on Unix and Windows without symlink privileges.
    // Invalidation metadata already refuses hard links during bundle preparation.
    for (index, source) in sources.iter().enumerate().filter(|(i, _)| *i != 3) {
        let alias = root.join(format!("hard-link-{index}"));
        std::fs::hard_link(source, &alias).unwrap();
        destinations.push(alias);
    }
    #[cfg(unix)]
    {
        let alias = root.join("journal-symlink");
        std::os::unix::fs::symlink(&sources[0], &alias).unwrap();
        destinations.push(alias);
        let parent_alias = root.join("meta-symlink");
        std::os::unix::fs::symlink(&meta, &parent_alias).unwrap();
        destinations.push(parent_alias.join("journal.jsonl"));
    }
    for destination in destinations {
        let output = export(&runs, &destination);
        assert!(
            !output.status.success(),
            "export to {destination} succeeded"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("aliases source"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        for (source, bytes) in sources.iter().zip(&before) {
            assert_eq!(std::fs::read(source).unwrap(), *bytes, "{source} changed");
        }
    }
    // Independent existing destinations still get replaced, rather than refused.
    let archive = root.join("independent.zip");
    std::fs::write(&archive, b"replace me").unwrap();
    let output = export(&runs, &archive);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut zip = zip::ZipArchive::new(std::fs::File::open(archive).unwrap()).unwrap();
    let mut actual = Vec::new();
    zip.by_name("artifacts/result.txt")
        .unwrap()
        .read_to_end(&mut actual)
        .unwrap();
    assert_eq!(actual, b"result");
    drop(zip);
    std::fs::remove_dir_all(root).unwrap();
}
