use camino::Utf8PathBuf;
use std::process::Command;

#[test]
fn runs_continue_refuses_without_leaking_paths() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "qcg-continue-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
    let runs = root.join("runs");
    std::fs::create_dir_all(&runs).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .args(["runs", "continue", "missing-run", "--runs-dir"])
        .arg(&runs)
        .arg("--json")
        .env("AUTO_GC", "false")
        .env_remove("PROVIDERS")
        .output()
        .unwrap();
    assert!(!output.status.success(), "missing source must be refused");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("continuation refused: source run is unavailable"),
        "unexpected refusal: {stderr}"
    );
    assert!(
        !stderr.contains(root.as_str()),
        "refusal must not echo a path: {stderr}"
    );
    assert!(
        output.stdout.is_empty(),
        "refusal must not print a child id"
    );
    std::fs::remove_dir_all(root).unwrap();
}
