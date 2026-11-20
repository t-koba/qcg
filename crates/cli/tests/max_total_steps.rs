//! Literal `qcg run` half of the `MAX_TOTAL_STEPS` fixed check. The parity
//! test in `main.rs` observes the journaled ceiling on both startup paths;
//! this one runs the released binary so the direct execution path itself is
//! covered, not only the service it constructs.

use camino::{Utf8Path, Utf8PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const MANIFEST: &str = r#"[generator]
id = "three-steps"
name = "Three Steps"
version = "0.1.0"
[[inputs.stages]]
id = "basic"
[permissions]
fs_read = []
fs_write = ["workspace"]
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
[permissions.containers]
enabled = false
[[flow]]
id = "one"
type = "write"
[flow.params]
content = "one"
output_file = "one.txt"
[[flow]]
id = "two"
type = "write"
[flow.params]
content = "two"
output_file = "two.txt"
[[flow]]
id = "three"
type = "write"
[flow.params]
content = "three"
output_file = "three.txt"
"#;

fn fixture(name: &str) -> Utf8PathBuf {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("qcg-max-steps-{name}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(root.join("generator")).unwrap();
    std::fs::write(
        root.join("generator").join(contract::MANIFEST_FILE),
        MANIFEST,
    )
    .unwrap();
    root
}

fn run(root: &Utf8Path, ceiling: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .current_dir(root)
        .args(["run", "generator", "--output", "out", "--yes"])
        .env("MAX_TOTAL_STEPS", ceiling)
        .env("AUTO_GC", "0")
        .env_remove("PROVIDERS")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("qcg run exceeded the 10 second bound");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

#[test]
fn direct_run_honors_the_deployment_ceiling() {
    let root = fixture("ceiling");
    let output = run(&root, "2");
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("global step budget exceeded: 2 >= 2"),
        "a request over the ceiling must be refused: {stderr}"
    );
    let meta = root.join(service::direct_run_meta_dir(Utf8Path::new("out")));
    let journal = std::fs::read_to_string(meta.join("journal.jsonl"))
        .unwrap_or_else(|error| panic!("direct run journal should read: {error}"));
    let queued = journal
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find(|event| event["t"] == "run_queued")
        .expect("the direct run should journal its admission");
    assert_eq!(queued["effective_max_total_steps"], 2);
    assert_eq!(
        queued["effective_policy_origin"],
        format!(
            "service-ceiling:2 caps contract:{}",
            engine::RunOptions::default_max_total_steps()
        )
    );
    let _ = std::fs::remove_dir_all(&root);

    let zero = fixture("zero");
    let output = run(&zero, "0");
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("MAX_TOTAL_STEPS must be greater than zero"),
        "{stderr}"
    );
    assert!(
        !zero.join(".data/runs").exists() && !zero.join("out").exists(),
        "an invalid ceiling must refuse before creating run or output directories"
    );
    let _ = std::fs::remove_dir_all(&zero);
}
