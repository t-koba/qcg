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

/// A run that pauses on a question after charging its only allowed step, so
/// a resume has to spend a second step beyond the admitted ceiling.
const PAUSED_MANIFEST: &str = r#"[generator]
id = "ask-then-write"
name = "Ask Then Write"
version = "0.1.0"
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
id = "ask"
type = "ask_user"
output = "mode"
[flow.params]
content = "Choose the output mode"
options = ["brief", "detailed"]
[[flow]]
id = "write"
type = "write"
[flow.params]
content = "mode={{ steps.mode.output }}"
output_file = "answer.txt"
"#;

fn fixture(name: &str) -> Utf8PathBuf {
    fixture_with_manifest(name, MANIFEST)
}

fn fixture_with_manifest(name: &str, manifest: &str) -> Utf8PathBuf {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("qcg-max-steps-{name}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(root.join("generator")).unwrap();
    std::fs::write(
        root.join("generator").join(contract::MANIFEST_FILE),
        manifest,
    )
    .unwrap();
    root
}

fn run(root: &Utf8Path, ceiling: &str) -> Output {
    run_with(root, ceiling, &[])
}

fn run_with(root: &Utf8Path, ceiling: &str, extra_args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_qcg"))
        .current_dir(root)
        .args(["run", "generator", "--output", "out", "--yes"])
        .args(extra_args)
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

/// A direct run resumed in another process keeps the ceiling recorded at its
/// admission, even when MAX_TOTAL_STEPS changes in between (E04).
#[test]
fn resumed_direct_run_enforces_the_journaled_ceiling() {
    let root = fixture_with_manifest("resume-ceiling", PAUSED_MANIFEST);
    let first = run(&root, "1");
    assert!(!first.status.success(), "{first:?}");
    let stderr = String::from_utf8_lossy(&first.stderr);
    assert!(
        stderr.contains("run is waiting for user input"),
        "the first run should pause on its question: {stderr}"
    );

    let journal_path = root
        .join(service::direct_run_meta_dir(Utf8Path::new("out")))
        .join("journal.jsonl");
    let journal = std::fs::read_to_string(&journal_path)
        .unwrap_or_else(|error| panic!("direct run journal should read: {error}"));
    let events: Vec<serde_json::Value> = journal
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let queued = events
        .iter()
        .find(|event| event["t"] == "run_queued")
        .expect("the direct run should journal its admission");
    assert_eq!(queued["effective_max_total_steps"], 1);
    let question_id = events
        .iter()
        .find_map(|event| {
            (event["t"] == "step_finished")
                .then(|| event["question"]["id"].as_str())
                .flatten()
        })
        .expect("the paused run should record its pending question")
        .to_owned();

    // A higher ceiling in a second process must not replace the admitted one.
    let resumed = run_with(&root, "10", &["--answer", &format!("{question_id}=brief")]);
    assert!(!resumed.status.success(), "{resumed:?}");
    let stderr = String::from_utf8_lossy(&resumed.stderr);
    assert!(
        stderr.contains("global step budget exceeded: 1 >= 1"),
        "resume must stop at the original ceiling: {stderr}"
    );

    let after = std::fs::read_to_string(&journal_path)
        .unwrap_or_else(|error| panic!("direct run journal should read: {error}"));
    let queued_after: Vec<serde_json::Value> = after
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|event| event["t"] == "run_queued")
        .collect::<Vec<_>>();
    assert_eq!(queued_after.len(), 1, "resume must not re-admit the run");
    assert_eq!(
        &queued_after[0], queued,
        "the journaled ceiling must be unchanged"
    );
    assert_eq!(queued_after[0]["effective_max_total_steps"], 1);
    let _ = std::fs::remove_dir_all(&root);
}

/// A journal missing its admitted ceiling must refuse the resume rather than
/// silently applying the ceiling from the current process environment.
#[test]
fn resumed_direct_run_without_journaled_ceiling_fails_closed() {
    let root = fixture_with_manifest("resume-no-ceiling", PAUSED_MANIFEST);
    let first = run(&root, "1");
    assert!(!first.status.success(), "{first:?}");
    let journal_path = root
        .join(service::direct_run_meta_dir(Utf8Path::new("out")))
        .join("journal.jsonl");
    let journal = std::fs::read_to_string(&journal_path).unwrap();
    let mut events: Vec<serde_json::Value> = journal
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let question_id = events
        .iter()
        .find_map(|event| {
            (event["t"] == "step_finished")
                .then(|| event["question"]["id"].as_str())
                .flatten()
        })
        .expect("the paused run should record its pending question")
        .to_owned();
    let queued = events
        .iter_mut()
        .find(|event| event["t"] == "run_queued")
        .expect("the direct run should journal its admission");
    assert!(
        queued
            .as_object_mut()
            .unwrap()
            .remove("effective_max_total_steps")
            .is_some()
    );
    let stripped = events
        .iter()
        .map(|event| serde_json::to_string(event).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(&journal_path, stripped).unwrap();

    let resumed = run_with(&root, "10", &["--answer", &format!("{question_id}=brief")]);
    assert!(!resumed.status.success(), "{resumed:?}");
    let stderr = String::from_utf8_lossy(&resumed.stderr);
    assert!(
        stderr.contains("effective_max_total_steps") && stderr.contains("refusing to run"),
        "a missing journaled ceiling must fail closed: {stderr}"
    );
    let after = std::fs::read_to_string(&journal_path).unwrap();
    assert!(
        after
            .lines()
            .filter(|line| line.contains("\"t\":\"run_queued\""))
            .count()
            == 1,
        "the failed resume must not append another admission"
    );
    let _ = std::fs::remove_dir_all(&root);
}
