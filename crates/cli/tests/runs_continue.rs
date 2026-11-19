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

#[cfg(unix)]
mod cli_continuation {
    use api::{RunStatus, StartRun};
    use camino::Utf8PathBuf;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::process::Command;

    const CONTINUE_MANIFEST: &str = r#"
    [generator]
    id = "continue-source"
    name = "Continue Source"
    version = "0.1.0"
    description = "Read-only checks over two file inputs."

    [runtime]
    file_input_limit_bytes = 4096

    [[inputs.stages]]
    id = "files"

    [[inputs.stages.fields]]
    id = "good"
    required = true
    type = "file"

    [[inputs.stages.fields]]
    id = "bad"
    required = true
    type = "file"

    [[flow]]
    id = "check_good"
    type = "check.format"

    [flow.params]
    content = "json"
    source = "{{ inputs.good }}"

    [[flow]]
    id = "check_bad"
    type = "check.format"

    [flow.params]
    content = "json"
    source = "{{ inputs.bad }}"

    [permissions]
    fs_read = ["workspace"]
    fs_write = []
    network = []
    commands = []
    side_effects = "none"

    [permissions.containers]
    enabled = false
    "#;

    struct Fixture(Utf8PathBuf);

    impl Fixture {
        fn new() -> Self {
            let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .expect("temporary directory path must be UTF-8")
                .join(format!("qcg-cli-continue-{}", uuid::Uuid::now_v7()));
            let generator = root.join("generators").join("continue-source");
            std::fs::create_dir_all(&generator).expect("generator directory");
            std::fs::write(generator.join(contract::MANIFEST_FILE), CONTINUE_MANIFEST)
                .expect("generator manifest");
            std::fs::create_dir_all(root.join("runs")).expect("runs directory");
            Self(root)
        }

        fn runs(&self) -> Utf8PathBuf {
            self.0.join("runs")
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn default_service(root: &Utf8PathBuf, runs: &Utf8PathBuf) -> service::LocalService {
        service::LocalService::with_generator_roots_policy_and_store_mode(
            vec![root.join("generators")],
            runs.clone(),
            None,
            policy::DEFAULT_MAX_ACTIVE_RUNS,
            policy::DEFAULT_MAX_TRACKED_RUNS,
            service::RunStoreMode::Exclusive,
            service::ServiceDeploymentPolicy::default(),
        )
        .expect("default-policy service")
    }

    fn journal_events(run_dir: &Utf8PathBuf) -> Vec<Value> {
        std::fs::read_to_string(run_dir.join("meta/journal.jsonl"))
            .expect("journal should be readable")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("journal event"))
            .collect()
    }

    async fn wait_for_terminal(service: &service::LocalService, id: &str) -> RunStatus {
        for _ in 0..500 {
            let snapshot = service
                .snapshot(id.to_string())
                .await
                .expect("run snapshot should exist");
            if !matches!(snapshot.state, RunStatus::Queued | RunStatus::Running) {
                return snapshot.state;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("run {id} did not reach a terminal state");
    }

    /// The CLI owns its service for one command only, so the test-owned service
    /// must surrender the exclusive store lock before the binary starts.
    async fn release_store_lock(runs: Utf8PathBuf) {
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(runs.join(".service.lock"))
                .expect("service lock file");
            file.lock().expect("store lock should become available");
            file.unlock().expect("test store lock should release");
        })
        .await
        .expect("store lock waiter should complete");
    }

    #[tokio::test]
    async fn runs_continue_creates_linked_child_from_default_wallclock_source() {
        let fixture = Fixture::new();
        let runs = fixture.runs();
        let service = default_service(&fixture.0, &runs);
        let source_id = service
            .start_run(StartRun {
                generator_id: "continue-source".into(),
                inputs: BTreeMap::from([
                    ("good".into(), json!({"name": "good.json", "text": "{}"})),
                    ("bad".into(), json!({"name": "bad.json", "text": "{"})),
                ]),
                ..Default::default()
            })
            .await
            .expect("failed source should be admitted");
        assert_eq!(
            wait_for_terminal(&service, &source_id).await,
            RunStatus::Failed
        );

        let source_dir = runs.join(&source_id);
        let source_journal =
            std::fs::read_to_string(source_dir.join("meta/journal.jsonl")).expect("source journal");
        let source_events = journal_events(&source_dir);
        let source_started_at = source_events
            .iter()
            .find(|event| event["t"] == "run_queued")
            .expect("source queue")["ts"]
            .clone();
        assert!(
            source_dir.join("meta/.clean_shutdown").exists(),
            "terminal source journal must be sealed"
        );

        service
            .shutdown_active_runs()
            .await
            .expect("source service should shut down");
        drop(service);
        release_store_lock(runs.clone()).await;

        let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
            .args(["runs", "continue", &source_id, "--runs-dir"])
            .arg(&runs)
            .arg("--json")
            .env("AUTO_GC", "false")
            .env_remove("PROVIDERS")
            .env("RUST_LOG", "off")
            .stdin(std::process::Stdio::null())
            .output()
            .expect("qcg runs continue should complete");
        assert!(
            output.status.success(),
            "continuation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let payload: Value =
            serde_json::from_slice(&output.stdout).expect("continue should print one JSON object");
        let child_id = payload["child_run_id"]
            .as_str()
            .expect("child_run_id in CLI JSON")
            .to_owned();
        assert_ne!(child_id, source_id);

        // The CLI command returns as soon as the child is admitted, so its own
        // service exits before the queued child executes. A fresh service resumes
        // the durable queue exactly as the service continuation test does.
        let resumed = default_service(&fixture.0, &runs);
        resumed.resume_recovered_runs().await;
        assert_eq!(
            wait_for_terminal(&resumed, &child_id).await,
            RunStatus::Failed
        );

        let child_dir = runs.join(&child_id);
        let child_events = journal_events(&child_dir);
        let fork = child_events
            .iter()
            .find(|event| event["t"] == "run_forked")
            .expect("run_forked");
        assert_eq!(fork["source_run_id"], source_id);
        assert_eq!(
            fork["source_contract_sha256"],
            child_events[0]["contract_sha256"]
        );

        let overlay = child_events
            .iter()
            .find(|event| event["t"] == "child_reset_overlay")
            .expect("child_reset_overlay");
        let candidate = &overlay["overlay"]["candidate"];
        assert_eq!(candidate["preserved_outcomes"], json!(["check_good"]));
        assert_eq!(candidate["stale_outcomes"], json!([]));
        assert_eq!(candidate["retry_roots"], json!(["check_bad"]));
        assert_eq!(
            candidate["reset_nodes"]
                .as_object()
                .expect("reset nodes")
                .keys()
                .collect::<Vec<_>>(),
            vec!["check_bad"]
        );
        let budget = &candidate["inherited_budget"];
        assert_eq!(budget["steps_executed"], 2);
        assert_eq!(budget["steps_succeeded"], 1);
        assert_eq!(budget["steps_failed"], 1);
        assert_eq!(budget["budget_charged"], 2);
        assert_eq!(budget["elapsed_budget_mode"], "wall_clock");
        assert!(budget["elapsed"].is_null());
        assert_eq!(budget["started_at"], source_started_at);

        let fork_seq = fork["seq"].as_u64().expect("seq");
        let child_queued = child_events
            .iter()
            .find(|event| event["t"] == "run_queued" && event["seq"].as_u64() > Some(fork_seq))
            .expect("child run_queued");
        assert_eq!(child_queued["parent_run_id"], source_id);
        assert_eq!(
            std::fs::read_to_string(source_dir.join("meta/journal.jsonl")).expect("source journal"),
            source_journal,
            "continuation must not modify the source run"
        );

        let admission_seq = child_queued["seq"].as_u64().expect("seq");
        let rerun = child_events
            .iter()
            .filter(|event| {
                event["t"] == "step_started"
                    && event["seq"].as_u64().is_some_and(|seq| seq > admission_seq)
            })
            .map(|event| event["node"].as_str().unwrap_or("").to_owned())
            .collect::<Vec<_>>();
        assert_eq!(rerun, vec!["check_bad"]);
    }

    /// A continuation-safe probe fails on its first execution and succeeds
    /// once it can see that prior attempt in the child's inherited journal.
    /// This exercises the full CLI path to a terminal successful child.
    #[tokio::test]
    #[cfg(unix)]
    async fn runs_continue_reaches_success_without_restarting_preserved_node() {
        const RECOVERABLE_MANIFEST: &str = r#"
    [generator]
    id = "continue-recoverable"
    name = "Continue Recoverable"
    version = "0.1.0"
    description = "An early format check and a late, recoverable probe."

    [runtime]
    file_input_limit_bytes = 4096

    [[inputs.stages]]
    id = "files"

    [[inputs.stages.fields]]
    id = "good"
    required = true
    type = "file"

    [[flow]]
    id = "check_good"
    type = "check.format"

    [flow.params]
    content = "json"
    source = "{{ inputs.good }}"

    [[flow]]
    id = "late_probe"
    type = "test.recover_probe"

    [flow.params]
    attempts_before_success = 1

    [permissions]
    fs_read = ["workspace"]
    fs_write = []
    network = []
    commands = []
    side_effects = "none"

    [permissions.containers]
    enabled = false
    "#;

        let fixture = Fixture::new();
        let generator = fixture.0.join("generators").join("continue-recoverable");
        std::fs::create_dir_all(&generator).expect("recoverable generator directory");
        std::fs::write(
            generator.join(contract::MANIFEST_FILE),
            RECOVERABLE_MANIFEST,
        )
        .expect("recoverable generator manifest");
        let runs = fixture.runs();
        let service = default_service(&fixture.0, &runs);
        let source_id = service
            .start_run(StartRun {
                generator_id: "continue-recoverable".into(),
                inputs: BTreeMap::from([(
                    "good".into(),
                    json!({"name": "good.json", "text": "{}"}),
                )]),
                ..Default::default()
            })
            .await
            .expect("recoverable source should be admitted");
        assert_eq!(
            wait_for_terminal(&service, &source_id).await,
            RunStatus::Failed
        );

        let source_dir = runs.join(&source_id);
        let source_before = std::fs::read(source_dir.join("meta/journal.jsonl"))
            .expect("source journal before continuation");
        let source_events = journal_events(&source_dir);
        let source_queued = source_events
            .iter()
            .find(|event| event["t"] == "run_queued")
            .expect("source queue");
        assert_eq!(source_queued["elapsed_budget_mode"], "wall_clock");
        let source_probe = source_events
            .iter()
            .find(|event| event["t"] == "step_finished" && event["node"] == "late_probe")
            .expect("source probe outcome");
        assert_eq!(source_probe["status"], "check_failed");
        assert!(
            source_probe["reason"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("0 prior attempts")),
            "probe must fail on its first attempt: {source_probe}"
        );
        assert!(source_dir.join("meta/.clean_shutdown").exists());

        service
            .shutdown_active_runs()
            .await
            .expect("source service should shut down");
        drop(service);
        release_store_lock(runs.clone()).await;

        let output = Command::new(env!("CARGO_BIN_EXE_qcg"))
            .args(["runs", "continue", &source_id, "--runs-dir"])
            .arg(&runs)
            .arg("--json")
            .env("AUTO_GC", "false")
            .env_remove("PROVIDERS")
            .env("RUST_LOG", "off")
            .stdin(std::process::Stdio::null())
            .output()
            .expect("qcg runs continue should complete");
        assert!(
            output.status.success(),
            "continuation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let payload: Value =
            serde_json::from_slice(&output.stdout).expect("continue should print one JSON object");
        let child_id = payload["child_run_id"]
            .as_str()
            .expect("child_run_id in CLI JSON")
            .to_owned();
        assert_ne!(child_id, source_id);

        // The CLI returns as soon as the child is admitted; a fresh service
        // resumes its durable queue to completion.
        let resumed = default_service(&fixture.0, &runs);
        resumed.resume_recovered_runs().await;
        assert_eq!(
            wait_for_terminal(&resumed, &child_id).await,
            RunStatus::Succeeded,
            "recovered child must reach terminal success"
        );

        let child_events = journal_events(&runs.join(&child_id));
        let fork = child_events
            .iter()
            .find(|event| event["t"] == "run_forked")
            .expect("run_forked");
        assert_eq!(fork["source_run_id"], source_id);
        assert_eq!(
            fork["source_contract_sha256"],
            child_events[0]["contract_sha256"]
        );
        let overlay = child_events
            .iter()
            .find(|event| event["t"] == "child_reset_overlay")
            .expect("child_reset_overlay");
        let candidate = &overlay["overlay"]["candidate"];
        assert_eq!(candidate["preserved_outcomes"], json!(["check_good"]));
        assert_eq!(candidate["stale_outcomes"], json!([]));
        assert_eq!(candidate["retry_roots"], json!(["late_probe"]));
        assert_eq!(
            candidate["reset_nodes"]
                .as_object()
                .expect("reset nodes")
                .keys()
                .collect::<Vec<_>>(),
            vec!["late_probe"]
        );
        let budget = &candidate["inherited_budget"];
        assert_eq!(budget["steps_executed"], 2);
        assert_eq!(budget["steps_succeeded"], 1);
        assert_eq!(budget["steps_failed"], 1);
        assert_eq!(budget["budget_charged"], 2);
        assert_eq!(budget["elapsed_budget_mode"], "wall_clock");
        assert!(budget["elapsed"].is_null());
        assert_eq!(budget["started_at"], source_queued["ts"]);

        let fork_seq = fork["seq"].as_u64().expect("fork seq");
        let child_queued = child_events
            .iter()
            .find(|event| event["t"] == "run_queued" && event["seq"].as_u64() > Some(fork_seq))
            .expect("child run_queued");
        assert_eq!(child_queued["parent_run_id"], source_id);
        let admission_seq = child_queued["seq"].as_u64().expect("child queue seq");
        let after_admission =
            |event: &Value| event["seq"].as_u64().is_some_and(|seq| seq > admission_seq);
        assert!(
            !child_events.iter().any(|event| {
                event["t"] == "step_started"
                    && event["node"] == "check_good"
                    && after_admission(event)
            }),
            "early success must not start again"
        );
        let child_state: Value = serde_json::from_slice(
            &std::fs::read(runs.join(&child_id).join("meta/state.json")).expect("child state"),
        )
        .expect("child state JSON");
        assert_eq!(child_state["nodes"]["check_good"]["status"], "success");
        assert_eq!(
            child_state["node_executions"]["check_good"], 1,
            "preserved outcome keeps only its source execution"
        );
        let started = child_events
            .iter()
            .filter(|event| event["t"] == "step_started" && after_admission(event))
            .map(|event| event["node"].as_str().unwrap_or(""))
            .collect::<Vec<_>>();
        assert_eq!(started, vec!["late_probe"], "only the failed node restarts");
        let child_probe = child_events
            .iter()
            .find(|event| {
                event["t"] == "step_finished"
                    && event["node"] == "late_probe"
                    && after_admission(event)
            })
            .expect("child probe outcome");
        assert_eq!(child_probe["status"], "success");
        assert_eq!(child_probe["output"]["prior_attempts"], 1);
        let finished = child_events
            .iter()
            .find(|event| event["t"] == "run_finished" && after_admission(event))
            .expect("child run_finished");
        assert_eq!(finished["status"], "success");
        assert_eq!(
            std::fs::read(source_dir.join("meta/journal.jsonl"))
                .expect("source journal after child"),
            source_before,
            "continuation must not modify the source journal"
        );
    }
}
