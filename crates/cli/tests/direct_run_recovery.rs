use camino::{Utf8Path, Utf8PathBuf};
use engine::{Interaction, RunState};
use serde_json::Value;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

struct Fixture(Utf8PathBuf);
impl Fixture {
    fn new(generator: &str) -> Self {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-direct-recovery-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(root.join("generator")).unwrap();
        let source = Utf8Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/generators")
            .join(generator)
            .join(contract::MANIFEST_FILE);
        std::fs::copy(source, root.join("generator").join(contract::MANIFEST_FILE)).unwrap();
        Self(root)
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut child = Command::new(env!("CARGO_BIN_EXE_qcg"))
            .current_dir(&self.0)
            .args(args)
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
                let output = child.wait_with_output().unwrap();
                panic!("{args:?} exceeded deadline: {output:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        child.wait_with_output().unwrap()
    }

    fn show(&self, id: &str) -> Value {
        let output = self.run(&["runs", "show", id, "--json"]);
        assert!(output.status.success(), "{output:?}");
        serde_json::from_slice(&output.stdout).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn paused_direct_journals_coexist_resume_and_can_be_discarded() {
    for (generator, status) in [
        ("ask-user", "waiting"),
        ("side-effect-confirm", "confirming"),
    ] {
        let fixture = Fixture::new(generator);
        let first = fixture.run(&["run", "generator", "--output", "out", "--yes"]);
        assert!(!first.status.success(), "{first:?}");
        let runs = fixture.0.join(".data/runs");
        let (summary, _) = service::list_run_summaries(&runs, 100)
            .unwrap()
            .pop()
            .unwrap();
        let id = summary.run_id;
        let journal = runs.join(&id).join("meta/journal.jsonl");
        let state = RunState::fold_journal(&journal).unwrap();
        assert!(state.terminal.is_none());
        assert!(state.package_sha256.is_none());
        let answer = match state.settled_pending().unwrap() {
            Interaction::Question { question } => ("--answer", format!("{}=brief", question.id)),
            Interaction::Confirmation { confirm } => {
                ("--confirm", format!("{}=approve", confirm.id))
            }
        };
        let shown = fixture.show(&id);
        assert_eq!(shown["status"], status);
        assert_eq!(shown["direct_owner"], "inactive");
        assert!(
            shown["recovery"]
                .as_str()
                .unwrap()
                .contains("qcg runs delete")
        );
        assert_eq!(shown["finished_at"], Value::Null);

        // This used to fail at construction with "no pinned generator package identity".
        let second = fixture.run(&["run", "generator", "--output", "second", "--yes"]);
        assert!(!second.status.success());
        assert!(!String::from_utf8_lossy(&second.stderr).contains("no pinned generator"));
        let summaries = service::list_run_summaries(&runs, 100).unwrap();
        assert_eq!(summaries.len(), 2);
        let other = summaries
            .iter()
            .find(|(s, _)| s.run_id != id)
            .unwrap()
            .0
            .run_id
            .clone();
        assert_eq!(fixture.show(&other)["status"], status);
        let listed = fixture.run(&["runs", "list"]);
        assert!(listed.status.success(), "{listed:?}");
        let text = String::from_utf8_lossy(&listed.stdout);
        assert!(
            text.contains(&id) && text.contains(&other) && text.contains("direct owner inactive")
        );

        // Both store modes may start and recover without adopting direct work,
        // even with more direct journals than service scheduling slots.
        let before = std::fs::read(&journal).unwrap();
        for mode in [
            service::RunStoreMode::Exclusive,
            service::RunStoreMode::SharedFilesystem,
        ] {
            let service = service::LocalService::with_generator_roots_policy_and_store_mode(
                vec![fixture.0.join("generator")],
                runs.clone(),
                None,
                1,
                1,
                mode,
                service::ServiceDeploymentPolicy::default(),
            )
            .unwrap();
            service.resume_recovered_runs().await;
            assert_eq!(std::fs::read(&journal).unwrap(), before);
            let snapshot = service.snapshot(id.clone()).await.unwrap();
            assert_eq!(
                snapshot.state,
                if status == "waiting" {
                    api::RunStatus::Waiting
                } else {
                    api::RunStatus::Confirming
                }
            );
        }

        // A live direct owner holds the same execution lease as service runs.
        let lease = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(runs.join(&other).join("meta/execution.lock"))
            .unwrap();
        lease.try_lock().unwrap();
        assert_eq!(fixture.show(&other)["direct_owner"], "active");
        let refused = fixture.run(&["runs", "delete", &other]);
        assert!(!refused.status.success());
        assert!(String::from_utf8_lossy(&refused.stderr).contains("executing elsewhere"));
        assert!(runs.join(&other).exists());
        drop(lease);
        let discarded = fixture.run(&["runs", "delete", &other]);
        assert!(discarded.status.success(), "{discarded:?}");
        assert!(!runs.join(&other).exists());

        let continued = fixture.run(&[
            "run",
            "generator",
            "--output",
            "out",
            "--yes",
            answer.0,
            &answer.1,
        ]);
        assert!(continued.status.success(), "{continued:?}");
        assert_eq!(fixture.show(&id)["status"], "success");
        let events = service::read_journal_events(&runs.join(&id)).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event["t"] == "run_started")
                .count(),
            1
        );
        assert!(events.iter().any(|event| event["t"] == "run_resumed"));
        let deleted = fixture.run(&["runs", "delete", &id]);
        assert!(deleted.status.success(), "{deleted:?}");
    }
}

#[test]
fn changed_direct_contract_has_actionable_recovery_without_poisoning_store() {
    let fixture = Fixture::new("ask-user");
    fixture.run(&["run", "generator", "--output", "out", "--yes"]);
    let runs = fixture.0.join(".data/runs");
    let id = service::list_run_summaries(&runs, 100)
        .unwrap()
        .pop()
        .unwrap()
        .0
        .run_id;
    let manifest = fixture.0.join("generator").join(contract::MANIFEST_FILE);
    let source = std::fs::read_to_string(&manifest).unwrap();
    std::fs::write(
        manifest,
        source.replace("Choose the output mode", "Changed question"),
    )
    .unwrap();
    let resumed = fixture.run(&["run", "generator", "--output", "out", "--yes"]);
    assert!(!resumed.status.success());
    let error = String::from_utf8_lossy(&resumed.stderr);
    assert!(error.contains("contract changed"), "{error}");
    assert!(error.contains(&format!("qcg runs delete {id}")), "{error}");
    assert_eq!(fixture.show(&id)["status"], "waiting");
    assert!(fixture.run(&["runs", "delete", &id]).status.success());
}

#[test]
fn inactive_direct_owner_does_not_make_unsettled_execution_terminal_or_deletable() {
    let fixture = Fixture::new("ask-user");
    fixture.run(&["run", "generator", "--output", "out", "--yes"]);
    let runs = fixture.0.join(".data/runs");
    let id = service::list_run_summaries(&runs, 100)
        .unwrap()
        .pop()
        .unwrap()
        .0
        .run_id;
    let directory = runs.join(&id);
    let events = service::read_journal_events(&directory).unwrap();
    // Model a dead owner before the question is settled, without starting or
    // abandoning external effects. Only the journal determines run status.
    let started = events
        .iter()
        .find(|event| event["t"] == "run_started")
        .unwrap();
    std::fs::write(
        directory.join("meta/journal.jsonl"),
        format!("{}\n", serde_json::to_string(started).unwrap()),
    )
    .unwrap();
    let shown = fixture.show(&id);
    assert_eq!(shown["status"], "running");
    assert_eq!(shown["direct_owner"], "inactive");
    assert_eq!(shown["finished_at"], Value::Null);
    let listed = fixture.run(&["runs", "list"]);
    assert!(listed.status.success());
    let text = String::from_utf8_lossy(&listed.stdout);
    assert!(text.contains("\trunning\t") && text.contains("direct owner inactive"));
    let refused = fixture.run(&["runs", "delete", &id]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("unsettled direct execution"));
    assert!(directory.exists());
}
