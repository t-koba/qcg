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

#[cfg(unix)]
mod signals {
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    use std::process::Child;

    // Always clean up the fixture's processes, including when an assertion or
    // readiness deadline fails. Never signal the test runner's process group.
    struct Running {
        child: Option<Child>,
        pid_file: Utf8PathBuf,
    }
    impl Drop for Running {
        fn drop(&mut self) {
            let Some(child) = &mut self.child else { return };
            let _ = child.kill();
            let _ = child.wait();
            if let Ok(pids) = std::fs::read_to_string(&self.pid_file)
                && let Some(pid) = pids.split_whitespace().next()
                && let Ok(pid) = pid.parse::<i32>()
            {
                // SAFETY: only the fixture command's own process group is
                // targeted, and only while its original leader still exists.
                unsafe {
                    if libc::getpgid(pid) == pid {
                        libc::killpg(pid, libc::SIGKILL);
                    }
                }
            }
        }
    }

    fn wait_until(mut ready: impl FnMut() -> bool, detail: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready() {
            assert!(Instant::now() < deadline, "timed out: {detail}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn check_signal(signal: i32) {
        for json in [false, true] {
            let fixture = Fixture::new("side-effect-confirm");
            std::fs::write(
                fixture.0.join("generator").join(contract::MANIFEST_FILE),
                r#"
[generator]
id = "signal-test"
name = "Signal test"
version = "0.1.0"
[permissions]
network = []
fs_read = ["workspace"]
fs_write = ["workspace"]
side_effects = "allowed"
containers = { enabled = false }
[[permissions.commands]]
bin = "sh"
args = ["-c", "sleep 60 & echo $$ $! > pids; wait"]
purpose = "bounded cancellation fixture"
isolation = "trusted_host"
[[flow]]
id = "long_command"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 60 & echo $$ $! > pids; wait"]
"#,
            )
            .unwrap();
            let mut command = Command::new(env!("CARGO_BIN_EXE_qcg"));
            command
                .current_dir(&fixture.0)
                .args(["run", "generator", "--output", "out", "--yes"])
                .env("AUTO_GC", "0")
                .env_remove("PROVIDERS")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if json {
                command.arg("--json");
            }
            let mut running = Running {
                child: Some(command.spawn().unwrap()),
                pid_file: fixture.0.join("out/pids"),
            };
            let mut pids = Vec::new();
            wait_until(
                || {
                    if running
                        .child
                        .as_mut()
                        .unwrap()
                        .try_wait()
                        .unwrap()
                        .is_some()
                    {
                        let output = running.child.take().unwrap().wait_with_output().unwrap();
                        panic!("qcg exited before command readiness: {output:?}");
                    }
                    pids = std::fs::read_to_string(&running.pid_file)
                        .unwrap_or_default()
                        .split_whitespace()
                        .map(|pid| pid.parse::<i32>().unwrap())
                        .collect();
                    pids.len() == 2
                },
                "command and descendant readiness",
            );
            // Signal only qcg, not its children: the run's cancellation token
            // and ProcessTreeGuard must do the actual descendant cleanup.
            let pid = i32::try_from(running.child.as_ref().unwrap().id()).unwrap();
            // SAFETY: pid identifies the live qcg child owned by this test.
            assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
            wait_until(
                || {
                    running
                        .child
                        .as_mut()
                        .unwrap()
                        .try_wait()
                        .unwrap()
                        .is_some()
                },
                "qcg cancellation settlement",
            );
            wait_until(
                || {
                    pids.iter().all(|pid| {
                        // SAFETY: signal zero probes existence without killing.
                        (unsafe { libc::kill(*pid, 0) == -1 })
                            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                    })
                },
                "command and descendant reaping",
            );
            let output = running.child.take().unwrap().wait_with_output().unwrap();
            assert!(!output.status.success(), "{output:?}");
            assert_eq!(output.status.signal(), None, "{output:?}");
            let runs = fixture.0.join(".data/runs");
            let id = service::list_run_summaries(&runs, 100)
                .unwrap()
                .pop()
                .unwrap()
                .0
                .run_id;
            let state = RunState::fold_journal(&runs.join(&id).join("meta/journal.jsonl")).unwrap();
            assert_eq!(state.terminal, Some(engine::TerminalState::Canceled));
            let events = service::read_journal_events(&runs.join(&id)).unwrap();
            assert_eq!(events.last().unwrap()["t"], "run_canceled");
            assert_eq!(fixture.show(&id)["status"], "canceled");
            let deleted = fixture.run(&["runs", "delete", &id]);
            assert!(deleted.status.success(), "{deleted:?}");
            assert!(!runs.join(&id).exists());
            // A second direct run can acquire the same store and output lease.
            let source = Utf8Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../fixtures/generators/ask-user")
                .join(contract::MANIFEST_FILE);
            std::fs::copy(
                source,
                fixture.0.join("generator").join(contract::MANIFEST_FILE),
            )
            .unwrap();
            let second = fixture.run(&["run", "generator", "--output", "out", "--yes"]);
            assert!(!second.status.success(), "{second:?}");
            assert_eq!(fixture.show(&id)["status"], "waiting");
        }
    }

    #[test]
    fn sigint_settles_direct_runs_and_reaps_descendants() {
        check_signal(libc::SIGINT);
    }

    #[test]
    fn sigterm_settles_direct_runs_and_reaps_descendants() {
        check_signal(libc::SIGTERM);
    }
}
