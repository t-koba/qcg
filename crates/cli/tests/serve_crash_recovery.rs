//! Real `qcg serve` SIGKILL/restart coverage for host effects. The default
//! path uses a bounded, self-terminating command and proves the fail-closed
//! refusals; the delegated path uses a long-lived command and proves cgroup
//! reclaim. The fixture only owns its `serve` children: it never signals
//! saved numeric PIDs or process groups.
//!
//! Code inspection for host MCP stdio (no separate runtime change):
//! `crates/mcp/src/bounded_stdio.rs::spawn` forwards to
//! `spawn_with_scope(..., None)`, and `crates/mcp/src/mcp/runtime.rs` keeps
//! that unscoped path when `host_stdio_scope` has no delegated cgroup. MCP
//! steps use the same `guard_external_operation` fail-closed ledger as the
//! command step below, so the refusal and evidence assertions here cover
//! host MCP stdio on the default path too.
#![cfg(unix)]

use camino::{Utf8Path, Utf8PathBuf};
use serde_json::{Value, json};
use std::fs::File;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Liveness detector only: no semantic timing assertion depends on it.
const TEST_LIVENESS_GUARD: Duration = Duration::from_secs(60);
const UNRESOLVED_EFFECT: &str = "qcg has not proved that the effect stopped";
// Fixed, redacted HTTP refusal categories; the detailed unresolved-effect
// reason is durable in the journal, not in the refusal response.
const CONTINUE_REFUSAL: &str = "source run is executing or has in-flight work";
const DELETE_REFUSAL: &str = "is retained by idempotency records";
const GENERATOR_ID: &str = "serve-crash-host-command";
// The default command and its descendant exit together after 30s, well past
// this test's normal runtime, so no saved-PID cleanup is needed.
const DEFAULT_COMMAND: [&str; 3] = ["sh", "-c", "sleep 30 & echo $$ $! > pids; wait"];
// Delegated containment must prove cgroup reclaim, not natural expiry.
const DELEGATED_COMMAND: [&str; 3] = ["sh", "-c", "sleep 120 & echo $$ $! > pids; wait"];

struct Fixture {
    root: Utf8PathBuf,
    generators_dir: Utf8PathBuf,
    runs_dir: Utf8PathBuf,
    serve: Option<Child>,
}

impl Fixture {
    fn new(command: &[&str]) -> Self {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-serve-crash-recovery-{}", uuid::Uuid::now_v7()));
        let generators_dir = root.join("generators").join(GENERATOR_ID);
        let runs_dir = root.join("runs");
        std::fs::create_dir_all(&generators_dir).unwrap();
        std::fs::create_dir_all(&runs_dir).unwrap();
        // The 300s contract limit is a product bound, not the fixture
        // lifetime: the default command self-terminates, and delegated
        // containment is reclaimed by the restarted service.
        std::fs::write(
            generators_dir.join(contract::MANIFEST_FILE),
            format!(
                r#"
[generator]
id = "{GENERATOR_ID}"
name = "Serve crash host command"
version = "0.1.0"
[runtime]
command_timeout_seconds = 300
[permissions]
network = []
fs_read = ["workspace"]
fs_write = ["workspace"]
side_effects = "allowed"
containers = {{ enabled = false }}
[[permissions.commands]]
bin = "sh"
args = {args:?}
purpose = "bounded crash-recovery fixture"
isolation = "trusted_host"
[[flow]]
id = "long_command"
type = "command"
[flow.params]
command = {command:?}
"#,
                args = &command[1..],
                command = command,
            ),
        )
        .unwrap();
        Self {
            root,
            generators_dir: generators_dir.parent().unwrap().to_owned(),
            runs_dir,
            serve: None,
        }
    }

    fn start_serve(&mut self, port: u16, log_name: &str) {
        assert!(self.serve.is_none(), "previous serve child is still owned");
        let log = File::create(self.root.join(log_name)).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_qcg"))
            .current_dir(&self.root)
            .args([
                "serve",
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--generators-dir",
                self.generators_dir.as_str(),
                "--runs-dir",
                self.runs_dir.as_str(),
            ])
            .env("AUTO_GC", "0")
            .env_remove("PROVIDERS")
            .env_remove("API_TOKEN_FILE")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        self.serve = Some(child);
    }

    /// Signal only the owned serve child. Descendant cleanup belongs to
    /// product recovery (delegated) or to the command's own lifetime (default).
    fn kill_serve(&mut self) {
        let child = self.serve.as_mut().expect("serve child should be live");
        child.kill().unwrap();
        let deadline = Instant::now() + TEST_LIVENESS_GUARD;
        while child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "SIGKILLed serve did not exit");
            std::thread::sleep(Duration::from_millis(10));
        }
        let status = child.wait().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL), "{status:?}");
        // The dead child is reaped; the restart owns a fresh handle.
        self.serve = None;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(child) = &mut self.serve {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn read_pids(path: &Utf8Path) -> Vec<i32> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .split_whitespace()
        .filter_map(|pid| pid.parse::<i32>().ok())
        .collect()
}

fn process_exists(pid: i32) -> bool {
    // SAFETY: signal zero probes existence without delivering a signal.
    let signalled = unsafe { libc::kill(pid, 0) == 0 };
    signalled || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

async fn wait_ready(client: &reqwest::Client, base: &str, child: &mut Child, log: &Utf8Path) {
    tokio::time::timeout(TEST_LIVENESS_GUARD, async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                let log = std::fs::read_to_string(log).unwrap_or_default();
                panic!("serve exited before readiness ({status}): {log}");
            }
            if client
                .get(format!("{base}/healthz"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("serve should become ready within the liveness guard");
}

fn client_and_base() -> (reqwest::Client, String, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    (
        reqwest::Client::builder().no_proxy().build().unwrap(),
        format!("http://127.0.0.1:{port}"),
        port,
    )
}

fn assert_unresolved_effect(journal: &str) {
    let refusal = journal
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|event| {
            event["t"] == "run_error"
                && event["error"]
                    .as_str()
                    .is_some_and(|error| error.contains(UNRESOLVED_EFFECT))
        })
        .unwrap_or_else(|| panic!("run_error must name the unresolved effect: {journal}"));
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("remains Started"),
        "{refusal}"
    );
}

async fn fetch_journal(client: &reqwest::Client, base: &str, run_id: &str) -> String {
    client
        .get(format!("{base}/api/runs/{run_id}/journal"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap()
}

/// Crash, restart and fail-closed checks shared by both containment paths.
/// Returns the command/descendant PIDs observed before the kill.
async fn recover_and_assert_refusals(
    fixture: &mut Fixture,
    client: &reqwest::Client,
    base: &str,
    port: u16,
) -> Vec<i32> {
    fixture.start_serve(port, "serve-before.log");
    let before_log = fixture.root.join("serve-before.log");
    wait_ready(client, base, fixture.serve.as_mut().unwrap(), &before_log).await;

    let detail: Value = client
        .get(format!("{base}/api/generators/{GENERATOR_ID}"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let package_sha256 = detail["package_sha256"].as_str().unwrap();
    let created = client
        .post(format!("{base}/api/runs"))
        .header(policy::IDEMPOTENCY_HEADER, uuid::Uuid::now_v7().to_string())
        .header("x-qcg-package-sha256", package_sha256)
        .json(&json!({ "generator_id": GENERATOR_ID, "inputs": {} }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        created.status(),
        reqwest::StatusCode::CREATED,
        "{created:?}"
    );
    let location = created
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let run_id = location.rsplit('/').next().unwrap().to_owned();

    // Both the host command and its own descendant are live before the kill:
    // the external effect has demonstrably started.
    let pid_file = fixture.runs_dir.join(&run_id).join("workspace/pids");
    let pids = tokio::time::timeout(TEST_LIVENESS_GUARD, async {
        loop {
            let pids = read_pids(&pid_file);
            if pids.len() == 2 && pids.iter().all(|&pid| pid > 1 && process_exists(pid)) {
                return pids;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("command and descendant should start before the liveness guard");

    fixture.kill_serve();

    fixture.start_serve(port, "serve-after.log");
    let after_log = fixture.root.join("serve-after.log");
    wait_ready(client, base, fixture.serve.as_mut().unwrap(), &after_log).await;

    let snapshot_url = format!("{base}/api/runs/{run_id}");
    let snapshot = tokio::time::timeout(TEST_LIVENESS_GUARD, async {
        loop {
            let response = client.get(&snapshot_url).send().await.unwrap();
            if response.status().is_success() {
                let value: Value = response.json().await.unwrap();
                if matches!(value["state"].as_str(), Some("failed" | "interrupted")) {
                    return value;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("restarted service should report a failed/interrupted run");
    assert!(
        matches!(snapshot["state"].as_str(), Some("failed" | "interrupted")),
        "{snapshot}"
    );

    // Neither path may claim the effect stopped: recovery journals this
    // run_error, and the refusals below must preserve that evidence.
    assert_unresolved_effect(&fetch_journal(client, base, &run_id).await);

    let replay = client
        .post(format!("{base}/api/runs/{run_id}/continue"))
        .header(policy::IDEMPOTENCY_HEADER, uuid::Uuid::now_v7().to_string())
        .send()
        .await
        .unwrap();
    let replay_status = replay.status();
    let replay_body = replay.text().await.unwrap();
    assert_eq!(
        replay_status,
        reqwest::StatusCode::CONFLICT,
        "{replay_body}"
    );
    assert!(replay_body.contains(CONTINUE_REFUSAL), "{replay_body}");
    assert_unresolved_effect(&fetch_journal(client, base, &run_id).await);

    let deleted = client.delete(&snapshot_url).send().await.unwrap();
    let deleted_status = deleted.status();
    let deleted_body = deleted.text().await.unwrap();
    assert_eq!(
        deleted_status,
        reqwest::StatusCode::CONFLICT,
        "{deleted_body}"
    );
    assert!(deleted_body.contains(DELETE_REFUSAL), "{deleted_body}");
    assert_unresolved_effect(&fetch_journal(client, base, &run_id).await);

    // Every refusal must preserve the run and its unresolved evidence.
    let after_refusals: Value = client
        .get(&snapshot_url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        matches!(
            after_refusals["state"].as_str(),
            Some("failed" | "interrupted")
        ),
        "{after_refusals}"
    );
    assert_unresolved_effect(&fetch_journal(client, base, &run_id).await);

    pids
}

#[tokio::test]
async fn killed_serve_restart_reports_unresolved_effect_and_refuses_replay_and_delete() {
    let mut fixture = Fixture::new(&DEFAULT_COMMAND);
    let (client, base, port) = client_and_base();
    recover_and_assert_refusals(&mut fixture, &client, &base, port).await;
    // The bounded command exits by itself; Drop only reaps the serve child.
}

#[tokio::test]
async fn delegated_serve_restart_reclaims_command_and_descendant() {
    if !engine::host_cgroup::host_cgroup_tests_enabled() {
        return;
    }
    let mut fixture = Fixture::new(&DELEGATED_COMMAND);
    let (client, base, port) = client_and_base();
    let pids = recover_and_assert_refusals(&mut fixture, &client, &base, port).await;
    tokio::time::timeout(TEST_LIVENESS_GUARD, async {
        loop {
            if pids.iter().all(|&pid| !process_exists(pid)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("delegated cgroup reclaim should stop command and descendant");
}
