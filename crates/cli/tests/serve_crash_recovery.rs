//! Real `qcg serve` SIGKILL/restart coverage for host effects. The default
//! path uses a bounded, self-terminating command and proves the fail-closed
//! refusals; the delegated path uses a long-lived command and proves cgroup
//! reclaim. The fixture only owns its `serve` children: it never signals
//! saved numeric PIDs or process groups.
//!
//! Code inspection for host MCP stdio: `crates/mcp/src/bounded_stdio.rs::spawn`
//! forwards to `spawn_with_scope(..., None)`, and `crates/mcp/src/mcp/runtime.rs`
//! keeps that unscoped path when `host_stdio_scope` has no delegated cgroup.
//! A stdio step journals a distinct `mcp.stdio_launch` operation record before
//! `runtime.connect()` on the same `guard_external_operation` fail-closed
//! ledger as the command step below, and leaves it `Started` on an
//! indeterminate connect. The refusal and evidence assertions here cover that
//! launch identity on the default path too.
//!
//! Both paths assert against the journal's authoritative `operation_started`
//! record, not only the diagnostic text. The delegated path checks scope
//! reclaim at the first failed/interrupted snapshot, before any endpoint that
//! could reconcile the cgroup itself is called.
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
// Fixed, redacted HTTP refusal categories. Continue's detail is redacted in
// the response; delete's Started-gate detail is carried verbatim, so the
// assertion below names the real unresolved-operation gate, not an
// idempotency pin that could short-circuit it.
const CONTINUE_REFUSAL: &str = "source run is executing or has in-flight work";
const DELETE_REFUSAL: &str = "unresolved external operation";
// Transient, fail-closed lease refusal. Recovery/replay can briefly hold the
// source execution lease after restart; this precedes, and is distinct from,
// the unresolved Started gate the DELETE assertions below must reach.
const EXECUTING_ELSEWHERE: &str = "is executing elsewhere";
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
    delegate: bool,
}

impl Fixture {
    /// Only the delegated path may inherit the test runner's cgroup parent.
    fn new(command: &[&str], delegate: bool) -> Self {
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
            delegate,
        }
    }

    fn start_serve(&mut self, port: u16, log_name: &str) {
        assert!(self.serve.is_none(), "previous serve child is still owned");
        let log = File::create(self.root.join(log_name)).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_qcg"));
        command
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
            .stderr(Stdio::from(log));
        // The delegated script exports the parent for the whole test process;
        // the default fixture must not silently inherit that configuration.
        if !self.delegate {
            command.env_remove(policy::HOST_CGROUP_PARENT_ENV);
        }
        let child = command.spawn().unwrap();
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

/// Signal zero still succeeds on an unreaped zombie, which cannot execute.
/// Treat a zombie as stopped: the retired scope is the primary containment
/// evidence, and this probe only rules out a live pre-kill descendant.
fn process_can_run(pid: i32) -> bool {
    if !process_exists(pid) {
        return false;
    }
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        // The process state is the third whitespace-separated stat field.
        Ok(stat) => stat.split_whitespace().nth(2) != Some("Z"),
        // No procfs view of this pid: keep the conservative existence signal.
        Err(_) => true,
    }
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

/// The diagnostic alone can persist after a refusal path finished or
/// discarded the launch evidence. Check the operation id against the
/// authoritative `operation_started`/`operation_finished` journal events too.
/// `expected` is the operation id captured from the live journal before the
/// crash: the recovery diagnostic must name that same original operation, so
/// a rewrite to a different Started operation cannot silently pass.
fn assert_unresolved_effect(journal: &str, expected: &str) {
    let events: Vec<Value> = journal
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect();
    let refusal = events
        .iter()
        .find(|event| {
            event["t"] == "run_error"
                && event["error"]
                    .as_str()
                    .is_some_and(|error| error.contains(UNRESOLVED_EFFECT))
        })
        .unwrap_or_else(|| panic!("run_error must name the unresolved effect: {journal}"));
    let detail = refusal["error"].as_str().unwrap();
    // The single operator-facing diagnostic assertion.
    assert!(detail.contains("remains Started"), "{refusal}");
    let operation_id = detail
        .split_once("unresolved external operation `")
        .and_then(|(_, rest)| rest.split_once("` remains Started"))
        .map(|(id, _)| id)
        .unwrap_or_else(|| panic!("unresolved diagnostic must name its operation: {refusal}"));
    assert_eq!(
        operation_id, expected,
        "refusal must name the original pre-kill operation `{expected}`, not `{operation_id}`: {refusal}"
    );
    let has_record = |kind: &str| {
        events
            .iter()
            .any(|event| event["t"] == kind && event["operation_id"].as_str() == Some(operation_id))
    };
    assert!(
        has_record("operation_started"),
        "operation `{operation_id}` must still have a Started journal record: {journal}"
    );
    assert!(
        !has_record("operation_finished"),
        "operation `{operation_id}` must have no Finished journal record: {journal}"
    );
}

/// The single external operation durably Started (with no matching Finished)
/// while the command and its descendant are still live, before the crash.
/// This is the original launch identity that recovery must name and preserve;
/// it is captured before SIGKILL so a recovery-side rewrite to a different
/// Started operation cannot substitute the id the assertions pin to.
fn live_operation_id(journal: &str) -> String {
    let events: Vec<Value> = journal
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect();
    let finished: Vec<&str> = events
        .iter()
        .filter(|event| event["t"] == "operation_finished")
        .filter_map(|event| event["operation_id"].as_str())
        .collect();
    let mut live: Vec<&str> = events
        .iter()
        .filter(|event| event["t"] == "operation_started")
        .filter_map(|event| event["operation_id"].as_str())
        .filter(|id| !finished.contains(id))
        .collect();
    live.sort_unstable();
    live.dedup();
    assert_eq!(
        live.len(),
        1,
        "exactly one external operation must be live before the kill: {journal}"
    );
    live[0].to_owned()
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

/// The first observable recovery result: run id, the command/descendant PIDs
/// probed before the kill, and the original external operation id captured
/// from the live journal before SIGKILL. No reconciling endpoint has been
/// called yet.
struct RecoveredRun {
    run_id: String,
    pids: Vec<i32>,
    operation_id: String,
}

/// Crash, restart and wait for the first failed/interrupted snapshot shared by
/// both containment paths. The delegated test inspects this point before
/// `assert_refusals` calls any endpoint that can reconcile.
async fn crash_and_recover(
    fixture: &mut Fixture,
    client: &reqwest::Client,
    base: &str,
    port: u16,
) -> RecoveredRun {
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

    // Capture the original external operation while the command is still
    // live, before any crash/recovery can rewrite the record or diagnostic.
    let operation_id = live_operation_id(&fetch_journal(client, base, &run_id).await);

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

    RecoveredRun {
        run_id,
        pids,
        operation_id,
    }
}

/// Fail-closed checks shared by both containment paths. All endpoints here
/// run after the first recovery observation (and, for the delegated path,
/// after its scope-reclaim assertion).
async fn assert_refusals(client: &reqwest::Client, base: &str, recovered: &RecoveredRun) {
    let run_id = &recovered.run_id;
    let snapshot_url = format!("{base}/api/runs/{run_id}");
    let operation_id = &recovered.operation_id;
    // Neither path may claim the effect stopped: recovery journals this
    // run_error, and the refusals below must preserve that evidence. Match it
    // to the operation id captured live before the crash, never one derived
    // only from the restarted service's diagnostic.
    assert_unresolved_effect(&fetch_journal(client, base, run_id).await, operation_id);

    let replay = client
        .post(format!("{base}/api/runs/{run_id}/continue"))
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
    assert_unresolved_effect(&fetch_journal(client, base, run_id).await, operation_id);

    // The replay above and recovered-run reconciliation can briefly hold the
    // source execution lease after the restart. While that lease is held,
    // DELETE is refused at the earlier "executing elsewhere" gate rather than
    // the unresolved Started gate this test exists to prove. Wait out only
    // that transient, fail-closed lease conflict; every assertion below is
    // unchanged, so the real Started-gate refusal is still required.
    let (deleted_status, deleted_body) = tokio::time::timeout(TEST_LIVENESS_GUARD, async {
        loop {
            let deleted = client.delete(&snapshot_url).send().await.unwrap();
            let status = deleted.status();
            let body = deleted.text().await.unwrap();
            if !body.contains(EXECUTING_ELSEWHERE) {
                return (status, body);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("DELETE should reach the unresolved Started gate within the liveness guard");
    assert_eq!(
        deleted_status,
        reqwest::StatusCode::CONFLICT,
        "{deleted_body}"
    );
    assert!(deleted_body.contains(DELETE_REFUSAL), "{deleted_body}");
    assert!(
        deleted_body.contains(UNRESOLVED_EFFECT),
        "delete must reach the unresolved Started gate: {deleted_body}"
    );
    assert_unresolved_effect(&fetch_journal(client, base, run_id).await, operation_id);

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
    assert_unresolved_effect(&fetch_journal(client, base, run_id).await, operation_id);
}

/// Read-only proof at the first recovery observation: the persisted run scope
/// is retired and the original descendant cannot run. No saved PID/group is
/// signalled, and no `continue`/DELETE has had a chance to reconcile here.
fn assert_scope_reclaimed(runs_dir: &Utf8Path, recovered: &RecoveredRun) {
    let meta = runs_dir.join(&recovered.run_id).join("meta");
    // Single observation at the first recovered snapshot: reconciliation is
    // synchronous and precedes publication of the failed/interrupted outcome,
    // so a live binding here is a real regression, never a timing artefact.
    let retired = match engine::host_cgroup::load_host_binding(&meta)
        .expect("host binding must be readable after restart")
    {
        Some(binding) if binding.cleaned && binding.empty_confirmed => {
            let events = binding.run_root.join("cgroup.events");
            match std::fs::read_to_string(&events) {
                Ok(contents) => contents.lines().any(|line| line == "populated 0"),
                // Retirement removes the run root once the kernel confirms it empty.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                Err(error) => panic!("cannot read {}: {error}", events.display()),
            }
        }
        // No binding or an unretired binding makes this containment claim
        // vacuous; it must never accompany a published recovery outcome.
        other => {
            panic!("first recovery observation must find a retired bound scope, got {other:?}")
        }
    };
    assert!(
        retired,
        "bound scope must be confirmed empty at first recovery observation: no async reconciliation may follow the published outcome"
    );
    for &pid in &recovered.pids {
        assert!(
            !process_can_run(pid),
            "original descendant {pid} must be unable to run at first recovery observation"
        );
    }
}

#[tokio::test]
async fn killed_serve_restart_reports_unresolved_effect_and_refuses_replay_and_delete() {
    let mut fixture = Fixture::new(&DEFAULT_COMMAND, false);
    let (client, base, port) = client_and_base();
    let recovered = crash_and_recover(&mut fixture, &client, &base, port).await;
    assert_refusals(&client, &base, &recovered).await;
    // The bounded command exits by itself; Drop only reaps the serve child.
}

#[tokio::test]
async fn delegated_serve_restart_reclaims_command_and_descendant() {
    if !engine::host_cgroup::host_cgroup_tests_enabled() {
        return;
    }
    let mut fixture = Fixture::new(&DELEGATED_COMMAND, true);
    let (client, base, port) = client_and_base();
    let recovered = crash_and_recover(&mut fixture, &client, &base, port).await;
    assert_scope_reclaimed(&fixture.runs_dir, &recovered);
    assert_refusals(&client, &base, &recovered).await;
}
