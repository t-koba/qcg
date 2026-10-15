#![cfg(unix)]
use super::support::*;
use crate::*;
use api::{RunStatus, StartRun};
use camino::Utf8Path;
use std::time::{Duration, Instant};

fn generator(root: &Utf8Path, id: &str, delay: &str) {
    let dir = root.join(id);
    std::fs::create_dir_all(&dir).unwrap();
    let script = format!("echo started > started; sleep {delay}; echo first >> effects");
    std::fs::write(dir.join(contract::MANIFEST_FILE), format!(r#"
[generator]
id = "{id}"
name = "Scheduler probe"
version = "0.1.0"
[[flow]]
id = "first"
type = "command"
[flow.params]
command = ["sh", "-c", "{script}"]
[[flow]]
id = "after"
type = "command"
needs = ["first"]
[flow.params]
command = ["sh", "-c", "echo after >> effects"]
[permissions]
fs_read = []
fs_write = ["workspace"]
network = []
side_effects = "allowed"
side_effects_scope = "invocation"
commands = [{{ bin = "sh", args = ["-c", "{script}"], purpose = "bounded probe", isolation = "trusted_host" }}, {{ bin = "sh", args = ["-c", "echo after >> effects"], purpose = "next boundary", isolation = "trusted_host" }}]
[permissions.containers]
enabled = false
[runtime]
command_timeout_seconds = 10
"#)).unwrap();
}

fn service(root: &Utf8Path, policy: ServiceDeploymentPolicy) -> LocalService {
    LocalService::with_generator_roots_policy_and_store_mode(
        vec![root.to_owned()],
        root.join("runs"),
        None,
        1,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        policy,
    )
    .unwrap()
}

async fn start(service: &LocalService, generator: &str, priority: i32) -> String {
    service
        .start_run(StartRun {
            generator_id: generator.into(),
            priority: Some(priority),
            ..Default::default()
        })
        .await
        .unwrap()
}

async fn operation_started(service: &LocalService, id: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if read_journal_string(service, id.to_owned())
                .await
                .contains("operation_started")
                && run_workspace_dir(&service.inner.runs_dir.join(id))
                    .join("started")
                    .exists()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bounded_scheduler_comparison_boundary_safety_and_interrupt_latency() {
    for mode in [PreemptionMode::StepBoundary, PreemptionMode::Interrupt] {
        let root = temp_run_dir(&format!("scheduler-comparison-{mode:?}"));
        generator(&root, "slow", "0.3");
        generator(&root, "urgent", "0.1");
        let deployment = if mode == PreemptionMode::Interrupt {
            ServiceDeploymentPolicy {
                preemption_mode: mode,
                ..Default::default()
            }
        } else {
            // Exercise the deployment default, not an explicit boundary override.
            ServiceDeploymentPolicy::default()
        };
        let service = service(&root, deployment);
        let victim = start(&service, "slow", 0).await;
        operation_started(&service, &victim).await;
        let clock = Instant::now();
        let urgent = start(&service, "urgent", 5).await;
        wait_for_snapshot(&service, &urgent, RunStatus::Running).await;
        let admission = clock.elapsed();
        let journal = read_journal_string(&service, victim.clone()).await;
        if mode == PreemptionMode::StepBoundary {
            assert!(journal.contains("run_yielded"));
            assert!(journal.contains("operation_finished"));
            assert!(!journal.contains("run_canceled"));
            assert!(!journal.contains("run_error"));
            let events: Vec<serde_json::Value> = journal
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let checkpoint = events
                .iter()
                .position(|e| e["t"] == "step_finished" && e["node"] == "first")
                .unwrap();
            let yielded = events.iter().position(|e| e["t"] == "run_yielded").unwrap();
            assert!(
                checkpoint < yielded,
                "checkpoint must precede durable handoff"
            );
            assert!(
                !journal.contains("\"node\":\"after\""),
                "no next node before handoff"
            );
        }
        assert_eq!(
            wait_for_terminal_snapshot(&service, &urgent).await.state,
            RunStatus::Succeeded
        );
        let victim_state = wait_for_terminal_snapshot(&service, &victim).await.state;
        eprintln!(
            "scheduler comparison {mode:?}: urgent admission {} ms; victim {victim_state:?}",
            admission.as_millis()
        );
        if mode == PreemptionMode::StepBoundary {
            assert_eq!(victim_state, RunStatus::Succeeded);
            let events: Vec<serde_json::Value> = read_journal_string(&service, victim.clone())
                .await
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e["t"] == "step_started" && e["node"] == "first")
                    .count(),
                1
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e["t"] == "operation_started")
                    .count(),
                2,
                "one command per node, no duplicate invocation"
            );
            assert_eq!(
                events.iter().filter(|e| e["t"] == "run_finished").count(),
                1
            );
            assert_eq!(
                std::fs::read_to_string(
                    run_workspace_dir(&root.join("runs").join(&victim)).join("effects")
                )
                .unwrap(),
                "first\nafter\n"
            );
        } else {
            assert_eq!(victim_state, RunStatus::Failed);
            assert!(
                read_journal_string(&service, victim)
                    .await
                    .contains("indeterminate result after interruption")
            );
        }
        service.shutdown_active_runs().await.unwrap();
    }
}

#[tokio::test]
async fn user_cancel_wins_while_boundary_preemption_drains() {
    let root = temp_run_dir("scheduler-boundary-cancel");
    generator(&root, "slow", "0.5");
    generator(&root, "urgent", "0.1");
    let service = service(&root, ServiceDeploymentPolicy::default());
    let victim = start(&service, "slow", 0).await;
    operation_started(&service, &victim).await;
    let clone = service.clone();
    let arriving = tokio::spawn(async move { start(&clone, "urgent", 5).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !service.inner.runs.read().await[&victim].preempted {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    service.cancel(victim.clone()).await.unwrap();
    let urgent = arriving.await.unwrap();
    assert_eq!(
        wait_for_terminal_snapshot(&service, &victim).await.state,
        RunStatus::Canceled
    );
    assert_eq!(
        wait_for_terminal_snapshot(&service, &urgent).await.state,
        RunStatus::Succeeded
    );
    let journal = read_journal_string(&service, victim).await;
    assert_eq!(
        journal.matches("\"t\":\"run_queued\"").count(),
        1,
        "cancel cannot be requeued"
    );
    service.shutdown_active_runs().await.unwrap();
}

#[tokio::test]
async fn boundary_drain_does_not_abort_after_five_seconds() {
    let root = temp_run_dir("scheduler-boundary-stop-timeout");
    generator(
        &root,
        "slow",
        "0; while [ ! -f release ]; do sleep 0.02; done",
    );
    generator(&root, "urgent", "0.1");
    let service = service(&root, ServiceDeploymentPolicy::default());
    let victim = start(&service, "slow", 0).await;
    operation_started(&service, &victim).await;
    let clone = service.clone();
    let arriving = tokio::spawn(async move { start(&clone, "urgent", 5).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !service.inner.runs.read().await[&victim].preempted {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    // The external command remains gated beyond the interrupt stop deadline.
    tokio::time::sleep(Duration::from_millis(5100)).await;
    assert!(
        !arriving.is_finished(),
        "boundary admission must still be draining"
    );
    assert_eq!(
        service.snapshot(victim.clone()).await.unwrap().state,
        RunStatus::Running
    );
    let journal = read_journal_string(&service, victim.clone()).await;
    assert!(!journal.contains("run_canceled"));
    assert!(!journal.contains("run_yielded"));
    std::fs::write(
        run_workspace_dir(&root.join("runs").join(&victim)).join("release"),
        "go",
    )
    .unwrap();
    let urgent = arriving.await.unwrap();
    assert_eq!(
        wait_for_terminal_snapshot(&service, &urgent).await.state,
        RunStatus::Succeeded
    );
    assert_eq!(
        wait_for_terminal_snapshot(&service, &victim).await.state,
        RunStatus::Succeeded
    );
    assert_eq!(
        std::fs::read_to_string(
            run_workspace_dir(&root.join("runs").join(&victim)).join("effects")
        )
        .unwrap(),
        "first\nafter\n"
    );
    service.shutdown_active_runs().await.unwrap();
}
