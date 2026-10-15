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

#[tokio::test]
async fn scheduler_preserves_fifo_key_by_default() {
    preemption_fifo(ServiceDeploymentPolicy::default()).await;
}

#[tokio::test]
async fn scheduler_tail_uses_actual_requeue_time() {
    preemption_fifo(ServiceDeploymentPolicy {
        preemption_queue_position: PreemptionQueuePosition::Tail,
        ..Default::default()
    })
    .await;
}

async fn preemption_fifo(deployment: ServiceDeploymentPolicy) {
    let preserve = deployment.preemption_queue_position == PreemptionQueuePosition::Preserve;
    let root = temp_run_dir(&format!("scheduler-fifo-{preserve}"));
    let _temp = TempGuard(root.clone());
    generator(&root, "victim", "0.3");
    generator(&root, "blocker", "60");
    let manifest = root.join("victim").join(contract::MANIFEST_FILE);
    let contents = std::fs::read_to_string(&manifest)
        .unwrap()
        .replace("sleep 0.3", "while [ ! -f release_first ]; do sleep 0.01; done")
        .replace(
            "echo after >> effects",
            "echo second > second; while [ ! -f release_second ]; do sleep 0.01; done; echo after >> effects",
        );
    std::fs::write(
        manifest,
        format!(
            r#"{contents}
[[flow]]
id = "ask"
type = "ask_user"
needs = ["after"]
[flow.params]
content = "Continue?"
options = ["yes"]
"#
        ),
    )
    .unwrap();
    let service = service(&root, deployment);
    let victim = start(&service, "victim", 0).await;
    operation_started(&service, &victim).await;
    let initial_key = service.inner.runs.read().await[&victim].queued_at.unwrap();
    let mut previous_key = initial_key;
    for round in 0..2 {
        if round == 1 {
            tokio::time::timeout(Duration::from_secs(3), async {
                while !run_workspace_dir(&root.join("runs").join(&victim))
                    .join("second")
                    .exists()
                {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .unwrap();
        }
        let equal = start(&service, "blocker", 0).await;
        wait_for_snapshot(&service, &equal, RunStatus::Queued).await;
        let arrival_service = service.clone();
        let arrival = tokio::spawn(async move { start(&arrival_service, "blocker", 5).await });
        // Explicit rendezvous avoids racing fixed sleeps under a busy test
        // runner: release this command only once the scheduler requests yield.
        tokio::time::timeout(Duration::from_secs(3), async {
            while !service.inner.runs.read().await[&victim]
                .scheduler_yield
                .is_cancelled()
            {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        std::fs::write(
            run_workspace_dir(&root.join("runs").join(&victim)).join(if round == 0 {
                "release_first"
            } else {
                "release_second"
            }),
            "",
        )
        .unwrap();
        let urgent = arrival.await.unwrap();
        wait_for_snapshot(&service, &urgent, RunStatus::Running).await;
        let snapshot = wait_for_snapshot(&service, &victim, RunStatus::Queued).await;
        assert_eq!(snapshot.queue_position, Some(if preserve { 1 } else { 2 }));
        assert_eq!(
            service
                .snapshot(equal.clone())
                .await
                .unwrap()
                .queue_position,
            Some(if preserve { 2 } else { 1 })
        );
        let dir = root.join("runs").join(&victim);
        let values = crate::summaries::read_journal_events(&dir).unwrap();
        let queued: Vec<_> = values.iter().filter(|e| e["t"] == "run_queued").collect();
        assert_eq!(queued.len(), round + 2);
        let requeue = queued.last().unwrap();
        let event_at = chrono::DateTime::parse_from_rfc3339(requeue["ts"].as_str().unwrap())
            .unwrap()
            .with_timezone(&chrono::Utc);
        let key = service.inner.runs.read().await[&victim].queued_at.unwrap();
        if preserve {
            assert_eq!(key, initial_key);
            assert_eq!(requeue["queued_at"], initial_key.to_rfc3339());
            assert!(event_at > key, "event ts must not be backdated");
        } else {
            assert!(
                requeue.get("queued_at").is_none(),
                "tail omits the override"
            );
            assert_eq!(key, event_at);
            assert!(key > previous_key);
        }
        previous_key = key;
        let folded =
            engine::RunState::fold_journal(&run_meta_dir(&dir).join("journal.jsonl")).unwrap();
        let folded_at = chrono::DateTime::parse_from_rfc3339(folded.queued_at.as_deref().unwrap())
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(folded_at, key);
        assert_eq!(snapshot.queued_at, folded.queued_at);
        let recovered = crate::summaries::rehydrate_runs(
            &root.join("runs"),
            policy::DEFAULT_MAX_TRACKED_RUNS,
            policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
        )
        .unwrap();
        assert_eq!(recovered[&victim].queued_at, Some(key));
        // Use the same queue-head selector as execution after rehydration;
        // exclude the active urgent run, which recovery correctly queues too.
        let recovered_equals = recovered
            .into_iter()
            .filter(|(id, _)| id == &victim || id == &equal)
            .collect();
        let head = if preserve { &victim } else { &equal };
        assert_eq!(
            crate::queue::queue_head(&recovered_equals).as_ref(),
            Some(head)
        );

        service.cancel(urgent).await.unwrap();
        // Execution agrees with snapshot FIFO order, not just its display.
        if preserve {
            if round == 0 {
                wait_for_snapshot(&service, &victim, RunStatus::Running).await;
            }
        } else {
            wait_for_snapshot(&service, &equal, RunStatus::Running).await;
        }
        service.cancel(equal).await.unwrap();
    }
    let waiting = wait_for_snapshot(&service, &victim, RunStatus::Waiting).await;
    let blocker = start(&service, "blocker", 5).await;
    wait_for_snapshot(&service, &blocker, RunStatus::Running).await;
    let values = crate::summaries::read_journal_events(&root.join("runs").join(&victim)).unwrap();
    let checkpoint = values.iter().find(|e| e["t"] == "step_finished").unwrap();
    let fork = service
        .fork_run(
            &victim,
            api::ForkRun {
                at_seq: checkpoint["seq"].as_u64().unwrap(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let fork_snapshot = wait_for_snapshot(&service, &fork, RunStatus::Queued).await;
    let fork_at =
        chrono::DateTime::parse_from_rfc3339(fork_snapshot.queued_at.as_deref().unwrap()).unwrap();
    assert!(
        fork_at > previous_key,
        "fork admission must not inherit the source key"
    );
    assert_eq!(fork_snapshot.queue_position, Some(1));
    service.cancel(fork).await.unwrap();
    service
        .answer(
            victim.clone(),
            waiting.question.unwrap().id,
            api::AnswerPayload {
                values: std::collections::BTreeMap::from([(
                    "answer".into(),
                    serde_json::json!("yes"),
                )]),
            },
        )
        .await
        .unwrap();
    let answered = wait_for_snapshot(&service, &victim, RunStatus::Queued).await;
    let answer_at =
        chrono::DateTime::parse_from_rfc3339(answered.queued_at.as_deref().unwrap()).unwrap();
    assert!(
        answer_at > previous_key,
        "HITL answer establishes a new FIFO key"
    );
    assert_eq!(
        service.inner.runs.read().await[&victim].queued_at.unwrap(),
        answer_at
    );
    service.cancel(victim).await.unwrap();
    service.cancel(blocker).await.unwrap();
    service.shutdown_active_runs().await.unwrap();
}
