#![cfg(unix)]
use super::support::*;
use crate::*;
use api::{RunStatus, StartRun};
use camino::Utf8Path;
use std::time::{Duration, Instant};

/// The elapsed budget the timing tests declare.
///
/// It must exceed the run's own queue and startup: a loaded runner spent
/// 1.04 s there, so a one-second budget expired before the run reached the
/// state under test.
const ELAPSED_BUDGET_SECS: u64 = 3;

/// The dwell that lets the wall-clock budget expire before the resume.
///
/// It must exceed the budget, or the wall-clock case would not prove that
/// the expired budget is what stops the run.
const ELAPSED_BUDGET_DWELL: Duration = Duration::from_millis(3500);

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
    for elapsed in [
        ElapsedBudgetMode::WallClock,
        ElapsedBudgetMode::ActiveExecution,
    ] {
        for mode in [PreemptionMode::StepBoundary, PreemptionMode::Interrupt] {
            let root = temp_run_dir(&format!("scheduler-comparison-{mode:?}"));
            generator(&root, "slow", "0.3");
            generator(&root, "urgent", "0.1");
            let mut deployment = if mode == PreemptionMode::Interrupt {
                ServiceDeploymentPolicy {
                    preemption_mode: mode,
                    ..Default::default()
                }
            } else {
                // Exercise the deployment default, not an explicit boundary override.
                ServiceDeploymentPolicy::default()
            };
            deployment.elapsed_budget_mode = elapsed;
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
                    read_journal_string(&service, victim.clone())
                        .await
                        .contains("indeterminate result after interruption")
                );
            }
            let values =
                crate::summaries::read_journal_events(&root.join("runs").join(&victim)).unwrap();
            let state = engine::RunState::fold_values(&values).unwrap();
            assert_eq!(state.budget.elapsed_budget_mode, elapsed);
            for event in values.iter().filter(|e| e["t"] == "run_queued") {
                assert_eq!(
                    event["elapsed_budget_mode"],
                    serde_json::to_value(elapsed).unwrap()
                );
            }
            if elapsed == ElapsedBudgetMode::ActiveExecution {
                let requeue = values
                    .iter()
                    .enumerate()
                    .filter(|(_, event)| event["t"] == "run_queued")
                    .nth(1)
                    .map(|(index, _)| index)
                    .expect("preemption must append a second queued episode");
                let close = values[..requeue]
                    .iter()
                    .rposition(|event| {
                        event["t"] == "elapsed_accounted" && event["active_started_at"].is_null()
                    })
                    .expect("active handoff must close its interval before requeue");
                assert!(close < requeue);
                let ledger = state.budget.elapsed.unwrap();
                assert!(ledger.active_started_at.is_none());
                assert!(ledger.active_nanoseconds > 0);
                let first = chrono::DateTime::parse_from_rfc3339(
                    values.first().unwrap()["ts"].as_str().unwrap(),
                )
                .unwrap();
                let last = chrono::DateTime::parse_from_rfc3339(
                    values.last().unwrap()["ts"].as_str().unwrap(),
                )
                .unwrap();
                let lifetime = (last - first).to_std().unwrap();
                assert!(
                    lifetime
                        >= Duration::from_nanos(ledger.active_nanoseconds)
                            + Duration::from_millis(50),
                    "urgent execution's preempted wait must not be charged"
                );

                // Ordinary cancel closes survive interrupt's canceled-suffix truncation.
                let closes: Vec<_> = values
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| {
                        e["t"] == "elapsed_accounted" && e["active_started_at"].is_null()
                    })
                    .collect();
                assert!(
                    closes.len() >= 2,
                    "identity initializes the ledger; two execution episodes close"
                );
            }
            service.shutdown_active_runs().await.unwrap();
        }
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
    // Advance paused Tokio time instead of wall-sleeping: the live gated
    // command stays blocked on `release` while virtual time passes the
    // interrupt stop deadline.
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(5100)).await;
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
    tokio::time::resume();
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

#[tokio::test]
async fn elapsed_modes_initial_queue_and_exhausted_admission_start_no_effects() {
    for mode in [
        ElapsedBudgetMode::WallClock,
        ElapsedBudgetMode::ActiveExecution,
    ] {
        let root = temp_run_dir("elapsed-initial-queue");
        let _temp = TempGuard(root.clone());
        generator(&root, "blocker", "30");
        generator(&root, "victim", "0.01");
        let path = root.join("victim").join(contract::MANIFEST_FILE);
        let text = std::fs::read_to_string(&path).unwrap();
        // One named pair for both modes, so the difference between the
        // branches is attributable to the elapsed-budget mode alone. The
        // wall-clock branch needs the budget to expire while the victim is
        // still queued, and the shared pair satisfies that: the budget is
        // shorter than the dwell, and the blocker holds the execution slot
        // for 30 s.
        std::fs::write(
            path,
            format!("{text}\n[budget]\nmax_elapsed_seconds = {ELAPSED_BUDGET_SECS}\n"),
        )
        .unwrap();
        let service = service(
            &root,
            ServiceDeploymentPolicy {
                elapsed_budget_mode: mode,
                ..Default::default()
            },
        );
        let blocker = start(&service, "blocker", 5).await;
        operation_started(&service, &blocker).await;
        let victim = start(&service, "victim", 0).await;
        wait_for_snapshot(&service, &victim, RunStatus::Queued).await;
        let queued_events =
            crate::summaries::read_journal_events(&root.join("runs").join(&victim)).unwrap();
        assert_eq!(
            queued_events.len(),
            1,
            "admission is a single atomic identity append"
        );
        assert_eq!(queued_events[0]["t"], "run_queued");
        let queued = engine::RunState::fold_values(&queued_events).unwrap();
        if mode == ElapsedBudgetMode::ActiveExecution {
            let ledger = queued.budget.elapsed.unwrap();
            assert_eq!(ledger.active_nanoseconds, 0);
            assert!(ledger.active_started_at.is_none());
        } else {
            assert!(queued.budget.elapsed.is_none());
        }
        tokio::time::sleep(ELAPSED_BUDGET_DWELL).await;
        service.cancel(blocker).await.unwrap();
        let terminal = wait_for_terminal_snapshot(&service, &victim).await;
        let workspace = run_workspace_dir(&root.join("runs").join(&victim));
        if mode == ElapsedBudgetMode::WallClock {
            assert_eq!(terminal.state, RunStatus::Failed);
            assert!(!workspace.join("started").exists());
            assert!(
                !read_journal_string(&service, victim)
                    .await
                    .contains("operation_started")
            );
        } else {
            assert_eq!(terminal.state, RunStatus::Succeeded);
            let state = engine::RunState::fold_values(
                &crate::summaries::read_journal_events(&root.join("runs").join(&victim)).unwrap(),
            )
            .unwrap();
            // The victim's charged time stays inside its own budget. The
            // engine-level test in crates/engine/src/engine/elapsed.rs
            // asserts the stronger property that the queue is not charged
            // at all; this checks the charge against the budget.
            assert!(
                state.budget.elapsed.unwrap().active_nanoseconds
                    < ELAPSED_BUDGET_SECS * 1_000_000_000
            );
        }
        service.shutdown_active_runs().await.unwrap();
    }
}

#[tokio::test]
async fn elapsed_hitl_question_confirmation_restart_and_deployment_change() {
    for mode in [
        ElapsedBudgetMode::WallClock,
        ElapsedBudgetMode::ActiveExecution,
    ] {
        let root = temp_run_dir("elapsed-hitl");
        let _temp = TempGuard(root.clone());
        let dir = root.join("hitl");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(contract::MANIFEST_FILE), r#"
[generator]
id = "hitl"
name = "Elapsed HITL"
version = "0.1.0"
[[flow]]
id = "ask"
type = "ask_user"
[flow.params]
content = "Continue?"
options = ["yes"]
[[flow]]
id = "effect"
type = "command"
needs = ["ask"]
[flow.params]
command = ["sh", "-c", "echo effect > effect"]
[permissions]
fs_read = []
fs_write = ["workspace"]
network = []
side_effects = "confirm"
side_effects_scope = "invocation"
commands = [{ bin = "sh", args = ["-c", "echo effect > effect"], purpose = "probe", isolation = "trusted_host" }]
[permissions.containers]
enabled = false
"#)
        .unwrap();
        let manifest = dir.join(contract::MANIFEST_FILE);
        let text = std::fs::read_to_string(&manifest).unwrap();
        std::fs::write(
            manifest,
            format!("{text}\n[budget]\nmax_elapsed_seconds = {ELAPSED_BUDGET_SECS}\n"),
        )
        .unwrap();
        let first = service(
            &root,
            ServiceDeploymentPolicy {
                elapsed_budget_mode: mode,
                ..Default::default()
            },
        );
        let id = start(&first, "hitl", 0).await;
        let waiting = wait_for_snapshot(&first, &id, RunStatus::Waiting).await;
        let before = engine::RunState::fold_values(
            &crate::summaries::read_journal_events(&root.join("runs").join(&id)).unwrap(),
        )
        .unwrap();
        if mode == ElapsedBudgetMode::ActiveExecution {
            assert!(
                before
                    .budget
                    .elapsed
                    .as_ref()
                    .unwrap()
                    .active_started_at
                    .is_none()
            );
        }
        first.mark_shutting_down();
        drop(first);
        tokio::time::sleep(ELAPSED_BUDGET_DWELL).await;
        // Restart with the opposite configuration: journal truth must win.
        let service = service(
            &root,
            ServiceDeploymentPolicy {
                elapsed_budget_mode: if mode == ElapsedBudgetMode::WallClock {
                    ElapsedBudgetMode::ActiveExecution
                } else {
                    ElapsedBudgetMode::WallClock
                },
                ..Default::default()
            },
        );
        if mode == ElapsedBudgetMode::ActiveExecution {
            let values =
                crate::summaries::read_journal_events(&root.join("runs").join(&id)).unwrap();
            let fork = service
                .fork_run(
                    &id,
                    api::ForkRun {
                        at_seq: values.last().unwrap()["seq"].as_u64().unwrap(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            wait_for_snapshot(&service, &fork, RunStatus::Waiting).await;
            let fork_values =
                crate::summaries::read_journal_events(&root.join("runs").join(&fork)).unwrap();
            let inherited = engine::RunState::fold_values(&fork_values).unwrap();
            assert_eq!(inherited.budget.elapsed_budget_mode, mode);
            assert!(
                inherited.budget.elapsed.unwrap().active_nanoseconds
                    >= before.budget.elapsed.as_ref().unwrap().active_nanoseconds
            );
            assert_eq!(
                fork_values
                    .iter()
                    .rfind(|e| e["t"] == "run_queued")
                    .unwrap()["elapsed_budget_mode"],
                "active_execution"
            );
            service.cancel(fork).await.unwrap();
        }
        service
            .answer(
                id.clone(),
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
        if mode == ElapsedBudgetMode::WallClock {
            assert_eq!(
                wait_for_terminal_snapshot(&service, &id).await.state,
                RunStatus::Failed
            );
        } else {
            let confirm = wait_for_snapshot(&service, &id, RunStatus::Confirming)
                .await
                .confirm
                .unwrap();
            let values =
                crate::summaries::read_journal_events(&root.join("runs").join(&id)).unwrap();
            let state = engine::RunState::fold_values(&values).unwrap();
            assert_eq!(state.budget.elapsed_budget_mode, mode);
            assert!(
                state
                    .budget
                    .elapsed
                    .as_ref()
                    .unwrap()
                    .active_started_at
                    .is_none()
            );
            assert!(
                state.budget.elapsed.as_ref().unwrap().active_nanoseconds
                    >= before.budget.elapsed.unwrap().active_nanoseconds
            );
            tokio::time::sleep(ELAPSED_BUDGET_DWELL).await;
            service
                .confirm(
                    id.clone(),
                    confirm.id,
                    api::ConfirmDecision {
                        decision: api::ConfirmationDecision::Approve,
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                wait_for_terminal_snapshot(&service, &id).await.state,
                RunStatus::Succeeded
            );
            assert!(
                run_workspace_dir(&root.join("runs").join(&id))
                    .join("effect")
                    .exists()
            );
        }
        service.shutdown_active_runs().await.unwrap();
    }
}

#[tokio::test]
async fn active_open_prompt_crash_recovers_before_publishing_waiting() {
    let root = temp_run_dir("elapsed-prompt-crash");
    let _temp = TempGuard(root.clone());
    let runs = root.join("runs");
    generator(&root, "victim", "0.01");
    let manifest = root.join("victim").join(contract::MANIFEST_FILE);
    let text = std::fs::read_to_string(&manifest).unwrap();
    std::fs::write(
        manifest,
        format!(
            r#"{text}
[budget]
max_elapsed_seconds = 10
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
    let first = service(
        &root,
        ServiceDeploymentPolicy {
            elapsed_budget_mode: ElapsedBudgetMode::ActiveExecution,
            ..Default::default()
        },
    );
    let id = start(&first, "victim", 0).await;
    wait_for_snapshot(&first, &id, RunStatus::Waiting).await;
    first.mark_shutting_down();
    drop(first);
    wait_for_runs_store_release(&runs).await;
    let journal_path = run_meta_dir(&runs.join(&id)).join("journal.jsonl");
    let writer = engine::JournalWriter::create(&journal_path, &id, false, None).unwrap();
    let total = writer
        .state()
        .budget
        .elapsed
        .as_ref()
        .unwrap()
        .active_nanoseconds;
    // Simulate an episode that emitted its prompt and crashed before closing.
    writer.event("elapsed_accounted", serde_json::json!({
        "active_nanoseconds":total,
        "active_started_at": (chrono::Utc::now() - chrono::Duration::seconds(2)).to_rfc3339()
    })).unwrap();
    assert!(writer.state().settled_pending().is_none());
    drop(writer);
    let restored = service(&root, ServiceDeploymentPolicy::default());
    assert_eq!(
        restored.inner.runs.read().await[&id].state,
        RunStatus::Queued
    );
    restored.resume_recovered_runs().await;
    wait_for_snapshot(&restored, &id, RunStatus::Waiting).await;
    let recovered = engine::RunState::fold_journal(&journal_path).unwrap();
    assert!(
        recovered
            .budget
            .elapsed
            .as_ref()
            .unwrap()
            .active_nanoseconds
            >= total + 2_000_000_000
    );
    assert!(
        recovered
            .budget
            .elapsed
            .as_ref()
            .unwrap()
            .active_started_at
            .is_none()
    );
    let values = crate::summaries::read_journal_events(&root.join("runs").join(&id)).unwrap();
    assert_eq!(
        values
            .iter()
            .filter(|e| e["t"] == "operation_started")
            .count(),
        2,
        "completed effects must only replay"
    );
    restored.mark_shutting_down();
    drop(restored);
    wait_for_runs_store_release(&runs).await;
    let again = service(&root, ServiceDeploymentPolicy::default());
    let seq = recovered.last_seq;
    again.resume_recovered_runs().await;
    assert_eq!(
        engine::RunState::fold_journal(&journal_path)
            .unwrap()
            .last_seq,
        seq,
        "closed paused run is not recovered/charged again"
    );
    again.shutdown_active_runs().await.unwrap();
}

#[tokio::test]
async fn wall_clock_confirmation_dwell_counts_before_effects() {
    let root = temp_run_dir("wall-confirm-dwell");
    let _temp = TempGuard(root.clone());
    generator(&root, "victim", "0.01");
    let manifest = root.join("victim").join(contract::MANIFEST_FILE);
    let text = std::fs::read_to_string(&manifest)
        .unwrap()
        .replace("side_effects = \"allowed\"", "side_effects = \"confirm\"");
    std::fs::write(
        manifest,
        format!("{text}\n[budget]\nmax_elapsed_seconds = {ELAPSED_BUDGET_SECS}\n"),
    )
    .unwrap();
    let service = service(&root, ServiceDeploymentPolicy::default());
    let id = start(&service, "victim", 0).await;
    let confirm = wait_for_snapshot(&service, &id, RunStatus::Confirming)
        .await
        .confirm
        .unwrap();
    tokio::time::sleep(ELAPSED_BUDGET_DWELL).await;
    service
        .confirm(
            id.clone(),
            confirm.id,
            api::ConfirmDecision {
                decision: api::ConfirmationDecision::Approve,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        wait_for_terminal_snapshot(&service, &id).await.state,
        RunStatus::Failed
    );
    assert!(
        !read_journal_string(&service, id.clone())
            .await
            .contains("operation_started")
    );
    assert!(
        !run_workspace_dir(&root.join("runs").join(&id))
            .join("started")
            .exists()
    );
    service.shutdown_active_runs().await.unwrap();
}
