//! One-shot observation faults are scoped to a unique run path, not a process
//! flag: concurrent service tests cannot consume another test's failure.
use super::support::*;
use crate::*;
use api::{RunStatus, StartRun};
use camino::{Utf8Path, Utf8PathBuf};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

static READ_FAILURES: Mutex<BTreeMap<(Utf8PathBuf, &'static str), usize>> =
    Mutex::new(BTreeMap::new());

pub(crate) fn fail_observation(
    directory: &Utf8Path,
    point: &'static str,
) -> Result<(), ServiceError> {
    let mut failures = READ_FAILURES.lock().unwrap();
    let key = (directory.to_owned(), point);
    if let Some(skip) = failures.get_mut(&key) {
        if *skip == 0 {
            failures.remove(&key);
            return Err(ServiceError::Io(std::io::Error::other(format!(
                "one-shot {point} observation failure"
            ))));
        }
        *skip -= 1;
    }
    Ok(())
}

fn fail_once(directory: &Utf8Path, point: &'static str, skip: usize) {
    assert!(
        READ_FAILURES
            .lock()
            .unwrap()
            .insert((directory.to_owned(), point), skip)
            .is_none()
    );
}

fn assert_fault_consumed(directory: &Utf8Path, point: &'static str) {
    assert!(
        !READ_FAILURES
            .lock()
            .unwrap()
            .contains_key(&(directory.to_owned(), point))
    );
}

async fn join_run_task(service: &LocalService, id: &str) {
    let task = {
        let runs = service.inner.runs.read().await;
        runs[id].task.lock().unwrap().take()
    };
    if let Some(task) = task {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("service task must finish within the bounded test window")
            .expect("service task must not panic");
    }
}

async fn waiting_run() -> (TempGuard, LocalService, String, Utf8PathBuf) {
    suspended_run("ask-user", RunStatus::Waiting).await
}

async fn suspended_run(
    generator_id: &str,
    status: RunStatus,
) -> (TempGuard, LocalService, String, Utf8PathBuf) {
    let root = temp_run_dir("cancel-observation");
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: generator_id.into(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_snapshot(&service, &id, status).await;
    join_run_task(&service, &id).await;
    let directory = service.run_dir_for(&id).await.unwrap();
    (TempGuard(root), service, id, directory)
}

async fn assert_unsettled(service: &LocalService, id: &str, state: RunStatus) {
    let runs = service.inner.runs.read().await;
    let record = &runs[id];
    assert_eq!(record.state, state);
    assert!(
        record.question.is_some(),
        "observation must retain interaction"
    );
    assert!(!record.cancellation.is_cancelled());
    let folded = fold_run_state(&record.run_dir).expect("journal remains readable");
    assert!(
        folded.terminal.is_none(),
        "observation must not invent a terminal event"
    );
}

#[tokio::test]
async fn cancel_observation_startup_errors_retry_without_fabricating_cancel() {
    // Each queued check is exercised separately, as is the initial drain.
    for (point, skip) in [("remote", 0), ("remote", 1), ("drain", 0)] {
        let (_guard, service, id, directory) = waiting_run().await;
        service.inner.runs.write().await.get_mut(&id).unwrap().state = RunStatus::Queued;
        let journal = std::fs::read(run_meta_dir(&directory).join("journal.jsonl")).unwrap();
        fail_once(&directory, point, skip);

        service.resume_recovered_runs().await;
        join_run_task(&service, &id).await;
        assert_fault_consumed(&directory, point);
        assert_unsettled(&service, &id, RunStatus::Queued).await;
        assert_eq!(
            std::fs::read(run_meta_dir(&directory).join("journal.jsonl")).unwrap(),
            journal,
            "a failed observation must neither execute nor settle"
        );

        service.resume_recovered_runs().await;
        join_run_task(&service, &id).await;
        assert_unsettled(&service, &id, RunStatus::Waiting).await;
    }
}

#[tokio::test]
async fn cancel_observation_settlement_errors_keep_accepted_cancel_retryable() {
    for point in ["drain", "fold"] {
        for shutdown in [false, true] {
            let (_guard, service, id, directory) = waiting_run().await;
            crate::run_dirs::request_remote_cancel(&directory, &id, "test").unwrap();
            service.inner.runs.write().await.get_mut(&id).unwrap().state =
                RunStatus::CancelRequested;
            fail_once(&directory, point, 0);
            {
                let _lease = crate::run_dirs::try_lock_run_execution(&directory)
                    .unwrap()
                    .expect("no engine writer");
                service
                    .settle_queued_cancel(&id, &directory, "cancellation requested")
                    .await;
            }
            assert_fault_consumed(&directory, point);
            assert_unsettled(&service, &id, RunStatus::CancelRequested).await;
            let state = fold_run_state(&directory).unwrap();
            assert_eq!(state.cancel_operations.len(), usize::from(point == "fold"));
            assert_eq!(
                crate::run_dirs::has_pending_cancel_control(&directory),
                point == "drain",
                "an undrained acceptance must remain in the mailbox"
            );

            if shutdown {
                service.mark_shutting_down();
                let _lease = crate::run_dirs::try_lock_run_execution(&directory)
                    .unwrap()
                    .expect("no engine writer");
                service
                    .settle_queued_cancel(&id, &directory, "shutdown requested")
                    .await;
            } else {
                service.resume_recovered_runs().await;
                join_run_task(&service, &id).await;
            }
            let expected = if shutdown {
                RunStatus::Interrupted
            } else {
                RunStatus::Canceled
            };
            assert_eq!(service.inner.runs.read().await[&id].state, expected);
            assert_eq!(
                fold_run_state(&directory).unwrap().terminal,
                Some(if shutdown {
                    engine::TerminalState::Interrupted
                } else {
                    engine::TerminalState::Canceled
                })
            );
            assert!(!crate::run_dirs::has_pending_cancel_control(&directory));
        }
    }
}

#[tokio::test]
async fn cancel_observation_persistent_corruption_stays_diagnostic_not_canceled() {
    let (_guard, service, id, directory) = waiting_run().await;
    service.inner.runs.write().await.get_mut(&id).unwrap().state = RunStatus::Queued;
    let path = run_meta_dir(&directory).join("journal.jsonl");
    let original = std::fs::read_to_string(&path).unwrap();
    let corrupt = format!("{original}\nnot-json\n");
    std::fs::write(&path, &corrupt).unwrap();
    for _ in 0..2 {
        service.resume_recovered_runs().await;
        join_run_task(&service, &id).await;
        assert_eq!(std::fs::read_to_string(&path).unwrap(), corrupt);
        let error = fold_run_state(&directory).expect_err("corruption remains visible");
        assert!(error.to_string().contains("invalid journal JSON"));
        let runs = service.inner.runs.read().await;
        assert_eq!(runs[&id].state, RunStatus::Queued);
        assert!(runs[&id].question.is_some());
        assert!(!runs[&id].cancellation.is_cancelled());
    }
    std::fs::write(&path, original).unwrap();
    service.resume_recovered_runs().await;
    join_run_task(&service, &id).await;
    assert_unsettled(&service, &id, RunStatus::Waiting).await;
}

#[tokio::test]
async fn cancel_observation_known_terminal_converges_without_another_terminal() {
    let (_guard, service, id, directory) = waiting_run().await;
    let question = service.inner.runs.read().await[&id]
        .question
        .clone()
        .unwrap();
    service
        .answer(
            id.clone(),
            question.id,
            api::AnswerPayload {
                values: BTreeMap::from([("answer".into(), serde_json::json!("brief"))]),
            },
        )
        .await
        .unwrap();
    wait_for_snapshot(&service, &id, RunStatus::Succeeded).await;
    join_run_task(&service, &id).await;
    let journal = std::fs::read(run_meta_dir(&directory).join("journal.jsonl")).unwrap();
    // Even shutdown cannot replace a verified terminal with Interrupted.
    service.mark_shutting_down();
    service.inner.runs.write().await.get_mut(&id).unwrap().state = RunStatus::CancelRequested;
    crate::run_dirs::request_remote_cancel(&directory, &id, "test").unwrap();
    let _lease = crate::run_dirs::try_lock_run_execution(&directory)
        .unwrap()
        .expect("no engine writer");
    service
        .settle_queued_cancel(&id, &directory, "cancellation requested")
        .await;
    assert_eq!(
        service.inner.runs.read().await[&id].state,
        RunStatus::Succeeded
    );
    assert_eq!(
        std::fs::read(run_meta_dir(&directory).join("journal.jsonl")).unwrap(),
        journal
    );
    assert!(!crate::run_dirs::has_pending_cancel_control(&directory));
}

// These tests use Tokio's current-thread runtime, so a thread-local subscriber
// captures spawned-task diagnostics without changing the process-wide logger.
#[derive(Clone, Default)]
struct ObservationDiagnostics(std::sync::Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for ObservationDiagnostics {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::ERROR
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        use std::fmt::Write;
        let mut text = String::new();
        event.record(
            &mut |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                write!(&mut text, "{field}={value:?} ").unwrap();
            },
        );
        self.0.lock().unwrap().push(text);
    }
}

impl ObservationDiagnostics {
    fn assert_cause(&self, id: &str, cause: &str, attempts: usize) {
        let events = self.0.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.contains("pre_execution_journal")
                        && event.contains(id)
                        && event.contains(cause)
                })
                .count(),
            attempts,
            "full phase-specific cause must be retained: {events:?}"
        );
    }
}

// Drive the production resumer's immediate first tick, with an aged queue
// entry, rather than calling spawn directly or waiting for a wall-clock timer.
async fn queued_resumer_attempt(service: &LocalService, id: &str) {
    let shutdown = tokio_util::sync::CancellationToken::new();
    let resumer = service.start_queued_resumer(shutdown.clone());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if service.inner.runs.read().await[id]
                .task
                .lock()
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("queued resumer must install a task");
    shutdown.cancel();
    resumer.await.unwrap().unwrap();
    join_run_task(service, id).await;
}

async fn retained_state(service: &LocalService, id: &str) -> serde_json::Value {
    let runs = service.inner.runs.read().await;
    let r = &runs[id];
    serde_json::json!({
        "state": r.state, "question": r.question, "confirm": r.confirm,
        "inputs": r.inputs, "answers": r.answers, "confirmations": r.confirmations,
        "artifacts": r.artifacts, "priority": r.priority, "preempted": r.preempted,
        "queued_at": r.queued_at, "owner_id": r.owner_id, "parent": r.parent_run_id,
        "contract": r.contract_sha256, "package": r.package_sha256,
        "canceled": r.cancellation.is_cancelled(),
        "yielded": r.scheduler_yield.is_cancelled(),
    })
}

async fn age_queue(service: &LocalService, id: &str) {
    let mut runs = service.inner.runs.write().await;
    let record = runs.get_mut(id).unwrap();
    record.state = RunStatus::Queued;
    record.queued_at = Some(chrono::Utc::now() - chrono::Duration::seconds(11));
}

#[tokio::test]
async fn pre_execution_observation_one_shot_io_recovers_in_same_service() {
    for (generator, suspended) in [
        ("ask-user", RunStatus::Waiting),
        ("side-effect-confirm", RunStatus::Confirming),
    ] {
        let (_guard, service, id, directory) = suspended_run(generator, suspended).await;
        let diagnostics = ObservationDiagnostics::default();
        let _subscriber = tracing::subscriber::set_default(diagnostics.clone());
        age_queue(&service, &id).await;
        let before = retained_state(&service, &id).await;
        assert!(
            before[if suspended == RunStatus::Waiting {
                "question"
            } else {
                "confirm"
            }]
            .is_object()
        );
        let path = run_meta_dir(&directory).join("journal.jsonl");
        let journal = std::fs::read(&path).unwrap();
        let budget = fold_run_state(&directory).unwrap().budget;
        fail_once(&directory, "pre_state", 0);

        queued_resumer_attempt(&service, &id).await;
        assert_fault_consumed(&directory, "pre_state");
        diagnostics.assert_cause(&id, "one-shot pre_state observation failure", 1);
        assert_eq!(retained_state(&service, &id).await, before);
        assert_eq!(std::fs::read(&path).unwrap(), journal);
        let folded = fold_run_state(&directory).unwrap();
        assert!(folded.terminal.is_none());
        assert_eq!(
            serde_json::to_value(folded.budget).unwrap(),
            serde_json::to_value(budget).unwrap()
        );

        queued_resumer_attempt(&service, &id).await;
        assert_eq!(service.inner.runs.read().await[&id].state, suspended);
        assert!(fold_run_state(&directory).unwrap().terminal.is_none());
        // Recovery returns to the same outstanding interaction, not a fresh run.
        let after = retained_state(&service, &id).await;
        assert_eq!(after["question"], before["question"]);
        assert_eq!(after["confirm"], before["confirm"]);
    }
}

#[tokio::test]
async fn pre_execution_observation_semantic_corruption_remains_retryable() {
    for (generator, suspended) in [
        ("ask-user", RunStatus::Waiting),
        ("side-effect-confirm", RunStatus::Confirming),
    ] {
        let (_guard, service, id, directory) = suspended_run(generator, suspended).await;
        let diagnostics = ObservationDiagnostics::default();
        let _subscriber = tracing::subscriber::set_default(diagnostics.clone());
        age_queue(&service, &id).await;
        let before = retained_state(&service, &id).await;
        let path = run_meta_dir(&directory).join("journal.jsonl");
        // Preserve valid JSON and event shape, but splice foreign identity into
        // the final non-identity event. Reads/cancel checks succeed; only the
        // engine's semantic fold rejects this journal.
        let mut values = crate::summaries::read_journal_events(&directory).unwrap();
        let last = values.last_mut().unwrap();
        assert!(!matches!(
            last["t"].as_str(),
            Some("run_started" | "run_queued" | "run_forked")
        ));
        last["run_id"] = serde_json::json!("foreign-run");
        let corrupt = values.iter().map(|v| format!("{v}\n")).collect::<String>();
        std::fs::write(&path, &corrupt).unwrap();
        let readable = crate::summaries::read_journal_events(&directory).unwrap();
        let cause = engine::RunState::fold_values(&readable)
            .unwrap_err()
            .to_string();
        assert!(
            cause.contains("run event belongs to `foreign-run`"),
            "{cause}"
        );
        for attempt in 1..=2 {
            queued_resumer_attempt(&service, &id).await;
            diagnostics.assert_cause(&id, &cause, attempt);
            assert_eq!(retained_state(&service, &id).await, before);
            // Exact equality includes all accounting records and excludes any
            // fabricated run_failed, run_canceled, or run_interrupted event.
            assert_eq!(std::fs::read_to_string(&path).unwrap(), corrupt);
            assert_eq!(fold_run_state(&directory).unwrap_err().to_string(), cause);
        }
    }
}
