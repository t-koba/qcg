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

pub(crate) fn fail_cancel_observation(
    directory: &Utf8Path,
    point: &'static str,
) -> Result<(), ServiceError> {
    let mut failures = READ_FAILURES.lock().unwrap();
    let key = (directory.to_owned(), point);
    if let Some(skip) = failures.get_mut(&key) {
        if *skip == 0 {
            failures.remove(&key);
            return Err(ServiceError::Io(std::io::Error::other(
                "one-shot cancel observation failure",
            )));
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
    let root = temp_run_dir("cancel-observation");
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
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
