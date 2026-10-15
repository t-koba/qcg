use super::support::*;
use crate::*;
use api::{AnswerPayload, ForkRun, RunStatus, StartRun};
use camino::Utf8Path;
use serde_json::json;
use std::collections::BTreeMap;

fn package(root: &Utf8Path, ask: bool) {
    let dir = root.join("pinned");
    std::fs::create_dir_all(&dir).unwrap();
    let question = if ask {
        r#"
[[flow]]
id = "choose_mode"
type = "ask_user"
[flow.params]
content = "Choose"
options = ["brief", "detailed"]
"#
    } else {
        ""
    };
    std::fs::write(
        dir.join(contract::MANIFEST_FILE),
        format!(
            r#"
[generator]
id = "pinned"
name = "Pinned"
version = "0.1.0"
{question}
[[flow]]
id = "render"
type = "render"
artifact = {{ label = "Result", required = true }}
[flow.params]
template = "body.j2"
output_file = "result.txt"
[permissions]
fs_read = []
fs_write = ["workspace"]
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
[permissions.containers]
enabled = false
"#
        ),
    )
    .unwrap();
    std::fs::write(dir.join("body.j2"), "admitted bytes").unwrap();
}

async fn start(service: &LocalService) -> String {
    service
        .start_run(StartRun {
            generator_id: "pinned".into(),
            ..Default::default()
        })
        .await
        .unwrap()
}

async fn answer(service: &LocalService, id: &str) {
    let waiting = wait_for_snapshot(service, id, RunStatus::Waiting).await;
    let question = waiting.question.unwrap();
    service
        .answer(
            id.into(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("brief"))]),
            },
        )
        .await
        .unwrap();
}

fn output(runs: &Utf8Path, id: &str) -> String {
    std::fs::read_to_string(runs.join(id).join("workspace/result.txt")).unwrap()
}

#[tokio::test]
async fn queued_package_replacement_does_not_change_runtime_bytes_or_identity() {
    let root = temp_run_dir("queued-package-pin");
    let _guard = TempGuard(root.clone());
    let generators = root.join("generators");
    let runs = root.join("runs");
    package(&generators, false);
    let service = test_service(vec![generators.clone()], runs.clone());
    // Hold every slot so mutation is deterministically after admission and
    // before runtime template resolution.
    let permit = (0..policy::DEFAULT_MAX_ACTIVE_RUNS)
        .map(|_| {
            crate::queue::PriorityPermits::try_take(
                &service.inner.execution_permits,
                service.inner.queue_notify.clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let id = start(&service).await;
    let admitted = service.inner.runs.read().await[&id]
        .package_sha256
        .clone()
        .unwrap();
    assert_eq!(
        service.inner.runs.read().await[&id].contract.root,
        runs.join(&id).join("generator-package")
    );
    std::fs::write(generators.join("pinned/body.j2"), "replaced bytes").unwrap();
    // A same-manifest/different-template request cannot adopt the identity.
    assert!(matches!(
        service
            .start_run_with_id(
                StartRun {
                    generator_id: "pinned".into(),
                    ..Default::default()
                },
                Some(id.clone())
            )
            .await,
        Err(api::ApiError::Conflict { .. })
    ));

    drop(permit);
    assert_eq!(
        wait_for_terminal_snapshot(&service, &id).await.state,
        RunStatus::Succeeded
    );
    assert_eq!(output(&runs, &id), "admitted bytes");
    let events = crate::summaries::read_journal_events(&runs.join(&id)).unwrap();
    for event in events
        .iter()
        .filter(|e| matches!(e["t"].as_str(), Some("run_queued" | "run_started")))
    {
        assert_eq!(event["package_sha256"], admitted);
        assert_eq!(event["generator_path"], generators.join("pinned").as_str());
    }
}

#[tokio::test]
async fn queued_missing_or_mismatched_snapshot_refuses_execution() {
    for missing in [false, true] {
        let root = temp_run_dir("queued-bad-package-pin");
        let _guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        package(&generators, false);
        let service = test_service(vec![generators], runs.clone());
        let permit = (0..policy::DEFAULT_MAX_ACTIVE_RUNS)
            .map(|_| {
                crate::queue::PriorityPermits::try_take(
                    &service.inner.execution_permits,
                    service.inner.queue_notify.clone(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let id = start(&service).await;
        let snapshot = runs.join(&id).join("generator-package");
        if missing {
            std::fs::remove_dir_all(snapshot).unwrap();
        } else {
            std::fs::write(snapshot.join("body.j2"), "tampered").unwrap();
        }
        drop(permit);
        assert_eq!(
            wait_for_terminal_snapshot(&service, &id).await.state,
            RunStatus::Failed
        );
        assert!(!runs.join(&id).join("workspace/result.txt").exists());
        let events = crate::summaries::read_journal_events(&runs.join(&id)).unwrap();
        assert!(!events.iter().any(|e| e["t"] == "run_started"));
        assert!(events.iter().any(|e| e["t"] == "run_error"));
    }
}

#[tokio::test]
async fn restart_and_fork_use_admitted_package_after_installed_package_removal() {
    let root = temp_run_dir("restart-fork-package-pin");
    let _guard = TempGuard(root.clone());
    let generators = root.join("generators");
    let runs = root.join("runs");
    package(&generators, true);
    let service = test_service(vec![generators.clone()], runs.clone());
    let id = start(&service).await;
    wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    drop(service);
    wait_for_runs_store_release(&runs).await;
    std::fs::remove_dir_all(generators.join("pinned")).unwrap();
    let rebuilt = test_service(vec![generators], runs.clone());
    answer(&rebuilt, &id).await;
    assert_eq!(
        wait_for_snapshot(&rebuilt, &id, RunStatus::Succeeded)
            .await
            .state,
        RunStatus::Succeeded
    );
    assert_eq!(output(&runs, &id), "admitted bytes");
    let events = crate::summaries::read_journal_events(&runs.join(&id)).unwrap();
    let at_seq = events
        .iter()
        .find(|e| e["t"] == "step_finished" && e["status"] == "success")
        .unwrap()["seq"]
        .as_u64()
        .unwrap();
    let fork_id = rebuilt
        .fork_run(
            &id,
            ForkRun {
                at_seq,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        wait_for_snapshot(&rebuilt, &fork_id, RunStatus::Succeeded)
            .await
            .state,
        RunStatus::Succeeded
    );
    assert_eq!(output(&runs, &fork_id), "admitted bytes");
    let parent = crate::summaries::fold_run_state(&runs.join(&id)).unwrap();
    let child = crate::summaries::fold_run_state(&runs.join(&fork_id)).unwrap();
    assert_eq!(parent.package_sha256, child.package_sha256);
    assert_eq!(parent.generator_path, child.generator_path);
    // A completed fork admission owns its copy; retry does not consult the
    // source run again, and must never replace its admitted identity.
    std::fs::remove_dir_all(runs.join(&id)).unwrap();
    assert_eq!(
        rebuilt
            .fork_run_with_id(
                &id,
                ForkRun {
                    at_seq,
                    ..Default::default()
                },
                Some(fork_id.clone())
            )
            .await
            .unwrap(),
        fork_id
    );
}

#[cfg(unix)]
#[tokio::test]
async fn fork_refuses_symlinked_target_without_following_external_journal() {
    use std::os::unix::fs::symlink;

    let root = temp_run_dir("fork-symlink-target");
    let _guard = TempGuard(root.clone());
    let generators = root.join("generators");
    let runs = root.join("runs");
    package(&generators, true);
    let service = test_service(vec![generators], runs.clone());
    let source_id = start(&service).await;
    answer(&service, &source_id).await;
    wait_for_terminal_snapshot(&service, &source_id).await;
    let events = crate::summaries::read_journal_events(&runs.join(&source_id)).unwrap();
    let at_seq = events
        .iter()
        .find(|event| event["t"] == "step_finished" && event["status"] == "success")
        .unwrap()["seq"]
        .as_u64()
        .unwrap();

    let external = root.join("external-run");
    std::fs::create_dir_all(external.join("meta")).unwrap();
    let journal = external.join("meta/journal.jsonl");
    std::fs::write(&journal, b"outside\n").unwrap();
    let target = runs.join("fork-symlink");
    symlink(&external, &target).unwrap();
    let error = service
        .fork_run_with_id(
            &source_id,
            ForkRun {
                at_seq,
                ..Default::default()
            },
            Some("fork-symlink".into()),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("plain directory"));
    assert!(
        std::fs::symlink_metadata(&target)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read(&journal).unwrap(), b"outside\n");
}

#[tokio::test]
async fn recovery_and_fork_refuse_missing_mismatched_and_legacy_snapshots() {
    for fault in ["missing", "mismatch", "legacy"] {
        let root = temp_run_dir(fault);
        let _guard = TempGuard(root.clone());
        let generators = root.join("generators");
        let runs = root.join("runs");
        package(&generators, true);
        let service = test_service(vec![generators], runs.clone());
        let id = start(&service).await;
        wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let run_dir = runs.join(&id);
        let events = crate::summaries::read_journal_events(&run_dir).unwrap();
        let at_seq = events.iter().find(|e| e["t"] == "run_started").unwrap()["seq"]
            .as_u64()
            .unwrap();
        match fault {
            "missing" => std::fs::remove_dir_all(run_dir.join("generator-package")).unwrap(),
            "mismatch" => {
                std::fs::write(run_dir.join("generator-package/body.j2"), "tampered").unwrap()
            }
            _ => {
                // Simulate schema-v2 history written before package pinning.
                let text = events
                    .into_iter()
                    .map(|mut event| {
                        event.as_object_mut().unwrap().remove("package_sha256");
                        serde_json::to_string(&event).unwrap() + "\n"
                    })
                    .collect::<String>();
                std::fs::write(run_dir.join("meta/journal.jsonl"), text).unwrap();
            }
        }
        assert!(
            crate::summaries::rehydrate_runs(&runs, 32, 64).is_err(),
            "{fault}"
        );
        assert!(
            service.reserve_fork_run_id(&id).await.is_ok(),
            "idempotency reservation must not inspect the source snapshot: {fault}"
        );
        let target = "refused-fork";
        assert!(
            service
                .fork_run_with_id(
                    &id,
                    ForkRun {
                        at_seq,
                        ..Default::default()
                    },
                    Some(target.into())
                )
                .await
                .is_err(),
            "{fault}"
        );
        assert!(!runs.join(target).exists());
        assert!(run_dir.exists(), "source survives refusal");
    }
}

#[tokio::test]
async fn queued_restart_uses_snapshot_without_installed_package() {
    let root = temp_run_dir("queued-restart-package-pin");
    let _guard = TempGuard(root.clone());
    let generators = root.join("generators");
    let runs = root.join("runs");
    package(&generators, false);
    let service = test_service(vec![generators.clone()], runs.clone());
    let permits = (0..policy::DEFAULT_MAX_ACTIVE_RUNS)
        .map(|_| {
            crate::queue::PriorityPermits::try_take(
                &service.inner.execution_permits,
                service.inner.queue_notify.clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let id = start(&service).await;
    // Model process loss after durable admission, without shutdown settlement.
    let task = service.inner.runs.read().await[&id]
        .task
        .lock()
        .unwrap()
        .take()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(permits);
    drop(service);
    wait_for_runs_store_release(&runs).await;
    std::fs::remove_dir_all(generators.join("pinned")).unwrap();
    let rebuilt = test_service(vec![generators], runs.clone());
    rebuilt.resume_recovered_runs().await;
    wait_for_snapshot(&rebuilt, &id, RunStatus::Succeeded).await;
    assert_eq!(output(&runs, &id), "admitted bytes");
}

#[test]
fn checkpoint_failure_cleans_only_its_fresh_target() {
    let root = temp_run_dir("checkpoint-copy-cleanup");
    let _guard = TempGuard(root.clone());
    let source = root.join("source");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(source.join("sentinel"), "source").unwrap();
    let target = root.join("target");
    let patch = api::ForkStatePatch::default();
    // Missing checkpoint history fails after fresh target allocation.
    assert!(
        crate::run_dirs::prepare_checkpoint_fork(&source, "source", &target, "target", 1, &patch)
            .is_err()
    );
    assert!(!target.exists());
    assert_eq!(
        std::fs::read_to_string(source.join("sentinel")).unwrap(),
        "source"
    );
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("sentinel"), "pre-existing").unwrap();
    assert!(
        crate::run_dirs::prepare_checkpoint_fork(&source, "source", &target, "target", 1, &patch)
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(target.join("sentinel")).unwrap(),
        "pre-existing"
    );
}
