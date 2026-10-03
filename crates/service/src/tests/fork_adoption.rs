use super::support::*;
use crate::*;
use api::{AnswerPayload, ApiError, ForkRun, ForkStatePatch, RunStatus, StartRun};
use camino::{Utf8Path, Utf8PathBuf};
use policy::DEFAULT_MAX_TRACKED_RUNS;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn journal_order_wins_restart_and_fork_continue_v2() {
    // E06 end to end: z_first writes v1, a_second overwrites v2 (same
    // path), then a question suspends. Answering, restarting, and
    // forking must all continue with v2: journal order decides, never
    // node-name order.
    let root = temp_run_dir("e06-v2-chain");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "e06-chain"
name = "E06 Chain"
version = "0.1.0"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
[[flow]]
id = "z_first"
type = "write"
[flow.params]
output_file = "out.txt"
content = "v1"
[[flow]]
id = "a_second"
type = "write"
needs = ["z_first"]
[flow.params]
output_file = "out.txt"
content = "v2"
[[flow]]
id = "ask"
type = "ask_user"
needs = ["a_second"]
[flow.params]
content = "Continue?"
options = ["yes"]"#,
    )
    .expect("generator manifest should be written");
    let service = test_service(vec![root.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let waiting = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question = waiting.question.expect("run should expose its question");
    let run_dir = runs.join(&id);
    let workspace_file = run_dir.join("workspace").join("out.txt");
    assert_eq!(
        std::fs::read_to_string(&workspace_file).expect("workspace file"),
        "v2",
        "journal order must project v2 before the question"
    );
    // Fork from the waiting checkpoint before answering.
    let waiting_seq = crate::summaries::read_journal_events(&run_dir)
        .expect("journal should read")
        .iter()
        .filter_map(|event| {
            (event.get("t").and_then(Value::as_str) == Some("run_waiting"))
                .then(|| event.get("seq").and_then(Value::as_u64))
                .flatten()
        })
        .max()
        .expect("waiting checkpoint should exist");
    let fork_id = service
        .fork_run(
            &id,
            ForkRun {
                at_seq: waiting_seq,
                state_patch: ForkStatePatch::default(),
                ..Default::default()
            },
        )
        .await
        .expect("fork should succeed");
    let fork_waiting = wait_for_snapshot(&service, &fork_id, RunStatus::Waiting).await;
    service
        .answer(
            fork_id.clone(),
            fork_waiting
                .question
                .expect("fork should expose its question")
                .id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("yes"))]),
            },
        )
        .await
        .expect("fork answer should be accepted");
    // Answer the source run too.
    service
        .answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("yes"))]),
            },
        )
        .await
        .expect("answer should be accepted");
    assert_eq!(
        wait_for_terminal_snapshot(&service, &id).await.state,
        RunStatus::Succeeded
    );
    assert_eq!(
        std::fs::read_to_string(&workspace_file).expect("workspace file"),
        "v2",
        "answering must not roll the workspace back to v1"
    );
    // Restart: rebuild the service on the same store and confirm the
    // settled run still projects v2.
    drop(service);
    wait_for_runs_store_release(&runs).await;
    let rebuilt = test_service(vec![root.clone()], runs.clone());
    assert_eq!(
        std::fs::read_to_string(&workspace_file).expect("workspace file"),
        "v2",
        "restart must keep projecting v2"
    );
    assert_eq!(
        wait_for_terminal_snapshot(&rebuilt, &fork_id).await.state,
        RunStatus::Succeeded,
        "the fork must converge without re-running finished steps"
    );
    let fork_workspace = runs.join(&fork_id).join("workspace").join("out.txt");
    assert_eq!(
        std::fs::read_to_string(&fork_workspace).expect("fork workspace file"),
        "v2",
        "the fork must continue with v2, not v1"
    );
    drop(rebuilt);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn adopting_an_orphaned_run_restores_its_pending_question() {
    // E03: a disk adopt must fold the journal so accepted answers and
    // the pending prompt survive instead of being reset to Queued.
    let root = temp_run_dir("adopt-pending-question");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "adopt-pending"
name = "Adopt Pending"
version = "0.1.0"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
[[flow]]
id = "ask"
type = "ask_user"
[flow.params]
content = "Continue?"
options = ["yes"]"#,
    )
    .expect("generator manifest should be written");
    let service = test_service(vec![root], runs);
    let request = || StartRun {
        generator_id: "generator".into(),
        inputs: BTreeMap::new(),
        ..Default::default()
    };
    let reserved = "generator-adopt-1".to_string();
    let id = service
        .start_run_with_id(request(), Some(reserved.clone()))
        .await
        .expect("first start should win");
    let waiting = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question = waiting.question.expect("run should expose its question");
    // Simulate eviction/restart: drop the in-memory record. The run is
    // suspended, so no engine task is executing.
    service.inner.runs.write().await.remove(&id);
    let adopted = service
        .start_run_with_id(request(), Some(reserved))
        .await
        .expect("adoption should succeed");
    assert_eq!(adopted, id);
    let restored = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    assert_eq!(
        restored
            .question
            .as_ref()
            .map(|question| question.id.as_str()),
        Some(question.id.as_str()),
        "the adopted record must keep the same pending question"
    );
    // The suspended engine task releases the execution lease just after
    // publishing Waiting; let that settle before answering so this test
    // does not depend on the server-side queued resumer.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    service
        .answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("yes"))]),
            },
        )
        .await
        .expect("answer should resume the adopted run");
    let terminal = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(terminal.state, RunStatus::Succeeded);
}

#[tokio::test]
async fn snapshot_displays_a_disk_only_waiting_run() {
    // E03: a run this process has not rehydrated (peer-owned, or removed
    // from memory before adoption) must still display its durable
    // pending question instead of an empty queued snapshot.
    let runs = temp_run_dir("disk-only-waiting");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs);
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    let question = wait_for_snapshot(&service, &id, RunStatus::Waiting)
        .await
        .question
        .expect("run should have a pending question");
    service.inner.runs.write().await.remove(&id);
    let snapshot = service
        .snapshot(id.clone())
        .await
        .expect("disk-only snapshot should exist");
    assert_eq!(snapshot.state, RunStatus::Waiting);
    assert_eq!(
        snapshot.question.as_ref().map(|item| &item.id),
        Some(&question.id),
        "the durable pending question must survive without an in-memory record"
    );
    drop(service);
}

#[test]
fn adoption_snapshot_covers_seed_and_fork_inputs_without_second_read() {
    // E03: start/fork adoption performs exactly ONE journal read on the
    // normal path. `try_adopt_run_dir_with_snapshot` reads once and
    // folds once; its only second read is the conditional recheck taken
    // solely when the journal lacks this run's own admission (partial
    // fork/empty journal), never on the adopted path below. Every
    // downstream derivation (seed, fork inputs, HITL maps, queued
    // identity) is pure over `snapshot.events`. Proof by deletion: the
    // run directory is removed after adoption, so any second journal
    // I/O would fail and this test would turn red.
    let root = temp_run_dir("adopt-single-read");
    let _guard = TempGuard(root.clone());
    let contract = contract::Contract::load(
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators/ask-user"),
    )
    .expect("fixture contract should load");
    let run_id = "adopt-single-read-1";
    let run_dir = root.join("runs").join(run_id);
    prepare_api_run_directory(&run_dir).expect("run directory should prepare");
    let (events, _) = broadcast::channel(8);
    let record = RunRecord {
        contract: contract.clone(),
        contract_sha256: contract.sha256.clone(),
        inputs: BTreeMap::new(),
        answers: BTreeMap::new(),
        confirmations: BTreeMap::new(),
        priority: 0,
        parent_run_id: None,
        preempted: false,
        state: RunStatus::Queued,
        run_dir: run_dir.clone(),
        artifacts: None,
        question: None,
        confirm: None,
        events,
        cancellation: CancellationToken::new(),
        task: Arc::new(Mutex::new(None)),
        queued_at: None,
        owner_id: String::new(),
        ephemeral: false,
    };
    write_run_event(
        &record,
        "run_queued",
        json!({
            "run_id": run_id,
            "generator": "ask-user@0.1.0",
            "generator_path": contract.root,
            "contract_sha256": contract.sha256,
            "inputs": {},
            "answers": {},
            "confirmations": {},
            "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "priority": 0,
            "parent_run_id": Value::Null,
        }),
    )
    .expect("queued event should append");
    // The single journal read on the normal path.
    let snapshot = crate::run_dirs::try_adopt_run_dir_with_snapshot(&run_dir, run_id)
        .expect("adopt check should succeed");
    assert!(snapshot.adopted, "the completed admission should adopt");
    assert!(
        !snapshot.events.is_empty(),
        "the snapshot should carry events"
    );
    assert!(
        snapshot.state.is_some(),
        "the snapshot should carry its fold"
    );
    let events_before = snapshot.events.clone();
    // Delete the journal: any re-read from disk now fails.
    std::fs::remove_dir_all(&run_dir).expect("run directory should be removable");
    assert!(
        !run_dir.exists(),
        "the journal must be gone for this proof to mean anything"
    );
    // Seed, fork inputs, HITL maps, and queued identity all derive from
    // the snapshot with zero journal I/O.
    let seed = crate::runs_api::seed_adopted_run_from_snapshot(
        &snapshot,
        &crate::runs_api::AdoptedRunIdentity {
            inputs: &BTreeMap::new(),
            contract_sha256: &contract.sha256,
            priority: 0,
            parent: None,
            answers: &BTreeMap::new(),
            confirmations: &BTreeMap::new(),
        },
    )
    .expect("snapshot seed should succeed without the journal")
    .expect("the adopted run should carry a seed");
    assert!(seed.answers.is_empty());
    assert!(seed.confirmations.is_empty());
    let fork_inputs = crate::runs_api::fork_checkpoint_inputs(&snapshot, &contract, run_id, 1)
        .expect("fork inputs should derive without the journal");
    assert!(
        fork_inputs.is_empty(),
        "empty inputs should resolve to empty"
    );
    let folded = engine::RunState::fold_values(&snapshot.events)
        .expect("the snapshot should fold without the journal");
    assert!(folded.answers.is_empty());
    assert!(folded.confirmations.is_empty());
    assert_eq!(folded.priority, 0);
    assert!(folded.parent_run_id.is_none());
    assert_eq!(
        snapshot.events, events_before,
        "derivations must not mutate the snapshot"
    );
}

#[tokio::test]
async fn adoption_refuses_mismatched_inputs() {
    // E03: adoption resumes the admitted identity only; different inputs
    // for the same reserved run id are refused.
    let root = temp_run_dir("adopt-inputs-mismatch");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "generator"
name = "Adopt Inputs"
version = "0.1.0"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "name"
required = true
type = "string"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]
"#,
    )
    .expect("generator manifest should be written");
    let service = test_service(vec![root], runs);
    let reserved = "generator-adopt-inputs-1".to_string();
    let request = |name: &str| StartRun {
        generator_id: "generator".into(),
        inputs: BTreeMap::from([("name".into(), json!(name))]),
        ..Default::default()
    };
    let id = service
        .start_run_with_id(request("first"), Some(reserved.clone()))
        .await
        .expect("first admission should succeed");
    wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    service.inner.runs.write().await.remove(&id);
    let error = service
        .start_run_with_id(request("second"), Some(reserved.clone()))
        .await
        .expect_err("different inputs must not adopt the run");
    assert!(
        matches!(error, ApiError::Conflict { .. }),
        "mismatched adoption must be a conflict: {error}"
    );
    let adopted = service
        .start_run_with_id(request("first"), Some(reserved))
        .await
        .expect("identical inputs should adopt the run");
    assert_eq!(adopted, id);
    wait_for_snapshot(&service, &adopted, RunStatus::Waiting).await;
    // Answers are part of the admitted identity: a retry carrying
    // different answers for the same reserved run id is refused instead
    // of silently re-targeting the run (E03).
    let changed = StartRun {
        answers: BTreeMap::from([("name".into(), json!("changed"))]),
        ..request("first")
    };
    service.inner.runs.write().await.remove(&adopted);
    let error = service
        .start_run_with_id(changed, Some(adopted.clone()))
        .await
        .expect_err("changed answers must not adopt the run");
    assert!(
        matches!(error, ApiError::Conflict { .. }),
        "changed answers must be a conflict: {error}"
    );
    drop(service);
}

#[tokio::test]
async fn adoption_refuses_a_changed_contract_or_priority() {
    // E03: a different contract or priority for the same reserved run
    // id is a different admission and must not adopt the journal.
    let root = temp_run_dir("adopt-identity-mismatch");
    let _ = std::fs::remove_dir_all(&root);
    let write_manifest = |dir: &Utf8Path, id: &str, content: &str| {
        std::fs::create_dir_all(dir).expect("generator directory should be created");
        std::fs::write(
            dir.join(contract::MANIFEST_FILE),
            format!(
                r#"
[generator]
id = "{id}"
name = "Adopt {id}"
version = "0.1.0"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "name"
required = true
type = "string"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "{content}?"
options = ["yes"]
"#,
            ),
        )
        .expect("generator manifest should be written");
    };
    let primary = root.join("primary");
    let secondary = root.join("secondary");
    let runs = root.join("runs");
    write_manifest(&primary.join("generator"), "generator", "Continue");
    write_manifest(&secondary.join("generator2"), "generator2", "Proceed");
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![primary, secondary],
        runs,
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let reserved = "generator-adopt-identity-1".to_string();
    let request = |generator: &str| StartRun {
        generator_id: generator.into(),
        inputs: BTreeMap::from([("name".into(), json!("first"))]),
        ..Default::default()
    };
    let id = service
        .start_run_with_id(request("generator"), Some(reserved.clone()))
        .await
        .expect("first admission should succeed");
    wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    service.inner.runs.write().await.remove(&id);
    let error = service
        .start_run_with_id(request("generator2"), Some(reserved.clone()))
        .await
        .expect_err("a different contract must not adopt the run");
    assert!(
        matches!(error, ApiError::Conflict { .. }),
        "contract mismatch must be a conflict: {error}"
    );
    let prioritized = StartRun {
        priority: Some(1),
        ..request("generator")
    };
    let error = service
        .start_run_with_id(prioritized, Some(reserved.clone()))
        .await
        .expect_err("a different priority must not adopt the run");
    assert!(
        error.to_string().contains("priority"),
        "priority mismatch must be named: {error}"
    );
    drop(service);
}

#[tokio::test]
async fn fork_adoption_requires_the_same_source() {
    // E03: re-admitting an adopted fork converges on the fork made from
    // the recorded source; a different source is refused.
    let runs = temp_run_dir("fork-adopt-source");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators.clone()], runs.clone());
    let source = start_ask_user_and_answer(&service, "brief").await;
    let terminal = wait_for_terminal_snapshot(&service, &source).await;
    assert_eq!(terminal.state, RunStatus::Succeeded);
    let checkpoint = read_journal_string(&service, source.clone())
        .await
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).expect("test journal lines must parse strictly")
        })
        .filter(|event| event.get("t").and_then(Value::as_str) == Some("step_finished"))
        .filter_map(|event| event.get("seq").and_then(Value::as_u64))
        .max()
        .expect("source should have a finished step");
    let reserved = "ask-user-fork-adopt-1".to_string();
    let fork = service
        .fork_run_with_id(
            &source,
            ForkRun {
                at_seq: checkpoint,
                ..Default::default()
            },
            Some(reserved.clone()),
        )
        .await
        .expect("fork admission should succeed");
    service.inner.runs.write().await.remove(&fork);
    let adopted = service
        .fork_run_with_id(
            &source,
            ForkRun {
                at_seq: checkpoint,
                ..Default::default()
            },
            Some(reserved),
        )
        .await
        .expect("identical fork should adopt");
    assert_eq!(adopted, fork);
    // A fork id bound to another source must not be re-adopted.
    let other = start_ask_user_and_answer(&service, "brief").await;
    let terminal = wait_for_terminal_snapshot(&service, &other).await;
    assert_eq!(terminal.state, RunStatus::Succeeded);
    // The seed identity (contract, inputs, priority, parent) is
    // asserted directly against a fork journal so no engine timing can
    // hide a mismatch behind terminal convergence.
    let contract = contract::Contract::load(generators.join("ask-user")).expect("contract loads");
    let fork_dir = runs.join("unit-fork");
    let fork_meta = crate::summaries::run_meta_dir(&fork_dir);
    std::fs::create_dir_all(&fork_meta).expect("fork meta");
    let event = json!({
        "t": "run_queued",
        "ts": "2026-01-01T00:00:00Z",
        "seq": 1,
        "run_id": "unit-fork",
        "trace_id": api::trace_id_for_run("unit-fork"),
        "span_id": api::span_id_for_seq(1),
        "generator": "ask-user@0.1.0",
        "generator_path": generators.join("ask-user").as_str(),
        "contract_sha256": contract.sha256,
        "inputs": {},
        "answers": {},
        "confirmations": {},

        "schema_version": api::JOURNAL_SCHEMA_VERSION,
        "retention_days": 0,
        "priority": 0,
        "parent_run_id": source,
        "effective_max_total_steps": 64,
        "effective_policy_origin": "unit-test",
    });
    std::fs::write(fork_meta.join("journal.jsonl"), format!("{}\n", event)).expect("fork journal");
    let empty = BTreeMap::new();
    // Single-read proof: materialize one snapshot, then seed purely
    // from it (no second journal read).
    let snapshot_events =
        crate::summaries::read_journal_events(&fork_dir).expect("fork journal should read");
    let snapshot_state =
        engine::RunState::fold_values(&snapshot_events).expect("fork journal should fold");
    let snapshot = crate::run_dirs::AdoptSnapshot {
        adopted: true,
        events: snapshot_events,
        state: Some(snapshot_state),
    };
    let seed =
        |inputs: &BTreeMap<String, Value>, sha: &str, priority: i32, parent: Option<&str>| {
            crate::runs_api::seed_adopted_run_from_snapshot(
                &snapshot,
                &crate::runs_api::AdoptedRunIdentity {
                    inputs,
                    contract_sha256: sha,
                    priority,
                    parent,
                    answers: &empty,
                    confirmations: &BTreeMap::new(),
                },
            )
        };
    assert!(
        seed(&empty, &contract.sha256, 0, Some(&source))
            .expect("matching identity should seed")
            .is_some(),
        "seed should exist"
    );
    for (label, inputs, sha, priority, parent) in [
        ("contract", &empty, "other-sha", 0, Some(source.as_str())),
        (
            "inputs",
            &BTreeMap::from([("extra".to_string(), json!(1))]),
            contract.sha256.as_str(),
            0,
            Some(source.as_str()),
        ),
        (
            "priority",
            &empty,
            contract.sha256.as_str(),
            1,
            Some(source.as_str()),
        ),
        (
            "parent",
            &empty,
            contract.sha256.as_str(),
            0,
            Some(other.as_str()),
        ),
    ] {
        let error = seed(inputs, sha, priority, parent)
            .expect_err(&format!("a {label} mismatch must conflict"));
        assert!(
            matches!(error, ApiError::Conflict { .. }),
            "a {label} mismatch must be a conflict: {error}"
        );
    }
    drop(service);
}

#[tokio::test]
async fn plain_start_cannot_adopt_a_fork_journal() {
    // E03: a reserved id admitted as a plain start must not adopt a
    // directory whose journal records a fork parent, even when every
    // other field agrees.
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let contract = contract::Contract::load(generators.join("ask-user")).expect("contract loads");
    let runs = temp_run_dir("start-adopt-fork");
    let _ = std::fs::remove_dir_all(&runs);
    let fork_dir = runs.join("plain-adopt-1");
    let fork_meta = crate::summaries::run_meta_dir(&fork_dir);
    std::fs::create_dir_all(&fork_meta).expect("fork meta");
    let event = json!({
        "t": "run_queued",
        "ts": "2026-01-01T00:00:00Z",
        "seq": 1,
        "run_id": "plain-adopt-1",
        "trace_id": api::trace_id_for_run("plain-adopt-1"),
        "span_id": api::span_id_for_seq(1),
        "generator": "ask-user@0.1.0",
        "generator_path": generators.join("ask-user").as_str(),
        "contract_sha256": contract.sha256,
        "inputs": {},
        "answers": {},
        "confirmations": {},

        "schema_version": api::JOURNAL_SCHEMA_VERSION,
        "retention_days": 0,
        "priority": 0,
        "parent_run_id": "some-source",
        "effective_max_total_steps": 64,
        "effective_policy_origin": "unit-test",
    });
    std::fs::write(fork_meta.join("journal.jsonl"), format!("{}\n", event)).expect("fork journal");
    let empty = BTreeMap::new();
    let snapshot_events =
        crate::summaries::read_journal_events(&fork_dir).expect("fork journal should read");
    let snapshot_state =
        engine::RunState::fold_values(&snapshot_events).expect("fork journal should fold");
    let snapshot = crate::run_dirs::AdoptSnapshot {
        adopted: true,
        events: snapshot_events,
        state: Some(snapshot_state),
    };
    let error = crate::runs_api::seed_adopted_run_from_snapshot(
        &snapshot,
        &crate::runs_api::AdoptedRunIdentity {
            inputs: &empty,
            contract_sha256: &contract.sha256,
            priority: 0,
            parent: None,
            answers: &empty,
            confirmations: &BTreeMap::new(),
        },
    )
    .expect_err("a fork journal must not adopt as a plain start");
    assert!(
        matches!(error, ApiError::Conflict { .. }),
        "the fork hole must be a conflict: {error}"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn adopted_seed_keeps_later_user_answers() {
    // E03: a retry replays the original request, so answers accepted
    // after admission must not turn the legitimate resume into a
    // conflict; the seed still carries them for the resumed engine.
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let contract = contract::Contract::load(generators.join("ask-user")).expect("contract loads");
    let runs = temp_run_dir("adopt-later-answers");
    let _ = std::fs::remove_dir_all(&runs);
    let run_dir = runs.join("resume-1");
    let meta = crate::summaries::run_meta_dir(&run_dir);
    std::fs::create_dir_all(&meta).expect("meta");
    let queued = json!({
        "t": "run_queued",
        "ts": "2026-01-01T00:00:00Z",
        "seq": 1,
        "run_id": "resume-1",
        "trace_id": api::trace_id_for_run("resume-1"),
        "span_id": api::span_id_for_seq(1),
        "generator": "ask-user@0.1.0",
        "generator_path": generators.join("ask-user").as_str(),
        "contract_sha256": contract.sha256,
        "inputs": {},
        "answers": {},
        "confirmations": {},

        "schema_version": api::JOURNAL_SCHEMA_VERSION,
        "retention_days": 0,
        "priority": 0,
        "parent_run_id": null,
        "effective_max_total_steps": 64,
        "effective_policy_origin": "unit-test",
    });
    let answered = json!({
        "t": "user_answered",
        "ts": "2026-01-01T00:00:01Z",
        "seq": 2,
        "run_id": "resume-1",
        "trace_id": api::trace_id_for_run("resume-1"),
        "span_id": api::span_id_for_seq(2),
        "question_id": "q1",
        "values": {"answer": "yes"},
    });
    std::fs::write(
        meta.join("journal.jsonl"),
        format!("{}\n{}\n", queued, answered),
    )
    .expect("journal");
    let empty = BTreeMap::new();
    let snapshot_events =
        crate::summaries::read_journal_events(&run_dir).expect("journal should read");
    let snapshot_state =
        engine::RunState::fold_values(&snapshot_events).expect("journal should fold");
    let snapshot = crate::run_dirs::AdoptSnapshot {
        adopted: true,
        events: snapshot_events,
        state: Some(snapshot_state),
    };
    let seed = crate::runs_api::seed_adopted_run_from_snapshot(
        &snapshot,
        &crate::runs_api::AdoptedRunIdentity {
            inputs: &empty,
            contract_sha256: &contract.sha256,
            priority: 0,
            parent: None,
            answers: &empty,
            confirmations: &BTreeMap::new(),
        },
    )
    .expect("a legitimate resume must seed")
    .expect("the run is not terminal");
    assert_eq!(
        seed.answers.get("q1"),
        Some(&json!({"answer": "yes"})),
        "the seed must carry the accepted answer for resume"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn adopting_a_settled_run_converges_without_reexecution() {
    // E03: adopting a terminal run must not erase its result or start a
    // second execution.
    let root = temp_run_dir("adopt-terminal-run");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "adopt-terminal"
name = "Adopt Terminal"
version = "0.1.0"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "done""#,
    )
    .expect("generator manifest should be written");
    let service = test_service(vec![root], runs);
    let request = || StartRun {
        generator_id: "generator".into(),
        inputs: BTreeMap::new(),
        ..Default::default()
    };
    let reserved = "generator-terminal-1".to_string();
    let id = service
        .start_run_with_id(request(), Some(reserved.clone()))
        .await
        .expect("first start should win");
    wait_for_terminal_snapshot(&service, &id).await;
    service.inner.runs.write().await.remove(&id);
    let adopted = service
        .start_run_with_id(request(), Some(reserved))
        .await
        .expect("terminal adoption should converge");
    assert_eq!(adopted, id);
    let snapshot = service.snapshot(id.clone()).await.expect("snapshot");
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
        .expect("journal should be readable");
    assert_eq!(
        journal
            .lines()
            .filter(|line| line.contains("\"t\":\"run_queued\""))
            .count(),
        1,
        "adoption must not append a second admission"
    );
    assert!(run_workspace_dir(&run_dir).join("result.txt").exists());
}

#[tokio::test]
async fn fork_snapshot_carries_parent_link() {
    let runs = temp_run_dir("fork-parent");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let source = start_ask_user_and_answer(&service, "brief").await;
    assert_eq!(
        wait_for_terminal_snapshot(&service, &source).await.state,
        RunStatus::Succeeded
    );
    let checkpoint = read_journal_events(&service.run_dir_for(&source).await.expect("source dir"))
        .expect("source journal")
        .into_iter()
        .filter_map(|event| {
            (event.get("t").and_then(Value::as_str) == Some("step_finished"))
                .then(|| event.get("seq").and_then(Value::as_u64))
                .flatten()
        })
        .max()
        .expect("source should have a finished step");
    let fork = service
        .fork_run(
            &source,
            ForkRun {
                at_seq: checkpoint,
                ..Default::default()
            },
        )
        .await
        .expect("fork should start");
    let snapshot = wait_for_terminal_snapshot(&service, &fork).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    assert_eq!(snapshot.parent_run_id.as_deref(), Some(source.as_str()));
    // Observation history is part of the merged record view, so a fork
    // must carry it too (ADR 0001).
    let fork_dir = service.run_dir_for(&fork).await.expect("fork dir");
    let fork_events =
        crate::summaries::read_events_with_audit(&fork_dir).expect("fork events should read");
    assert!(
        fork_events
            .iter()
            .any(|event| event.get("t").and_then(Value::as_str) == Some("graph_resolved")),
        "a fork must preserve the source observation history: {fork_events:#?}"
    );
    assert!(
        run_meta_dir(&fork_dir).join("audit.jsonl").exists(),
        "observation history lives in the fork audit stream"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn fork_copies_every_referenced_checkpoint_revision() {
    // E06: a path overwritten by a later step keeps both revisions in
    // the fork's blob store, so resume can verify the historical pin.
    use sha2::{Digest as _, Sha256};
    let runs = temp_run_dir("fork-all-revisions");
    let _ = std::fs::remove_dir_all(&runs);
    let source_dir = runs.join("source");
    let source_meta = run_meta_dir(&source_dir);
    std::fs::create_dir_all(source_meta.join("checkpoint-blobs")).expect("source meta");
    std::fs::create_dir_all(run_workspace_dir(&source_dir)).expect("source workspace");
    let v1 = b"first revision";
    let v2 = b"second revision";
    let failed = b"failed revision";
    let sha = |bytes: &[u8]| hex::encode(Sha256::digest(bytes));
    std::fs::write(source_meta.join("checkpoint-blobs").join(sha(v1)), v1).expect("v1 blob");
    std::fs::write(source_meta.join("checkpoint-blobs").join(sha(v2)), v2).expect("v2 blob");
    std::fs::write(
        source_meta.join("checkpoint-blobs").join(sha(failed)),
        failed,
    )
    .expect("failed blob");
    std::fs::write(run_workspace_dir(&source_dir).join("out.txt"), v2).expect("workspace v2");
    let events = [
        json!({
            "t": "run_queued",
            "ts": "2026-01-01T00:00:00Z",
            "seq": 1,
            "run_id": "source",
            "generator": "g@1.0.0",
            "generator_path": "/tmp/g",
            "contract_sha256": "sha",
            "inputs": {},
            "answers": {},
            "confirmations": {},

            "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "retention_days": 0,
            "priority": 0,
            "parent_run_id": null,
            "effective_max_total_steps": 64,
            "effective_policy_origin": "unit-test",
        }),
        json!({
            "t": "step_finished",
            "ts": "2026-01-01T00:00:01Z",
            "seq": 2,
            "run_id": "source",
            "node": "a",
            "status": "success",
            "files": [{"path": "out.txt", "sha256": sha(v1)}],
        }),
        json!({
            "t": "step_finished",
            "ts": "2026-01-01T00:00:02Z",
            "seq": 3,
            "run_id": "source",
            "node": "b",
            "status": "success",
            "files": [{"path": "out.txt", "sha256": sha(v2)}],
        }),
        json!({
            "t": "step_finished",
            "ts": "2026-01-01T00:00:03Z",
            "seq": 4,
            "run_id": "source",
            "node": "c",
            "status": "check_failed",
            "reason": {"code": "check_failed", "message": "late failure"},
            "files": [{"path": "out.txt", "sha256": sha(failed)}],
        }),
    ];
    let mut journal = String::new();
    for event in &events {
        journal.push_str(&event.to_string());
        journal.push('\n');
    }
    std::fs::write(source_meta.join("journal.jsonl"), journal).expect("source journal");
    let fork_id = "g-fork-1".to_string();
    let fork_dir = runs.join(&fork_id);
    crate::run_dirs::prepare_checkpoint_fork(
        &source_dir,
        "source",
        &fork_dir,
        &fork_id,
        4,
        &ForkStatePatch::default(),
    )
    .expect("checkpoint copy should succeed");
    let fork_blobs = run_meta_dir(&fork_dir).join("checkpoint-blobs");
    for digest in [sha(v1), sha(v2), sha(failed)] {
        assert!(
            fork_blobs.join(&digest).is_file(),
            "fork must carry blob {digest}"
        );
    }
    assert_eq!(
        std::fs::read(run_workspace_dir(&fork_dir).join("out.txt")).expect("fork workspace"),
        v2,
        "only a successful step projects the fork workspace revision"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn fork_reverse_name_order_projects_journal_later_revision() {
    // E06: `z_first` writes v1 and `a_second` overwrites with v2.
    // Dictionary order would project v1; the fork must project the
    // journal-later v2 with identical workspace bytes.
    use sha2::{Digest as _, Sha256};
    let runs = temp_run_dir("fork-reverse-order");
    let _ = std::fs::remove_dir_all(&runs);
    let _temp_guard = TempGuard(runs.clone());
    let source_dir = runs.join("source");
    let source_meta = run_meta_dir(&source_dir);
    std::fs::create_dir_all(source_meta.join("checkpoint-blobs")).expect("source meta");
    std::fs::create_dir_all(run_workspace_dir(&source_dir)).expect("source workspace");
    let v1 = b"first revision";
    let v2 = b"second revision";
    let sha = |bytes: &[u8]| hex::encode(Sha256::digest(bytes));
    std::fs::write(source_meta.join("checkpoint-blobs").join(sha(v1)), v1).expect("v1 blob");
    std::fs::write(source_meta.join("checkpoint-blobs").join(sha(v2)), v2).expect("v2 blob");
    std::fs::write(run_workspace_dir(&source_dir).join("out.txt"), v2).expect("workspace v2");
    let events = [
        json!({
            "t": "run_queued",
            "ts": "2026-01-01T00:00:00Z",
            "seq": 1,
            "run_id": "source",
            "generator": "g@1.0.0",
            "generator_path": "/tmp/g",
            "contract_sha256": "sha",
            "inputs": {},
            "answers": {},
            "confirmations": {},

            "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "retention_days": 0,
            "priority": 0,
            "parent_run_id": null,
            "effective_max_total_steps": 64,
            "effective_policy_origin": "unit-test",
        }),
        json!({
            "t": "step_finished",
            "ts": "2026-01-01T00:00:01Z",
            "seq": 2,
            "run_id": "source",
            "node": "z_first",
            "status": "success",
            "files": [{"path": "out.txt", "sha256": sha(v1)}],
        }),
        json!({
            "t": "step_finished",
            "ts": "2026-01-01T00:00:02Z",
            "seq": 3,
            "run_id": "source",
            "node": "a_second",
            "status": "success",
            "files": [{"path": "out.txt", "sha256": sha(v2)}],
        }),
    ];
    let mut journal = String::new();
    for event in &events {
        journal.push_str(&event.to_string());
        journal.push('\n');
    }
    std::fs::write(source_meta.join("journal.jsonl"), journal).expect("source journal");
    let fork_id = "g-fork-rev-1".to_string();
    let fork_dir = runs.join(&fork_id);
    crate::run_dirs::prepare_checkpoint_fork(
        &source_dir,
        "source",
        &fork_dir,
        &fork_id,
        3,
        &ForkStatePatch::default(),
    )
    .expect("checkpoint copy should succeed");
    assert_eq!(
        std::fs::read(run_workspace_dir(&fork_dir).join("out.txt")).expect("fork workspace"),
        v2,
        "the fork must project the journal-later revision despite reverse name order"
    );
}

#[tokio::test]
async fn partial_fork_is_not_mistaken_for_completed_admission() {
    // A03: a crash between checkpoint copy and the fork's own
    // `run_queued` leaves a journal whose (rewritten) source
    // `run_queued` predates `run_forked`. Adoption must wipe it for a
    // deterministic redo instead of resuming the partial fork.
    let runs = temp_run_dir("fork-partial-adopt");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let source = start_ask_user_and_answer(&service, "brief").await;
    assert_eq!(
        wait_for_terminal_snapshot(&service, &source).await.state,
        RunStatus::Succeeded
    );
    let source_dir = service
        .run_dir_for(&source)
        .await
        .expect("source dir should resolve");
    let checkpoint = read_journal_events(&source_dir)
        .expect("source journal")
        .into_iter()
        .filter_map(|event| {
            (event.get("t").and_then(Value::as_str) == Some("step_finished"))
                .then(|| event.get("seq").and_then(Value::as_u64))
                .flatten()
        })
        .max()
        .expect("source should have a finished step");
    let fork_id = format!("ask-user-fork-{}", uuid::Uuid::now_v7());
    let fork_dir = runs.join(&fork_id);
    // Simulate the crash window: checkpoint copy done, own `run_queued`
    // never appended.
    crate::run_dirs::prepare_checkpoint_fork(
        &source_dir,
        &source,
        &fork_dir,
        &fork_id,
        checkpoint,
        &ForkStatePatch::default(),
    )
    .expect("checkpoint copy should succeed");
    assert!(
        !crate::run_dirs::try_adopt_run_dir_with_snapshot(&fork_dir, &fork_id)
            .expect("adoption check should succeed")
            .adopted,
        "partial fork must not adopt"
    );
    assert!(
        !fork_dir.exists(),
        "partial fork must be wiped for a deterministic redo"
    );
    // The same reserved id now converges through a fresh fork.
    let fork = service
        .fork_run_with_id(
            &source,
            ForkRun {
                at_seq: checkpoint,
                ..Default::default()
            },
            Some(fork_id.clone()),
        )
        .await
        .expect("fork redo should start");
    assert_eq!(fork, fork_id);
    let snapshot = wait_for_terminal_snapshot(&service, &fork).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    assert_eq!(snapshot.parent_run_id.as_deref(), Some(source.as_str()));
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn checkpoint_fork_restores_files_and_resumes_with_an_explicit_state_patch() {
    let root = temp_run_dir("checkpoint-fork-generators");
    let runs = temp_run_dir("checkpoint-fork-runs");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&runs);
    let generator = root.join("generator");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join(contract::MANIFEST_FILE),
        r#"
[generator]
id = "generator"
name = "generator"
version = "0.1.0"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
[resources.docs]
type = "dir"
path = "docs"
llm_visible = true

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "marker"
required = true
type = "string"

[[flow]]
id = "before"
type = "write"
artifact = { label = "Before", required = true }
[flow.params]
output_file = "before.txt"
content = "{{ inputs.marker }}"

[[flow]]
id = "after"
type = "write"
needs = ["before"]
artifact = { label = "After", required = true }
[flow.params]
output_file = "after.txt"
content = "{{ inputs.marker }}""#,
    )
    .expect("generator manifest should be written");
    std::fs::create_dir_all(generator.join("docs/nested"))
        .expect("directory resource should be created");
    std::fs::write(generator.join("docs/guide.md"), "guide")
        .expect("directory resource file should be written");
    std::fs::write(generator.join("docs/nested/reference.md"), "reference")
        .expect("nested directory resource file should be written");
    let service = test_service(vec![root], runs);
    let source_id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::from([("marker".into(), json!("source"))]),
            ..Default::default()
        })
        .await
        .expect("source run should start");
    assert_eq!(
        wait_for_terminal_snapshot(&service, &source_id).await.state,
        RunStatus::Succeeded
    );
    let source_dir = service
        .run_dir_for(&source_id)
        .await
        .expect("source run directory");
    let checkpoint_seq = read_journal_events(&source_dir)
        .expect("source journal")
        .into_iter()
        .find(|event| {
            event.get("t").and_then(Value::as_str) == Some("step_finished")
                && event.get("node").and_then(Value::as_str) == Some("before")
        })
        .and_then(|event| event.get("seq").and_then(Value::as_u64))
        .expect("before checkpoint sequence");
    let fork_id = service
        .fork_run(
            &source_id,
            ForkRun {
                at_seq: checkpoint_seq,
                state_patch: ForkStatePatch {
                    inputs: BTreeMap::from([("marker".into(), json!("fork"))]),
                    ..ForkStatePatch::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("checkpoint fork should start");
    assert_eq!(
        wait_for_terminal_snapshot(&service, &fork_id).await.state,
        RunStatus::Succeeded
    );
    let fork_dir = service
        .run_dir_for(&fork_id)
        .await
        .expect("fork run directory");
    assert_eq!(
        std::fs::read_to_string(run_workspace_dir(&fork_dir).join("before.txt"))
            .expect("restored checkpoint file"),
        "source"
    );
    assert_eq!(
        std::fs::read_to_string(run_workspace_dir(&fork_dir).join("after.txt"))
            .expect("resumed output file"),
        "fork"
    );
    let journal = read_journal_events(&fork_dir).expect("fork journal");
    assert!(journal.iter().any(|event| {
        event.get("t").and_then(Value::as_str) == Some("resource")
            && event.get("name").and_then(Value::as_str) == Some("docs")
            && event
                .get("files")
                .and_then(Value::as_array)
                .is_some_and(|files| files.len() == 2)
    }));
    assert!(journal.iter().any(|event| {
        event.get("t").and_then(Value::as_str) == Some("run_forked")
            && event.get("source_run_id").and_then(Value::as_str) == Some(source_id.as_str())
            && event.get("source_seq").and_then(Value::as_u64) == Some(checkpoint_seq)
    }));
    assert!(run_meta_dir(&source_dir).join("checkpoint-blobs").is_dir());
}

#[test]
fn fork_adoption_ignores_source_admission_without_a_marker() {
    // E03: without a `run_forked` marker only the fork's own
    // `run_queued` counts. The source's admission answers must not
    // judge the fork: seeding with the fork's own values succeeds even
    // though the source was admitted differently.
    let root = temp_run_dir("fork-no-marker");
    let _ = std::fs::remove_dir_all(&root);
    let _temp_guard = TempGuard(root.clone());
    let run_dir = root.join("g-fork-1");
    let meta = run_meta_dir(&run_dir);
    std::fs::create_dir_all(&meta).expect("meta should be created");
    let event = |seq: u64, run_id: &str, answers: serde_json::Value| {
        serde_json::json!({
            "t": "run_queued",
            "ts": "2026-01-01T00:00:00Z",
            "seq": seq,
            "run_id": run_id,
            "trace_id": format!("trace-{run_id}"),
            "span_id": format!("span-{seq}"),
            "generator": "g@1.0.0",
            "generator_path": "/tmp/g",
            "contract_sha256": "sha",
            "inputs": {},
            "answers": answers,
            "confirmations": {},

            "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "retention_days": 0,
            "priority": 0,
            "parent_run_id": null,
            "effective_max_total_steps": 64,
            "effective_policy_origin": "unit-test",
        })
    };
    let mut fork_event = event(2, "g-fork-1", serde_json::json!({}));
    fork_event["parent_run_id"] = serde_json::json!("source");
    let journal = [
        event(1, "source", serde_json::json!({"stale": "answer"})),
        fork_event,
    ];
    let mut text = String::new();
    for event in &journal {
        text.push_str(&event.to_string());
        text.push('\n');
    }
    std::fs::write(meta.join("journal.jsonl"), text).expect("journal should be written");
    // Single-read: one snapshot serves both seed derivations below.
    let snapshot_events =
        crate::summaries::read_journal_events(&run_dir).expect("journal should read");
    let snapshot_state =
        engine::RunState::fold_values(&snapshot_events).expect("journal should fold");
    let snapshot = crate::run_dirs::AdoptSnapshot {
        adopted: true,
        events: snapshot_events,
        state: Some(snapshot_state),
    };
    let seed_with = |answers: &BTreeMap<String, Value>| {
        let inputs = BTreeMap::new();
        let confirmations = BTreeMap::new();
        crate::runs_api::seed_adopted_run_from_snapshot(
            &snapshot,
            &crate::runs_api::AdoptedRunIdentity {
                inputs: &inputs,
                contract_sha256: "sha",
                priority: 0,
                parent: Some("source"),
                answers,
                confirmations: &confirmations,
            },
        )
    };
    let empty = BTreeMap::new();
    seed_with(&empty)
        .expect("seeding with the fork's own values should succeed")
        .expect("the run is not terminal");
    // The source's admission answers must not satisfy the fork.
    let foreign = BTreeMap::from([("foreign".to_string(), serde_json::json!("answer"))]);
    let error = seed_with(&foreign).expect_err("foreign answers must not seed the fork");
    assert!(
        error.to_string().contains("different answers"),
        "the refusal must name the mismatch: {error}"
    );
}

#[test]
fn fork_policy_prefers_own_admission_over_source() {
    // E03/E04: a fork journal carries the source `run_queued` ahead of
    // `run_forked`, then the fork's own `run_queued`. Policy resolution
    // must use the fork's own ceiling, never the source's.
    let policy = crate::types::ResolvedExecutionPolicy::for_execution(
        &[
            json!({
                "t": "run_queued",
                "run_id": "fork-1",
                "effective_max_total_steps": 10,
            }),
            json!({"t": "run_forked", "run_id": "fork-1"}),
            json!({
                "t": "run_queued",
                "run_id": "fork-1",
                "effective_max_total_steps": 25,
            }),
        ],
        None,
    )
    .expect("fork policy should resolve");
    assert_eq!(
        policy.max_total_steps, 25,
        "the fork's own admission must win over the source copy"
    );
    let start = crate::types::ResolvedExecutionPolicy::for_execution(
        &[json!({
            "t": "run_queued",
            "run_id": "run-1",
            "effective_max_total_steps": 11,
        })],
        None,
    )
    .expect("start policy should resolve");
    assert_eq!(start.max_total_steps, 11);
}

#[tokio::test]
async fn fork_snapshot_derivations_work_after_journal_removal() {
    // E03: a fresh fork threads its single admission snapshot through to
    // the spawn, so the fork pays exactly one journal read. Proof by
    // deletion (same shape as the adopt single-read test): the single
    // read serves fork inputs, the seed, and the spawn snapshot, and
    // every downstream derivation is pure over the snapshot with zero
    // further journal I/O.
    let root = temp_run_dir("fork-single-read");
    let _guard = TempGuard(root.clone());
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service = LocalService::with_generator_roots_policy_and_store_mode(
        vec![generators],
        runs.clone(),
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let source = start_ask_user_and_answer(&service, "brief").await;
    assert_eq!(
        wait_for_terminal_snapshot(&service, &source).await.state,
        RunStatus::Succeeded
    );
    let checkpoint = read_journal_events(&service.run_dir_for(&source).await.expect("source dir"))
        .expect("source journal")
        .into_iter()
        .filter_map(|event| {
            (event.get("t").and_then(Value::as_str) == Some("step_finished"))
                .then(|| event.get("seq").and_then(Value::as_u64))
                .flatten()
        })
        .max()
        .expect("source should have a finished step");
    let fork = service
        .fork_run(
            &source,
            ForkRun {
                at_seq: checkpoint,
                ..Default::default()
            },
        )
        .await
        .expect("fork should admit");
    let fork_dir = service
        .run_dir_for(&fork)
        .await
        .expect("fork directory should resolve");
    // The single admission read: fork inputs, the seed, and the spawn
    // snapshot all derive from this one snapshot with no second scan.
    let snapshot = crate::run_dirs::try_adopt_run_dir_with_snapshot(&fork_dir, &fork)
        .expect("fork adoption should succeed");
    assert!(snapshot.adopted, "the fork admission should adopt");
    assert!(
        !snapshot.events.is_empty(),
        "the snapshot should carry events"
    );
    let events_before = snapshot.events.clone();
    std::fs::remove_dir_all(&fork_dir).expect("fork directory should be removable");
    assert!(
        !fork_dir.exists(),
        "the journal must be gone for this proof"
    );
    let contract = service
        .inner
        .runs
        .read()
        .await
        .get(&fork)
        .map(|record| record.contract.clone())
        .expect("fork record should exist");
    let inputs = crate::runs_api::fork_checkpoint_inputs(&snapshot, &contract, &source, checkpoint)
        .expect("fork inputs should derive without the journal");
    assert_eq!(
        inputs,
        snapshot
            .state
            .as_ref()
            .unwrap()
            .inputs
            .clone()
            .unwrap_or_default()
    );
    let policy = crate::types::ResolvedExecutionPolicy::for_execution(&snapshot.events, None)
        .expect("policy should resolve without the journal");
    assert!(
        policy.max_total_steps > 0,
        "policy must resolve from the snapshot"
    );
    assert_eq!(
        snapshot.events, events_before,
        "derivations must not mutate the snapshot"
    );
    // Structural pin: the fresh-fork spawn must carry a snapshot
    // (`Some`), never `None` (which would force a second disk read).
    let admission = include_str!("../runs_api/admission.rs");
    assert!(
        admission.contains("journal_snapshot: Some(spawn_snapshot)"),
        "fresh forks must thread the single snapshot to the spawn"
    );
}

#[tokio::test]
async fn waiting_run_survives_gc_and_rehydrates_after_restart() {
    let runs = temp_run_dir("waiting-rehydrate");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question = snapshot
        .question
        .expect("run should have a pending question");
    assert!(
        gc_run_directories(
            &runs,
            0,
            0,
            true,
            policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES
        )
        .expect("GC should inspect runs")
        .is_empty(),
        "GC must not delete a waiting run"
    );
    assert!(runs.join(&id).is_dir());
    drop(service);

    let restored = test_service(vec![generators], runs);
    let snapshot = restored
        .snapshot(id.clone())
        .await
        .expect("rehydrated snapshot should exist");
    assert_eq!(snapshot.state, RunStatus::Waiting);
    assert_eq!(
        snapshot.question.as_ref().map(|item| &item.id),
        Some(&question.id)
    );
    restored
        .answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("brief"))]),
            },
        )
        .await
        .expect("rehydrated run should accept its answer");
    let snapshot = wait_for_terminal_snapshot(&restored, &id).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
}

#[tokio::test]
async fn a_rebuilt_service_rehydrates_a_waiting_run_after_shutdown() {
    // E05: shutdown must leave durable state that a fresh process on the
    // same runs directory can rebuild and continue.
    let root = temp_run_dir("shutdown-rebuild");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service = test_service(vec![generators.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("interactive run should start");
    let question = wait_for_snapshot(&service, &id, RunStatus::Waiting)
        .await
        .question
        .expect("run should have a pending question");
    service.mark_shutting_down();
    assert_eq!(
        service
            .snapshot(id.clone())
            .await
            .expect("settled snapshot")
            .state,
        RunStatus::Waiting,
        "shutdown must not rewrite a still-durable waiting run"
    );
    drop(service);

    let rebuilt = test_service(vec![generators], runs);
    let snapshot = rebuilt
        .snapshot(id.clone())
        .await
        .expect("rebuilt snapshot should exist");
    assert_eq!(snapshot.state, RunStatus::Waiting);
    assert_eq!(
        snapshot.question.as_ref().map(|item| &item.id),
        Some(&question.id)
    );
    rebuilt
        .answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("brief"))]),
            },
        )
        .await
        .expect("rebuilt run should accept its answer");
    let snapshot = wait_for_terminal_snapshot(&rebuilt, &id).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    drop(rebuilt);
    let _ = std::fs::remove_dir_all(&root);
}
