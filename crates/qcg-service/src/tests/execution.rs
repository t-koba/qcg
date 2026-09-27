use super::support::*;
use crate::*;
use camino::Utf8PathBuf;
use qcg_api::ConfirmDecision;
use qcg_api::ConfirmationDecision;
use qcg_api::{AnswerPayload, RunStatus, StartRun};
use qcg_policy::DEFAULT_MAX_TRACKED_RUNS;
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[cfg(unix)]
#[tokio::test]
async fn resume_after_confirmation_replays_prior_steps_and_runs_side_effect_once() {
    let service = test_service(
        vec![Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators")],
        temp_run_dir("confirmation-resume"),
    );
    let id = service
        .start_run(StartRun {
            generator_id: "side-effect-confirm".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let snapshot = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
    let confirm = snapshot.confirm.expect("run should request confirmation");
    service
        .confirm(
            id.clone(),
            confirm.id.clone(),
            ConfirmDecision {
                decision: ConfirmationDecision::Approve,
            },
        )
        .await
        .expect("confirmation should resume run");
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    let journal = read_journal_string(&service, id).await;
    let approved_effects = journal
        .lines()
        .filter(|line| {
            line.contains("\"t\":\"side_effect\"")
                && line.contains("\"node\":\"effect\"")
                && line.contains("\"decision\":\"approved_by_user\"")
        })
        .count();
    assert_eq!(
        approved_effects, 1,
        "confirmed side effect must execute once"
    );
    assert!(!confirm.id.is_empty());
}

#[tokio::test]
async fn foreach_items_accepts_expressions() {
    let root = temp_run_dir("foreach-expression");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator dir should be created");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[blocks.site]]
id = "write_site"
type = "write"

[blocks.site.params]
content = "site={{ item }}"
output_file = "sites/{{ item }}.txt"

[[flow]]
id = "emit_sites"
type = "foreach"

[flow.params]
items = "reverse(inputs.sites)"
max_iterations = 10
subflow = "site"
parallel = 1

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "sites"
item_type = "string"
required = true
type = "list"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
    )
    .expect("generator manifest should be written");
    let service = test_service(vec![root.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::from([("sites".into(), json!(["a", "b"]))]),
            ..Default::default()
        })
        .await
        .expect("expression foreach should start");
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    let workspace = run_workspace_dir(&runs.join(&id));
    assert_eq!(
        std::fs::read_to_string(workspace.join("sites/a.txt")).expect("site a should render"),
        "site=a"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("sites/b.txt")).expect("site b should render"),
        "site=b"
    );
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn agent_ask_user_questions_are_scoped_per_call() {
    // E08: the same ask_user tool asked twice yields two question
    // identities; the first answer must not satisfy the second
    // question.
    let root = temp_run_dir("agent-ask-twice");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(generator.join("prompts")).expect("generator dirs");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "agent"
output = "agent_result"
type = "llm.agent"

[flow.params]
max_iterations = 4
max_tokens_total = 4096
prompt = "prompts/agent.j2"

[[flow.params.tools]]
kind = "ask_user"
name = "ask_mode"

[llm]
max_tokens = 256
temperature = 0.0

[llm.model]
model = "fake"
provider = "fake"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
    )
    .expect("generator manifest");
    std::fs::write(
            generator.join("prompts/agent.j2"),
            r#"Ask two questions, then finish.

FAKE_TOOL_SEQUENCE: [{"name":"ask_mode","args":{"question":"City?","options":["tokyo","osaka"]}},{"name":"ask_mode","args":{"question":"Postal code?","options":["100","200"]}}]
FAKE_AGENT_FINAL:
done
"#,
        )
        .expect("prompt");
    let service = test_service(vec![root.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("agent run should start");
    let first = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question1 = first.question.expect("first question");
    service
        .answer(
            id.clone(),
            question1.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("tokyo"))]),
            },
        )
        .await
        .expect("first answer");
    let mut question2 = None;
    for _ in 0..400 {
        let snapshot = service.snapshot(id.clone()).await.expect("snapshot");
        if snapshot.state.is_terminal() {
            break;
        }
        if let Some(question) = snapshot.question
            && question.id != question1.id
        {
            question2 = Some(question);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let question2 = question2.expect("a distinct second question must be asked");
    // A late answer addressed to the first question must not complete
    // the second one (E08).
    let late = service
        .answer(
            id.clone(),
            question1.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("osaka"))]),
            },
        )
        .await;
    assert!(
        late.is_err(),
        "a late answer for the first question must be rejected"
    );
    service
        .answer(
            id.clone(),
            question2.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("100"))]),
            },
        )
        .await
        .expect("second answer");
    let terminal = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(terminal.state, RunStatus::Succeeded);
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    let events = read_events_from_meta(&run_meta_dir(&run_dir)).expect("journal");
    let answered: Vec<&serde_json::Value> = events
        .iter()
        .filter(|event| event.get("t").and_then(serde_json::Value::as_str) == Some("user_answered"))
        .collect();
    assert_eq!(answered.len(), 2, "both questions must be answered");
    assert_ne!(
        answered[0].get("question_id"),
        answered[1].get("question_id"),
        "each call must have its own question identity"
    );
    assert_eq!(answered[0]["values"]["answer"], json!("tokyo"));
    assert_eq!(answered[1]["values"]["answer"], json!("100"));
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn resume_does_not_restore_updated_file_inputs() {
    // E06: a completed step may legitimately update a file input; the
    // resume must not overwrite it with the original upload bytes.
    let root = temp_run_dir("resume-file-input");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator dir");
    std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "update"
type = "command"

[flow.params]
command = ["sh", "-c", "echo updated > files/note/note.txt; printf '%s' '{\"status\":\"success\",\"output\":null,\"files\":[\"files/note/note.txt\"],\"findings\":[]}'"]
result = "structured"

[[flow]]
id = "ask"
type = "ask_user"
needs = ["update"]

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
fs_write = []
network = []
side_effects_scope = "invocation"
fs_read = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "echo updated > files/note/note.txt; printf '%s' '{\"status\":\"success\",\"output\":null,\"files\":[\"files/note/note.txt\"],\"findings\":[]}'"], purpose = "update input", isolation = "trusted_host" }]


[permissions.containers]
enabled = false
[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "note"
required = true
type = "file""#,
        )
        .expect("generator manifest");
    let service = test_service(vec![root.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::from([(
                "note".into(),
                json!({ "name": "note.txt", "text": "original" }),
            )]),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let waiting = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question = waiting.question.expect("question");
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    assert_eq!(
        std::fs::read_to_string(run_workspace_dir(&run_dir).join("files/note/note.txt"))
            .expect("input should exist"),
        "updated\n"
    );
    service
        .answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("yes"))]),
            },
        )
        .await
        .expect("answer");
    let terminal = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(terminal.state, RunStatus::Succeeded);
    assert_eq!(
        std::fs::read_to_string(run_workspace_dir(&run_dir).join("files/note/note.txt"))
            .expect("input should survive resume"),
        "updated\n",
        "resume must not roll the input back to the upload bytes"
    );
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn agent_unanswered_question_id_survives_restart() {
    // E08: restarting while an agent AskUser question is pending must
    // re-issue the stored call with the same question identity, without
    // depending on the model regenerating an identical call.
    let root = temp_run_dir("agent-ask-restart");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(generator.join("prompts")).expect("generator dirs");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "agent"
output = "agent_result"
type = "llm.agent"

[flow.params]
max_iterations = 3
max_tokens_total = 4096
prompt = "prompts/agent.j2"

[[flow.params.tools]]
kind = "ask_user"
name = "ask_mode"

[llm]
max_tokens = 256
temperature = 0.0

[llm.model]
model = "fake"
provider = "fake"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
    )
    .expect("generator manifest");
    std::fs::write(
            generator.join("prompts/agent.j2"),
            r#"Ask once, then finish.

FAKE_TOOL_SEQUENCE: [{"name":"ask_mode","args":{"question":"Choose mode","options":["brief","detailed"]}}]
FAKE_AGENT_FINAL:
done
"#,
        )
        .expect("prompt");
    let service = test_service(vec![root.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("agent run should start");
    let first = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question_id = first.question.expect("pending question").id;
    drop(service);
    // Rehydrate the same store; the pending call is re-issued verbatim.
    let restored = loop {
        match try_service(vec![root.clone()], runs.clone()) {
            Ok(service) => break service,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    };
    let second = wait_for_snapshot(&restored, &id, RunStatus::Waiting).await;
    assert_eq!(
        second
            .question
            .as_ref()
            .map(|question| question.id.as_str()),
        Some(question_id.as_str()),
        "the restarted run must present the same question identity"
    );
    restored
        .answer(
            id.clone(),
            question_id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("brief"))]),
            },
        )
        .await
        .expect("answer after restart");
    let terminal = wait_for_terminal_snapshot(&restored, &id).await;
    assert_eq!(terminal.state, RunStatus::Succeeded);
    drop(restored);
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(unix)]
#[tokio::test]
async fn priority_preempts_and_resumes_in_order() {
    let root = temp_run_dir("priority-preemption");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator dir should be created");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "nap"
type = "command"

[flow.params]
command = ["sleep", "60"]

[permissions]
fs_read = []
network = []
side_effects_scope = "invocation"
fs_write = ["workspace"]
side_effects = "allowed"

[[permissions.commands]]
bin = "sleep"
args = ["60"]
purpose = "priority scheduling test"
isolation = "trusted_host"


[permissions.containers]
enabled = false
[runtime]
command_timeout_seconds = 300"#,
    )
    .expect("generator manifest should be written");
    let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![root.clone()],
        runs.clone(),
        None,
        1,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let start_prioritized = async |service: &LocalQcgService, priority: i32| {
        service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                priority: Some(priority),
                ..Default::default()
            })
            .await
            .expect("prioritized run should start")
    };
    let running = start_prioritized(&service, 0).await;
    assert_eq!(
        wait_for_snapshot(&service, &running, RunStatus::Running)
            .await
            .state,
        RunStatus::Running
    );
    let queued_equal = start_prioritized(&service, 0).await;
    assert_eq!(
        wait_for_snapshot(&service, &queued_equal, RunStatus::Queued)
            .await
            .state,
        RunStatus::Queued
    );
    // A higher-priority arrival preempts the running run instead of
    // waiting behind it.
    let urgent = start_prioritized(&service, 5).await;
    assert_eq!(
        wait_for_snapshot(&service, &urgent, RunStatus::Running)
            .await
            .state,
        RunStatus::Running
    );
    assert_eq!(
        wait_for_snapshot(&service, &running, RunStatus::Queued)
            .await
            .state,
        RunStatus::Queued
    );
    let snapshot = service
        .snapshot(queued_equal.clone())
        .await
        .expect("queued snapshot should load");
    assert_eq!(snapshot.queue_position, Some(1));
    let snapshot = service
        .snapshot(running.clone())
        .await
        .expect("preempted snapshot should load");
    assert_eq!(snapshot.queue_position, Some(2));
    // The preempted writer must have exited before the requeue append:
    // seqs stay unique and monotonic across the handoff.
    let journal = read_journal_string(&service, running.clone()).await;
    let seqs: Vec<u64> = journal
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line)
                .expect("journal line should be JSON")
                .get("seq")
                .and_then(Value::as_u64)
                .expect("event should carry seq")
        })
        .collect();
    assert!(!seqs.is_empty());
    let mut deduped = seqs.clone();
    deduped.sort_unstable();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        seqs.len(),
        "preempted journal seqs must be unique across the writer handoff"
    );
    assert!(
        seqs.windows(2).all(|pair| pair[0] < pair[1]),
        "preempted journal seqs must stay monotonic"
    );
    // Finishing the urgent run resumes equals in FIFO order.
    service
        .cancel(urgent.clone())
        .await
        .expect("urgent run should cancel");
    assert_eq!(
        wait_for_snapshot(&service, &queued_equal, RunStatus::Running)
            .await
            .state,
        RunStatus::Running
    );
    service
        .cancel(queued_equal.clone())
        .await
        .expect("queued run should cancel");
    assert_eq!(
        wait_for_snapshot(&service, &running, RunStatus::Running)
            .await
            .state,
        RunStatus::Running
    );
    service
        .cancel(running.clone())
        .await
        .expect("preempted run should cancel");
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn await_node_joins_sibling_runs() {
    let root = temp_run_dir("await-join");
    let _ = std::fs::remove_dir_all(&root);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let runs = root.join("runs");
    let service = LocalQcgService::with_generator_roots_policy_and_store_mode(
        vec![root.clone(), generators],
        runs.clone(),
        None,
        8,
        DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
    .expect("service should initialize");
    let child = start_ask_user_and_answer(&service, "brief").await;
    assert_eq!(
        wait_for_terminal_snapshot(&service, &child).await.state,
        RunStatus::Succeeded
    );
    let waiter_root = root.join("waiter");
    std::fs::create_dir_all(&waiter_root).expect("waiter dir should be created");
    std::fs::write(
        waiter_root.join("qcg.toml"),
        format!(
            r#"
[generator]
id = "waiter"
name = "Waiter"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "join_children"
type = "await"

[flow.params]
runs = ["{child}"]
timeout_secs = 30

[[flow]]
id = "done"
type = "write"
needs = ["join_children"]
artifact = {{ label = "Done", required = true }}

[flow.params]
content = "joined"
output_file = "done.txt"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
        ),
    )
    .expect("waiter manifest should be written");
    let waiter = service
        .start_run(StartRun {
            generator_id: "waiter".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("waiter run should start");
    let snapshot = wait_for_terminal_snapshot(&service, &waiter).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    let unknown_root = root.join("waiter-unknown");
    std::fs::create_dir_all(&unknown_root).expect("waiter dir should be created");
    std::fs::write(
        unknown_root.join("qcg.toml"),
        r#"
[generator]
id = "waiter-unknown"
name = "Waiter Unknown"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "join_missing"
type = "await"

[flow.params]
runs = ["does-not-exist"]

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]

[permissions.containers]
enabled = false"#,
    )
    .expect("waiter manifest should be written");
    let unknown = service
        .start_run(StartRun {
            generator_id: "waiter-unknown".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("waiter run should start");
    assert_eq!(
        wait_for_terminal_snapshot(&service, &unknown).await.state,
        RunStatus::Failed
    );
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn delete_run_removes_terminal_runs_and_rejects_active_ones() {
    let runs = temp_run_dir("delete-run");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let finished = start_ask_user_and_answer(&service, "brief").await;
    assert_eq!(
        wait_for_terminal_snapshot(&service, &finished).await.state,
        RunStatus::Succeeded
    );
    let waiting = service
        .start_run(StartRun {
            generator_id: "side-effect-confirm".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("confirming run should start");
    assert_eq!(
        wait_for_snapshot(&service, &waiting, RunStatus::Confirming)
            .await
            .state,
        RunStatus::Confirming
    );
    let conflict = service
        .delete_run(&waiting)
        .await
        .expect_err("active runs must not be deletable");
    assert!(conflict.to_string().contains("still active"));
    service
        .delete_run(&finished)
        .await
        .expect("terminal runs should be deletable");
    assert!(!runs.join(&finished).exists());
    assert!(runs.join(&waiting).exists());
    let missing = service
        .delete_run(&finished)
        .await
        .expect_err("repeated deletes must report not-found");
    assert!(missing.to_string().contains("was not found"));
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn run_bundle_contains_snapshot_journal_and_artifacts() {
    let runs = temp_run_dir("run-bundle");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let id = start_ask_user_and_answer(&service, "brief").await;
    assert_eq!(
        wait_for_terminal_snapshot(&service, &id).await.state,
        RunStatus::Succeeded
    );
    let parts = service
        .run_bundle_parts(&id)
        .await
        .expect("bundle parts should assemble");
    let mut bundle = Vec::new();
    write_run_bundle_stream_with_limits(
        &parts.snapshot,
        &parts.inputs,
        &parts.journal,
        parts.outputs.as_ref(),
        &parts.verified,
        &mut bundle,
        &ArtifactZipLimits::default(),
    )
    .expect("bundle should write");
    let archive =
        zip::ZipArchive::new(std::io::Cursor::new(bundle)).expect("bundle should be a valid zip");
    let mut names = archive.file_names().map(str::to_string).collect::<Vec<_>>();
    names.sort();
    assert!(names.contains(&"snapshot.json".to_string()), "{names:?}");
    assert!(names.contains(&"inputs.json".to_string()), "{names:?}");
    assert!(names.contains(&"journal.jsonl".to_string()), "{names:?}");
    assert!(names.contains(&"outputs.json".to_string()), "{names:?}");
    assert!(
        names.contains(&"artifacts/answer.txt".to_string()),
        "{names:?}"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn api_answers_complete_run_without_interaction() {
    // Admission-time answers keyed by bare node id are never honored
    // (question ids bind the full resolved content, E08f), so this
    // covers the interactive path: the run waits, the returned prompt
    // id is answered, and the run completes with the answered artifact.
    let runs = temp_run_dir("preprovisioned-answers");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let id = start_ask_user_and_answer(&service, "brief").await;
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    assert!(snapshot.question.is_none());
    let workspace = run_workspace_dir(&runs.join(&id));
    assert!(
        std::fs::read_to_string(workspace.join("answer.txt"))
            .expect("answer artifact should exist")
            .contains("mode=brief")
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn digest_bound_confirmation_authorizes_the_exact_operation() {
    let runs = temp_run_dir("preprovisioned-confirmations");
    let _ = std::fs::remove_dir_all(&runs);
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "side-effect-confirm".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    // Only the digest-bound confirmation journaled for this exact
    // operation authorizes it.
    let snapshot = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
    let confirm = snapshot.confirm.expect("run should request confirmation");
    assert!(
        confirm.id.starts_with("effect:command:"),
        "confirmation must bind the operation digest"
    );
    service
        .confirm(
            id.clone(),
            confirm.id.clone(),
            ConfirmDecision {
                decision: ConfirmationDecision::Approve,
            },
        )
        .await
        .expect("confirmation should resume run");
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    assert!(snapshot.confirm.is_none());
    let journal = read_journal_string(&service, id).await;
    assert!(
        journal
            .lines()
            .any(|line| line.contains("\"t\":\"side_effect\"")
                && line.contains("\"node\":\"effect\"")
                && line.contains("\"decision\":\"approved_by_user\"")
                && !line.contains("approved_by_bulk")),
        "approved side effect must execute"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[cfg(unix)]
#[tokio::test]
async fn foreach_questions_and_confirmations_are_scoped_per_iteration() {
    let root = temp_run_dir("foreach-interactions");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "foreach-interactions"
name = "Foreach Interactions"
version = "0.1.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "items"
type = "list"
item_type = "string"
required = true
min_items = 1

[[flow]]
id = "each"
type = "foreach"

[flow.params]
items = "inputs.items"
subflow = "item"
max_iterations = 10
parallel = 1

[[flow]]
id = "done"
type = "write"
artifact = { label = "Done", required = true }

[flow.params]
output_file = "done.txt"
content = "done"

[[blocks.item]]
id = "ask"
type = "ask_user"

[blocks.item.params]
content = "Approve {{ item }}"
options = ["yes"]

[[blocks.item]]
id = "effect"
type = "command"

[blocks.item.params]
command = ["echo", "{{ item }}"]

[permissions]
fs_read = []
network = []
side_effects_scope = "invocation"
fs_write = ["workspace"]
side_effects = "confirm"

[[permissions.commands]]
bin = "echo"
args = ["*"]
purpose = "record each confirmed iteration"
isolation = "trusted_host"

[permissions.containers]
enabled = false"#,
    )
    .expect("generator manifest should be written");
    let service = test_service(vec![root.clone()], runs);
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::from([("items".into(), json!(["alpha", "beta"]))]),
            ..Default::default()
        })
        .await
        .expect("foreach run should start");

    // Per-iteration question ids stay distinct: same node, different
    // resolved content per item, so no iteration can consume another's
    // answer (E08f).
    let mut asked = Vec::new();
    for (index, item) in ["alpha", "beta"].into_iter().enumerate() {
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = snapshot.question.expect("iteration should ask a question");
        // Question ids scope the iteration AND bind the resolved
        // content (E08f): the bare node id is never honored.
        assert!(
            question
                .id
                .starts_with(&format!("each[{index}]/ask:ask_user:")),
            "iteration question id must scope the iteration and bind content, got `{}`",
            question.id
        );
        asked.push(question.id.clone());
        service
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
            .await
            .expect("iteration answer should resume the run");

        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
        let confirm = snapshot
            .confirm
            .expect("iteration should request side-effect confirmation");
        assert!(
            confirm
                .id
                .starts_with(&format!("each[{index}]/effect:command:")),
            "confirm id must bind the operation digest, got `{}`",
            confirm.id
        );
        assert_eq!(confirm.target, format!("echo {item}"));
        assert!(
            !confirm.operation_digest.is_empty(),
            "confirm must carry the operation digest"
        );
        service
            .confirm(
                id.clone(),
                confirm.id,
                ConfirmDecision {
                    decision: ConfirmationDecision::Approve,
                },
            )
            .await
            .expect("iteration confirmation should resume the run");
    }

    asked.sort();
    asked.dedup();
    assert_eq!(
        asked.len(),
        2,
        "each iteration must ask a distinct content-bound question"
    );
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    let journal = read_journal_string(&service, id).await;
    for index in 0..2 {
        let node = format!("\"node\":\"each[{index}]/effect\"");
        assert_eq!(
            journal
                .lines()
                .filter(|line| {
                    line.contains("\"t\":\"side_effect\"")
                        && line.contains(&node)
                        && line.contains("\"decision\":\"approved_by_user\"")
                })
                .count(),
            1,
            "each iteration side effect must be approved exactly once"
        );
    }
}

#[tokio::test]
async fn nested_foreach_restores_parent_item_and_addresses_every_iteration() {
    let root = temp_run_dir("nested-foreach");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator directory should be created");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "nested-foreach"
name = "Nested Foreach"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_read = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"
fs_write = ["workspace"]


[permissions.containers]
enabled = false
[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "outer"
type = "list"
item_type = "string"
required = true
min_items = 1

[[inputs.stages.fields]]
id = "inner"
type = "list"
item_type = "string"
required = true
min_items = 1

[[flow]]
id = "outer"
type = "foreach"

[flow.params]
items = "inputs.outer"
subflow = "outer_body"
max_iterations = 4
parallel = 1

[[blocks.outer_body]]
id = "inner"
type = "foreach"

[blocks.outer_body.params]
items = "inputs.inner"
subflow = "inner_body"
parallel = 1
max_iterations = 4

[[blocks.outer_body]]
id = "write_outer"
type = "write"

[blocks.outer_body.params]
output_file = "outer-{{ item }}.txt"
content = "{{ item }}"

[[blocks.inner_body]]
id = "write_inner"
type = "write"

[blocks.inner_body.params]
output_file = "inner-{{ item }}.txt"
content = "{{ item }}""#,
    )
    .expect("generator manifest should be written");
    let service = test_service(vec![root], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::from([
                ("outer".into(), json!(["a", "b"])),
                ("inner".into(), json!(["x", "y"])),
            ]),
            ..Default::default()
        })
        .await
        .expect("nested foreach run should start");
    let snapshot = wait_for_snapshot(&service, &id, RunStatus::Succeeded).await;
    assert_eq!(snapshot.state, RunStatus::Succeeded);
    let workspace = run_workspace_dir(&runs.join(&id));
    assert_eq!(
        std::fs::read_to_string(workspace.join("outer-a.txt")).unwrap(),
        "a"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("outer-b.txt")).unwrap(),
        "b"
    );
    let journal = std::fs::read_to_string(run_meta_dir(&runs.join(&id)).join("journal.jsonl"))
        .expect("journal should be readable");
    assert!(journal.contains("outer[0]/inner[0]/write_inner"));
    assert!(journal.contains("outer[1]/inner[1]/write_inner"));
}

#[tokio::test]
async fn run_labels_and_audit_raise_are_admitted_and_journaled() {
    let runs = temp_run_dir("run-labels");
    let _ = std::fs::remove_dir_all(&runs);
    let generators = runs.join("generators");
    write_generator_package(&generators, "labeled-gen");
    let service = test_service(vec![generators], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "labeled-gen".into(),
            inputs: BTreeMap::from([("name".into(), json!("qcg"))]),
            labels: BTreeMap::from([("owner".into(), "team-a".into())]),
            audit_level: Some(qcg_policy::AuditLevel::Standard),
            ..Default::default()
        })
        .await
        .expect("labeled run should start");
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    assert_eq!(
        snapshot.labels.get("owner").map(String::as_str),
        Some("team-a"),
        "labels are returned with the run"
    );
    let journal = std::fs::read_to_string(
        run_meta_dir(&service.run_dir_for(&id).await.unwrap()).join("journal.jsonl"),
    )
    .expect("journal should read");
    assert!(
        journal.contains("\"audit_raise\":\"standard\""),
        "the requested audit raise is journaled: {journal}"
    );
    let _ = std::fs::remove_dir_all(&runs);
}

#[tokio::test]
async fn rehydrate_restores_accepted_answers_from_journal() {
    let root = temp_run_dir("hitl-rehydrate");
    let _ = std::fs::remove_dir_all(&root);
    let run_id = format!("synth-{}", uuid::Uuid::now_v7());
    let record = synthetic_hitl_record(&root, &run_id);
    let question = synthetic_question();
    write_run_event(
        &record,
        "run_waiting",
        json!({ "question_id": "q1", "question": question }),
    )
    .expect("waiting event should append");
    let runs_dir = root.join("runs");
    let pending = rehydrate_runs(
        &runs_dir,
        DEFAULT_MAX_TRACKED_RUNS,
        qcg_policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
    )
    .expect("rehydrate should succeed");
    let waiting = pending.get(&run_id).expect("run should rehydrate");
    assert_eq!(waiting.state, RunStatus::Waiting);
    assert!(waiting.answers.is_empty());
    write_run_event(
        &record,
        "user_answered",
        json!({ "question_id": "q1", "values": { "answer": "brief" } }),
    )
    .expect("answer event should append");
    let pending = rehydrate_runs(
        &runs_dir,
        DEFAULT_MAX_TRACKED_RUNS,
        qcg_policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
    )
    .expect("rehydrate should succeed");
    let queued = pending.get(&run_id).expect("run should rehydrate");
    assert_eq!(queued.state, RunStatus::Queued);
    assert_eq!(
        queued.answers.get("q1"),
        Some(&json!({ "answer": "brief" })),
        "accepted answer must survive a restart before the engine consumes it"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn mcp_pending_continuation_roundtrips_through_journal() {
    let root = temp_run_dir("mcp-pending");
    let _ = std::fs::remove_dir_all(&root);
    let run_id = format!("synth-{}", uuid::Uuid::now_v7());
    let record = synthetic_hitl_record(&root, &run_id);
    write_run_event(
        &record,
        "mcp_input_pending",
        json!({
            "node": "fetch",
            "pending_key": "fetch:mcpcont:server/tool:abc123#__mcp_pending",
            "question_id": "fetch:mcp:server/tool:deadbeef",
            "server": "server",
            "tool": "tool",
            "call_id": "call-1",
            "arguments": { "q": "x" },
            "request_state": "state-1",
            "input_requests": {},
        }),
    )
    .expect("pending event should append");
    // The typed journal store (not the answers map) carries the
    // descriptor, including the invocation binding.
    let state = crate::summaries::fold_run_state(&root.join("runs").join(&run_id))
        .expect("journal should fold");
    let descriptor = state
        .mcp_pending
        .get("fetch:mcpcont:server/tool:abc123#__mcp_pending")
        .expect("pending descriptor should be keyed by reservation");
    assert_eq!(
        descriptor.get("request_state").and_then(Value::as_str),
        Some("state-1")
    );
    assert_eq!(
        descriptor.get("question_id").and_then(Value::as_str),
        Some("fetch:mcp:server/tool:deadbeef")
    );
    assert_eq!(
        descriptor.get("call_id").and_then(Value::as_str),
        Some("call-1")
    );
    // Completion consumes the continuation so later identical calls
    // start fresh instead of resuming it.
    write_run_event(
        &record,
        "mcp_continuation_consumed",
        json!({
            "node": "fetch",
            "pending_key": "fetch:mcpcont:server/tool:abc123#__mcp_pending",
        }),
    )
    .expect("consume event should append");
    let state = crate::summaries::fold_run_state(&root.join("runs").join(&run_id))
        .expect("journal should fold");
    assert!(
        !state
            .mcp_pending
            .contains_key("fetch:mcpcont:server/tool:abc123#__mcp_pending"),
        "consumed continuation must leave the store"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn e07_8_repeat_and_fail_policies_via_real_agent_entry() {
    // E07-8: Repeat and Fail policies for indeterminate agent side
    // effects, driven through the real LlmAgentStep entry via the
    // service (not guard-harness-direct). Repeat re-executes and
    // journals `operation_repeated`; Fail refuses with FailureCode
    // Refused and journals the refusal.
    let (repeat_state, repeat_journal, _) =
        run_interrupted_agent_command("agent-repeat-e078", "repeat").await;
    assert_eq!(
        repeat_state,
        RunStatus::Succeeded,
        "repeat policy must converge: {repeat_journal}"
    );
    assert!(
        repeat_journal.contains("\"t\":\"operation_repeated\""),
        "repeat must be journaled: {repeat_journal}"
    );
    let (fail_state, fail_journal, _) =
        run_interrupted_agent_command("agent-fail-e078", "fail").await;
    assert_eq!(
        fail_state,
        RunStatus::Failed,
        "fail policy must refuse indeterminate replay: {fail_journal}"
    );
    assert!(
        fail_journal.contains("refusing automatic replay"),
        "guard refusal must be journaled: {fail_journal}"
    );
    assert!(
        fail_journal.contains("refused"),
        "fail policy must surface FailureCode Refused: {fail_journal}"
    );
}

#[tokio::test]
async fn e08_6_two_questions_restart_and_late_refusal_end_to_end() {
    // E08-6: two-question separation end-to-end through the real
    // llm.agent step, restart persistence of the pending answer, and
    // late-answer-to-wrong-question refusal. No mocks: real service,
    // real fake LLM tool sequence, real journal.
    let root = temp_run_dir("agent-twoq-e086");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(generator.join("prompts")).expect("generator dirs");
    std::fs::write(
        generator.join("qcg.toml"),
        r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "agent"
output = "agent_result"
type = "llm.agent"

[flow.params]
max_iterations = 4
max_tokens_total = 4096
prompt = "prompts/agent.j2"

[[flow.params.tools]]
kind = "ask_user"
name = "ask_mode"

[llm]
max_tokens = 256
temperature = 0.0

[llm.model]
model = "fake"
provider = "fake"

[permissions]
fs_read = []
fs_write = ["workspace"]
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"

[permissions.containers]
enabled = false
"#,
    )
    .expect("generator manifest");
    std::fs::write(
            generator.join("prompts/agent.j2"),
            r#"Ask two questions, then finish.

FAKE_TOOL_SEQUENCE: [{"name":"ask_mode","args":{"question":"City?","options":["tokyo","osaka"]}},{"name":"ask_mode","args":{"question":"Postal code?","options":["100","200"]}}]
FAKE_AGENT_FINAL:
done
"#,
        )
        .expect("prompt");
    let service = test_service(vec![root.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("agent run should start");
    let first = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
    let question1 = first.question.expect("first question");
    service
        .answer(
            id.clone(),
            question1.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("tokyo"))]),
            },
        )
        .await
        .expect("first answer");
    let mut question2 = None;
    for _ in 0..400 {
        let snapshot = service.snapshot(id.clone()).await.expect("snapshot");
        if snapshot.state.is_terminal() {
            break;
        }
        if let Some(question) = snapshot.question
            && question.id != question1.id
        {
            question2 = Some(question);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let question2 = question2.expect("a distinct second question must be asked");
    assert_ne!(
        question1.id, question2.id,
        "two calls from the same tool must have distinct identities"
    );
    drop(service);
    let restored = loop {
        match try_service(vec![root.clone()], runs.clone()) {
            Ok(service) => break service,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    };
    let second = wait_for_snapshot(&restored, &id, RunStatus::Waiting).await;
    assert_eq!(
        second
            .question
            .as_ref()
            .map(|question| question.id.as_str()),
        Some(question2.id.as_str()),
        "restart must re-issue the same pending question identity"
    );
    let late = restored
        .answer(
            id.clone(),
            question1.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("osaka"))]),
            },
        )
        .await;
    assert!(
        late.is_err(),
        "a late answer for the first question must be refused"
    );
    restored
        .answer(
            id.clone(),
            question2.id.clone(),
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!("100"))]),
            },
        )
        .await
        .expect("second answer");
    let terminal = wait_for_terminal_snapshot(&restored, &id).await;
    assert_eq!(terminal.state, RunStatus::Succeeded);
    let run_dir = restored.run_dir_for(&id).await.expect("run dir");
    let events = read_events_from_meta(&run_meta_dir(&run_dir)).expect("journal");
    let answered: Vec<&Value> = events
        .iter()
        .filter(|event| event.get("t").and_then(Value::as_str) == Some("user_answered"))
        .collect();
    assert_eq!(answered.len(), 2, "both questions must be answered");
    assert_ne!(
        answered[0].get("question_id"),
        answered[1].get("question_id"),
        "each call must keep its own identity"
    );
    drop(restored);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn e09_7_header_content_type_stdin_matrix_and_cross_confirmation() {
    // E09-7: both-entry matrix (header + Content-Type + stdin changes in
    // the same invocation) through the real http/command detail builders
    // used by the steps, plus a cross-confirmation integration test
    // through real http/command steps (no mocks, no harness-direct guard
    // decisions).
    use std::collections::BTreeMap as Map;
    let salt = qcg_engine::operation_id_for("run-e097", "node-e097", "call-same");
    let base_headers: Map<String, String> =
        Map::from([("X-Tenant".to_string(), "alpha".to_string())]);
    let base_body: Option<&[u8]> = Some(b"hello" as &[u8]);
    let base_sensitive: Map<String, String> = Map::new();
    let base = qcg_engine::http_operation_details(
        "POST",
        &base_headers,
        base_body,
        &base_sensitive,
        &salt,
    )
    .expect("base http details");
    let mut changed_header = base_headers.clone();
    changed_header.insert("X-Tenant".to_string(), "beta".to_string());
    let header_changed = qcg_engine::http_operation_details(
        "POST",
        &changed_header,
        base_body,
        &base_sensitive,
        &salt,
    )
    .expect("header-changed details");
    assert_ne!(
        base["headers_sha256"], header_changed["headers_sha256"],
        "a changed header must change the approval binding in the same invocation"
    );
    let mut content_type_headers: Map<String, String> = Map::new();
    content_type_headers.insert("content-type".to_string(), "application/json".to_string());
    let content_type_base = qcg_engine::http_operation_details(
        "POST",
        &content_type_headers,
        Some(b"{}" as &[u8]),
        &base_sensitive,
        &salt,
    )
    .expect("content-type base details");
    let mut content_type_changed = Map::new();
    content_type_changed.insert("content-type".to_string(), "text/plain".to_string());
    let content_type_other = qcg_engine::http_operation_details(
        "POST",
        &content_type_changed,
        Some(b"{}" as &[u8]),
        &base_sensitive,
        &salt,
    )
    .expect("content-type changed details");
    assert_ne!(
        content_type_base["headers_sha256"], content_type_other["headers_sha256"],
        "a changed Content-Type must change the binding in the same invocation"
    );
    let body_other = qcg_engine::http_operation_details(
        "POST",
        &base_headers,
        Some(b"other" as &[u8]),
        &base_sensitive,
        &salt,
    )
    .expect("body-changed details");
    assert_ne!(
        base["body_sha256"], body_other["body_sha256"],
        "a changed body must change the binding in the same invocation"
    );
    assert!(
        !base.to_string().contains("alpha") && !header_changed.to_string().contains("beta"),
        "header values must stay out of journaled details"
    );
    let stdin_base = qcg_engine::bind_command_stdin(
        "node-e097",
        json!({"argv": ["cat"]}),
        Some(b"one" as &[u8]),
        &salt,
    )
    .expect("base stdin details");
    let stdin_other = qcg_engine::bind_command_stdin(
        "node-e097",
        json!({"argv": ["cat"]}),
        Some(b"two" as &[u8]),
        &salt,
    )
    .expect("changed stdin details");
    assert_ne!(
        stdin_base["stdin_sha256"], stdin_other["stdin_sha256"],
        "changed stdin must change the command binding in the same invocation"
    );
    // Cross-confirmation through real steps: an http approval must not
    // satisfy a command, and vice versa. A local loopback server backs
    // the http POST so no external network is required.
    let root = temp_run_dir("cross-confirm-e097");
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(&generator).expect("generator dir");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let port = listener
        .local_addr()
        .expect("listener should have an address")
        .port();
    let server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let mut buffer = vec![0u8; 8192];
            let _ = socket.read(&mut buffer).await;
            let body = b"ok";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.write_all(body).await;
        }
    });
    std::fs::write(
            generator.join("qcg.toml"),
            format!(
                r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "fetch"
type = "http"

[flow.params]
url = "http://127.0.0.1:{port}/api"
method = "POST"
headers = {{ Authorization = "Bearer test" }}
content_type = "application/json"
body_text = "hello"

[[flow]]
id = "run"
type = "command"
needs = ["fetch"]

[flow.params]
command = ["sh", "-c", "echo hi"]

[permissions]
fs_read = []
fs_write = ["workspace"]
network = ["127.0.0.1"]
side_effects = "confirm"
side_effects_scope = "invocation"
commands = [{{ bin = "sh", args = ["-c", "echo hi"], purpose = "cross confirmation probe", isolation = "trusted_host" }}]

[permissions.containers]
enabled = false
"#,
            ),
        )
        .expect("generator manifest");
    let service = test_service(vec![root.clone()], runs.clone());
    let id = service
        .start_run(StartRun {
            generator_id: "generator".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("run should start");
    let first = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
    let http_confirm = first.confirm.expect("http confirm");
    assert_eq!(http_confirm.kind, "http");
    let http_id = http_confirm.id.clone();
    assert!(
        !http_confirm.operation_digest.is_empty(),
        "http approval must carry a digest"
    );
    service
        .confirm(
            id.clone(),
            http_id.clone(),
            qcg_api::ConfirmDecision {
                decision: ConfirmationDecision::Approve,
            },
        )
        .await
        .expect("http approval");
    let second = wait_for_snapshot(&service, &id, RunStatus::Confirming).await;
    let cmd_confirm = second.confirm.expect("command confirm");
    assert_eq!(cmd_confirm.kind, "command");
    assert_ne!(
        cmd_confirm.id, http_id,
        "a command approval must not reuse the http approval"
    );
    assert_ne!(
        cmd_confirm.operation_digest, http_confirm.operation_digest,
        "cross-kind approvals must bind different digests"
    );
    server.abort();
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
}
