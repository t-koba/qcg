use crate::*;
use api::{AnswerPayload, RunSnapshot, RunStatus, StartRun};
use camino::{Utf8Path, Utf8PathBuf};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub(crate) fn temp_run_dir(name: &str) -> Utf8PathBuf {
    let dir = std::env::temp_dir().join(format!("service-test-{name}-{}", uuid::Uuid::now_v7()));
    Utf8PathBuf::from_path_buf(dir).expect("temporary directory path must be UTF-8")
}

/// Removes the temp root on drop so a failed assertion cannot leak test
/// directories (E04). A removal failure warns instead of being silently
/// ignored (E01).
pub(crate) struct TempGuard(pub(crate) Utf8PathBuf);

impl Drop for TempGuard {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(self.0.as_std_path()) {
            tracing::warn!(path = %self.0, %error, "test temp cleanup failed");
        }
    }
}

/// Service over `roots`/`runs_dir` with the deployment policy that every
/// test uses: no providers file, the default limits, and exclusive store
/// ownership. A test that needs another mode calls the constructor.
pub(crate) fn try_service(
    roots: Vec<Utf8PathBuf>,
    runs_dir: Utf8PathBuf,
) -> Result<LocalService, ServiceError> {
    LocalService::with_generator_roots_policy_and_store_mode(
        roots,
        runs_dir,
        None,
        policy::DEFAULT_MAX_ACTIVE_RUNS,
        policy::DEFAULT_MAX_TRACKED_RUNS,
        RunStoreMode::Exclusive,
        ServiceDeploymentPolicy::default(),
    )
}

/// [`try_service`] for the tests that only need a usable service.
pub(crate) fn test_service(roots: Vec<Utf8PathBuf>, runs_dir: Utf8PathBuf) -> LocalService {
    try_service(roots, runs_dir).expect("service should initialize")
}

pub(crate) async fn read_journal_string(service: &LocalService, id: String) -> String {
    use tokio::io::AsyncReadExt as _;
    let mut opened = service
        .open_journal_stream(id)
        .await
        .expect("journal should open");
    let mut bytes = Vec::new();
    opened
        .file
        .read_to_end(&mut bytes)
        .await
        .expect("journal should read");
    if let Some(mut audit) = opened.audit {
        audit
            .read_to_end(&mut bytes)
            .await
            .expect("audit should read");
    }
    // Reassemble the merged record view in seq order: durable and
    // observation records share one seq space across sibling files.
    let text = String::from_utf8(bytes).expect("journal should be UTF-8");
    let mut lines: Vec<(u64, &str)> = text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let seq = serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|value| value.get("seq").and_then(Value::as_u64))
                .unwrap_or(0);
            (seq, line)
        })
        .collect();
    lines.sort_by_key(|(seq, _)| *seq);
    let mut merged = lines
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n");
    if !merged.is_empty() {
        merged.push('\n');
    }
    merged
}

pub(crate) fn write_generator_package(root: &Utf8Path, id: &str) {
    let dir = root.join(id);
    std::fs::create_dir_all(&dir).expect("package directory should be created");
    std::fs::write(
        dir.join(contract::MANIFEST_FILE),
        format!(
            r#"
[generator]
id = "{id}"
name = "{id}"
version = "0.1.0"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "name"
required = true
type = "string"

[permissions]
fs_read = []
fs_write = []
network = []
commands = []
side_effects = "none"
side_effects_scope = "invocation"

[permissions.containers]
enabled = false"#
        ),
    )
    .expect("manifest should be written");
}

pub(crate) async fn run_interrupted_agent_command(
    name: &str,
    on_indeterminate: &str,
) -> (RunStatus, String, Utf8PathBuf) {
    let root = temp_run_dir(name);
    let _ = std::fs::remove_dir_all(&root);
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(generator.join("prompts")).expect("generator dirs");
    std::fs::write(
            generator.join(contract::MANIFEST_FILE),
            format!(
                r#"
[generator]
id = "generator"
name = "Generator"
version = "0.1.0"

[[flow]]
id = "agent"
output = "agent_result"
type = "llm.agent"
# 5s lets a slow shell land `touch ran` before the kill; the 30s sleep
# still guarantees the interrupt, so attempt 2 always observes `ran`.
retry = {{ max_attempts = 2, backoff_ms = 0, timeout_secs = 5, on_indeterminate = "{on_indeterminate}" }}

[flow.params]
max_iterations = 4
max_tokens_total = 4096
prompt = "prompts/agent.j2"

[[flow.params.tools]]
kind = "command"
name = "long_task"
command = ["sh", "-c", "if [ ! -f ran ]; then touch ran; sleep 30; else echo done; fi"]

[llm]
max_tokens = 256
temperature = 0.0

[llm.model]
model = "fake"
provider = "fake"

[permissions]
fs_read = []
fs_write = []
network = []
side_effects_scope = "invocation"
side_effects = "allowed"
commands = [{{ bin = "sh", args = ["-c", "if [ ! -f ran ]; then touch ran; sleep 30; else echo done; fi"], purpose = "interrupted side effect", isolation = "trusted_host" }}]

[permissions.containers]
enabled = false"#
            ),
        )
        .expect("generator manifest");
    std::fs::write(
        generator.join("prompts/agent.j2"),
        r#"Run the long task once, then finish.

FAKE_TOOL_SEQUENCE: [{"name":"long_task","args":{}}]
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
    let snapshot = wait_for_terminal_snapshot(&service, &id).await;
    let run_dir = service.run_dir_for(&id).await.expect("run dir");
    let journal =
        std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl")).unwrap_or_default();
    drop(service);
    let _ = std::fs::remove_dir_all(&root);
    (snapshot.state, journal, run_dir)
}

pub(crate) async fn wait_for_snapshot(
    service: &LocalService,
    id: &str,
    state: RunStatus,
) -> RunSnapshot {
    for _ in 0..200 {
        let snapshot = service
            .snapshot(id.to_string())
            .await
            .expect("run snapshot should exist");
        if snapshot.state == state {
            return snapshot;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "run {id} did not reach state `{state}`; snapshot={:?}; journal={}",
        service.snapshot(id.to_string()).await,
        read_journal_string(service, id.to_string()).await
    );
}

/// Waits until the exclusive runs-directory lock is free again.
///
/// A dropped `LocalService` releases the store only when the last
/// clone of its inner state is gone, and in-flight run tasks hold those
/// clones while they settle. A restart on the same runs directory must
/// therefore wait for the store instead of racing the previous service's
/// teardown.
pub(crate) async fn wait_for_runs_store_release(runs: &Utf8Path) {
    for _ in 0..2000 {
        match crate::run_dirs::lock_runs_directory(runs) {
            // The probe lock is released immediately; it only proves the
            // previous owner is gone.
            Ok(file) => {
                drop(file);
                return;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    }
    panic!("runs directory `{runs}` was never released by the previous service");
}

pub(crate) async fn wait_for_terminal_snapshot(service: &LocalService, id: &str) -> RunSnapshot {
    // 20s polling budget: long-timeout interrupt tests (5s attempt
    // timeout plus retry) must fit inside the wait.
    for _ in 0..2000 {
        let snapshot = service
            .snapshot(id.to_string())
            .await
            .expect("run snapshot should exist");
        if snapshot.state.is_terminal() {
            return snapshot;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("run did not reach a terminal state");
}

/// Starts the `ask-user` fixture and answers its prompt through the API.
/// Admission-time answers keyed by bare node id are never honored:
/// question ids bind the full resolved content
/// (`<node>:ask_user:<hex>`, E08f), so tests drive answers
/// interactively and assert on the returned prompt id instead of
/// assuming a node id. Returns the run id once the answer is accepted.
pub(crate) async fn start_ask_user_and_answer(service: &LocalService, mode: &str) -> String {
    let id = service
        .start_run(StartRun {
            generator_id: "ask-user".into(),
            inputs: BTreeMap::new(),
            ..Default::default()
        })
        .await
        .expect("ask-user run should start");
    let waiting = wait_for_snapshot(service, &id, RunStatus::Waiting).await;
    let question = waiting.question.expect("run should ask its question");
    assert!(
        question.id.starts_with("choose_mode:ask_user:"),
        "question id must bind the resolved content, got `{}`",
        question.id
    );
    service
        .answer(
            id.clone(),
            question.id,
            AnswerPayload {
                values: BTreeMap::from([("answer".into(), json!(mode))]),
            },
        )
        .await
        .expect("answer should be accepted");
    id
}

pub(crate) fn synthetic_hitl_record(root: &Utf8Path, run_id: &str) -> RunRecord {
    let generators =
        Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
    let service = test_service(vec![generators], root.join("runs"));
    let contract = service
        .load_generator("ask-user")
        .expect("ask-user fixture should load");
    let run_dir = root.join("runs").join(run_id);
    prepare_api_run_directory(&run_dir).expect("run directory should prepare");
    let (events, _) = broadcast::channel(512);
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
        queued_at: Some(chrono::Utc::now()),
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
        }),
    )
    .expect("queued event should append");
    record
}

pub(crate) fn synthetic_question() -> api::FormSpec {
    api::FormSpec {
        id: "q1".into(),
        title: "Question".into(),
        title_i18n: Default::default(),
        fields: vec![],
    }
}

pub(crate) fn temp_root(name: &str) -> Utf8PathBuf {
    let dir = std::env::temp_dir().join(format!("accept-{name}-{}", uuid::Uuid::now_v7()));
    Utf8PathBuf::from_path_buf(dir).expect("temp path must be UTF-8")
}
