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
    let _temp = TempGuard(root.clone());
    let generator = root.join("generator");
    let runs = root.join("runs");
    std::fs::create_dir_all(generator.join("prompts")).expect("generator dirs");
    // The marker is written by the parent only AFTER spawning and probing the
    // long-lived child. `ran` alone is pre-spawn and cannot synchronize a timeout.
    // Keep the real shell, 60s sleep, and the second-attempt branch unchanged.
    let script = r#"if [ ! -f ran ]; then : > ran; sleep 60 & child=$!; if kill -0 "$child"; then printf '%s %s\n' "$$" "$child" > ready; fi; wait "$child"; else echo done; fi"#;
    let script_toml = serde_json::to_string(script).expect("quote shell script");
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
# Keep the real 15s deadline and 60s side effect. The test holds Tokio's
# clock until the real child is ready, including on slow Windows runners.
retry = {{ max_attempts = 2, backoff_ms = 0, timeout_secs = 15, on_indeterminate = "{on_indeterminate}" }}

[flow.params]
max_iterations = 4
max_tokens_total = 4096
prompt = "prompts/agent.j2"

[[flow.params.tools]]
kind = "command"
name = "long_task"
command = ["sh", "-c", {script_toml}]

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
commands = [{{ bin = "sh", args = ["-c", {script_toml}], purpose = "interrupted side effect", isolation = "trusted_host" }}]

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
    let clock = tokio::time::Instant::now();
    let (id, run_dir, pids) = with_fixed_clock("agent child readiness", async {
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
                ..Default::default()
            })
            .await
            .expect("agent run should start");
        let run_dir = service.run_dir_for(&id).await.expect("run dir");
        let ready = run_workspace_dir(&run_dir).join("ready");
        // A new UUID root/run workspace makes this acknowledgement fresh.
        // No Tokio sleeps: neither readiness nor its watchdog may move time.
        let pids = loop {
            if let Ok(text) = std::fs::read_to_string(&ready) {
                let pids: Vec<u32> = text
                    .split_whitespace()
                    .filter_map(|pid| pid.parse().ok())
                    .collect();
                if pids.len() == 2 && pids.iter().all(|pid| *pid > 0) {
                    break pids;
                }
            }
            tokio::task::yield_now().await;
        };
        assert!(
            pids.iter().all(|pid| agent_process_exists(*pid)),
            "ready child must still be alive"
        );
        (id, run_dir, pids)
    })
    .await;
    assert_eq!(tokio::time::Instant::now(), clock);
    tokio::time::advance(std::time::Duration::from_secs(15)).await;
    assert_eq!(
        tokio::time::Instant::now() - clock,
        std::time::Duration::from_secs(15)
    );
    // The retry has its own 15s timer. Keep time held while real process I/O,
    // cancellation, journal writes and the repeat/fail attempt settle; ordinary
    // polling helpers use Tokio sleeps and would auto-advance that timer.
    let snapshot = with_fixed_clock("agent terminal/cleanup", async {
        let snapshot = loop {
            let snapshot = service.snapshot(id.clone()).await.expect("snapshot");
            if snapshot.state.is_terminal() {
                break snapshot;
            }
            tokio::task::yield_now().await;
        };
        while pids.iter().any(|pid| agent_process_exists(*pid)) {
            tokio::task::yield_now().await;
        }
        snapshot
    })
    .await;
    let journal =
        std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl")).unwrap_or_default();
    drop(service);
    (snapshot.state, journal, run_dir)
}

/// Poll a test phase without letting Tokio become idle and auto-advance paused
/// time. The always-runnable yield branch also bounds missing readiness and
/// stuck real I/O by WALL time, not by the very clock this helper must hold.
async fn with_fixed_clock<T>(phase: &str, future: impl std::future::Future<Output = T>) -> T {
    let clock = tokio::time::Instant::now();
    let watchdog = std::time::Instant::now();
    tokio::pin!(future);
    loop {
        assert_eq!(
            tokio::time::Instant::now(),
            clock,
            "clock moved during {phase}"
        );
        assert!(
            watchdog.elapsed() < std::time::Duration::from_secs(60),
            "wall-clock watchdog expired during {phase}"
        );
        tokio::select! {
            biased;
            result = &mut future => {
                assert_eq!(tokio::time::Instant::now(), clock, "clock moved during {phase}");
                return result;
            }
            _ = tokio::task::yield_now() => {}
        }
    }
}

// Use the same shell PID namespace as the command on Unix and Windows (Git sh).
// Signal zero observes only; the production ProcessTreeGuard must kill/reap the
// real process tree. Probe both the parent and child, including zombies.
fn agent_process_exists(pid: u32) -> bool {
    std::process::Command::new("sh")
        .args(["-c", "kill -0 \"$1\" 2>/dev/null", "probe"])
        .arg(pid.to_string())
        .status()
        .expect("probe agent process tree")
        .success()
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
    // 60s polling budget: long-timeout interrupt tests (15s attempt
    // timeout plus retry and Windows scheduling overhead) must fit
    // inside the wait.
    for _ in 0..6000 {
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
    let package_sha256 = pin_test_package(&contract.root, &run_dir);
    let (events, _) = broadcast::channel(512);
    let record = RunRecord {
        contract: contract.clone(),
        contract_sha256: contract.sha256.clone(),
        package_sha256: Some(package_sha256.clone()),
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
        scheduler_yield: CancellationToken::new(),
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
            "package_sha256": package_sha256,
            "inputs": {},
            "answers": {},
            "confirmations": {},
            "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "elapsed_budget_mode": policy::ElapsedBudgetMode::WallClock,
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

/// Counts actual pre-execution snapshot uses and disk fallbacks in one store.
/// Registration is scoped; production builds contain no probe or registry.
pub(crate) struct SpawnJournalProbe {
    root: Utf8PathBuf,
    counts: Arc<Mutex<BTreeMap<Utf8PathBuf, (usize, usize)>>>,
}

type SpawnProbeRegistry =
    BTreeMap<Utf8PathBuf, std::sync::Weak<Mutex<BTreeMap<Utf8PathBuf, (usize, usize)>>>>;
fn spawn_probe_registry() -> &'static Mutex<SpawnProbeRegistry> {
    static REGISTRY: std::sync::OnceLock<Mutex<SpawnProbeRegistry>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}
impl SpawnJournalProbe {
    pub(crate) fn new(root: Utf8PathBuf) -> Self {
        let counts = Arc::new(Mutex::new(BTreeMap::new()));
        spawn_probe_registry()
            .lock()
            .unwrap()
            .insert(root.clone(), Arc::downgrade(&counts));
        Self { root, counts }
    }
    pub(crate) fn counts(&self, path: &Utf8Path) -> (usize, usize) {
        self.counts
            .lock()
            .unwrap()
            .get(path)
            .copied()
            .unwrap_or_default()
    }
}
impl Drop for SpawnJournalProbe {
    fn drop(&mut self) {
        spawn_probe_registry().lock().unwrap().remove(&self.root);
    }
}
pub(crate) fn record_spawn_journal_use(path: &Utf8Path, snapshot: bool) {
    for (root, probe) in spawn_probe_registry().lock().unwrap().iter() {
        if path.starts_with(root)
            && let Some(counts) = probe.upgrade()
        {
            let mut counts = counts.lock().unwrap();
            let entry = counts.entry(path.to_path_buf()).or_default();
            if snapshot {
                entry.0 += 1;
            } else {
                entry.1 += 1;
            }
        }
    }
}

/// Complete a synthetic admission with the same pinned package as production.
pub(crate) fn pin_test_package(source: &Utf8Path, run_dir: &Utf8Path) -> String {
    let sha =
        crate::package::package_identity_sha256(source, &crate::package::PackageLimits::default())
            .expect("package identity");
    crate::run_package::copy(source, run_dir, &sha).expect("pin test package");
    sha
}
