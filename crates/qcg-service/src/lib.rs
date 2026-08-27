use camino::{Utf8Path, Utf8PathBuf};
use futures_util::{StreamExt as _, stream::BoxStream};
use qcg_api::{
    AnswerPayload, ApiError, ConfirmDecision, ConfirmationDecision, GeneratorDetail,
    GeneratorSummary, RunListItem, RunSnapshot, RunStatus, StartRun,
};
use qcg_contract::{Contract, ContractError, validate_form_values};
use qcg_engine::{
    ConfirmSpec, Engine, FailureCode, FailureDetail, FormSpec, Interaction, JournalWriter,
    OutputArtifact, OutputManifest, Progress, RunEvent, RunFailureKind, RunOptions, RunState,
    read_output_manifest, resolve_artifact_path,
};
use qcg_steps::deterministic_registry;
use qcg_types::{FileValue, is_safe_relative_path};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Write};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use zip::write::SimpleFileOptions;

const DEFAULT_ARTIFACT_ZIP_LIMIT_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
}

impl From<ServiceError> for ApiError {
    fn from(error: ServiceError) -> Self {
        Self::internal(error.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct LocalQcgService {
    inner: Arc<LocalQcgServiceInner>,
}

#[derive(Debug, Clone)]
pub struct DirectRun {
    pub generator_path: Utf8PathBuf,
    pub inputs: BTreeMap<String, Value>,
    pub output_dir: Utf8PathBuf,
    pub json_events: bool,
    pub interactive: bool,
    pub answers: BTreeMap<String, Value>,
    pub confirmations: BTreeMap<String, bool>,
    pub llm_seed_override: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct DirectRunEvents {
    pub manifest: OutputManifest,
    pub events: Vec<RunEvent>,
}

#[derive(Debug)]
struct LocalQcgServiceInner {
    /// All generator roots in precedence order; the first root containing an
    /// id wins and is also the writable install target. Later roots (for
    /// example the bundled `share/qcg/generators`) are read-only catalogs.
    generator_roots: Vec<Utf8PathBuf>,
    runs_dir: Utf8PathBuf,
    runs: RwLock<BTreeMap<String, RunRecord>>,
    llm_runtime: Arc<qcg_llm::LlmRuntime>,
}

#[derive(Debug, Clone)]
struct RunRecord {
    contract: Contract,
    contract_sha256: String,
    inputs: BTreeMap<String, Value>,
    answers: BTreeMap<String, Value>,
    confirmations: BTreeMap<String, bool>,
    state: RunStatus,
    run_dir: Utf8PathBuf,
    artifacts: Option<OutputManifest>,
    question: Option<FormSpec>,
    confirm: Option<ConfirmSpec>,
    events: broadcast::Sender<RunEvent>,
    cancellation: CancellationToken,
    task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl LocalQcgService {
    /// Creates a service and resolves the providers registry once during
    /// initialization. An explicit path is authoritative; `None` delegates
    /// to the registry resolver's environment and installation search.
    pub fn new(
        generators_dir: Utf8PathBuf,
        runs_dir: Utf8PathBuf,
        providers_path: Option<Utf8PathBuf>,
    ) -> Result<Self, ServiceError> {
        Self::with_generator_roots(vec![generators_dir], runs_dir, providers_path)
    }

    /// Creates a service whose generators resolve across multiple roots in
    /// precedence order. The first root is the writable install target; the
    /// rest act as read-only catalogs so bundled demos appear alongside
    /// installed packages. The providers path has the same authoritative
    /// semantics as [`Self::new`].
    pub fn with_generator_roots(
        generator_roots: Vec<Utf8PathBuf>,
        runs_dir: Utf8PathBuf,
        providers_path: Option<Utf8PathBuf>,
    ) -> Result<Self, ServiceError> {
        let mut roots = generator_roots;
        if roots.is_empty() {
            roots.push(Utf8PathBuf::from("generators"));
        }
        let runs = rehydrate_runs(&runs_dir)?;
        let llm_runtime = Arc::new(
            match qcg_llm::LlmRouter::load_optional(providers_path.as_deref()) {
                Ok(Some(router)) => router.into_runtime(),
                // No registry named or found: the built-in `fake` provider keeps
                // fake-only generators working; other ids get setup guidance.
                Ok(None) => qcg_llm::LlmRuntime::fake_only(),
                Err(error) => return Err(ServiceError::Invalid(error.to_string())),
            },
        );
        Ok(Self {
            inner: Arc::new(LocalQcgServiceInner {
                generator_roots: roots,
                runs_dir,
                runs: RwLock::new(runs),
                llm_runtime,
            }),
        })
    }

    pub fn start_retention_gc(&self) -> Option<JoinHandle<()>> {
        let enabled = std::env::var("QCG_AUTO_GC")
            .map(|value| !matches!(value.as_str(), "0" | "false" | "off"))
            .unwrap_or(true);
        if !enabled {
            return None;
        }
        let runs_dir = self.inner.runs_dir.clone();
        Some(tokio::spawn(async move {
            let _ = gc_run_directories(&runs_dir, 50, true);
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(24 * 60 * 60));
            interval.tick().await;
            loop {
                interval.tick().await;
                let _ = gc_run_directories(&runs_dir, 50, true);
            }
        }))
    }

    async fn live_receiver(&self, id: &str) -> Result<broadcast::Receiver<RunEvent>, ApiError> {
        let runs = self.inner.runs.read().await;
        let record = runs
            .get(id)
            .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
        Ok(record.events.subscribe())
    }

    pub async fn run_dir_for(&self, id: &str) -> Result<Utf8PathBuf, ApiError> {
        let memory = self
            .inner
            .runs
            .read()
            .await
            .get(id)
            .map(|run| run.run_dir.clone());
        if let Some(run_dir) = memory {
            return Ok(run_dir);
        }
        if !is_safe_id(id) {
            return Err(api_bad_request(format!("run id `{id}` is not allowed")));
        }
        let run_dir = self.inner.runs_dir.join(id);
        if !run_meta_dir(&run_dir).join("journal.jsonl").is_file() {
            return Err(api_not_found(format!("run `{id}` was not found")));
        }
        Ok(run_dir)
    }

    pub async fn list_runs(&self) -> Result<Vec<RunSnapshot>, ApiError> {
        let mut runs = Vec::new();
        if self.inner.runs_dir.exists() {
            for entry in std::fs::read_dir(&self.inner.runs_dir).map_err(api_internal)? {
                let entry = entry.map_err(api_internal)?;
                let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                    api_internal(format!("run path is not valid UTF-8: {}", path.display()))
                })?;
                if !path.is_dir() {
                    continue;
                }
                if !run_meta_dir(&path).join("journal.jsonl").is_file() {
                    continue;
                }
                let summary = run_summary(&path).map_err(api_internal)?;
                let run_id = summary.run_id;
                let artifacts = read_optional_output_manifest(&path).map_err(api_internal)?;
                let contract_sha256 = read_run_contract_sha256(&path).map_err(api_internal)?;
                let run_state = fold_run_state(&path).map_err(api_internal)?;
                runs.push(RunSnapshot {
                    run_id,
                    state: status_from_journal(&summary.status).map_err(api_internal)?,
                    seq: run_state.last_seq,
                    contract_sha256,
                    artifacts,
                    question: None,
                    confirm: None,
                });
            }
            runs.sort_by(|left, right| left.run_id.cmp(&right.run_id));
        }
        for memory_run in self.runs_from_memory().await? {
            if !runs.iter().any(|run| run.run_id == memory_run.run_id) {
                runs.push(memory_run);
            }
        }
        Ok(runs)
    }

    pub async fn list_run_items(&self) -> Result<Vec<RunListItem>, ApiError> {
        let mut items = Vec::new();
        if !self.inner.runs_dir.exists() {
            return Ok(items);
        }
        for entry in std::fs::read_dir(&self.inner.runs_dir).map_err(api_internal)? {
            let entry = entry.map_err(api_internal)?;
            let run_dir = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                api_internal(format!("run path is not valid UTF-8: {}", path.display()))
            })?;
            if !run_dir.is_dir() || !run_meta_dir(&run_dir).join("journal.jsonl").is_file() {
                continue;
            }
            let summary = run_summary(&run_dir).map_err(api_internal)?;
            let seq = fold_run_state(&run_dir).map_err(api_internal)?.last_seq;
            items.push(RunListItem {
                run_id: summary.run_id,
                state: status_from_journal(&summary.status).map_err(api_internal)?,
                generator_id: summary.generator,
                started_at: summary.started_at,
                seq,
            });
        }
        items.sort_by(|left, right| left.run_id.cmp(&right.run_id));
        Ok(items)
    }

    async fn runs_from_memory(&self) -> Result<Vec<RunSnapshot>, ApiError> {
        let runs = self.inner.runs.read().await;
        let mut snapshots = Vec::with_capacity(runs.len());
        for (run_id, record) in runs.iter() {
            snapshots.push(RunSnapshot {
                run_id: run_id.clone(),
                state: record.state,
                seq: fold_run_state(&record.run_dir)
                    .map_err(api_internal)?
                    .last_seq,
                contract_sha256: Some(record.contract_sha256.clone()),
                artifacts: record.artifacts.clone(),
                question: record.question.clone(),
                confirm: record.confirm.clone(),
            });
        }
        Ok(snapshots)
    }

    fn load_generator(&self, id: &str) -> Result<Contract, ApiError> {
        if !is_safe_id(id) {
            return Err(api_bad_request(format!(
                "generator id `{id}` is not allowed"
            )));
        }
        for root in &self.inner.generator_roots {
            let path = root.join(id);
            if path.join("qcg.toml").exists() {
                return Contract::load(path).map_err(api_internal);
            }
        }
        Err(api_not_found(format!(
            "generator `{id}` was not found in any generator root: {}",
            self.inner
                .generator_roots
                .iter()
                .map(|root| root.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    fn spawn_engine_run(&self, request: SpawnRun) {
        let service = self.clone();
        let SpawnRun {
            run_id,
            contract,
            inputs,
            run_dir,
            events,
            answers,
            confirmations,
            cancellation,
            task,
        } = request;
        let runtime = Arc::clone(&self.inner.llm_runtime);
        let handle = tokio::spawn(async move {
            let engine = Engine::new(app_registry(Arc::clone(&runtime)));
            let progress = engine
                .advance_with_id(
                    run_id.clone(),
                    run_meta_dir(&run_dir),
                    contract,
                    inputs,
                    RunOptions {
                        output_dir: run_workspace_dir(&run_dir),
                        json_events: false,
                        event_sender: Some(events.clone()),
                        interactive: false,
                        answers,
                        confirmations,
                        max_total_steps: RunOptions::default_max_total_steps(),
                        max_parallel_steps: RunOptions::default_max_parallel_steps(),
                        llm_provider: Some(Arc::clone(&runtime.provider)),
                        llm_seed_override: None,
                        cancellation,
                    },
                )
                .await;
            service.finish_run(run_id, progress).await;
        });
        tokio::spawn(async move {
            *task.lock().await = Some(handle);
        });
    }

    pub async fn run_generator_path(&self, run: DirectRun) -> Result<OutputManifest, ApiError> {
        let contract = Contract::load(&run.generator_path).map_err(api_internal)?;
        let runtime = Arc::clone(&self.inner.llm_runtime);
        let run_id = direct_run_id(&run.output_dir);
        let metadata_dir = direct_run_meta_dir(&run.output_dir);
        Engine::new(app_registry(Arc::clone(&runtime)))
            .run_with_id(
                run_id.clone(),
                metadata_dir,
                contract,
                run.inputs,
                RunOptions {
                    output_dir: run.output_dir,
                    json_events: run.json_events,
                    event_sender: None,
                    interactive: run.interactive,
                    answers: run.answers,
                    confirmations: run.confirmations,
                    max_total_steps: RunOptions::default_max_total_steps(),
                    max_parallel_steps: RunOptions::default_max_parallel_steps(),
                    llm_provider: Some(Arc::clone(&runtime.provider)),
                    llm_seed_override: run.llm_seed_override,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .map_err(api_internal)
    }

    pub async fn run_generator_path_with_events(
        &self,
        run: DirectRun,
    ) -> Result<DirectRunEvents, ApiError> {
        let contract = Contract::load(&run.generator_path).map_err(api_internal)?;
        let runtime = Arc::clone(&self.inner.llm_runtime);
        let (events, mut receiver) = broadcast::channel(512);
        let run_id = direct_run_id(&run.output_dir);
        let metadata_dir = direct_run_meta_dir(&run.output_dir);
        let manifest = Engine::new(app_registry(Arc::clone(&runtime)))
            .run_with_id(
                run_id.clone(),
                metadata_dir.clone(),
                contract,
                run.inputs,
                RunOptions {
                    output_dir: run.output_dir.clone(),
                    json_events: false,
                    event_sender: Some(events),
                    interactive: run.interactive,
                    answers: run.answers,
                    confirmations: run.confirmations,
                    max_total_steps: RunOptions::default_max_total_steps(),
                    max_parallel_steps: RunOptions::default_max_parallel_steps(),
                    llm_provider: Some(Arc::clone(&runtime.provider)),
                    llm_seed_override: run.llm_seed_override,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .map_err(api_internal)?;
        let mut collected = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(event) => collected.push(event),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    let last_seq = collected.last().map_or(0, |event| event.seq);
                    collected.push(RunEvent::lagged(
                        run_id.clone(),
                        last_seq.saturating_add(skipped),
                    ));
                }
                Err(broadcast::error::TryRecvError::Closed) => break,
            }
        }
        let journal_events = read_events_from_meta(&metadata_dir)
            .map_err(api_internal)?
            .into_iter()
            .map(|event| RunEvent::from_flat(&event, &run_id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(api_internal)?;
        if collected.is_empty() {
            collected = journal_events;
        } else if event_kinds(&collected) != event_kinds(&journal_events) {
            return Err(api_internal(format!(
                "direct run event stream diverged from journal in `{}`",
                run.output_dir
            )));
        }
        Ok(DirectRunEvents {
            manifest,
            events: collected,
        })
    }

    async fn finish_run(&self, run_id: String, progress: Progress) {
        let mut runs = self.inner.runs.write().await;
        if let Some(record) = runs.get_mut(&run_id) {
            if record.state == RunStatus::Canceled {
                return;
            }
            match progress {
                Progress::Done(outputs) => {
                    record.state = RunStatus::Succeeded;
                    record.artifacts = Some(outputs);
                    record.question = None;
                    record.confirm = None;
                }
                Progress::Suspended(Interaction::Question { question }) => {
                    record.state = RunStatus::Waiting;
                    record.question = Some(question.clone());
                    record.confirm = None;
                    let _ = write_run_event(
                        record,
                        "run_waiting",
                        json!({ "question_id": question.id, "question": question }),
                    );
                }
                Progress::Suspended(Interaction::Confirmation { confirm }) => {
                    record.state = RunStatus::Confirming;
                    record.question = None;
                    record.confirm = Some(confirm.clone());
                    let _ =
                        write_run_event(record, "confirm_request", json!({ "confirm": confirm }));
                }
                Progress::Failed(error) => {
                    record.state = if error.kind == RunFailureKind::Canceled {
                        RunStatus::Canceled
                    } else {
                        RunStatus::Failed
                    };
                    record.question = None;
                    record.confirm = None;
                    let _ = write_run_event(record, "run_error", json!({ "error": error.message }));
                }
                Progress::Advanced => record.state = RunStatus::Running,
            }
        }
    }
}

fn write_run_event(record: &RunRecord, kind: &str, payload: Value) -> Result<(), ServiceError> {
    JournalWriter::create(
        &run_meta_dir(&record.run_dir).join("journal.jsonl"),
        false,
        Some(record.events.clone()),
    )
    .map_err(|error| ServiceError::Invalid(error.to_string()))?
    .event(kind, payload)
    .map_err(|error| ServiceError::Invalid(error.to_string()))
}

fn direct_run_id(workspace: &Utf8Path) -> String {
    let digest = hex::encode(Sha256::digest(workspace.as_str().as_bytes()));
    format!("direct-{}", &digest[..16])
}

pub fn direct_run_meta_dir(workspace: &Utf8Path) -> Utf8PathBuf {
    let run_id = direct_run_id(workspace);
    workspace
        .parent()
        .unwrap_or_else(|| Utf8Path::new("."))
        .join(".qcg/runs")
        .join(&run_id)
        .join("meta")
}

fn app_registry(runtime: Arc<qcg_llm::LlmRuntime>) -> qcg_engine::StepRegistry {
    let mut registry = deterministic_registry();
    qcg_llm_steps::register_llm_steps(&mut registry, runtime);
    registry
}

pub fn built_in_step_param_schemas() -> BTreeMap<String, Value> {
    let mut registry = deterministic_registry();
    qcg_llm_steps::register_fake_llm_steps(&mut registry);
    registry.params_schemas()
}

pub fn step_param_schemas_markdown() -> Result<String, ServiceError> {
    let mut markdown = String::new();
    for (step_type, schema) in built_in_step_param_schemas() {
        markdown.push_str("### `");
        markdown.push_str(&step_type);
        markdown.push_str("`\n\n```json\n");
        markdown.push_str(&serde_json::to_string_pretty(&schema)?);
        markdown.push_str("\n```\n\n");
    }
    Ok(markdown)
}

#[derive(Debug)]
struct SpawnRun {
    run_id: String,
    contract: Contract,
    inputs: BTreeMap<String, Value>,
    run_dir: Utf8PathBuf,
    events: broadcast::Sender<RunEvent>,
    answers: BTreeMap<String, Value>,
    confirmations: BTreeMap<String, bool>,
    cancellation: CancellationToken,
    task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl LocalQcgService {
    pub async fn list_generators(&self) -> Vec<GeneratorSummary> {
        let mut generators = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for root in &self.inner.generator_roots {
            if !root.exists() {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(root) else {
                continue;
            };
            for entry in entries.flatten() {
                let Ok(path) = Utf8PathBuf::from_path_buf(entry.path()) else {
                    continue;
                };
                if !path.join("qcg.toml").exists() {
                    continue;
                }
                let Ok(contract) = Contract::load(&path) else {
                    continue;
                };
                if seen.insert(contract.manifest.generator.id.clone()) {
                    generators.push(GeneratorSummary {
                        id: contract.manifest.generator.id,
                        name: contract.manifest.generator.name,
                        version: contract.manifest.generator.version,
                        description: contract.manifest.generator.description,
                    });
                }
            }
        }
        generators
    }

    pub async fn describe(&self, id: &str) -> Result<GeneratorDetail, ApiError> {
        let contract = self.load_generator(id)?;
        Ok(GeneratorDetail {
            generator: contract.manifest.generator,
            inputs: contract.manifest.inputs,
            assets: contract.manifest.assets,
        })
    }

    pub async fn read_generator_asset(
        &self,
        id: String,
        path: String,
    ) -> Result<Vec<u8>, ApiError> {
        let contract = self.load_generator(&id)?;
        if !is_safe_relative_path(&path)
            || path.to_ascii_lowercase().contains("%2e")
            || path.to_ascii_lowercase().contains("%2f")
            || path.to_ascii_lowercase().contains("%5c")
        {
            return Err(api_bad_request(format!(
                "generator asset path `{path}` is not allowed"
            )));
        }
        let exact = contract
            .manifest
            .assets
            .files
            .iter()
            .any(|file| file == &path);
        let in_dir = contract.manifest.assets.dirs.iter().any(|dir| {
            path.strip_prefix(dir)
                .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1)
        });
        if !exact && !in_dir {
            return Err(api_not_found(format!(
                "generator asset `{path}` was not declared"
            )));
        }
        let root = dunce::canonicalize(&contract.root).map_err(api_internal)?;
        let requested = match dunce::canonicalize(contract.root.join(&path)) {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(api_not_found(format!(
                    "generator asset `{path}` was not found"
                )));
            }
            Err(error) => return Err(api_internal(error)),
        };
        if !requested.starts_with(&root) || !requested.is_file() {
            return Err(api_not_found(format!(
                "generator asset `{path}` was not found"
            )));
        }
        tokio::fs::read(requested).await.map_err(api_internal)
    }

    pub async fn start_run(&self, req: StartRun) -> Result<String, ApiError> {
        let contract = self.load_generator(&req.generator_id)?;
        match contract.manifest.resolve_inputs(req.inputs.clone()) {
            Ok(_) => {}
            Err(ContractError::PayloadTooLarge {
                actual_bytes,
                limit_bytes,
                ..
            }) => {
                return Err(ApiError::TooLarge {
                    actual_bytes,
                    limit_bytes,
                });
            }
            Err(error) => return Err(ApiError::invalid_field("inputs", error.to_string())),
        }
        let run_id = format!("{}-{}", req.generator_id, uuid::Uuid::now_v7());
        let run_dir = self.inner.runs_dir.join(&run_id);
        let (events, _) = broadcast::channel(512);
        let cancellation = CancellationToken::new();
        let task = Arc::new(tokio::sync::Mutex::new(None));
        let inputs = req.inputs;
        self.inner.runs.write().await.insert(
            run_id.clone(),
            RunRecord {
                contract: contract.clone(),
                contract_sha256: contract.sha256.clone(),
                inputs: inputs.clone(),
                answers: BTreeMap::new(),
                confirmations: BTreeMap::new(),
                state: RunStatus::Running,
                run_dir: run_dir.clone(),
                artifacts: None,
                question: None,
                confirm: None,
                events: events.clone(),
                cancellation: cancellation.clone(),
                task: task.clone(),
            },
        );
        self.spawn_engine_run(SpawnRun {
            run_id: run_id.clone(),
            contract,
            inputs,
            run_dir,
            events,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            cancellation,
            task,
        });
        Ok(run_id)
    }

    pub async fn snapshot(&self, id: String) -> Result<RunSnapshot, ApiError> {
        let memory = self.inner.runs.read().await.get(&id).cloned();
        let run_dir = match &memory {
            Some(record) => record.run_dir.clone(),
            None => self.run_dir_for(&id).await?,
        };
        let artifacts = match memory.as_ref().and_then(|record| record.artifacts.clone()) {
            Some(artifacts) => Some(artifacts),
            None => read_optional_output_manifest(&run_dir).map_err(api_internal)?,
        };
        let disk_state = fold_run_state(&run_dir).map_err(api_internal)?;
        let state = memory
            .as_ref()
            .map(|record| record.state)
            .unwrap_or_else(|| {
                disk_state
                    .terminal
                    .as_ref()
                    .map_or(RunStatus::Interrupted, |terminal| match terminal {
                        qcg_engine::TerminalState::Succeeded => RunStatus::Succeeded,
                        qcg_engine::TerminalState::Failed => RunStatus::Failed,
                        qcg_engine::TerminalState::Canceled => RunStatus::Canceled,
                        qcg_engine::TerminalState::Interrupted => RunStatus::Interrupted,
                    })
            });
        let contract_sha256 = match memory.as_ref() {
            Some(record) => Some(record.contract_sha256.clone()),
            None => read_run_contract_sha256(&run_dir).map_err(api_internal)?,
        };
        Ok(RunSnapshot {
            run_id: id,
            state,
            seq: disk_state.last_seq,
            contract_sha256,
            artifacts,
            question: memory.as_ref().and_then(|record| record.question.clone()),
            confirm: memory.and_then(|record| record.confirm),
        })
    }

    pub async fn subscribe(&self, id: String) -> Result<BoxStream<'static, RunEvent>, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let live_receiver = self.live_receiver(&id).await.ok();
        let history = read_run_events(&run_dir).map_err(ApiError::from)?;
        let history_last_seq = history.last().map_or(0, |event| event.seq);
        let history_stream = futures_util::stream::iter(history);
        let lagged_run_id = id.clone();
        let live_stream = match live_receiver {
            Some(receiver) => {
                let mut delivered_seq = history_last_seq;
                BroadcastStream::new(receiver)
                    .filter_map(move |event| {
                        let run_id = lagged_run_id.clone();
                        let result = match event {
                            Ok(event) if event.seq > delivered_seq => {
                                delivered_seq = event.seq;
                                Some(event)
                            }
                            Ok(_) => None,
                            Err(
                                tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(
                                    skipped,
                                ),
                            ) => {
                                delivered_seq = delivered_seq.saturating_add(skipped);
                                Some(RunEvent::lagged(run_id, delivered_seq))
                            }
                        };
                        async move { result }
                    })
                    .boxed()
            }
            None => futures_util::stream::empty().boxed(),
        };
        Ok(history_stream.chain(live_stream).boxed())
    }

    pub async fn answer(
        &self,
        id: String,
        question_id: String,
        payload: AnswerPayload,
    ) -> Result<(), ApiError> {
        let answer = json!(payload.values);
        let (contract, inputs, answers, confirmations, run_dir, events, cancellation, task) = {
            let mut runs = self.inner.runs.write().await;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
            if record.state != RunStatus::Waiting {
                return Err(api_bad_request(format!(
                    "run `{id}` is not waiting for user input"
                )));
            }
            let question = record
                .question
                .as_ref()
                .ok_or_else(|| api_bad_request(format!("run `{id}` has no question")))?;
            if question.id != question_id {
                return Err(api_bad_request(format!(
                    "answer was for `{}`, but run is waiting for `{}`",
                    question_id, question.id
                )));
            }
            validate_form_values(&question.fields, &answer).map_err(|error| {
                ApiError::invalid_field("values", format!("invalid form answer: {error}"))
            })?;
            record.answers.insert(question_id, answer);
            record.state = RunStatus::Running;
            record.question = None;
            record.confirm = None;
            record.artifacts = None;
            let cancellation = CancellationToken::new();
            record.cancellation = cancellation.clone();
            (
                record.contract.clone(),
                record.inputs.clone(),
                record.answers.clone(),
                record.confirmations.clone(),
                record.run_dir.clone(),
                record.events.clone(),
                cancellation,
                record.task.clone(),
            )
        };
        self.spawn_engine_run(SpawnRun {
            run_id: id,
            contract,
            inputs,
            run_dir,
            events,
            answers,
            confirmations,
            cancellation,
            task,
        });
        Ok(())
    }

    pub async fn confirm(
        &self,
        id: String,
        confirmation_id: String,
        decision: ConfirmDecision,
    ) -> Result<(), ApiError> {
        let (contract, inputs, answers, confirmations, run_dir, events, cancellation, task) = {
            let mut runs = self.inner.runs.write().await;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
            if record.state != RunStatus::Confirming {
                return Err(api_bad_request(format!(
                    "run `{id}` is not waiting for side-effect confirmation"
                )));
            }
            let confirm = record
                .confirm
                .clone()
                .ok_or_else(|| api_bad_request(format!("run `{id}` has no confirmation")))?;
            if confirm.id != confirmation_id {
                return Err(ApiError::Conflict {
                    detail: format!(
                        "confirmation was for `{confirmation_id}`, but run is waiting for `{}`",
                        confirm.id
                    ),
                });
            }
            if decision.decision == ConfirmationDecision::Deny {
                record.state = RunStatus::Failed;
                record.confirm = None;
                write_run_event(
                    record,
                    "side_effect",
                    json!({
                        "kind": confirm.kind,
                        "target": confirm.target,
                        "decision": "denied_by_user",
                    }),
                )
                .map_err(api_internal)?;
                write_run_event(
                    record,
                    "run_finished",
                    json!({
                        "status": "failed",
                        "reason": FailureDetail::new(
                            FailureCode::ExecutionFailed,
                            "side effect denied by user",
                        ),
                    }),
                )
                .map_err(api_internal)?;
                return Ok(());
            }
            record.confirmations.insert(confirm.id, true);
            record.state = RunStatus::Running;
            record.confirm = None;
            record.artifacts = None;
            let cancellation = CancellationToken::new();
            record.cancellation = cancellation.clone();
            (
                record.contract.clone(),
                record.inputs.clone(),
                record.answers.clone(),
                record.confirmations.clone(),
                record.run_dir.clone(),
                record.events.clone(),
                cancellation,
                record.task.clone(),
            )
        };
        self.spawn_engine_run(SpawnRun {
            run_id: id,
            contract,
            inputs,
            run_dir,
            events,
            answers,
            confirmations,
            cancellation,
            task,
        });
        Ok(())
    }

    pub async fn cancel(&self, id: String) -> Result<(), ApiError> {
        let mut runs = self.inner.runs.write().await;
        let record = runs
            .get_mut(&id)
            .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
        if record.state.is_terminal() {
            return Ok(());
        }
        record.cancellation.cancel();
        record.state = RunStatus::Canceled;
        record.question = None;
        record.confirm = None;
        record.artifacts = None;
        write_run_event(
            record,
            "run_canceled",
            json!({
                "reason": FailureDetail::new(
                    FailureCode::Canceled,
                    "cancellation requested",
                ),
            }),
        )
        .map_err(api_internal)?;
        Ok(())
    }

    pub async fn shutdown_active_runs(&self) -> Result<(), ApiError> {
        let active = self
            .inner
            .runs
            .read()
            .await
            .iter()
            .filter(|(_, record)| !record.state.is_terminal())
            .map(|(id, record)| (id.clone(), Arc::clone(&record.task)))
            .collect::<Vec<_>>();
        for (id, _) in &active {
            self.cancel(id.clone()).await?;
        }
        for (id, task) in active {
            let Some(handle) = task.lock().await.take() else {
                continue;
            };
            match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(api_internal(format!(
                        "run `{id}` task failed during shutdown: {error}"
                    )));
                }
                Err(_) => {
                    return Err(api_internal(format!(
                        "run `{id}` did not stop within the shutdown deadline"
                    )));
                }
            }
        }
        Ok(())
    }

    pub async fn artifacts(&self, id: String) -> Result<OutputManifest, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        read_output_manifest(&run_meta_dir(&run_dir)).map_err(api_internal)
    }

    pub async fn read_artifact(
        &self,
        id: String,
        path: String,
    ) -> Result<(OutputArtifact, Vec<u8>), ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let manifest = read_output_manifest(&run_meta_dir(&run_dir)).map_err(api_internal)?;
        let artifact = manifest
            .artifacts
            .into_iter()
            .find(|artifact| artifact.path == path)
            .ok_or_else(|| api_not_found(format!("artifact `{path}` was not found")))?;
        let resolved = resolve_artifact_path(&run_workspace_dir(&run_dir), &artifact.path)
            .map_err(api_internal)?;
        let bytes = tokio::fs::read(resolved).await.map_err(api_internal)?;
        Ok((artifact, bytes))
    }

    pub async fn read_journal(&self, id: String) -> Result<String, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        tokio::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
            .await
            .map_err(api_internal)
    }
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub run_id: String,
    pub status: String,
    pub generator: String,
    pub generator_path: Value,
    pub contract_sha256: Value,
    pub inputs: Value,
    pub started_at: String,
    pub finished_at: String,
    pub artifacts: Value,
    pub retain_days: Option<u32>,
}

impl RunSummary {
    pub fn to_json(&self) -> Value {
        json!({
            "run_id": self.run_id,
            "status": self.status,
            "generator": self.generator,
            "generator_path": self.generator_path,
            "contract_sha256": self.contract_sha256,
            "inputs": summarize_file_values(&self.inputs),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "artifacts": self.artifacts,
            "retain_days": self.retain_days,
        })
    }
}

fn summarize_file_values(inputs: &Value) -> Value {
    let Some(inputs) = inputs.as_object() else {
        return inputs.clone();
    };
    Value::Object(
        inputs
            .iter()
            .map(|(field, value)| {
                let summary = FileValue::from_value(value)
                    .and_then(|file| {
                        let bytes = file.decode()?;
                        Ok(json!({
                            "name": file.name,
                            "bytes": bytes.len(),
                            "sha256": hex::encode(Sha256::digest(&bytes)),
                        }))
                    })
                    .unwrap_or_else(|_| value.clone());
                (field.clone(), summary)
            })
            .collect(),
    )
}

pub fn run_meta_dir(run_dir: &Utf8Path) -> Utf8PathBuf {
    run_dir.join("meta")
}

pub fn run_workspace_dir(run_dir: &Utf8Path) -> Utf8PathBuf {
    run_dir.join("workspace")
}

fn fold_run_state(run_dir: &Utf8Path) -> Result<RunState, ServiceError> {
    RunState::fold_journal(&run_meta_dir(run_dir).join("journal.jsonl"))
        .map_err(|error| ServiceError::Invalid(error.to_string()))
}

fn read_optional_output_manifest(
    run_dir: &Utf8Path,
) -> Result<Option<OutputManifest>, ServiceError> {
    match read_output_manifest(&run_meta_dir(run_dir)) {
        Ok(manifest) => Ok(Some(manifest)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

fn status_from_journal(status: &str) -> Result<RunStatus, ServiceError> {
    match status {
        "running" => Ok(RunStatus::Running),
        "waiting" => Ok(RunStatus::Waiting),
        "confirming" => Ok(RunStatus::Confirming),
        "success" => Ok(RunStatus::Succeeded),
        "failed" => Ok(RunStatus::Failed),
        "canceled" => Ok(RunStatus::Canceled),
        "interrupted" => Ok(RunStatus::Interrupted),
        _ => Err(ServiceError::Invalid(format!(
            "unknown journal run status `{status}`"
        ))),
    }
}

fn rehydrate_runs(runs_dir: &Utf8Path) -> Result<BTreeMap<String, RunRecord>, ServiceError> {
    let mut records = BTreeMap::new();
    if !runs_dir.exists() {
        return Ok(records);
    }
    for entry in std::fs::read_dir(runs_dir)? {
        let entry = entry?;
        let run_dir = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
        })?;
        let journal_path = run_meta_dir(&run_dir).join("journal.jsonl");
        if !run_dir.is_dir() || !journal_path.is_file() {
            continue;
        }
        let mut state = RunState::fold_journal(&journal_path)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        if state.terminal.is_some() {
            continue;
        }
        let run_id = run_dir
            .file_name()
            .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?
            .to_string();
        let (record_state, question, confirm) = match state.pending.take() {
            Some(Interaction::Question { question }) => (RunStatus::Waiting, Some(question), None),
            Some(Interaction::Confirmation { confirm }) => {
                (RunStatus::Confirming, None, Some(confirm))
            }
            None => {
                JournalWriter::create(&journal_path, false, None)
                    .map_err(|error| ServiceError::Invalid(error.to_string()))?
                    .event(
                        "run_interrupted",
                        json!({
                            "reason": FailureDetail::new(
                                FailureCode::SchedulerFailed,
                                "service restarted without a live task",
                            ),
                        }),
                    )
                    .map_err(|error| ServiceError::Invalid(error.to_string()))?;
                (RunStatus::Interrupted, None, None)
            }
        };
        if record_state == RunStatus::Interrupted {
            continue;
        }
        let generator_path = read_run_generator_path(&run_dir)?.ok_or_else(|| {
            ServiceError::Invalid(format!("run `{run_id}` has no generator_path"))
        })?;
        let contract = Contract::load(&generator_path)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        let inputs = read_run_inputs(&run_dir)?;
        let (events, _) = broadcast::channel(512);
        records.insert(
            run_id,
            RunRecord {
                contract_sha256: contract.sha256.clone(),
                contract,
                inputs,
                answers: BTreeMap::new(),
                confirmations: BTreeMap::new(),
                state: record_state,
                run_dir: run_dir.clone(),
                artifacts: read_optional_output_manifest(&run_dir)?,
                question,
                confirm,
                events,
                cancellation: CancellationToken::new(),
                task: Arc::new(tokio::sync::Mutex::new(None)),
            },
        );
    }
    Ok(records)
}

pub fn resolve_run_dir(runs_dir: &Utf8Path, id: &str) -> Result<Utf8PathBuf, ServiceError> {
    if id.contains('/') || id.contains('\\') || id == "." || id == ".." {
        return Err(ServiceError::Invalid(format!(
            "run id `{id}` is not allowed"
        )));
    }
    let run_dir = runs_dir.join(id);
    if !run_meta_dir(&run_dir).join("journal.jsonl").exists() {
        return Err(ServiceError::Invalid(format!(
            "run `{id}` was not found under `{runs_dir}`"
        )));
    }
    Ok(run_dir)
}

pub fn read_journal_events(run_dir: &Utf8Path) -> Result<Vec<Value>, ServiceError> {
    read_events_from_meta(&run_meta_dir(run_dir))
}

fn read_events_from_meta(meta_dir: &Utf8Path) -> Result<Vec<Value>, ServiceError> {
    let file = File::open(meta_dir.join("journal.jsonl"))?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event = serde_json::from_str::<Value>(&line).map_err(|error| {
            ServiceError::Invalid(format!(
                "invalid journal JSON at line {}: {error}",
                index + 1
            ))
        })?;
        events.push(event);
    }
    Ok(events)
}

pub fn read_run_events(run_dir: &Utf8Path) -> Result<Vec<RunEvent>, ServiceError> {
    let default_run_id = run_dir.file_name().unwrap_or("unscoped").to_string();
    read_journal_events(run_dir)?
        .into_iter()
        .map(|event| RunEvent::from_flat(&event, &default_run_id).map_err(ServiceError::Invalid))
        .collect()
}

pub fn read_run_generator_path(run_dir: &Utf8Path) -> Result<Option<Utf8PathBuf>, ServiceError> {
    let events = read_journal_events(run_dir)?;
    let Some(started) = events
        .iter()
        .find(|event| event.get("t").and_then(Value::as_str) == Some("run_started"))
    else {
        return Ok(None);
    };
    Ok(started
        .get("generator_path")
        .and_then(Value::as_str)
        .map(Utf8PathBuf::from))
}

fn read_run_contract_sha256(run_dir: &Utf8Path) -> Result<Option<String>, ServiceError> {
    Ok(read_run_events(run_dir)?
        .into_iter()
        .find(|event| event.kind == "run_started")
        .and_then(|event| {
            event
                .data
                .run_started()
                .map(|data| data.contract_sha256.clone())
        }))
}

pub fn read_run_inputs(run_dir: &Utf8Path) -> Result<BTreeMap<String, Value>, ServiceError> {
    let events = read_journal_events(run_dir)?;
    let started = events
        .iter()
        .find(|event| event.get("t").and_then(Value::as_str) == Some("run_started"))
        .ok_or_else(|| {
            ServiceError::Invalid(format!("run `{run_dir}` has no run_started event"))
        })?;
    let inputs = started
        .get("inputs")
        .cloned()
        .unwrap_or_else(|| Value::Object(Default::default()));
    Ok(serde_json::from_value(inputs)?)
}

pub fn run_summary(run_dir: &Utf8Path) -> Result<RunSummary, ServiceError> {
    let events = read_journal_events(run_dir)?;
    let started = events
        .iter()
        .find(|event| event.get("t").and_then(Value::as_str) == Some("run_started"))
        .ok_or_else(|| {
            ServiceError::Invalid(format!("run `{run_dir}` has no run_started event"))
        })?;
    let lifecycle = events
        .iter()
        .rev()
        .find(|event| {
            matches!(
                event.get("t").and_then(Value::as_str),
                Some(
                    "run_started"
                        | "run_waiting"
                        | "confirm_request"
                        | "run_finished"
                        | "run_error"
                        | "run_canceled"
                        | "run_interrupted"
                )
            )
        })
        .ok_or_else(|| ServiceError::Invalid("run has no lifecycle event".into()))?;
    let lifecycle_kind = lifecycle
        .get("t")
        .and_then(Value::as_str)
        .ok_or_else(|| ServiceError::Invalid("run lifecycle event has no string kind".into()))?;
    let status = match lifecycle_kind {
        "run_started" => "running",
        "run_waiting" => "waiting",
        "confirm_request" => "confirming",
        "run_error" => "failed",
        "run_canceled" => "canceled",
        "run_interrupted" => "interrupted",
        "run_finished" => lifecycle
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ServiceError::Invalid("run_finished event has no string status".into())
            })?,
        _ => return Err(ServiceError::Invalid("unsupported lifecycle event".into())),
    };
    let artifacts = read_optional_output_manifest(run_dir)?
        .map(|manifest| serde_json::to_value(manifest.artifacts))
        .transpose()?
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let retain_days = started
        .get("retain_days")
        .and_then(Value::as_u64)
        .map(u32::try_from)
        .transpose()
        .map_err(|_| ServiceError::Invalid("retain_days exceeds u32".into()))?;
    Ok(RunSummary {
        run_id: run_dir
            .file_name()
            .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?
            .to_string(),
        status: status.to_string(),
        generator: started
            .get("generator")
            .and_then(Value::as_str)
            .ok_or_else(|| ServiceError::Invalid("run_started has no generator".into()))?
            .to_string(),
        generator_path: started
            .get("generator_path")
            .cloned()
            .unwrap_or(Value::Null),
        contract_sha256: started
            .get("contract_sha256")
            .cloned()
            .unwrap_or(Value::Null),
        inputs: started.get("inputs").cloned().unwrap_or_else(|| json!({})),
        started_at: started
            .get("ts")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        finished_at: lifecycle
            .get("ts")
            .and_then(Value::as_str)
            .filter(|_| matches!(status, "success" | "failed" | "canceled"))
            .unwrap_or("")
            .to_string(),
        artifacts,
        retain_days,
    })
}

pub fn gc_run_directories(
    runs_dir: &Utf8Path,
    keep: usize,
    delete: bool,
) -> Result<Vec<Utf8PathBuf>, ServiceError> {
    if !runs_dir.exists() {
        return Ok(Vec::new());
    }
    let now = chrono::Utc::now();
    let mut candidates = Vec::new();
    for entry in std::fs::read_dir(runs_dir)? {
        let entry = entry?;
        let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
        })?;
        if !path.is_dir() || !run_meta_dir(&path).join("journal.jsonl").exists() {
            continue;
        }
        let summary = run_summary(&path)?;
        if !matches!(summary.status.as_str(), "success" | "failed" | "canceled") {
            continue;
        }
        let expired = summary.retain_days.is_some_and(|days| {
            chrono::DateTime::parse_from_rfc3339(&summary.started_at)
                .map(|started| {
                    started.with_timezone(&chrono::Utc)
                        < now - chrono::Duration::days(i64::from(days))
                })
                .unwrap_or(false)
        });
        candidates.push((summary.started_at, path, expired));
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0));
    let mut deleted = Vec::new();
    for (index, (_, path, expired)) in candidates.into_iter().enumerate() {
        if index < keep && !expired {
            continue;
        }
        if delete {
            std::fs::remove_dir_all(&path)?;
            deleted.push(path);
        }
    }
    Ok(deleted)
}

pub fn list_run_summaries(runs_dir: &Utf8Path) -> Result<Vec<RunSummary>, ServiceError> {
    if !runs_dir.exists() {
        return Ok(Vec::new());
    }
    let mut summaries = Vec::new();
    for entry in std::fs::read_dir(runs_dir)? {
        let entry = entry?;
        let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
        })?;
        if !path.is_dir() || !run_meta_dir(&path).join("journal.jsonl").exists() {
            continue;
        }
        summaries.push(run_summary(&path)?);
    }
    summaries.sort_by(|left, right| left.run_id.cmp(&right.run_id));
    Ok(summaries)
}

pub fn append_journal_event(run_dir: &Utf8Path, event: &Value) -> Result<(), ServiceError> {
    let meta_dir = run_meta_dir(run_dir);
    std::fs::create_dir_all(&meta_dir)?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(meta_dir.join("journal.jsonl"))?;
    let mut bytes = serde_json::to_vec(event)?;
    bytes.push(b'\n');
    file.write_all(&bytes)?;
    file.sync_data()?;
    Ok(())
}

pub fn read_artifacts_zip(run_dir: &Utf8Path) -> Result<Vec<u8>, ServiceError> {
    let mut cursor = Cursor::new(Vec::new());
    write_artifacts_zip_stream_with_limit(run_dir, &mut cursor, DEFAULT_ARTIFACT_ZIP_LIMIT_BYTES)?;
    Ok(cursor.into_inner())
}

pub fn write_artifacts_zip_stream<W: Write>(
    run_dir: &Utf8Path,
    writer: W,
) -> Result<(), ServiceError> {
    write_artifacts_zip_stream_with_limit(run_dir, writer, DEFAULT_ARTIFACT_ZIP_LIMIT_BYTES)
}

fn write_artifacts_zip_stream_with_limit<W: Write>(
    run_dir: &Utf8Path,
    writer: W,
    limit_bytes: u64,
) -> Result<(), ServiceError> {
    let manifest: OutputManifest = read_output_manifest(&run_meta_dir(run_dir))?;
    let artifact_paths = manifest
        .artifacts
        .into_iter()
        .map(|artifact| {
            let path = resolve_artifact_path(&run_workspace_dir(run_dir), &artifact.path)?;
            let bytes = std::fs::metadata(&path)?.len();
            Ok((artifact.path, path, bytes))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    let total_bytes = artifact_paths
        .iter()
        .try_fold(0_u64, |total, (_, _, bytes)| total.checked_add(*bytes))
        .ok_or_else(|| ServiceError::Invalid("artifact zip size overflowed".into()))?;
    if total_bytes > limit_bytes {
        return Err(ServiceError::Invalid(format!(
            "artifact zip source bytes exceed limit: {total_bytes} > {limit_bytes}"
        )));
    }
    let mut zip = zip::ZipWriter::new_stream(writer);
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    for (artifact_path, path, _) in artifact_paths {
        let mut file = File::open(path)?;
        zip.start_file(artifact_path, options)?;
        std::io::copy(&mut file, &mut zip)?;
    }
    zip.finish()?;
    Ok(())
}

fn api_bad_request(message: impl Into<String>) -> ApiError {
    ApiError::invalid(message)
}

fn api_not_found(message: impl Into<String>) -> ApiError {
    ApiError::not_found(message)
}

fn api_internal(error: impl std::fmt::Display) -> ApiError {
    ApiError::internal(error.to_string())
}

fn is_safe_id(id: &str) -> bool {
    !id.is_empty()
        && !id.starts_with('/')
        && !id.contains('\0')
        && !id.split('/').any(|part| part == ".." || part.is_empty())
}

fn event_kinds(events: &[RunEvent]) -> Vec<String> {
    events.iter().map(|event| event.kind.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use qcg_api::{ConfirmDecision, StartRun};
    use std::collections::BTreeSet;

    fn temp_run_dir(name: &str) -> Utf8PathBuf {
        let dir =
            std::env::temp_dir().join(format!("qcg-service-test-{name}-{}", std::process::id()));
        Utf8PathBuf::from_path_buf(dir).expect("temporary directory path must be UTF-8")
    }

    fn write_generator_package(root: &Utf8Path, id: &str) {
        let dir = root.join(id);
        std::fs::create_dir_all(&dir).expect("package directory should be created");
        std::fs::write(
            dir.join("qcg.toml"),
            format!(
                r#"
[generator]
id = "{id}"
name = "{id}"
version = "0.1.0"
qcg_version = "^0.1"

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "name"
required = true
type = "string"

[permissions]
"#
            ),
        )
        .expect("manifest should be written");
    }

    #[test]
    fn explicit_providers_path_is_loaded_without_fallback() {
        let root = temp_run_dir("explicit-providers");
        let _ = std::fs::remove_dir_all(&root);
        let providers_path = root.join("providers.toml");
        let provider_id = format!("explicit-{}", std::process::id());
        std::fs::create_dir_all(&root).expect("provider directory should be created");
        std::fs::write(
            &providers_path,
            format!(
                r#"
[[provider]]
id = "{provider_id}"
api = "chat_completions"
base_url = "http://127.0.0.1:9/v1"
"#
            ),
        )
        .expect("providers registry should be written");

        let service = LocalQcgService::new(
            root.join("generators"),
            root.join("runs"),
            Some(providers_path.clone()),
        )
        .expect("service should load the explicit providers registry");
        assert!(
            service
                .inner
                .llm_runtime
                .provider
                .capabilities_for(&provider_id)
                .is_some(),
            "the explicitly selected provider should be registered"
        );

        let missing = root.join("missing.toml");
        let error = LocalQcgService::new(
            root.join("generators"),
            root.join("other-runs"),
            Some(missing.clone()),
        )
        .expect_err("an explicit missing path must not fall back to another registry");
        assert!(error.to_string().contains(missing.as_str()));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn generator_roots_merge_with_first_root_winning() {
        let primary = temp_run_dir("roots-primary");
        let secondary = temp_run_dir("roots-secondary");
        let runs = temp_run_dir("roots-runs");
        let _ = std::fs::remove_dir_all(&primary);
        let _ = std::fs::remove_dir_all(&secondary);
        let _ = std::fs::remove_dir_all(&runs);
        write_generator_package(&primary, "shared");
        write_generator_package(&primary, "only-primary");
        // The secondary copy of `shared` must never shadow the primary one.
        write_generator_package(&secondary, "shared");
        write_generator_package(&secondary, "bundled-demo");

        let service = LocalQcgService::with_generator_roots(
            vec![primary.clone(), secondary.clone()],
            runs.clone(),
            None,
        )
        .expect("service should initialize");

        let mut listed: Vec<String> = service
            .list_generators()
            .await
            .into_iter()
            .map(|generator| generator.id)
            .collect();
        listed.sort();
        assert_eq!(listed, vec!["bundled-demo", "only-primary", "shared"]);

        let shared = service
            .load_generator("shared")
            .expect("primary should win");
        assert!(
            shared.root.starts_with(&primary),
            "duplicate id must resolve from the first root, got {}",
            shared.root
        );

        let fallback = service
            .load_generator("bundled-demo")
            .expect("secondary root should resolve");
        assert_eq!(fallback.manifest.generator.id, "bundled-demo");
        assert!(service.load_generator("absent").is_err());

        for dir in [primary, secondary, runs] {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn artifact_zip_rejects_sources_above_limit() {
        let run_dir = temp_run_dir("zip-limit");
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
        std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
        std::fs::write(run_workspace_dir(&run_dir).join("large.txt"), "abcdef")
            .expect("artifact should be written");
        std::fs::write(
            run_meta_dir(&run_dir).join("outputs.json"),
            serde_json::to_string(&json!({
                "artifacts": [{
                    "path": "large.txt",
                    "sha256": "unused",
                    "bytes": 6,
                    "label": "Large",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let mut bytes = Cursor::new(Vec::new());
        let error = write_artifacts_zip_stream_with_limit(&run_dir, &mut bytes, 5)
            .expect_err("zip generation should reject oversized artifacts");
        assert!(error.to_string().contains("exceed limit"));
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[tokio::test]
    async fn concurrent_start_runs_have_unique_ids_and_independent_outputs() {
        let root = temp_run_dir("concurrent-start");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "concurrent"
name = "Concurrent"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_write = ["workspace"]

[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "ok"

"#,
        )
        .expect("generator manifest should be written");
        let service =
            LocalQcgService::new(root.clone(), runs, None).expect("service should initialize");
        let results = futures_util::future::join_all((0..10).map(|_| {
            let service = service.clone();
            async move {
                service
                    .start_run(StartRun {
                        generator_id: "generator".into(),
                        inputs: BTreeMap::new(),
                    })
                    .await
            }
        }))
        .await;
        let ids: Vec<String> = results
            .into_iter()
            .map(|result| result.expect("concurrent run should start"))
            .collect();
        assert_eq!(ids.len(), BTreeSet::<String>::from_iter(ids.clone()).len());
        for _ in 0..100 {
            let snapshots =
                futures_util::future::join_all(ids.iter().cloned().map(|id| service.snapshot(id)))
                    .await;
            if snapshots.iter().all(|snapshot| {
                snapshot
                    .as_ref()
                    .is_ok_and(|snapshot| snapshot.state == RunStatus::Succeeded)
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let snapshots = service.list_runs().await.expect("runs should be listable");
        assert_eq!(
            snapshots
                .iter()
                .filter(|snapshot| {
                    ids.contains(&snapshot.run_id) && snapshot.state == RunStatus::Succeeded
                })
                .count(),
            10
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancel_stops_a_running_service_command_before_following_nodes() {
        let root = temp_run_dir("service-cancel");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "cancelable"
name = "Cancelable"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "cancellation test" }]

[[flow]]
id = "sleep"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]

[[flow]]
id = "must_not_run"
type = "write"
needs = ["sleep"]
artifact = { label = "Unexpected", required = false }
[flow.params]
output_file = "must-not-exist.txt"
content = "unexpected"

"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root, runs, None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
            })
            .await
            .expect("cancelable run should start");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        service
            .cancel(id.clone())
            .await
            .expect("cancel should succeed");
        let run_dir = service
            .run_dir_for(&id)
            .await
            .expect("run directory should exist");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
                    .unwrap_or_default();
                if journal.contains("\"t\":\"run_canceled\"") {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("canceled run should finish promptly");
        assert!(
            !run_workspace_dir(&run_dir)
                .join("must-not-exist.txt")
                .exists()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resume_after_confirmation_replays_prior_steps_and_runs_side_effect_once() {
        let service = LocalQcgService::new(
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators"),
            temp_run_dir("confirmation-resume"),
            None,
        )
        .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "side-effect-confirm".into(),
                inputs: BTreeMap::new(),
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
        let journal = service
            .read_journal(id)
            .await
            .expect("journal should be readable");
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
side_effects = "confirm"

[[permissions.commands]]
bin = "echo"
args = ["*"]
purpose = "record each confirmed iteration"
"#,
        )
        .expect("generator manifest should be written");
        let service =
            LocalQcgService::new(root.clone(), runs, None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([("items".into(), json!(["alpha", "beta"]))]),
            })
            .await
            .expect("foreach run should start");

        for (index, item) in ["alpha", "beta"].into_iter().enumerate() {
            let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
            let question = snapshot.question.expect("iteration should ask a question");
            assert_eq!(question.id, format!("each[{index}]/ask"));
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
            assert_eq!(confirm.id, format!("each[{index}]/effect:command"));
            assert_eq!(confirm.target, format!("echo {item}"));
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

        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Succeeded);
        let journal = service
            .read_journal(id)
            .await
            .expect("journal should be readable");
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
    async fn waiting_run_survives_gc_and_rehydrates_after_restart() {
        let runs = temp_run_dir("waiting-rehydrate");
        let _ = std::fs::remove_dir_all(&runs);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::new(generators.clone(), runs.clone(), None)
            .expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "ask-user".into(),
                inputs: BTreeMap::new(),
            })
            .await
            .expect("interactive run should start");
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = snapshot
            .question
            .expect("run should have a pending question");
        assert!(
            gc_run_directories(&runs, 0, true)
                .expect("GC should inspect runs")
                .is_empty(),
            "GC must not delete a waiting run"
        );
        assert!(runs.join(&id).is_dir());
        drop(service);

        let restored = LocalQcgService::new(generators, runs, None)
            .expect("service should rehydrate waiting run");
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

    #[cfg(unix)]
    #[tokio::test]
    async fn cancel_racing_with_an_answer_has_one_canceled_terminal_state() {
        let root = temp_run_dir("cancel-answer-race");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "cancel-answer-race"
name = "Cancel Answer Race"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 30"]

[permissions]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "hold resumed run" }]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root, runs, None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
            })
            .await
            .expect("run should start");
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = snapshot
            .question
            .expect("run should be waiting for an answer");
        let (cancel_result, answer_result) = tokio::join!(
            service.cancel(id.clone()),
            service.answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
        );
        cancel_result.expect("cancel should succeed");
        if let Err(error) = answer_result {
            assert!(error.to_string().contains("not waiting for user input"));
        }
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Canceled).await;
        assert_eq!(snapshot.state, RunStatus::Canceled);
        let journal = service
            .read_journal(id)
            .await
            .expect("journal should be readable");
        assert_eq!(journal.matches("\"t\":\"run_canceled\"").count(), 1);
        assert!(!journal.contains("\"status\":\"success\",\"t\":\"run_finished\""));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn missing_workspace_blocks_resume_without_repeating_side_effects() {
        let root = temp_run_dir("missing-workspace-resume");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "missing-workspace-resume"
name = "Missing Workspace Resume"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "write_before"
type = "write"

[flow.params]
output_file = "before.txt"
content = "before"

[[flow]]
id = "effect"
type = "command"

[flow.params]
command = ["echo", "effect"]

[[flow]]
id = "ask"
type = "ask_user"

[flow.params]
content = "Continue?"
options = ["yes"]

[permissions]
side_effects = "allowed"
commands = [{ bin = "echo", args = ["effect"], purpose = "resume safety proof" }]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root, runs, None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
            })
            .await
            .expect("run should start");
        let snapshot = wait_for_snapshot(&service, &id, RunStatus::Waiting).await;
        let question = snapshot
            .question
            .expect("run should wait after the side effect");
        let run_dir = service
            .run_dir_for(&id)
            .await
            .expect("run directory should exist");
        std::fs::remove_dir_all(run_workspace_dir(&run_dir))
            .expect("workspace should be removed for the recovery test");
        service
            .answer(
                id.clone(),
                question.id,
                AnswerPayload {
                    values: BTreeMap::from([("answer".into(), json!("yes"))]),
                },
            )
            .await
            .expect("answer should trigger a guarded resume");
        let snapshot = wait_for_terminal_snapshot(&service, &id).await;
        assert_eq!(snapshot.state, RunStatus::Failed);
        let journal = service
            .read_journal(id)
            .await
            .expect("journal should be readable");
        assert_eq!(
            journal
                .lines()
                .filter(|line| {
                    line.contains("\"t\":\"side_effect\"")
                        && line.contains("\"node\":\"effect\"")
                        && line.contains("\"decision\":\"allowed\"")
                })
                .count(),
            1,
            "resume failure must not repeat a completed side effect"
        );
        assert!(journal.contains("output `before.txt` is unavailable"));
    }

    async fn wait_for_snapshot(
        service: &LocalQcgService,
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
        panic!("run did not reach state `{state}`");
    }

    async fn wait_for_terminal_snapshot(service: &LocalQcgService, id: &str) -> RunSnapshot {
        for _ in 0..200 {
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

    #[test]
    fn artifact_zip_contains_manifest_artifacts() {
        let run_dir = temp_run_dir("zip-file");
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
        std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
        std::fs::write(run_workspace_dir(&run_dir).join("result.txt"), "ok")
            .expect("artifact should be written");
        std::fs::write(
            run_meta_dir(&run_dir).join("outputs.json"),
            serde_json::to_string(&json!({
                "artifacts": [{
                    "path": "result.txt",
                    "sha256": "unused",
                    "bytes": 2,
                    "label": "Result",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let bytes = read_artifacts_zip(&run_dir).expect("zip should be written");
        assert!(!run_dir.join("artifacts.zip.tmp").exists());
        assert!(!run_dir.join("artifacts.zip").exists());
        let mut archive =
            zip::ZipArchive::new(Cursor::new(bytes)).expect("zip archive should parse");
        let mut entry = archive
            .by_name("result.txt")
            .expect("artifact should be present");
        let mut text = String::new();
        std::io::Read::read_to_string(&mut entry, &mut text).expect("artifact should read as text");
        assert_eq!(text, "ok");
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn multi_field_answers_are_preserved_as_an_object() {
        let payload = AnswerPayload {
            values: BTreeMap::from([
                ("decision".into(), json!("keep")),
                ("reason".into(), json!("required context")),
            ]),
        };
        let answer = json!(payload.values);
        assert_eq!(answer["decision"], "keep");
        assert_eq!(answer["reason"], "required context");
    }
}
