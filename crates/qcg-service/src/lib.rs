use camino::{Utf8Path, Utf8PathBuf};
use fs2::FileExt as _;
use futures_util::{StreamExt as _, stream::BoxStream};
use qcg_api::{
    AnswerPayload, ApiError, ConfirmDecision, ConfirmationDecision, ForkRun, ForkStatePatch,
    GeneratorDetail, GeneratorSummary, McpAuthorizationStart, McpServerList, McpServerSummary,
    RunListItem, RunSnapshot, RunStatus, StartRun,
};
use qcg_contract::{Contract, ContractError, PackagePathError, validate_form_values};
use qcg_engine::{
    ConfirmSpec, Engine, FailureCode, FailureDetail, FormSpec, Interaction, JournalLimits,
    JournalWriter, OutputArtifact, OutputManifest, Progress, RunEvent, RunFailureKind, RunOptions,
    RunState, append_serialized_json_line, read_journal_values, read_output_manifest,
    resolve_artifact_path, serialize_bounded,
};
use qcg_steps::deterministic_registry;
use qcg_types::{FileValue, RunCompletionStatus, RunEventData, is_safe_relative_path};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, SeekFrom, Write};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncSeekExt as _};
use tokio::sync::{RwLock, Semaphore, broadcast};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use zip::write::SimpleFileOptions;

const DEFAULT_ARTIFACT_ZIP_LIMIT_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_ARTIFACT_ZIP_ENTRY_LIMIT: usize = 10_000;
const DEFAULT_GENERATOR_ASSET_RESPONSE_LIMIT_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_ACTIVE_RUNS: usize = 8;
pub const DEFAULT_MAX_TRACKED_RUNS: usize = 4_096;
const MAX_DIRECTORY_SCAN_ENTRIES: usize = 100_000;
const JOURNAL_POLL_CHANNEL_CAPACITY: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStoreMode {
    Exclusive,
    SharedFilesystem,
}

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
    execution_permits: Arc<Semaphore>,
    max_tracked_runs: usize,
    run_store_mode: RunStoreMode,
    _runs_lock: Option<File>,
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
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
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
        Self::with_generator_roots_and_max_active_runs(
            generator_roots,
            runs_dir,
            providers_path,
            DEFAULT_MAX_ACTIVE_RUNS,
        )
    }

    pub fn with_generator_roots_and_max_active_runs(
        generator_roots: Vec<Utf8PathBuf>,
        runs_dir: Utf8PathBuf,
        providers_path: Option<Utf8PathBuf>,
        max_active_runs: usize,
    ) -> Result<Self, ServiceError> {
        Self::with_generator_roots_max_active_runs_and_store_mode(
            generator_roots,
            runs_dir,
            providers_path,
            max_active_runs,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::Exclusive,
        )
    }

    pub fn with_generator_roots_max_active_runs_and_store_mode(
        generator_roots: Vec<Utf8PathBuf>,
        runs_dir: Utf8PathBuf,
        providers_path: Option<Utf8PathBuf>,
        max_active_runs: usize,
        max_tracked_runs: usize,
        run_store_mode: RunStoreMode,
    ) -> Result<Self, ServiceError> {
        if max_active_runs == 0 {
            return Err(ServiceError::Invalid(
                "max_active_runs must be greater than zero".into(),
            ));
        }
        if max_tracked_runs < max_active_runs {
            return Err(ServiceError::Invalid(format!(
                "max_tracked_runs must be at least max_active_runs ({max_active_runs})"
            )));
        }
        let mut roots = generator_roots;
        if roots.is_empty() {
            roots.push(Utf8PathBuf::from("generators"));
        }
        std::fs::create_dir_all(&runs_dir)?;
        let runs_lock = match run_store_mode {
            RunStoreMode::Exclusive => Some(lock_runs_directory(&runs_dir)?),
            RunStoreMode::SharedFilesystem => None,
        };
        let runs = rehydrate_runs(&runs_dir, max_tracked_runs)?;
        let llm_runtime = Arc::new(
            match qcg_llm::LlmRouter::load_optional(providers_path.as_deref()) {
                Ok(Some(router)) => router.into_runtime(),
                // No registry was found: built-in capabilities stay available
                // while other ids receive setup guidance during validation.
                Ok(None) => qcg_llm::LlmRuntime::builtins(),
                Err(error) => return Err(ServiceError::Invalid(error.to_string())),
            },
        );
        Ok(Self {
            inner: Arc::new(LocalQcgServiceInner {
                generator_roots: roots,
                runs_dir,
                runs: RwLock::new(runs),
                llm_runtime,
                execution_permits: Arc::new(Semaphore::new(max_active_runs)),
                max_tracked_runs,
                run_store_mode,
                _runs_lock: runs_lock,
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
        let service = self.clone();
        Some(tokio::spawn(async move {
            service.collect_retained_runs().await;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(24 * 60 * 60));
            interval.tick().await;
            loop {
                interval.tick().await;
                service.collect_retained_runs().await;
            }
        }))
    }

    pub fn start_shared_store_recovery(&self) -> Option<JoinHandle<()>> {
        if self.inner.run_store_mode != RunStoreMode::SharedFilesystem {
            return None;
        }
        let service = self.clone();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Err(error) = service.refresh_shared_runs().await {
                    tracing::error!(%error, "failed to refresh shared run store");
                    continue;
                }
                service.resume_recovered_runs().await;
            }
        }))
    }

    async fn refresh_shared_runs(&self) -> Result<(), ServiceError> {
        let recovered = rehydrate_runs(&self.inner.runs_dir, self.inner.max_tracked_runs)?;
        let mut runs = self.inner.runs.write().await;
        runs.retain(|_, record| !record.state.is_terminal());
        for (run_id, record) in recovered {
            runs.entry(run_id).or_insert(record);
        }
        for record in runs.values_mut() {
            if record
                .task
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_some()
            {
                continue;
            }
            let state = fold_run_state(&record.run_dir)?;
            if let Some(terminal) = state.terminal {
                record.state = match terminal {
                    qcg_engine::TerminalState::Succeeded => RunStatus::Succeeded,
                    qcg_engine::TerminalState::Failed => RunStatus::Failed,
                    qcg_engine::TerminalState::Canceled => RunStatus::Canceled,
                    qcg_engine::TerminalState::Interrupted => RunStatus::Interrupted,
                };
            }
        }
        Ok(())
    }

    /// Restarts durable runs that were queued or executing when the previous
    /// service process stopped. Completed steps are replayed from the journal;
    /// their pinned files and resources are verified by the engine before any
    /// remaining work is admitted.
    pub async fn resume_recovered_runs(&self) {
        let recovered = {
            let runs = self.inner.runs.read().await;
            runs.iter()
                .filter(|(_, record)| {
                    record.state == RunStatus::Queued
                        && record
                            .task
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .is_none()
                })
                .map(|(run_id, record)| SpawnRun {
                    run_id: run_id.clone(),
                    contract: record.contract.clone(),
                    inputs: record.inputs.clone(),
                    run_dir: record.run_dir.clone(),
                    events: record.events.clone(),
                    answers: record.answers.clone(),
                    confirmations: record.confirmations.clone(),
                    cancellation: record.cancellation.clone(),
                    task: Arc::clone(&record.task),
                })
                .collect::<Vec<_>>()
        };
        for run in recovered {
            self.spawn_engine_run(run);
        }
    }

    pub async fn list_mcp_servers(&self) -> Result<McpServerList, ServiceError> {
        let mut items = Vec::new();
        for id in self.inner.llm_runtime.mcp.server_ids() {
            let profile = self
                .inner
                .llm_runtime
                .mcp
                .resolve(id)
                .map_err(|error| ServiceError::Invalid(error.to_string()))?;
            items.push(McpServerSummary {
                id: id.to_string(),
                transport: profile.transport_name().to_string(),
                auth: profile.auth_name().to_string(),
                authorized: self
                    .inner
                    .llm_runtime
                    .mcp
                    .is_authorized(id)
                    .await
                    .map_err(|error| ServiceError::Invalid(error.to_string()))?,
            });
        }
        Ok(McpServerList { items })
    }

    pub async fn start_mcp_authorization(
        &self,
        id: &str,
        redirect_uri: &str,
    ) -> Result<McpAuthorizationStart, ServiceError> {
        let authorization_url = self
            .inner
            .llm_runtime
            .mcp
            .start_authorization(id, redirect_uri)
            .await
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        Ok(McpAuthorizationStart { authorization_url })
    }

    pub async fn complete_mcp_authorization(
        &self,
        callback_url: &str,
    ) -> Result<String, ServiceError> {
        self.inner
            .llm_runtime
            .mcp
            .complete_authorization(callback_url)
            .await
            .map_err(|error| ServiceError::Invalid(error.to_string()))
    }

    pub async fn clear_mcp_authorization(&self, id: &str) -> Result<(), ServiceError> {
        self.inner
            .llm_runtime
            .mcp
            .clear_authorization(id)
            .await
            .map_err(|error| ServiceError::Invalid(error.to_string()))
    }

    pub async fn cancel_pending_mcp_authorization(&self, id: &str) -> Result<(), ServiceError> {
        self.inner
            .llm_runtime
            .mcp
            .cancel_pending_authorization(id)
            .await
            .map_err(|error| ServiceError::Invalid(error.to_string()))
    }

    async fn collect_retained_runs(&self) {
        let maintenance_lock = match try_lock_store_maintenance(&self.inner.runs_dir) {
            Ok(Some(lock)) => lock,
            Ok(None) => return,
            Err(error) => {
                tracing::error!(%error, "failed to acquire run retention lease");
                return;
            }
        };
        if let Err(error) = gc_run_directories(&self.inner.runs_dir, 50, true) {
            tracing::error!(%error, "run retention failed");
            return;
        }
        self.inner
            .runs
            .write()
            .await
            .retain(|_, record| !record.state.is_terminal() || record.run_dir.is_dir());
        drop(maintenance_lock);
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
            let mut entry_count = 0_usize;
            for entry in std::fs::read_dir(&self.inner.runs_dir).map_err(api_internal)? {
                entry_count = entry_count.saturating_add(1);
                if entry_count > MAX_DIRECTORY_SCAN_ENTRIES {
                    return Err(api_internal(format!(
                        "runs directory contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
                    )));
                }
                let entry = entry.map_err(api_internal)?;
                let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                    api_internal(format!("run path is not valid UTF-8: {}", path.display()))
                })?;
                if !path.is_dir() {
                    continue;
                }
                let journal_path = run_meta_dir(&path).join("journal.jsonl");
                if !journal_path.is_file()
                    || journal_is_empty(&journal_path).map_err(api_internal)?
                {
                    continue;
                }
                let summary = run_summary(&path).map_err(api_internal)?;
                let run_id = summary.run_id;
                let artifacts = read_optional_output_manifest(&path).map_err(api_internal)?;
                let contract_sha256 = Some(read_run_contract_sha256(&path).map_err(api_internal)?);
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
        let mut entry_count = 0_usize;
        for entry in std::fs::read_dir(&self.inner.runs_dir).map_err(api_internal)? {
            entry_count = entry_count.saturating_add(1);
            if entry_count > MAX_DIRECTORY_SCAN_ENTRIES {
                return Err(api_internal(format!(
                    "runs directory contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
                )));
            }
            let entry = entry.map_err(api_internal)?;
            let run_dir = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                api_internal(format!("run path is not valid UTF-8: {}", path.display()))
            })?;
            let journal_path = run_meta_dir(&run_dir).join("journal.jsonl");
            if !run_dir.is_dir()
                || !journal_path.is_file()
                || journal_is_empty(&journal_path).map_err(api_internal)?
            {
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
        let execution_lock = match try_lock_run_execution(&request.run_dir) {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                *request.task.lock().unwrap_or_else(PoisonError::into_inner) = None;
                return;
            }
            Err(error) => {
                tracing::error!(%error, run_id = %request.run_id, "failed to acquire run execution lease");
                *request.task.lock().unwrap_or_else(PoisonError::into_inner) = None;
                return;
            }
        };
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
        let permits = Arc::clone(&self.inner.execution_permits);
        let handle = tokio::spawn(async move {
            let _execution_lock = execution_lock;
            let execution_permit = tokio::select! {
                _ = cancellation.cancelled() => return,
                permit = permits.acquire_owned() => {
                    permit.expect("execution semaphore must remain open while the service is alive")
                }
            };
            {
                let mut runs = service.inner.runs.write().await;
                let Some(record) = runs.get_mut(&run_id) else {
                    return;
                };
                if record.state == RunStatus::Canceled {
                    return;
                }
                record.state = RunStatus::Running;
            }
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
            drop(execution_permit);
            service.finish_run(run_id, progress).await;
        });
        *task.lock().unwrap_or_else(PoisonError::into_inner) = Some(handle);
    }

    pub async fn run_generator_path(&self, run: DirectRun) -> Result<OutputManifest, ApiError> {
        let contract = Contract::load(&run.generator_path).map_err(api_internal)?;
        let runtime = Arc::clone(&self.inner.llm_runtime);
        let run_id = direct_run_id(&run.output_dir);
        let metadata_dir = direct_run_meta_dir(&run.output_dir);
        let _run_lock = lock_direct_run(&metadata_dir).map_err(api_internal)?;
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
        let _run_lock = lock_direct_run(&metadata_dir).map_err(api_internal)?;
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
            .map(|event| RunEvent::from_flat(&event))
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
                let terminal = match fold_run_state(&record.run_dir) {
                    Ok(state) => state.terminal,
                    Err(error) => {
                        tracing::error!(
                            %error,
                            %run_id,
                            "failed to read run state after cancellation"
                        );
                        None
                    }
                };
                match terminal {
                    Some(qcg_engine::TerminalState::Succeeded) => {
                        record.state = RunStatus::Succeeded;
                        record.artifacts = match progress {
                            Progress::Done(outputs) => Some(outputs),
                            _ => match read_optional_output_manifest(&record.run_dir) {
                                Ok(artifacts) => artifacts,
                                Err(error) => {
                                    tracing::error!(
                                        %error,
                                        %run_id,
                                        "failed to read completed artifacts after cancellation race"
                                    );
                                    None
                                }
                            },
                        };
                    }
                    Some(qcg_engine::TerminalState::Failed) => {
                        record.state = RunStatus::Failed;
                    }
                    Some(qcg_engine::TerminalState::Interrupted) => {
                        record.state = RunStatus::Interrupted;
                    }
                    Some(qcg_engine::TerminalState::Canceled) => {}
                    None => {
                        if let Err(error) = write_run_event(
                            record,
                            "run_canceled",
                            json!({
                                "reason": FailureDetail::new(
                                    FailureCode::Canceled,
                                    "cancellation requested",
                                ),
                            }),
                        ) {
                            tracing::error!(
                                %error,
                                %run_id,
                                "failed to record terminal cancellation event"
                            );
                        }
                    }
                }
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
    let run_id = record
        .run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory must have a run id".into()))?;
    JournalWriter::create_with_limits(
        &run_meta_dir(&record.run_dir).join("journal.jsonl"),
        run_id,
        false,
        Some(record.events.clone()),
        JournalLimits::from(&record.contract.manifest.runtime),
    )
    .map_err(|error| ServiceError::Invalid(error.to_string()))?
    .event(kind, payload)
    .map_err(|error| ServiceError::Invalid(error.to_string()))
}

fn direct_run_id(workspace: &Utf8Path) -> String {
    let digest = hex::encode(Sha256::digest(workspace.as_str().as_bytes()));
    format!("direct-{digest}")
}

fn is_lock_contention(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }

    #[cfg(windows)]
    {
        // LockFileEx reports sharing and lock violations without mapping them to WouldBlock.
        matches!(error.raw_os_error(), Some(32 | 33))
    }

    #[cfg(not(windows))]
    {
        false
    }
}

fn lock_runs_directory(runs_dir: &Utf8Path) -> Result<File, ServiceError> {
    std::fs::create_dir_all(runs_dir)?;
    let lock_path = runs_dir.join(".service.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    file.try_lock_exclusive().map_err(|error| {
        if is_lock_contention(&error) {
            ServiceError::Invalid(format!(
                "runs directory `{runs_dir}` is already owned by another qcg service"
            ))
        } else {
            ServiceError::Io(error)
        }
    })?;
    Ok(file)
}

fn try_lock_run_execution(run_dir: &Utf8Path) -> Result<Option<File>, ServiceError> {
    let metadata = run_meta_dir(run_dir);
    std::fs::create_dir_all(&metadata)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(metadata.join("execution.lock"))?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(file)),
        Err(error) if is_lock_contention(&error) => Ok(None),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

fn try_lock_store_maintenance(runs_dir: &Utf8Path) -> Result<Option<File>, ServiceError> {
    std::fs::create_dir_all(runs_dir)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(runs_dir.join(".maintenance.lock"))?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(file)),
        Err(error) if is_lock_contention(&error) => Ok(None),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

fn prepare_api_run_directory(run_dir: &Utf8Path) -> Result<(), ServiceError> {
    let metadata = run_meta_dir(run_dir);
    std::fs::create_dir_all(&metadata)?;
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(metadata.join("journal.jsonl"))?;
    Ok(())
}

fn prepare_checkpoint_fork(
    source_dir: &Utf8Path,
    source_id: &str,
    target_dir: &Utf8Path,
    target_id: &str,
    at_seq: u64,
    patch: &ForkStatePatch,
) -> Result<(), ServiceError> {
    let source_meta = run_meta_dir(source_dir);
    let source_events = read_events_from_meta(&source_meta)?;
    if !source_events
        .iter()
        .any(|event| event.get("seq").and_then(Value::as_u64) == Some(at_seq))
    {
        return Err(ServiceError::Invalid(format!(
            "run `{source_id}` has no checkpoint sequence {at_seq}"
        )));
    }
    let selected = source_events
        .into_iter()
        .filter(|event| {
            event
                .get("seq")
                .and_then(Value::as_u64)
                .is_some_and(|seq| seq <= at_seq)
        })
        .filter(|event| {
            !matches!(
                event.get("t").and_then(Value::as_str),
                Some(
                    "run_finished"
                        | "run_error"
                        | "run_canceled"
                        | "run_interrupted"
                        | "run_waiting"
                        | "confirm_request"
                )
            )
        })
        .collect::<Vec<_>>();
    if !selected.iter().any(|event| {
        matches!(
            event.get("t").and_then(Value::as_str),
            Some("run_started" | "run_queued")
        )
    }) {
        return Err(ServiceError::Invalid(format!(
            "checkpoint {source_id}@{at_seq} predates run initialization"
        )));
    }

    prepare_api_run_directory(target_dir)?;
    let target_meta = run_meta_dir(target_dir);
    let target_workspace = run_workspace_dir(target_dir);
    std::fs::create_dir_all(&target_workspace)?;
    std::fs::create_dir_all(target_meta.join("checkpoint-blobs"))?;

    let mut latest_files = BTreeMap::<Utf8PathBuf, String>::new();
    for event in &selected {
        if event.get("t").and_then(Value::as_str) != Some("step_finished") {
            continue;
        }
        let Some(files) = event.get("files").and_then(Value::as_array) else {
            continue;
        };
        for file in files {
            let path = file
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| ServiceError::Invalid("checkpoint file pin has no path".into()))?;
            let path = Utf8PathBuf::from(path);
            if !is_safe_relative_path(path.as_str()) {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint file path `{path}` is unsafe"
                )));
            }
            let digest = file.get("sha256").and_then(Value::as_str).ok_or_else(|| {
                ServiceError::Invalid(format!("checkpoint file `{path}` has no sha256"))
            })?;
            latest_files.insert(path, digest.to_string());
        }
    }
    for (path, digest) in latest_files {
        let source_blob = source_meta.join("checkpoint-blobs").join(&digest);
        let source = if source_blob.is_file() {
            source_blob
        } else {
            let current = run_workspace_dir(source_dir).join(&path);
            if !current.is_file() {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint blob `{digest}` for `{path}` is unavailable; the source run predates checkpoint snapshots"
                )));
            }
            if hash_file_sha256(&current).map_err(|_| {
                ServiceError::Invalid(format!(
                    "checkpoint blob `{digest}` for `{path}` is unavailable; the source run predates checkpoint snapshots"
                ))
            })? != digest
            {
                return Err(ServiceError::Invalid(format!(
                    "checkpoint blob `{digest}` for `{path}` is unavailable and the current workspace contains a different revision"
                )));
            }
            current
        };
        if hash_file_sha256(&source)? != digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint blob `{digest}` for `{path}` failed integrity verification"
            )));
        }
        let destination = target_workspace.join(&path);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&source, &destination)?;
        if hash_file_sha256(&destination)? != digest {
            return Err(ServiceError::Invalid(format!(
                "checkpoint file `{path}` changed while it was copied"
            )));
        }
        std::fs::copy(
            &destination,
            target_meta.join("checkpoint-blobs").join(&digest),
        )?;
    }

    let journal_path = target_meta.join("journal.jsonl");
    let mut journal = OpenOptions::new().append(true).open(&journal_path)?;
    let mut next_seq = 0_u64;
    for mut event in selected {
        next_seq = next_seq.saturating_add(1);
        let object = event.as_object_mut().ok_or_else(|| {
            ServiceError::Invalid("checkpoint journal event is not an object".into())
        })?;
        object.insert("seq".into(), Value::Number(next_seq.into()));
        object.insert("run_id".into(), Value::String(target_id.to_string()));
        object.insert(
            "trace_id".into(),
            Value::String(qcg_types::trace_id_for_run(target_id)),
        );
        object.insert(
            "span_id".into(),
            Value::String(qcg_types::span_id_for_seq(next_seq)),
        );
        if object.contains_key("parent_span_id") {
            let scope = object
                .get("node")
                .and_then(Value::as_str)
                .map(|node| format!("step:{node}"))
                .unwrap_or_else(|| "run".to_string());
            object.insert(
                "parent_span_id".into(),
                Value::String(qcg_types::span_id_for_scope(target_id, &scope)),
            );
        }
        serde_json::to_writer(&mut journal, &event)?;
        journal.write_all(b"\n")?;
    }
    next_seq = next_seq.saturating_add(1);
    let fork_event = json!({
        "t": "run_forked",
        "ts": chrono::Utc::now().to_rfc3339(),
        "seq": next_seq,
        "run_id": target_id,
        "trace_id": qcg_types::trace_id_for_run(target_id),
        "span_id": qcg_types::span_id_for_seq(next_seq),
        "source_run_id": source_id,
        "source_seq": at_seq,
    });
    serde_json::to_writer(&mut journal, &fork_event)?;
    journal.write_all(b"\n")?;
    if !patch.inputs.is_empty() || !patch.step_outputs.is_empty() || !patch.step_statuses.is_empty()
    {
        next_seq = next_seq.saturating_add(1);
        let patch_event = json!({
            "t": "state_patched",
            "ts": chrono::Utc::now().to_rfc3339(),
            "seq": next_seq,
            "run_id": target_id,
            "trace_id": qcg_types::trace_id_for_run(target_id),
            "span_id": qcg_types::span_id_for_seq(next_seq),
            "inputs": patch.inputs,
            "step_outputs": patch.step_outputs,
            "step_statuses": patch.step_statuses,
        });
        serde_json::to_writer(&mut journal, &patch_event)?;
        journal.write_all(b"\n")?;
    }
    journal.sync_all()?;
    let state = RunState::fold_journal(&journal_path)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    state
        .persist_atomic(&target_meta.join("state.json"))
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    Ok(())
}

fn journal_is_empty(path: &Utf8Path) -> std::io::Result<bool> {
    Ok(std::fs::metadata(path)?.len() == 0)
}

fn lock_direct_run(metadata_dir: &Utf8Path) -> Result<File, ServiceError> {
    std::fs::create_dir_all(metadata_dir)?;
    let lock_path = metadata_dir.join(".run.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    file.try_lock_exclusive().map_err(|error| {
        if is_lock_contention(&error) {
            ServiceError::Invalid(format!(
                "output metadata `{metadata_dir}` is already active in another qcg run"
            ))
        } else {
            ServiceError::Io(error)
        }
    })?;
    Ok(file)
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
    let mut registry = qcg_steps::deterministic_registry_with_mcp(Arc::new(runtime.mcp.clone()));
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
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl LocalQcgService {
    pub async fn list_generators(&self) -> Result<Vec<GeneratorSummary>, ApiError> {
        let mut generators = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for root in &self.inner.generator_roots {
            if !root.exists() {
                continue;
            }
            let entries = std::fs::read_dir(root).map_err(api_internal)?;
            let mut entry_count = 0_usize;
            for entry in entries {
                entry_count = entry_count.saturating_add(1);
                if entry_count > MAX_DIRECTORY_SCAN_ENTRIES {
                    return Err(api_internal(format!(
                        "generator directory `{root}` contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
                    )));
                }
                let entry = entry.map_err(api_internal)?;
                let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                    api_internal(format!(
                        "generator path is not valid UTF-8: {}",
                        path.display()
                    ))
                })?;
                if !path.join("qcg.toml").exists() {
                    continue;
                }
                let contract = Contract::load(&path).map_err(api_internal)?;
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
        Ok(generators)
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
        let requested = match contract.resolve_package_path(&path) {
            Ok(path) => path,
            Err(PackagePathError::Path { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                return Err(api_not_found(format!(
                    "generator asset `{path}` was not found"
                )));
            }
            Err(PackagePathError::Escapes { .. }) => {
                return Err(api_not_found(format!(
                    "generator asset `{path}` was not found"
                )));
            }
            Err(error) => return Err(api_internal(error)),
        };
        if !requested.is_file() {
            return Err(api_not_found(format!(
                "generator asset `{path}` was not found"
            )));
        }
        let file = tokio::fs::File::open(requested)
            .await
            .map_err(api_internal)?;
        let mut bytes = Vec::new();
        file.take(
            u64::try_from(DEFAULT_GENERATOR_ASSET_RESPONSE_LIMIT_BYTES)
                .expect("generator asset response limit must fit in u64")
                .saturating_add(1),
        )
        .read_to_end(&mut bytes)
        .await
        .map_err(api_internal)?;
        if bytes.len() > DEFAULT_GENERATOR_ASSET_RESPONSE_LIMIT_BYTES {
            return Err(ApiError::TooLarge {
                actual_bytes: bytes.len(),
                limit_bytes: DEFAULT_GENERATOR_ASSET_RESPONSE_LIMIT_BYTES,
            });
        }
        Ok(bytes)
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
        let task = Arc::new(Mutex::new(None));
        let inputs = req.inputs;
        {
            let mut runs = self.inner.runs.write().await;
            if runs.len() >= self.inner.max_tracked_runs {
                runs.retain(|_, record| !record.state.is_terminal());
            }
            if runs.len() >= self.inner.max_tracked_runs {
                return Err(ApiError::Unavailable {
                    detail: format!(
                        "run capacity is exhausted: {} non-terminal runs are already tracked",
                        self.inner.max_tracked_runs
                    ),
                });
            }
            prepare_api_run_directory(&run_dir).map_err(api_internal)?;
            runs.insert(
                run_id.clone(),
                RunRecord {
                    contract: contract.clone(),
                    contract_sha256: contract.sha256.clone(),
                    inputs: inputs.clone(),
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    state: RunStatus::Queued,
                    run_dir: run_dir.clone(),
                    artifacts: None,
                    question: None,
                    confirm: None,
                    events: events.clone(),
                    cancellation: cancellation.clone(),
                    task: task.clone(),
                },
            );
            let record = runs.get(&run_id).expect("queued run was inserted");
            if let Err(error) = write_run_event(
                record,
                "run_queued",
                json!({
                    "run_id": &run_id,
                    "generator": format!("{}@{}", contract.manifest.generator.id, contract.manifest.generator.version),
                    "generator_path": &contract.root,
                    "contract_sha256": &contract.sha256,
                    "inputs": &inputs,
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": 1,
                    "retain_days": contract.manifest.journal.retain_days,
                }),
            ) {
                runs.remove(&run_id);
                drop(runs);
                if let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir) {
                    return Err(api_internal(format!(
                        "{error}; failed to clean incomplete run `{run_id}`: {cleanup_error}"
                    )));
                }
                return Err(api_internal(error));
            }
        }
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

    pub async fn fork_run(&self, source_id: &str, request: ForkRun) -> Result<String, ApiError> {
        if request.at_seq == 0 {
            return Err(ApiError::invalid_field(
                "at_seq",
                "checkpoint sequence must be greater than zero",
            ));
        }
        if let Some(source) = self.inner.runs.read().await.get(source_id)
            && matches!(source.state, RunStatus::Queued | RunStatus::Running)
        {
            return Err(ApiError::Conflict {
                detail: format!(
                    "run `{source_id}` is still executing; fork a stable waiting or terminal checkpoint"
                ),
            });
        }
        let source_dir = self.run_dir_for(source_id).await?;
        let generator_path = read_run_generator_path(&source_dir).map_err(api_internal)?;
        let contract = Contract::load(&generator_path).map_err(api_internal)?;
        let run_id = format!(
            "{}-fork-{}",
            contract.manifest.generator.id,
            uuid::Uuid::now_v7()
        );
        let run_dir = self.inner.runs_dir.join(&run_id);
        let (events, _) = broadcast::channel(512);
        let cancellation = CancellationToken::new();
        let task = Arc::new(Mutex::new(None));
        let inputs = {
            let mut runs = self.inner.runs.write().await;
            if runs.len() >= self.inner.max_tracked_runs {
                runs.retain(|_, record| !record.state.is_terminal());
            }
            if runs.len() >= self.inner.max_tracked_runs {
                return Err(ApiError::Unavailable {
                    detail: format!(
                        "run capacity is exhausted: {} non-terminal runs are already tracked",
                        self.inner.max_tracked_runs
                    ),
                });
            }
            let prepare_result = (|| -> Result<_, ApiError> {
                prepare_checkpoint_fork(
                    &source_dir,
                    source_id,
                    &run_dir,
                    &run_id,
                    request.at_seq,
                    &request.state_patch,
                )
                .map_err(api_internal)?;
                let state = fold_run_state(&run_dir).map_err(api_internal)?;
                let inputs = state.inputs.ok_or_else(|| {
                    api_internal(format!(
                        "checkpoint {source_id}@{} has no canonical inputs",
                        request.at_seq
                    ))
                })?;
                contract
                    .manifest
                    .resolve_inputs(inputs.clone())
                    .map_err(|error| {
                        ApiError::invalid_field("state_patch.inputs", error.to_string())
                    })?;
                Ok(inputs)
            })();
            let inputs = match prepare_result {
                Ok(inputs) => inputs,
                Err(error) => {
                    if run_dir.exists()
                        && let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir)
                    {
                        return Err(api_internal(format!(
                            "{error}; failed to clean incomplete fork `{run_id}`: {cleanup_error}"
                        )));
                    }
                    return Err(api_internal(error));
                }
            };
            runs.insert(
                run_id.clone(),
                RunRecord {
                    contract: contract.clone(),
                    contract_sha256: contract.sha256.clone(),
                    inputs: inputs.clone(),
                    answers: BTreeMap::new(),
                    confirmations: BTreeMap::new(),
                    state: RunStatus::Queued,
                    run_dir: run_dir.clone(),
                    artifacts: None,
                    question: None,
                    confirm: None,
                    events: events.clone(),
                    cancellation: cancellation.clone(),
                    task: task.clone(),
                },
            );
            inputs
        };
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
            None => Some(read_run_contract_sha256(&run_dir).map_err(api_internal)?),
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
            None => poll_journal_events(run_dir, id, history_last_seq),
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
            if let Some(existing) = record.answers.get(&question_id) {
                return if existing == &answer {
                    Ok(())
                } else {
                    Err(ApiError::Conflict {
                        detail: format!(
                            "question `{question_id}` was already answered with different values"
                        ),
                    })
                };
            }
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
            validate_form_values(&question.fields, &answer, &record.contract.manifest.runtime)
                .map_err(|error| {
                    ApiError::invalid_field("values", format!("invalid form answer: {error}"))
                })?;
            record.answers.insert(question_id, answer);
            record.state = RunStatus::Queued;
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
            let approved = decision.decision == ConfirmationDecision::Approve;
            if let Some(existing) = record.confirmations.get(&confirmation_id) {
                return if *existing == approved {
                    Ok(())
                } else {
                    Err(ApiError::Conflict {
                        detail: format!(
                            "confirmation `{confirmation_id}` already has a different decision"
                        ),
                    })
                };
            }
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
                record.confirmations.insert(confirm.id.clone(), false);
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
            record.state = RunStatus::Queued;
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
        let task = {
            let mut runs = self.inner.runs.write().await;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
            if record.state.is_terminal() {
                return Ok(());
            }
            let engine_is_active = record.state == RunStatus::Running;
            record.cancellation.cancel();
            record.state = RunStatus::Canceled;
            record.question = None;
            record.confirm = None;
            record.artifacts = None;
            if !engine_is_active {
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
                None
            } else {
                record
                    .task
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
            }
        };
        if let Some(task) = task {
            task.await.map_err(|error| {
                api_internal(format!(
                    "run `{id}` task failed during cancellation: {error}"
                ))
            })?;
        }
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
            let Some(handle) = task.lock().unwrap_or_else(PoisonError::into_inner).take() else {
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
    ) -> Result<(OutputArtifact, Utf8PathBuf), ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let manifest = read_output_manifest(&run_meta_dir(&run_dir)).map_err(api_internal)?;
        let artifact = manifest
            .artifacts
            .into_iter()
            .find(|artifact| artifact.path == path)
            .ok_or_else(|| api_not_found(format!("artifact `{path}` was not found")))?;
        let resolved = resolve_artifact_path(&run_workspace_dir(&run_dir), &artifact.path)
            .map_err(api_internal)?;
        Ok((artifact, resolved))
    }

    pub async fn read_journal(&self, id: String) -> Result<String, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let limits = JournalLimits::default();
        let file = tokio::fs::File::open(run_meta_dir(&run_dir).join("journal.jsonl"))
            .await
            .map_err(api_internal)?;
        let mut bytes = Vec::new();
        file.take(limits.max_total_bytes.saturating_add(1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(api_internal)?;
        if bytes.len() > limits.max_total_bytes {
            return Err(ApiError::TooLarge {
                actual_bytes: bytes.len(),
                limit_bytes: limits.max_total_bytes,
            });
        }
        String::from_utf8(bytes)
            .map_err(|error| api_internal(format!("journal is not valid UTF-8: {error}")))
    }
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub run_id: String,
    pub status: String,
    pub generator: String,
    pub generator_path: String,
    pub contract_sha256: String,
    pub inputs: BTreeMap<String, Value>,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub artifacts: Vec<OutputArtifact>,
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

fn summarize_file_values(inputs: &BTreeMap<String, Value>) -> Value {
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
        "queued" => Ok(RunStatus::Queued),
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

fn rehydrate_runs(
    runs_dir: &Utf8Path,
    max_tracked_runs: usize,
) -> Result<BTreeMap<String, RunRecord>, ServiceError> {
    let mut records = BTreeMap::new();
    if !runs_dir.exists() {
        return Ok(records);
    }
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
            return Err(ServiceError::Invalid(format!(
                "run store contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
            )));
        }
        let entry = entry?;
        let run_dir = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
        })?;
        let journal_path = run_meta_dir(&run_dir).join("journal.jsonl");
        if !run_dir.is_dir() || !journal_path.is_file() {
            continue;
        }
        if journal_is_empty(&journal_path)? {
            continue;
        }
        let mut state = RunState::fold_journal(&journal_path)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        if state.terminal.is_some() {
            continue;
        }
        if records.len() >= max_tracked_runs {
            return Err(ServiceError::Invalid(format!(
                "run store contains more than {max_tracked_runs} non-terminal runs"
            )));
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
            None => (RunStatus::Queued, None, None),
        };
        let generator_path = read_run_generator_path(&run_dir)?;
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
                task: Arc::new(Mutex::new(None)),
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
    read_journal_values(&meta_dir.join("journal.jsonl"), JournalLimits::default())
        .map(|scan| scan.events)
        .map_err(|error| ServiceError::Invalid(error.to_string()))
}

pub fn read_run_events(run_dir: &Utf8Path) -> Result<Vec<RunEvent>, ServiceError> {
    read_journal_events(run_dir)?
        .into_iter()
        .map(|event| RunEvent::from_flat(&event).map_err(ServiceError::Invalid))
        .collect()
}

fn poll_journal_events(
    run_dir: Utf8PathBuf,
    run_id: String,
    mut delivered_seq: u64,
) -> BoxStream<'static, RunEvent> {
    let (sender, receiver) = tokio::sync::mpsc::channel(JOURNAL_POLL_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let limits = JournalLimits::default();
        let journal_path = run_meta_dir(&run_dir).join("journal.jsonl");
        let mut offset = 0_u64;
        let mut event_count = 0_usize;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            interval.tick().await;
            let metadata = match tokio::fs::metadata(&journal_path).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    tracing::error!(%error, %run_id, "failed to inspect shared run journal");
                    return;
                }
            };
            if metadata.len() > limits.max_total_bytes as u64 {
                tracing::error!(%run_id, limit = limits.max_total_bytes, actual = metadata.len(), "shared run journal exceeds byte limit");
                return;
            }
            let mut file = match tokio::fs::File::open(&journal_path).await {
                Ok(file) => file,
                Err(error) => {
                    tracing::error!(%error, %run_id, "failed to open shared run journal");
                    return;
                }
            };
            if let Err(error) = file.seek(SeekFrom::Start(offset)).await {
                tracing::error!(%error, %run_id, "failed to seek shared run journal");
                return;
            }
            let mut reader = tokio::io::BufReader::new(file);
            loop {
                let mut line = Vec::new();
                let line_limit = limits.max_event_bytes.saturating_add(2);
                let read = match (&mut reader)
                    .take(line_limit as u64)
                    .read_until(b'\n', &mut line)
                    .await
                {
                    Ok(read) => read,
                    Err(error) => {
                        tracing::error!(%error, %run_id, "failed to read shared run journal");
                        return;
                    }
                };
                if read == 0 {
                    break;
                }
                let has_newline = line.last() == Some(&b'\n');
                if !has_newline {
                    if read == line_limit {
                        tracing::error!(%run_id, limit = limits.max_event_bytes, "shared run journal event exceeds byte limit");
                        return;
                    }
                    break;
                }
                offset = match offset.checked_add(read as u64) {
                    Some(offset) => offset,
                    None => {
                        tracing::error!(%run_id, "shared run journal offset overflowed");
                        return;
                    }
                };
                line.pop();
                if line.len() > limits.max_event_bytes {
                    tracing::error!(%run_id, limit = limits.max_event_bytes, actual = line.len(), "shared run journal event exceeds byte limit");
                    return;
                }
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                event_count = event_count.saturating_add(1);
                if event_count > limits.max_event_count {
                    tracing::error!(%run_id, limit = limits.max_event_count, "shared run journal event count exceeds limit");
                    return;
                }
                let value = match serde_json::from_slice::<Value>(&line) {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::error!(%error, %run_id, "shared run journal contains invalid JSON");
                        return;
                    }
                };
                let event = match RunEvent::from_flat(&value) {
                    Ok(event) => event,
                    Err(error) => {
                        tracing::error!(%error, %run_id, "shared run journal contains an invalid event");
                        return;
                    }
                };
                let terminal = matches!(
                    event.kind.as_str(),
                    "run_finished" | "run_error" | "run_canceled"
                );
                if event.seq > delivered_seq {
                    delivered_seq = event.seq;
                    if sender.send(event).await.is_err() {
                        return;
                    }
                }
                if terminal {
                    return;
                }
            }
        }
    });
    ReceiverStream::new(receiver).boxed()
}

pub fn read_run_generator_path(run_dir: &Utf8Path) -> Result<Utf8PathBuf, ServiceError> {
    let events = read_run_events(run_dir)?;
    let (_, started) = run_identity_event(run_dir, &events)?;
    Ok(Utf8PathBuf::from(&started.generator_path))
}

fn read_run_contract_sha256(run_dir: &Utf8Path) -> Result<String, ServiceError> {
    let events = read_run_events(run_dir)?;
    let (_, started) = run_identity_event(run_dir, &events)?;
    Ok(started.contract_sha256.clone())
}

pub fn read_run_inputs(run_dir: &Utf8Path) -> Result<BTreeMap<String, Value>, ServiceError> {
    let events = read_run_events(run_dir)?;
    let (_, started) = run_identity_event(run_dir, &events)?;
    Ok(started.inputs.clone())
}

pub fn run_summary(run_dir: &Utf8Path) -> Result<RunSummary, ServiceError> {
    let events = read_run_events(run_dir)?;
    let (started_event, started) = run_identity_event(run_dir, &events)?;
    let lifecycle = events
        .iter()
        .rev()
        .find(|event| {
            matches!(
                event.kind.as_str(),
                "run_queued"
                    | "run_started"
                    | "run_waiting"
                    | "confirm_request"
                    | "run_finished"
                    | "run_error"
                    | "run_canceled"
                    | "run_interrupted"
            )
        })
        .ok_or_else(|| ServiceError::Invalid("run has no lifecycle event".into()))?;
    let status = match &lifecycle.data {
        RunEventData::RunQueued(_) => "queued",
        RunEventData::RunStarted(_) => "running",
        RunEventData::RunWaiting(_) => "waiting",
        RunEventData::ConfirmRequest(_) => "confirming",
        RunEventData::RunError(_) => "failed",
        RunEventData::RunCanceled(_) => "canceled",
        RunEventData::RunInterrupted(_) => "interrupted",
        RunEventData::RunFinished(data) => match data.status {
            RunCompletionStatus::Success => "success",
            RunCompletionStatus::Failed => "failed",
        },
        _ => return Err(ServiceError::Invalid("unsupported lifecycle event".into())),
    };
    let artifacts = read_optional_output_manifest(run_dir)?
        .map(|manifest| manifest.artifacts)
        .unwrap_or_default();
    let retain_days = started
        .retain_days
        .map(u32::try_from)
        .transpose()
        .map_err(|_| ServiceError::Invalid("retain_days exceeds u32".into()))?;
    Ok(RunSummary {
        run_id: run_dir
            .file_name()
            .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?
            .to_string(),
        status: status.to_string(),
        generator: started.generator.clone(),
        generator_path: started.generator_path.clone(),
        contract_sha256: started.contract_sha256.clone(),
        inputs: started.inputs.clone(),
        started_at: started_event.ts.clone(),
        finished_at: matches!(status, "success" | "failed" | "canceled" | "interrupted")
            .then(|| lifecycle.ts.clone()),
        artifacts,
        retain_days,
    })
}

fn run_identity_event<'a>(
    run_dir: &Utf8Path,
    events: &'a [RunEvent],
) -> Result<(&'a RunEvent, &'a qcg_types::RunStartedEventData), ServiceError> {
    events
        .iter()
        .find_map(|event| event.data.run_started().map(|data| (event, data)))
        .ok_or_else(|| ServiceError::Invalid(format!("run `{run_dir}` has no run identity event")))
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
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
            return Err(ServiceError::Invalid(format!(
                "runs directory contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
            )));
        }
        let entry = entry?;
        let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
        })?;
        let journal_path = run_meta_dir(&path).join("journal.jsonl");
        if !path.is_dir() || !journal_path.exists() || journal_is_empty(&journal_path)? {
            continue;
        }
        let summary = run_summary(&path)?;
        if !matches!(
            summary.status.as_str(),
            "success" | "failed" | "canceled" | "interrupted"
        ) {
            continue;
        }
        let expired = match summary.retain_days {
            Some(days) => {
                let started =
                    chrono::DateTime::parse_from_rfc3339(&summary.started_at).map_err(|error| {
                        ServiceError::Invalid(format!(
                            "run `{}` has invalid started_at: {error}",
                            summary.run_id
                        ))
                    })?;
                started.with_timezone(&chrono::Utc) < now - chrono::Duration::days(i64::from(days))
            }
            None => false,
        };
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
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
            return Err(ServiceError::Invalid(format!(
                "runs directory contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
            )));
        }
        let entry = entry?;
        let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
        })?;
        let journal_path = run_meta_dir(&path).join("journal.jsonl");
        if !path.is_dir() || !journal_path.exists() || journal_is_empty(&journal_path)? {
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
    let limits = JournalLimits::default();
    let journal_path = meta_dir.join("journal.jsonl");
    let scan = read_journal_values(&journal_path, limits)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal_path)?;
    let bytes = serialize_bounded(event, limits.max_event_bytes, "event")
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    let mut stats = scan.stats;
    append_serialized_json_line(&mut file, bytes, &mut stats, limits)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
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
    write_artifacts_zip_stream_bounded(
        run_dir,
        CountingWriter::new(writer, limit_bytes),
        limit_bytes,
    )
}

fn write_artifacts_zip_stream_bounded<W: Write>(
    run_dir: &Utf8Path,
    writer: CountingWriter<W>,
    limit_bytes: u64,
) -> Result<(), ServiceError> {
    let manifest: OutputManifest = read_output_manifest(&run_meta_dir(run_dir))?;
    let artifact_paths = manifest
        .artifacts
        .iter()
        .map(|artifact| {
            let path = resolve_artifact_path(&run_workspace_dir(run_dir), &artifact.path)?;
            let metadata = std::fs::metadata(&path)?;
            let bytes = metadata.len();
            if bytes != artifact.bytes {
                return Err(ServiceError::Invalid(format!(
                    "artifact `{}` bytes mismatch: manifest={}, actual={bytes}",
                    artifact.path, artifact.bytes
                )));
            }
            Ok((
                artifact.path.clone(),
                path,
                metadata,
                bytes,
                artifact.sha256.clone(),
            ))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    let total_bytes = artifact_paths
        .iter()
        .try_fold(0_u64, |total, (_, _, _, bytes, _)| {
            total.checked_add(*bytes)
        })
        .ok_or_else(|| ServiceError::Invalid("artifact zip size overflowed".into()))?;
    if total_bytes > limit_bytes {
        return Err(ServiceError::Invalid(format!(
            "artifact zip source bytes exceed limit: {total_bytes} > {limit_bytes}"
        )));
    }
    for (artifact_path, path, _, _, expected_sha256) in &artifact_paths {
        let actual_sha256 = hash_file_sha256(path)?;
        if actual_sha256 != *expected_sha256 {
            return Err(ServiceError::Invalid(format!(
                "artifact `{artifact_path}` sha256 mismatch: manifest={expected_sha256}, actual={actual_sha256}"
            )));
        }
    }
    let workspace = run_workspace_dir(run_dir);
    let mut directories = BTreeSet::new();
    for (artifact_path, _, _, _, _) in &artifact_paths {
        let mut parent = Utf8Path::new(artifact_path).parent();
        while let Some(directory) = parent.filter(|directory| !directory.as_str().is_empty()) {
            directories.insert(directory.to_path_buf());
            parent = directory.parent();
        }
    }
    let entry_count = directories
        .len()
        .checked_add(artifact_paths.len())
        .ok_or_else(|| ServiceError::Invalid("artifact zip entry count overflowed".into()))?;
    if entry_count > DEFAULT_ARTIFACT_ZIP_ENTRY_LIMIT {
        return Err(ServiceError::Invalid(format!(
            "artifact zip entries exceed limit: {entry_count} > {DEFAULT_ARTIFACT_ZIP_ENTRY_LIMIT}"
        )));
    }
    let mut zip = zip::ZipWriter::new_stream(writer);
    for directory in directories {
        let metadata = std::fs::metadata(workspace.join(&directory))?;
        zip.add_directory(
            format!("{directory}/"),
            artifact_zip_options(&metadata, true)?,
        )
        .map_err(artifact_zip_error)?;
    }
    for (artifact_path, path, metadata, _, _) in artifact_paths {
        let mut file = File::open(path)?;
        zip.start_file(artifact_path, artifact_zip_options(&metadata, false)?)
            .map_err(artifact_zip_error)?;
        std::io::copy(&mut file, &mut zip)?;
    }
    let writer = zip.finish().map_err(artifact_zip_error)?.into_inner();
    if writer.exceeded {
        return Err(ServiceError::Invalid(format!(
            "artifact zip output exceeds {} bytes",
            writer.limit
        )));
    }
    Ok(())
}

fn artifact_zip_error(error: zip::result::ZipError) -> ServiceError {
    match error {
        zip::result::ZipError::Io(error) => ServiceError::Invalid(error.to_string()),
        error => ServiceError::Zip(error),
    }
}

struct CountingWriter<W> {
    inner: W,
    written: u64,
    limit: u64,
    exceeded: bool,
}

impl<W> CountingWriter<W> {
    fn new(inner: W, limit: u64) -> Self {
        Self {
            inner,
            written: 0,
            limit,
            exceeded: false,
        }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let remaining = usize::try_from(self.limit.saturating_sub(self.written))
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        if remaining > 0 {
            self.inner.write_all(&bytes[..remaining])?;
            self.written =
                self.written
                    .checked_add(u64::try_from(remaining).map_err(|_| {
                        std::io::Error::other("artifact zip output size overflowed")
                    })?)
                    .ok_or_else(|| std::io::Error::other("artifact zip output size overflowed"))?;
        }
        if remaining < bytes.len() {
            self.exceeded = true;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn artifact_zip_options(
    metadata: &std::fs::Metadata,
    directory: bool,
) -> Result<SimpleFileOptions, ServiceError> {
    let compression = if directory {
        zip::CompressionMethod::Stored
    } else {
        zip::CompressionMethod::Deflated
    };
    let mut options = SimpleFileOptions::default().compression_method(compression);
    let modified = metadata.modified()?;
    let modified = chrono::DateTime::<chrono::Utc>::from(modified).naive_utc();
    let modified = zip::DateTime::try_from(modified).map_err(|error| {
        ServiceError::Invalid(format!(
            "artifact modification time is not representable in ZIP: {error}"
        ))
    })?;
    options = options.last_modified_time(modified);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        options = options.unix_permissions(metadata.permissions().mode());
    }
    Ok(options)
}

fn hash_file_sha256(path: &Utf8Path) -> Result<String, std::io::Error> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut Sha256Writer(&mut digest))?;
    Ok(hex::encode(digest.finalize()))
}

struct Sha256Writer<'a>(&'a mut Sha256);

impl std::io::Write for Sha256Writer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
    is_safe_relative_path(id)
}

fn event_kinds(events: &[RunEvent]) -> Vec<String> {
    events.iter().map(|event| event.kind.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use qcg_api::ConfirmDecision;
    use qcg_api::StartRun;
    use std::collections::BTreeSet;

    fn temp_run_dir(name: &str) -> Utf8PathBuf {
        let dir =
            std::env::temp_dir().join(format!("qcg-service-test-{name}-{}", uuid::Uuid::now_v7()));
        Utf8PathBuf::from_path_buf(dir).expect("temporary directory path must be UTF-8")
    }

    #[test]
    fn ids_reject_platform_specific_paths_and_traversal() {
        for id in [
            "",
            "/",
            "/etc/passwd",
            "\\etc\\passwd",
            "\\\\server\\share",
            "C:/Windows/System32",
            "C:\\Windows\\System32",
            "C:Windows/System32",
            "../generator",
            "nested/../generator",
            "nested\\..\\generator",
            ".",
            "./generator",
            "nested//generator",
        ] {
            assert!(
                !is_safe_id(id),
                "{id:?} must not be accepted as an identifier"
            );
        }
        for id in ["generator", "generator.v1", "nested/generator"] {
            assert!(is_safe_id(id), "{id:?} should be accepted as an identifier");
        }
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
            .expect("generators should be listed")
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

    #[test]
    fn artifact_zip_rejects_compressed_output_above_limit() {
        let run_dir = temp_run_dir("zip-output-limit");
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(run_workspace_dir(&run_dir)).expect("workspace should be created");
        std::fs::create_dir_all(run_meta_dir(&run_dir)).expect("metadata should be created");
        std::fs::write(run_workspace_dir(&run_dir).join("a.txt"), "a")
            .expect("artifact should be written");
        std::fs::write(
            run_meta_dir(&run_dir).join("outputs.json"),
            serde_json::to_string(&json!({
                "artifacts": [{
                    "path": "a.txt",
                    "sha256": "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
                    "bytes": 1,
                    "label": "A",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let mut bytes = Cursor::new(Vec::new());
        let error = write_artifacts_zip_stream_with_limit(&run_dir, &mut bytes, 32)
            .expect_err("zip generation should reject oversized compressed output");
        assert!(
            error.to_string().contains("artifact zip output exceeds"),
            "{error}"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn artifact_zip_rejects_manifest_byte_mismatch_before_writing() {
        let run_dir = temp_run_dir("zip-bytes-mismatch");
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
                    "sha256": "2689367b205c16ce32ed4200942b8b8b1e262dfc70d9bc9fbc77c49699a4f1df",
                    "bytes": 3,
                    "label": "Result",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let mut bytes = Cursor::new(Vec::new());
        let error = write_artifacts_zip_stream_with_limit(&run_dir, &mut bytes, 128)
            .expect_err("zip generation should reject a byte-count mismatch");
        assert!(error.to_string().contains("bytes mismatch"));
        assert!(
            bytes.get_ref().is_empty(),
            "zip output must not start before validation"
        );
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    #[test]
    fn artifact_zip_rejects_manifest_sha256_mismatch_before_writing() {
        let run_dir = temp_run_dir("zip-sha256-mismatch");
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
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "bytes": 2,
                    "label": "Result",
                    "required": true
                }]
            }))
            .expect("manifest should serialize"),
        )
        .expect("manifest should be written");

        let mut bytes = Cursor::new(Vec::new());
        let error = write_artifacts_zip_stream_with_limit(&run_dir, &mut bytes, 128)
            .expect_err("zip generation should reject a sha256 mismatch");
        assert!(error.to_string().contains("sha256 mismatch"));
        assert!(
            bytes.get_ref().is_empty(),
            "zip output must not start before validation"
        );
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

[[inputs.stages]]
id = "basic"

[[inputs.stages.fields]]
id = "marker"
required = true
type = "string"

[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = true }
[flow.params]
output_file = "result.txt"
content = "{{ inputs.marker }}"

"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_and_max_active_runs(
            vec![root.clone()],
            runs.clone(),
            None,
            10,
        )
        .expect("service should initialize");
        let results = futures_util::future::join_all((0..10).map(|index| {
            let service = service.clone();
            let marker = format!("marker-{index}");
            async move {
                let id = service
                    .start_run(StartRun {
                        generator_id: "generator".into(),
                        inputs: BTreeMap::from([(String::from("marker"), json!(marker.clone()))]),
                    })
                    .await?;
                Ok::<_, ApiError>((id, marker))
            }
        }))
        .await;
        let runs_with_markers: Vec<(String, String)> = results
            .into_iter()
            .map(|result| result.expect("concurrent run should start"))
            .collect();
        let ids: Vec<String> = runs_with_markers.iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(ids.len(), BTreeSet::<String>::from_iter(ids.clone()).len());
        let snapshots = futures_util::future::join_all(
            runs_with_markers
                .iter()
                .map(|(id, _)| wait_for_terminal_snapshot(&service, id)),
        )
        .await;
        for ((id, marker), snapshot) in runs_with_markers.iter().zip(snapshots) {
            assert_eq!(snapshot.run_id, *id);
            assert_eq!(snapshot.state, RunStatus::Succeeded);
            let run_dir = service
                .run_dir_for(id)
                .await
                .expect("run directory should exist");
            assert_eq!(run_dir, runs.join(id));
            assert_eq!(
                std::fs::read_to_string(run_workspace_dir(&run_dir).join("result.txt"))
                    .expect("run artifact should be readable"),
                *marker
            );

            let events = read_journal_events(&run_dir).expect("journal should be valid JSONL");
            assert!(!events.is_empty(), "journal should contain run events");
            let seqs: Vec<u64> = events
                .iter()
                .map(|event| {
                    event
                        .get("seq")
                        .and_then(Value::as_u64)
                        .expect("journal event should have a sequence")
                })
                .collect();
            assert_eq!(
                seqs,
                (1..=events.len() as u64).collect::<Vec<_>>(),
                "run journal sequence should be contiguous for {id}"
            );
            assert_eq!(
                snapshot.seq,
                *seqs.last().expect("journal should have a last seq")
            );
            let started = events
                .iter()
                .find(|event| event.get("t").and_then(Value::as_str) == Some("run_started"))
                .expect("journal should start the run");
            assert_eq!(
                started.get("run_id").and_then(Value::as_str),
                Some(id.as_str())
            );
            assert_eq!(
                started
                    .get("inputs")
                    .and_then(|inputs| inputs.get("marker"))
                    .and_then(Value::as_str),
                Some(marker.as_str())
            );
            let terminal_events: Vec<&Value> = events
                .iter()
                .filter(|event| {
                    matches!(
                        event.get("t").and_then(Value::as_str),
                        Some("run_finished")
                            | Some("run_error")
                            | Some("run_canceled")
                            | Some("run_interrupted")
                    )
                })
                .collect();
            assert_eq!(
                terminal_events.len(),
                1,
                "run should have one terminal event"
            );
            assert_eq!(
                terminal_events[0].get("t").and_then(Value::as_str),
                Some("run_finished")
            );
            assert!(
                read_run_events(&run_dir)
                    .expect("typed journal events should parse")
                    .iter()
                    .all(|event| event.run_id == *id),
                "all events should remain scoped to {id}"
            );
        }

        let listed = service.list_runs().await.expect("runs should be listable");
        assert_eq!(
            listed
                .iter()
                .filter(|snapshot| ids.contains(&snapshot.run_id))
                .count(),
            10
        );
    }

    #[tokio::test]
    async fn concurrent_hitl_sessions_keep_answers_and_artifacts_isolated() {
        let root = temp_run_dir("concurrent-hitl");
        let _ = std::fs::remove_dir_all(&root);
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = LocalQcgService::with_generator_roots_and_max_active_runs(
            vec![generators],
            root.clone(),
            None,
            8,
        )
        .expect("service should initialize");
        let ids = futures_util::future::join_all((0..8).map(|_| {
            let service = service.clone();
            async move {
                service
                    .start_run(StartRun {
                        generator_id: "ask-user".into(),
                        inputs: BTreeMap::new(),
                    })
                    .await
            }
        }))
        .await
        .into_iter()
        .map(|result| result.expect("HITL run should start"))
        .collect::<Vec<_>>();
        let waiting = futures_util::future::join_all(
            ids.iter()
                .map(|id| wait_for_snapshot(&service, id, RunStatus::Waiting)),
        )
        .await;
        let expected = waiting
            .iter()
            .enumerate()
            .map(|(index, snapshot)| {
                (
                    snapshot.run_id.clone(),
                    snapshot
                        .question
                        .as_ref()
                        .expect("run should expose its own question")
                        .id
                        .clone(),
                    if index % 2 == 0 { "brief" } else { "detailed" },
                )
            })
            .collect::<Vec<_>>();
        let answers =
            futures_util::future::join_all(expected.iter().map(|(run_id, question_id, answer)| {
                let service = service.clone();
                let run_id = run_id.clone();
                let question_id = question_id.clone();
                let answer = (*answer).to_string();
                async move {
                    service
                        .answer(
                            run_id,
                            question_id,
                            AnswerPayload {
                                values: BTreeMap::from([("answer".into(), json!(answer))]),
                            },
                        )
                        .await
                }
            }))
            .await;
        assert!(answers.into_iter().all(|result| result.is_ok()));
        for (run_id, _, answer) in expected {
            let snapshot = wait_for_terminal_snapshot(&service, &run_id).await;
            assert_eq!(snapshot.state, RunStatus::Succeeded);
            let artifact = std::fs::read_to_string(
                run_workspace_dir(
                    &service
                        .run_dir_for(&run_id)
                        .await
                        .expect("run directory should exist"),
                )
                .join("answer.txt"),
            )
            .expect("HITL artifact should be readable");
            assert_eq!(artifact, format!("mode={answer}"));
        }
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_limits_bound_active_and_tracked_runs() {
        let root = temp_run_dir("max-active-runs");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "max-active"
name = "Max Active"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 1"], purpose = "active run limit test", isolation = "trusted_host" }]

[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 1"]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![root.clone()],
            runs,
            None,
            1,
            2,
            RunStoreMode::Exclusive,
        )
        .expect("service should initialize");
        let fork_source_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
            })
            .await
            .expect("fork source should start");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &fork_source_id)
                .await
                .state,
            RunStatus::Succeeded
        );
        let fork_source_dir = service
            .run_dir_for(&fork_source_id)
            .await
            .expect("fork source directory should exist");
        let fork_seq = read_journal_events(&fork_source_dir)
            .expect("fork source journal should be readable")
            .into_iter()
            .find(|event| event.get("t").and_then(Value::as_str) == Some("step_finished"))
            .and_then(|event| event.get("seq").and_then(Value::as_u64))
            .expect("fork source should contain a stable step checkpoint");
        let first_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
            })
            .await
            .expect("first run should start");
        let second_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
            })
            .await
            .expect("second run should enter the durable queue");
        let capacity_error = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
            })
            .await
            .expect_err("tracked run capacity should reject another queued run");
        assert!(
            capacity_error
                .to_string()
                .contains("run capacity is exhausted")
        );
        let fork_capacity_error = service
            .fork_run(
                &fork_source_id,
                ForkRun {
                    at_seq: fork_seq,
                    state_patch: ForkStatePatch::default(),
                },
            )
            .await
            .expect_err("fork must share the tracked run capacity");
        assert!(
            fork_capacity_error
                .to_string()
                .contains("run capacity is exhausted")
        );
        assert_eq!(
            service
                .snapshot(second_id.clone())
                .await
                .expect("queued run should be visible")
                .state,
            RunStatus::Queued
        );
        assert_eq!(
            wait_for_terminal_snapshot(&service, &first_id).await.state,
            RunStatus::Succeeded
        );
        assert_eq!(
            wait_for_terminal_snapshot(&service, &second_id).await.state,
            RunStatus::Succeeded
        );
        let replacement_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::new(),
            })
            .await
            .expect("terminal runs should be evicted when capacity is reused");
        assert_eq!(
            wait_for_terminal_snapshot(&service, &replacement_id)
                .await
                .state,
            RunStatus::Succeeded
        );
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn shared_journal_poll_rejects_event_body_above_limit_with_newline() {
        let run_dir = temp_run_dir("shared-journal-event-limit");
        let _ = std::fs::remove_dir_all(&run_dir);
        let meta_dir = run_meta_dir(&run_dir);
        std::fs::create_dir_all(&meta_dir).expect("run metadata should be created");
        let limits = JournalLimits::default();
        let mut oversized = vec![b' '; limits.max_event_bytes + 1];
        oversized.push(b'\n');
        std::fs::write(meta_dir.join("journal.jsonl"), oversized)
            .expect("oversized journal event should be written");

        let mut events = poll_journal_events(run_dir.clone(), "oversized-run".into(), 0);
        let next = tokio::time::timeout(std::time::Duration::from_secs(2), events.next())
            .await
            .expect("poller should terminate after rejecting the event");
        assert!(next.is_none());
        std::fs::remove_dir_all(run_dir).expect("temporary run should be removed");
    }

    #[test]
    fn runs_directory_is_exclusive_between_services() {
        let root = temp_run_dir("runs-directory-lock");
        let _ = std::fs::remove_dir_all(&root);
        let generators = root.join("generators");
        let runs = root.join("runs");
        let first = LocalQcgService::new(generators.clone(), runs.clone(), None)
            .expect("first service should acquire the runs directory");
        let error = LocalQcgService::new(generators.clone(), runs.clone(), None)
            .expect_err("a second service must not share the runs directory");
        assert!(
            error.to_string().contains("already owned"),
            "lock failure should explain ownership conflict, got {error}"
        );
        drop(first);
        let second = LocalQcgService::new(generators, runs, None)
            .expect("the runs directory should be reusable after the first service drops");
        drop(second);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_filesystem_store_allows_multiple_services_and_serializes_each_run() {
        let root = temp_run_dir("shared-runs-directory");
        let _ = std::fs::remove_dir_all(&root);
        let generators = root.join("generators");
        let runs = root.join("runs");
        let first = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![generators.clone()],
            runs.clone(),
            None,
            1,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
        )
        .expect("first shared service should initialize");
        let second = LocalQcgService::with_generator_roots_max_active_runs_and_store_mode(
            vec![generators],
            runs,
            None,
            1,
            DEFAULT_MAX_TRACKED_RUNS,
            RunStoreMode::SharedFilesystem,
        )
        .expect("second shared service should initialize");

        let run_dir = root.join("shared-run");
        let first_lease = try_lock_run_execution(&run_dir)
            .expect("first lease attempt should succeed")
            .expect("first process should own the run");
        assert!(
            try_lock_run_execution(&run_dir)
                .expect("second lease attempt should be valid")
                .is_none(),
            "a run must have only one execution owner"
        );
        drop(first_lease);
        assert!(
            try_lock_run_execution(&run_dir)
                .expect("released lease should be reusable")
                .is_some()
        );
        drop(first);
        drop(second);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn direct_runs_reject_the_same_active_output_directory() {
        let root = temp_run_dir("direct-output-lock");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let output = root.join("output");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "direct-output-lock"
name = "Direct Output Lock"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 1"], purpose = "direct output lock test", isolation = "trusted_host" }]

[[flow]]
id = "sleep"
type = "command"

[flow.params]
command = ["sh", "-c", "sleep 1"]
"#,
        )
        .expect("generator manifest should be written");
        let service = LocalQcgService::new(root.clone(), root.join("runs"), None)
            .expect("service should initialize");
        let direct_run = || DirectRun {
            generator_path: generator.clone(),
            inputs: BTreeMap::new(),
            output_dir: output.clone(),
            json_events: false,
            interactive: false,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            llm_seed_override: None,
        };
        let first = tokio::spawn({
            let service = service.clone();
            let run = direct_run();
            async move { service.run_generator_path(run).await }
        });
        let lock_path = direct_run_meta_dir(&output).join(".run.lock");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(probe) = OpenOptions::new().read(true).write(true).open(&lock_path) {
                    match probe.try_lock_exclusive() {
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(error) => panic!("output lock probe failed: {error}"),
                        Ok(()) => {}
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first direct run should acquire its output lock");
        let error = service
            .run_generator_path(direct_run())
            .await
            .expect_err("a second direct run must not share an active output directory");
        assert!(error.to_string().contains("already active"));
        first
            .await
            .expect("first direct run task should not panic")
            .expect("first direct run should succeed");
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
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
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "cancellation test", isolation = "trusted_host" }]

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
        let journal = std::fs::read_to_string(run_meta_dir(&run_dir).join("journal.jsonl"))
            .expect("journal should be complete when cancel returns");
        assert!(journal.contains("\"t\":\"run_canceled\""));
        assert_eq!(
            service
                .snapshot(id)
                .await
                .expect("canceled snapshot should be available")
                .state,
            RunStatus::Canceled
        );
        assert!(
            !run_workspace_dir(&run_dir)
                .join("must-not-exist.txt")
                .exists()
        );
    }

    #[tokio::test]
    async fn completion_racing_with_cancel_commits_exactly_one_terminal_state() {
        let root = temp_run_dir("completion-cancel-race");
        let _ = std::fs::remove_dir_all(&root);
        let generator = root.join("generator");
        let runs = root.join("runs");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "completion-cancel-race"
name = "Completion Cancel Race"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_write = ["workspace"]

[[flow]]
id = "write"
type = "write"
artifact = { label = "Result", required = false }
[flow.params]
output_file = "result.txt"
content = "complete"

"#,
        )
        .expect("generator manifest should be written");
        let service =
            LocalQcgService::new(root.clone(), runs, None).expect("service should initialize");

        for _ in 0..32 {
            let id = service
                .start_run(StartRun {
                    generator_id: "generator".into(),
                    inputs: BTreeMap::new(),
                })
                .await
                .expect("run should start");
            tokio::task::yield_now().await;
            service
                .cancel(id.clone())
                .await
                .expect("cancel should settle the run");
            let snapshot = service
                .snapshot(id.clone())
                .await
                .expect("settled snapshot should be available");
            assert!(matches!(
                snapshot.state,
                RunStatus::Succeeded | RunStatus::Canceled
            ));
            let run_dir = service
                .run_dir_for(&id)
                .await
                .expect("run directory should exist");
            let events = read_journal_events(&run_dir).expect("journal should be valid JSONL");
            let terminal_count = events
                .iter()
                .filter(|event| {
                    matches!(
                        event.get("t").and_then(Value::as_str),
                        Some("run_finished")
                            | Some("run_error")
                            | Some("run_canceled")
                            | Some("run_interrupted")
                    )
                })
                .count();
            assert_eq!(terminal_count, 1);
        }
        drop(service);
        let _ = std::fs::remove_dir_all(&root);
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
fs_write = ["workspace"]
side_effects = "confirm"

[[permissions.commands]]
bin = "echo"
args = ["*"]
purpose = "record each confirmed iteration"
isolation = "trusted_host"
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
fs_write = ["workspace"]

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
max_iterations = 4
parallel = 1

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
content = "{{ item }}"
"#,
        )
        .expect("generator manifest should be written");
        let service =
            LocalQcgService::new(root, runs.clone(), None).expect("service should initialize");
        let id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([
                    ("outer".into(), json!(["a", "b"])),
                    ("inner".into(), json!(["x", "y"])),
                ]),
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
fs_write = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "hold resumed run", isolation = "trusted_host" }]
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
fs_write = ["workspace"]
side_effects = "allowed"
commands = [{ bin = "echo", args = ["effect"], purpose = "resume safety proof", isolation = "trusted_host" }]
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

    #[tokio::test]
    async fn checkpoint_fork_restores_files_and_resumes_with_an_explicit_state_patch() {
        let root = temp_run_dir("checkpoint-fork-generators");
        let runs = temp_run_dir("checkpoint-fork-runs");
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&runs);
        let generator = root.join("generator");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(
            generator.join("qcg.toml"),
            r#"
[generator]
id = "generator"
name = "generator"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
fs_write = ["workspace"]

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
content = "{{ inputs.marker }}"
"#,
        )
        .expect("generator manifest should be written");
        std::fs::create_dir_all(generator.join("docs/nested"))
            .expect("directory resource should be created");
        std::fs::write(generator.join("docs/guide.md"), "guide")
            .expect("directory resource file should be written");
        std::fs::write(generator.join("docs/nested/reference.md"), "reference")
            .expect("nested directory resource file should be written");
        let service = LocalQcgService::new(root, runs, None).expect("service should initialize");
        let source_id = service
            .start_run(StartRun {
                generator_id: "generator".into(),
                inputs: BTreeMap::from([("marker".into(), json!("source"))]),
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
        std::fs::create_dir_all(run_workspace_dir(&run_dir).join("reports"))
            .expect("artifact directory should be written");
        std::fs::write(run_workspace_dir(&run_dir).join("reports/result.txt"), "ok")
            .expect("artifact should be written");
        std::fs::write(
            run_meta_dir(&run_dir).join("outputs.json"),
            serde_json::to_string(&json!({
                "artifacts": [{
                    "path": "reports/result.txt",
                    "sha256": "2689367b205c16ce32ed4200942b8b8b1e262dfc70d9bc9fbc77c49699a4f1df",
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
        assert!(
            archive
                .by_name("reports/")
                .expect("directory entry")
                .is_dir()
        );
        let mut entry = archive
            .by_name("reports/result.txt")
            .expect("artifact should be present");
        assert!(
            entry.last_modified().expect("artifact timestamp").year() > 1980,
            "artifact timestamp must come from source metadata"
        );
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
