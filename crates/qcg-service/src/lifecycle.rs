use crate::artifacts::{api_bad_request, api_internal, api_not_found, event_kinds, is_safe_id};
use crate::queue::{PriorityPermits, queue_head, queue_positions};
use crate::run_dirs::{
    app_registry, direct_run_id, direct_run_meta_dir, journal_is_empty, lock_direct_run,
    lock_runs_directory, try_lock_run_execution, try_lock_store_maintenance,
    warn_if_shared_runs_dir_owned, write_run_event,
};
use crate::summaries::{
    fold_run_state, gc_run_directories, read_events_from_meta, read_optional_output_manifest,
    read_queued_identity, read_run_contract_sha256, read_run_metrics, rehydrate_runs, run_meta_dir,
    run_summary, run_workspace_dir, status_from_journal,
};
use crate::types::{
    DirectRun, DirectRunEvents, FinishTransition, LocalQcgService, LocalQcgServiceInner, RunRecord,
    RunStoreMode, ServiceError, ServiceSnapshotSource,
};
use camino::Utf8PathBuf;
use qcg_api::RunEvent;
use qcg_api::{
    ApiError, McpAuthorizationStart, McpServerList, McpServerSummary, RunListItem, RunSnapshot,
    RunStatus,
};
use qcg_contract::Contract;
use qcg_engine::{Engine, Interaction, Progress, RunFailureKind, RunOptions};
use qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES;
use qcg_policy::{DEFAULT_MAX_ACTIVE_RUNS, DEFAULT_MAX_TRACKED_RUNS};
use qcg_types::{FailureCode, FailureDetail, OutputManifest};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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
        let llm_runtime = crate::run_dirs::load_llm_runtime(providers_path.as_deref())?;
        Ok(Self {
            inner: Arc::new(LocalQcgServiceInner {
                generator_roots: roots,
                runs_dir,
                runs: RwLock::new(runs),
                llm_runtime,
                execution_permits: Arc::new(PriorityPermits::new(max_active_runs)),
                max_active_runs,
                max_tracked_runs,
                run_store_mode,
                _runs_lock: runs_lock,
                queue_notify: Arc::new(tokio::sync::Notify::new()),
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
                    priority: record.priority,
                    cancellation: record.cancellation.clone(),
                    task: Arc::clone(&record.task),
                })
                .collect::<Vec<_>>()
        };
        for run in recovered {
            self.spawn_engine_run(run);
        }
    }

    /// Periodically resumes queued runs that lost their engine task (lease
    /// contention, crash between registration and spawn). Only runs queued
    /// for longer than the grace period are retried, so freshly registered
    /// runs are never double-spawned. Runs in both store modes.
    pub fn start_queued_resumer(&self) -> JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                service.resume_stuck_queued_runs().await;
            }
        })
    }

    async fn resume_stuck_queued_runs(&self) {
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(10);
        let stuck = {
            let runs = self.inner.runs.read().await;
            runs.iter()
                .filter(|(_, record)| {
                    record.state == RunStatus::Queued
                        && record
                            .task
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .is_none()
                        && record.queued_at.is_none_or(|at| at <= cutoff)
                })
                .map(|(run_id, record)| SpawnRun {
                    run_id: run_id.clone(),
                    contract: record.contract.clone(),
                    inputs: record.inputs.clone(),
                    run_dir: record.run_dir.clone(),
                    events: record.events.clone(),
                    answers: record.answers.clone(),
                    confirmations: record.confirmations.clone(),
                    priority: record.priority,
                    cancellation: record.cancellation.clone(),
                    task: Arc::clone(&record.task),
                })
                .collect::<Vec<_>>()
        };
        for run in stuck {
            tracing::info!(run_id = %run.run_id, "resuming queued run without an engine task");
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

    pub(crate) async fn live_receiver(
        &self,
        id: &str,
    ) -> Result<broadcast::Receiver<RunEvent>, ApiError> {
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
                    queued_at: None,
                    queue_position: None,
                    priority: 0,
                    parent_run_id: read_queued_identity(&path)
                        .map(|(_, parent)| parent)
                        .unwrap_or(None),
                    metrics: read_run_metrics(&path).map_err(api_internal)?,
                });
            }
            runs.sort_by(|left, right| left.run_id.cmp(&right.run_id));
        }
        for memory_run in self.runs_from_memory().await? {
            // Live memory state (including queue position) wins over the
            // disk-folded copy of the same run.
            if let Some(slot) = runs.iter_mut().find(|run| run.run_id == memory_run.run_id) {
                *slot = memory_run;
            } else {
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
        let positions = queue_positions(&runs);
        let mut snapshots = Vec::with_capacity(runs.len());
        for (run_id, record) in runs.iter() {
            let queued = record.state == RunStatus::Queued;
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
                queued_at: queued
                    .then(|| record.queued_at.map(|at| at.to_rfc3339()))
                    .flatten(),
                queue_position: queued.then(|| positions.get(run_id).copied()).flatten(),
                priority: record.priority,
                parent_run_id: record.parent_run_id.clone(),
                metrics: read_run_metrics(&record.run_dir).map_err(api_internal)?,
            });
        }
        Ok(snapshots)
    }

    pub(crate) fn load_generator(&self, id: &str) -> Result<Contract, ApiError> {
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

    pub(crate) fn spawn_engine_run(&self, request: SpawnRun) {
        let execution_lock = match try_lock_run_execution(&request.run_dir) {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                tracing::warn!(
                    run_id = %request.run_id,
                    run_dir = %request.run_dir,
                    "run execution lease is held elsewhere; run stays queued until the lease is released"
                );
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
            priority: _priority,
            cancellation,
            task,
        } = request;
        let runtime = Arc::clone(&self.inner.llm_runtime);
        let permits = Arc::clone(&self.inner.execution_permits);
        let queue_notify = Arc::clone(&self.inner.queue_notify);
        let handle = tokio::spawn(async move {
            let _execution_lock = execution_lock;
            // Priority admission: only the queue head takes a slot, so a
            // freed slot wakes waiters but cannot be taken out of order.
            // Equal priorities keep FIFO order; cancellation aborts the wait.
            let _permit = loop {
                let head = {
                    let runs = service.inner.runs.read().await;
                    queue_head(&runs)
                };
                if head.as_deref() == Some(run_id.as_str())
                    && let Some(permit) =
                        PriorityPermits::try_take(&permits, Arc::clone(&queue_notify))
                {
                    break permit;
                }
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = queue_notify.notified() => continue,
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
            let engine = Engine::new(app_registry(Arc::clone(&runtime))).with_snapshot_source(
                Arc::new(ServiceSnapshotSource {
                    service: service.clone(),
                }),
            );
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
            drop(_permit);
            let preempted = service
                .inner
                .runs
                .read()
                .await
                .get(&run_id)
                .is_some_and(|record| record.preempted);
            if !preempted {
                service.finish_run(run_id, progress).await;
            }
            // Preempted runs stay Queued; preempt_run() respawns them so the
            // journal replay skips already finished steps.
        });
        *task.lock().unwrap_or_else(PoisonError::into_inner) = Some(handle);
    }

    pub async fn run_generator_path(&self, run: DirectRun) -> Result<OutputManifest, ApiError> {
        let contract = Contract::load(&run.generator_path).map_err(api_internal)?;
        let runtime = Arc::clone(&self.inner.llm_runtime);
        let run_id = direct_run_id(&run.output_dir);
        let metadata_dir = direct_run_meta_dir(&run.output_dir);
        let _run_lock = lock_direct_run(&metadata_dir).map_err(api_internal)?;
        warn_if_shared_runs_dir_owned(&run.output_dir);
        // Direct executions share this process's execution permits so embedded
        // and test use stays coherent with API runs. A server in another
        // process coordinates only through provider quotas (see warning above).
        let _permit = loop {
            if let Some(permit) = PriorityPermits::try_take(
                &self.inner.execution_permits,
                Arc::clone(&self.inner.queue_notify),
            ) {
                break permit;
            }
            self.inner.queue_notify.notified().await;
        };
        Engine::new(app_registry(Arc::clone(&runtime)))
            .with_snapshot_source(Arc::new(ServiceSnapshotSource {
                service: self.clone(),
            }))
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
        warn_if_shared_runs_dir_owned(&run.output_dir);
        let _permit = loop {
            if let Some(permit) = PriorityPermits::try_take(
                &self.inner.execution_permits,
                Arc::clone(&self.inner.queue_notify),
            ) {
                break permit;
            }
            self.inner.queue_notify.notified().await;
        };
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
        // The terminal race with cancel() is settled without holding the
        // run-map lock during journal and disk I/O: reconcile at most twice,
        // once per observable state.
        for _ in 0..2 {
            let record = {
                let runs = self.inner.runs.read().await;
                match runs.get(&run_id) {
                    Some(record) => record.clone(),
                    None => return,
                }
            };
            if record.state == RunStatus::Canceled {
                self.finish_canceled_run(&run_id, &record, &progress).await;
                self.inner.queue_notify.notify_waiters();
                return;
            }
            // Progress outcome for a non-canceled run. Journal appends happen
            // outside the lock; a concurrent cancel() is detected below and
            // settles through the canceled branch instead.
            let mut transition = FinishTransition::finished(RunStatus::Running);
            match &progress {
                Progress::Done(outputs) => {
                    transition.state = Some(RunStatus::Succeeded);
                    transition.artifacts = Some(outputs.clone());
                }
                Progress::Suspended(Interaction::Question { question }) => {
                    transition.state = Some(RunStatus::Waiting);
                    transition.question = Some(question.clone());
                    transition.writes.push((
                        "run_waiting",
                        json!({ "question_id": question.id, "question": question }),
                    ));
                }
                Progress::Suspended(Interaction::Confirmation { confirm }) => {
                    transition.state = Some(RunStatus::Confirming);
                    transition.confirm = Some(confirm.clone());
                    transition
                        .writes
                        .push(("confirm_request", json!({ "confirm": confirm })));
                }
                Progress::Failed(error) => {
                    transition.state = Some(if error.kind == RunFailureKind::Canceled {
                        RunStatus::Canceled
                    } else {
                        RunStatus::Failed
                    });
                    transition
                        .writes
                        .push(("run_error", json!({ "error": error.message })));
                }
                Progress::Advanced => {}
            }
            for (kind, payload) in &transition.writes {
                if let Err(error) = write_run_event(&record, kind, payload.clone()) {
                    tracing::error!(%error, %run_id, "failed to record run progress event");
                }
            }
            {
                let mut runs = self.inner.runs.write().await;
                let Some(record) = runs.get_mut(&run_id) else {
                    return;
                };
                if record.state == RunStatus::Canceled {
                    continue;
                }
                if let Some(state) = transition.state {
                    record.state = state;
                }
                record.artifacts = transition.artifacts;
                record.question = transition.question;
                record.confirm = transition.confirm;
                self.inner.queue_notify.notify_waiters();
                return;
            }
        }
    }

    /// Settles an execution whose record was canceled while it was running.
    /// A completion that won the race with the cancellation request is
    /// preserved instead of writing a second terminal event.
    async fn finish_canceled_run(&self, run_id: &str, record: &RunRecord, progress: &Progress) {
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
        let mut transition = FinishTransition::finished(RunStatus::Canceled);
        match terminal {
            Some(qcg_engine::TerminalState::Succeeded) => {
                transition.state = Some(RunStatus::Succeeded);
                transition.artifacts = match progress {
                    Progress::Done(outputs) => Some(outputs.clone()),
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
                transition.state = Some(RunStatus::Failed);
            }
            Some(qcg_engine::TerminalState::Interrupted) => {
                transition.state = Some(RunStatus::Interrupted);
            }
            Some(qcg_engine::TerminalState::Canceled) | None => {
                if terminal.is_none() {
                    transition.writes.push((
                        "run_canceled",
                        json!({
                            "reason": FailureDetail::new(
                                FailureCode::Canceled,
                                "cancellation requested",
                            ),
                        }),
                    ));
                }
            }
        }
        for (kind, payload) in &transition.writes {
            if let Err(error) = write_run_event(record, kind, payload.clone()) {
                tracing::error!(
                    %error,
                    %run_id,
                    "failed to record terminal cancellation event"
                );
            }
        }
        let mut runs = self.inner.runs.write().await;
        let Some(record) = runs.get_mut(run_id) else {
            return;
        };
        // A newer execution (resume after answer/confirm) owns the record now;
        // never overwrite a non-canceled state observed here.
        if record.state != RunStatus::Canceled {
            return;
        }
        if let Some(state) = transition.state {
            record.state = state;
        }
        record.artifacts = transition.artifacts;
        self.inner.queue_notify.notify_waiters();
    }
}

#[derive(Debug)]
pub(crate) struct SpawnRun {
    pub(crate) run_id: String,
    pub(crate) contract: Contract,
    pub(crate) inputs: BTreeMap<String, Value>,
    pub(crate) run_dir: Utf8PathBuf,
    pub(crate) events: broadcast::Sender<RunEvent>,
    pub(crate) answers: BTreeMap<String, Value>,
    pub(crate) confirmations: BTreeMap<String, bool>,
    pub(crate) priority: i32,
    pub(crate) cancellation: CancellationToken,
    pub(crate) task: Arc<Mutex<Option<JoinHandle<()>>>>,
}
