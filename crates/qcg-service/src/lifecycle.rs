use crate::artifacts::{api_bad_request, api_internal, api_not_found, event_kinds, is_safe_id};
use crate::queue::{PriorityPermits, queue_head};
use crate::run_dirs::{
    app_registry, consume_cancel_control, direct_run_id, direct_run_meta_dir,
    list_pending_cancel_controls, lock_direct_run, lock_runs_directory, try_lock_run_execution,
    try_lock_store_maintenance, warn_if_shared_runs_dir_owned, write_run_event,
};
use crate::summaries::{
    fold_run_state, gc_run_directories, has_remote_cancel_request, read_events_from_meta,
    read_optional_output_manifest, rehydrate_runs, run_meta_dir, run_workspace_dir,
    status_from_journal,
};
use crate::types::{
    DirectRun, DirectRunEvents, FinishTransition, LocalQcgService, LocalQcgServiceInner, RunRecord,
    RunStoreMode, ServiceError, ServiceSnapshotSource,
};
use camino::Utf8PathBuf;
use qcg_api::RunEvent;
use qcg_api::{
    ApiError, McpAuthorizationStart, McpServerList, McpServerSummary, RunListItem, RunStatus,
};
use qcg_contract::Contract;
use qcg_engine::{Engine, Interaction, Progress, RunFailureKind, RunOptions};
use qcg_policy::{DEFAULT_MAX_ACTIVE_RUNS, DEFAULT_MAX_TRACKED_RUNS};
use qcg_types::{FailureCode, FailureDetail, OutputManifest};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Maps a durable terminal outcome to its display status. Every memory
/// convergence from journal truth goes through this one mapping so peers
/// never disagree on what a terminal journal means (B10).
fn terminal_status(terminal: &qcg_engine::TerminalState) -> RunStatus {
    match terminal {
        qcg_engine::TerminalState::Succeeded => RunStatus::Succeeded,
        qcg_engine::TerminalState::Failed => RunStatus::Failed,
        qcg_engine::TerminalState::Canceled => RunStatus::Canceled,
        qcg_engine::TerminalState::Interrupted => RunStatus::Interrupted,
    }
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
                owner_id: uuid::Uuid::now_v7().as_simple().to_string(),
                preemption_enabled: std::sync::Mutex::new(true),
                max_total_steps: std::sync::Mutex::new(None),
            }),
        })
    }

    /// Selects whether higher-priority arrivals preempt running runs.
    /// Admission order stays priority-ordered regardless; this only toggles
    /// forced interruption of running jobs (3.2).
    pub fn set_preemption_enabled(&self, enabled: bool) {
        *self
            .inner
            .preemption_enabled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = enabled;
    }

    /// Sets the deployment ceiling for per-run total steps. The host
    /// resolves and validates the value (flag or environment); the service
    /// never reinterprets it (3.2).
    pub fn set_max_total_steps(&self, ceiling: Option<usize>) {
        *self
            .inner
            .max_total_steps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ceiling;
    }

    pub(crate) fn max_total_steps(&self) -> Option<usize> {
        *self
            .inner
            .max_total_steps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Starts the retention GC unconditionally. Hosts decide enablement via
    /// configuration (for example `QCG_AUTO_GC`) before calling, so the
    /// service never reinterprets environment policy internally (3.2).
    pub fn start_retention_gc(&self) -> Option<JoinHandle<()>> {
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

    pub(crate) async fn refresh_shared_runs(&self) -> Result<(), ServiceError> {
        let recovered = rehydrate_runs(&self.inner.runs_dir, self.inner.max_tracked_runs)?;
        let mut runs = self.inner.runs.write().await;
        runs.retain(|_, record| !record.state.is_terminal());
        for (run_id, record) in recovered {
            runs.entry(run_id).or_insert(record);
        }
        // Merge durable journal progress into existing records so a browser
        // attached to a non-owning process observes questions, answers, and
        // terminal settlement. Remote cancel requests are honored here.
        // The owner (task.is_some()) must observe them too: cancel its local
        // token so the running engine stops, but never journal from here
        // while the owner writer is live (A01/A02).
        let ids: Vec<String> = runs.keys().cloned().collect();
        for run_id in ids {
            let Some(record) = runs.get_mut(&run_id) else {
                continue;
            };
            let owns_task = record
                .task
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_some();
            if owns_task {
                // Owner path: propagate peer cancel to the local engine task.
                // The engine task converts the control mailbox to a single
                // journal event under its own writer; this refresh never
                // appends while the owner writer is live. Journal I/O
                // failures propagate instead of silently missing a cancel.
                if has_remote_cancel_request(&record.run_dir)? {
                    record.cancellation.cancel();
                }
                continue;
            }
            let state = fold_run_state(&record.run_dir)?;
            // Terminal settlement first: a bare cancel request must never
            // shadow an owner-acknowledged terminal outcome as a bare
            // "canceled" display. Cancellation only applies to runs with no
            // durable terminal state yet.
            if let Some(terminal) = state.terminal.clone() {
                record.state = terminal_status(&terminal);
                continue;
            }
            if has_remote_cancel_request(&record.run_dir)? {
                // A peer requested cancellation via the shared journal.
                // Apply locally: stop any local task and mark the request
                // accepted. Only a journaled terminal state settles this
                // into `Canceled` (A02). Settlement follows through the
                // common finalizer: the periodic resumer spawns task-less
                // accepted runs and the spawned task settles, so refresh
                // itself never journals while ownership is ambiguous (B10).
                record.cancellation.cancel();
                record.state = RunStatus::CancelRequested;
                record.preempted = false;
                record.question = None;
                record.confirm = None;
                continue;
            }
            // Refresh pending prompts and durable HITL maps from the journal.
            // Journal I/O failures fail the refresh instead of merging
            // half-read maps. Requeue instants always derive from the
            // journal so every process observes identical FIFO order.
            let mut pending = state.pending.clone();
            let (answers, confirmations) = crate::summaries::read_persisted_hitl(&record.run_dir)?;
            let journal_queued_at = crate::summaries::read_last_queued_at(&record.run_dir);
            let requeue = |record: &mut RunRecord| {
                record.state = RunStatus::Queued;
                // The durable instant wins; the memory value survives only
                // when the journal has nothing to say. Nothing here stamps a
                // fresh local time, so FIFO order is identical everywhere.
                record.queued_at = journal_queued_at.or(record.queued_at);
                record.question = None;
                record.confirm = None;
            };
            // An accepted cancel request survives prompt refreshes: the
            // request stays accepted until a terminal outcome settles it.
            // Otherwise a peer prompt would silently clear the acceptance
            // display before settlement (A02).
            let cancel_accepted = record.state == RunStatus::CancelRequested;
            match pending.take() {
                Some(Interaction::Question { question }) => {
                    // If the journal already holds an accepted answer for
                    // this question (written by a peer), resume locally.
                    if answers.contains_key(&question.id) {
                        record.answers = answers;
                        record.confirmations = confirmations;
                        if !cancel_accepted {
                            requeue(record);
                        }
                    } else {
                        record.answers = answers;
                        record.confirmations = confirmations;
                        if !cancel_accepted {
                            record.state = RunStatus::Waiting;
                            record.question = Some(question);
                            record.confirm = None;
                        }
                    }
                }
                Some(Interaction::Confirmation { confirm }) => {
                    if confirmations.contains_key(&confirm.id) {
                        record.answers = answers;
                        record.confirmations = confirmations;
                        if !cancel_accepted {
                            requeue(record);
                        }
                    } else {
                        record.answers = answers;
                        record.confirmations = confirmations;
                        if !cancel_accepted {
                            record.state = RunStatus::Confirming;
                            record.question = None;
                            record.confirm = Some(confirm);
                        }
                    }
                }
                None => {
                    record.answers = answers;
                    record.confirmations = confirmations;
                    if !cancel_accepted
                        && (record.state == RunStatus::Waiting
                            || record.state == RunStatus::Confirming)
                    {
                        requeue(record);
                    }
                }
            }
        }
        Ok(())
    }

    /// Settles a run canceled before (or without) execution through the one
    /// terminal path every other settlement uses. Callers hold the run
    /// execution lease (no live owner writer exists), so draining and the
    /// terminal append are atomic against peers. Mailbox drain comes first
    /// so the cancel request itself journals; then a terminal `run_canceled`
    /// converges memory, task slot, and waiters.
    ///
    /// Memory never precedes the journal here (B10): the in-memory state
    /// becomes `Canceled` only after the terminal event is durably
    /// appended (or when the fold already shows a terminal outcome, in
    /// which case memory converges to that exact outcome, never a second
    /// terminal event). A failed terminal append leaves memory at
    /// `CancelRequested` with the task slot cleared, so the periodic
    /// resumer retries settlement instead of wedging on a believed
    /// terminal state the journal never recorded. A task slot holding a
    /// completed handle is not a running task, so the slot is always
    /// cleared here.
    pub(crate) async fn settle_queued_cancel(
        &self,
        run_id: &str,
        run_dir: &Utf8PathBuf,
        reason: &str,
    ) {
        if let Err(error) = self.drain_cancel_controls(run_id, run_dir).await {
            tracing::warn!(run_id = %run_id, %error, "cancel drain failed during queued settlement; mailbox retained");
        }
        let terminal = match crate::summaries::fold_run_state(run_dir) {
            Ok(state) => state.terminal.clone(),
            Err(error) => {
                tracing::error!(run_id = %run_id, %error, "state fold failed during queued settlement");
                None
            }
        };
        if let Some(terminal) = terminal {
            // Already terminal: converge memory to the durable outcome.
            let mut runs = self.inner.runs.write().await;
            if let Some(record) = runs.get_mut(run_id) {
                record.state = terminal_status(&terminal);
                record.preempted = false;
                record.question = None;
                record.confirm = None;
                record
                    .task
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
            }
            self.inner.queue_notify.notify_waiters();
            return;
        }
        let snapshot = {
            let runs = self.inner.runs.read().await;
            runs.get(run_id).cloned()
        };
        let Some(record) = snapshot else {
            return;
        };
        match crate::run_dirs::write_run_event(
            &record,
            "run_canceled",
            serde_json::json!({
                "reason": FailureDetail::new(
                    FailureCode::Canceled,
                    reason,
                ),
            }),
        ) {
            Ok(()) => {
                let mut runs = self.inner.runs.write().await;
                if let Some(record) = runs.get_mut(run_id) {
                    // Terminal settlement only moves forward; a
                    // peer-settled outcome observed meanwhile is preserved.
                    if !record.state.is_terminal() {
                        record.state = RunStatus::Canceled;
                    }
                    record.preempted = false;
                    record.question = None;
                    record.confirm = None;
                    record
                        .task
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take();
                }
            }
            Err(error) => {
                tracing::error!(run_id = %run_id, %error, "queued cancel settlement failed; acceptance retained for retry");
                let mut runs = self.inner.runs.write().await;
                if let Some(record) = runs.get_mut(run_id) {
                    // Accepted but not settled: report acceptance, never a
                    // terminal state the journal does not contain.
                    if !record.state.is_terminal() {
                        record.state = RunStatus::CancelRequested;
                    }
                    record.question = None;
                    record.confirm = None;
                    record
                        .task
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take();
                }
            }
        }
        self.inner.queue_notify.notify_waiters();
    }

    /// Converts pending cancel mailbox files to single
    /// `user_cancel_requested` journal events. Integrity comes from the
    /// journal lock with a per-operation re-check, so concurrent drainers
    /// cannot double-journal. Callers hold (or have awaited the release of)
    /// the run execution lease as settlement authority, proving no live
    /// owner writer exists while draining. Each `operation_id` journals
    /// once; consumed controls are deleted (A02).
    pub(crate) async fn drain_cancel_controls(
        &self,
        run_id: &str,
        run_dir: &Utf8PathBuf,
    ) -> Result<(), ServiceError> {
        let pending = list_pending_cancel_controls(run_dir)?;
        if pending.is_empty() {
            return Ok(());
        }
        let snapshot = {
            let runs = self.inner.runs.read().await;
            runs.get(run_id).cloned()
        };
        let Some(record) = snapshot else {
            return Ok(());
        };
        for (operation_id, control) in pending {
            let requester = control
                .get("requester")
                .and_then(Value::as_str)
                .unwrap_or("peer")
                .to_string();
            // Re-check under the journal lock so two draining peers cannot
            // journal the same operation twice. An already-journaled
            // operation only consumes its control file.
            let result = crate::run_dirs::write_run_event_if(
                &record,
                "user_cancel_requested",
                json!({
                    "run_id": run_id,
                    "operation_id": operation_id,
                    "requester": requester,
                }),
                |state| {
                    if state.cancel_operations.contains(&operation_id) {
                        return Err(qcg_engine::JournalError::PreconditionFailed(
                            "cancel operation was already journaled".into(),
                        ));
                    }
                    // A run that already settled must not gain a cancel
                    // event: the request is stale, so consume it without
                    // journaling (A02).
                    if state.terminal.is_some() {
                        return Err(qcg_engine::JournalError::PreconditionFailed(
                            "run is already terminal".into(),
                        ));
                    }
                    Ok(())
                },
            );
            match result {
                Ok(()) => consume_cancel_control(run_dir, &operation_id),
                Err(crate::types::ServiceError::PreconditionFailed(_)) => {
                    consume_cancel_control(run_dir, &operation_id)
                }
                Err(error) => {
                    // Journal I/O failures keep the control file for the
                    // next drain instead of dropping the cancel.
                    tracing::warn!(run_id = %run_id, %error, "cancel drain failed; control retained");
                }
            }
        }
        Ok(())
    }

    /// Restarts durable runs that were queued or executing when the previous
    /// service process stopped. Completed steps are replayed from the journal;
    /// their pinned files and resources are verified by the engine before any
    /// remaining work is admitted. Runs left in accepted-but-unsettled
    /// `CancelRequested` (acceptance journaled or observed, terminal event
    /// never written) are adopted too: the spawned task observes the
    /// cancellation and settles through the common finalizer instead of
    /// leaving the acceptance terminally un-converged (B10).
    pub async fn resume_recovered_runs(&self) {
        let recovered = {
            let runs = self.inner.runs.read().await;
            runs.iter()
                // Ephemeral direct placeholders are driven inline, never by
                // a spawned engine task; adopting one would double-execute.
                .filter(|(_, record)| {
                    !record.ephemeral
                        && (record.state == RunStatus::Queued
                            || record.state == RunStatus::CancelRequested)
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
                    !record.ephemeral
                        && record
                            .task
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .is_none()
                        // Stale queue entries re-enter admission; accepted
                        // cancellations without a task converge through the
                        // common finalizer on the next spawn (B10).
                        && ((record.state == RunStatus::Queued
                            && record.queued_at.is_none_or(|at| at <= cutoff))
                            || record.state == RunStatus::CancelRequested)
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

    /// Whether this process drives the run's live event broadcast.
    /// Exclusive stores always do; shared stores only when this process owns
    /// execution or still runs the local engine task. Otherwise subscribers
    /// must follow the durable journal so peer progress stays visible.
    pub(crate) async fn owns_live_stream(&self, id: &str) -> bool {
        if self.inner.run_store_mode == RunStoreMode::Exclusive {
            return true;
        }
        let runs = self.inner.runs.read().await;
        let Some(record) = runs.get(id) else {
            return false;
        };
        if record
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
        {
            return true;
        }
        record.owner_id == self.inner.owner_id
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

    pub async fn list_run_items(&self) -> Result<Vec<RunListItem>, ApiError> {
        // Single directory scan lives in `list_run_summaries`, which folds
        // each run exactly once and returns its `last_seq` alongside: this
        // layer only projects into list items so scan, filter, fold, and
        // sort never drift apart and no second fold per run exists.
        let summaries =
            crate::summaries::list_run_summaries(&self.inner.runs_dir).map_err(api_internal)?;
        let mut items = Vec::with_capacity(summaries.len());
        for (summary, seq) in summaries {
            items.push(RunListItem {
                run_id: summary.run_id,
                state: status_from_journal(&summary.status).map_err(api_internal)?,
                generator_id: summary
                    .generator
                    .split_once('@')
                    .map(|(id, _)| id.to_string())
                    .unwrap_or(summary.generator),
                started_at: summary.started_at,
                seq,
            });
        }
        Ok(items)
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
            // Convert pending cancel mailbox entries to single journal events
            // before the engine writer starts. This task holds the execution
            // lease, so no other owner appends concurrently (A01/A02). A
            // failed drain refuses to start: settle terminally so the run
            // converges instead of wedging half-started past a cancel.
            if let Err(error) = service.drain_cancel_controls(&run_id, &run_dir).await {
                tracing::error!(run_id = %run_id, %error, "cancel drain failed; refusing to start execution");
                service
                    .settle_queued_cancel(&run_id, &run_dir, "cancel drain failed before execution")
                    .await;
                return;
            }
            // Priority admission: only the queue head takes a slot, so a
            // freed slot wakes waiters but cannot be taken out of order.
            // Equal priorities keep FIFO order; cancellation aborts the wait.
            // The notified future is created before the state check so a
            // notify between check and wait is never lost (A11).
            let _permit = loop {
                let notified = queue_notify.notified();
                // A peer cancel observed while queued settles without ever
                // starting the engine (A02). Journal I/O failures fail
                // closed: an unreadable cancel state must not start work.
                let queued_cancel = match crate::summaries::has_remote_cancel_request(&run_dir) {
                    Ok(cancel) => cancel,
                    Err(error) => {
                        tracing::error!(%error, run_id = %run_id, "cancel check failed while queued; settling as canceled");
                        true
                    }
                };
                if queued_cancel || cancellation.is_cancelled() {
                    // Terminal settlement through the common finalizer: an
                    // accepted cancel converges instead of lingering.
                    service
                        .settle_queued_cancel(&run_id, &run_dir, "cancellation requested")
                        .await;
                    return;
                }
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
                    _ = cancellation.cancelled() => {
                        service
                            .settle_queued_cancel(&run_id, &run_dir, "cancellation requested")
                            .await;
                        return;
                    }
                    _ = notified => continue,
                }
            };
            {
                let mut runs = service.inner.runs.write().await;
                let Some(record) = runs.get_mut(&run_id) else {
                    return;
                };
                // Re-check peer cancel after admission: a cancel that landed
                // while waiting must win over starting execution (A02).
                // An unreadable cancel state refuses to start execution:
                // starting work nobody can observe or stop is the unsafe
                // direction, so the error arm means cancel.
                let peer_cancel = match crate::summaries::has_remote_cancel_request(&run_dir) {
                    Ok(cancel) => cancel,
                    Err(error) => {
                        tracing::error!(%error, run_id = %run_id, "cancel check failed; refusing to start execution");
                        true
                    }
                };
                // A settled run never regresses: only non-terminal records
                // may move into accepted cancellation.
                if record.state.is_terminal() {
                    return;
                }
                if record.state == RunStatus::CancelRequested
                    || peer_cancel
                    || cancellation.is_cancelled()
                {
                    drop(runs);
                    // Terminal settlement through the common finalizer:
                    // execution never starts, so settle without starting it.
                    service
                        .settle_queued_cancel(&run_id, &run_dir, "cancellation requested")
                        .await;
                    return;
                }
                // Never resume past a durable terminal state: a stale Queued
                // memory record after a peer settlement must not re-run.
                match crate::summaries::fold_run_state(&run_dir) {
                    Ok(state) if state.terminal.is_some() => {
                        record.state = match state.terminal {
                            Some(terminal) => terminal_status(&terminal),
                            None => RunStatus::Canceled,
                        };
                        record.preempted = false;
                        record.question = None;
                        record.confirm = None;
                        return;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!(%error, run_id = %run_id, "state fold failed; refusing to start execution");
                        return;
                    }
                }
                record.state = RunStatus::Running;
                // Claim shared-store ownership while holding the execution
                // lease so peers observe the active owner.
                record.owner_id = service.inner.owner_id.clone();
            }
            let policy = crate::types::ResolvedExecutionPolicy::resolve(
                contract.manifest.budget.max_steps,
                service.max_total_steps(),
            );
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
                        max_total_steps: policy.max_total_steps,
                        max_parallel_steps: policy.max_parallel_steps,
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
        // Admission canonicalization matches the API path so memory records
        // and journal execution observe identical inputs.
        let canonical_direct_inputs = match contract.manifest.resolve_inputs(run.inputs.clone()) {
            Ok(resolved) => qcg_engine::canonical_file_inputs(&contract, resolved)
                .map_err(|error| ApiError::invalid_field("inputs", error.to_string()))?,
            Err(qcg_contract::ContractError::PayloadTooLarge {
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
        };
        let runtime = Arc::clone(&self.inner.llm_runtime);
        let run_id = direct_run_id(&run.output_dir);
        let metadata_dir = direct_run_meta_dir(&run.output_dir);
        let _run_lock = lock_direct_run(&metadata_dir).map_err(api_internal)?;
        warn_if_shared_runs_dir_owned(&run.output_dir);
        // Unified scheduler: direct executions register an ephemeral Queued
        // record so the same queue_head ordering governs API and direct runs.
        // Priority 0, FIFO by admission; removed on completion.
        let (events, _) = broadcast::channel(512);
        let ephemeral_dir = metadata_dir
            .parent()
            .map(|parent| parent.to_owned())
            .unwrap_or_else(|| metadata_dir.clone());
        {
            let mut runs = self.inner.runs.write().await;
            // A stale ephemeral from a crashed direct run must not block.
            if runs
                .get(&run_id)
                .is_none_or(|record| record.state.is_terminal())
            {
                runs.insert(
                    run_id.clone(),
                    RunRecord {
                        contract: contract.clone(),
                        contract_sha256: contract.sha256.clone(),
                        inputs: canonical_direct_inputs.clone(),
                        answers: run.answers.clone(),
                        confirmations: run.confirmations.clone(),
                        priority: 0,
                        parent_run_id: None,
                        preempted: false,
                        state: RunStatus::Queued,
                        run_dir: ephemeral_dir,
                        artifacts: None,
                        question: None,
                        confirm: None,
                        events: events.clone(),
                        cancellation: CancellationToken::new(),
                        task: Arc::new(Mutex::new(None)),
                        queued_at: Some(chrono::Utc::now()),
                        owner_id: self.inner.owner_id.clone(),
                        ephemeral: true,
                    },
                );
            }
        }
        struct RemoveEphemeralOnDrop {
            service: LocalQcgService,
            run_id: String,
        }
        impl Drop for RemoveEphemeralOnDrop {
            fn drop(&mut self) {
                let service = self.service.clone();
                let run_id = self.run_id.clone();
                tokio::spawn(async move {
                    service.inner.runs.write().await.remove(&run_id);
                    service.inner.queue_notify.notify_waiters();
                });
            }
        }
        let _ephemeral_guard = RemoveEphemeralOnDrop {
            service: self.clone(),
            run_id: run_id.clone(),
        };
        let _permit = loop {
            let notified = self.inner.queue_notify.notified();
            let head = {
                let runs = self.inner.runs.read().await;
                queue_head(&runs)
            };
            if head.as_deref() == Some(run_id.as_str())
                && let Some(permit) = PriorityPermits::try_take(
                    &self.inner.execution_permits,
                    Arc::clone(&self.inner.queue_notify),
                )
            {
                break permit;
            }
            // Direct runs never preempt API runs; they wait for head.
            notified.await;
        };
        {
            let mut runs = self.inner.runs.write().await;
            if let Some(record) = runs.get_mut(&run_id) {
                record.state = RunStatus::Running;
            }
        }
        let policy = crate::types::ResolvedExecutionPolicy::resolve(
            contract.manifest.budget.max_steps,
            self.max_total_steps(),
        );
        let result = Engine::new(app_registry(Arc::clone(&runtime)))
            .with_snapshot_source(Arc::new(ServiceSnapshotSource {
                service: self.clone(),
            }))
            .run_with_id(
                run_id.clone(),
                metadata_dir,
                contract,
                canonical_direct_inputs.clone(),
                RunOptions {
                    output_dir: run.output_dir,
                    json_events: run.json_events,
                    event_sender: None,
                    interactive: run.interactive,
                    answers: run.answers,
                    confirmations: run.confirmations,
                    max_total_steps: policy.max_total_steps,
                    max_parallel_steps: policy.max_parallel_steps,
                    llm_provider: Some(Arc::clone(&runtime.provider)),
                    llm_seed_override: run.llm_seed_override,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .map_err(api_internal);
        self.inner.runs.write().await.remove(&run_id);
        self.inner.queue_notify.notify_waiters();
        result
    }

    pub async fn run_generator_path_with_events(
        &self,
        run: DirectRun,
    ) -> Result<DirectRunEvents, ApiError> {
        let contract = Contract::load(&run.generator_path).map_err(api_internal)?;
        let canonical_direct_inputs = match contract.manifest.resolve_inputs(run.inputs.clone()) {
            Ok(resolved) => qcg_engine::canonical_file_inputs(&contract, resolved)
                .map_err(|error| ApiError::invalid_field("inputs", error.to_string()))?,
            Err(qcg_contract::ContractError::PayloadTooLarge {
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
        };
        let runtime = Arc::clone(&self.inner.llm_runtime);
        let (events, mut receiver) = broadcast::channel(512);
        let run_id = direct_run_id(&run.output_dir);
        let metadata_dir = direct_run_meta_dir(&run.output_dir);
        let _run_lock = lock_direct_run(&metadata_dir).map_err(api_internal)?;
        warn_if_shared_runs_dir_owned(&run.output_dir);
        let ephemeral_dir = metadata_dir
            .parent()
            .map(|parent| parent.to_owned())
            .unwrap_or_else(|| metadata_dir.clone());
        {
            let mut runs = self.inner.runs.write().await;
            if runs
                .get(&run_id)
                .is_none_or(|record| record.state.is_terminal())
            {
                runs.insert(
                    run_id.clone(),
                    RunRecord {
                        contract: contract.clone(),
                        contract_sha256: contract.sha256.clone(),
                        inputs: canonical_direct_inputs.clone(),
                        answers: run.answers.clone(),
                        confirmations: run.confirmations.clone(),
                        priority: 0,
                        parent_run_id: None,
                        preempted: false,
                        state: RunStatus::Queued,
                        run_dir: ephemeral_dir,
                        artifacts: None,
                        question: None,
                        confirm: None,
                        events: events.clone(),
                        cancellation: CancellationToken::new(),
                        task: Arc::new(Mutex::new(None)),
                        queued_at: Some(chrono::Utc::now()),
                        owner_id: self.inner.owner_id.clone(),
                        ephemeral: true,
                    },
                );
            }
        }
        struct RemoveEphemeralOnDrop {
            service: LocalQcgService,
            run_id: String,
        }
        impl Drop for RemoveEphemeralOnDrop {
            fn drop(&mut self) {
                let service = self.service.clone();
                let run_id = self.run_id.clone();
                tokio::spawn(async move {
                    service.inner.runs.write().await.remove(&run_id);
                    service.inner.queue_notify.notify_waiters();
                });
            }
        }
        let _ephemeral_guard = RemoveEphemeralOnDrop {
            service: self.clone(),
            run_id: run_id.clone(),
        };
        let _permit = loop {
            let notified = self.inner.queue_notify.notified();
            let head = {
                let runs = self.inner.runs.read().await;
                queue_head(&runs)
            };
            if head.as_deref() == Some(run_id.as_str())
                && let Some(permit) = PriorityPermits::try_take(
                    &self.inner.execution_permits,
                    Arc::clone(&self.inner.queue_notify),
                )
            {
                break permit;
            }
            notified.await;
        };
        {
            let mut runs = self.inner.runs.write().await;
            if let Some(record) = runs.get_mut(&run_id) {
                record.state = RunStatus::Running;
            }
        }
        let policy = crate::types::ResolvedExecutionPolicy::resolve(
            contract.manifest.budget.max_steps,
            self.max_total_steps(),
        );
        let manifest = Engine::new(app_registry(Arc::clone(&runtime)))
            .run_with_id(
                run_id.clone(),
                metadata_dir.clone(),
                contract,
                canonical_direct_inputs.clone(),
                RunOptions {
                    output_dir: run.output_dir.clone(),
                    json_events: false,
                    event_sender: Some(events),
                    interactive: run.interactive,
                    answers: run.answers,
                    confirmations: run.confirmations,
                    max_total_steps: policy.max_total_steps,
                    max_parallel_steps: policy.max_parallel_steps,
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
            self.inner.runs.write().await.remove(&run_id);
            self.inner.queue_notify.notify_waiters();
            return Err(api_internal(format!(
                "direct run event stream diverged from journal in `{}`",
                run.output_dir
            )));
        }
        self.inner.runs.write().await.remove(&run_id);
        self.inner.queue_notify.notify_waiters();
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
            if matches!(
                record.state,
                RunStatus::Canceled | RunStatus::CancelRequested
            ) {
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
                    // Journal failure with a finished engine: memory still
                    // advances so snapshots report the true outcome, and the
                    // next restart replays the journal and retries these
                    // settlement appends. Disk failures surface here and in
                    // every later journal write until repaired.
                    tracing::error!(%error, %run_id, "failed to record run progress event");
                }
            }
            {
                let mut runs = self.inner.runs.write().await;
                let Some(record) = runs.get_mut(&run_id) else {
                    return;
                };
                if matches!(
                    record.state,
                    RunStatus::Canceled | RunStatus::CancelRequested
                ) {
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
        // Consume the cancel mailbox before settling: without this drain a
        // canceled run retains its control files forever and
        // `has_pending_cancel_control` never clears (A02). Against an
        // already-terminal journal the drain only consumes. This task holds
        // the execution lease, so no other owner appends concurrently.
        if let Err(error) = self.drain_cancel_controls(run_id, &record.run_dir).await {
            tracing::warn!(
                %error,
                %run_id,
                "cancel drain failed during cancellation settlement; mailbox retained"
            );
        }
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
        // never overwrite a non-canceled state observed here. An accepted
        // cancel request settles through the same terminal path.
        if !matches!(
            record.state,
            RunStatus::Canceled | RunStatus::CancelRequested
        ) {
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
