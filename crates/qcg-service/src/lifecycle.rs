use crate::artifacts::{api_bad_request, api_internal, api_not_found, event_kinds, is_safe_id};
use crate::queue::{PriorityPermits, queue_head};
use crate::run_dirs::{
    app_registry, claim_run_execution_owner, consume_cancel_control, direct_run_id,
    direct_run_meta_dir, list_pending_cancel_controls, lock_direct_run, lock_runs_directory,
    lock_runs_directory_shared, read_run_execution_owner, try_lock_run_execution,
    try_lock_store_maintenance, warn_if_shared_runs_dir_owned, write_run_event,
};
use crate::summaries::{
    fold_run_state, gc_run_directories, has_remote_cancel_request, read_last_queued_at,
    read_merged_events_from_meta, read_optional_output_manifest, read_persisted_hitl,
    rehydrate_runs, run_meta_dir, run_workspace_dir, status_from_journal,
};
use crate::types::{
    DirectRun, DirectRunEvents, FinishTransition, LocalQcgService, LocalQcgServiceInner, RunRecord,
    RunStoreMode, ServiceDeploymentPolicy, ServiceError, ServiceSnapshotSource,
};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::RunEvent;
use qcg_api::{
    ApiError, McpAuthorizationStart, McpServerList, McpServerSummary, RunListItem, RunStatus,
};
use qcg_contract::Contract;
use qcg_engine::{Engine, Interaction, Progress, RunFailureKind, RunOptions};
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

/// Drop fallback for direct-run ephemeral records (E05): `Drop` cannot
/// await, so abandonment (panic, early return, shutdown race) spawns a
/// best-effort removal. The normal path removes synchronously via
/// `remove_ephemeral_now` and forgets the guard, so the spawned fallback
/// never double-removes on success. Shutdown must not rely on the fallback
/// alone: it races the same shutdown it cleans up after.
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

impl LocalQcgService {
    /// Constructor that takes deployment policy up front so hosts never race
    /// a post-construction setter against recovery execution (E04). Policy
    /// is frozen into the service before it is returned: no recovery or
    /// resident task can observe a half-configured service.
    pub fn with_generator_roots_policy_and_store_mode(
        generator_roots: Vec<Utf8PathBuf>,
        runs_dir: Utf8PathBuf,
        providers_path: Option<Utf8PathBuf>,
        max_active_runs: usize,
        max_tracked_runs: usize,
        run_store_mode: RunStoreMode,
        policy: ServiceDeploymentPolicy,
    ) -> Result<Self, ServiceError> {
        Self::build_service(
            generator_roots,
            runs_dir,
            providers_path,
            max_active_runs,
            max_tracked_runs,
            run_store_mode,
            policy,
        )
    }

    /// Shared construction core: validates limits, acquires the store lock,
    /// rehydrates, and freezes the deployment policy in one step. The older
    /// 6-argument constructor below delegates with the default policy; the
    /// policy constructor above delegates with the host policy. Neither
    /// exposes a half-built service (E04).
    ///
    /// Host ordering contract (E04): construction performs lock plus
    /// directory creation plus rehydration only, and starts no resident
    /// task and no recovery. The host must build the router next and run
    /// recovery (`resume_recovered_runs`, shared-store recovery, GC,
    /// resumer) only after the router succeeds, so a refused boot drops
    /// the service with its lock released and zero recovered side effects.
    /// No resident or recovery work may run before router success.
    fn build_service(
        generator_roots: Vec<Utf8PathBuf>,
        runs_dir: Utf8PathBuf,
        providers_path: Option<Utf8PathBuf>,
        max_active_runs: usize,
        max_tracked_runs: usize,
        run_store_mode: RunStoreMode,
        policy: ServiceDeploymentPolicy,
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
        // The runs directory itself (not its contents) is created here; the
        // lock file inside it is created by the lock call below. On lock
        // contention nothing is removed: unlinking a locked path would let
        // a later boot lock a fresh inode while the holder still owns the
        // old one (split brain). A refused boot therefore leaves the
        // directory and the released lock file behind; only the lock fd is
        // released by drop (E04).
        std::fs::create_dir_all(&runs_dir)?;
        let runs_lock = Some(match run_store_mode {
            RunStoreMode::Exclusive => lock_runs_directory(&runs_dir)?,
            RunStoreMode::SharedFilesystem => lock_runs_directory_shared(&runs_dir)?,
        });
        let runs = rehydrate_runs(
            &runs_dir,
            max_tracked_runs,
            policy.max_directory_scan_entries,
        )?;
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
                deployment_policy: policy,
                shutdown: CancellationToken::new(),
                journal_pollers: std::sync::Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// Marks the service as shutting down. New admissions are refused and
    /// existing runs are settled by the caller's shutdown sequence (E05).
    /// This cancels the same token returned by [`Self::shutdown_token`]:
    /// the two are one shutdown state, not two APIs that can drift.
    pub fn mark_shutting_down(&self) {
        self.inner.shutdown.cancel();
    }

    /// The service's shutdown token. Server hosts wire resident tasks, SSE,
    /// and the admission gate to this single token so an embedded service
    /// and its server cannot drift into two shutdown states (E05).
    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }

    /// Whether the embedding server has started graceful shutdown.
    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutdown.is_cancelled()
    }

    /// Aborts every engine task synchronously for abort-path cleanup. The
    /// graceful path settles tasks instead and never calls this. Best
    /// effort by necessity (`Drop` cannot await): only uncontended locks
    /// are taken, never blocking the executor; contended work is left for
    /// process teardown. Aborted tasks release their service clones (and
    /// the run-store lock) as they exit (E05). A contended skip is not a
    /// leak by itself: distinguishing it from a true leak requires a
    /// poll/wait reacquire assertion, which the abort-path test below does
    /// with a bounded sleep (E05).
    pub fn abort_all_engine_tasks(&self) {
        // Cancel the shared shutdown token first so detached pollers and
        // requeue spawns (which hold service clones) observe shutdown and
        // exit instead of pinning the run-store lock after abort (E05).
        self.inner.shutdown.cancel();
        let Ok(runs_guard) = self.inner.runs.try_read() else {
            tracing::warn!("abort-path engine cleanup skipped: run map is locked");
            return;
        };
        Self::abort_guarded(&runs_guard);
    }

    fn abort_guarded(runs: &std::collections::BTreeMap<String, crate::types::RunRecord>) {
        for (run_id, record) in runs.iter() {
            // Single non-blocking attempt: sleeping or blocking the
            // executor here would stall the abort path itself (E05).
            // A contended slot is left for teardown.
            let Ok(mut slot) = record.task.try_lock() else {
                tracing::warn!(run_id = %run_id, "abort-path engine cleanup skipped: task slot is locked");
                continue;
            };
            if let Some(handle) = slot.take() {
                if !handle.is_finished() {
                    tracing::warn!(run_id = %run_id, "aborting engine task on serve abort");
                }
                handle.abort();
            }
        }
    }

    /// Deployment policy is frozen at construction through
    /// [`Self::with_generator_roots_policy_and_store_mode`]: post-construction
    /// setters were removed so policy can never race recovery execution (E04).
    /// Stored by value (`Copy`), read without locking.
    pub(crate) fn max_total_steps(&self) -> Option<usize> {
        self.inner.deployment_policy.max_total_steps
    }

    /// Deployment policy accessor without locking (E04): frozen by value at
    /// construction, never mutated afterwards.
    pub(crate) fn preemption_enabled(&self) -> bool {
        self.inner.deployment_policy.preemption_enabled
    }

    /// Starts the retention GC unconditionally. Hosts decide enablement via
    /// configuration (for example `QCG_AUTO_GC`) before calling, so the
    /// service never reinterprets environment policy internally (3.2).
    /// Each pass retries transient failures boundedly and the task ends
    /// with Err when the bound is exceeded, so a wedged store surfaces at
    /// shutdown instead of looping silently as Ok forever (E05).
    pub fn start_retention_gc(
        &self,
        shutdown: CancellationToken,
    ) -> Option<JoinHandle<Result<(), ServiceError>>> {
        let service = self.clone();
        Some(tokio::spawn(async move {
            // The first pass is cancellable too: a service that starts
            // during shutdown must not run an uncancellable GC sweep (E05).
            let first = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                result = service.collect_retained_runs_with_retry() => result,
            };
            if let Err(error) = first {
                tracing::error!(%error, "run retention failed; ending GC task");
                return Err(error);
            }
            let interval_secs = service.inner.deployment_policy.gc_interval_secs.max(1);
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = interval.tick() => {}
                }
                let result = service.collect_retained_runs_with_retry().await;
                if let Err(error) = result {
                    tracing::error!(%error, "run retention failed; ending GC task");
                    return Err(error);
                }
            }
        }))
    }

    /// One GC pass with bounded retries for transient lease and sweep
    /// failures. Contended maintenance locks (another process owns the
    /// sweep) are a normal skip, not a failure. Persistent errors return
    /// Err so the owning task ends visibly instead of looping silently
    /// (E05).
    async fn collect_retained_runs_with_retry(&self) -> Result<(), ServiceError> {
        let mut last_error = None;
        for _ in 0..3 {
            match self.collect_retained_runs().await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(%error, "run retention pass failed; retrying");
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
        // The loop above returns on the first success, so reaching here
        // means all three attempts failed and a last error exists (E03).
        match last_error {
            Some(error) => Err(error),
            None => Err(ServiceError::Invalid(
                "run retention failed without a recorded error".into(),
            )),
        }
    }

    /// Refreshes the external model catalog in the background while the
    /// configured metadata is stale. Failures are logged and retried on the
    /// next interval: a broken catalog source never blocks serving, and the
    /// catalog view keeps reporting the error and stale flag.
    pub fn start_catalog_refresh(
        &self,
        shutdown: CancellationToken,
    ) -> Option<JoinHandle<Result<(), ServiceError>>> {
        let catalog = Arc::clone(&self.inner.llm_runtime.catalog);
        if !catalog.has_sources() {
            return None;
        }
        let interval = catalog.refresh_interval();
        Some(tokio::spawn(async move {
            // The first refresh is shutdown-selectable like every later
            // tick: a stop racing service boot grace-aborts instead of
            // blocking teardown on catalog I/O (E05).
            if catalog.is_stale() {
                let refreshed = tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    result = catalog.refresh() => result,
                };
                if let Err(error) = refreshed {
                    tracing::warn!(%error, "model catalog refresh failed; keeping cached metadata");
                }
            }
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = ticker.tick() => {
                        if catalog.is_stale()
                            && let Err(error) = catalog.refresh().await
                        {
                            tracing::warn!(
                                %error,
                                "model catalog refresh failed; keeping cached metadata"
                            );
                        }
                    }
                }
            }
        }))
    }

    pub fn start_shared_store_recovery(
        &self,
        shutdown: CancellationToken,
    ) -> Option<JoinHandle<Result<(), ServiceError>>> {
        if self.inner.run_store_mode != RunStoreMode::SharedFilesystem {
            return None;
        }
        let service = self.clone();
        let rescan_secs = self.inner.deployment_policy.shared_store_rescan_secs.max(1);
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(rescan_secs));
            let mut consecutive_failures: u32 = 0;
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = interval.tick() => {}
                }
                if let Err(error) = service.refresh_shared_runs().await {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    tracing::error!(%error, failures = consecutive_failures, "failed to refresh shared run store");
                    // Persistent failures end the task visibly instead of
                    // looping silently with a wedged store (E05).
                    if consecutive_failures >= 10 {
                        return Err(error);
                    }
                    continue;
                }
                consecutive_failures = 0;
                service.resume_recovered_runs().await;
            }
        }))
    }

    pub(crate) async fn refresh_shared_runs(&self) -> Result<(), ServiceError> {
        let recovered = rehydrate_runs(
            &self.inner.runs_dir,
            self.inner.max_tracked_runs,
            self.inner.deployment_policy.max_directory_scan_entries,
        )?;
        {
            let mut runs = self.inner.runs.write().await;
            runs.retain(|_, record| !record.state.is_terminal());
            for (run_id, record) in recovered {
                runs.entry(run_id).or_insert(record);
            }
        }
        // Merge durable journal progress into existing records so a browser
        // attached to a non-owning process observes questions, answers, and
        // terminal settlement. Remote cancel requests are honored here.
        // The owner (task.is_some()) must observe them too: cancel its local
        // token so the running engine stops, but never journal from here
        // while the owner writer is live (A01/A02).
        // Per-run lock release: filesystem I/O (cancel probe, fold, HITL)
        // runs without holding the run-map lock, so refresh latency scales
        // with I/O, not with lock contention against admission and
        // subscribe (E12). Each iteration snapshots one run_dir, drops the
        // lock for I/O, then re-acquires briefly to merge.
        // A registration landing between the snapshot and its merge must
        // not wait a full interval: one bounded delta pass covers ids that
        // appeared mid-refresh. Two rounds cap the work under continuous
        // admissions; anything newer waits for the next interval (E12).
        // Staleness bound: the id collect/snapshot/merge gaps above mean a
        // refresh observes state at most one 5 s interval plus two delta
        // rounds old. That only delays convergence, never corrupts it: every
        // merge re-derives from journal truth under a fresh write lock, so
        // a racing admission or settlement wins by re-merge on the next
        // interval instead of by lock ordering (E12).
        let mut refreshed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for _ in 0..2 {
            let ids: Vec<String> = {
                let runs = self.inner.runs.read().await;
                runs.keys()
                    .filter(|id| !refreshed.contains(id.as_str()))
                    .cloned()
                    .collect()
            };
            if ids.is_empty() {
                break;
            }
            for run_id in &ids {
                let run_dir: Option<Utf8PathBuf> = {
                    let runs = self.inner.runs.read().await;
                    runs.get(run_id.as_str()).map(|r| r.run_dir.clone())
                };
                let Some(run_dir) = run_dir else {
                    continue;
                };
                // Filesystem observations without any run-map lock.
                let cancel_probe = has_remote_cancel_request(&run_dir)?;
                let state = fold_run_state(&run_dir)?;
                let hitl = read_persisted_hitl(&run_dir)?;
                let journal_queued_at = read_last_queued_at(&run_dir);
                // Short write lock for the in-memory merge only.
                let mut runs = self.inner.runs.write().await;
                let Some(record) = runs.get_mut(run_id.as_str()) else {
                    continue;
                };
                // Owner hand-off convergence (E12): adopt a changed
                // execution-owner claim observed on disk. The claim is
                // written by whoever holds the execution lease at spawn, so
                // a peer takeover (shared-store hand-off, restart under a
                // new owner id) surfaces here within one refresh interval
                // instead of only at spawn time. The lease plus the journal
                // stay authoritative; the claim is an advisory hint, and an
                // unscannable claim keeps the current owner (never a guess).
                if let Some(claimed) = read_run_execution_owner(&run_dir)
                    && claimed != record.owner_id
                {
                    tracing::info!(run_id = %run_id, old_owner = %record.owner_id, new_owner = %claimed, "shared refresh observed an execution owner change");
                    record.owner_id = claimed;
                }
                // A finished handle no longer owns execution: merge durable
                // progress for it instead of treating it as a live owner
                // (E12). Live owners also merge read-only below: the fold and
                // HITL maps converge terminal/pending/answers/prompts into
                // memory without ever appending while the owner writer is live.
                // Only the cancel token is prodded here; settlement stays with
                // the owner writer (A01/A02/E12). The pre-I/O observations
                // above are reused; no second filesystem probe occurs here.
                if crate::types::task_slot_is_live(&record.task) {
                    // Owner path: propagate peer cancel to the local engine task.
                    // The engine task converts the control mailbox to a single
                    // journal event under its own writer; this refresh never
                    // appends while the owner writer is live. Journal I/O
                    // failures propagate instead of silently missing a cancel.
                    if cancel_probe {
                        record.cancellation.cancel();
                    }
                    // Fall through to the read-only merge below instead of
                    // continuing: a live Waiting owner whose peer answered must
                    // converge instead of staying stale (E12).
                }
                // Terminal settlement first: a bare cancel request must never
                // shadow an owner-acknowledged terminal outcome as a bare
                // "canceled" display. Cancellation only applies to runs with no
                // durable terminal state yet.
                if let Some(terminal) = state.terminal.clone() {
                    record.state = terminal_status(&terminal);
                    continue;
                }
                if cancel_probe {
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
                // The pre-I/O HITL snapshot is reused (no second read).
                let mut pending = state.pending.clone();
                let (answers, confirmations) = hitl;
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
                refreshed.insert(run_id.to_string());
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
        // Shutdown-aware settlement at settle time (E05): a queued cancel
        // racing shutdown must settle as Interrupted, not Canceled.
        let shutting_down = self.is_shutting_down();
        let (event_kind, code, settled_state) = if shutting_down {
            (
                "run_interrupted",
                FailureCode::Interrupted,
                RunStatus::Interrupted,
            )
        } else {
            ("run_canceled", FailureCode::Canceled, RunStatus::Canceled)
        };
        match crate::run_dirs::write_run_event(
            &record,
            event_kind,
            serde_json::json!({
                "reason": FailureDetail::new(
                    code,
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
                        record.state = settled_state;
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
            // Display-only attribution for the journal event. A missing or
            // empty requester (pre-attribution controls, or a writer that
            // omitted it) is journaled as `unknown` rather than dropped:
            // the cancel itself is fully identified by the bound file
            // name, `op`, `run_id`, and `operation_id`, so dropping it
            // would lose a legitimate cancellation for a cosmetic field
            // (E01).
            let requester = control
                .get("requester")
                .and_then(Value::as_str)
                .filter(|requester| !requester.is_empty())
                .unwrap_or("unknown")
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
                Ok(()) => {
                    if !consume_cancel_control(run_dir, &operation_id) {
                        tracing::warn!(run_id = %run_id, operation_id = %operation_id, "cancel control phantom: removal failed after journaling");
                    }
                }
                Err(crate::types::ServiceError::PreconditionFailed(_)) => {
                    if !consume_cancel_control(run_dir, &operation_id) {
                        tracing::warn!(run_id = %run_id, operation_id = %operation_id, "cancel control phantom: removal failed after convergence");
                    }
                }
                Err(error) => {
                    // Journal I/O failures keep the control file for the
                    // next drain instead of dropping the cancel, but the
                    // drain as a whole reports failure so the caller
                    // refuses to start execution past an unreadable cancel
                    // state (E05). Fail-closed, never warn-and-Ok.
                    tracing::warn!(run_id = %run_id, %error, "cancel drain failed; control retained");
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// Restarts durable runs that were queued, executing, or suspended on
    /// human input when the previous service process stopped. Completed
    /// steps are replayed from the journal; their pinned files and resources
    /// are verified by the engine before any remaining work is admitted.
    /// Waiting/Confirming runs re-issue their journaled prompt (or consume
    /// a durably answered one) through the same spawn path. Runs left in
    /// accepted-but-unsettled `CancelRequested` (acceptance journaled or
    /// observed, terminal event never written) are adopted too: the spawned
    /// task observes the cancellation and settles through the common
    /// finalizer instead of leaving the acceptance terminally un-converged
    /// (B10).
    pub async fn resume_recovered_runs(&self) {
        if self.is_shutting_down() {
            return;
        }
        // Disk scan for untracked Queued orphans (Q3): shutdown leaves
        // never-tracked Queued journals on disk (see shutdown exception),
        // and a peer may have admitted Queued runs this process never
        // rehydrated. Memory-only resume would strand them forever, so
        // adopt disk-only Queued runs into memory before spawning. Only
        // Queued (never-started, no pending, no cancel, no terminal) is
        // adopted here: other states either settled at shutdown or converge
        // via shared refresh; adopting them here would resurrect settled
        // work. Fail-closed on unreadable journals (skip, never guess).
        {
            let known: std::collections::BTreeSet<String> =
                { self.inner.runs.read().await.keys().cloned().collect() };
            // Reuse the construction-time rehydration (bounded, single read
            // per run) and merge only untracked Queued orphans.
            match crate::summaries::rehydrate_runs(
                &self.inner.runs_dir,
                self.inner.max_tracked_runs,
                self.inner.deployment_policy.max_directory_scan_entries,
            ) {
                Ok(disk) => {
                    let mut runs = self.inner.runs.write().await;
                    for (run_id, record) in disk {
                        if known.contains(&run_id) || runs.contains_key(&run_id) {
                            continue;
                        }
                        // Only the shutdown-exception shape resumes here:
                        // Queued with no live task. Waiting/Confirming etc.
                        // are owned via refresh or settled at shutdown.
                        if record.state == RunStatus::Queued {
                            runs.insert(run_id, record);
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "resume disk scan for untracked queued orphans failed; memory-only resume proceeds");
                }
            }
        }
        let recovered = {
            let runs = self.inner.runs.read().await;
            runs.iter()
                // Ephemeral direct placeholders live outside the runs
                // directory and are driven inline, never by a spawned engine
                // task; adopting one would double-execute.
                .filter(|(_, record)| {
                    !record.ephemeral
                        && (record.state == RunStatus::Queued
                            || record.state == RunStatus::Waiting
                            || record.state == RunStatus::Confirming
                            || record.state == RunStatus::CancelRequested)
                        && !crate::types::task_slot_is_live(&record.task)
                })
                .map(|(run_id, record)| SpawnRun {
                    run_id: run_id.clone(),
                    contract: record.contract.clone(),
                    inputs: record.inputs.clone(),
                    run_dir: record.run_dir.clone(),
                    events: record.events.clone(),
                    answers: record.answers.clone(),
                    confirmations: record.confirmations.clone(),
                    // Recovery has no admission snapshot: the spawn reads
                    // the journal once (E03).
                    journal_snapshot: None,
                    cancellation: record.cancellation.clone(),
                    task: Arc::clone(&record.task),
                })
                .collect::<Vec<_>>()
        };
        for run in recovered {
            self.clone().spawn_engine_run(run).await;
        }
    }

    /// Periodically resumes queued runs that lost their engine task (lease
    /// contention, crash between registration and spawn). Only runs queued
    /// for longer than the grace period are retried, so freshly registered
    /// runs are never double-spawned. Runs in both store modes.
    pub fn start_queued_resumer(
        &self,
        shutdown: CancellationToken,
    ) -> JoinHandle<Result<(), ServiceError>> {
        let service = self.clone();
        let resumer_secs = self.inner.deployment_policy.queued_resumer_secs.max(1);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(resumer_secs));
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = interval.tick() => {}
                }
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
                        && !crate::types::task_slot_is_live(&record.task)
                        // Stale queue entries re-enter admission; accepted
                        // cancellations without a task converge through the
                        // common finalizer on the next spawn (B10). A
                        // Running record with no live task is a spawn that
                        // never reached the engine (lease lost, shutdown
                        // refusal): it re-enters here so execution is
                        // retried instead of parking forever. (Journal
                        // failures no longer linger as Running: the spawn
                        // marks the record failed-closed with an explicit
                        // error (E03).)
                        && ((record.state == RunStatus::Queued
                            && record.queued_at.is_none_or(|at| at <= cutoff))
                            || record.state == RunStatus::CancelRequested
                            || record.state == RunStatus::Running)
                })
                .map(|(run_id, record)| SpawnRun {
                    run_id: run_id.clone(),
                    contract: record.contract.clone(),
                    inputs: record.inputs.clone(),
                    run_dir: record.run_dir.clone(),
                    events: record.events.clone(),
                    answers: record.answers.clone(),
                    confirmations: record.confirmations.clone(),
                    // The resumer has no admission snapshot: the spawn reads
                    // the journal once (E03).
                    journal_snapshot: None,
                    cancellation: record.cancellation.clone(),
                    task: Arc::clone(&record.task),
                })
                .collect::<Vec<_>>()
        };
        for run in stuck {
            tracing::info!(run_id = %run.run_id, "resuming queued run without an engine task");
            self.clone().spawn_engine_run(run).await;
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

    /// Retention sweep. Lease contention (another process owns the sweep)
    /// is a normal skip. Every other lease or sweep failure propagates as
    /// Err so the GC task retries boundedly and then ends visibly instead
    /// of looping silently as Ok (E05).
    async fn collect_retained_runs(&self) -> Result<(), ServiceError> {
        let maintenance_lock = match try_lock_store_maintenance(&self.inner.runs_dir)? {
            Some(lock) => lock,
            None => return Ok(()),
        };
        gc_run_directories(
            &self.inner.runs_dir,
            self.inner.deployment_policy.gc_keep,
            self.inner.deployment_policy.gc_keep_failed,
            true,
            self.inner.deployment_policy.max_directory_scan_entries,
        )?;
        self.inner
            .runs
            .write()
            .await
            .retain(|_, record| !record.state.is_terminal() || record.run_dir.is_dir());
        drop(maintenance_lock);
        Ok(())
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

    pub async fn list_run_items(&self) -> Result<Vec<RunListItem>, ApiError> {
        // Single directory scan lives in `list_run_summaries`, which folds
        // each run exactly once and returns its `last_seq` alongside: this
        // layer only projects into list items so scan, filter, fold, and
        // sort never drift apart and no second fold per run exists.
        let summaries = crate::summaries::list_run_summaries(
            &self.inner.runs_dir,
            self.inner.deployment_policy.max_directory_scan_entries,
        )
        .map_err(api_internal)?;
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

    /// In-flight preemption fact for observability only: counts in-memory
    /// runs currently marked preempted. No threshold or alert lives here;
    /// policy decisions stay outside. Async so the read takes the lock
    /// instead of reporting 0 under contention.
    pub async fn preempted_inflight(&self) -> usize {
        self.inner
            .runs
            .read()
            .await
            .values()
            .filter(|record| record.preempted)
            .count()
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
                let mut contract = Contract::load(path).map_err(api_internal)?;
                contract.apply_audit_floor(self.inner.deployment_policy.audit_floor);
                return Ok(contract);
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

    /// Takes an owned service handle and returns a boxed future. The box
    /// is structural, not stylistic: the spawn is re-entered from the
    /// engine task it creates (task → finish → requeue → spawn), so an
    /// `async fn` opaque return would recurse through its own type and
    /// the compiler rejects the cycle (rustc E0391, unrelated to audit
    /// item E03). Callers `.await` it exactly like an `async fn`.
    pub(crate) fn spawn_engine_run(
        self,
        request: SpawnRun,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async move {
            // A spawn admitted just before the shutdown snapshot must not start
            // new execution: the record stays queued and resumes on the next
            // boot, and shutdown_active_runs settles it as interrupted (E05).
            if self.is_shutting_down() {
                tracing::warn!(
                    run_id = %request.run_id,
                    "service is shutting down; refusing to spawn engine execution"
                );
                return;
            }
            // An answer accepted while its engine is suspending (or any two
            // racing spawns) must still find a driver: wait briefly for the
            // lease instead of dropping the spawn at the first contention.
            // The shared task slot stays untouched throughout, so a concurrent
            // winner's live handle is never orphaned and no second execution
            // can start; an exiting holder frees the lease within milliseconds,
            // while a genuinely busy holder falls through to the queued verdict
            // below and the resumer backstop (E03). A single acquisition
            // attempt plus one bounded 2s wait: no retry loop is needed
            // because the resumer re-evaluates promptly via queue_notify.
            let execution_lock = match try_lock_run_execution(&request.run_dir) {
                Ok(Some(lock)) => lock,
                Ok(None) => {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
                    let acquired = loop {
                        if request.cancellation.is_cancelled() || self.is_shutting_down() {
                            return;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        if request.cancellation.is_cancelled() || self.is_shutting_down() {
                            return;
                        }
                        match try_lock_run_execution(&request.run_dir) {
                            Ok(Some(lock)) => break Some(lock),
                            Ok(None) if std::time::Instant::now() >= deadline => break None,
                            Ok(None) => continue,
                            Err(error) => {
                                tracing::error!(%error, run_id = %request.run_id, "failed to acquire run execution lease");
                                self.inner.queue_notify.notify_waiters();
                                return;
                            }
                        }
                    };
                    match acquired {
                        Some(lock) => lock,
                        None => {
                            tracing::warn!(
                                run_id = %request.run_id,
                                run_dir = %request.run_dir,
                                "run execution lease is held elsewhere; run stays queued until the lease is released"
                            );
                            // Settlement belongs to the lease holder;
                            // wake queue waiters so the resumer backstop
                            // re-evaluates promptly instead of waiting a
                            // full interval (E03).
                            self.inner.queue_notify.notify_waiters();
                            return;
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(%error, run_id = %request.run_id, "failed to acquire run execution lease");
                    self.inner.queue_notify.notify_waiters();
                    return;
                }
            };
            // Publish this process as the execution owner in the lease file
            // itself so shared-store peers observe the hand-off in their
            // refresh merge (E12). Advisory only: the flock above is the
            // authority, so a claim-write failure warns and execution still
            // proceeds under the held lease.
            if let Err(error) = claim_run_execution_owner(&execution_lock, &self.inner.owner_id) {
                tracing::warn!(run_id = %request.run_id, %error, "execution owner claim failed; lease authority is unaffected");
            }
            let service = self.clone();
            let SpawnRun {
                run_id,
                contract,
                inputs,
                run_dir,
                events,
                answers,
                confirmations,
                journal_snapshot,
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
                        .settle_queued_cancel(
                            &run_id,
                            &run_dir,
                            "cancel drain failed before execution",
                        )
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
                    let queued_cancel = match crate::summaries::has_remote_cancel_request(&run_dir)
                    {
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
                    // The admission snapshot is reused when the spawn carries
                    // one (adopted retries, fresh forks): the pre-execution
                    // fold then costs no second journal read or re-fold of
                    // the same events (E03).
                    let pre_state: Result<qcg_engine::RunState, crate::types::ServiceError> =
                        match &journal_snapshot {
                            Some(values) => {
                                qcg_engine::RunState::fold_values(values).map_err(|error| {
                                    crate::types::ServiceError::Invalid(error.to_string())
                                })
                            }
                            None => crate::summaries::fold_run_state(&run_dir),
                        };
                    match pre_state {
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
                            // Fail closed with an explicit error instead of
                            // leaving the run Queued for the 5s/10s resumer
                            // timers to retry blindly (E03). Memory reports
                            // failure; the journal stays the truth, so a
                            // restart re-derives from durable events and a
                            // transient I/O failure heals instead of
                            // lingering silently.
                            tracing::error!(%error, run_id = %run_id, "pre-execution state fold failed; marking the run failed instead of lingering queued");
                            record.state = RunStatus::Failed;
                            record.preempted = false;
                            record.question = None;
                            record.confirm = None;
                            drop(runs);
                            service.inner.queue_notify.notify_waiters();
                            return;
                        }
                    }
                    record.state = RunStatus::Running;
                    // Claim shared-store ownership while holding the execution
                    // lease so peers observe the active owner.
                    record.owner_id = service.inner.owner_id.clone();
                }
                // Prefer the journaled admission instant over re-resolving:
                // a ceiling change between admission and execution must not
                // diverge the two (E04). An unreadable journal fails closed:
                // defaulting to an empty event list would silently run under
                // the current ceiling instead of the admitted one (E04).
                // The admission snapshot is reused when the spawn carries
                // one, so adopted retries and fresh forks pay no second
                // journal read here (E03).
                let journaled: Vec<Value> = match journal_snapshot {
                    Some(values) => values,
                    None => match crate::summaries::read_journal_events(&run_dir) {
                        Ok(events) => events,
                        Err(error) => {
                            // Fail closed with an explicit error instead of
                            // leaving a Running memory record with no driver
                            // for the resumer timers (E03): mark the run
                            // failed in memory, wake the queue, and return.
                            // The journal stays the truth on restart.
                            tracing::error!(%error, run_id = %run_id, "admission journal unreadable; marking the run failed instead of starting execution");
                            let mut runs = service.inner.runs.write().await;
                            if let Some(record) = runs.get_mut(&run_id)
                                && !record.state.is_terminal()
                            {
                                record.state = RunStatus::Failed;
                                record.preempted = false;
                                record.question = None;
                                record.confirm = None;
                            }
                            drop(runs);
                            service.inner.queue_notify.notify_waiters();
                            return;
                        }
                    },
                };
                let policy = match crate::types::ResolvedExecutionPolicy::for_execution(
                    &journaled,
                    self.inner.deployment_policy.max_parallel_steps,
                ) {
                    Ok(policy) => policy,
                    Err(error) => {
                        // Fail closed with an explicit error: the journal
                        // lacks the admitted ceiling, so running under a
                        // re-resolved one would silently diverge admission
                        // from execution (E04).
                        tracing::error!(error = %error, run_id = %run_id, "admission policy missing from journal; marking the run failed instead of re-resolving");
                        let mut runs = service.inner.runs.write().await;
                        if let Some(record) = runs.get_mut(&run_id)
                            && !record.state.is_terminal()
                        {
                            record.state = RunStatus::Failed;
                            record.preempted = false;
                            record.question = None;
                            record.confirm = None;
                        }
                        drop(runs);
                        service.inner.queue_notify.notify_waiters();
                        return;
                    }
                };
                let engine = Engine::new(app_registry(Arc::clone(&runtime))).with_snapshot_source(
                    Arc::new(ServiceSnapshotSource {
                        service: service.clone(),
                    }),
                );
                let run_refs = match crate::run_refs::load_or_resolve_run_refs(
                    &service.inner.runs_dir,
                    &contract,
                    &run_meta_dir(&run_dir),
                ) {
                    Ok(run_refs) => run_refs,
                    Err(error) => {
                        tracing::error!(%error, run_id = %run_id, "run reference resolution failed; marking the run failed instead of starting execution");
                        // The failure precedes the engine, so journal the
                        // terminal record here; a silent pre-engine failure
                        // would leave the run with no durable cause.
                        if let Err(journal_error) = qcg_engine::JournalWriter::append_single_event(
                            &run_meta_dir(&run_dir).join("journal.jsonl"),
                            &run_id,
                            "run_error",
                            serde_json::json!({
                                "error": format!("run reference resolution failed: {error}"),
                            }),
                            qcg_engine::JournalLimits::default(),
                            None,
                        ) {
                            tracing::error!(%journal_error, run_id = %run_id, "failed to journal the run reference failure");
                        }
                        let mut runs = service.inner.runs.write().await;
                        if let Some(record) = runs.get_mut(&run_id)
                            && !record.state.is_terminal()
                        {
                            record.state = RunStatus::Failed;
                            record.preempted = false;
                            record.question = None;
                            record.confirm = None;
                        }
                        drop(runs);
                        service.inner.queue_notify.notify_waiters();
                        return;
                    }
                };
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
                            run_refs,
                            shutdown: Some(service.shutdown_token()),
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
            // Compare-and-install under the slot lock (E03): this spawn
            // holds the execution lease, so no rival spawn can be live for
            // the same run — but recheck slot liveness immediately before
            // install anyway. A live slot means a winner already installed
            // (lease handoff race or resumer double-spawn); the loser must
            // abort its own task and return without touching the winner's
            // handle, never overwriting it.
            {
                let mut slot = task.lock().unwrap_or_else(PoisonError::into_inner);
                let live = slot.as_ref().is_some_and(|handle| !handle.is_finished());
                if live {
                    drop(slot);
                    handle.abort();
                    return;
                }
                *slot = Some(handle);
            }
        })
    }

    /// Removes a direct-run ephemeral record synchronously (E05): the
    /// single shared removal used by both the normal path and the drop
    /// fallback below, so shutdown races cannot leave divergent cleanup.
    async fn remove_ephemeral_now(&self, run_id: &str) {
        self.inner.runs.write().await.remove(run_id);
        self.inner.queue_notify.notify_waiters();
    }

    pub async fn run_generator_path(&self, run: DirectRun) -> Result<OutputManifest, ApiError> {
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
        let mut contract = Contract::load(&run.generator_path).map_err(api_internal)?;
        contract.apply_audit_floor(self.inner.deployment_policy.audit_floor);
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
        let run_refs = crate::run_refs::load_or_resolve_run_refs(
            &self.inner.runs_dir,
            &contract,
            &metadata_dir,
        )?;
        warn_if_shared_runs_dir_owned(&run.output_dir);
        // Unified scheduler: direct executions register an ephemeral Queued
        // record so the same queue_head ordering governs API and direct runs.
        // Priority 0, FIFO by admission; removed on completion.
        let (events, _) =
            broadcast::channel(self.inner.deployment_policy.live_event_channel_capacity);
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
        let _ephemeral_guard = RemoveEphemeralOnDrop {
            service: self.clone(),
            run_id: run_id.clone(),
        };
        let _permit = loop {
            let notified = self.inner.queue_notify.notified();
            let shutdown = self.inner.shutdown.cancelled();
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
            // Shutdown ends the wait instead of hanging the direct caller
            // past the drain (E05).
            tokio::select! {
                _ = shutdown => {
                    return Err(ApiError::Unavailable {
                        detail: "server is shutting down".into(),
                    });
                }
                _ = notified => continue,
            }
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
            self.inner.deployment_policy.max_parallel_steps,
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
                    run_refs: run_refs.clone(),
                    cancellation: CancellationToken::new(),
                    // Direct executions observe the service drain like
                    // spawned runs: a shutdown mid-run ends the wait
                    // instead of overrunning it (E05).
                    shutdown: Some(self.shutdown_token()),
                },
            )
            .await
            .map_err(api_internal);
        self.remove_ephemeral_now(&run_id).await;
        std::mem::forget(_ephemeral_guard);
        result
    }

    pub async fn run_generator_path_with_events(
        &self,
        run: DirectRun,
    ) -> Result<DirectRunEvents, ApiError> {
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
        let mut contract = Contract::load(&run.generator_path).map_err(api_internal)?;
        contract.apply_audit_floor(self.inner.deployment_policy.audit_floor);
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
        let (events, mut receiver) =
            broadcast::channel(self.inner.deployment_policy.live_event_channel_capacity);
        let run_id = direct_run_id(&run.output_dir);
        let metadata_dir = direct_run_meta_dir(&run.output_dir);
        let _run_lock = lock_direct_run(&metadata_dir).map_err(api_internal)?;
        let run_refs = crate::run_refs::load_or_resolve_run_refs(
            &self.inner.runs_dir,
            &contract,
            &metadata_dir,
        )?;
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
        let _ephemeral_guard = RemoveEphemeralOnDrop {
            service: self.clone(),
            run_id: run_id.clone(),
        };
        let _permit = loop {
            let notified = self.inner.queue_notify.notified();
            let shutdown = self.inner.shutdown.cancelled();
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
            // Shutdown ends the wait instead of hanging the direct caller
            // past the drain (E05).
            tokio::select! {
                _ = shutdown => {
                    return Err(ApiError::Unavailable {
                        detail: "server is shutting down".into(),
                    });
                }
                _ = notified => continue,
            }
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
            self.inner.deployment_policy.max_parallel_steps,
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
                    run_refs: run_refs.clone(),
                    cancellation: CancellationToken::new(),
                    // Direct executions observe the service drain like
                    // spawned runs: a shutdown mid-run ends the wait
                    // instead of overrunning it (E05).
                    shutdown: Some(self.shutdown_token()),
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
                    // Single lag rule shared with the subscribe path: never
                    // fabricate a cursor from the dropped count (E12a).
                    let last_seq = collected.last().map_or(0, |event| event.seq);
                    collected.push(RunEvent::lagged(
                        run_id.clone(),
                        crate::runs_api::lagged_resync_seq(last_seq, skipped),
                    ));
                }
                Err(broadcast::error::TryRecvError::Closed) => break,
            }
        }
        let journal_events = read_merged_events_from_meta(&metadata_dir)
            .map_err(api_internal)?
            .into_iter()
            .map(|event| RunEvent::from_flat(&event))
            .collect::<Result<Vec<_>, _>>()
            .map_err(api_internal)?;
        if collected.is_empty() {
            collected = journal_events;
        } else if collected.iter().any(|event| event.kind == "lagged") {
            // A lagged live tail dropped broadcasts; the durable journal is
            // authoritative, so converge onto it instead of erroring (E12).
            collected = journal_events;
        } else if event_kinds(&collected) != event_kinds(&journal_events) {
            self.remove_ephemeral_now(&run_id).await;
            std::mem::forget(_ephemeral_guard);
            return Err(api_internal(format!(
                "direct run event stream diverged from journal in `{}`",
                run.output_dir
            )));
        }
        self.remove_ephemeral_now(&run_id).await;
        std::mem::forget(_ephemeral_guard);
        Ok(DirectRunEvents {
            manifest,
            events: collected,
        })
    }

    /// Re-drives a suspending run whose prompt was already answered (or
    /// decided) in the journal. Returns true when it requeued, meaning the
    /// caller must not publish the suspension. An answer that lands between
    /// the engine's initial fold and its suspension would otherwise park
    /// the run on a satisfied prompt with no driver (E03).
    async fn suspend_lost_to_an_answer(
        &self,
        run_id: &str,
        run_dir: &Utf8Path,
        question: &qcg_api::FormSpec,
    ) -> bool {
        // An unreadable journal fails closed toward suspension: without
        // evidence of an answer the run parks as waiting instead of
        // requeueing on a guess (E03).
        let answered = match crate::summaries::fold_run_state(run_dir) {
            Ok(state) => state.answers.contains_key(&question.id),
            Err(error) => {
                tracing::warn!(run_id = %run_id, %error, "suspend race check failed; keeping suspension");
                return false;
            }
        };
        if !answered {
            return false;
        }
        self.requeue_answered_suspension(run_id, run_dir).await
    }

    /// Confirmation half of [`Self::suspend_lost_to_an_answer`] (E03).
    async fn suspend_lost_to_a_decision(
        &self,
        run_id: &str,
        run_dir: &Utf8Path,
        confirm: &qcg_api::ConfirmSpec,
    ) -> bool {
        let decided = match crate::summaries::fold_run_state(run_dir) {
            Ok(state) => state.confirmations.contains_key(&confirm.id),
            Err(error) => {
                tracing::warn!(run_id = %run_id, %error, "suspend race check failed; keeping suspension");
                return false;
            }
        };
        if !decided {
            return false;
        }
        self.requeue_answered_suspension(run_id, run_dir).await
    }

    /// Merges durable HITL maps into memory, requeues the run, and spawns
    /// a fresh engine pass that consumes the early answer. No suspension
    /// event is journaled: the prompt was never published as waiting.
    /// Takes an owned service clone: this runs inside spawned engine
    /// tasks, so borrowing `&self` across the awaits would demand `Sync`
    /// instead of just `Send` (E03). Returns whether the requeue happened;
    /// a failure leaves the suspension path to the caller instead of
    /// reporting a parked run as requeued (E03).
    async fn requeue_answered_suspension(&self, run_id: &str, run_dir: &Utf8Path) -> bool {
        let service = self.clone();
        if service.is_shutting_down() {
            return false;
        }
        let (answers, confirmations) = match crate::summaries::read_persisted_hitl(run_dir) {
            Ok(maps) => maps,
            Err(error) => {
                tracing::error!(run_id = %run_id, %error, "early answer requeue failed to read HITL maps");
                return false;
            }
        };
        let mut runs = self.inner.runs.write().await;
        let Some(record) = runs.get_mut(run_id) else {
            return false;
        };
        // Never resurrect a settled run and never steal an accepted
        // cancel: only live non-terminal records may requeue (E03).
        // (`CancelRequested` is accepted-not-settled, hence not covered by
        // `is_terminal` and checked explicitly.)
        if record.state.is_terminal() || record.state == RunStatus::CancelRequested {
            return false;
        }
        record.answers = answers;
        record.confirmations = confirmations;
        record.state = RunStatus::Queued;
        // Derive the queue instant from the durable answer/confirmation
        // timestamp, not a fresh local clock read, so memory order matches
        // what restarts and peers derive from the same journal (E03).
        record.queued_at = crate::summaries::read_last_queued_at(&record.run_dir)
            .or_else(|| Some(chrono::Utc::now()));
        record.question = None;
        record.confirm = None;
        record.artifacts = None;
        let cancellation = CancellationToken::new();
        record.cancellation = cancellation.clone();
        let request = SpawnRun {
            run_id: run_id.to_string(),
            contract: record.contract.clone(),
            inputs: record.inputs.clone(),
            run_dir: record.run_dir.clone(),
            events: record.events.clone(),
            answers: record.answers.clone(),
            confirmations: record.confirmations.clone(),
            // Requeue re-drives from durable answers: no admission snapshot
            // exists, so the spawn reads the journal once (E03).
            journal_snapshot: None,
            cancellation,
            task: record.task.clone(),
        };
        drop(runs);
        // Detached, never awaited: this runs inside the finishing engine
        // task that itself holds the execution lease, so awaiting the
        // spawn's bounded lease wait would self-deadlock until it gives
        // up. The detached spawn waits out the holder's exit, then drives
        // (E03). Queue waiters are notified for the resumer backstop
        // either way.
        tokio::spawn(service.clone().spawn_engine_run(request));
        service.inner.queue_notify.notify_waiters();
        true
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
                    // An answer may have landed (early accept) between the
                    // engine's initial fold and this suspension: suspending
                    // anyway would park the run on an already-answered
                    // prompt. Requeue instead so the answer is consumed on
                    // the next pass (E03).
                    if self
                        .suspend_lost_to_an_answer(&run_id, &record.run_dir, question)
                        .await
                    {
                        return;
                    }
                    transition.state = Some(RunStatus::Waiting);
                    transition.question = Some(question.clone());
                    transition.writes.push((
                        "run_waiting",
                        json!({ "question_id": question.id, "question": question }),
                    ));
                }
                Progress::Suspended(Interaction::Confirmation { confirm }) => {
                    // Same race as questions, for confirmations (E03).
                    if self
                        .suspend_lost_to_a_decision(&run_id, &record.run_dir, confirm)
                        .await
                    {
                        return;
                    }
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
                    transition.writes.push((
                        "run_error",
                        json!({
                            "error": error.message,
                            "reason": FailureDetail::new(error.code, error.message.clone()),
                        }),
                    ));
                }
                Progress::Advanced => {}
            }
            for (kind, payload) in &transition.writes {
                if let Err(error) = write_run_event(&record, kind, payload.clone()) {
                    // Journal failure with a finished engine: memory must NOT
                    // advance ahead of the journal. Reporting a terminal
                    // outcome that is not durable would let a restart
                    // re-execute after clients observed success (E03).
                    // The run stays in its pre-finish state so the resumer
                    // and restart replay retry settlement.
                    tracing::error!(%error, %run_id, "failed to record run progress event; memory stays unadvanced for retry");
                    {
                        let runs = self.inner.runs.read().await;
                        if runs.get(&run_id).is_some() {
                            self.inner.queue_notify.notify_waiters();
                        }
                    }
                    return;
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
                    // Shutdown settlement wins the race against a concurrent
                    // cancel: when the service shutdown token is already
                    // cancelled, settle as Interrupted (not Canceled) so
                    // shutdown_active_runs converges live runs to Interrupted
                    // with a journaled run_interrupted event (E05).
                    // Non-shutdown cancels keep settling as Canceled.
                    if self.is_shutting_down() {
                        transition.state = Some(RunStatus::Interrupted);
                        transition.writes.push((
                            "run_interrupted",
                            json!({
                                "reason": FailureDetail::new(
                                    FailureCode::Interrupted,
                                    "service shutdown",
                                ),
                            }),
                        ));
                    } else {
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
        }
        for (kind, payload) in &transition.writes {
            if let Err(error) = write_run_event(record, kind, payload.clone()) {
                // Journal failure must not advance memory ahead of the
                // journal (same rule as the progress path above): a
                // reported terminal state that is not durable would let a
                // restart re-execute after clients observed settlement
                // (E05). Stay in the pre-finish state for retry.
                tracing::error!(
                    %error,
                    %run_id,
                    "failed to record terminal cancellation event; memory stays unadvanced for retry"
                );
                self.inner.queue_notify.notify_waiters();
                return;
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
    // Note: no `priority` field. Admission order comes from the registered
    // record's priority via `queue_head`; duplicating it here invited drift
    // between the spawn request and the registered record (E04).
    /// Journal snapshot accepted at admission, reused for the pre-execution
    /// terminal check and policy resolution so the spawn never re-reads or
    /// re-folds what admission already observed. `Some` on snapshot-carrying
    /// admissions (adopted retries, fresh forks); `None` on paths without
    /// one (resumes, answers, confirms, preemptions, direct runs), where the
    /// spawn reads the journal once (E03).
    pub(crate) journal_snapshot: Option<Vec<Value>>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) task: Arc<Mutex<Option<JoinHandle<()>>>>,
}
