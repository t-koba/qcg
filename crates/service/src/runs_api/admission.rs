//! Admission operations.
use super::*;

fn verify_fork_source_identity(state: &engine::RunState, source_id: &str) -> Result<(), ApiError> {
    match state.run_id.as_deref() {
        Some(run_id) if !run_id.is_empty() && run_id == source_id => Ok(()),
        Some(run_id) => Err(api_internal(format!(
            "fork source `{source_id}` journal identifies run `{run_id}`; refusing checkpoint copy"
        ))),
        None => Err(api_internal(format!(
            "fork source `{source_id}` journal has no durable run identity; refusing checkpoint copy"
        ))),
    }
}

/// Refused-admission shutdown with ownership cleanup: only this admission's own
/// directory is removed, adopted ones survive. Cleanup failure is Internal (E03/E04).
fn refused_shutdown(own_dir: Option<&Utf8Path>) -> Result<ApiError, ApiError> {
    if let Some(dir) = own_dir {
        std::fs::remove_dir_all(dir).map_err(api_internal)?;
    }
    Ok(ApiError::Unavailable {
        detail: "server is shutting down".into(),
    })
}

/// Capacity refusal with the same ownership rule; message identical across paths (E03).
fn refused_capacity(
    own_dir: Option<&Utf8Path>,
    max_tracked_runs: usize,
) -> Result<ApiError, ApiError> {
    if let Some(dir) = own_dir {
        std::fs::remove_dir_all(dir).map_err(api_internal)?;
    }
    Ok(ApiError::Unavailable {
        detail: format!(
            "run capacity is exhausted: {max_tracked_runs} non-terminal runs are already tracked"
        ),
    })
}

fn fold_prepared_fork_snapshot(events: &[Value]) -> Result<engine::RunState, engine::JournalError> {
    let reset_prefix = events.last().is_some_and(|event| {
        event["t"] == "child_reset_overlay"
            && events
                .iter()
                .rev()
                .find(|candidate| candidate["t"] == "run_forked")
                .is_some_and(crate::fork_commit::reset_bound)
    });
    if reset_prefix {
        engine::RunState::fold_admission_values(events)
    } else {
        engine::RunState::fold_values(events)
    }
}

impl LocalService {
    /// Complete start/fork admission after the allocation-free probe (E03).
    /// The channel predates the journal append. Only the registered winner
    /// spawns, sharing exactly the registered task slot and cancellation token.
    async fn finish_prepared_admission(
        &self,
        run_id: String,
        staged: RunRecord,
        snapshot: crate::run_dirs::AdoptSnapshot,
        created_fresh: bool,
        queued_payload: Option<Value>,
    ) -> Result<String, ApiError> {
        let adopted = snapshot.adopted;
        let is_fork = staged.parent_run_id.is_some();
        // Adopted retries supply no payload: their run_queued is already
        // durable. Only fresh forks retain a copy for the spawn snapshot.
        let fork_payload = if is_fork {
            queued_payload.clone()
        } else {
            None
        };
        let checkpoint_fork = (is_fork && fork_payload.is_some())
            .then(|| {
                snapshot
                    .events
                    .iter()
                    .rev()
                    .find(|event| event["t"] == "run_forked")
            })
            .flatten();
        if let Some(fork) = checkpoint_fork
            && crate::fork_commit::reset_bound(fork)
            && fork["fork_tree_commit_version"].as_u64() != Some(crate::fork_commit::VERSION)
        {
            return Err(api_internal(
                "reset-bound fork requires a supported fork tree commit protocol",
            ));
        }
        // Fence only the durable membership append, not preparation or
        // idempotent/rejected admissions that never write a new identity.
        let mut membership = (self.inner.run_store_mode == RunStoreMode::Exclusive)
            .then(|| self.inner.read_store.membership_write());
        if let Some(payload) = queued_payload
            && let Err(error) = write_run_event(&staged, "run_queued", payload)
        {
            // Remove only this admission's own directory, never an adopted or
            // retained incomplete fork directory (or its cancel mailbox).
            if created_fresh && let Err(cleanup_error) = std::fs::remove_dir_all(&staged.run_dir) {
                let directory = if is_fork { "fork" } else { "fresh run" };
                return Err(api_internal(format!(
                    "{error}; failed to clean {directory} directory: {cleanup_error}"
                )));
            }
            return Err(api_internal(error));
        }
        if is_fork && fork_payload.is_some() {
            // Reuse forks are fresh starts, not checkpoint-tree publications.
            if let Some(fork) = checkpoint_fork
                && fork["fork_tree_commit_version"].as_u64() == Some(crate::fork_commit::VERSION)
            {
                let reset_bound = crate::fork_commit::reset_bound(fork);
                let queue_seq = snapshot
                    .events
                    .iter()
                    .filter_map(|event| event["seq"].as_u64())
                    .max()
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| api_internal("fork queue sequence overflow"))?;
                let directory = staged.run_dir.clone();
                let identity = run_id.clone();
                if let Err(error) = self
                    .blocking_work(move || {
                        crate::fork_commit::publish(&directory, &identity, queue_seq, reset_bound)
                    })
                    .await
                {
                    // Do not let a queued but uncommitted child be resumed
                    // after this admission reported failure.
                    Self::settle_refused_admission(&staged.run_dir);
                    return Err(api_internal(error));
                }
            }
            if let Err(error) = crate::run_dirs::clear_fork_admission_pending(&staged.run_dir) {
                // A committed queue without a cleared marker must be settled
                // as refused before returning, preserving the prior admission
                // contract if the marker removal itself was not durable.
                Self::settle_refused_admission(&staged.run_dir);
                return Err(api_internal(format!(
                    "fork queue was committed but its visibility marker could not be cleared: {error}"
                )));
            }
        }
        if let Some(membership) = &mut membership {
            membership.admitted(&staged.run_dir);
        }
        // Registration clones the staged values for the map; the originals
        // below are handed to the engine with the same shared channel/slot.
        // The losing admission discards its channel/token/task: substituting
        // them for the winner's would orphan the registered engine (E03).
        match self
            .register_admission(
                run_id.clone(),
                staged.clone(),
                created_fresh.then_some(staged.run_dir.as_path()),
            )
            .await
        {
            Ok(AdmissionVerdict::Converged) => {
                // A rival may have registered after an empty adoption read;
                // verification reuses the snapshot or falls back once (E03).
                let identity = AdoptedRunIdentity {
                    inputs: &staged.inputs,
                    contract_sha256: &staged.contract_sha256,
                    package_sha256: staged.package_sha256.as_deref(),
                    priority: staged.priority,
                    parent: staged.parent_run_id.as_deref(),
                    answers: &staged.answers,
                    confirmations: &staged.confirmations,
                };
                verify_live_admission_from_snapshot(&snapshot, &identity, &staged.run_dir)?;
                return Ok(run_id);
            }
            Ok(AdmissionVerdict::Inserted) => {}
            Err(error @ ApiError::Unavailable { .. }) => {
                // A fresh request reported refused must not run on next boot
                // just because its run_queued append preceded shutdown (E05).
                if !adopted {
                    Self::settle_refused_admission(&staged.run_dir);
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        }
        // Registration hands off without holding the run-map lock across
        // spawn's execution-lease wait. A racing resumer shares the registered
        // task slot; the execution-lease loser leaves it untouched (E03).
        let journal_snapshot = if adopted {
            Some(snapshot.events)
        } else if let Some(mut payload) = fork_payload {
            // Fresh forks append their own admission to the copied checkpoint
            // in memory: the fork-aware fold sees the child's ceiling without
            // a second journal read. Reusing the durable payload keeps every
            // admission field identical; only the event envelope is added.
            let mut events = snapshot.events;
            let next_seq = events
                .iter()
                .filter_map(|event| event.get("seq").and_then(Value::as_u64))
                .max()
                .unwrap_or(0)
                .saturating_add(1);
            payload["t"] = json!("run_queued");
            payload["ts"] = json!(chrono::Utc::now().to_rfc3339());
            payload["seq"] = json!(next_seq);
            payload["trace_id"] = json!(api::trace_id_for_run(&run_id));
            payload["span_id"] = json!(api::span_id_for_seq(next_seq));
            events.push(payload);
            Some(events)
        } else {
            // Fresh starts read their just-written journal in spawn (E03).
            None
        };
        self.clone()
            .spawn_engine_run(SpawnRun {
                run_id: run_id.clone(),
                contract: staged.contract,
                inputs: staged.inputs,
                run_dir: staged.run_dir,
                events: staged.events,
                answers: staged.answers,
                confirmations: staged.confirmations,
                journal_snapshot,
                cancellation: staged.cancellation,
                task: staged.task,
            })
            .await;
        self.inner.queue_notify.notify_waiters();
        self.preempt_for_priority(staged.priority).await;
        Ok(run_id)
    }

    /// Fails write-path API calls during shutdown (E05).
    pub(crate) fn ensure_running(&self) -> Result<(), ApiError> {
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
        Ok(())
    }

    /// Phase 2a: a losing admission allocates nothing. `Some(run_id)` means the
    /// retry converged onto an already registered record, verified against the
    /// admission snapshot (a rival registering after the snapshot read leaves
    /// it empty and falls back to one fresh read) (E03).
    async fn admission_gate(
        &self,
        run_id: &str,
        run_dir: &Utf8Path,
        created_fresh: bool,
        snapshot: &crate::run_dirs::AdoptSnapshot,
        identity: &AdoptedRunIdentity<'_>,
    ) -> Result<Option<String>, ApiError> {
        let own_dir = created_fresh.then_some(run_dir);
        match self.probe_admission(run_id).await {
            Ok(AdmissionProbe::Proceed) => Ok(None),
            Ok(AdmissionProbe::Converged) => {
                verify_live_admission_from_snapshot(snapshot, identity, run_dir)?;
                Ok(Some(run_id.to_owned()))
            }
            Err(ProbeRejection::Shutdown) => Err(refused_shutdown(own_dir)?),
            Err(ProbeRejection::Capacity) => {
                Err(refused_capacity(own_dir, self.inner.max_tracked_runs)?)
            }
        }
    }

    /// Phase-2a admission probe shared by start and fork (E03). Decides
    /// shutdown, duplicate, and capacity under the run-map lock before the
    /// caller allocates a broadcast channel, a cancellation token, a task
    /// slot, or builds a record, so rejected admissions allocate nothing.
    pub(super) async fn probe_admission(
        &self,
        run_id: &str,
    ) -> Result<AdmissionProbe, ProbeRejection> {
        let mut runs = self.inner.runs.write().await;
        // A shutdown that started before registration must not admit a
        // record whose spawn the shutdown guard will refuse (E05).
        if self.is_shutting_down() {
            return Err(ProbeRejection::Shutdown);
        }
        if runs.contains_key(run_id) {
            return Ok(AdmissionProbe::Converged);
        }
        if runs.len() >= self.inner.max_tracked_runs {
            runs.retain(|_, record| !record.state.is_terminal());
        }
        if runs.len() >= self.inner.max_tracked_runs {
            return Err(ProbeRejection::Capacity);
        }
        Ok(AdmissionProbe::Proceed)
    }

    /// Phase-2b registration shared by start and fork (E03). Re-checks
    /// shutdown, duplicate, and capacity under the lock and inserts; a
    /// rival that won between the probe and here converges instead of
    /// replacing the winner's record and orphaning its engine. A terminal
    /// record converges without resurrection. On capacity exhaustion
    /// `own_dir` (present only when this admission created the directory)
    /// is removed; adopted or pre-existing directories always survive.
    /// On shutdown refusal the directory stays Queued on disk and resumes
    /// on the next boot.
    pub(super) async fn register_admission(
        &self,
        run_id: String,
        staged: RunRecord,
        own_dir: Option<&Utf8Path>,
    ) -> Result<AdmissionVerdict, ApiError> {
        let mut runs = self.inner.runs.write().await;
        // Re-check under the admission lock: a shutdown that started
        // between the probe and here must not register a record whose
        // spawn the shutdown guard will refuse (E05).
        self.ensure_running()?;
        if runs.contains_key(&run_id) {
            return Ok(AdmissionVerdict::Converged);
        }
        if runs.len() >= self.inner.max_tracked_runs {
            runs.retain(|_, record| !record.state.is_terminal());
        }
        if runs.len() >= self.inner.max_tracked_runs {
            drop(runs);
            return Err(refused_capacity(own_dir, self.inner.max_tracked_runs)?);
        }
        runs.insert(run_id, staged);
        Ok(AdmissionVerdict::Inserted)
    }

    /// H01: cancellable admission-shard acquisition shared by start and fork.
    /// Shard contention is physical, not a same-run conflict: distinct ids
    /// sharing one of the 64 shards wait cancellably for the shard, and the
    /// semantic duplicate is decided after acquisition via adoption/identity
    /// checks. Shutdown during the wait refuses without starting work, and
    /// the live-hit path is re-checked under the lock so a racing drain is
    /// never missed (H01-03/E05).
    pub(super) async fn acquire_admission_shard(
        &self,
        run_dir: &Utf8Path,
    ) -> Result<crate::run_dirs::RunAdmissionLock, ApiError> {
        match crate::run_dirs::lock_run_admission(run_dir, &self.inner.shutdown)
            .await
            .map_err(api_internal)?
        {
            Some(lock) => Ok(lock),
            None => Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            }),
        }
    }

    /// Settles a fresh admission journal refused at registration (shutdown
    /// or capacity race after our `run_queued` write) as interrupted so the
    /// next boot sees a terminal outcome instead of running a request we
    /// reported as refused (E05). Best-effort: a contended lease means a
    /// peer owns execution and settles it.
    fn settle_refused_admission(run_dir: &Utf8Path) {
        if let Err(error) = crate::run_dirs::try_interrupt_admission(
            run_dir,
            model::FailureCode::Interrupted,
            "service shutdown before admission completed",
            None,
        ) {
            tracing::warn!(run_dir = %run_dir, %error, "refused admission settlement failed");
        }
    }

    /// Whether a local, non-terminal record already owns this run id. Such
    /// a record holds the live task handle, cancellation token, and event
    /// channel, so a retry must converge onto it instead of replacing it
    /// with fresh admission state (E03).
    pub(super) async fn live_run_record(&self, run_id: &str) -> bool {
        self.inner
            .runs
            .read()
            .await
            .get(run_id)
            .is_some_and(|record| !record.state.is_terminal())
    }

    /// Reserves the run id for a start request before any side effect, so an
    /// idempotency claim can bind retries to one run directory.
    pub fn reserve_start_run_id(&self, generator_id: &str) -> Result<String, ApiError> {
        // The reserved id becomes a run directory name, so a generator id that
        // carries a separator (`nested/generator`) or `..` is refused here
        // instead of producing a run id that escapes the runs directory.
        if !policy::is_safe_path_component(generator_id) {
            return Err(ApiError::invalid(format!(
                "generator id `{generator_id}` is not allowed"
            )));
        }
        Ok(format!("{generator_id}-{}", uuid::Uuid::now_v7()))
    }

    /// Reserves the run id for a fork request before any side effect.
    pub async fn reserve_fork_run_id(&self, _source_id: &str) -> Result<String, ApiError> {
        // This reservation precedes idempotency admission in the HTTP handler.
        // It must not read the source run/package: a Ready or Pending replay
        // must remain independent of the source's retention or snapshot state.
        // A fresh owner validates and loads the pinned source in `fork_run_with_id`.
        let reserved = format!("fork-{}", uuid::Uuid::now_v7());
        if !policy::is_safe_path_component(&reserved) {
            return Err(ApiError::invalid(format!(
                "fork run id `{reserved}` is not allowed"
            )));
        }
        Ok(reserved)
    }

    pub async fn start_run(&self, req: StartRun) -> Result<String, ApiError> {
        self.start_run_inner(req, None, None).await
    }

    pub async fn start_run_with_id(
        &self,
        req: StartRun,
        reserved_run_id: Option<String>,
    ) -> Result<String, ApiError> {
        self.start_run_inner(req, reserved_run_id, None).await
    }

    /// HTTP start admission with the package identity reviewed by the client.
    /// This second check runs after idempotency admission but immediately
    /// after loading the package and before any run-side filesystem effects.
    pub async fn start_run_with_package_identity(
        &self,
        req: StartRun,
        reserved_run_id: Option<String>,
        expected_package_sha256: &str,
    ) -> Result<String, ApiError> {
        self.start_run_inner(req, reserved_run_id, Some(expected_package_sha256))
            .await
    }

    async fn start_run_inner(
        &self,
        req: StartRun,
        reserved_run_id: Option<String>,
        expected_package_sha256: Option<&str>,
    ) -> Result<String, ApiError> {
        self.ensure_running()?;
        let mut contract = self.load_generator(&req.generator_id)?;
        let package_sha256 = crate::package::package_identity_sha256(
            &contract.root,
            &crate::package::PackageLimits::default(),
        )
        .map_err(|_| api_internal("failed to verify generator package identity"))?;
        let generator_path = contract.root.clone();
        if let Some(expected) = expected_package_sha256 {
            if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(ApiError::invalid(
                    "package identity must be a 64-character SHA-256 hex digest",
                ));
            }
            if !package_sha256.eq_ignore_ascii_case(expected) {
                return Err(ApiError::Conflict {
                    detail: "generator package identity does not match the reviewed identity"
                        .into(),
                });
            }
        }
        // Run metadata is metadata only: it never confers authorization and
        // is bounded so a request cannot grow journals without limit.
        if req.labels.len() > policy::MAX_RUN_LABELS {
            return Err(ApiError::invalid_field(
                "labels",
                format!("at most {} labels are accepted", policy::MAX_RUN_LABELS),
            ));
        }
        for (key, value) in &req.labels {
            if key.is_empty()
                || key.len() > policy::MAX_RUN_LABEL_BYTES
                || value.len() > policy::MAX_RUN_LABEL_BYTES
            {
                return Err(ApiError::invalid_field(
                    "labels",
                    format!(
                        "label keys and values must be non-empty and at most {} bytes",
                        policy::MAX_RUN_LABEL_BYTES
                    ),
                ));
            }
        }
        // A run may raise its audit level; the contract and deployment
        // floor keep their ability to raise it further.
        if req.audit_level == Some(policy::AuditLevel::Standard)
            && contract.manifest.audit.level == policy::AuditLevel::Minimal
        {
            contract.manifest.audit.level = policy::AuditLevel::Standard;
        }
        // Resolve defaults and FileValue normalization once at admission and
        // persist the canonical inputs everywhere (A10). Raw requests never
        // reach the journal or the engine.
        let canonical_inputs = match contract.manifest.resolve_inputs(req.inputs.clone()) {
            Ok(resolved) => engine::canonical_file_inputs(&contract, resolved)
                .map_err(|error| ApiError::invalid_field("inputs", error.to_string()))?,
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
        };
        let run_id = reserved_run_id
            .unwrap_or_else(|| format!("{}-{}", req.generator_id, uuid::Uuid::now_v7()));
        if !policy::is_safe_path_component(&run_id) {
            return Err(ApiError::invalid(format!(
                "run id `{run_id}` is not allowed"
            )));
        }
        let run_dir = self.inner.runs_dir.join(&run_id);
        // H01: cancellable shard wait shared with fork (see helper).
        let _admission = self.acquire_admission_shard(&run_dir).await?;
        // Adoption snapshot first: one journal read serves the live-hit
        // verification, the seed, and the queue instant below. The live
        // check reuses this snapshot instead of rescanning, so each
        // admission reads the journal at most once (E03).
        // Adoption: a retry bound to this run id by an expired idempotency
        // claim resumes the orphaned directory instead of creating a second
        // run. Same key and digest imply identical canonical inputs.
        let adopt_snapshot = crate::run_dirs::try_adopt_run_dir_with_snapshot(&run_dir, &run_id)
            .map_err(api_internal)?;
        let adopted = adopt_snapshot.adopted;
        if adopted {
            let state = adopt_snapshot
                .state
                .as_ref()
                .ok_or_else(|| api_internal("missing adoption state"))?;
            crate::run_package::load(&run_dir, state).map_err(api_internal)?;
        }
        if self.live_run_record(&run_id).await {
            // A live record holds the task, token, and channel: reuse it
            // only after proving the retry carries the admitted identity,
            // so a direct caller cannot silently reuse another admission
            // (E03). Re-checked under the admission lock (E05). The adopt
            // snapshot is reused when available; a rival that registered
            // after our snapshot left it empty falls back to a single
            // verification read.
            let identity = AdoptedRunIdentity {
                inputs: &canonical_inputs,
                contract_sha256: &contract.sha256,
                package_sha256: Some(&package_sha256),
                priority: req.priority.unwrap_or(0),
                parent: None,
                answers: &req.answers,
                confirmations: &req.confirmations,
            };
            verify_live_admission_from_snapshot(&adopt_snapshot, &identity, &run_dir)?;
            return Ok(run_id);
        }
        let inputs = canonical_inputs;
        let mut answers = req.answers;
        let mut confirmations = req.confirmations;
        let priority = req.priority.unwrap_or(0);
        // `adopted` / `adopt_snapshot` above are reused here: no second
        // journal scan occurs on this path (E03).
        // Phase 1: filesystem preparation happens outside the run-map lock so
        // concurrent runs never block on unrelated directory and journal I/O.
        // `created_fresh` is exactly `!adopted` after this block: only this
        // admission's own fresh directory is ever removed on failure, never
        // an adopted one (E03).
        if !adopted && let Err(error) = prepare_api_run_directory(&run_dir) {
            return Err(api_internal(error));
        }
        let created_fresh = !adopted;
        if created_fresh {
            let pinned = crate::run_package::copy(&generator_path, &run_dir, &package_sha256)
                .and_then(|pinned| {
                    if pinned.sha256 != contract.sha256 {
                        return Err(crate::types::ServiceError::Invalid(
                            "generator contract changed during admission".into(),
                        ));
                    }
                    Ok(pinned)
                });
            match pinned {
                Ok(pinned) => {
                    contract = pinned;
                    contract.apply_effective_audit(
                        req.audit_level,
                        self.inner.deployment_policy.audit_floor,
                    );
                }
                Err(error) => {
                    std::fs::remove_dir_all(&run_dir).map_err(api_internal)?;
                    return Err(api_internal(error));
                }
            }
        } else {
            contract = crate::run_package::load(
                &run_dir,
                adopt_snapshot
                    .state
                    .as_ref()
                    .ok_or_else(|| api_internal("missing adoption state"))?,
            )
            .map_err(api_internal)?;
            contract
                .apply_effective_audit(req.audit_level, self.inner.deployment_policy.audit_floor);
        }

        // The seed's queued instant rides along so the staged record
        // below never re-scans the journal for it (E03).
        let mut adopted_queued_at = None;
        if adopted {
            match seed_adopted_run_from_snapshot(
                &adopt_snapshot,
                &AdoptedRunIdentity {
                    inputs: &inputs,
                    contract_sha256: &contract.sha256,
                    package_sha256: Some(&package_sha256),
                    priority,
                    parent: None,
                    answers: &answers,
                    confirmations: &confirmations,
                },
            )? {
                Some(seed) => {
                    answers = seed.answers;
                    confirmations = seed.confirmations;
                    adopted_queued_at = seed.queued_at;
                }
                // The adopted run already settled: converge onto its
                // durable terminal state (E03).
                None => return Ok(run_id.to_string()),
            }
        }
        // Phase 2a first: losers allocate no channel, no token, no task, and no record. The
        // channel must still predate the journal write below (subscribers
        // cannot exist yet, so nothing is lost) (E03). Failure cleanup below
        // removes only this admission's own fresh directory; the id is a
        // fresh UUIDv7 no concurrent canceller can know, so no live mailbox
        // is destroyed with it (E03).
        let identity = AdoptedRunIdentity {
            inputs: &inputs,
            contract_sha256: &contract.sha256,
            package_sha256: Some(&package_sha256),
            priority,
            parent: None,
            answers: &answers,
            confirmations: &confirmations,
        };
        if let Some(id) = self
            .admission_gate(&run_id, &run_dir, created_fresh, &adopt_snapshot, &identity)
            .await?
        {
            return Ok(id);
        }
        // All per-admission allocations happen after the probe, so rejected
        // admissions allocate nothing (E03).
        let cancellation = CancellationToken::new();
        let task = Arc::new(Mutex::new(None));
        let (events, _) =
            broadcast::channel(self.inner.deployment_policy.live_event_channel_capacity);
        let mut staged = RunRecord {
            contract_sha256: contract.sha256.clone(),
            contract,
            package_sha256: Some(package_sha256.clone()),
            inputs,
            answers,
            confirmations,
            priority,
            parent_run_id: None,
            preempted: false,
            state: RunStatus::Queued,
            run_dir,
            artifacts: None,
            question: None,
            confirm: None,
            events,
            cancellation,
            scheduler_yield: CancellationToken::new(),
            task,
            queued_at: None,
            owner_id: self.inner.owner_id.clone(),
            ephemeral: false,
        };
        let effective = crate::types::ResolvedExecutionPolicy::resolve(
            staged.contract.manifest.budget.max_steps,
            self.inner.deployment_policy.max_total_steps,
            self.inner.deployment_policy.max_parallel_steps,
        );
        // Fresh admissions share one durable queue instant with memory;
        // adopted retries retain the instant from their single snapshot (E03).
        let fresh_queued_at = (!adopted).then(chrono::Utc::now);
        let queued_payload = (!adopted).then(|| {
            json!({
                "run_id": &run_id,
                "generator": format!("{}@{}", staged.contract.manifest.generator.id, staged.contract.manifest.generator.version),
                "generator_path": &generator_path,
                "package_sha256": &package_sha256,
                "contract_sha256": &staged.contract.sha256,
                "inputs": &staged.inputs,
                "answers": &staged.answers,
                "confirmations": &staged.confirmations,
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": self.inner.deployment_policy.elapsed_budget_mode,
                "retention_days": staged.contract.manifest.retention.days,
                "priority": staged.priority,
                "parent_run_id": &staged.parent_run_id,
                "labels": &req.labels,
                "audit_raise": req.audit_level,
                "effective_max_total_steps": effective.max_total_steps,
                "effective_policy_origin": effective.origin,
                "queued_at": fresh_queued_at.map(|at| at.to_rfc3339()),
            })
        });
        staged.queued_at = adopted_queued_at.or(fresh_queued_at);
        self.finish_prepared_admission(
            run_id,
            staged,
            adopt_snapshot,
            created_fresh,
            queued_payload,
        )
        .await
    }

    pub async fn fork_run(&self, source_id: &str, request: ForkRun) -> Result<String, ApiError> {
        self.fork_run_with_id(source_id, request, None).await
    }

    /// Checks the current source journal under execution ownership in either
    /// store mode. The caller holds the lease through the fresh checkpoint
    /// copy, including for terminal sources with unresolved operations.
    async fn fork_source_copy_lease(
        &self,
        source_id: &str,
        source_dir: &Utf8Path,
    ) -> Result<crate::run_dirs::RunExecutionLock, ApiError> {
        let conflict = || ApiError::Conflict {
            detail: format!(
                "run `{source_id}` is still executing; fork a stable waiting or terminal checkpoint"
            ),
        };
        if self.inner.run_store_mode == RunStoreMode::Exclusive
            && self
                .inner
                .runs
                .read()
                .await
                .get(source_id)
                .is_some_and(|source| {
                    matches!(source.state, RunStatus::Queued | RunStatus::Running)
                })
        {
            return Err(conflict());
        }
        let lease = crate::run_dirs::try_lock_run_execution(source_dir)
            .map_err(api_internal)?
            .ok_or_else(conflict)?;
        let state = self.authoritative_state(source_dir).await?;
        verify_fork_source_identity(&state, source_id)?;
        state
            .ensure_no_fail_closed_inflight_operations()
            .map_err(|detail| ApiError::Conflict { detail })?;
        if state.terminal.is_some() {
            return Ok(lease);
        }
        let pending_controls =
            crate::run_dirs::list_pending_cancel_controls(source_dir).map_err(api_internal)?;
        if state.settled_pending().is_some()
            && !state.cancel_requested
            && pending_controls.is_empty()
        {
            Ok(lease)
        } else {
            Err(conflict())
        }
    }

    pub async fn fork_run_with_id(
        &self,
        source_id: &str,
        request: ForkRun,
        reserved_run_id: Option<String>,
    ) -> Result<String, ApiError> {
        self.fork_run_prepared(source_id, request, reserved_run_id, None, || Ok(()))
            .await
    }

    /// Internal prepared-plan admission. The coordinator retains its key gate;
    /// `bind` runs under target admission and (for fresh copies) source execution.
    #[doc(hidden)]
    pub async fn fork_run_prepared(
        &self,
        source_id: &str,
        request: ForkRun,
        reserved_run_id: Option<String>,
        prepared: Option<&crate::PreparedForkPlan>,
        bind: impl FnOnce() -> Result<(), ApiError>,
    ) -> Result<String, ApiError> {
        self.ensure_running()?;
        if request.at_seq == 0 && request.reuse.is_none() {
            return Err(ApiError::invalid_field(
                "at_seq",
                "checkpoint sequence must be greater than zero",
            ));
        }
        // A reserved id comes from the HTTP idempotency admission. Do not
        // inspect either the source package or target directory until the
        // target's admission lock has been acquired: Ready/Pending retries
        // must not depend on source retention, and target probes must be
        // serialized before reading their pinned package.
        let mut preloaded_source = None;
        let run_id = if let Some(run_id) = reserved_run_id {
            run_id
        } else {
            let source_dir = self.run_dir_for(source_id).await?;
            let state = crate::summaries::fold_run_state(&source_dir).map_err(api_internal)?;
            let contract = crate::run_package::load(&source_dir, &state).map_err(api_internal)?;
            let run_id = format!(
                "{}-fork-{}",
                contract.manifest.generator.id,
                uuid::Uuid::now_v7()
            );
            preloaded_source = Some((source_dir, state, contract));
            run_id
        };
        if !policy::is_safe_path_component(&run_id) {
            return Err(ApiError::invalid(format!(
                "run id `{run_id}` is not allowed"
            )));
        }
        let run_dir = self.inner.runs_dir.join(&run_id);
        // H01: cancellable shard wait shared with start (see helper).
        let _admission = self.acquire_admission_shard(&run_dir).await?;
        // Re-checked under the admission lock so a drain racing the first
        // probe cannot be missed (E05).
        let adopt_snapshot = crate::run_dirs::try_adopt_run_dir_with_snapshot(&run_dir, &run_id)
            .map_err(api_internal)?;
        if adopt_snapshot.adopted {
            let lineage = adopt_snapshot
                .events
                .iter()
                .rev()
                .find(|event| event["t"] == "run_forked")
                .ok_or_else(|| api_internal("fork lineage is missing"))?;
            let stored: Option<crate::ForkPlanBinding> = lineage
                .get("binding")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()
                .map_err(api_internal)?;
            if stored.as_ref() != prepared.map(crate::PreparedForkPlan::binding) {
                return Err(ApiError::Conflict {
                    detail: "fork lineage binding mismatch".into(),
                });
            }
        }
        let mut bind = Some(bind);
        let adopted = adopt_snapshot.adopted;
        let live_hit = self.live_run_record(&run_id).await;
        if live_hit && !adopted {
            return Err(ApiError::Conflict {
                detail: format!(
                    "run `{run_id}` is live but has no journal; refusing to overwrite it with a fork copy"
                ),
            });
        }

        let (package_sha256, generator_path, mut contract, source_dir) = if adopted {
            let state = adopt_snapshot
                .state
                .as_ref()
                .ok_or_else(|| api_internal("missing adoption state"))?;
            let package_sha256 = state
                .package_sha256
                .clone()
                .ok_or_else(|| api_internal("fork target has no pinned package identity"))?;
            let generator_path = state
                .generator_path
                .clone()
                .ok_or_else(|| api_internal("fork target has no original generator path"))?;
            let contract = crate::run_package::load(&run_dir, state).map_err(api_internal)?;
            (package_sha256, generator_path, contract, None)
        } else {
            let (source_dir, state, contract) = match preloaded_source {
                Some(source) => source,
                None => {
                    let source_dir = self.run_dir_for(source_id).await?;
                    let state =
                        crate::summaries::fold_run_state(&source_dir).map_err(api_internal)?;
                    let contract =
                        crate::run_package::load(&source_dir, &state).map_err(api_internal)?;
                    (source_dir, state, contract)
                }
            };
            let package_sha256 = state.package_sha256.clone().ok_or_else(|| {
                api_internal("fork source has no pinned generator package identity")
            })?;
            let generator_path = state
                .generator_path
                .clone()
                .ok_or_else(|| api_internal("fork source has no original generator path"))?;
            (package_sha256, generator_path, contract, Some(source_dir))
        };
        if live_hit {
            if let Some(bind) = bind.take() {
                bind()?;
            }
            resolve_fork_live_hit(&adopt_snapshot, &contract, source_id, &request)?;
            return Ok(run_id);
        }
        contract.apply_audit_floor(self.inner.deployment_policy.audit_floor);
        let mut created_fresh = false;
        if !adopted {
            let source_dir = source_dir
                .as_deref()
                .ok_or_else(|| api_internal("fresh fork is missing its source directory"))?;
            // A child adoption or live hit converges before source eligibility
            // is consulted. Source leases remain held through the copy
            // and are released before child execution.
            let source_lease = self.fork_source_copy_lease(source_id, source_dir).await?;
            if let Some(bind) = bind.take() {
                bind()?;
            }
            let (fresh, staged_source) =
                crate::run_dirs::prepare_checkpoint_fork_prepared_with_limits(
                    source_dir,
                    source_id,
                    &run_dir,
                    &run_id,
                    request.at_seq,
                    &request.state_patch,
                    prepared,
                    engine::JournalLimits::from(&contract.manifest.runtime),
                )
                .map_err(api_internal)?;
            created_fresh = fresh;
            let source_dir = staged_source
                .as_ref()
                .map_or(source_dir, |staged| staged.0.as_path());
            let copied = crate::run_package::copy(
                &crate::run_package::package_dir(source_dir),
                &run_dir,
                &package_sha256,
            );
            match copied {
                Ok(pinned) => contract = pinned,
                Err(error) => {
                    if created_fresh {
                        std::fs::remove_dir_all(&run_dir).map_err(api_internal)?;
                    }
                    return Err(api_internal(error));
                }
            }
            if let Some(staged) = staged_source {
                staged.finish().map_err(api_internal)?;
            }
            drop(source_lease);
        }
        if let Some(bind) = bind.take() {
            bind()?;
        }
        // Single snapshot for the whole fork admission: adopted reuses the
        // adoption read (with its fold), fresh reads and folds once after
        // the checkpoint copy (E03).
        let fork_snapshot: crate::run_dirs::AdoptSnapshot = if adopted {
            adopt_snapshot
        } else {
            let events = crate::summaries::read_journal_events(&run_dir).map_err(api_internal)?;
            let state = fold_prepared_fork_snapshot(&events).map_err(api_internal)?;
            crate::run_dirs::AdoptSnapshot {
                adopted: false,
                events,
                state: Some(state),
            }
        };
        let fork_elapsed_mode = fork_snapshot
            .state
            .as_ref()
            .ok_or_else(|| api_internal("fork missing folded state"))?
            .budget
            .elapsed_budget_mode;
        let fork_inputs =
            fork_checkpoint_inputs(&fork_snapshot, &contract, source_id, request.at_seq);
        let inputs = match fork_inputs {
            Ok(inputs) => inputs,
            Err(error) => {
                if created_fresh {
                    match std::fs::symlink_metadata(&run_dir) {
                        Ok(meta) if meta.file_type().is_symlink() => {
                            return Err(api_internal(format!(
                                "{error}; run directory `{run_id}` is a symlink; refusing cleanup"
                            )));
                        }
                        Ok(_) => {
                            if created_fresh
                                && let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir)
                            {
                                return Err(api_internal(format!(
                                    "{error}; failed to clean incomplete fork `{run_id}`: {cleanup_error}"
                                )));
                            }
                        }
                        Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => {}
                        Err(cleanup) => {
                            return Err(api_internal(format!(
                                "{error}; failed to stat incomplete fork `{run_id}`: {cleanup}"
                            )));
                        }
                    }
                }
                return Err(error);
            }
        };
        let mut fork_answers = request.answers.clone();
        let mut fork_confirmations = request.confirmations.clone();
        let mut adopted_queued_at = None;
        if adopted {
            match seed_adopted_run_from_snapshot(
                &fork_snapshot,
                &AdoptedRunIdentity {
                    inputs: &inputs,
                    contract_sha256: &contract.sha256,
                    package_sha256: Some(&package_sha256),
                    priority: request.priority.unwrap_or(0),
                    parent: Some(source_id),
                    answers: &fork_answers,
                    confirmations: &fork_confirmations,
                },
            )? {
                Some(seed) => {
                    fork_answers = seed.answers;
                    fork_confirmations = seed.confirmations;
                    adopted_queued_at = seed.queued_at;
                }
                // The adopted fork already settled: converge onto its
                // durable terminal state (E03).
                None => return Ok(run_id.to_string()),
            }
        }
        // Phase 2a first: losers allocate no channel, no token, no task, and no record. The
        // channel must still predate the journal write below (subscribers
        // cannot exist yet, so nothing is lost) (E03). Failure cleanup below
        // removes only this admission's own fresh directory; the id is a
        // fresh UUIDv7 no concurrent canceller can know, so no live mailbox
        // is destroyed with it (E03).
        let identity = AdoptedRunIdentity {
            inputs: &inputs,
            contract_sha256: &contract.sha256,
            package_sha256: Some(&package_sha256),
            priority: request.priority.unwrap_or(0),
            parent: Some(source_id),
            answers: &fork_answers,
            confirmations: &fork_confirmations,
        };
        if let Some(id) = self
            .admission_gate(&run_id, &run_dir, created_fresh, &fork_snapshot, &identity)
            .await?
        {
            return Ok(id);
        }
        // All per-admission allocations happen after the probe, so rejected
        // admissions allocate nothing (E03).
        let cancellation = CancellationToken::new();
        let task = Arc::new(Mutex::new(None));
        let (events, _) =
            broadcast::channel(self.inner.deployment_policy.live_event_channel_capacity);
        let mut staged = RunRecord {
            contract_sha256: contract.sha256.clone(),
            contract,
            package_sha256: Some(package_sha256.clone()),
            inputs,
            answers: fork_answers,
            confirmations: fork_confirmations,
            priority: request.priority.unwrap_or(0),
            parent_run_id: Some(source_id.to_string()),
            preempted: false,
            state: RunStatus::Queued,
            run_dir,
            artifacts: None,
            question: None,
            confirm: None,
            events,
            cancellation,
            scheduler_yield: CancellationToken::new(),
            task,
            queued_at: None,
            owner_id: self.inner.owner_id.clone(),
            ephemeral: false,
        };
        let fork_effective = crate::types::ResolvedExecutionPolicy::resolve(
            staged.contract.manifest.budget.max_steps,
            self.inner.deployment_policy.max_total_steps,
            self.inner.deployment_policy.max_parallel_steps,
        );
        // Fresh admissions share one durable queue instant with memory;
        // adopted retries retain the instant from their single snapshot (E03).
        let fresh_queued_at = (!adopted).then(chrono::Utc::now);
        let queued_payload = (!adopted).then(|| {
            json!({
                "run_id": &run_id,
                "generator": format!("{}@{}", staged.contract.manifest.generator.id, staged.contract.manifest.generator.version),
                "generator_path": &generator_path,
                "package_sha256": &package_sha256,
                "contract_sha256": &staged.contract.sha256,
                "inputs": &staged.inputs,
                "answers": &staged.answers,
                "confirmations": &staged.confirmations,
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": fork_elapsed_mode,
                "retention_days": staged.contract.manifest.retention.days,
                "priority": staged.priority,
                "parent_run_id": &staged.parent_run_id,
                "reuse": request.reuse,
                "queued_at": fresh_queued_at.map(|at| at.to_rfc3339()),
                "effective_max_total_steps": fork_effective.max_total_steps,
                "effective_policy_origin": fork_effective.origin,
            })
        });
        staged.queued_at = adopted_queued_at.or(fresh_queued_at);
        self.finish_prepared_admission(run_id, staged, fork_snapshot, created_fresh, queued_payload)
            .await
    }
}

#[cfg(test)]
mod fork_protocol_tests {
    use super::*;
    use crate::{run_dirs, summaries, tests::support::*};

    fn prepared_fork(markers: Value) -> (TempGuard, LocalService, RunRecord, Vec<Value>, Value) {
        let root = TempGuard(temp_run_dir("fork-admission-protocol"));
        let source = synthetic_hitl_record(&root.0, "source");
        prepared_fork_from(root, source, markers)
    }

    fn prepared_fork_from(
        root: TempGuard,
        source: RunRecord,
        markers: Value,
    ) -> (TempGuard, LocalService, RunRecord, Vec<Value>, Value) {
        let service = test_service(vec![], root.0.join("runs"));
        let mut child = source.clone();
        child.run_dir = root.0.join("runs/child");
        child.parent_run_id = Some("source".into());
        run_dirs::prepare_checkpoint_fork(
            &source.run_dir,
            "source",
            &child.run_dir,
            "child",
            1,
            &api::ForkStatePatch::default(),
        )
        .unwrap();
        crate::run_package::copy(
            &crate::run_package::package_dir(&source.run_dir),
            &child.run_dir,
            source.package_sha256.as_deref().unwrap(),
        )
        .unwrap();
        let mut events = summaries::read_journal_events(&child.run_dir).unwrap();
        let fork = events.last_mut().unwrap().as_object_mut().unwrap();
        assert_eq!(fork["t"], "run_forked");
        fork.remove("fork_tree_commit_version");
        fork.extend(markers.as_object().unwrap().clone());
        write_journal(&child, &events);
        let mut queue = events[0].clone();
        for key in ["seq", "ts", "trace_id", "span_id", "t"] {
            queue.as_object_mut().unwrap().remove(key);
        }
        queue["run_id"] = json!("child");
        queue["parent_run_id"] = json!("source");
        let effective = crate::types::ResolvedExecutionPolicy::resolve(
            child.contract.manifest.budget.max_steps,
            service.inner.deployment_policy.max_total_steps,
            service.inner.deployment_policy.max_parallel_steps,
        );
        queue["effective_max_total_steps"] = json!(effective.max_total_steps);
        queue["effective_policy_origin"] = json!(effective.origin);
        (root, service, child, events, queue)
    }

    fn write_journal(child: &RunRecord, events: &[Value]) {
        std::fs::write(
            summaries::run_meta_dir(&child.run_dir).join("journal.jsonl"),
            events
                .iter()
                .map(|event| format!("{event}\n"))
                .collect::<String>(),
        )
        .unwrap();
    }

    async fn join_engine(child: &RunRecord) {
        let handle = child.task.lock().unwrap().take().expect("spawned task");
        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("engine must return")
            .expect("engine task must not panic");
    }

    fn assert_not_executed(events: &[Value]) {
        assert!(!events.iter().any(|event| matches!(
            event["t"].as_str(),
            Some("run_started" | "run_resumed" | "step_started")
        )));
    }

    #[tokio::test]
    async fn reset_bound_admission_requires_supported_protocol() {
        // Presence of either marker (even malformed) must fail closed.
        for marker in [
            json!({"binding": {"reset_candidate_sha256": null}}),
            json!({"reset_overlay_sha256": null}),
        ] {
            for version in [None, Some(json!(999)), Some(Value::Null)] {
                let mut markers = marker.clone();
                if let Some(version) = version {
                    markers["fork_tree_commit_version"] = version;
                }
                let (_root, service, child, events, queue) = prepared_fork(markers);
                let before = events.clone();
                let error = service
                    .finish_prepared_admission(
                        "child".into(),
                        child.clone(),
                        run_dirs::AdoptSnapshot {
                            adopted: false,
                            events,
                            state: None,
                        },
                        true,
                        Some(queue),
                    )
                    .await
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("requires a supported fork tree commit protocol")
                );
                assert!(run_dirs::fork_admission_pending(&child.run_dir).unwrap());
                assert!(!service.inner.runs.read().await.contains_key("child"));
                assert!(child.task.lock().unwrap().is_none());
                assert_eq!(
                    summaries::read_journal_events(&child.run_dir).unwrap(),
                    before
                );
                assert_not_executed(&before);
            }
        }
    }

    // Wire-only fixture kept inside this test module: it creates no public
    // reset candidate constructor or production admission caller.
    fn prepared_reset_bound_fork(
        version: Option<Value>,
    ) -> (TempGuard, LocalService, RunRecord, Vec<Value>, Value) {
        let root = TempGuard(temp_run_dir("reset-fork-admission-protocol"));
        let source = synthetic_record(&root.0, "source", "format-check");
        let (_root, service, child, mut before, mut queue) =
            prepared_fork_from(root, source, json!({}));
        let registry = run_dirs::app_registry(run_dirs::load_llm_runtime(None).unwrap());
        let attestation = registry
            .continuation_attestation_for_tests(&child.contract.graph.nodes["check_json"])
            .expect("format check executor must be continuation-safe");
        // The completed admission folds successfully, including when protocol
        // metadata is missing: rejection tests must reach the service gate.
        let mut fork = before.pop().unwrap();
        let mut failed = fork.clone();
        for key in ["source_run_id", "source_seq"] {
            failed.as_object_mut().unwrap().remove(key);
        }
        failed["t"] = json!("step_finished");
        failed["node"] = json!("check_json");
        failed["status"] = json!("failed");
        before.push(failed);
        let state = engine::RunState::fold_values(&before).unwrap();
        let digest = |value: &Value| {
            use sha2::{Digest, Sha256};
            hex::encode(Sha256::digest(serde_json::to_vec(value).unwrap()))
        };
        let candidate = json!({
            "identity": vec![0u8; 32],
            "source_run_id": "source",
            "source_contract_sha256": child.contract_sha256,
            "source_last_seq": state.last_seq,
            "retry_roots": ["check_json"],
            "stale_outcomes": [],
            "preserved_outcomes": [],
            "inherited_budget": state.budget,
            "reset_nodes": {
                "check_json": {"output_alias": "check_json", "predecessors": []}
            },
            "expected_executor_attestations": {"check_json": attestation}
        });
        let candidate_digest = digest(&candidate);
        let binding = json!({"binding_version": 1, "plan_digest": "a".repeat(64),
            "reset_candidate_sha256": candidate_digest});
        let overlay = json!({"version": 1, "candidate": candidate, "binding": binding});
        fork["seq"] = json!(state.last_seq + 1);
        fork["source_seq"] = json!(state.last_seq);
        fork["source_contract_sha256"] = json!(child.contract_sha256);
        fork["binding"] = binding;
        fork["reset_overlay_sha256"] = json!(digest(&overlay));
        if let Some(version) = version {
            fork["fork_tree_commit_version"] = version;
        }
        let mut overlay_event = fork.clone();
        for key in [
            "source_run_id",
            "source_seq",
            "source_contract_sha256",
            "binding",
            "reset_overlay_sha256",
            "fork_tree_commit_version",
        ] {
            overlay_event.as_object_mut().unwrap().remove(key);
        }
        overlay_event["t"] = json!("child_reset_overlay");
        overlay_event["seq"] = json!(state.last_seq + 2);
        overlay_event["overlay"] = overlay;
        for key in ["ts", "trace_id", "span_id"] {
            queue[key] = overlay_event[key].clone();
        }
        queue["t"] = json!("run_queued");
        queue["seq"] = json!(state.last_seq + 3);
        before.extend([fork, overlay_event, queue]);
        assert!(
            engine::RunState::fold_values(&before)
                .unwrap()
                .terminal
                .is_none()
        );
        let queue = before.pop().unwrap();
        write_journal(&child, &before);
        (_root, service, child, before, queue)
    }

    #[tokio::test]
    async fn reset_bound_lifecycle_requires_supported_protocol() {
        for version in [None, Some(json!(999)), Some(Value::Null)] {
            let (_root, service, child, mut before, queue) = prepared_reset_bound_fork(version);
            before.push(queue);
            write_journal(&child, &before);
            // Model a registered run with its pending marker already cleared.
            run_dirs::clear_fork_admission_pending(&child.run_dir).unwrap();
            service
                .inner
                .runs
                .write()
                .await
                .insert("child".into(), child.clone());
            service
                .clone()
                .spawn_engine_run(SpawnRun {
                    run_id: "child".into(),
                    contract: child.contract.clone(),
                    inputs: child.inputs.clone(),
                    run_dir: child.run_dir.clone(),
                    events: child.events.clone(),
                    answers: child.answers.clone(),
                    confirmations: child.confirmations.clone(),
                    journal_snapshot: Some(before.clone()),
                    cancellation: child.cancellation.clone(),
                    task: child.task.clone(),
                })
                .await;
            join_engine(&child).await;
            assert_eq!(
                service.inner.runs.read().await["child"].state,
                RunStatus::Queued
            );
            assert_eq!(
                summaries::read_journal_events(&child.run_dir).unwrap(),
                before
            );
            assert_not_executed(&before);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reset_bound_first_execution_rechecks_committed_tree_after_queue_delay() {
        let (_root, service, child, events, mut queue) =
            prepared_reset_bound_fork(Some(json!(crate::fork_commit::VERSION)));
        let state = fold_prepared_fork_snapshot(&events).unwrap();
        let probe = QueueProbe(child.run_dir.clone());
        let (admitted, release) = probe.gate("admitted");
        for key in ["seq", "ts", "trace_id", "span_id", "t"] {
            queue.as_object_mut().unwrap().remove(key);
        }
        service
            .finish_prepared_admission(
                "child".into(),
                child.clone(),
                run_dirs::AdoptSnapshot {
                    adopted: false,
                    events,
                    state: Some(state),
                },
                true,
                Some(queue),
            )
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), admitted)
            .await
            .unwrap()
            .unwrap();

        let queued_events = summaries::read_journal_events(&child.run_dir).unwrap();
        let fork = queued_events
            .iter()
            .rev()
            .find(|event| event["t"] == "run_forked")
            .unwrap();
        assert!(crate::fork_commit::reset_bound(fork));
        assert_eq!(
            fork["fork_tree_commit_version"],
            crate::fork_commit::VERSION
        );
        assert_eq!(queued_events.last().unwrap()["t"], "run_queued");
        assert!(!run_dirs::fork_admission_pending(&child.run_dir).unwrap());
        crate::fork_commit::verify(&child.run_dir, "child", &queued_events).unwrap();
        assert_not_executed(&queued_events);
        // Only the tree changes after successful publication: the reset-bound
        // dispatch must run the full verifier again before first execution.
        std::fs::write(
            child.run_dir.join("workspace/uncommitted"),
            b"unexpected payload",
        )
        .unwrap();
        release.send(()).unwrap();
        join_engine(&child).await;

        assert_eq!(
            service.inner.runs.read().await["child"].state,
            RunStatus::Queued
        );
        let events = summaries::read_journal_events(&child.run_dir).unwrap();
        assert_eq!(events, queued_events);
        assert_not_executed(&events);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reset_bound_fork_admits_executes_and_adopts_with_fsync_commit() {
        let (_root, service, child, events, mut queue) =
            prepared_reset_bound_fork(Some(json!(crate::fork_commit::VERSION)));
        assert!(run_dirs::fork_admission_pending(&child.run_dir).unwrap());
        let queue_seq = queue["seq"].as_u64().unwrap();
        assert!(engine::RunState::fold_values(&events).is_err());
        let state = fold_prepared_fork_snapshot(&events).unwrap();
        let journal_path = summaries::run_meta_dir(&child.run_dir).join("journal.jsonl");
        let before = summaries::read_journal_events(&child.run_dir).unwrap();
        let error = run_dirs::write_run_event(&child, "step_started", json!({"node":"check_json"}))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("reset admission requires exactly one run_queued event")
        );
        assert_eq!(
            summaries::read_journal_events(&child.run_dir).unwrap(),
            before
        );
        assert!(run_dirs::fork_admission_pending(&child.run_dir).unwrap());
        let mut queued_payload = queue.clone();
        for key in ["seq", "ts", "trace_id", "span_id", "t"] {
            queued_payload.as_object_mut().unwrap().remove(key);
        }
        let error = engine::JournalWriter::append_events_if(
            &journal_path,
            "child",
            vec![("run_queued", queued_payload), ("run_started", json!({}))],
            engine::JournalLimits::from(&child.contract.manifest.runtime),
            None,
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("reset admission requires exactly one run_queued event")
        );
        assert_eq!(
            summaries::read_journal_events(&child.run_dir).unwrap(),
            before
        );
        assert!(run_dirs::fork_admission_pending(&child.run_dir).unwrap());
        assert!(
            engine::RunState::fold_journal_with_limits(
                &journal_path,
                engine::JournalLimits::from(&child.contract.manifest.runtime)
            )
            .is_err()
        );
        for key in ["seq", "ts", "trace_id", "span_id", "t"] {
            queue.as_object_mut().unwrap().remove(key);
        }
        let admitted = service
            .finish_prepared_admission(
                "child".into(),
                child.clone(),
                run_dirs::AdoptSnapshot {
                    adopted: false,
                    events,
                    state: Some(state),
                },
                true,
                Some(queue),
            )
            .await
            .unwrap();
        assert_eq!(admitted, "child");
        assert!(!run_dirs::fork_admission_pending(&child.run_dir).unwrap());
        assert!(service.inner.runs.read().await.contains_key("child"));
        join_engine(&child).await;

        let events = summaries::read_journal_events(&child.run_dir).unwrap();
        let queue = events
            .iter()
            .find(|event| {
                event["t"] == "run_queued"
                    && event["run_id"] == "child"
                    && event["seq"].as_u64() == Some(queue_seq)
            })
            .expect("durable reset admission queue");
        assert_eq!(queue["seq"], queue_seq);
        let commit: Value = serde_json::from_slice(
            &std::fs::read(summaries::run_meta_dir(&child.run_dir).join("fork-tree.commit.json"))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(commit["durability"], "fsync");
        assert_eq!(commit["run_id"], "child");
        assert_eq!(commit["queue_seq"], queue_seq);
        let verification = crate::fork_commit::verify(&child.run_dir, "child", &events);
        assert!(
            verification.is_ok(),
            "commit verification failed: {verification:?}"
        );
        assert!(
            events.iter().any(|event| {
                event["t"] == "run_started"
                    && event["run_id"] == "child"
                    && event["seq"].as_u64().unwrap() > queue_seq
            }),
            "child did not start; state={:?}; events={events:#?}",
            service.inner.runs.read().await["child"].state
        );
        let adopted = run_dirs::try_adopt_run_dir_with_snapshot(&child.run_dir, "child")
            .expect("settled reset-bound fork must remain adoptable");
        assert!(adopted.adopted);
        assert_eq!(adopted.events, events);
        assert_eq!(adopted.state.unwrap().run_id.as_deref(), Some("child"));
        assert!(!run_dirs::fork_admission_pending(&child.run_dir).unwrap());
    }

    #[tokio::test]
    async fn ordinary_legacy_fork_admits_and_executes_without_commit_protocol() {
        let (_root, service, child, events, queue) = prepared_fork(json!({}));
        service
            .finish_prepared_admission(
                "child".into(),
                child.clone(),
                run_dirs::AdoptSnapshot {
                    adopted: false,
                    events,
                    state: None,
                },
                true,
                Some(queue),
            )
            .await
            .unwrap();
        join_engine(&child).await;
        assert!(!run_dirs::fork_admission_pending(&child.run_dir).unwrap());
        assert!(service.inner.runs.read().await.contains_key("child"));
        assert!(
            summaries::read_journal_events(&child.run_dir)
                .unwrap()
                .iter()
                .any(
                    |event| matches!(event["t"].as_str(), Some("run_started" | "run_resumed"))
                        && event["run_id"] == "child"
                )
        );
    }
}
