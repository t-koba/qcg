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
                    Self::settle_refused_admission(&staged.run_dir).await;
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
    pub(super) async fn settle_refused_admission(run_dir: &Utf8Path) {
        let lease = match crate::run_dirs::try_lock_run_execution(run_dir) {
            Ok(lease) => lease,
            Err(error) => {
                tracing::warn!(run_dir = %run_dir, %error, "refused admission settlement failed to lock execution");
                return;
            }
        };
        if lease.is_none() {
            return;
        }
        let Some(run_id) = run_dir.file_name() else {
            tracing::warn!(run_dir = %run_dir, "refused admission settlement without a run id");
            return;
        };
        let journal = crate::summaries::run_meta_dir(run_dir).join("journal.jsonl");
        match engine::JournalWriter::append_single_event(
            &journal,
            run_id,
            "run_interrupted",
            serde_json::json!({
                "reason": {"code": "interrupted", "message": "service shutdown before admission completed"},
            }),
            engine::JournalLimits::default(),
            None,
        ) {
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(run_dir = %run_dir, %error, "refused admission settlement failed to journal");
            }
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
        if !is_safe_run_id(generator_id) {
            return Err(api_bad_request(format!(
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
        if !is_safe_run_id(&reserved) {
            return Err(api_bad_request(format!(
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
                return Err(api_bad_request(
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
        if !is_safe_run_id(&run_id) {
            return Err(api_bad_request(format!("run id `{run_id}` is not allowed")));
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
        match self.probe_admission(&run_id).await {
            Ok(AdmissionProbe::Proceed) => {}
            Ok(AdmissionProbe::Converged) => {
                // Reuse the adoption snapshot when it exists: a second
                // journal read here would double I/O per converged retry
                // (E03). A fresh (empty) snapshot means a rival registered
                // after our adoption read; fall back to a fresh read so the
                // retry converges instead of conflicting on empty input.
                let identity = AdoptedRunIdentity {
                    inputs: &inputs,
                    contract_sha256: &contract.sha256,
                    package_sha256: Some(&package_sha256),
                    priority,
                    parent: None,
                    answers: &answers,
                    confirmations: &confirmations,
                };
                verify_live_admission_from_snapshot(&adopt_snapshot, &identity, &run_dir)?;
                return Ok(run_id);
            }
            Err(ProbeRejection::Shutdown) => {
                return Err(refused_shutdown(
                    created_fresh.then_some(run_dir.as_path()),
                )?);
            }
            Err(ProbeRejection::Capacity) => {
                return Err(refused_capacity(
                    created_fresh.then_some(run_dir.as_path()),
                    self.inner.max_tracked_runs,
                )?);
            }
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
            self.max_total_steps(),
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

    /// Checks source eligibility only for a fresh checkpoint copy. Shared
    /// stores use the source journal as authority; a lease protects a
    /// nonterminal pending checkpoint while it is copied. Terminal state
    /// needs no lease, since its durable settlement already prevents further
    /// execution writes.
    async fn fork_source_copy_lease(
        &self,
        source_id: &str,
        source_dir: &Utf8Path,
    ) -> Result<Option<crate::run_dirs::RunExecutionLock>, ApiError> {
        let conflict = || ApiError::Conflict {
            detail: format!(
                "run `{source_id}` is still executing; fork a stable waiting or terminal checkpoint"
            ),
        };
        if self.inner.run_store_mode == RunStoreMode::Exclusive {
            if self
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
            return Ok(None);
        }

        let state = self.authoritative_state(source_dir).await?;
        verify_fork_source_identity(&state, source_id)?;
        if state.terminal.is_some() {
            return Ok(None);
        }

        match crate::run_dirs::try_lock_run_execution(source_dir).map_err(api_internal)? {
            Some(lease) => {
                let state = self.authoritative_state(source_dir).await?;
                verify_fork_source_identity(&state, source_id)?;
                if state.terminal.is_some() {
                    return Ok(None);
                }
                let pending_controls = crate::run_dirs::list_pending_cancel_controls(source_dir)
                    .map_err(api_internal)?;
                if state.settled_pending().is_some()
                    && !state.cancel_requested
                    && pending_controls.is_empty()
                {
                    Ok(Some(lease))
                } else {
                    Err(conflict())
                }
            }
            None => {
                let state = self.authoritative_state(source_dir).await?;
                verify_fork_source_identity(&state, source_id)?;
                if state.terminal.is_some() {
                    Ok(None)
                } else {
                    Err(conflict())
                }
            }
        }
    }

    pub async fn fork_run_with_id(
        &self,
        source_id: &str,
        request: ForkRun,
        reserved_run_id: Option<String>,
    ) -> Result<String, ApiError> {
        self.ensure_running()?;
        if request.at_seq == 0 {
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
        if !is_safe_run_id(&run_id) {
            return Err(api_bad_request(format!("run id `{run_id}` is not allowed")));
        }
        let run_dir = self.inner.runs_dir.join(&run_id);
        // H01: cancellable shard wait shared with start (see helper).
        let _admission = self.acquire_admission_shard(&run_dir).await?;
        // Re-checked under the admission lock so a drain racing the first
        // probe cannot be missed (E05).
        let adopt_snapshot = crate::run_dirs::try_adopt_run_dir_with_snapshot(&run_dir, &run_id)
            .map_err(api_internal)?;
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
            // is consulted. Shared-mode leases remain held through the copy
            // and are released before child execution.
            let source_lease = self.fork_source_copy_lease(source_id, source_dir).await?;
            created_fresh = prepare_checkpoint_fork(
                source_dir,
                source_id,
                &run_dir,
                &run_id,
                request.at_seq,
                &request.state_patch,
            )
            .map_err(api_internal)?;
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
            drop(source_lease);
        }
        // Single snapshot for the whole fork admission: adopted reuses the
        // adoption read (with its fold), fresh reads and folds once after
        // the checkpoint copy (E03).
        let fork_snapshot: crate::run_dirs::AdoptSnapshot = if adopted {
            adopt_snapshot
        } else {
            let events = crate::summaries::read_journal_events(&run_dir).map_err(api_internal)?;
            let state = engine::RunState::fold_values(&events).map_err(api_internal)?;
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
        match self.probe_admission(&run_id).await {
            Ok(AdmissionProbe::Proceed) => {}
            Ok(AdmissionProbe::Converged) => {
                // Reuse the single admission snapshot when it exists, never
                // a second scan or fold (E03). A rival that registered after
                // our snapshot read leaves it empty here; fall back to a
                // fresh read so the retry converges instead of conflicting.
                let identity = AdoptedRunIdentity {
                    inputs: &inputs,
                    contract_sha256: &contract.sha256,
                    package_sha256: Some(&package_sha256),
                    priority: request.priority.unwrap_or(0),
                    parent: Some(source_id),
                    answers: &fork_answers,
                    confirmations: &fork_confirmations,
                };
                verify_live_admission_from_snapshot(&fork_snapshot, &identity, &run_dir)?;
                return Ok(run_id);
            }
            Err(ProbeRejection::Shutdown) => {
                return Err(refused_shutdown(
                    created_fresh.then_some(run_dir.as_path()),
                )?);
            }
            Err(ProbeRejection::Capacity) => {
                return Err(refused_capacity(
                    created_fresh.then_some(run_dir.as_path()),
                    self.inner.max_tracked_runs,
                )?);
            }
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
            self.max_total_steps(),
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
