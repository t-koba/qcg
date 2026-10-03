//! Admission operations.
use super::*;

impl LocalService {
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
            if let Some(dir) = own_dir {
                std::fs::remove_dir_all(dir).map_err(api_internal)?;
            }
            return Err(ApiError::Unavailable {
                detail: format!(
                    "run capacity is exhausted: {} non-terminal runs are already tracked",
                    self.inner.max_tracked_runs
                ),
            });
        }
        runs.insert(run_id, staged);
        Ok(AdmissionVerdict::Inserted)
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
    pub async fn reserve_fork_run_id(&self, source_id: &str) -> Result<String, ApiError> {
        let source_dir = self.run_dir_for(source_id).await?;
        let generator_path = read_run_generator_path(&source_dir).map_err(api_internal)?;
        let contract = Contract::load(&generator_path).map_err(api_internal)?;
        let reserved = format!(
            "{}-fork-{}",
            contract.manifest.generator.id,
            uuid::Uuid::now_v7()
        );
        // The composed id names a run directory, so it is validated as one even
        // though it is derived from an already-loaded contract.
        if !is_safe_run_id(&reserved) {
            return Err(api_bad_request(format!(
                "fork run id `{reserved}` is not allowed"
            )));
        }
        Ok(reserved)
    }

    pub async fn start_run(&self, req: StartRun) -> Result<String, ApiError> {
        self.start_run_with_id(req, None).await
    }

    pub async fn start_run_with_id(
        &self,
        req: StartRun,
        reserved_run_id: Option<String>,
    ) -> Result<String, ApiError> {
        self.ensure_running()?;
        let mut contract = self.load_generator(&req.generator_id)?;
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
        // H01: shard contention is physical, not a same-run conflict.
        // Distinct ids sharing one of the 64 shards wait cancellably for
        // the shard; the semantic same-run duplicate is decided after
        // acquisition via the adoption/identity checks below. Shutdown
        // during the wait refuses without starting any work (H01-03).
        // The live-hit fast path is re-checked under this lock so a drain
        // racing the check cannot be missed (E05).
        let _admission = match crate::run_dirs::lock_run_admission(&run_dir, &self.inner.shutdown)
            .await
            .map_err(api_internal)?
        {
            Some(lock) => lock,
            None => {
                return Err(ApiError::Unavailable {
                    detail: "server is shutting down".into(),
                });
            }
        };
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
                priority: req.priority.unwrap_or(0),
                parent: None,
                answers: &req.answers,
                confirmations: &req.confirmations,
            };
            if adopt_snapshot.is_empty() {
                let fallback = materialize_adopt_snapshot_for_fallback(&run_dir)?;
                verify_live_admission_from_snapshot(&fallback, &identity)?;
            } else {
                verify_live_admission_from_snapshot(&adopt_snapshot, &identity)?;
            }
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
        // The seed's queued instant rides along so the staged record
        // below never re-scans the journal for it (E03).
        let mut adopted_queued_at = None;
        if adopted {
            match seed_adopted_run_from_snapshot(
                &adopt_snapshot,
                &AdoptedRunIdentity {
                    inputs: &inputs,
                    contract_sha256: &contract.sha256,
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
                    priority,
                    parent: None,
                    answers: &answers,
                    confirmations: &confirmations,
                };
                if adopt_snapshot.is_empty() {
                    let fallback = materialize_adopt_snapshot_for_fallback(&run_dir)?;
                    verify_live_admission_from_snapshot(&fallback, &identity)?;
                } else {
                    verify_live_admission_from_snapshot(&adopt_snapshot, &identity)?;
                }
                return Ok(run_id);
            }
            Err(ProbeRejection::Shutdown) => {
                // A shutdown that landed after preparation must not leave
                // an empty directory behind: only directories this
                // admission created are removed, and the run stays
                // resumable nowhere (it never started) (E04).
                if created_fresh {
                    std::fs::remove_dir_all(&run_dir).map_err(api_internal)?;
                }
                return Err(ApiError::Unavailable {
                    detail: "server is shutting down".into(),
                });
            }
            Err(ProbeRejection::Capacity) => {
                // Only directories this admission created are removed: an
                // adopted directory predates us and always survives (E03).
                if created_fresh {
                    std::fs::remove_dir_all(&run_dir).map_err(api_internal)?;
                }
                return Err(ApiError::Unavailable {
                    detail: format!(
                        "run capacity is exhausted: {} non-terminal runs are already tracked",
                        self.inner.max_tracked_runs
                    ),
                });
            }
        }
        // All per-admission allocations happen after the probe, so rejected
        // admissions allocate nothing (E03).
        let cancellation = CancellationToken::new();
        let task = Arc::new(Mutex::new(None));
        let (events, _) =
            broadcast::channel(self.inner.deployment_policy.live_event_channel_capacity);
        let mut staged = RunRecord {
            contract: contract.clone(),
            contract_sha256: contract.sha256.clone(),
            inputs: inputs.clone(),
            answers: answers.clone(),
            confirmations: confirmations.clone(),
            priority,
            parent_run_id: None,
            preempted: false,
            state: RunStatus::Queued,
            run_dir: run_dir.clone(),
            artifacts: None,
            question: None,
            confirm: None,
            events: events.clone(),
            cancellation: cancellation.clone(),
            task: task.clone(),
            queued_at: None,
            owner_id: self.inner.owner_id.clone(),
            ephemeral: false,
        };
        let effective = crate::types::ResolvedExecutionPolicy::resolve(
            contract.manifest.budget.max_steps,
            self.max_total_steps(),
            self.inner.deployment_policy.max_parallel_steps,
        );
        // Fresh admissions stamp one durable instant explicitly so memory
        // and journal share it without a second scan; adopted retries reuse
        // their seed instant (E03). Fork fresh already does the same.
        let fresh_queued_at = (!adopted).then(chrono::Utc::now);
        // An adopted retry already carries its run_queued event; appending
        // another would fork the journal.
        if !adopted
            && let Err(error) = write_run_event(
                &staged,
                "run_queued",
                json!({
                    "run_id": &run_id,
                    "generator": format!("{}@{}", contract.manifest.generator.id, contract.manifest.generator.version),
                    "generator_path": &contract.root,
                    "contract_sha256": &contract.sha256,
                    "inputs": &inputs,
                    "answers": &answers,
                    "confirmations": &confirmations,
                    "schema_version": api::JOURNAL_SCHEMA_VERSION,
                    "retention_days": contract.manifest.retention.days,
                    "priority": priority,
                    "parent_run_id": Value::Null,
                    "labels": &req.labels,
                    "audit_raise": req.audit_level,
                    "effective_max_total_steps": effective.max_total_steps,
                    "effective_policy_origin": effective.origin,
                    "queued_at": fresh_queued_at.map(|at| at.to_rfc3339()),
                }),
            )
        {
            // `created_fresh` is set only when this admission prepared an
            // empty directory, so removal cannot destroy foreign data (E03).
            // Cleanup failures propagate alongside the write failure instead
            // of leaking the directory silently (E05).
            if created_fresh && let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir) {
                return Err(api_internal(format!(
                    "{error}; failed to clean fresh run directory: {cleanup_error}"
                )));
            }
            return Err(api_internal(error));
        }
        // Memory observes the same durable admission instant as restarts and
        // peers: fresh reuses the explicit instant above, adopted reuses its
        // seed. No second scan exists on either path (E03).
        staged.queued_at = adopted_queued_at.or(fresh_queued_at);
        // Phase 2b: only the inserting admission spawns. A converged rival
        // re-verifies identity and returns: spawning with the loser's
        // staged channel and token would diverge from the registered
        // record, and the owner or the resumer already drives it (E03).
        // The loser's staged channel/token/task are discarded by design
        // (E03): deferring allocation until after register would break the
        // channel-predates-journal invariant for fresh admissions
        // (`write_run_event` needs `record.events`), and only the adopted
        // no-write path could defer — one channel plus one token per rare
        // race is not worth the divergent path.
        match self
            .register_admission(
                run_id.to_string(),
                staged,
                created_fresh.then_some(run_dir.as_path()),
            )
            .await
        {
            Ok(AdmissionVerdict::Converged) => {
                // Reuse the adoption snapshot when it exists; a rival that
                // registered after our adoption read leaves it empty, in
                // which case fall back to a fresh read (E03).
                let identity = AdoptedRunIdentity {
                    inputs: &inputs,
                    contract_sha256: &contract.sha256,
                    priority,
                    parent: None,
                    answers: &answers,
                    confirmations: &confirmations,
                };
                if adopt_snapshot.is_empty() {
                    let fallback = materialize_adopt_snapshot_for_fallback(&run_dir)?;
                    verify_live_admission_from_snapshot(&fallback, &identity)?;
                } else {
                    verify_live_admission_from_snapshot(&adopt_snapshot, &identity)?;
                }
                return Ok(run_id);
            }
            Ok(AdmissionVerdict::Inserted) => {}
            Err(error @ ApiError::Unavailable { .. }) => {
                // A shutdown that landed after our journal write would leave
                // a Queued journal for a request we report as refused, which
                // would then run on the next boot by surprise (E05). Settle
                // our own fresh admission as interrupted when we wrote it.
                if !adopted {
                    Self::settle_refused_admission(&run_dir).await;
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        }
        // Registration hands off directly to spawn_engine_run: a racing
        // resumer either observes the registered record (and its handle)
        // or wins the execution lease first, in which case the loser
        // leaves the shared slot untouched (E03). (The spawn itself may
        // await on lease contention inside; the handoff holds no lock
        // across it.)
        self.clone()
            .spawn_engine_run(SpawnRun {
                run_id: run_id.clone(),
                contract,
                inputs,
                run_dir,
                events,
                answers,
                confirmations,
                // Adopted retries carry the adoption snapshot so the spawn
                // reuses it instead of re-reading the journal; fresh
                // admissions just wrote their run_queued event, so the
                // spawn reads the journal once (E03).
                journal_snapshot: adopted.then(|| adopt_snapshot.events.clone()),
                cancellation,
                task,
            })
            .await;
        self.inner.queue_notify.notify_waiters();
        self.preempt_for_priority(priority).await;
        Ok(run_id.to_string())
    }

    pub async fn fork_run(&self, source_id: &str, request: ForkRun) -> Result<String, ApiError> {
        self.fork_run_with_id(source_id, request, None).await
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
        let mut contract = Contract::load(&generator_path).map_err(api_internal)?;
        contract.apply_audit_floor(self.inner.deployment_policy.audit_floor);
        let run_id = reserved_run_id.unwrap_or_else(|| {
            format!(
                "{}-fork-{}",
                contract.manifest.generator.id,
                uuid::Uuid::now_v7()
            )
        });
        if !is_safe_run_id(&run_id) {
            return Err(api_bad_request(format!("run id `{run_id}` is not allowed")));
        }
        let run_dir = self.inner.runs_dir.join(&run_id);
        // H01: same cancellable shard wait as start admissions (see above).
        let _admission = match crate::run_dirs::lock_run_admission(&run_dir, &self.inner.shutdown)
            .await
            .map_err(api_internal)?
        {
            Some(lock) => lock,
            None => {
                return Err(ApiError::Unavailable {
                    detail: "server is shutting down".into(),
                });
            }
        };
        // Re-checked under the admission lock so a drain racing the first
        // probe cannot be missed (E05).
        // Filesystem preparation happens outside the run-map lock so concurrent
        // runs never block on checkpoint copies, hashing, or journal I/O.
        // An adopted retry skips preparation; its checkpoint copy already
        // completed under the same key and digest. One snapshot serves
        // adoption, fork inputs, the seed, and the queue instant: the fork
        // never scans per field (E03).
        // Same order as start admissions: the adopt snapshot comes first,
        // then the live check reuses it. Live hits still converge before
        // any checkpoint copy: copying first and discarding it on the live
        // return would waste I/O and could mask a
        // live-record-without-journal inconsistency (E03).
        let adopt_snapshot = match crate::run_dirs::try_adopt_run_dir_with_snapshot(
            &run_dir, &run_id,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                match std::fs::symlink_metadata(&run_dir) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        return Err(api_internal(format!(
                            "{error}; run directory `{run_id}` is a symlink; refusing cleanup"
                        )));
                    }
                    Ok(_) => {
                        if let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir) {
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
                return Err(api_internal(error));
            }
        };
        let adopted = adopt_snapshot.adopted;
        let live_hit = self.live_run_record(&run_id).await;
        if live_hit {
            // One helper for every live-hit shape (E03): adopted journals
            // verify against the snapshot above and converge, while a live
            // record with no journal is refused instead of being
            // overwritten by a fork copy.
            resolve_fork_live_hit(
                &run_id,
                &adopt_snapshot,
                adopted,
                &contract,
                source_id,
                &request,
            )?;
            return Ok(run_id);
        }
        if !adopted {
            prepare_checkpoint_fork(
                &source_dir,
                source_id,
                &run_dir,
                &run_id,
                request.at_seq,
                &request.state_patch,
            )
            .map_err(api_internal)?;
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
        let fork_inputs =
            fork_checkpoint_inputs(&fork_snapshot, &contract, source_id, request.at_seq);
        let inputs = match fork_inputs {
            Ok(inputs) => inputs,
            Err(error) => {
                if !adopted {
                    match std::fs::symlink_metadata(&run_dir) {
                        Ok(meta) if meta.file_type().is_symlink() => {
                            return Err(api_internal(format!(
                                "{error}; run directory `{run_id}` is a symlink; refusing cleanup"
                            )));
                        }
                        Ok(_) => {
                            if let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir) {
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
                    priority: request.priority.unwrap_or(0),
                    parent: Some(source_id),
                    answers: &fork_answers,
                    confirmations: &fork_confirmations,
                };
                if fork_snapshot.is_empty() {
                    let fallback = materialize_adopt_snapshot_for_fallback(&run_dir)?;
                    verify_live_admission_from_snapshot(&fallback, &identity)?;
                } else {
                    verify_live_admission_from_snapshot(&fork_snapshot, &identity)?;
                }
                return Ok(run_id);
            }
            Err(ProbeRejection::Shutdown) => {
                // Same ownership rule as capacity below: a checkpoint copy
                // this admission made is removed; an adopted one predates
                // us and always survives (E03/E04).
                if !adopted {
                    std::fs::remove_dir_all(&run_dir).map_err(api_internal)?;
                }
                return Err(ApiError::Unavailable {
                    detail: "server is shutting down".into(),
                });
            }
            Err(ProbeRejection::Capacity) => {
                // Only directories this admission created are removed: an
                // adopted checkpoint copy predates us and always survives
                // (E03).
                if !adopted {
                    std::fs::remove_dir_all(&run_dir).map_err(api_internal)?;
                }
                return Err(ApiError::Unavailable {
                    detail: format!(
                        "run capacity is exhausted: {} non-terminal runs are already tracked",
                        self.inner.max_tracked_runs
                    ),
                });
            }
        }
        // All per-admission allocations happen after the probe, so rejected
        // admissions allocate nothing (E03).
        let cancellation = CancellationToken::new();
        let task = Arc::new(Mutex::new(None));
        let (events, _) =
            broadcast::channel(self.inner.deployment_policy.live_event_channel_capacity);
        let mut staged = RunRecord {
            contract: contract.clone(),
            contract_sha256: contract.sha256.clone(),
            inputs: inputs.clone(),
            answers: fork_answers.clone(),
            confirmations: fork_confirmations.clone(),
            priority: request.priority.unwrap_or(0),
            parent_run_id: Some(source_id.to_string()),
            preempted: false,
            state: RunStatus::Queued,
            run_dir: run_dir.clone(),
            artifacts: None,
            question: None,
            confirm: None,
            events: events.clone(),
            cancellation: cancellation.clone(),
            task: task.clone(),
            queued_at: None,
            owner_id: self.inner.owner_id.clone(),
            ephemeral: false,
        };
        // One durable instant shared by the journal and memory: fresh forks
        // stamp it explicitly so the post-write memory needs no second scan
        // and the single pre-write snapshot stays the only journal read
        // (E03). Adopted retries reuse their seed instant below.
        let fresh_queued_at = (!adopted).then(chrono::Utc::now);
        // Fork admissions record the same effective policy as start
        // admissions so execution reuses the journaled value instead of
        // re-resolving under a possibly changed ceiling (E04).
        let fork_effective = crate::types::ResolvedExecutionPolicy::resolve(
            contract.manifest.budget.max_steps,
            self.max_total_steps(),
            self.inner.deployment_policy.max_parallel_steps,
        );
        // An adopted retry already carries its run_queued event; appending
        // another would fork the journal.
        // KEEP IN SYNC (E03): this durable `run_queued` payload and the
        // synthesized in-memory tail at spawn below must carry identical
        // admission fields (generator/inputs/answers/confirmations/queued_at
        // /effective_*). A future `RunEvent` required field must be added to
        // both; the envelope (t/ts/seq/trace_id/span_id) is synthesis-only
        // (`JournalWriter` supplies it for the durable write).
        if !adopted
            && let Err(error) = write_run_event(
                &staged,
                "run_queued",
                json!({
                    "run_id": &run_id,
                    "generator": format!("{}@{}", contract.manifest.generator.id, contract.manifest.generator.version),
                    "generator_path": &contract.root,
                    "contract_sha256": &contract.sha256,
                    "inputs": &staged.inputs,
                    "answers": &staged.answers,
                    "confirmations": &staged.confirmations,
                    "schema_version": api::JOURNAL_SCHEMA_VERSION,
                    "retention_days": contract.manifest.retention.days,
                    "priority": staged.priority,
                    "parent_run_id": source_id,
                    "queued_at": fresh_queued_at.map(|at| at.to_rfc3339()),
                    "effective_max_total_steps": fork_effective.max_total_steps,
                    "effective_policy_origin": fork_effective.origin,
                }),
            )
        {
            // This write runs only when `!adopted`, so the directory holds
            // nothing but this admission's own checkpoint copy: removal
            // cannot destroy foreign data (E03). Cleanup failures propagate
            // with the write failure instead of leaking silently (E05).
            if let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir) {
                return Err(api_internal(format!(
                    "{error}; failed to clean fork directory: {cleanup_error}"
                )));
            }
            return Err(api_internal(error));
        }
        // Memory observes the same durable admission instant as restarts and
        // peers. Adopted seeds carry their snapshot instant; fresh forks
        // reuse the explicit instant durably written above, so neither path
        // re-scans the journal (E03).
        staged.queued_at = adopted_queued_at.or(fresh_queued_at);
        // Phase 2b: only the inserting admission spawns. A converged rival
        // re-verifies identity and returns without spawning (E03).
        match self
            .register_admission(
                run_id.to_string(),
                staged,
                (!adopted).then_some(run_dir.as_path()),
            )
            .await
        {
            Ok(AdmissionVerdict::Converged) => {
                // Reuse the single admission snapshot when it exists; fall
                // back to a fresh read when a rival registered after our
                // snapshot read left it empty (E03).
                let identity = AdoptedRunIdentity {
                    inputs: &inputs,
                    contract_sha256: &contract.sha256,
                    priority: request.priority.unwrap_or(0),
                    parent: Some(source_id),
                    answers: &fork_answers,
                    confirmations: &fork_confirmations,
                };
                if fork_snapshot.is_empty() {
                    let fallback = materialize_adopt_snapshot_for_fallback(&run_dir)?;
                    verify_live_admission_from_snapshot(&fallback, &identity)?;
                } else {
                    verify_live_admission_from_snapshot(&fork_snapshot, &identity)?;
                }
                return Ok(run_id);
            }
            Ok(AdmissionVerdict::Inserted) => {}
            Err(error @ ApiError::Unavailable { .. }) => {
                if !adopted {
                    Self::settle_refused_admission(&run_dir).await;
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        }
        // Registration hands off directly to spawn_engine_run; the handoff
        // holds no lock across the spawn's internal lease wait (E03).
        let fork_priority = request.priority.unwrap_or(0);
        // Single-read spawn (E03): the admission snapshot above is the only
        // journal read. Adopted retries reuse it directly; fresh forks
        // synthesize their own `run_queued` in-memory (same fields as the
        // durable write above, appended last with the next seq) so the spawn
        // observes the fork's own admission without a second disk read.
        // KEEP IN SYNC (E03): the synthesized admission fields below must
        // match the durable payload above; a future `RunEvent` required
        // field must be added to both.
        // `for_execution` is fork-aware (last `run_queued` after every
        // `run_forked` wins), so the synthesized tail resolves to the fork's
        // ceiling, never the source's. The synthesized event carries a valid
        // `seq`/`ts`/`trace_id`/`span_id` so `RunEvent::from_flat` and the
        // fold accept it exactly like the durable event.
        let spawn_snapshot: Vec<Value> = if adopted {
            fork_snapshot.events.clone()
        } else {
            let mut synthesized = fork_snapshot.events.clone();
            let next_seq = synthesized
                .iter()
                .filter_map(|event| event.get("seq").and_then(Value::as_u64))
                .max()
                .unwrap_or(0)
                .saturating_add(1);
            synthesized.push(json!({
                "t": "run_queued",
                "ts": chrono::Utc::now().to_rfc3339(),
                "seq": next_seq,
                "run_id": run_id.as_str(),
                "trace_id": api::trace_id_for_run(&run_id),
                "span_id": api::span_id_for_seq(next_seq),
                "generator": format!("{}@{}", contract.manifest.generator.id, contract.manifest.generator.version),
                "generator_path": &contract.root,
                "contract_sha256": &contract.sha256,
                "inputs": &inputs,
                "answers": &fork_answers,
                "confirmations": &fork_confirmations,
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "retention_days": contract.manifest.retention.days,
                "priority": fork_priority,
                "parent_run_id": source_id,
                "queued_at": fresh_queued_at.map(|at| at.to_rfc3339()),
                "effective_max_total_steps": fork_effective.max_total_steps,
                "effective_policy_origin": fork_effective.origin,
            }));
            synthesized
        };
        self.clone()
            .spawn_engine_run(SpawnRun {
                run_id: run_id.clone(),
                contract,
                inputs,
                run_dir,
                events,
                answers: fork_answers,
                confirmations: fork_confirmations,
                journal_snapshot: Some(spawn_snapshot),
                cancellation,
                task,
            })
            .await;
        self.inner.queue_notify.notify_waiters();
        self.preempt_for_priority(fork_priority).await;
        Ok(run_id.to_string())
    }
}
