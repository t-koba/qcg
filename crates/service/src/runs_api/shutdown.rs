//! Shutdown operations.
use super::*;

impl LocalService {
    /// Settles a run with no live local engine task as interrupted during
    /// shutdown: lease-gated drain, no double terminal, memory retired.
    /// Shared by the task-less memory path, the aborted-task path, and the
    /// disk-only orphan pass below (E05).
    pub(super) async fn settle_shutdown_without_task(
        &self,
        id: &str,
        record: &RunRecord,
    ) -> Result<(), ApiError> {
        // No local engine task (queued/waiting): settle durably
        // only while holding the execution lease. A contended
        // lease means a peer owns execution and settles it.
        let _lease =
            crate::run_dirs::try_lock_run_execution(&record.run_dir).map_err(api_internal)?;
        if _lease.is_none() {
            return Ok(());
        }
        // Shutdown settles the terminal event first; a drain
        // failure is logged and the retained mailbox converges
        // on the next drain instead of blocking shutdown.
        if let Err(error) = self.drain_cancel_controls(id, &record.run_dir).await {
            tracing::warn!(run_id = %id, %error, "cancel drain failed during shutdown; mailbox retained");
        }
        // A writer may have settled first (e.g. the queued
        // finalizer): never journal a second terminal outcome
        // over it.
        let settled = self
            .authoritative_state(&record.run_dir)
            .await?
            .terminal
            .is_some();
        if settled {
            return Ok(());
        }
        // Prefer the live record; fall back to the owned
        // pre-shutdown copy, which carries the same run
        // directory and id the event needs. An eviction in
        // between must not fail the settlement (E05).
        let settled_now = {
            let runs = self.inner.runs.read().await;
            runs.get(id).cloned().unwrap_or_else(|| record.clone())
        };
        write_run_event(
            &settled_now,
            "run_interrupted",
            json!({
                "reason": FailureDetail::new(
                    FailureCode::Interrupted,
                    "service shutdown",
                ),
            }),
        )
        .map_err(api_internal)?;
        // Settlement journaled the terminal outcome: retire the
        // acceptance display into the settled state.
        {
            let mut runs = self.inner.runs.write().await;
            if let Some(record) = runs.get_mut(id) {
                record.state = RunStatus::Interrupted;
                record.question = None;
                record.confirm = None;
            }
        }
        Ok(())
    }

    pub async fn shutdown_active_runs(&self) -> Result<(), ApiError> {
        // Own the shutdown state even for standalone calls: the serve path
        // calls `mark_shutting_down` first, but a direct call must also
        // stop new execution sources before settling. Cancelling an
        // already-cancelled token is a no-op, so this is idempotent (E05).
        self.mark_shutting_down();
        // Phase 1: durable mailbox signal + local cancellation first without
        // awaiting any task, so one unresponsive executor cannot block the
        // deadline for others. Never append to the journal while an engine
        // task is live (A01/A09). The runs lock is never held across
        // filesystem I/O: targets snapshot under a brief read, mailbox
        // writes run lock-free, and memory converges under a second brief
        // write, so lock hold time no longer scales with run count (E05).
        // Admissions racing the snapshot are refused by the shutdown guard
        // at probe and registration: fresh ones settle their own journal
        // as interrupted via settle_refused_admission, so no new Queued
        // journal can appear after the snapshot except pre-existing
        // adopted orphans, which correctly resume on the next boot (E05).
        let targets: Vec<(String, Utf8PathBuf)> = {
            let runs = self.inner.runs.read().await;
            runs.iter()
                // Ephemeral direct placeholders are driven inline by their
                // owner and removed on completion; shutdown must not journal
                // into their metadata dirs (E05).
                .filter(|(_, record)| !record.state.is_terminal() && !record.ephemeral)
                .map(|(id, record)| (id.clone(), record.run_dir.clone()))
                .collect()
        };
        for (id, run_dir) in &targets {
            if let Err(error) =
                crate::run_dirs::request_remote_cancel(run_dir, id, &self.inner.owner_id)
            {
                tracing::error!(%error, run_id = %id, "failed to record shutdown cancel signal");
            }
        }
        let active: Vec<(String, RunRecord)> = {
            let mut runs = self.inner.runs.write().await;
            let mut active = Vec::new();
            for (id, _) in &targets {
                let Some(record) = runs.get_mut(id) else {
                    continue;
                };
                if record.state.is_terminal() {
                    continue;
                }
                record.cancellation.cancel();
                // Acceptance, not settlement: the journal settles each run
                // as `Interrupted` below, and the display must not claim
                // `Canceled` before that terminal outcome exists (A09).
                record.state = RunStatus::CancelRequested;
                record.preempted = false;
                record.question = None;
                record.confirm = None;
                record.artifacts = None;
                active.push((id.clone(), record.clone()));
            }
            active
        };
        // Phase 2: wait concurrently with a shared per-run deadline, so N
        // stuck runs still converge in about 5 seconds instead of 5 x N.
        // Expired waits abort the task and await its exit so no detached
        // writer survives to race with settlement (A09). Settlement appends
        // only after the task handle has completed.
        let service = self.clone();
        let waits = active.into_iter().map(|(id, record)| {
            let service = service.clone();
            async move {
                let handle_opt = record
                    .task
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                let Some(handle) = handle_opt else {
                    service.settle_shutdown_without_task(&id, &record).await?;
                    return Ok::<(), ApiError>(());
                };
                let mut handle = handle;
                tokio::select! {
                    result = &mut handle => {
                        result.map_err(|error| {
                            api_internal(format!(
                                "run `{id}` task failed during shutdown: {error}"
                            ))
                        })?;
                        // The finished task settles the journal itself; drain
                        // only the mailbox so no control file leaks into the
                        // next boot (A02).
                        let _lease = crate::run_dirs::try_lock_run_execution(&record.run_dir)
                            .map_err(api_internal)?;
                        if _lease.is_some()
                            && let Err(error) =
                                service.drain_cancel_controls(&id, &record.run_dir).await
                        {
                            tracing::warn!(run_id = %id, %error, "cancel drain failed during shutdown; mailbox retained");
                        }
                        Ok::<(), ApiError>(())
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                        tracing::warn!(run_id = %id, "run did not stop within shutdown deadline; aborting task");
                        // Keep handle ownership across the deadline so abort
                        // actually stops the task instead of detaching a live
                        // writer (A09).
                        handle.abort();
                        let _ = handle.await;
                        // Settle only while holding the execution lease: a
                        // peer may have taken ownership in the meantime, in
                        // which case it settles and we must not append after
                        // its terminal events.
                        let _lease = crate::run_dirs::try_lock_run_execution(&record.run_dir)
                            .map_err(api_internal)?;
                        if _lease.is_none() {
                            tracing::warn!(run_id = %id, "execution moved elsewhere during shutdown; skipping settlement");
                            return Ok::<(), ApiError>(());
                        }
                        if let Err(error) =
                            service.drain_cancel_controls(&id, &record.run_dir).await
                        {
                            tracing::warn!(run_id = %id, %error, "cancel drain failed during shutdown; mailbox retained");
                        }
                        // A writer may have settled first: never journal a
                        // second terminal outcome over it.
                        let settled = service.authoritative_state(&record.run_dir).await?.terminal.is_some();
                        if settled {
                            return Ok::<(), ApiError>(());
                        }
                        // Prefer the live record; fall back to the owned
                        // pre-shutdown copy, which carries the same run
                        // directory and id the event needs. An eviction in
                        // between must not fail the settlement (E05).
                        let settled_now = {
                            let runs = service.inner.runs.read().await;
                            runs.get(&id).cloned().unwrap_or_else(|| record.clone())
                        };
                        write_run_event(
                            &settled_now,
                            "run_interrupted",
                            json!({
                                "reason": FailureDetail::new(
                                    FailureCode::Interrupted,
                                    "shutdown deadline exceeded; task was aborted",
                                ),
                            }),
                        )
                        .map_err(api_internal)?;
                        // Settlement journaled the terminal outcome: retire
                        // the acceptance display into the settled state.
                        {
                            let mut runs = service.inner.runs.write().await;
                            if let Some(record) = runs.get_mut(&id) {
                                record.state = RunStatus::Interrupted;
                                record.question = None;
                                record.confirm = None;
                            }
                        }
                        Ok::<(), ApiError>(())
                    }
                }
            }
        });
        for result in futures_util::future::join_all(waits).await {
            result?;
        }
        // Second pass: admissions (answer/confirm requeue, direct inline
        // registration) that landed after the snapshot converge here instead
        // of escaping shutdown into a surprise next-boot resume (E05).
        // Disk-only non-terminal journals with no memory record converge too
        // when the lease is free; a contended lease means a peer settles.
        let late: Vec<(String, RunRecord)> = {
            let runs = self.inner.runs.read().await;
            runs.iter()
                .filter(|(_, record)| {
                    !record.state.is_terminal()
                        && !record.ephemeral
                        && !crate::types::task_slot_is_live(&record.task)
                })
                .map(|(id, record)| (id.clone(), record.clone()))
                .collect()
        };
        for (id, record) in &late {
            self.settle_shutdown_without_task(id, record).await?;
        }
        let disk_orphans = tokio::task::spawn_blocking({
            let runs_dir = self.inner.runs_dir.clone();
            move || -> Vec<(String, Utf8PathBuf)> {
                let mut orphans = Vec::new();
                let Ok(read_dir) = std::fs::read_dir(&runs_dir) else {
                    return orphans;
                };
                for entry in read_dir.flatten() {
                    let Ok(run_dir) = Utf8PathBuf::from_path_buf(entry.path()) else {
                        continue;
                    };
                    let Some(run_id) = run_dir.file_name().map(str::to_string) else {
                        continue;
                    };
                    if run_id == "idempotency" || run_id.starts_with('.') {
                        continue;
                    }
                    orphans.push((run_id, run_dir));
                }
                orphans
            }
        })
        .await
        .map_err(api_internal)?;
        // Disk-only journals with no memory record and no terminal outcome
        // settle here when the lease is free, so shutdown converges them
        // instead of leaving them for a surprise next-boot resume while
        // memory-tracked runs all report Interrupted (E05). Settlement
        // writes the terminal event directly: no memory record exists to
        // retire.
        // Exception (Q3, docs/operations 447): never-tracked Queued
        // orphans (no execution ever started, no pending prompt, no cancel
        // acceptance, no terminal) are LEFT for the next boot instead of
        // being interrupted here. Interrupting them would destroy a valid
        // queued request this process never owned; the next boot's resume
        // (disk scan for untracked Queued) picks them up. All other
        // disk-only states (Waiting/Confirming/Running/CancelRequested)
        // still settle as Interrupted under the lease (single-peer
        // enforcement via the lease gate below).
        for (id, run_dir) in disk_orphans {
            if self.inner.runs.read().await.contains_key(&id) {
                continue;
            }
            let state = match self.authoritative_state(&run_dir).await {
                Ok(state) => state,
                Err(_) => continue,
            };
            if state.terminal.is_some() {
                continue;
            }
            // Never-tracked Queued exception: leave for next-boot resume.
            if !state.execution_started && state.pending.is_none() && !state.cancel_requested {
                tracing::info!(run_id = %id, "shutdown leaves never-tracked queued orphan for next-boot resume");
                continue;
            }
            let lease = crate::run_dirs::try_lock_run_execution(&run_dir).map_err(api_internal)?;
            if lease.is_none() {
                continue;
            }
            if let Err(error) = self.drain_cancel_controls(&id, &run_dir).await {
                tracing::warn!(run_id = %id, %error, "cancel drain failed during shutdown; mailbox retained");
            }
            if self.authoritative_state(&run_dir).await?.terminal.is_some() {
                continue;
            }
            let journal = crate::summaries::run_meta_dir(&run_dir).join("journal.jsonl");
            engine::JournalWriter::append_single_event(
                &journal,
                &id,
                "run_interrupted",
                serde_json::json!({
                    "reason": {"code": "interrupted", "message": "service shutdown"},
                }),
                engine::JournalLimits::default(),
                None,
            )
            .map_err(api_internal)?;
        }
        // Detached container cleanups from dropped guards must finish
        // before shutdown reports done; otherwise "stopped" races orphaned
        // instances still being torn down. A nonzero remainder or failure
        // count is surfaced, never silently equated with a clean stop:
        // a finished cleanup thread alone does not prove its instance is
        // gone (C05).
        let cleanup =
            container::await_outstanding_cleanups(std::time::Duration::from_secs(65)).await;
        if cleanup.outstanding > 0 || cleanup.failed > 0 {
            // A finished cleanup thread does not prove its instance is
            // gone: surface the incomplete teardown instead of reporting a
            // clean stop (C05/E05).
            return Err(ApiError::Internal {
                detail: format!(
                    "container cleanups outstanding or failed past shutdown deadline: {} outstanding, {} failed",
                    cleanup.outstanding, cleanup.failed
                ),
            });
        }
        Ok(())
    }
}
