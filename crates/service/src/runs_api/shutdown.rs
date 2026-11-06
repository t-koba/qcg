//! Shutdown operations.
use super::*;

impl LocalService {
    /// Settle task-less or aborted local runs under the execution lease.
    /// Accepted denials win; observation failures preserve nonterminal evidence.
    pub(super) async fn settle_shutdown_without_task(
        &self,
        id: &str,
        record: &RunRecord,
        reason: &str,
    ) -> Result<(), ApiError> {
        // A peer holding the execution lease owns settlement.
        let _lease =
            crate::run_dirs::try_lock_run_execution(&record.run_dir).map_err(api_internal)?;
        if _lease.is_none() {
            return Ok(());
        }
        let state = self.authoritative_state(&record.run_dir).await?;
        if let Some((confirmation, _)) =
            state.confirmations.iter().find(|(_, approved)| !**approved)
        {
            return self
                .reconcile_denial(id, &record.run_dir, confirmation)
                .await;
        }
        // Never settle past an unreadable or conflicting mailbox.
        self.drain_cancel_controls(id, &record.run_dir)
            .await
            .map_err(api_internal)?;
        // Preserve an outcome already settled by another writer.
        let settled = self
            .authoritative_state(&record.run_dir)
            .await?
            .terminal
            .is_some();
        if settled {
            return Ok(());
        }
        // An evicted record still has the same owned directory and identity.
        let settled_now = {
            let runs = self.inner.runs.read().await;
            runs.get(id).cloned().unwrap_or_else(|| record.clone())
        };
        crate::run_dirs::settle_active_elapsed(&settled_now).map_err(api_internal)?;
        write_run_event(
            &settled_now,
            "run_interrupted",
            json!({
                "reason": FailureDetail::new(
                    FailureCode::Interrupted,
                    reason,
                ),
            }),
        )
        .map_err(api_internal)?;
        // Update memory only after durable settlement.
        {
            let mut runs = self.inner.runs.write().await;
            if let Some(record) = runs.get_mut(id) {
                record.state = RunStatus::Interrupted;
                record.clear_prompt();
            }
        }
        Ok(())
    }

    pub async fn shutdown_active_runs(&self) -> Result<(), ApiError> {
        // Standalone calls also stop new admissions before settlement.
        self.mark_shutting_down();
        // Publish before waiting, outside the runs lock, inside the journal lock.
        // Racing fresh admissions settle themselves through the shutdown guard.
        let targets: Vec<(String, Utf8PathBuf)> = {
            let runs = self.inner.runs.read().await;
            runs.iter()
                // Direct placeholders are driven and retired by their inline owner.
                .filter(|(_, record)| !record.state.is_terminal() && !record.ephemeral)
                .map(|(id, record)| (id.clone(), record.run_dir.clone()))
                .collect()
        };
        let mut accepted_targets = Vec::new();
        let mut publication_error = None;
        for (id, run_dir) in targets {
            match self
                .publish_cancel_control(&id, &run_dir, &uuid::Uuid::now_v7().to_string())
                .await
            {
                Ok(Some(_)) => accepted_targets.push((id, run_dir)),
                Ok(None) => {} // Earlier denial settled Failed, not Interrupted.
                Err(error) => {
                    tracing::error!(%error, run_id = %id, "failed to record shutdown cancel signal");
                    publication_error = Some(error);
                }
            }
        }
        let active: Vec<(String, RunRecord)> = {
            let mut runs = self.inner.runs.write().await;
            let mut active = Vec::new();
            for (id, _) in &accepted_targets {
                let Some(record) = runs.get_mut(id) else {
                    continue;
                };
                if record.state.is_terminal() {
                    continue;
                }
                record.cancellation.cancel();
                // Display acceptance until the journal settles the outcome.
                record.state = RunStatus::CancelRequested;
                record.preempted = false;
                record.clear_prompt();
                record.artifacts = None;
                active.push((id.clone(), record.clone()));
            }
            active
        };
        // Wait concurrently. Abort and join overdue writers before settlement.
        let service = self.clone();
        let waits = active.into_iter().map(|(id, record)| {
            let service = service.clone();
            async move {
                let handle_opt = record.take_task_slot();
                let Some(handle) = handle_opt else {
                    service.settle_shutdown_without_task(&id, &record, "service shutdown").await?;
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
                        // The task settled its journal; retire remaining controls.
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
                        // Join the aborted writer before taking its execution lease.
                        handle.abort();
                        let _ = handle.await;
                        service.settle_shutdown_without_task(
                            &id, &record, "shutdown deadline exceeded; task was aborted",
                        ).await?;
                        Ok::<(), ApiError>(())
                    }
                }
            }
        });
        for result in futures_util::future::join_all(waits).await {
            result?;
        }
        if let Some(error) = publication_error {
            return Err(error);
        }
        // Converge late task-less admissions and disk-only orphans.
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
            self.settle_shutdown_without_task(id, record, "service shutdown")
                .await?;
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
        // Disk-only active runs settle under the lease, except never-tracked
        // queued orphans: those remain valid requests for next-boot recovery.
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
            if let Some((confirmation, _)) =
                state.confirmations.iter().find(|(_, approved)| !**approved)
            {
                self.reconcile_denial(&id, &run_dir, confirmation).await?;
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
            self.drain_cancel_controls(&id, &run_dir)
                .await
                .map_err(api_internal)?;
            if self.authoritative_state(&run_dir).await?.terminal.is_some() {
                continue;
            }
            let journal_path = crate::summaries::run_meta_dir(&run_dir).join("journal.jsonl");
            // Disk-only legacy settlement retains its scan limits. Ledger
            // recovery must succeed before appending an interruption.
            let journal = engine::JournalWriter::create_with_limits(
                &journal_path,
                &id,
                false,
                None,
                engine::JournalLimits::default(),
            )
            .map_err(api_internal)?;
            engine::settle_active_elapsed(&journal, chrono::Utc::now()).map_err(api_internal)?;
            journal
                .event(
                    "run_interrupted",
                    serde_json::json!({
                        "reason": {"code": "interrupted", "message": "service shutdown"},
                    }),
                )
                .map_err(api_internal)?;
        }
        // Dropped guards may still own container cleanups; await their results.
        let cleanup =
            container::await_outstanding_cleanups(std::time::Duration::from_secs(65)).await;
        if cleanup.outstanding > 0 || cleanup.failed > 0 {
            // Thread completion alone does not prove resource cleanup.
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
