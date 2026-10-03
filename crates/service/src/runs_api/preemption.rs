//! Preemption operations.
use super::*;

impl LocalService {
    /// Preempt one running run for an incoming higher-priority run. The victim
    /// keeps its journal and returns to Queued; already finished steps replay
    /// on resume. At most one victim per arrival; equal priorities never
    /// preempt each other.
    pub(super) async fn preempt_for_priority(&self, priority: i32) {
        if !self.preemption_enabled() {
            return;
        }
        let victim = {
            let runs = self.inner.runs.read().await;
            let running = runs
                .iter()
                .filter(|(_, record)| record.state == RunStatus::Running && !record.ephemeral)
                .map(|(run_id, record)| (run_id.as_str(), record.priority))
                .collect::<Vec<_>>();
            crate::queue::select_preemption_victim(&running, self.inner.max_active_runs, priority)
        };
        if let Some(victim) = victim
            && let Err(error) = self.preempt_run(&victim).await
        {
            tracing::warn!(%error, run_id = %victim, "priority preemption failed");
        }
    }

    pub(super) async fn preempt_run(&self, id: &str) -> Result<(), ApiError> {
        let (handle, requeue) = {
            let mut runs = self.inner.runs.write().await;
            let Some(record) = runs.get_mut(id) else {
                return Err(api_not_found(format!("run `{id}` was not found")));
            };
            if record.state != RunStatus::Running {
                return Ok(());
            }
            let requeue = RunRecord {
                contract: record.contract.clone(),
                contract_sha256: record.contract_sha256.clone(),
                inputs: record.inputs.clone(),
                answers: record.answers.clone(),
                confirmations: record.confirmations.clone(),
                priority: record.priority,
                parent_run_id: record.parent_run_id.clone(),
                preempted: false,
                state: RunStatus::Queued,
                run_dir: record.run_dir.clone(),
                artifacts: None,
                question: None,
                confirm: None,
                events: record.events.clone(),
                cancellation: CancellationToken::new(),
                task: Arc::clone(&record.task),
                queued_at: None,
                owner_id: self.inner.owner_id.clone(),
                ephemeral: false,
            };
            record.cancellation.cancel();
            record.state = RunStatus::Queued;
            record.question = None;
            record.confirm = None;
            record.artifacts = None;
            record.preempted = true;
            let handle = record
                .task
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            (handle, requeue)
        };
        let staged = requeue;
        // Single-writer rule: wait for the preempted engine task and its
        // journal writer to exit before appending requeue state. Writing
        // run_queued earlier races with cancel-time events from the old
        // writer, producing duplicate seq and last-writer-wins state.json.
        // Bounded with abort like cancel and shutdown so a stuck executor
        // cannot wedge preemption or leak a detached writer.
        if let Some(handle) = handle {
            let mut handle = handle;
            tokio::select! {
                result = &mut handle => {
                    result.map_err(|error| {
                        api_internal(format!("run `{id}` task failed during preemption: {error}"))
                    })?;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    tracing::warn!(run_id = %id, "run did not stop within preemption deadline; aborting task");
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }
        truncate_trailing_canceled_events(&staged.run_dir).map_err(api_internal)?;
        // Preemption requeues preserve the journaled effective policy:
        // re-resolving under a changed ceiling would diverge admission from
        // execution (E04). An unreadable journal fails the requeue instead
        // of silently running under the current ceiling (E04).
        let prior_events =
            crate::summaries::read_journal_events(&staged.run_dir).map_err(api_internal)?;
        let requeue_effective = crate::types::ResolvedExecutionPolicy::for_execution(
            &prior_events,
            self.inner.deployment_policy.max_parallel_steps,
        )
        .map_err(|error| api_internal(format!("preemption requeue refused: {error}")))?;
        write_run_event(
            &staged,
            "run_queued",
            json!({
                "run_id": id,
                "generator": format!("{}@{}", staged.contract.manifest.generator.id, staged.contract.manifest.generator.version),
                "generator_path": &staged.contract.root,
                "contract_sha256": &staged.contract.sha256,
                "inputs": &staged.inputs,
                "answers": &staged.answers,
                "confirmations": &staged.confirmations,
                "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "retention_days": staged.contract.manifest.retention.days,
                "priority": staged.priority,
                "parent_run_id": staged.parent_run_id.clone(),
                "effective_max_total_steps": requeue_effective.max_total_steps,
                "effective_policy_origin": requeue_effective.origin,
            }),
        )
        .map_err(api_internal)?;
        let mut staged = staged;
        // Memory observes the same durable requeue instant as restarts.
        staged.queued_at = crate::summaries::read_last_queued_at(&staged.run_dir);
        let resume = {
            let mut runs = self.inner.runs.write().await;
            match runs.get_mut(id) {
                // A concurrent cancel() or delete wins over the resume.
                Some(record) if record.state == RunStatus::Queued && record.preempted => {
                    *record = staged.clone();
                    true
                }
                _ => false,
            }
        };
        if resume {
            let runs = self.inner.runs.read().await;
            let Some(record) = runs.get(id) else {
                return Ok(());
            };
            self.clone()
                .spawn_engine_run(SpawnRun {
                    run_id: id.to_string(),
                    contract: record.contract.clone(),
                    inputs: record.inputs.clone(),
                    run_dir: record.run_dir.clone(),
                    events: record.events.clone(),
                    answers: record.answers.clone(),
                    confirmations: record.confirmations.clone(),
                    // The requeue event was just journaled above: no
                    // admission snapshot exists, so the spawn reads the
                    // journal once (E03).
                    journal_snapshot: None,
                    cancellation: record.cancellation.clone(),
                    task: Arc::clone(&record.task),
                })
                .await;
        }
        self.inner.queue_notify.notify_waiters();
        Ok(())
    }
}
