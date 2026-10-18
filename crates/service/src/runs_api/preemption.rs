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
            let occupied = runs
                .values()
                .filter(|record| record.state == RunStatus::Running && !record.ephemeral)
                .count();
            let running = runs
                .iter()
                .filter(|(_, record)| {
                    record.state == RunStatus::Running && !record.ephemeral && !record.preempted
                })
                .map(|(run_id, record)| (run_id.as_str(), record.priority))
                .collect::<Vec<_>>();
            crate::queue::select_preemption_victim(
                &running,
                occupied,
                self.inner.max_active_runs,
                priority,
            )
        };
        if let Some(victim) = victim {
            let service = self.clone();
            // The rendezvous must survive an arriving client's disconnect;
            // otherwise a yielded victim could be left without a requeue owner.
            let result = tokio::spawn(async move {
                if let Err(error) = service.preempt_run(&victim).await {
                    tracing::warn!(%error, run_id = %victim, "priority preemption failed");
                }
            })
            .await;
            if let Err(error) = result {
                tracing::error!(%error, "priority preemption rendezvous task failed");
            }
        }
    }

    pub(super) async fn preempt_run(&self, id: &str) -> Result<(), ApiError> {
        let boundary =
            self.inner.deployment_policy.preemption_mode == policy::PreemptionMode::StepBoundary;
        let (handle, completion, requeue) = {
            let mut runs = self.inner.runs.write().await;
            let Some(record) = runs.get_mut(id) else {
                return Err(api_not_found(format!("run `{id}` was not found")));
            };
            if record.state != RunStatus::Running || record.preempted {
                return Ok(());
            }
            // Observe this exact task without taking cancellation/shutdown's
            // join handle. The shared slot can be taken or replaced by a
            // concurrent cancel/HITL resume; it is not a completion witness.
            let completion = if boundary {
                let slot = record.task.lock().unwrap_or_else(PoisonError::into_inner);
                let Some(handle) = slot.as_ref() else {
                    return Ok(());
                };
                Some(handle.abort_handle())
            } else {
                None
            };
            let requeue = RunRecord {
                contract: record.contract.clone(),
                contract_sha256: record.contract_sha256.clone(),
                package_sha256: record.package_sha256.clone(),
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
                scheduler_yield: CancellationToken::new(),
                task: Arc::clone(&record.task),
                // Keep the durable key while the old writer drains. Only
                // scheduler preemption may carry it into a new queue episode.
                queued_at: record.queued_at,
                owner_id: self.inner.owner_id.clone(),
                ephemeral: false,
            };
            record.preempted = true;
            let handle = if self.inner.deployment_policy.preemption_mode
                == policy::PreemptionMode::Interrupt
            {
                record.cancellation.cancel();
                record.state = RunStatus::Queued;
                record.question = None;
                record.confirm = None;
                record.artifacts = None;
                record
                    .task
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
            } else {
                record.scheduler_yield.cancel();
                // Leave the handle installed so user cancellation and shutdown
                // retain their bounded-stop/abort authority while we drain.
                None
            };
            (handle, completion, requeue)
        };
        let staged = requeue;
        if let Some(completion) = completion {
            // No timeout-to-interrupt fallback: only user cancellation,
            // shutdown, or the run's own budgets may interrupt started work.
            while !completion.is_finished() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        // Single-writer rule: wait for the preempted engine task and its
        // journal writer to exit before appending requeue state. Writing
        // run_queued earlier races with cancel-time events from the old
        // writer, producing duplicate seq and last-writer-wins state.json.
        // Interrupt mode alone uses the bounded abort fallback. Boundary
        // mode above never turns a stop timeout into an implicit interrupt.
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
        // Hold the execution lease across requeue. A resumer or peer must
        // not start a writer between the rendezvous and the durable event.
        // Contention can be transient during an owner handoff; once the old
        // task has yielded, never abandon its durable requeue on WouldBlock.
        let Some(lease) =
            crate::run_dirs::lock_run_execution(&staged.run_dir, &self.inner.shutdown)
                .await
                .map_err(api_internal)?
        else {
            tracing::info!(run_id = %id, "preemption requeue stopped during service shutdown");
            return Ok(());
        };
        if !boundary {
            truncate_trailing_canceled_events(&staged.run_dir).map_err(api_internal)?;
        }
        // The task is gone and any trailing canceled events have been
        // truncated. Close a forced-abort episode at this known handoff so
        // time spent waiting in the queue is never charged as active work.
        crate::run_dirs::settle_active_elapsed(&staged).map_err(api_internal)?;
        // Preemption requeues preserve the journaled effective policy:
        // re-resolving under a changed ceiling would diverge admission from
        // execution (E04). An unreadable journal fails the requeue instead
        // of silently running under the current ceiling (E04).
        let prior_events =
            crate::summaries::read_journal_events(&staged.run_dir).map_err(api_internal)?;
        let state = engine::RunState::fold_values(&prior_events).map_err(api_internal)?;
        if boundary {
            let mut runs = self.inner.runs.write().await;
            let Some(record) = runs.get_mut(id) else {
                return Ok(());
            };
            // A concurrent cancel, completion, or HITL settlement wins.
            if record.state != RunStatus::Running
                || !record.preempted
                || state.terminal.is_some()
                || state.pending.is_some()
            {
                return Ok(());
            }
            let last = prior_events
                .last()
                .and_then(|event| event.get("t"))
                .and_then(Value::as_str);
            if last != Some("run_yielded") {
                return Err(api_internal(
                    "boundary preemption refused: task exited without a durable yield",
                ));
            }
            record.state = RunStatus::Queued;
        }
        let requeue_effective = crate::types::ResolvedExecutionPolicy::for_execution(
            &prior_events,
            self.inner.deployment_policy.max_parallel_steps,
        )
        .map_err(|error| api_internal(format!("preemption requeue refused: {error}")))?;
        let mut payload = json!({
            "run_id": id,
            "generator": format!("{}@{}", staged.contract.manifest.generator.id, staged.contract.manifest.generator.version),
            "generator_path": read_run_generator_path(&staged.run_dir).map_err(api_internal)?,
            "package_sha256": &staged.package_sha256,
            "contract_sha256": &staged.contract.sha256,
            "inputs": &staged.inputs,
            "answers": &staged.answers,
            "confirmations": &staged.confirmations,
            "schema_version": api::JOURNAL_SCHEMA_VERSION,
            "elapsed_budget_mode": state.budget.elapsed_budget_mode,
            "retention_days": staged.contract.manifest.retention.days,
            "priority": staged.priority,
            "parent_run_id": staged.parent_run_id.clone(),
            "effective_max_total_steps": requeue_effective.max_total_steps,
            "effective_policy_origin": requeue_effective.origin,
        });
        if self.inner.deployment_policy.preemption_queue_position
            == policy::PreemptionQueuePosition::Preserve
            && let Some(queued_at) = staged.queued_at
        {
            payload["queued_at"] = json!(queued_at.to_rfc3339());
        }
        // Tail omits the override: the writer's real event ts is the new
        // FIFO key. Preserve never backdates that event timestamp.
        write_run_event(&staged, "run_queued", payload).map_err(api_internal)?;
        let mut staged = staged;
        // Memory observes the same durable FIFO key as restarts.
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
        drop(lease);
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
