//! Cancellation operations.
use super::*;

impl LocalService {
    pub async fn cancel(&self, id: String) -> Result<(), ApiError> {
        // A cancel accepted during the shutdown drain would race the
        // interruption settlement and journal a competing terminal event;
        // refuse it like every other mutating operation (E05). Terminal
        // runs stay idempotent under shutdown: re-cancelling a settled run
        // reports success without journaling (E05).
        if self.is_shutting_down()
            && let Ok(run_dir) = self.run_dir_for(&id).await
            && self
                .authoritative_state(&run_dir)
                .await
                .map(|state| state.terminal.is_some())
                .unwrap_or(false)
        {
            return Ok(());
        }
        self.ensure_running()?;
        // Durable cross-process cancel mailbox first so a peer owner observes
        // the request even when this process tracks no local task (A02).
        // Control-file creation is fail-closed: I/O errors are reported
        // instead of silently dropping the cancel request (A01).
        let run_dir = self.run_dir_for(&id).await?;
        let cancel_operation =
            crate::run_dirs::request_remote_cancel(&run_dir, &id, &self.inner.owner_id)
                .map_err(api_internal)?;
        let (task, settled) = {
            let mut runs = self.inner.runs.write().await;
            let Some(record) = runs.get_mut(&id) else {
                // No local record, but the durable mailbox already carries
                // the cancel to the owning peer (A02). Report success so a
                // non-tracking peer can still stop a shared run, even under
                // shutdown: no local journal race exists here (E05).
                return Ok(());
            };
            if record.state.is_terminal() {
                // The mailbox file just created above would otherwise linger
                // with no engine left to drain it, pinning has_pending true
                // and blocking deletion (E03). Withdraw it: the run already
                // settled, so there is nothing to cancel. Terminal cancels
                // stay idempotent even under shutdown (E05).
                if !crate::run_dirs::consume_cancel_control(&run_dir, &cancel_operation) {
                    tracing::warn!(run_id = %id, "terminal cancel left a phantom cancel control");
                }
                return Ok(());
            }
            // Re-check under the runs lock: a shutdown that started between
            // the outer gate and here must not journal a competing cancel
            // against the interruption settlement (E05). The just-written
            // mailbox file is withdrawn so no phantom cancel outlives the
            // refusal (E04).
            if self.is_shutting_down() {
                if !crate::run_dirs::consume_cancel_control(&run_dir, &cancel_operation) {
                    tracing::warn!(run_id = %id, "shutdown refusal left a phantom cancel control");
                }
                return Err(ApiError::Unavailable {
                    detail: "server is shutting down".into(),
                });
            }
            // A live engine task is the journal writer even while parked in
            // Waiting/Confirming, and it holds the execution lease for the
            // whole run. Rendezvous with it instead of racing its lease:
            // probing the lease here would report a locally owned run as
            // executing elsewhere whenever cancel lands before an answer.
            // Liveness uses the shared slot predicate: a parked finished
            // handle no longer owns execution (E12).
            let engine_is_active = crate::types::task_slot_is_live(&record.task);
            record.cancellation.cancel();
            // Acceptance, not settlement: the mailbox carries the request
            // and only a journaled terminal state reports `Canceled` (A02).
            record.state = RunStatus::CancelRequested;
            record.preempted = false;
            record.question = None;
            record.confirm = None;
            record.artifacts = None;
            if !engine_is_active {
                let settled = record.clone();
                (None, Some(settled))
            } else {
                // Single-writer rule: never append while the engine task is
                // live. The owner task drains the mailbox and settles the
                // journal after exiting (A01).
                let task = record
                    .task
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                (task, None)
            }
        };
        if let Some(settled) = settled {
            // No local engine task: settle here unless a peer owns
            // execution, in which case the durable mailbox already carries
            // this cancel there.
            if !self.settle_canceled_here(&id, &settled).await? {
                return Err(ApiError::Conflict {
                    detail: format!("run `{id}` is executing elsewhere; cancel was signaled"),
                });
            }
            return Ok(());
        }
        if let Some(task) = task {
            // Bounded wait with abort, mirroring shutdown: a stuck executor
            // must not wedge cancellation, and a detached writer must never
            // survive to race settlement.
            let mut task = task;
            tokio::select! {
                result = &mut task => {
                    result.map_err(|error| {
                        api_internal(format!(
                            "run `{id}` task failed during cancellation: {error}"
                        ))
                    })?;
                    // The writer exited: when it settled the journal itself,
                    // only drain the mailbox. Queued or parked legs can
                    // return without a terminal event; settle here under the
                    // freed lease instead of losing the cancellation (the
                    // mailbox alone never marks the run Canceled).
                    let terminal = {
                        let runs = self.inner.runs.read().await;
                        runs.get(&id).is_some_and(|record| record.state.is_terminal())
                    };
                    if terminal {
                        // The finished task settles the journal itself; drain
                        // only the mailbox here so no control file leaks when
                        // the task exited without draining (A02). Settlement
                        // stays with the task to avoid a second terminal event.
                        let run_dir = self.run_dir_for(&id).await?;
                        let _lease = crate::run_dirs::try_lock_run_execution(&run_dir)
                            .map_err(api_internal)?;
                        if _lease.is_some() {
                            self.drain_cancel_controls(&id, &run_dir)
                                .await
                                .map_err(api_internal)?;
                        }
                    } else {
                        let fallback = {
                            let runs = self.inner.runs.read().await;
                            runs.get(&id).cloned()
                        };
                        let Some(fallback) = fallback else {
                            return Err(api_internal(format!(
                                "run `{id}` vanished during cancellation"
                            )));
                        };
                        if !self.settle_canceled_here(&id, &fallback).await? {
                            return Err(ApiError::Conflict {
                                detail: format!(
                                    "run `{id}` is executing elsewhere; cancel was signaled"
                                ),
                            });
                        }
                        return Ok(());
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    tracing::warn!(run_id = %id, "run did not stop within cancel deadline; aborting task");
                    task.abort();
                    let _ = task.await;
                    // The aborted task never reaches its own settlement, so
                    // settle here: without a terminal event a restart would
                    // resume a canceled run. Lease-gated; a peer owner
                    // settles instead when it holds execution.
                    let run_dir = self.run_dir_for(&id).await?;
                    let lease = crate::run_dirs::try_lock_run_execution(&run_dir)
                        .map_err(api_internal)?;
                    let Some(lease) = lease else {
                        return Err(ApiError::Conflict {
                            detail: format!("run `{id}` is executing elsewhere; cancel was signaled"),
                        });
                    };
                    let settled_now = {
                        let runs = self.inner.runs.read().await;
                        runs.get(&id).cloned().ok_or_else(|| {
                            api_internal(format!("run `{id}` vanished during cancellation"))
                        })?
                    };
                    // The aborted task may have settled through the queued
                    // finalizer first: reuse the terminal-checked settlement
                    // instead of journaling a second terminal outcome.
                    // G01: the lease above is held across settlement, so
                    // settle under it instead of re-acquiring through a
                    // second open (which `flock` reports as a peer
                    // conflict against our own first fd).
                    if !self
                        .settle_canceled_under_lease(&id, &settled_now, &run_dir, &lease)
                        .await?
                    {
                        return Err(ApiError::Conflict {
                            detail: format!("run `{id}` is executing elsewhere; cancel was signaled"),
                        });
                    }
                }
            }
        }
        self.inner.queue_notify.notify_waiters();
        Ok(())
    }

    /// Journal the terminal `run_canceled` settlement in this process.
    /// The caller must have rendezvoused with any local engine task, so no
    /// live writer in this process remains. Holds the execution lease
    /// across drain and settlement; when a peer owns execution it settles
    /// instead and the durable mailbox already carries this cancel there.
    /// Returns Ok(true) once this process owns settlement.
    pub(super) async fn settle_canceled_here(
        &self,
        id: &str,
        fallback: &RunRecord,
    ) -> Result<bool, ApiError> {
        let run_dir = self.run_dir_for(id).await?;
        let lease = crate::run_dirs::try_lock_run_execution(&run_dir).map_err(api_internal)?;
        let Some(lease) = lease else {
            return Ok(false);
        };
        self.settle_canceled_under_lease(id, fallback, &run_dir, &lease)
            .await
    }

    /// Settlement body with the execution lease already held (G01): the
    /// forced-cancel abort path owns the lease across the abort and must
    /// not re-acquire it through a second open (same-process `flock` on a
    /// fresh fd reports `WouldBlock` against our own first fd and would
    /// misreport self-contention as a peer conflict). Callers that do not
    /// hold the lease use [`Self::settle_canceled_here`], which acquires
    /// it and delegates here so the drain-plus-journal sequence lives in
    /// exactly one place.
    pub(super) async fn settle_canceled_under_lease(
        &self,
        id: &str,
        fallback: &RunRecord,
        run_dir: &camino::Utf8PathBuf,
        _lease: &std::fs::File,
    ) -> Result<bool, ApiError> {
        // No live engine writer, so settling here is safe. Drain the
        // mailbox to a single journal event first, then record cancel.
        // A failed drain aborts settlement instead of dropping the
        // cancel request.
        self.drain_cancel_controls(id, run_dir)
            .await
            .map_err(api_internal)?;
        // A terminal event may already exist (a writer settled between our
        // check and the lease): never journal a second terminal outcome.
        let terminal = self.authoritative_state(run_dir).await?.terminal;
        // Shutdown-aware settlement at settle time (E05): a cancel racing
        // shutdown settles as Interrupted, not Canceled.
        let shutting_down = self.is_shutting_down();
        if terminal.is_none() {
            let settled_now = {
                let runs = self.inner.runs.read().await;
                runs.get(id).cloned().unwrap_or_else(|| fallback.clone())
            };
            let (event_kind, code, reason) = if shutting_down {
                (
                    "run_interrupted",
                    FailureCode::Interrupted,
                    "service shutdown",
                )
            } else {
                (
                    "run_canceled",
                    FailureCode::Canceled,
                    "cancellation requested",
                )
            };
            write_run_event(
                &settled_now,
                event_kind,
                json!({
                    "reason": FailureDetail::new(
                        code,
                        reason,
                    ),
                }),
            )
            .map_err(api_internal)?;
        }
        // Settlement journaled the terminal outcome: the accepted
        // request is now a settled cancellation.
        {
            let mut runs = self.inner.runs.write().await;
            if let Some(record) = runs.get_mut(id)
                && record.state == RunStatus::CancelRequested
            {
                record.state = if shutting_down {
                    RunStatus::Interrupted
                } else {
                    RunStatus::Canceled
                };
            }
        }
        self.inner.queue_notify.notify_waiters();
        Ok(true)
    }
}
