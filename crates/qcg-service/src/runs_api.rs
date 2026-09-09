use crate::artifacts::{
    api_bad_request, api_internal, api_not_found, check_verified_artifact_hashes,
    collect_verified_artifacts,
};
use crate::lifecycle::SpawnRun;
use crate::queue::queue_positions;
use crate::run_dirs::{prepare_api_run_directory, prepare_checkpoint_fork, write_run_event};
use crate::summaries::{
    fold_run_state, poll_journal_events, read_optional_output_manifest, read_run_contract_sha256,
    read_run_events, read_run_generator_path, read_run_inputs, run_meta_dir, run_workspace_dir,
    truncate_trailing_canceled_events,
};
use crate::types::{LocalQcgService, RunBundleParts, RunRecord};
use camino::{Utf8Path, Utf8PathBuf};
use futures_util::{StreamExt as _, stream::BoxStream};
use qcg_api::{
    AnswerPayload, ApiError, ConfirmDecision, ConfirmationDecision, ForkRun, RunCostMetrics,
    RunSnapshot, RunStatus, StartRun,
};
use qcg_api::{RunEvent, RunEventData};
use qcg_contract::{Contract, ContractError, validate_form_values};
use qcg_engine::{JournalLimits, read_output_manifest, resolve_artifact_path};
use qcg_types::{FailureCode, FailureDetail, OutputArtifact, OutputManifest};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;

impl LocalQcgService {
    /// Reserves the run id for a start request before any side effect, so an
    /// idempotency claim can bind retries to one run directory.
    pub fn reserve_start_run_id(&self, generator_id: &str) -> Result<String, ApiError> {
        if !crate::artifacts::is_safe_id(generator_id) {
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
        Ok(format!(
            "{}-fork-{}",
            contract.manifest.generator.id,
            uuid::Uuid::now_v7()
        ))
    }

    pub async fn start_run(&self, req: StartRun) -> Result<String, ApiError> {
        self.start_run_with_id(req, None).await
    }

    pub async fn start_run_with_id(
        &self,
        req: StartRun,
        reserved_run_id: Option<String>,
    ) -> Result<String, ApiError> {
        let contract = self.load_generator(&req.generator_id)?;
        // Resolve defaults and FileValue normalization once at admission and
        // persist the canonical inputs everywhere (A10). Raw requests never
        // reach the journal or the engine.
        let canonical_inputs = match contract.manifest.resolve_inputs(req.inputs.clone()) {
            Ok(resolved) => qcg_engine::canonical_file_inputs(&contract, resolved)
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
        let run_dir = self.inner.runs_dir.join(&run_id);
        let (events, _) = broadcast::channel(512);
        let cancellation = CancellationToken::new();
        let task = Arc::new(Mutex::new(None));
        let inputs = canonical_inputs;
        let answers = req.answers;
        let confirmations = req.confirmations;
        let priority = req.priority.unwrap_or(0);
        // Adoption: a retry bound to this run id by an expired idempotency
        // claim resumes the orphaned directory instead of creating a second
        // run. Same key and digest imply identical canonical inputs.
        let adopted =
            crate::run_dirs::try_adopt_run_dir(&run_dir, &run_id).map_err(api_internal)?;
        // Phase 1: filesystem preparation happens outside the run-map lock so
        // concurrent runs never block on unrelated directory and journal I/O.
        let mut created_fresh = false;
        if !adopted {
            if let Err(error) = prepare_api_run_directory(&run_dir) {
                return Err(api_internal(error));
            }
            created_fresh = true;
        }
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
        );
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
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": 1,
                    "retain_days": contract.manifest.journal.retain_days,
                    "priority": priority,
                    "parent_run_id": Value::Null,
                    "effective_max_total_steps": effective.max_total_steps,
                    "effective_policy_origin": effective.origin,
                }),
            )
        {
            if created_fresh {
                let _ = std::fs::remove_dir_all(&run_dir);
            }
            return Err(api_internal(error));
        }
        // Memory observes the same durable admission instant as restarts and
        // peers: derive from the journal, never re-stamp locally.
        staged.queued_at = crate::summaries::read_last_queued_at(&run_dir);
        // Phase 2: short critical section covering only capacity and registration.
        {
            let mut runs = self.inner.runs.write().await;
            if runs.len() >= self.inner.max_tracked_runs {
                runs.retain(|_, record| !record.state.is_terminal());
            }
            if runs.len() >= self.inner.max_tracked_runs {
                drop(runs);
                if created_fresh {
                    let _ = std::fs::remove_dir_all(&run_dir);
                }
                return Err(ApiError::Unavailable {
                    detail: format!(
                        "run capacity is exhausted: {} non-terminal runs are already tracked",
                        self.inner.max_tracked_runs
                    ),
                });
            }
            runs.insert(run_id.clone(), staged);
        }
        self.spawn_engine_run(SpawnRun {
            run_id: run_id.clone(),
            contract,
            inputs,
            run_dir,
            events,
            answers,
            confirmations,
            priority,
            cancellation,
            task,
        });
        self.inner.queue_notify.notify_waiters();
        self.preempt_for_priority(priority).await;
        Ok(run_id)
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
        let contract = Contract::load(&generator_path).map_err(api_internal)?;
        let run_id = reserved_run_id.unwrap_or_else(|| {
            format!(
                "{}-fork-{}",
                contract.manifest.generator.id,
                uuid::Uuid::now_v7()
            )
        });
        let run_dir = self.inner.runs_dir.join(&run_id);
        let (events, _) = broadcast::channel(512);
        let cancellation = CancellationToken::new();
        let task = Arc::new(Mutex::new(None));
        // Filesystem preparation happens outside the run-map lock so concurrent
        // runs never block on checkpoint copies, hashing, or journal I/O.
        // An adopted retry skips preparation; its checkpoint copy already
        // completed under the same key and digest.
        let adopted = match crate::run_dirs::try_adopt_run_dir(&run_dir, &run_id) {
            Ok(adopted) => adopted,
            Err(error) => {
                if run_dir.exists()
                    && let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir)
                {
                    return Err(api_internal(format!(
                        "{error}; failed to clean incomplete fork `{run_id}`: {cleanup_error}"
                    )));
                }
                return Err(api_internal(error));
            }
        };
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
        let fork_inputs = (|| -> Result<BTreeMap<String, Value>, ApiError> {
            let state = fold_run_state(&run_dir).map_err(api_internal)?;
            let inputs = state.inputs.ok_or_else(|| {
                api_internal(format!(
                    "checkpoint {source_id}@{} has no canonical inputs",
                    request.at_seq
                ))
            })?;
            contract
                .manifest
                .resolve_inputs(inputs.clone())
                .and_then(|resolved| {
                    qcg_engine::canonical_file_inputs(&contract, resolved).map_err(|error| {
                        ContractError::Invalid(format!("invalid file input: {error}"))
                    })
                })
                .map_err(|error| ApiError::invalid_field("state_patch.inputs", error.to_string()))
        })();
        let inputs = match fork_inputs {
            Ok(inputs) => inputs,
            Err(error) => {
                if !adopted
                    && run_dir.exists()
                    && let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir)
                {
                    return Err(api_internal(format!(
                        "{error}; failed to clean incomplete fork `{run_id}`: {cleanup_error}"
                    )));
                }
                return Err(error);
            }
        };
        let mut staged = RunRecord {
            contract: contract.clone(),
            contract_sha256: contract.sha256.clone(),
            inputs: inputs.clone(),
            answers: request.answers.clone(),
            confirmations: request.confirmations.clone(),
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
                    "inputs": &staged.inputs,
                    "answers": &staged.answers,
                    "confirmations": &staged.confirmations,
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": 1,
                    "retain_days": contract.manifest.journal.retain_days,
                    "priority": staged.priority,
                    "parent_run_id": source_id,
                }),
            )
        {
            let _ = std::fs::remove_dir_all(&run_dir);
            return Err(api_internal(error));
        }
        // Memory observes the same durable admission instant as restarts and
        // peers: derive from the journal, never re-stamp locally.
        staged.queued_at = crate::summaries::read_last_queued_at(&run_dir);
        // Short critical section covering only capacity and registration.
        {
            let mut runs = self.inner.runs.write().await;
            if runs.len() >= self.inner.max_tracked_runs {
                runs.retain(|_, record| !record.state.is_terminal());
            }
            if runs.len() >= self.inner.max_tracked_runs {
                drop(runs);
                if !adopted {
                    let _ = std::fs::remove_dir_all(&run_dir);
                }
                return Err(ApiError::Unavailable {
                    detail: format!(
                        "run capacity is exhausted: {} non-terminal runs are already tracked",
                        self.inner.max_tracked_runs
                    ),
                });
            }
            runs.insert(run_id.clone(), staged);
        }
        let fork_priority = request.priority.unwrap_or(0);
        self.spawn_engine_run(SpawnRun {
            run_id: run_id.clone(),
            contract,
            inputs,
            run_dir,
            events,
            answers: request.answers,
            confirmations: request.confirmations,
            priority: fork_priority,
            cancellation,
            task,
        });
        self.inner.queue_notify.notify_waiters();
        self.preempt_for_priority(fork_priority).await;
        Ok(run_id)
    }

    /// Delete a terminal run directory. Active runs are rejected so deletion
    /// never interrupts execution; cancel first. Missing runs are an error.
    /// A repeated delete after success reports not-found; list first when
    /// retrying unattended cleanup.
    pub async fn delete_run(&self, id: &str) -> Result<(), ApiError> {
        let run_dir = self.run_dir_for(id).await?;
        // A peer may own execution while this process sees no local task:
        // verify the execution lease and pending cancel mailbox in addition
        // to local terminal state (A02). Deletion before the owner ACKs the
        // stop would orphan a live writer.
        if crate::run_dirs::has_pending_cancel_control(&run_dir) {
            return Err(ApiError::Conflict {
                detail: format!(
                    "run `{id}` has a pending cancel; wait for settlement before deletion"
                ),
            });
        }
        // Hold the execution lease until the directory is gone so no owner
        // can start appending between the check and the removal (TOCTOU).
        let _execution_lease =
            crate::run_dirs::try_lock_run_execution(&run_dir).map_err(api_internal)?;
        if _execution_lease.is_none() {
            return Err(ApiError::Conflict {
                detail: format!(
                    "run `{id}` is executing elsewhere; cancel and wait for settlement before deletion"
                ),
            });
        }
        let terminal = match self.inner.runs.read().await.get(id) {
            Some(record) => {
                // A still-running local task means the run is active even if
                // memory state was already marked canceled but not settled.
                // A finished handle (is_finished) no longer owns execution.
                let live_task = record
                    .task
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .as_ref()
                    .is_some_and(|handle| !handle.is_finished());
                if live_task {
                    false
                } else {
                    record.state.is_terminal()
                }
            }
            None => fold_run_state(&run_dir)
                .map_err(api_internal)?
                .terminal
                .is_some(),
        };
        if !terminal {
            return Err(ApiError::Conflict {
                detail: format!("run `{id}` is still active; cancel it before deletion"),
            });
        }
        self.inner.runs.write().await.remove(id);
        std::fs::remove_dir_all(&run_dir).map_err(api_internal)?;
        self.inner.queue_notify.notify_waiters();
        Ok(())
    }

    /// Gather the bundle inputs, verifying artifacts. Works for active runs.
    pub async fn run_bundle_parts(&self, id: &str) -> Result<RunBundleParts, ApiError> {
        let run_dir = self.run_dir_for(id).await?;
        let snapshot = self.snapshot(id.to_string()).await?;
        let inputs = read_run_inputs(&run_dir).map_err(api_internal)?;
        let journal = run_meta_dir(&run_dir).join("journal.jsonl");
        let outputs = read_optional_output_manifest(&run_dir).map_err(api_internal)?;
        // Runs without collected outputs (active or failed early) export
        // without artifacts instead of failing.
        let verified = match outputs {
            Some(_) => collect_verified_artifacts(&run_dir).map_err(api_internal)?,
            None => Vec::new(),
        };
        check_verified_artifact_hashes(&verified).map_err(api_internal)?;
        Ok(RunBundleParts {
            snapshot,
            inputs,
            journal,
            outputs,
            verified,
        })
    }

    pub async fn snapshot(&self, id: String) -> Result<RunSnapshot, ApiError> {
        let memory = self.inner.runs.read().await.get(&id).cloned();
        let run_dir = match &memory {
            Some(record) => record.run_dir.clone(),
            None => self.run_dir_for(&id).await?,
        };
        let artifacts = match memory.as_ref().and_then(|record| record.artifacts.clone()) {
            Some(artifacts) => Some(artifacts),
            None => read_optional_output_manifest(&run_dir).map_err(api_internal)?,
        };
        // One bounded journal read serves every durable derivation below
        // (fold, identity, queue instant, metrics): a snapshot never pays
        // a scan per field.
        let journal_values =
            crate::summaries::read_journal_events(&run_dir).map_err(api_internal)?;
        let disk_state =
            qcg_engine::RunState::fold_values(&journal_values).map_err(api_internal)?;
        let journal_events = journal_values
            .iter()
            .map(|event| qcg_api::RunEvent::from_flat(event).map_err(api_internal))
            .collect::<Result<Vec<_>, _>>()?;
        // A journaled terminal outcome always wins over memory: acceptance
        // displays (such as `CancelRequested`) settle into their terminal
        // state as soon as the journal records it, on every process (A02).
        // An unsettled run without memory is queued, never terminal: only a
        // journaled outcome may report a terminal state, so an orphaned or
        // peer-owned run resumes through the lease instead of being
        // misreported as dead.
        let state = disk_state
            .terminal
            .as_ref()
            .map(|terminal| match terminal {
                qcg_engine::TerminalState::Succeeded => RunStatus::Succeeded,
                qcg_engine::TerminalState::Failed => RunStatus::Failed,
                qcg_engine::TerminalState::Canceled => RunStatus::Canceled,
                qcg_engine::TerminalState::Interrupted => RunStatus::Interrupted,
            })
            .or_else(|| memory.as_ref().map(|record| record.state))
            .unwrap_or(RunStatus::Queued);
        let contract_sha256 = match memory.as_ref() {
            Some(record) => Some(record.contract_sha256.clone()),
            None => {
                Some(read_run_contract_sha256(&run_dir, &journal_events).map_err(api_internal)?)
            }
        };
        let memory_priority = memory.as_ref().map(|record| record.priority);
        let memory_parent = memory
            .as_ref()
            .and_then(|record| record.parent_run_id.clone());
        let (queued_at, queue_position) = if state == RunStatus::Queued {
            let runs = self.inner.runs.read().await;
            let positions = queue_positions(&runs);
            // Display the same durable instant used for ordering; memory is
            // only a fallback when the journal holds no instant (the read
            // above already succeeded, so an unreadable journal cannot
            // occur here).
            let displayed = crate::summaries::read_last_queued_at_from_values(&journal_values)
                .or_else(|| memory.as_ref().and_then(|record| record.queued_at))
                .map(|at| at.to_rfc3339());
            (
                displayed,
                runs.get(&id).and_then(|_| positions.get(&id).copied()),
            )
        } else {
            (None, None)
        };
        // Disk-only snapshots (evicted or restarted runs) derive the
        // generator from the journal identity event, never from run_id
        // string parsing. An unresolvable owner fails the snapshot instead
        // of emitting an ownerless record for the client to guess about.
        let generator_id = memory
            .as_ref()
            .map(|record| record.contract.manifest.generator.id.clone())
            .or_else(|| disk_state.generator_id.clone())
            .ok_or_else(|| api_internal(format!("run `{id}` has no owning generator")))?;
        // Priority and parent derive from the same single read above, so
        // disk-only snapshots never trigger another scan of the journal.
        let (journal_priority, journal_parent) =
            crate::summaries::read_queued_identity_from_values(&journal_values);
        Ok(RunSnapshot {
            run_id: id,
            state,
            seq: disk_state.last_seq,
            contract_sha256,
            generator_id,
            artifacts,
            question: memory.as_ref().and_then(|record| record.question.clone()),
            confirm: memory.and_then(|record| record.confirm),
            queued_at,
            queue_position,
            priority: memory_priority.unwrap_or(journal_priority),
            parent_run_id: memory_parent.or(journal_parent),
            metrics: crate::summaries::read_run_metrics_from_view(&journal_events, &disk_state)
                .map_err(api_internal)?,
        })
    }

    /// Cost metrics for one run with a USD estimate and pricing coverage.
    pub async fn run_cost_metrics(&self, id: String) -> Result<RunCostMetrics, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        // One bounded journal read serves the fold, the typed events, and
        // the pricing scan below.
        let journal_values =
            crate::summaries::read_journal_events(&run_dir).map_err(api_internal)?;
        let state = qcg_engine::RunState::fold_values(&journal_values).map_err(api_internal)?;
        let journal_events = journal_values
            .iter()
            .map(|event| qcg_api::RunEvent::from_flat(event).map_err(api_internal))
            .collect::<Result<Vec<_>, _>>()?;
        let metrics = crate::summaries::read_run_metrics_from_view(&journal_events, &state)
            .map_err(api_internal)?
            .unwrap_or_default();
        let memory_state = self
            .inner
            .runs
            .read()
            .await
            .get(&id)
            .map(|record| record.state);
        // Pricing coverage degrades loudly, never silently: an unloadable
        // contract reports totals as potentially understated instead of
        // failing metrics the journal already supports.
        let priced = match self.pricing_covered(&journal_events, &run_dir).await {
            Ok(priced) => priced,
            Err(error) => {
                tracing::warn!(run_id = %id, %error, "pricing coverage check failed; reporting unpriced");
                false
            }
        };
        Ok(RunCostMetrics {
            run_id: id,
            state: state
                .terminal
                .as_ref()
                .map(|terminal| match terminal {
                    qcg_engine::TerminalState::Succeeded => RunStatus::Succeeded,
                    qcg_engine::TerminalState::Failed => RunStatus::Failed,
                    qcg_engine::TerminalState::Canceled => RunStatus::Canceled,
                    qcg_engine::TerminalState::Interrupted => RunStatus::Interrupted,
                })
                .or(memory_state)
                .unwrap_or(RunStatus::Queued),
            metrics: metrics.clone(),
            cost_usd: metrics.cost_microusd as f64 / 1_000_000.0,
            priced,
        })
    }

    /// True when every billed LLM call resolves to contract pricing.
    /// Unpriced calls may understate the total, so callers must surface this.
    /// Takes already-read events so pricing never re-scans the journal.
    async fn pricing_covered(
        &self,
        events: &[qcg_api::RunEvent],
        run_dir: &Utf8Path,
    ) -> Result<bool, ApiError> {
        let calls: Vec<(&str, &str, u64, u64)> = events
            .iter()
            .filter_map(|event| match &event.data {
                RunEventData::LlmCall(data) => Some((
                    data.provider.as_str(),
                    data.model.as_str(),
                    data.tokens.input.saturating_add(data.tokens.output),
                    data.cost_microusd,
                )),
                _ => None,
            })
            .collect();
        if calls.is_empty() {
            return Ok(true);
        }
        let generator_path = read_run_generator_path(run_dir).map_err(api_internal)?;
        let contract = Contract::load(&generator_path).map_err(api_internal)?;
        let priced: Vec<(String, String)> = contract
            .manifest
            .llm
            .as_ref()
            .map(|llm| {
                llm.models
                    .iter()
                    .chain(llm.model.as_ref())
                    .filter(|model| {
                        model.input_cost_per_million_usd.is_some()
                            && model.output_cost_per_million_usd.is_some()
                    })
                    .map(|model| (model.provider.clone(), model.model.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Ok(calls.into_iter().all(|(provider, model, tokens, cost)| {
            tokens == 0
                || cost > 0
                || priced.iter().any(|(entry_provider, entry_model)| {
                    entry_provider == provider && entry_model == model
                })
        }))
    }

    pub async fn subscribe(&self, id: String) -> Result<BoxStream<'static, RunEvent>, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        // A memory-resident record does not imply local execution: in shared
        // mode another process may own the run while this one only mirrors
        // it. Non-owned runs follow the durable journal (~250ms poll) so
        // subscribers observe peer progress instead of a stale broadcast.
        let live_receiver = if self.owns_live_stream(&id).await {
            self.live_receiver(&id).await.ok()
        } else {
            None
        };
        let history = read_run_events(&run_dir).map_err(ApiError::from)?;
        let history_last_seq = history.last().map_or(0, |event| event.seq);
        let history_stream = futures_util::stream::iter(history);
        let lagged_run_id = id.clone();
        let live_stream = match live_receiver {
            Some(receiver) => {
                let mut delivered_seq = history_last_seq;
                BroadcastStream::new(receiver)
                    .filter_map(move |event| {
                        let run_id = lagged_run_id.clone();
                        let result = match event {
                            Ok(event) if event.seq > delivered_seq => {
                                delivered_seq = event.seq;
                                Some(event)
                            }
                            Ok(_) => None,
                            Err(
                                tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(
                                    skipped,
                                ),
                            ) => {
                                delivered_seq = delivered_seq.saturating_add(skipped);
                                Some(RunEvent::lagged(run_id, delivered_seq))
                            }
                        };
                        async move { result }
                    })
                    .boxed()
            }
            None => poll_journal_events(run_dir, id, history_last_seq),
        };
        Ok(history_stream.chain(live_stream).boxed())
    }

    pub async fn answer(
        &self,
        id: String,
        question_id: String,
        payload: AnswerPayload,
    ) -> Result<(), ApiError> {
        let answer = json!(payload.values);
        // Memory fast paths first; durable acceptance is decided atomically
        // under the journal lock below, so racing peers serialize and exactly
        // one conflicting acceptance wins (A02). Durable acceptance precedes
        // the success report, so a restart before the engine consumes the
        // queue still resumes with the same values.
        //
        // The write guard never spans an await: rejection classification
        // takes run-map locks, so awaiting it under the guard would deadlock
        // the runs map against itself (B01). The guard returns a decision;
        // classification and spawning happen outside.
        enum AnswerDecision {
            Spawn(Box<crate::lifecycle::SpawnRun>),
            Reject(crate::types::ServiceError),
        }
        let outcome = {
            let mut runs = self.inner.runs.write().await;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
            if let Some(existing) = record.answers.get(&question_id) {
                return if existing == &answer {
                    // Idempotent replay: the first call already journaled and
                    // scheduled the engine, so report success without duplicating.
                    Ok(())
                } else {
                    Err(ApiError::Conflict {
                        detail: format!(
                            "question `{question_id}` was already answered with different values"
                        ),
                    })
                };
            }
            if record.state != RunStatus::Waiting {
                return Err(api_bad_request(format!(
                    "run `{id}` is not waiting for user input"
                )));
            }
            let question = record
                .question
                .clone()
                .ok_or_else(|| api_bad_request(format!("run `{id}` has no question")))?;
            if question.id != question_id {
                return Err(api_bad_request(format!(
                    "answer was for `{}`, but run is waiting for `{}`",
                    question_id, question.id
                )));
            }
            validate_form_values(&question.fields, &answer, &record.contract.manifest.runtime)
                .map_err(|error| {
                    ApiError::invalid_field("values", format!("invalid form answer: {error}"))
                })?;
            let persist = record.clone();
            let check_answer = answer.clone();
            let check_question_id = question_id.clone();
            // Generation check: observe the durable pending generation just
            // before the atomic append. ID matching alone cannot tell a
            // regenerated prompt apart, so the lock-held precondition below
            // re-verifies this exact generation (A02).
            let observed = fold_run_state(&persist.run_dir).map_err(api_internal)?;
            // A stale observation still attempts the atomic write: the
            // lock-held precondition re-verifies, and classification below
            // reports the accurate outcome (terminal, answered, or gone)
            // instead of this spot guessing from possibly old data.
            let check_pending_seq = match &observed.pending {
                Some(qcg_engine::Interaction::Question { question })
                    if question.id == question_id =>
                {
                    observed.pending_seq
                }
                _ => None,
            };
            // One durable timestamp shared by the journal event and the
            // memory record so restarts observe the same requeue order.
            let queued_now = chrono::Utc::now();
            let accepted = crate::run_dirs::write_run_event_if(
                &persist,
                "user_answered",
                json!({
                    "question_id": check_question_id,
                    "values": check_answer,
                    "queued_at": queued_now.to_rfc3339(),
                }),
                move |state| {
                    use qcg_engine::JournalError;
                    if state.terminal.is_some() {
                        return Err(JournalError::PreconditionFailed(
                            "run is already terminal".into(),
                        ));
                    }
                    if state.cancel_requested {
                        return Err(JournalError::PreconditionFailed(
                            "a cancel was accepted".into(),
                        ));
                    }
                    match &state.pending {
                        Some(qcg_engine::Interaction::Question { question })
                            if question.id == check_question_id => {}
                        _ => {
                            return Err(JournalError::PreconditionFailed(
                                "run is not waiting for this question".into(),
                            ));
                        }
                    }
                    match (state.pending_seq, check_pending_seq) {
                        (Some(current), Some(expected)) if current == expected => {}
                        _ => {
                            return Err(JournalError::PreconditionFailed(
                                "pending prompt generation changed; refresh and retry".into(),
                            ));
                        }
                    }
                    if state.answers.contains_key(&check_question_id) {
                        return Err(JournalError::PreconditionFailed(
                            "question was already answered".into(),
                        ));
                    }
                    Ok(())
                },
            );
            match accepted {
                // The rejection already happened atomically; re-fold outside
                // the guard only to classify which error to report.
                Err(error) => AnswerDecision::Reject(error),
                Ok(()) => {
                    // Continuations live in the typed journal store now, so only
                    // the user answer joins the memory map.
                    record.answers.insert(question_id.clone(), answer.clone());
                    record.state = RunStatus::Queued;
                    record.queued_at = Some(queued_now);
                    record.question = None;
                    record.confirm = None;
                    record.artifacts = None;
                    let cancellation = CancellationToken::new();
                    record.cancellation = cancellation.clone();
                    AnswerDecision::Spawn(Box::new(crate::lifecycle::SpawnRun {
                        run_id: id.clone(),
                        contract: record.contract.clone(),
                        inputs: record.inputs.clone(),
                        run_dir: record.run_dir.clone(),
                        events: record.events.clone(),
                        answers: record.answers.clone(),
                        confirmations: record.confirmations.clone(),
                        priority: record.priority,
                        cancellation,
                        task: record.task.clone(),
                    }))
                }
            }
        };
        match outcome {
            AnswerDecision::Spawn(request) => {
                self.spawn_engine_run(*request);
                // Wake queue waiters: requeue changes the head and a freed ordering
                // slot must not wait for an unrelated notification (A11).
                self.inner.queue_notify.notify_waiters();
                Ok(())
            }
            AnswerDecision::Reject(error) => {
                self.classify_answer_rejection(&id, &question_id, &answer, error)
                    .await
            }
        }
    }

    pub async fn confirm(
        &self,
        id: String,
        confirmation_id: String,
        decision: ConfirmDecision,
    ) -> Result<(), ApiError> {
        let approved = decision.decision == ConfirmationDecision::Approve;
        // Validate, persist, and mutate under one write lock so concurrent
        // decisions cannot both journal conflicting values with last-wins.
        // Journal I/O is a short local append; engine scheduling and terminal
        // settlement stay outside the lock. Rejection classification also
        // stays outside: it takes run-map locks, so awaiting it under the
        // guard would deadlock the runs map against itself (B01).
        enum AfterLock {
            Deny {
                denied: Box<RunRecord>,
                confirm: Box<qcg_api::ConfirmSpec>,
            },
            Spawn(Box<SpawnRun>),
            Reject(crate::types::ServiceError),
        }
        let after = {
            let mut runs = self.inner.runs.write().await;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
            if let Some(existing) = record.confirmations.get(&confirmation_id) {
                return if *existing == approved {
                    // Idempotent replay: the first call already journaled and
                    // settled or scheduled, so report success without duplicating.
                    Ok(())
                } else {
                    Err(ApiError::Conflict {
                        detail: format!(
                            "confirmation `{confirmation_id}` already has a different decision"
                        ),
                    })
                };
            }
            if record.state != RunStatus::Confirming {
                return Err(api_bad_request(format!(
                    "run `{id}` is not waiting for side-effect confirmation"
                )));
            }
            let confirm = record
                .confirm
                .clone()
                .ok_or_else(|| api_bad_request(format!("run `{id}` has no confirmation")))?;
            if confirm.id != confirmation_id {
                return Err(ApiError::Conflict {
                    detail: format!(
                        "confirmation was for `{confirmation_id}`, but run is waiting for `{}`",
                        confirm.id
                    ),
                });
            }
            let persist = record.clone();
            let check_confirmation_id = confirmation_id.clone();
            // Generation check: observe the durable pending generation just
            // before the atomic append. ID matching alone cannot tell a
            // regenerated prompt apart, so the lock-held precondition below
            // re-verifies this exact generation (A02).
            let observed = fold_run_state(&persist.run_dir).map_err(api_internal)?;
            // A stale observation still attempts the atomic write: the
            // lock-held precondition re-verifies, and classification below
            // reports the accurate outcome instead of this spot guessing
            // from possibly old data.
            let check_pending_seq = match &observed.pending {
                Some(qcg_engine::Interaction::Confirmation { confirm })
                    if confirm.id == confirmation_id =>
                {
                    observed.pending_seq
                }
                _ => None,
            };
            let queued_now = chrono::Utc::now();
            if let Err(error) = crate::run_dirs::write_run_event_if(
                &persist,
                "user_confirmed",
                json!({
                    "confirmation_id": check_confirmation_id,
                    "approved": approved,
                    "queued_at": queued_now.to_rfc3339(),
                }),
                move |state| {
                    use qcg_engine::JournalError;
                    if state.terminal.is_some() {
                        return Err(JournalError::PreconditionFailed(
                            "run is already terminal".into(),
                        ));
                    }
                    if state.cancel_requested {
                        return Err(JournalError::PreconditionFailed(
                            "a cancel was accepted".into(),
                        ));
                    }
                    match &state.pending {
                        Some(qcg_engine::Interaction::Confirmation { confirm })
                            if confirm.id == check_confirmation_id => {}
                        _ => {
                            return Err(JournalError::PreconditionFailed(
                                "run is not waiting for this confirmation".into(),
                            ));
                        }
                    }
                    match (state.pending_seq, check_pending_seq) {
                        (Some(current), Some(expected)) if current == expected => {}
                        _ => {
                            return Err(JournalError::PreconditionFailed(
                                "pending prompt generation changed; refresh and retry".into(),
                            ));
                        }
                    }
                    if state.confirmations.contains_key(&check_confirmation_id) {
                        return Err(JournalError::PreconditionFailed(
                            "confirmation was already decided".into(),
                        ));
                    }
                    Ok(())
                },
            ) {
                // The rejection already happened atomically; re-fold outside
                // the guard only to classify which error to report.
                AfterLock::Reject(error)
            } else if !approved {
                record.confirmations.insert(confirm.id.clone(), false);
                record.state = RunStatus::Failed;
                record.confirm = None;
                AfterLock::Deny {
                    denied: Box::new(record.clone()),
                    confirm: Box::new(confirm),
                }
            } else {
                record.confirmations.insert(confirmation_id.clone(), true);
                record.state = RunStatus::Queued;
                record.queued_at = Some(queued_now);
                record.confirm = None;
                record.artifacts = None;
                let cancellation = CancellationToken::new();
                record.cancellation = cancellation.clone();
                AfterLock::Spawn(Box::new(SpawnRun {
                    run_id: id.clone(),
                    contract: record.contract.clone(),
                    inputs: record.inputs.clone(),
                    run_dir: record.run_dir.clone(),
                    events: record.events.clone(),
                    answers: record.answers.clone(),
                    confirmations: record.confirmations.clone(),
                    priority: record.priority,
                    cancellation,
                    task: record.task.clone(),
                }))
            }
        };
        match after {
            AfterLock::Deny { denied, confirm } => {
                // The denial decision already won exclusively via the atomic
                // user_confirmed check above. Both settlement events append
                // under one journal-lock hold so no writer interleaves them.
                crate::run_dirs::write_run_events(
                    &denied,
                    vec![
                        (
                            "side_effect",
                            json!({
                                "kind": confirm.kind,
                                "target": confirm.target,
                                "decision": "denied_by_user",
                            }),
                        ),
                        (
                            "run_finished",
                            json!({
                                "status": "failed",
                                "reason": FailureDetail::new(
                                    FailureCode::ExecutionFailed,
                                    "side effect denied by user",
                                ),
                            }),
                        ),
                    ],
                )
                .map_err(api_internal)?;
                Ok(())
            }
            AfterLock::Spawn(request) => {
                self.spawn_engine_run(*request);
                self.inner.queue_notify.notify_waiters();
                Ok(())
            }
            AfterLock::Reject(error) => {
                self.classify_confirm_rejection(&id, &confirmation_id, approved, error)
                    .await
            }
        }
    }

    /// Classifies an atomically rejected answer by re-folding the journal.
    /// The rejection itself already happened under the journal lock; this
    /// only decides which error to report, so a classification race cannot
    /// accept a second winner.
    async fn classify_answer_rejection(
        &self,
        id: &str,
        question_id: &str,
        answer: &Value,
        error: crate::types::ServiceError,
    ) -> Result<(), ApiError> {
        if !matches!(error, crate::types::ServiceError::PreconditionFailed(_)) {
            return Err(api_internal(error));
        }
        let run_dir = self.run_dir_for(id).await?;
        let state = fold_run_state(&run_dir).map_err(api_internal)?;
        if state.terminal.is_some() {
            return Err(ApiError::Conflict {
                detail: format!("run `{id}` is already terminal; answer was rejected"),
            });
        }
        if state.cancel_requested {
            return Err(api_bad_request(format!(
                "run `{id}` is not waiting for user input; a cancel was accepted"
            )));
        }
        match state.answers.get(question_id) {
            Some(existing) if existing == answer => {
                // A peer accepted the identical answer first. Adopt it so
                // this process observes the same durable acceptance.
                let mut runs = self.inner.runs.write().await;
                if let Some(record) = runs.get_mut(id) {
                    record
                        .answers
                        .insert(question_id.to_string(), answer.clone());
                }
                Ok(())
            }
            Some(_) => Err(ApiError::Conflict {
                detail: format!(
                    "question `{question_id}` was already answered with different values"
                ),
            }),
            None => Err(api_bad_request(format!(
                "run `{id}` is not waiting for user input"
            ))),
        }
    }

    /// Classifies an atomically rejected confirmation the same way.
    async fn classify_confirm_rejection(
        &self,
        id: &str,
        confirmation_id: &str,
        approved: bool,
        error: crate::types::ServiceError,
    ) -> Result<(), ApiError> {
        if !matches!(error, crate::types::ServiceError::PreconditionFailed(_)) {
            return Err(api_internal(error));
        }
        let run_dir = self.run_dir_for(id).await?;
        let state = fold_run_state(&run_dir).map_err(api_internal)?;
        if state.terminal.is_some() {
            return Err(ApiError::Conflict {
                detail: format!("run `{id}` is already terminal; confirm was rejected"),
            });
        }
        if state.cancel_requested {
            return Err(api_bad_request(format!(
                "run `{id}` is not waiting for side-effect confirmation; a cancel was accepted"
            )));
        }
        match state.confirmations.get(confirmation_id) {
            Some(existing) if *existing == approved => {
                // A peer decided identically first. Adopt the durable
                // decision so this process observes the same acceptance
                // instead of a stale confirmation prompt.
                let mut runs = self.inner.runs.write().await;
                if let Some(record) = runs.get_mut(id) {
                    record
                        .confirmations
                        .insert(confirmation_id.to_string(), approved);
                    if !approved {
                        // A peer denied first: settle locally as failed so a
                        // stale confirmation prompt never requeues denied work.
                        record.state = RunStatus::Failed;
                        record.confirm = None;
                        record.artifacts = None;
                    }
                }
                Ok(())
            }
            Some(_) => Err(ApiError::Conflict {
                detail: format!(
                    "confirmation `{confirmation_id}` already has a different decision"
                ),
            }),
            None => Err(api_bad_request(format!(
                "run `{id}` is not waiting for side-effect confirmation"
            ))),
        }
    }

    pub async fn cancel(&self, id: String) -> Result<(), ApiError> {
        // Durable cross-process cancel mailbox first so a peer owner observes
        // the request even when this process tracks no local task (A02).
        // Control-file creation is fail-closed: I/O errors are reported
        // instead of silently dropping the cancel request (A01).
        let run_dir = self.run_dir_for(&id).await?;
        crate::run_dirs::request_remote_cancel(&run_dir, &id, &self.inner.owner_id)
            .map_err(api_internal)?;
        let (task, settled) = {
            let mut runs = self.inner.runs.write().await;
            let Some(record) = runs.get_mut(&id) else {
                // No local record, but the durable mailbox already carries
                // the cancel to the owning peer (A02). Report success so a
                // non-tracking peer can still stop a shared run.
                return Ok(());
            };
            if record.state.is_terminal() {
                return Ok(());
            }
            // A live engine task is the journal writer even while parked in
            // Waiting/Confirming, and it holds the execution lease for the
            // whole run. Rendezvous with it instead of racing its lease:
            // probing the lease here would report a locally owned run as
            // executing elsewhere whenever cancel lands before an answer.
            let engine_is_active = record
                .task
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_some();
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
                    let _lease = crate::run_dirs::try_lock_run_execution(&run_dir)
                        .map_err(api_internal)?;
                    if _lease.is_none() {
                        return Err(ApiError::Conflict {
                            detail: format!("run `{id}` is executing elsewhere; cancel was signaled"),
                        });
                    }
                    let settled_now = {
                        let runs = self.inner.runs.read().await;
                        runs.get(&id).cloned().ok_or_else(|| {
                            api_internal(format!("run `{id}` vanished during cancellation"))
                        })?
                    };
                    // The aborted task may have settled through the queued
                    // finalizer first: reuse the terminal-checked settlement
                    // instead of journaling a second terminal outcome.
                    if !self.settle_canceled_here(&id, &settled_now).await? {
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
    async fn settle_canceled_here(&self, id: &str, fallback: &RunRecord) -> Result<bool, ApiError> {
        let run_dir = self.run_dir_for(id).await?;
        let _lease = crate::run_dirs::try_lock_run_execution(&run_dir).map_err(api_internal)?;
        if _lease.is_none() {
            return Ok(false);
        }
        // No live engine writer, so settling here is safe. Drain the
        // mailbox to a single journal event first, then record cancel.
        // A failed drain aborts settlement instead of dropping the
        // cancel request.
        self.drain_cancel_controls(id, &run_dir)
            .await
            .map_err(api_internal)?;
        // A terminal event may already exist (a writer settled between our
        // check and the lease): never journal a second terminal outcome.
        let terminal = fold_run_state(&run_dir)
            .map(|state| state.terminal)
            .map_err(api_internal)?;
        if terminal.is_none() {
            let settled_now = {
                let runs = self.inner.runs.read().await;
                runs.get(id).cloned().unwrap_or_else(|| fallback.clone())
            };
            write_run_event(
                &settled_now,
                "run_canceled",
                json!({
                    "reason": FailureDetail::new(
                        FailureCode::Canceled,
                        "cancellation requested",
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
                record.state = RunStatus::Canceled;
            }
        }
        self.inner.queue_notify.notify_waiters();
        Ok(true)
    }

    /// Preempt one running run for an incoming higher-priority run. The victim
    /// keeps its journal and returns to Queued; already finished steps replay
    /// on resume. At most one victim per arrival; equal priorities never
    /// preempt each other.
    async fn preempt_for_priority(&self, priority: i32) {
        if !*self
            .inner
            .preemption_enabled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
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

    async fn preempt_run(&self, id: &str) -> Result<(), ApiError> {
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
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": 1,
                "retain_days": staged.contract.manifest.journal.retain_days,
                "priority": staged.priority,
                "parent_run_id": staged.parent_run_id.clone(),
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
            // The preempted engine task records its own cancellation before
            // exiting. That bookkeeping event would read as terminal on
            // resume, so drop it: the requeue above is the true outcome and
            // no terminal settlement ran.
            truncate_trailing_canceled_events(&record.run_dir).map_err(api_internal)?;
            self.spawn_engine_run(SpawnRun {
                run_id: id.to_string(),
                contract: record.contract.clone(),
                inputs: record.inputs.clone(),
                run_dir: record.run_dir.clone(),
                events: record.events.clone(),
                answers: record.answers.clone(),
                confirmations: record.confirmations.clone(),
                priority: record.priority,
                cancellation: record.cancellation.clone(),
                task: Arc::clone(&record.task),
            });
        }
        self.inner.queue_notify.notify_waiters();
        Ok(())
    }

    pub async fn shutdown_active_runs(&self) -> Result<(), ApiError> {
        // Phase 1: durable mailbox signal + local cancellation first without
        // awaiting any task, so one unresponsive executor cannot block the
        // deadline for others. Never append to the journal while an engine
        // task is live (A01/A09).
        let active: Vec<(String, RunRecord)> = {
            let mut runs = self.inner.runs.write().await;
            let mut active = Vec::new();
            for (id, record) in runs.iter_mut() {
                if record.state.is_terminal() {
                    continue;
                }
                if let Err(error) = crate::run_dirs::request_remote_cancel(
                    &record.run_dir,
                    id,
                    &self.inner.owner_id,
                ) {
                    tracing::error!(%error, run_id = %id, "failed to record shutdown cancel signal");
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
                    // No local engine task (queued/waiting): settle durably
                    // only while holding the execution lease. A contended
                    // lease means a peer owns execution and settles it.
                    let _lease = crate::run_dirs::try_lock_run_execution(&record.run_dir)
                        .map_err(api_internal)?;
                    if _lease.is_none() {
                        return Ok::<(), ApiError>(());
                    }
                    // Shutdown settles the terminal event first; a drain
                    // failure is logged and the retained mailbox converges
                    // on the next drain instead of blocking shutdown.
                    if let Err(error) =
                        service.drain_cancel_controls(&id, &record.run_dir).await
                    {
                        tracing::warn!(run_id = %id, %error, "cancel drain failed during shutdown; mailbox retained");
                    }
                    // A writer may have settled first (e.g. the queued
                    // finalizer): never journal a second terminal outcome
                    // over it.
                    let settled = fold_run_state(&record.run_dir)
                        .map(|state| state.terminal.is_some())
                        .map_err(api_internal)?;
                    if settled {
                        return Ok::<(), ApiError>(());
                    }
                    let settled_now = {
                        let runs = service.inner.runs.read().await;
                        runs.get(&id).cloned().unwrap_or(record.clone())
                    };
                    write_run_event(
                        &settled_now,
                        "run_interrupted",
                        json!({
                            "reason": FailureDetail::new(
                                FailureCode::Canceled,
                                "service shutdown",
                            ),
                        }),
                    )
                    .map_err(api_internal)?;
                    // Settlement journaled the terminal outcome: retire the
                    // acceptance display into the settled state.
                    {
                        let mut runs = service.inner.runs.write().await;
                        if let Some(record) = runs.get_mut(&id) {
                            record.state = RunStatus::Interrupted;
                            record.question = None;
                            record.confirm = None;
                        }
                    }
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
                        let settled = fold_run_state(&record.run_dir)
                            .map(|state| state.terminal.is_some())
                            .map_err(api_internal)?;
                        if settled {
                            return Ok::<(), ApiError>(());
                        }
                        let settled_now = {
                            let runs = service.inner.runs.read().await;
                            runs.get(&id).cloned().unwrap_or(record.clone())
                        };
                        write_run_event(
                            &settled_now,
                            "run_interrupted",
                            json!({
                                "reason": FailureDetail::new(
                                    FailureCode::Canceled,
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
        // Detached container cleanups from dropped guards must finish
        // before shutdown reports done; otherwise "stopped" races orphaned
        // instances still being torn down. A nonzero remainder is surfaced,
        // never silently equated with a clean stop (B06).
        let outstanding =
            qcg_container::await_outstanding_cleanups(std::time::Duration::from_secs(65)).await;
        if outstanding > 0 {
            tracing::warn!(
                outstanding,
                "container cleanups outstanding past shutdown deadline; instances may need operator retry"
            );
        }
        Ok(())
    }

    pub async fn artifacts(&self, id: String) -> Result<OutputManifest, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        read_output_manifest(&run_meta_dir(&run_dir)).map_err(api_internal)
    }

    pub async fn read_artifact(
        &self,
        id: String,
        path: String,
    ) -> Result<(OutputArtifact, Utf8PathBuf), ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let manifest = read_output_manifest(&run_meta_dir(&run_dir)).map_err(api_internal)?;
        let artifact = manifest
            .artifacts
            .into_iter()
            .find(|artifact| artifact.path == path)
            .ok_or_else(|| api_not_found(format!("artifact `{path}` was not found")))?;
        let resolved = resolve_artifact_path(&run_workspace_dir(&run_dir), &artifact.path)
            .map_err(api_internal)?;
        Ok((artifact, resolved))
    }

    /// Opens the run journal for constant-memory streaming delivery. Limits
    /// come from the run contract, and a configured total bound is enforced
    /// against the file size before a single content byte is served, so no
    /// path allocates beyond its checked bound.
    pub async fn open_journal_stream(
        &self,
        id: String,
    ) -> Result<crate::types::JournalStream, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let generator_path = read_run_generator_path(&run_dir).map_err(api_internal)?;
        let contract = Contract::load(&generator_path).map_err(api_internal)?;
        let limits = JournalLimits::from(&contract.manifest.runtime);
        let path = run_meta_dir(&run_dir).join("journal.jsonl");
        let file = tokio::fs::File::open(&path).await.map_err(api_internal)?;
        let len = file.metadata().await.map_err(api_internal)?.len();
        if let Some(limit) = limits.max_total_bytes
            && len > limit as u64
        {
            return Err(ApiError::TooLarge {
                actual_bytes: len as usize,
                limit_bytes: limit,
            });
        }
        Ok(crate::types::JournalStream {
            file,
            len,
            limit: limits.max_total_bytes,
        })
    }
}
