use crate::artifacts::{
    api_bad_request, api_internal, api_not_found, check_verified_artifact_hashes,
    collect_verified_artifacts,
};
use crate::lifecycle::SpawnRun;
use crate::queue::queue_positions;
use crate::run_dirs::{prepare_api_run_directory, prepare_checkpoint_fork, write_run_event};
use crate::summaries::{
    fold_run_state, poll_journal_events, read_optional_output_manifest, read_queued_identity,
    read_run_contract_sha256, read_run_events, read_run_generator_path, read_run_inputs,
    read_run_metrics, run_meta_dir, run_workspace_dir, truncate_trailing_canceled_events,
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
use tokio::io::AsyncReadExt as _;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;

impl LocalQcgService {
    pub async fn start_run(&self, req: StartRun) -> Result<String, ApiError> {
        let contract = self.load_generator(&req.generator_id)?;
        match contract.manifest.resolve_inputs(req.inputs.clone()) {
            Ok(_) => {}
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
        }
        let run_id = format!("{}-{}", req.generator_id, uuid::Uuid::now_v7());
        let run_dir = self.inner.runs_dir.join(&run_id);
        let (events, _) = broadcast::channel(512);
        let cancellation = CancellationToken::new();
        let task = Arc::new(Mutex::new(None));
        let inputs = req.inputs;
        let answers = req.answers;
        let confirmations = req.confirmations;
        let priority = req.priority.unwrap_or(0);
        let queued_at = chrono::Utc::now();
        // Phase 1: filesystem preparation happens outside the run-map lock so
        // concurrent runs never block on unrelated directory and journal I/O.
        if let Err(error) = prepare_api_run_directory(&run_dir) {
            return Err(api_internal(error));
        }
        let staged = RunRecord {
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
            queued_at: Some(queued_at),
        };
        if let Err(error) = write_run_event(
            &staged,
            "run_queued",
            json!({
                "run_id": &run_id,
                "generator": format!("{}@{}", contract.manifest.generator.id, contract.manifest.generator.version),
                "generator_path": &contract.root,
                "contract_sha256": &contract.sha256,
                "inputs": &inputs,
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": 1,
                "retain_days": contract.manifest.journal.retain_days,
                "priority": priority,
                "parent_run_id": Value::Null,
            }),
        ) {
            let _ = std::fs::remove_dir_all(&run_dir);
            return Err(api_internal(error));
        }
        // Phase 2: short critical section covering only capacity and registration.
        {
            let mut runs = self.inner.runs.write().await;
            if runs.len() >= self.inner.max_tracked_runs {
                runs.retain(|_, record| !record.state.is_terminal());
            }
            if runs.len() >= self.inner.max_tracked_runs {
                drop(runs);
                let _ = std::fs::remove_dir_all(&run_dir);
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
        let run_id = format!(
            "{}-fork-{}",
            contract.manifest.generator.id,
            uuid::Uuid::now_v7()
        );
        let run_dir = self.inner.runs_dir.join(&run_id);
        let (events, _) = broadcast::channel(512);
        let cancellation = CancellationToken::new();
        let task = Arc::new(Mutex::new(None));
        // Filesystem preparation happens outside the run-map lock so concurrent
        // runs never block on checkpoint copies, hashing, or journal I/O.
        prepare_checkpoint_fork(
            &source_dir,
            source_id,
            &run_dir,
            &run_id,
            request.at_seq,
            &request.state_patch,
        )
        .map_err(api_internal)?;
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
                .map_err(|error| {
                    ApiError::invalid_field("state_patch.inputs", error.to_string())
                })?;
            Ok(inputs)
        })();
        let inputs = match fork_inputs {
            Ok(inputs) => inputs,
            Err(error) => {
                if run_dir.exists()
                    && let Err(cleanup_error) = std::fs::remove_dir_all(&run_dir)
                {
                    return Err(api_internal(format!(
                        "{error}; failed to clean incomplete fork `{run_id}`: {cleanup_error}"
                    )));
                }
                return Err(error);
            }
        };
        let staged = RunRecord {
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
            queued_at: Some(chrono::Utc::now()),
        };
        if let Err(error) = write_run_event(
            &staged,
            "run_queued",
            json!({
                "run_id": &run_id,
                "generator": format!("{}@{}", contract.manifest.generator.id, contract.manifest.generator.version),
                "generator_path": &contract.root,
                "contract_sha256": &contract.sha256,
                "inputs": &staged.inputs,
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": 1,
                "retain_days": contract.manifest.journal.retain_days,
                "priority": staged.priority,
                "parent_run_id": source_id,
            }),
        ) {
            let _ = std::fs::remove_dir_all(&run_dir);
            return Err(api_internal(error));
        }
        // Short critical section covering only capacity and registration.
        {
            let mut runs = self.inner.runs.write().await;
            if runs.len() >= self.inner.max_tracked_runs {
                runs.retain(|_, record| !record.state.is_terminal());
            }
            if runs.len() >= self.inner.max_tracked_runs {
                drop(runs);
                let _ = std::fs::remove_dir_all(&run_dir);
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
        let terminal = match self.inner.runs.read().await.get(id) {
            Some(record) => record.state.is_terminal(),
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
        let disk_state = fold_run_state(&run_dir).map_err(api_internal)?;
        let state = memory
            .as_ref()
            .map(|record| record.state)
            .unwrap_or_else(|| {
                disk_state
                    .terminal
                    .as_ref()
                    .map_or(RunStatus::Interrupted, |terminal| match terminal {
                        qcg_engine::TerminalState::Succeeded => RunStatus::Succeeded,
                        qcg_engine::TerminalState::Failed => RunStatus::Failed,
                        qcg_engine::TerminalState::Canceled => RunStatus::Canceled,
                        qcg_engine::TerminalState::Interrupted => RunStatus::Interrupted,
                    })
            });
        let contract_sha256 = match memory.as_ref() {
            Some(record) => Some(record.contract_sha256.clone()),
            None => Some(read_run_contract_sha256(&run_dir).map_err(api_internal)?),
        };
        let memory_priority = memory.as_ref().map(|record| record.priority);
        let memory_parent = memory
            .as_ref()
            .and_then(|record| record.parent_run_id.clone());
        let (queued_at, queue_position) = if state == RunStatus::Queued {
            let runs = self.inner.runs.read().await;
            let positions = queue_positions(&runs);
            (
                memory
                    .as_ref()
                    .and_then(|record| record.queued_at)
                    .map(|at| at.to_rfc3339()),
                runs.get(&id).and_then(|_| positions.get(&id).copied()),
            )
        } else {
            (None, None)
        };
        Ok(RunSnapshot {
            run_id: id,
            state,
            seq: disk_state.last_seq,
            contract_sha256,
            artifacts,
            question: memory.as_ref().and_then(|record| record.question.clone()),
            confirm: memory.and_then(|record| record.confirm),
            queued_at,
            queue_position,
            priority: memory_priority.unwrap_or(0),
            parent_run_id: memory_parent.or_else(|| {
                read_queued_identity(&run_dir)
                    .map(|(_, parent)| parent)
                    .unwrap_or(None)
            }),
            metrics: read_run_metrics(&run_dir).map_err(api_internal)?,
        })
    }

    /// Cost metrics for one run with a USD estimate and pricing coverage.
    pub async fn run_cost_metrics(&self, id: String) -> Result<RunCostMetrics, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let metrics = read_run_metrics(&run_dir)
            .map_err(api_internal)?
            .unwrap_or_default();
        let state = fold_run_state(&run_dir).map_err(api_internal)?;
        let memory_state = self
            .inner
            .runs
            .read()
            .await
            .get(&id)
            .map(|record| record.state);
        let priced = self.pricing_covered(&run_dir).await.unwrap_or(false);
        Ok(RunCostMetrics {
            run_id: id,
            state: memory_state.unwrap_or_else(|| {
                state
                    .terminal
                    .as_ref()
                    .map_or(RunStatus::Interrupted, |terminal| match terminal {
                        qcg_engine::TerminalState::Succeeded => RunStatus::Succeeded,
                        qcg_engine::TerminalState::Failed => RunStatus::Failed,
                        qcg_engine::TerminalState::Canceled => RunStatus::Canceled,
                        qcg_engine::TerminalState::Interrupted => RunStatus::Interrupted,
                    })
            }),
            metrics: metrics.clone(),
            cost_usd: metrics.cost_microusd as f64 / 1_000_000.0,
            priced,
        })
    }

    /// True when every billed LLM call resolves to contract pricing.
    /// Unpriced calls may understate the total, so callers must surface this.
    async fn pricing_covered(&self, run_dir: &Utf8Path) -> Result<bool, ApiError> {
        let events = read_run_events(run_dir).map_err(api_internal)?;
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
        let live_receiver = self.live_receiver(&id).await.ok();
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
        let (
            contract,
            inputs,
            answers,
            confirmations,
            priority,
            run_dir,
            events,
            cancellation,
            task,
        ) = {
            let mut runs = self.inner.runs.write().await;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
            if let Some(existing) = record.answers.get(&question_id) {
                return if existing == &answer {
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
                .as_ref()
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
            record.answers.insert(question_id, answer);
            record.state = RunStatus::Queued;
            record.queued_at = Some(chrono::Utc::now());
            record.question = None;
            record.confirm = None;
            record.artifacts = None;
            let cancellation = CancellationToken::new();
            record.cancellation = cancellation.clone();
            (
                record.contract.clone(),
                record.inputs.clone(),
                record.answers.clone(),
                record.confirmations.clone(),
                record.priority,
                record.run_dir.clone(),
                record.events.clone(),
                cancellation,
                record.task.clone(),
            )
        };
        self.spawn_engine_run(SpawnRun {
            run_id: id,
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
        Ok(())
    }

    pub async fn confirm(
        &self,
        id: String,
        confirmation_id: String,
        decision: ConfirmDecision,
    ) -> Result<(), ApiError> {
        let (
            contract,
            inputs,
            answers,
            confirmations,
            priority,
            run_dir,
            events,
            cancellation,
            task,
        ) = {
            let mut runs = self.inner.runs.write().await;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
            let approved = decision.decision == ConfirmationDecision::Approve;
            if let Some(existing) = record.confirmations.get(&confirmation_id) {
                return if *existing == approved {
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
            if decision.decision == ConfirmationDecision::Deny {
                record.confirmations.insert(confirm.id.clone(), false);
                record.state = RunStatus::Failed;
                record.confirm = None;
                let denied = record.clone();
                drop(runs);
                // Journal appends happen outside the map lock; the record is
                // already terminally Failed so no other writer can interleave.
                write_run_event(
                    &denied,
                    "side_effect",
                    json!({
                        "kind": confirm.kind,
                        "target": confirm.target,
                        "decision": "denied_by_user",
                    }),
                )
                .map_err(api_internal)?;
                write_run_event(
                    &denied,
                    "run_finished",
                    json!({
                        "status": "failed",
                        "reason": FailureDetail::new(
                            FailureCode::ExecutionFailed,
                            "side effect denied by user",
                        ),
                    }),
                )
                .map_err(api_internal)?;
                return Ok(());
            }
            record.confirmations.insert(confirm.id, true);
            record.state = RunStatus::Queued;
            record.queued_at = Some(chrono::Utc::now());
            record.confirm = None;
            record.artifacts = None;
            let cancellation = CancellationToken::new();
            record.cancellation = cancellation.clone();
            (
                record.contract.clone(),
                record.inputs.clone(),
                record.answers.clone(),
                record.confirmations.clone(),
                record.priority,
                record.run_dir.clone(),
                record.events.clone(),
                cancellation,
                record.task.clone(),
            )
        };
        self.spawn_engine_run(SpawnRun {
            run_id: id,
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
        Ok(())
    }

    pub async fn cancel(&self, id: String) -> Result<(), ApiError> {
        let (task, settled) = {
            let mut runs = self.inner.runs.write().await;
            let record = runs
                .get_mut(&id)
                .ok_or_else(|| api_not_found(format!("run `{id}` was not found")))?;
            if record.state.is_terminal() {
                return Ok(());
            }
            let engine_is_active = record.state == RunStatus::Running;
            record.cancellation.cancel();
            record.state = RunStatus::Canceled;
            record.preempted = false;
            record.question = None;
            record.confirm = None;
            record.artifacts = None;
            if !engine_is_active {
                let settled = record.clone();
                (None, Some(settled))
            } else {
                let task = record
                    .task
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                (task, None)
            }
        };
        if let Some(settled) = settled {
            // The journal append happens outside the map lock; the record is
            // already terminally Canceled so finish_run can only reconcile.
            write_run_event(
                &settled,
                "run_canceled",
                json!({
                    "reason": FailureDetail::new(
                        FailureCode::Canceled,
                        "cancellation requested",
                    ),
                }),
            )
            .map_err(api_internal)?;
            return Ok(());
        }
        if let Some(task) = task {
            task.await.map_err(|error| {
                api_internal(format!(
                    "run `{id}` task failed during cancellation: {error}"
                ))
            })?;
        }
        self.inner.queue_notify.notify_waiters();
        Ok(())
    }

    /// Preempt one running run for an incoming higher-priority run. The victim
    /// keeps its journal and returns to Queued; already finished steps replay
    /// on resume. At most one victim per arrival; equal priorities never
    /// preempt each other.
    async fn preempt_for_priority(&self, priority: i32) {
        let victim = {
            let runs = self.inner.runs.read().await;
            let running = runs
                .iter()
                .filter(|(_, record)| {
                    record.state == RunStatus::Running && record.priority < priority
                })
                .count();
            if running >= self.inner.max_active_runs {
                runs.iter()
                    .filter(|(_, record)| {
                        record.state == RunStatus::Running && record.priority < priority
                    })
                    .min_by(|left, right| {
                        left.1
                            .priority
                            .cmp(&right.1.priority)
                            .then_with(|| right.0.cmp(left.0))
                    })
                    .map(|(run_id, _)| run_id.clone())
            } else {
                None
            }
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
                queued_at: Some(chrono::Utc::now()),
            };
            record.cancellation.cancel();
            record.state = RunStatus::Queued;
            record.queued_at = Some(chrono::Utc::now());
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
        write_run_event(
            &staged,
            "run_queued",
            json!({
                "run_id": id,
                "generator": format!("{}@{}", staged.contract.manifest.generator.id, staged.contract.manifest.generator.version),
                "generator_path": &staged.contract.root,
                "contract_sha256": &staged.contract_sha256,
                "inputs": &staged.inputs,
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": 1,
                "retain_days": staged.contract.manifest.journal.retain_days,
                "priority": staged.priority,
                "parent_run_id": staged.parent_run_id.clone(),
            }),
        )
        .map_err(api_internal)?;
        if let Some(handle) = handle {
            handle.await.map_err(|error| {
                api_internal(format!("run `{id}` task failed during preemption: {error}"))
            })?;
        }
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
        let active = self
            .inner
            .runs
            .read()
            .await
            .iter()
            .filter(|(_, record)| !record.state.is_terminal())
            .map(|(id, record)| (id.clone(), Arc::clone(&record.task)))
            .collect::<Vec<_>>();
        for (id, _) in &active {
            self.cancel(id.clone()).await?;
        }
        for (id, task) in active {
            let Some(handle) = task.lock().unwrap_or_else(PoisonError::into_inner).take() else {
                continue;
            };
            match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(api_internal(format!(
                        "run `{id}` task failed during shutdown: {error}"
                    )));
                }
                Err(_) => {
                    return Err(api_internal(format!(
                        "run `{id}` did not stop within the shutdown deadline"
                    )));
                }
            }
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

    pub async fn read_journal(&self, id: String) -> Result<String, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let limits = JournalLimits::default();
        let path = run_meta_dir(&run_dir).join("journal.jsonl");
        let bytes = match limits.max_total_bytes {
            Some(limit) => {
                let file = tokio::fs::File::open(&path).await.map_err(api_internal)?;
                let mut bytes = Vec::new();
                file.take(limit.saturating_add(1) as u64)
                    .read_to_end(&mut bytes)
                    .await
                    .map_err(api_internal)?;
                if bytes.len() > limit {
                    return Err(ApiError::TooLarge {
                        actual_bytes: bytes.len(),
                        limit_bytes: limit,
                    });
                }
                bytes
            }
            None => tokio::fs::read(&path).await.map_err(api_internal)?,
        };
        String::from_utf8(bytes)
            .map_err(|error| api_internal(format!("journal is not valid UTF-8: {error}")))
    }
}
