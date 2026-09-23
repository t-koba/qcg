//! Run admission, snapshots, subscriptions, and queue positions.
//!
//! Responsibility split (C-5): admission (`probe_admission`,
//! `register_admission`, start/fork), observation (`snapshot`, `subscribe`,
//! `queued_disk_candidates` plus its bounded cache), and lifecycle
//! (`shutdown_active_runs`) share this module; a future split moves the
//! queue cache to `queue_cache.rs` with no behavior change.
use crate::artifacts::{
    api_bad_request, api_internal, api_not_found, check_verified_artifact_hashes,
    collect_verified_artifacts,
};
use crate::lifecycle::SpawnRun;
use crate::run_dirs::{prepare_api_run_directory, prepare_checkpoint_fork, write_run_event};
use crate::summaries::{
    fold_run_state, poll_journal_events, read_optional_output_manifest, read_run_contract_sha256,
    read_run_events, read_run_generator_path, run_meta_dir, run_workspace_dir,
    truncate_trailing_canceled_events,
};
use crate::types::{LocalQcgService, RunBundleParts, RunRecord, RunStoreMode};
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

/// One queue-order entry: run id, priority, and durable queue instant.
/// Shared by the snapshot queue position and its disk-scan cache so the
/// complex tuple type lives in exactly one place (E12).
type QueueOrderEntry = (String, i32, Option<chrono::DateTime<chrono::Utc>>);

/// In-memory admission state seeded from an adopted run's journal so
/// adoption never erases durable answers and approvals (E03). Pending
/// prompts are re-derived by the resumed engine from the same journal
/// instead of being seeded, so the spawned engine and the record never
/// disagree about the current interaction.
#[derive(Debug)]
pub(crate) struct AdoptedRunSeed {
    pub(crate) answers: BTreeMap<String, Value>,
    pub(crate) confirmations: BTreeMap<String, bool>,
    /// Durable admission instant read from the same journal scan, so the
    /// caller never re-scans for it (E03).
    pub(crate) queued_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// The admitted identity an adoption must match exactly. Retries bind the
/// same reserved run id, so every field that defines the admission must
/// agree with the journal instead of being silently replaced. A mismatched
/// contract or inputs is refused instead of silently executing different
/// work under the same id, and an already-settled run converges onto its
/// terminal state instead of spawning a second execution (E03).
pub(crate) struct AdoptedRunIdentity<'a> {
    pub(crate) inputs: &'a BTreeMap<String, Value>,
    pub(crate) contract_sha256: &'a str,
    pub(crate) priority: i32,
    pub(crate) parent: Option<&'a str>,
    pub(crate) answers: &'a BTreeMap<String, Value>,
    pub(crate) confirmations: &'a BTreeMap<String, bool>,
}

/// The ONE seed-from-snapshot function for every adoption path (E03):
/// start, fork, live-hit verification, and rival-race fallback all derive
/// the seed, queue instant, and identity proof from a single already-read
/// snapshot instead of scanning per field. Callers with no snapshot yet
/// (rival registered after the adoption read left it empty) materialize one
/// via `materialize_adopt_snapshot_for_fallback` below, then call this —
/// never a second per-field scan.
/// Seed from an adoption snapshot whose state was already folded by
/// `try_adopt_run_dir_with_snapshot`: no second fold of the same events
/// (E03).
pub(crate) fn seed_adopted_run_from_snapshot(
    snapshot: &crate::run_dirs::AdoptSnapshot,
    expected: &AdoptedRunIdentity<'_>,
) -> Result<Option<AdoptedRunSeed>, ApiError> {
    let Some(state) = snapshot.state.as_ref() else {
        return Err(api_internal(
            "adoption snapshot has no folded state; refusing adoption".to_string(),
        ));
    };
    seed_adopted_run_from_state(state, &snapshot.events, expected)
}

/// Materializes a snapshot for the rival-race fallback: a rival that
/// registered after our adoption read left the snapshot empty, so one
/// bounded read + fold serves verification instead of per-field scans
/// (E03). Fail-closed on unreadable journals.
fn materialize_adopt_snapshot_for_fallback(
    run_dir: &Utf8Path,
) -> Result<crate::run_dirs::AdoptSnapshot, ApiError> {
    let events = crate::summaries::read_journal_events(run_dir).map_err(api_internal)?;
    let state = qcg_engine::RunState::fold_values(&events).map_err(api_internal)?;
    Ok(crate::run_dirs::AdoptSnapshot {
        adopted: true,
        events,
        state: Some(state),
    })
}

fn seed_adopted_run_from_state(
    state: &qcg_engine::RunState,
    journal_values: &[Value],
    expected: &AdoptedRunIdentity<'_>,
) -> Result<Option<AdoptedRunSeed>, ApiError> {
    // One snapshot serves the fold, the identity checks, the HITL seed,
    // and the queued instant below: adoption never pays a scan per field
    // (E03).
    // Identity is verified before the terminal shortcut below: a settled
    // run converges only for its own admission, so reusing a terminal id
    // with different inputs is refused instead of silently aliasing a
    // finished run (E03).
    let run_id = state.run_id.as_deref().unwrap_or("<unknown>");
    match state.contract_sha256.as_deref() {
        Some(sha256) if sha256 == expected.contract_sha256 => {}
        _ => {
            return Err(ApiError::Conflict {
                detail: format!(
                    "run `{run_id}` was admitted with a different contract; refusing adoption"
                ),
            });
        }
    }
    match &state.inputs {
        Some(inputs) if inputs == expected.inputs => {}
        _ => {
            return Err(ApiError::Conflict {
                detail: format!(
                    "run `{run_id}` was admitted with different inputs; refusing adoption"
                ),
            });
        }
    }
    // The admission event itself must carry its priority: journals that
    // predate the field cannot be verified against the retry and are
    // refused instead of matching a convenient default (E03).
    let admission_queued = journal_values.iter().any(|event| {
        event.get("t").and_then(Value::as_str) == Some("run_queued")
            && event.get("priority").and_then(Value::as_i64).is_some()
    });
    if !admission_queued {
        return Err(ApiError::Conflict {
            detail: format!(
                "run `{run_id}` has no verifiable admission priority; refusing adoption"
            ),
        });
    }
    let (recorded_priority, recorded_parent) =
        crate::summaries::read_queued_identity_from_values(journal_values);
    if recorded_priority != expected.priority {
        let expected_priority = expected.priority;
        return Err(ApiError::Conflict {
            detail: format!(
                "run `{run_id}` was admitted with priority {recorded_priority}; refusing adoption at priority {expected_priority}"
            ),
        });
    }
    if let Some(expected_parent) = expected.parent
        && recorded_parent.as_deref() != Some(expected_parent)
    {
        return Err(ApiError::Conflict {
            detail: format!(
                "run `{run_id}` was not forked from `{expected_parent}`; refusing adoption"
            ),
        });
    }
    if expected.parent.is_none() && recorded_parent.is_some() {
        return Err(ApiError::Conflict {
            detail: format!(
                "run `{run_id}` is a fork and cannot be adopted as a plain start; refusing adoption"
            ),
        });
    }
    // Identity comparison uses only what the admission itself carried on
    // `run_queued`: later `user_answered` / `user_confirmed` events are
    // durable acceptances that a retry does not repeat, so they must not
    // turn a legitimate resume into a conflict. Fork journals inherit the
    // source's answers across `run_forked` (that inheritance is the
    // checkpoint), so only the fork's own lifetime is compared.
    let admission_values: Vec<Value> = if expected.parent.is_some() {
        // The fork's own admission is the `run_queued` sequenced after its
        // `run_forked` marker. Without a marker (hand-built or partial
        // journals), only the fork's own `run_queued` counts: admitting
        // the source's `run_queued` into the comparison set would judge
        // the fork by another run's admission answers (E03).
        let fork_markers: Vec<&Value> = journal_values
            .iter()
            .filter(|event| {
                event.get("t").and_then(Value::as_str) == Some("run_forked")
                    && event.get("run_id").and_then(Value::as_str) == Some(run_id)
            })
            .collect();
        // A `run_forked` marker without a sequence number is corrupt: the
        // boundary it should define is unknowable, so fail closed with a
        // clear error instead of silently treating it as sequence 0 (E03).
        if fork_markers
            .iter()
            .any(|event| event.get("seq").and_then(Value::as_u64).is_none())
        {
            return Err(ApiError::Conflict {
                detail: format!(
                    "run `{run_id}` has a fork marker without a sequence number; refusing adoption"
                ),
            });
        }
        let boundary: Option<u64> = fork_markers
            .iter()
            .filter_map(|event| event.get("seq").and_then(Value::as_u64))
            .max();
        journal_values
            .iter()
            .filter(|event| {
                event.get("t").and_then(Value::as_str) == Some("run_queued")
                    && event.get("run_id").and_then(Value::as_str) == Some(run_id)
                    && event
                        .get("seq")
                        .and_then(Value::as_u64)
                        .is_some_and(|seq| boundary.is_none_or(|fork_seq| seq > fork_seq))
            })
            .cloned()
            .collect()
    } else {
        journal_values
            .iter()
            .filter(|event| {
                event.get("t").and_then(Value::as_str) == Some("run_queued")
                    && event.get("run_id").and_then(Value::as_str) == Some(run_id)
            })
            .cloned()
            .collect()
    };
    let (admitted_answers, admitted_confirmations) =
        crate::summaries::read_persisted_hitl_from_values(&admission_values)
            .map_err(api_internal)?;
    if &admitted_answers != expected.answers {
        return Err(ApiError::Conflict {
            detail: format!(
                "run `{run_id}` was admitted with different answers; refusing adoption"
            ),
        });
    }
    if &admitted_confirmations != expected.confirmations {
        return Err(ApiError::Conflict {
            detail: format!(
                "run `{run_id}` was admitted with different confirmations; refusing adoption"
            ),
        });
    }
    if state.terminal.is_some() {
        return Ok(None);
    }
    // The seed itself carries the full durable state, including answers
    // and confirmations accepted after admission, so the resumed engine
    // observes exactly what the API already acknowledged.
    let (answers, confirmations) =
        crate::summaries::read_persisted_hitl_from_values(journal_values).map_err(api_internal)?;
    let queued_at = crate::summaries::read_last_queued_at_from_values(journal_values);
    Ok(Some(AdoptedRunSeed {
        answers,
        confirmations,
        queued_at,
    }))
}

/// Verifies a duplicate admission against the journaled identity before
/// converging onto the live record. The HTTP layer binds retries by
/// idempotency digest, but direct service callers bypass that binding: a
/// live hit with different contract, inputs, priority, parent, answers,
/// or confirmations is refused instead of silently reusing another
/// admission's task, token, and channel (E03). Terminal memory records are
/// never live hits (see live_run_record): they fall through to the disk
/// adopt path, which converges without resurrection only after the same
/// identity proof.
/// Single live-hit verifier for every admission path (E03): the caller
/// holds the adopt snapshot (read before the live check) and verification
/// reuses its pre-folded state instead of rescanning or refolding. The
/// rival-race fallback (empty snapshot) materializes one snapshot via a
/// single read + fold, then calls the same ONE seed function above — no
/// per-field scans, no duplicated 1-line delegations.
fn verify_live_admission_from_snapshot(
    snapshot: &crate::run_dirs::AdoptSnapshot,
    expected: &AdoptedRunIdentity<'_>,
) -> Result<Option<AdoptedRunSeed>, ApiError> {
    seed_adopted_run_from_snapshot(snapshot, expected)
}

/// Derives the canonical fork inputs from one already-folded admission
/// snapshot (E03): the single helper behind the live-hit check and the
/// admission path, so the fork never folds twice for the same inputs.
/// `pub(crate)` so the single-read test proves snapshot reuse without a
/// second journal scan after the adoption read.
pub(crate) fn fork_checkpoint_inputs(
    snapshot: &crate::run_dirs::AdoptSnapshot,
    contract: &Contract,
    source_id: &str,
    at_seq: u64,
) -> Result<BTreeMap<String, Value>, ApiError> {
    let state = snapshot.state.as_ref().ok_or_else(|| {
        api_internal(format!(
            "checkpoint {source_id}@{at_seq} has no folded admission state"
        ))
    })?;
    let inputs = state.inputs.clone().ok_or_else(|| {
        api_internal(format!(
            "checkpoint {source_id}@{at_seq} has no canonical inputs"
        ))
    })?;
    contract
        .manifest
        .resolve_inputs(inputs)
        .and_then(|resolved| {
            qcg_engine::canonical_file_inputs(contract, resolved)
                .map_err(|error| ContractError::Invalid(format!("invalid file input: {error}")))
        })
        .map_err(|error| ApiError::invalid_field("state_patch.inputs", error.to_string()))
}

/// Single live-hit resolver for every fork admission path (E03): the caller
/// holds the adopt snapshot (read before the live check, exactly like start
/// admissions) and the live check reuses it instead of rescanning the
/// journal or refolding the same events.
fn resolve_fork_live_hit(
    run_id: &str,
    snapshot: &crate::run_dirs::AdoptSnapshot,
    adopted: bool,
    contract: &Contract,
    source_id: &str,
    request: &ForkRun,
) -> Result<(), ApiError> {
    if !adopted {
        return Err(ApiError::Conflict {
            detail: format!(
                "run `{run_id}` is live but has no journal; refusing to overwrite it with a fork copy"
            ),
        });
    }
    let adopted_inputs = fork_checkpoint_inputs(snapshot, contract, source_id, request.at_seq)?;
    // The returned seed reuses the snapshot fold; converging callers only
    // need the identity proof, so the seed itself is not retained (E03).
    verify_live_admission_from_snapshot(
        snapshot,
        &AdoptedRunIdentity {
            inputs: &adopted_inputs,
            contract_sha256: &contract.sha256,
            priority: request.priority.unwrap_or(0),
            parent: Some(source_id),
            answers: &request.answers,
            confirmations: &request.confirmations,
        },
    )?;
    Ok(())
}

/// Phase-2b registration verdict (E03).
#[derive(PartialEq, Eq)]
enum AdmissionVerdict {
    /// This admission registered the record and must spawn its engine.
    Inserted,
    /// A rival already registered: converge without spawning. Spawning
    /// with the loser's staged channel and token would diverge from the
    /// registered record (E03); the owner or the resumer drives it.
    Converged,
}

/// Resync cursor after a broadcast lag: always the last position actually
/// delivered, never `delivered + skipped`. Adding the dropped count would
/// fabricate a cursor past real events (e.g. history 1000 + skipped 488 =
/// 1488 skips the real 1001). The client reconnects from the returned
/// position and the journal replay yields the next real event (E12a).
/// The skipped count is taken and explicitly ignored so a future caller
/// cannot reintroduce the addition without touching this signature.
pub(crate) fn lagged_resync_seq(delivered_seq: u64, _skipped: u64) -> u64 {
    delivered_seq
}

/// Phase-2a admission probe verdict (E03).
enum AdmissionProbe {
    /// No rival record, capacity available, server not shutting down.
    Proceed,
    /// A record is already registered: converge onto it.
    Converged,
}

/// Phase-2a admission rejection (E03). Shutdown and capacity share the
/// `Unavailable` surface but differ in cleanup, so they stay distinct
/// here: only directories this admission created are ever removed.
enum ProbeRejection {
    Shutdown,
    Capacity,
}

impl LocalQcgService {
    /// Phase-2a admission probe shared by start and fork (E03). Decides
    /// shutdown, duplicate, and capacity under the run-map lock before the
    /// caller allocates a broadcast channel, a cancellation token, a task
    /// slot, or builds a record, so rejected admissions allocate nothing.
    async fn probe_admission(&self, run_id: &str) -> Result<AdmissionProbe, ProbeRejection> {
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
    async fn register_admission(
        &self,
        run_id: String,
        staged: RunRecord,
        own_dir: Option<&Utf8Path>,
    ) -> Result<AdmissionVerdict, ApiError> {
        let mut runs = self.inner.runs.write().await;
        // Re-check under the admission lock: a shutdown that started
        // between the probe and here must not register a record whose
        // spawn the shutdown guard will refuse (E05).
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
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
    async fn settle_refused_admission(run_dir: &Utf8Path) {
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
        match qcg_engine::JournalWriter::append_single_event(
            &journal,
            run_id,
            "run_interrupted",
            serde_json::json!({
                "reason": {"code": "interrupted", "message": "service shutdown before admission completed"},
            }),
            qcg_engine::JournalLimits::default(),
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
    async fn live_run_record(&self, run_id: &str) -> bool {
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
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
        let mut contract = self.load_generator(&req.generator_id)?;
        // Run metadata is metadata only: it never confers authorization and
        // is bounded so a request cannot grow journals without limit.
        if req.labels.len() > qcg_policy::MAX_RUN_LABELS {
            return Err(ApiError::invalid_field(
                "labels",
                format!("at most {} labels are accepted", qcg_policy::MAX_RUN_LABELS),
            ));
        }
        for (key, value) in &req.labels {
            if key.is_empty()
                || key.len() > qcg_policy::MAX_RUN_LABEL_BYTES
                || value.len() > qcg_policy::MAX_RUN_LABEL_BYTES
            {
                return Err(ApiError::invalid_field(
                    "labels",
                    format!(
                        "label keys and values must be non-empty and at most {} bytes",
                        qcg_policy::MAX_RUN_LABEL_BYTES
                    ),
                ));
            }
        }
        // A run may raise its audit level; the contract and deployment
        // floor keep their ability to raise it further.
        if req.audit_level == Some(qcg_policy::AuditLevel::Standard)
            && contract.manifest.audit.level == qcg_policy::AuditLevel::Minimal
        {
            contract.manifest.audit.level = qcg_policy::AuditLevel::Standard;
        }
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
        // Only one admission may prepare, wipe, or adopt this run id at a
        // time; a concurrent caller fails closed and retries instead of
        // wiping a live prepare or writing a second admission event (E03).
        // The live-hit fast path is re-checked under this lock so a drain
        // racing the check cannot be missed (E05).
        let _admission = crate::run_dirs::try_lock_run_admission(&run_dir)
            .map_err(api_internal)?
            .ok_or_else(|| ApiError::Conflict {
                detail: format!("run `{run_id}` admission is already in progress; retry"),
            })?;
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
                None => return Ok(run_id),
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
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
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
                run_id.clone(),
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
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
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
        let run_dir = self.inner.runs_dir.join(&run_id);
        let _admission = crate::run_dirs::try_lock_run_admission(&run_dir)
            .map_err(api_internal)?
            .ok_or_else(|| ApiError::Conflict {
                detail: format!("run `{run_id}` admission is already in progress; retry"),
            })?;
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
            let state = qcg_engine::RunState::fold_values(&events).map_err(api_internal)?;
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
                None => return Ok(run_id),
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
                    "qcg": env!("CARGO_PKG_VERSION"),
                    "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
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
                run_id.clone(),
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
                "run_id": &run_id,
                "trace_id": qcg_api::trace_id_for_run(&run_id),
                "span_id": qcg_api::span_id_for_seq(next_seq),
                "generator": format!("{}@{}", contract.manifest.generator.id, contract.manifest.generator.version),
                "generator_path": &contract.root,
                "contract_sha256": &contract.sha256,
                "inputs": &inputs,
                "answers": &fork_answers,
                "confirmations": &fork_confirmations,
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
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
        Ok(run_id)
    }

    /// Delete a terminal run directory. Active runs are rejected so deletion
    /// never interrupts execution; cancel first. Missing runs are an error.
    /// A repeated delete after success reports not-found; list first when
    /// retrying unattended cleanup.
    pub async fn delete_run(&self, id: &str) -> Result<(), ApiError> {
        // Deletion during the shutdown drain could remove a directory that
        // shutdown settlement is still writing; refuse it like every other
        // mutating operation (E05).
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
        let run_dir = self.run_dir_for(id).await?;
        // A peer may own execution while this process sees no local task:
        // verify the execution lease and pending cancel mailbox in addition
        // to local terminal state (A02). Deletion before the owner ACKs the
        // stop would orphan a live writer.
        // This pre-check is advisory: deletion proceeds only for terminal
        // runs (checked below under the execution lease), and a cancel
        // arriving for an already-terminal run is a settled no-op, so a
        // publish racing this check loses no effect (E03).
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
        // Re-check under the execution lease: a shutdown that started after
        // the outer gate must not let the drain race a directory removal
        // (E05).
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
        let terminal = match self.inner.runs.read().await.get(id) {
            Some(record) => {
                // A still-running local task means the run is active even if
                // memory state was already marked canceled but not settled.
                if crate::types::task_slot_is_live(&record.task) {
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
    /// One journal read plus one manifest read serve the snapshot, the
    /// inputs, and the verified set: the bundle never re-reads what the
    /// snapshot already observed (E16).
    pub async fn run_bundle_parts(&self, id: &str) -> Result<RunBundleParts, ApiError> {
        let memory = self.inner.runs.read().await.get(id).cloned();
        let run_dir = match &memory {
            Some(record) => record.run_dir.clone(),
            None => self.run_dir_for(id).await?,
        };
        let artifacts = match memory.as_ref().and_then(|record| record.artifacts.clone()) {
            Some(artifacts) => Some(artifacts),
            None => read_optional_output_manifest(&run_dir).map_err(api_internal)?,
        };
        let journal_values =
            crate::summaries::read_journal_events(&run_dir).map_err(api_internal)?;
        let journal_events = journal_values
            .iter()
            .map(|event| qcg_api::RunEvent::from_flat(event).map_err(api_internal))
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = self
            .assemble_snapshot_from_read(
                id.to_string(),
                memory,
                &run_dir,
                &journal_values,
                &journal_events,
                artifacts.clone(),
            )
            .await?;
        let inputs = crate::summaries::read_run_inputs_from_events(&run_dir, &journal_events)
            .map_err(api_internal)?;
        let journal = run_meta_dir(&run_dir).join("journal.jsonl");
        let outputs = artifacts;
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
        let journal_events = journal_values
            .iter()
            .map(|event| qcg_api::RunEvent::from_flat(event).map_err(api_internal))
            .collect::<Result<Vec<_>, _>>()?;
        self.assemble_snapshot_from_read(
            id,
            memory,
            &run_dir,
            &journal_values,
            &journal_events,
            artifacts,
        )
        .await
    }

    /// Snapshot assembly from one already-read journal snapshot. Every
    /// durable derivation (fold, identity, queue instant, metrics) folds
    /// the same in-memory values, so callers never re-scan the journal
    /// per field and the bundle path reuses this single read (E16).
    async fn assemble_snapshot_from_read(
        &self,
        id: String,
        memory: Option<RunRecord>,
        run_dir: &Utf8Path,
        journal_values: &[Value],
        journal_events: &[RunEvent],
        artifacts: Option<OutputManifest>,
    ) -> Result<RunSnapshot, ApiError> {
        let disk_state = qcg_engine::RunState::fold_values(journal_values).map_err(api_internal)?;
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
            None => Some(read_run_contract_sha256(run_dir, journal_events).map_err(api_internal)?),
        };
        let memory_priority = memory.as_ref().map(|record| record.priority);
        let memory_parent = memory
            .as_ref()
            .and_then(|record| record.parent_run_id.clone());
        // Prompt display falls back to the durable pending interaction
        // whenever memory holds none: for a run this process has not
        // rehydrated (a peer-owned or still-adopting run) and for an
        // adopting record whose engine has not re-issued its prompt yet.
        // The durable pending interaction is the same prompt the engine is
        // about to re-issue (same id, journaled continuation), answered
        // prompts clear it in the fold, so displaying it can neither show
        // stale work nor accept work the record cannot run: answers land
        // durably and the engine consumes them on start (E03).
        let durable_pending = || match disk_state.pending.clone() {
            Some(qcg_engine::Interaction::Question { question }) => (Some(question), None),
            Some(qcg_engine::Interaction::Confirmation { confirm }) => (None, Some(confirm)),
            None => (None, None),
        };
        let (question, confirm) = match memory.as_ref() {
            Some(record) => match (record.question.clone(), record.confirm.clone()) {
                (None, None) => durable_pending(),
                found => found,
            },
            None => durable_pending(),
        };
        let state = if state == RunStatus::Queued {
            match (&question, &confirm) {
                (Some(_), _) => RunStatus::Waiting,
                (_, Some(_)) => RunStatus::Confirming,
                _ => state,
            }
        } else {
            state
        };
        let (queued_at, queue_position) = if state == RunStatus::Queued {
            // Display the same durable instant used for ordering; memory is
            // only a fallback when the journal holds no instant (the read
            // above already succeeded, so an unreadable journal cannot
            // occur here).
            let displayed = crate::summaries::read_last_queued_at_from_values(journal_values)
                .or_else(|| memory.as_ref().and_then(|record| record.queued_at))
                .map(|at| at.to_rfc3339());
            let position = self
                .queued_position_for_snapshot(&id, journal_values)
                .await?;
            (displayed, position)
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
            crate::summaries::read_queued_identity_from_values(journal_values);
        Ok(RunSnapshot {
            run_id: id,
            state,
            seq: disk_state.last_seq,
            contract_sha256,
            generator_id,
            artifacts,
            question,
            confirm,
            queued_at,
            queue_position,
            priority: memory_priority.unwrap_or(journal_priority),
            parent_run_id: memory_parent.or(journal_parent),
            metrics: crate::summaries::read_run_metrics_from_view(journal_events, &disk_state)
                .map_err(api_internal)?,
            labels: disk_state.labels.clone(),
        })
    }

    /// Queue position for a snapshot that is already known to be Queued.
    /// Memory and disk queued runs share one FIFO: the position merges the
    /// live map with a bounded disk scan, regardless of where the target
    /// itself lives. Computing memory targets from memory alone would
    /// ignore disk-queued runs ahead of them and pin a stale 304 (E16).
    /// The target's priority and instant come from the caller's single
    /// journal read, never a second scan.
    /// The disk half is served from a 1 s process-wide cache shared across
    /// subscribers (E12): one store scan per second, not one per snapshot.
    /// Memory-tracked runs (every local admission) are always merged fresh,
    /// so local subscribers never observe stale self-positions; only
    /// disk-only peer runs can lag by at most the TTL, which delays
    /// position display without ever misordering execution (the scheduler
    /// never consults this cache).
    async fn queued_position_for_snapshot(
        &self,
        id: &str,
        journal_values: &[Value],
    ) -> Result<Option<usize>, ApiError> {
        // One read lock serves both sets so an admission interleaving
        // between two locks cannot shift followers (E16).
        let (memory_queued, memory_all_ids) = {
            let runs = self.inner.runs.read().await;
            let queued = runs
                .iter()
                .filter(|(_, record)| record.state == RunStatus::Queued)
                .map(|(run_id, record)| (run_id.clone(), record.priority, record.queued_at))
                .collect::<Vec<_>>();
            let all: std::collections::BTreeSet<String> = runs.keys().cloned().collect();
            (queued, all)
        };
        // Exclude every memory-tracked run from the disk candidates, not
        // just queued ones: a Running record's stale disk journal still
        // shows `run_queued`, and counting it would duplicate the live run
        // as an extra queued rival and shift positions (E16).
        let (target_priority, _) =
            crate::summaries::read_queued_identity_from_values(journal_values);
        let target_queued_at = crate::summaries::read_last_queued_at_from_values(journal_values);
        // The target may already be present in the memory set below; push
        // only when absent so followers are never shifted by a duplicate
        // entry (E12/E16). Positions stay exact for every run, not just the
        // target.
        let mut order: Vec<QueueOrderEntry> = memory_queued;
        if !order.iter().any(|(run_id, _, _)| run_id == id) {
            order.push((id.to_string(), target_priority, target_queued_at));
        }
        let runs_dir = self.inner.runs_dir.clone();
        // Disk candidates exclude every memory-tracked run plus the pushed
        // target (for disk-only snapshots): otherwise the target's own
        // directory would be counted twice (E16).
        let mut memory_ids: std::collections::BTreeSet<String> = memory_all_ids;
        for (run_id, _, _) in &order {
            memory_ids.insert(run_id.clone());
        }
        let disk_entries = Self::queued_disk_candidates(&runs_dir, &memory_ids).await?;
        order.extend(disk_entries);
        // Shared queue key with the scheduler, never a second inline
        // ordering that could drift from it (E12/E16).
        order.sort_by(|left, right| {
            crate::queue::queue_key(left.1, left.2, left.0.as_str()).cmp(&crate::queue::queue_key(
                right.1,
                right.2,
                right.0.as_str(),
            ))
        });
        Ok(order
            .iter()
            .position(|(run_id, _, _)| run_id == id)
            .map(|index| index.saturating_add(1)))
    }

    /// Disk half of the queue-position merge, shared across subscribers
    /// through a 1 s process-wide cache (E12): one store scan per second,
    /// not one per snapshot. The cache holds raw on-disk candidates keyed
    /// by runs directory; the caller-side exclusion set (memory-tracked
    /// runs plus the target) is applied fresh on every call, so local
    /// admissions are never stale. Only disk-only peer runs can lag by at
    /// most the TTL, which delays position display without misordering
    /// execution (the scheduler never consults this cache). Stale display
    /// still serves the exact-body validator for up to the TTL by design:
    /// the snapshot ETag digests whatever body is served, so a lagging
    /// position yields a lagging-but-exact validator, never a mismatched
    /// one (E12/E16 trade-off; the strict validator lives in
    /// `qcg_server::conditional_response`). Errors are
    /// never cached: every failure re-scans on the next call.
    async fn queued_disk_candidates(
        runs_dir: &Utf8Path,
        exclude: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<QueueOrderEntry>, ApiError> {
        type CachedOrder = Vec<QueueOrderEntry>;
        // Bounded process-wide cache (E12): at most `CACHE_MAX_DIRS`
        // directory keys; expired entries are evicted on every insert and
        // the whole map is cleared on overflow, so test temp dirs cannot
        // grow it without bound. `tokio::sync::Mutex` (never held across
        // `.await` while locked: each guard drops before the blocking scan
        // below) so no async executor thread is ever blocked on the guard.
        static CACHE: std::sync::LazyLock<
            tokio::sync::Mutex<
                std::collections::HashMap<String, (std::time::Instant, CachedOrder)>,
            >,
        > = std::sync::LazyLock::new(|| tokio::sync::Mutex::new(std::collections::HashMap::new()));
        const CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(1);
        const CACHE_MAX_DIRS: usize = 128;
        let key = runs_dir.as_str().to_string();
        if let Some(cached) = CACHE
            .lock()
            .await
            .get(&key)
            .filter(|(at, _)| at.elapsed() < CACHE_TTL)
            .map(|(_, entries)| entries.clone())
        {
            return Ok(cached
                .into_iter()
                .filter(|(name, _, _)| !exclude.contains(name))
                .collect());
        }
        let scanned: CachedOrder = tokio::task::spawn_blocking({
            let runs_dir = runs_dir.to_path_buf();
            move || Self::scan_queue_disk_candidates(&runs_dir)
        })
        .await
        .map_err(api_internal)??;
        {
            let mut cache = CACHE.lock().await;
            cache.retain(|_, (at, _)| at.elapsed() < CACHE_TTL);
            if cache.len() >= CACHE_MAX_DIRS {
                cache.clear();
            }
            cache.insert(key, (std::time::Instant::now(), scanned.clone()));
        }
        Ok(scanned
            .into_iter()
            .filter(|(name, _, _)| !exclude.contains(name))
            .collect())
    }

    /// One bounded store scan for queue-eligible disk runs (E16). A peer
    /// without a journal file is not a queued run. An unreadable or corrupt
    /// peer journal is skipped with a warning instead of failing an
    /// unrelated run's snapshot: positions stay exact among orderable runs,
    /// and a corrupt journal cannot be ordered anyway (E12). Recovery keeps
    /// the stricter fail-closed rule because it must execute from what it
    /// reads; this read path only displays order.
    /// The started-vs-queued comparison reads both markers from the same
    /// per-peer journal snapshot, so no intra-peer TOCTOU exists; a peer
    /// appending mid-scan only shifts this advisory position within the
    /// 1 s cache bound (E16).
    fn scan_queue_disk_candidates(runs_dir: &Utf8Path) -> Result<Vec<QueueOrderEntry>, ApiError> {
        let mut entries = Vec::new();
        // Bounded like rehydration: an unbounded store scan per snapshot
        // would let run count set request latency (E16). Overflow degrades
        // gracefully: cap peers included with an explicit truncated
        // indicator (warn + capped positions) instead of failing the whole
        // snapshot with ApiError (E16). The DTO (`RunSnapshot`, FOREIGN)
        // carries no truncated flag, so the indicator is a warn log plus
        // this comment; a future DTO field should surface it. Positions
        // from the capped set stay exact among included runs; excluded
        // peers only shift followers within the documented bound.
        const MAX_SNAPSHOT_SCAN_ENTRIES: usize = 10_000;
        let mut scanned = 0_usize;
        let mut truncated = false;
        let read_dir = match std::fs::read_dir(runs_dir) {
            Ok(read_dir) => read_dir,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
            Err(error) => return Err(ApiError::internal(error.to_string())),
        };
        for entry in read_dir {
            scanned = scanned.saturating_add(1);
            if scanned > MAX_SNAPSHOT_SCAN_ENTRIES {
                // Graceful degrade: stop scanning, keep capped peers, flag
                // truncation explicitly instead of failing the snapshot.
                truncated = true;
                break;
            }
            let entry = entry.map_err(|error| ApiError::internal(error.to_string()))?;
            let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
                // Non-UTF8 run dirs never match memory ids via lossy conversion (E16).
                continue;
            };
            let run_dir = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                ApiError::internal(format!("run path is not UTF-8: {}", path.display()))
            })?;
            if !run_dir.is_dir() {
                continue;
            }
            let journal_path = crate::summaries::run_meta_dir(&run_dir).join("journal.jsonl");
            let symlink_meta = match std::fs::symlink_metadata(&journal_path) {
                Ok(meta) => meta,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    tracing::warn!(run_dir = %run_dir, %error, "skipping uninspectable peer journal in queue scan");
                    continue;
                }
            };
            if !symlink_meta.is_file() {
                continue;
            }
            let values = match crate::summaries::read_journal_events(&run_dir) {
                Ok(values) => values,
                Err(error) => {
                    tracing::warn!(run_dir = %run_dir, %error, "skipping unreadable peer journal in queue scan");
                    continue;
                }
            };
            let folded = match qcg_engine::RunState::fold_values(&values) {
                Ok(folded) => folded,
                Err(error) => {
                    tracing::warn!(run_dir = %run_dir, %error, "skipping corrupt peer journal in queue scan");
                    continue;
                }
            };
            if folded.terminal.is_some() || folded.pending.is_some() || folded.cancel_requested {
                continue;
            }
            // Count only actually-queued disk runs: a journal with a
            // `run_started` newer than its latest `run_queued` is
            // executing elsewhere, not queued behind the target (E16).
            // Preempted runs carry a newer `run_queued` and count.
            let mut last_started: Option<u64> = None;
            let mut last_queued: Option<u64> = None;
            for event in &values {
                let seq = event.get("seq").and_then(Value::as_u64);
                match event.get("t").and_then(Value::as_str) {
                    Some("run_started") => last_started = last_started.max(seq),
                    Some("run_queued") => last_queued = last_queued.max(seq),
                    _ => {}
                }
            }
            if last_started > last_queued {
                continue;
            }
            let (priority, _) = crate::summaries::read_queued_identity_from_values(&values);
            let queued_at = crate::summaries::read_last_queued_at_from_values(&values);
            entries.push((file_name, priority, queued_at));
        }
        if truncated {
            // Explicit truncated indicator for the graceful-degrade path
            // above: partial positions (capped peers) plus this warn, never
            // a total snapshot failure (E16). Needs a DTO `truncated` flag
            // (FOREIGN, `qcg-api` owns `RunSnapshot`); until then the warn
            // is the indicator operators observe.
            tracing::warn!(
                runs_dir = %runs_dir,
                scanned,
                included = entries.len(),
                "queue scan truncated at 10000 entries; snapshot positions are partial"
            );
        }
        Ok(entries)
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

    /// Event stream contract: history first, then the live tail. A
    /// broadcast lag ends the live tail with a `lagged` marker carrying
    /// the last actually-delivered seq; the client resubscribes with that
    /// seq as `Last-Event-ID` and the journal replay yields the next real
    /// event, so no event is skipped (E12a). A settled run returns history
    /// only and never pends on the broadcast (E12).
    /// Shared journal poller, one per run. The first subscriber spawns the
    /// underlying poll task, which polls every
    /// the deployment poll cadence (default 250 ms); later subscribers reuse its
    /// broadcast
    /// instead of spawning their own task, so N subscribers cost one poller
    /// (E12). The poller starts from the creating subscriber's cursor;
    /// every subscriber filters by its own history end, so an older cursor
    /// overlaps already-broadcast events (skipped by seq, never missed) and
    /// a newer cursor skips nothing the journal replay did not already
    /// serve (E12). Check-insert is atomic under one lock so concurrent
    /// subscribes never spawn two pollers for the same run, and the exit
    /// protocol below is race-free: exit-removal happens only under the
    /// same lock with a receiver-count re-check, so a concurrent subscribe
    /// either attaches first (keeping this task alive) or finds no entry
    /// (spawning a successor). No subscriber is ever stranded on an exited
    /// poller and no transient double poller ever broadcasts (E12).
    fn shared_poll_receiver(
        &self,
        run_dir: Utf8PathBuf,
        run_id: String,
        start_seq: u64,
    ) -> broadcast::Receiver<RunEvent> {
        let (sender, receiver) =
            broadcast::channel(self.inner.deployment_policy.live_event_channel_capacity);
        {
            let mut pollers = self
                .inner
                .journal_pollers
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            // Singleflight under one lock (E12): at most one current poller
            // per run. A present entry is reused by presence, not by
            // receiver count: the exit protocol below removes its entry
            // only under this same lock with a zero re-check, so a present
            // entry always belongs to a task that is alive or will stay
            // alive for this attach.
            if let Some(existing) = pollers.get(&run_id) {
                return existing.subscribe();
            }
            pollers.insert(run_id.clone(), sender.clone());
        }
        let service = self.clone();
        let shutdown = self.inner.shutdown.clone();
        let poll_interval_millis = self.inner.deployment_policy.journal_poll_interval_millis;
        tokio::spawn(async move {
            let mut poll_stream = poll_journal_events(
                run_dir,
                run_id.clone(),
                start_seq,
                poll_interval_millis,
                shutdown,
            );
            use futures_util::StreamExt as _;
            // The poll task owns one sender clone; the map owns the other.
            // Cleanup below removes the map entry only when it still points
            // to this task's channel (same_channel), so a successor poller
            // is never deleted (E12). Every exit path cleans up when still
            // ours: leaving a dead channel behind would let the next
            // subscribe reuse a poller that can never deliver the terminal
            // event.
            let run_id_for_cleanup = run_id.clone();
            let service_for_cleanup = service.clone();
            let cleanup = |sender: &broadcast::Sender<RunEvent>| {
                let mut pollers = service_for_cleanup
                    .inner
                    .journal_pollers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if pollers
                    .get(&run_id_for_cleanup)
                    .is_some_and(|existing| existing.same_channel(sender))
                {
                    pollers.remove(&run_id_for_cleanup);
                }
            };
            // Returns true when this task must exit: receiverless under the
            // pollers lock with the entry still ours. The lock pairs with
            // the attach in `shared_poll_receiver` above: a subscribe that
            // attached first raised the count (this task stays alive for
            // it); one that locks after the removal finds no entry and
            // spawns a successor (E12).
            let service_for_exit = service.clone();
            let run_id_for_exit = run_id.clone();
            let should_exit = |sender: &broadcast::Sender<RunEvent>| {
                if sender.receiver_count() != 0 {
                    return false;
                }
                let mut pollers = service_for_exit
                    .inner
                    .journal_pollers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if sender.receiver_count() == 0
                    && pollers
                        .get(&run_id_for_exit)
                        .is_some_and(|existing| existing.same_channel(sender))
                {
                    pollers.remove(&run_id_for_exit);
                    true
                } else {
                    false
                }
            };
            while let Some(event) = poll_stream.next().await {
                let is_terminal = qcg_api::is_terminal_event_kind(event.kind.as_str());
                // Exit early when no subscriber remains; the exit above is
                // race-free, so a concurrent attach keeps this task alive
                // instead of stranding on an exited poller (E12).
                if should_exit(&sender) {
                    return;
                }
                // A send fails only with zero receivers. A concurrent
                // attach that landed between the check and the send made
                // the send succeed, so a failure still means receiverless:
                // re-confirm under the lock and exit, otherwise redeliver
                // the still-owned event to the newcomer instead of
                // dropping it.
                if let Err(error) = sender.send(event) {
                    if should_exit(&sender) {
                        return;
                    }
                    if sender.send(error.0).is_err() && should_exit(&sender) {
                        return;
                    }
                }
                if is_terminal {
                    break;
                }
            }
            // Terminal runs never need a poller again (future subscribes
            // return history-only), so remove the entry to avoid leaking one
            // sender per settled run. Non-terminal exits (shutdown close,
            // failure close) also remove when still ours so a dead channel
            // is never reused (E12).
            cleanup(&sender);
        });
        receiver
    }

    /// Event stream contract: history first, then the live tail. A
    /// broadcast lag ends the live tail with a `lagged` marker carrying
    /// the last actually-delivered seq; the client resubscribes with that
    /// seq as `Last-Event-ID` and the journal replay yields the next real
    /// event, so no event is skipped (E12a). A `stream_error` marker ends
    /// the tail after delivery so failure-close is distinguishable from
    /// terminal-close (E05). A settled run returns history only and never
    /// pends on the broadcast (E12).
    /// Cursor-0 subscription for non-SSE callers (tests, internal
    /// tails): replays the full history. SSE callers use
    /// `subscribe_with_cursor` with their `Last-Event-ID` (E12).
    pub async fn subscribe(&self, id: String) -> Result<BoxStream<'static, RunEvent>, ApiError> {
        self.subscribe_with_cursor(id, 0).await
    }

    /// Event stream from a client cursor. A cursor ahead of the known
    /// history replays from the start: the journal cannot shrink, so the
    /// client never saw those events and skipping them would lose real
    /// events forever (E12). A cursor behind replays from the journal;
    /// markers always pass.
    /// Cursor-failure policy (E12): `Last-Event-ID` is client-controlled
    /// (`run_detail` is FOREIGN). Empty, missing, or garbage cursors fail
    /// toward REPLAY (never skip): unparseable values read as 0 (full
    /// replay) and future values clamp to 0 below, so a corrupt cursor can
    /// only duplicate (filtered by seq) never lose real events.
    pub async fn subscribe_with_cursor(
        &self,
        id: String,
        after_seq: u64,
    ) -> Result<BoxStream<'static, RunEvent>, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        // Shared mode always follows the durable journal (~250ms poll) so
        // every subscriber observes identical progress even when ownership
        // changes mid-run (HITL hand-off); only an Exclusive store owns its
        // local broadcast, so the mode check alone decides here (E12b).
        // This is a deliberate correctness-first trade-off, not a missing
        // optimization: attaching shared subscribers to a local broadcast
        // would pin them to one owner generation and stall them across a
        // hand-off, while the journal poll reflects every generation by
        // construction. The cost is bounded poll latency, never staleness:
        // an HITL answer lands on both streams up to one 250 ms tick plus
        // dispatch later, and liveness timeouts (not latency bounds) are
        // what tests pin (E12).
        // Disk-only runs (no memory record) have no live channel and always
        // use the shared poller. A missing memory record is an expected
        // fallback to the shared poller, not a hidden failure (E12).
        // Store-lock participation (E12): this subscribe path deliberately
        // does NOT join the runs-directory store lock. The store lock
        // serializes store writers (exclusive boot vs shared peer boots,
        // held for the process lifetime at construction); subscribing is a
        // read-only observation that must keep serving while any owner
        // writes. Per-run authority stays with the execution lease plus the
        // journal lock, and history always re-derives from journal truth,
        // so an unscannable journal fails the subscribe instead of serving
        // a stale "no events" view.
        // No pre-created poller exists here (E12): the shared poller is
        // created AFTER the history read and terminal check below with the
        // clamped history end, so terminal-known-upfront streams never pay
        // a zero-receiver futile poll, and the start seq never uses the
        // pre-clamp cursor (which would miss events on future cursors).
        // The Exclusive live receiver stays pre-attached (needed to avoid
        // losing broadcasts between history read and subscribe); shared
        // mode has no live broadcast to pre-attach.
        // The Exclusive live receiver is attached BEFORE the history read:
        // events broadcast between the history read and the subscription
        // would otherwise belong to neither half and be lost (E12). The
        // live tail below filters by the history end, so early duplicates
        // are skipped, never missed.
        let live_receiver = if self.inner.run_store_mode == RunStoreMode::Exclusive {
            self.live_receiver(&id).await.ok()
        } else {
            None
        };
        // Owner pinning for the Exclusive live tail (E12): capture the
        // memory owner at attach. An empty attach-time owner means the run
        // was rehydrated from disk but never claimed in this process yet:
        // pinning it would mistake the local spawn's owner claim for a
        // hand-off and cut resume-following streams. Only a known (non-empty)
        // owner pins; the same-process claim reuses the same broadcast
        // channel and needs no re-pin. The live tail below re-checks the
        // owner per event and ends with a `lagged` marker on hand-off, so
        // the client resubscribes and re-resolves instead of stalling on a
        // previous owner's channel. Owner ids are unique per process boot,
        // so an owner change fully captures a hand-off; same-owner restarts
        // reuse the same broadcast channel and need no re-pin. Shared-mode
        // subscribers need no pinning: the journal poll reflects every
        // generation by construction.
        let pinned_owner: Option<String> = if live_receiver.is_some() {
            self.inner
                .runs
                .read()
                .await
                .get(&id)
                .map(|record| record.owner_id.clone())
                .filter(|owner| !owner.is_empty())
        } else {
            None
        };
        let history = read_run_events(&run_dir).map_err(ApiError::from)?;
        // History length is bounded by the run's own journal, itself
        // bounded by step budgets and retention: no separate history cap is
        // enforced here, and the live tail filters by seq, not by count.
        // Cost note (E16): history plus the snapshot SHA256 is O(journal
        // size), so a huge journal near `JournalLimits` delays
        // subscribe/snapshot here with no timeout — measure before adding
        // one.
        let history_last_seq = history.last().map_or(0, |event| event.seq);
        // A cursor ahead of the known history never saw those events:
        // replay from the start instead of skipping real events forever. A
        // cursor behind replays the unseen tail. (Retention GC can shrink
        // the journal underneath a cursor; the shared poller resets its
        // byte offset on shrink and the delivered-seq filter prevents
        // duplicates, while replay-from-start here stays correct.)
        // Markers always pass (E12).
        let clamped_after = if after_seq > history_last_seq {
            tracing::warn!(run_id = %id, after_seq, history_last_seq, "future event cursor clamped to replay history");
            0
        } else {
            after_seq
        };
        // History below the cursor is already seen: serve only the unseen
        // tail, so a stale cursor replays and a future cursor (clamped
        // above) never skips (E12). No marker arms here: markers are
        // synthesized, never journaled, so the journal read cannot yield
        // them; the live tail below handles markers explicitly.
        let history: Vec<RunEvent> = history
            .into_iter()
            .filter(|event| event.seq > clamped_after)
            .collect();
        // A run that already settled needs no live tail: attaching the
        // broadcast would pend forever on a stream that can never deliver
        // again (E12). History alone is the complete stream.
        if history
            .last()
            .is_some_and(|event| qcg_api::is_terminal_event_kind(event.kind.as_str()))
        {
            return Ok(futures_util::stream::iter(history).boxed());
        }
        let history_stream = futures_util::stream::iter(history);
        let lag_id = id.clone();
        // The shared poller is created AFTER the terminal check above with
        // the clamped history end (`history_last_seq`): terminal-known
        // streams return history-only without ever creating a poller (no
        // zero-receiver futile poll), and every live tail shares one start
        // seq instead of pre-clamp vs fallback inconsistency (E12). N
        // subscribers share one underlying poll task instead of each
        // spawning their own. Exclusive disk-only runs (no memory record,
        // hence no live channel) use the same lazily created poller here.
        let receiver = match live_receiver {
            Some(receiver) => receiver,
            None => self.shared_poll_receiver(run_dir, id.clone(), history_last_seq),
        };
        // Owner watch for the Exclusive live tail: `Some` only when pinned
        // at attach above. Shared-poller tails carry `None` (the journal
        // reflects every generation by construction).
        let owner_watch: Option<(LocalQcgService, String, String)> =
            pinned_owner.map(|owner| (self.clone(), id.clone(), owner));
        let live_stream = {
            // Each subscriber filters by its own history position. Skipped
            // history must not end the stream: `scan` returning `None`
            // terminates, so an unfold loop skips stale seqs and only ends
            // after the terminal, lagged, or failure marker (E12b). The
            // poller starts at or before this subscriber's history end, so
            // the skip loop only handles overlap, not full replays (E12).
            // History carries no marker arms by construction: markers are
            // synthesized, never journaled, so the journal read cannot
            // yield them; the live tail below handles markers explicitly
            // (E12).
            let rx = BroadcastStream::new(receiver);
            futures_util::stream::unfold(
                (rx, history_last_seq, false, lag_id, owner_watch),
                |(mut rx, mut delivered_seq, mut done, lag_id, owner_watch)| async move {
                    use futures_util::StreamExt as _;
                    if done {
                        return None;
                    }
                    // Owner hand-off check for pinned Exclusive tails: when
                    // the memory owner no longer matches the attach-time
                    // owner (or the record is gone), end with a `lagged`
                    // marker at the last delivered position. The client
                    // resubscribes and re-resolves from journal truth
                    // instead of stalling on the previous owner's channel
                    // (E12).
                    if let Some((service, run_id, expected)) = &owner_watch {
                        let current = service.inner.runs.read().await;
                        let handed_off = current
                            .get(run_id.as_str())
                            .is_none_or(|record| record.owner_id != *expected);
                        drop(current);
                        if handed_off {
                            tracing::info!(run_id = %run_id, "execution owner changed during live tail; ending stream for resubscribe");
                            let lagged = RunEvent::lagged(
                                lag_id.clone(),
                                lagged_resync_seq(delivered_seq, 0),
                            );
                            return Some((
                                lagged,
                                (rx, delivered_seq, true, lag_id, owner_watch),
                            ));
                        }
                    }
                    loop {
                        let next = rx.next().await?;
                        match next {
                            // No `lagged` arm here by construction (E12):
                            // `lagged` markers are synthesized, never
                            // journaled and never broadcast through these
                            // channels — a broadcast lag surfaces as the
                            // `Err(Lagged)` arm below, and the shared
                            // poller forwards only journal events plus the
                            // `stream_error` failure marker. The journal
                            // history above carries no marker arms either.
                            Ok(event)
                                if event.seq > delivered_seq
                                    || event.kind.as_str() == "stream_error" =>
                            {
                                // The failure marker carries the last
                                // delivered seq rather than a new one, so
                                // only advance on real journal events.
                                if event.kind.as_str() != "stream_error" {
                                    delivered_seq = event.seq;
                                }
                                // End the live stream on the shared terminal set
                                // and on failure markers, so all layers agree and
                                // the stream closes even when the SSE wrapper is
                                // bypassed (E12/E05).
                                if qcg_api::is_terminal_event_kind(event.kind.as_str())
                                    || event.kind.as_str() == "stream_error"
                                {
                                    done = true;
                                }
                                return Some((
                                    event,
                                    (rx, delivered_seq, done, lag_id, owner_watch),
                                ));
                            }
                            Ok(_) => continue,
                            Err(
                                tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(
                                    skipped,
                                ),
                            ) => {
                                // Never fabricate a cursor from the dropped
                                // count: report the last position actually
                                // delivered and end the stream. The client
                                // reconnects and the journal replay resumes
                                // from that real position, so no event is
                                // skipped (E12a).
                                done = true;
                                let lagged = RunEvent::lagged(
                                    lag_id.clone(),
                                    lagged_resync_seq(delivered_seq, skipped),
                                );
                                return Some((
                                    lagged,
                                    (rx, delivered_seq, done, lag_id, owner_watch),
                                ));
                            }
                        }
                    }
                },
            )
            .boxed()
        };
        Ok(history_stream.chain(live_stream).boxed())
    }

    pub async fn answer(
        &self,
        id: String,
        question_id: String,
        payload: AnswerPayload,
    ) -> Result<(), ApiError> {
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
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
            // Re-check under the admission lock: a shutdown that started
            // between the outer check and this block must not accept an
            // answer it can no longer execute (E05).
            if self.is_shutting_down() {
                return Err(ApiError::Unavailable {
                    detail: "server is shutting down".into(),
                });
            }
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
            // Resolve the prompt from memory, falling back to the durable
            // pending interaction when the record has none yet. An
            // adopting record whose engine has not re-issued its prompt
            // still carries the journaled question (same id, journaled
            // continuation): refusing the answer would force clients to
            // poll until the engine starts, and the journal precondition
            // below still verifies id and generation atomically (E03).
            let question = match (&record.state, record.question.clone()) {
                (RunStatus::Waiting, Some(question)) if question.id == question_id => question,
                _ => {
                    let observed = fold_run_state(&record.run_dir).map_err(api_internal)?;
                    match observed.pending {
                        Some(qcg_engine::Interaction::Question { question })
                            if question.id == question_id =>
                        {
                            question
                        }
                        _ => {
                            if record.state != RunStatus::Waiting {
                                return Err(api_bad_request(format!(
                                    "run `{id}` is not waiting for user input"
                                )));
                            }
                            let question = record.question.clone().ok_or_else(|| {
                                api_bad_request(format!("run `{id}` has no question"))
                            })?;
                            return Err(api_bad_request(format!(
                                "answer was for `{}`, but run is waiting for `{}`",
                                question_id, question.id
                            )));
                        }
                    }
                }
            };
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
                        // The answer was just journaled above: no admission
                        // snapshot exists, so the spawn reads the journal
                        // once (E03).
                        journal_snapshot: None,
                        cancellation,
                        task: record.task.clone(),
                    }))
                }
            }
        };
        match outcome {
            AnswerDecision::Spawn(request) => {
                self.clone().spawn_engine_run(*request).await;
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
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
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
            // Re-check under the admission lock: a shutdown that started
            // between the outer check and this block must not accept a
            // decision it can no longer execute (E05).
            if self.is_shutting_down() {
                return Err(ApiError::Unavailable {
                    detail: "server is shutting down".into(),
                });
            }
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
            // Full-id string match is the corruption gate: a malformed or
            // foreign confirmation id never equals the pending id, so it
            // fails here as Conflict instead of aliasing another scope
            // (Q1). No separate corrupt-vs-mismatch branch is needed.
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
                    // The decision was just journaled above: no admission
                    // snapshot exists, so the spawn reads the journal once
                    // (E03).
                    journal_snapshot: None,
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
                self.clone().spawn_engine_run(*request).await;
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
        // A cancel accepted during the shutdown drain would race the
        // interruption settlement and journal a competing terminal event;
        // refuse it like every other mutating operation (E05). Terminal
        // runs stay idempotent under shutdown: re-cancelling a settled run
        // reports success without journaling (E05).
        if self.is_shutting_down()
            && let Ok(run_dir) = self.run_dir_for(&id).await
            && fold_run_state(&run_dir)
                .map(|state| state.terminal.is_some())
                .unwrap_or(false)
        {
            return Ok(());
        }
        if self.is_shutting_down() {
            return Err(ApiError::Unavailable {
                detail: "server is shutting down".into(),
            });
        }
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

    /// Preempt one running run for an incoming higher-priority run. The victim
    /// keeps its journal and returns to Queued; already finished steps replay
    /// on resume. At most one victim per arrival; equal priorities never
    /// preempt each other.
    async fn preempt_for_priority(&self, priority: i32) {
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
                "qcg": env!("CARGO_PKG_VERSION"),
                "schema_version": qcg_api::JOURNAL_SCHEMA_VERSION,
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
            // The preempted engine task records its own cancellation before
            // exiting. That bookkeeping event would read as terminal on
            // resume, so drop it: the requeue above is the true outcome and
            // no terminal settlement ran.
            truncate_trailing_canceled_events(&record.run_dir).map_err(api_internal)?;
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

    /// Settles a run with no live local engine task as interrupted during
    /// shutdown: lease-gated drain, no double terminal, memory retired.
    /// Shared by the task-less memory path, the aborted-task path, and the
    /// disk-only orphan pass below (E05).
    async fn settle_shutdown_without_task(
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
        let settled = fold_run_state(&record.run_dir)
            .map(|state| state.terminal.is_some())
            .map_err(api_internal)?;
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
                        let settled = fold_run_state(&record.run_dir)
                            .map(|state| state.terminal.is_some())
                            .map_err(api_internal)?;
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
            let state = match fold_run_state(&run_dir) {
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
            if fold_run_state(&run_dir)
                .map(|state| state.terminal.is_some())
                .map_err(api_internal)?
            {
                continue;
            }
            let journal = crate::summaries::run_meta_dir(&run_dir).join("journal.jsonl");
            qcg_engine::JournalWriter::append_single_event(
                &journal,
                &id,
                "run_interrupted",
                serde_json::json!({
                    "reason": {"code": "interrupted", "message": "service shutdown"},
                }),
                qcg_engine::JournalLimits::default(),
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
            qcg_container::await_outstanding_cleanups(std::time::Duration::from_secs(65)).await;
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
        let audit_path = path.with_file_name("audit.jsonl");
        let (audit, audit_len) = match tokio::fs::File::open(&audit_path).await {
            Ok(file) => {
                let len = file.metadata().await.map_err(api_internal)?.len();
                (Some(file), len)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (None, 0),
            Err(error) => return Err(api_internal(error)),
        };
        Ok(crate::types::JournalStream {
            file,
            len,
            limit: limits.max_total_bytes,
            audit,
            audit_len,
        })
    }
}

#[cfg(test)]
mod lagged_tests {
    use super::lagged_resync_seq;

    #[test]
    fn lagged_never_fabricates_a_cursor_past_real_events() {
        // E12a: history 1000 + skipped 488 must resync from 1000 (the last
        // position actually delivered), never 1488. The client reconnects
        // from the returned cursor and the journal replay yields the real
        // 1001 next.
        assert_eq!(lagged_resync_seq(1000, 488), 1000);
        let next_real = lagged_resync_seq(1000, 488) + 1;
        assert_eq!(next_real, 1001);
    }

    #[tokio::test]
    async fn broadcast_overflow_reports_delivered_not_skipped() {
        // E12a: force a real broadcast lag and assert the resync cursor is
        // the delivered position, not delivered + skipped. The receiver
        // must exist before the overflow so it actually lags.
        let (sender, _) = tokio::sync::broadcast::channel::<u64>(1);
        let mut receiver = sender.subscribe();
        for seq in [1001_u64, 1002, 1003] {
            let _ = sender.send(seq);
        }
        // Receiver missed earlier sends; next recv reports the drop count.
        // Regardless of the count, resync must be the last delivered (1000).
        let delivered = 1000_u64;
        match receiver.try_recv() {
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                assert!(skipped >= 1, "overflow must report skipped");
                assert_eq!(
                    lagged_resync_seq(delivered, skipped),
                    delivered,
                    "resync must not add skipped ({skipped}) to delivered"
                );
                assert!(
                    lagged_resync_seq(delivered, skipped) < 1001,
                    "the next real event 1001 must not be skipped"
                );
            }
            other => panic!("overflow must lag, got: {other:?}"),
        }
    }
}
