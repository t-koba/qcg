//! Run admission, snapshots, subscriptions, and queue positions.
//!
//! Responsibility split (C-5): admission (`probe_admission`,
//! `register_admission`, start/fork), observation (`snapshot`, `subscribe`,
//! `queued_disk_candidates` plus its bounded cache), and lifecycle
//! (`shutdown_active_runs`) share this module; a future split moves the
//! queue cache to `queue_cache.rs` with no behavior change.
use crate::artifacts::{
    api_bad_request, api_internal, api_not_found, check_verified_artifact_hashes,
    collect_verified_artifacts, is_safe_run_id,
};
use crate::lifecycle::SpawnRun;
use crate::run_dirs::{
    is_regular_directory, prepare_api_run_directory, prepare_checkpoint_fork, write_run_event,
};
use crate::summaries::{
    poll_journal_events, read_optional_output_manifest, read_run_generator_path, run_meta_dir,
    run_workspace_dir, truncate_trailing_canceled_events,
};
use crate::types::{LocalService, RunBundleParts, RunRecord, RunStoreMode};
use api::{
    AnswerPayload, ApiError, ConfirmDecision, ConfirmationDecision, ForkRun, RunCostMetrics,
    RunSnapshot, RunStatus, StartRun,
};
use api::{RunEvent, RunEventData};
use camino::{Utf8Path, Utf8PathBuf};
use contract::{Contract, ContractError, validate_form_values};
use engine::RunState;
use engine::{JournalLimits, read_output_manifest, resolve_artifact_path};
use futures_util::{StreamExt as _, stream::BoxStream};
use model::{FailureCode, FailureDetail, OutputArtifact, OutputManifest};
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
#[derive(Debug, Clone)]
pub(crate) struct QueueCache {
    at: std::time::Instant,
    entries: Vec<QueueOrderEntry>,
    incomplete: bool,
}
struct SnapshotRead<'a> {
    events: &'a [RunEvent],
    state: &'a RunState,
}

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
    let state = engine::RunState::fold_values(&events).map_err(api_internal)?;
    Ok(crate::run_dirs::AdoptSnapshot {
        adopted: true,
        events,
        state: Some(state),
    })
}

fn seed_adopted_run_from_state(
    state: &engine::RunState,
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
    let (recorded_priority, recorded_parent) = (state.priority, state.parent_run_id.clone());
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
    // Folded from the admission events alone: a retry is compared against what
    // admission carried, not against answers accepted since.
    let admitted = RunState::fold_values(&admission_values).map_err(api_internal)?;
    let (admitted_answers, admitted_confirmations) = (admitted.answers, admitted.confirmations);
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
    // The seed carries the folded HITL maps and queue instant, so no second
    // scan of the same read is needed.
    let answers = state.answers.clone();
    let confirmations = state.confirmations.clone();
    let queued_at = state
        .queued_at
        .as_deref()
        .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
        .map(|at| at.with_timezone(&chrono::Utc));
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
            engine::canonical_file_inputs(contract, resolved)
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

mod admission;
mod artifacts;
mod cancellation;
mod interaction;
mod preemption;
mod query;
mod shutdown;
mod subscription;

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
