use super::super::run_dirs::{is_regular_directory, is_regular_file, journal_is_empty};
use super::super::types::{RunRecord, ServiceError};
use crate::effects::{EffectCandidates, EffectProvenance};
use api::{RunEvent, RunStatus};
use camino::{Utf8Path, Utf8PathBuf};
use engine::{Interaction, RunState, read_output_manifest};
use model::OutputManifest;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

use super::reads::read_journal_events;
use super::summary::run_meta_dir;

pub(crate) fn fold_run_state(run_dir: &Utf8Path) -> Result<RunState, ServiceError> {
    RunState::fold_journal(&run_meta_dir(run_dir).join("journal.jsonl"))
        .map_err(ServiceError::invalid)
}

/// Latest durable FIFO key: the explicit `queued_at` payload field
/// when present, else the event timestamp. Memory, display, and recovery
/// all derive from this single source so requeue order survives restarts
/// without per-process re-stamping (C04).
pub(crate) fn read_last_queued_at(run_dir: &Utf8Path) -> Option<chrono::DateTime<chrono::Utc>> {
    match crate::summaries::read_journal_events(run_dir) {
        Ok(events) => read_last_queued_at_from_values(&events),
        // Callers treat absence as "no recorded instant" with a memory
        // fallback; an unreadable journal is therefore logged, never
        // silently equated with absence.
        Err(error) => {
            tracing::warn!(run_dir = %run_dir, %error, "queued-at scan failed; no durable instant");
            None
        }
    }
}

/// Latest durable FIFO key from already-read journal values. Malformed
/// timestamps are skipped toward older events instead of voiding the
/// whole lookup: one corrupt event must not erase the surviving order.
pub(crate) fn read_last_queued_at_from_values(
    events: &[serde_json::Value],
) -> Option<chrono::DateTime<chrono::Utc>> {
    // Requeue order must survive restarts. Answers and confirmations resume
    // execution without appending a fresh run_queued, so their durable
    // timestamps are the requeue basis together with run_queued (C04).
    events
        .iter()
        .rev()
        .filter(|event| {
            matches!(
                event.get("t").and_then(serde_json::Value::as_str),
                Some("run_queued" | "user_answered" | "user_confirmed")
            )
        })
        .filter_map(|event| {
            event
                .get("queued_at")
                .and_then(serde_json::Value::as_str)
                .or_else(|| event.get("ts").and_then(serde_json::Value::as_str))
        })
        .filter_map(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|ts| ts.with_timezone(&chrono::Utc))
        .next()
}

/// H02: resolve this run's journaled audit raise, fencing copied source
/// values at `run_forked`. Null requeues retain admission policy; unknown
/// levels do not add a raise.
pub(crate) fn audit_raise_from_values(events: &[serde_json::Value]) -> Option<policy::AuditLevel> {
    events
        .iter()
        .rev()
        .take_while(|event| event.get("t").and_then(Value::as_str) != Some("run_forked"))
        .filter(|event| event.get("t").and_then(serde_json::Value::as_str) == Some("run_queued"))
        .find_map(|event| match event.get("audit_raise") {
            Some(serde_json::Value::String(level)) => match level.as_str() {
                "standard" => Some(policy::AuditLevel::Standard),
                "minimal" => Some(policy::AuditLevel::Minimal),
                _ => None,
            },
            _ => None,
        })
}

pub(crate) fn read_optional_output_manifest(
    run_dir: &Utf8Path,
) -> Result<Option<OutputManifest>, ServiceError> {
    match read_output_manifest(&run_meta_dir(run_dir)) {
        Ok(manifest) => Ok(Some(manifest)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ServiceError::Io(error)),
    }
}

pub(crate) fn status_from_journal(status: &str) -> Result<RunStatus, ServiceError> {
    match status {
        "queued" => Ok(RunStatus::Queued),
        "running" => Ok(RunStatus::Running),
        "waiting" => Ok(RunStatus::Waiting),
        "confirming" => Ok(RunStatus::Confirming),
        "cancel_requested" => Ok(RunStatus::CancelRequested),
        "success" => Ok(RunStatus::Succeeded),
        "failed" => Ok(RunStatus::Failed),
        "canceled" => Ok(RunStatus::Canceled),
        "interrupted" => Ok(RunStatus::Interrupted),
        _ => Err(ServiceError::Invalid(format!(
            "unknown journal run status `{status}`"
        ))),
    }
}

/// Strict construction-time recovery: any unreadable entry refuses startup.
/// Busy shards are skipped for this snapshot and returned as candidates to revalidate.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "Recovery wrapper without effect discovery")
)]
pub(crate) fn rehydrate_runs(
    runs_dir: &Utf8Path,
    max_tracked_runs: usize,
    max_scan_entries: usize,
    live_event_channel_capacity: usize,
) -> Result<BTreeMap<String, RunRecord>, ServiceError> {
    rehydrate_runs_with_effects(
        runs_dir,
        max_tracked_runs,
        max_scan_entries,
        live_event_channel_capacity,
    )
    .map(|(runs, _)| runs)
}

/// Startup recovery: also reports every run directory (terminal or direct
/// included) whose folded journal still holds an unconfirmed container
/// effect, or whose admission shard was busy. Candidates must be revalidated.
pub(crate) fn rehydrate_runs_with_effects(
    runs_dir: &Utf8Path,
    max_tracked_runs: usize,
    max_scan_entries: usize,
    live_event_channel_capacity: usize,
) -> Result<(BTreeMap<String, RunRecord>, EffectCandidates), ServiceError> {
    let mut effects = EffectCandidates::new();
    scan_runs(
        runs_dir,
        max_tracked_runs,
        max_scan_entries,
        live_event_channel_capacity,
        false,
        &mut effects,
    )
    .map(|(runs, _)| (runs, effects))
}

/// Periodic recovery isolates entry failures, but never suppresses directory
/// traversal or configured scan/cap errors. The count reports degraded scans.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "Recovery wrapper without effect discovery")
)]
pub(crate) fn refresh_runs(
    runs_dir: &Utf8Path,
    max_tracked_runs: usize,
    max_scan_entries: usize,
    live_event_channel_capacity: usize,
) -> Result<(BTreeMap<String, RunRecord>, usize), ServiceError> {
    refresh_runs_with_effects(
        runs_dir,
        max_tracked_runs,
        max_scan_entries,
        live_event_channel_capacity,
    )
    .map(|scan| (scan.runs, scan.failures))
}

pub(crate) struct RefreshedRuns {
    pub(crate) runs: BTreeMap<String, RunRecord>,
    pub(crate) failures: usize,
    pub(crate) effects: EffectCandidates,
}

/// Periodic recovery also preserves unverified effect-discovery candidates,
/// including busy shards and entries whose evidence could not be read.
pub(crate) fn refresh_runs_with_effects(
    runs_dir: &Utf8Path,
    max_tracked_runs: usize,
    max_scan_entries: usize,
    live_event_channel_capacity: usize,
) -> Result<RefreshedRuns, ServiceError> {
    let mut effects = EffectCandidates::new();
    scan_runs(
        runs_dir,
        max_tracked_runs,
        max_scan_entries,
        live_event_channel_capacity,
        true,
        &mut effects,
    )
    .map(|(runs, failures)| RefreshedRuns {
        runs,
        failures,
        effects,
    })
}

fn scan_runs(
    runs_dir: &Utf8Path,
    max_tracked_runs: usize,
    max_scan_entries: usize,
    live_event_channel_capacity: usize,
    tolerate_entry_errors: bool,
    effects: &mut EffectCandidates,
) -> Result<(BTreeMap<String, RunRecord>, usize), ServiceError> {
    let mut records = BTreeMap::new();
    let mut failures = 0;
    if !tolerate_entry_errors && !runs_dir.exists() {
        return Ok((records, failures));
    }
    let mut scanned = 0_usize;
    let mut nonterminal_runs = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        let entry = entry?;
        // G03: coordination/metadata entries (dotfiles, legacy per-run
        // admission locks, the sharded admission directory, idempotency
        // bookkeeping) are skipped WITHOUT consuming the scan budget, so
        // lifetime admissions can never exhaust recovery. The name probe
        // uses the directory entry name only, never following links.
        if crate::run_dirs::is_coordination_entry(&entry) {
            continue;
        }
        scanned = scanned.saturating_add(1);
        if scanned > max_scan_entries {
            return Err(ServiceError::Invalid(format!(
                "run store contains more than {max_scan_entries} entries"
            )));
        }
        let recovered = Utf8PathBuf::from_path_buf(entry.path())
            .map_err(|path| {
                ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
            })
            .and_then(|run_dir| {
                let recovered = rehydrate_run(
                    &run_dir,
                    &mut nonterminal_runs,
                    live_event_channel_capacity,
                    effects,
                );
                if recovered.is_err() {
                    effects
                        .entry(run_dir)
                        .or_insert(EffectProvenance::PreserveEvidence);
                }
                recovered
            });
        // Count validated non-terminal journals even if their package or
        // manifest could not be loaded; isolation must not bypass the cap.
        if nonterminal_runs > max_tracked_runs {
            return Err(ServiceError::Invalid(format!(
                "run store contains more than {max_tracked_runs} non-terminal runs"
            )));
        }
        match recovered {
            Ok(Some((run_id, record))) => {
                records.insert(run_id, record);
            }
            Ok(None) => {}
            Err(error) if tolerate_entry_errors => {
                failures += 1;
                tracing::warn!(run_id = %entry.file_name().to_string_lossy(), %error,
                    "shared recovery skipped unreadable run; keeping any tracked state");
            }
            Err(error) => return Err(error),
        }
    }
    Ok((records, failures))
}

fn rehydrate_run(
    run_dir: &Utf8Path,
    nonterminal_runs: &mut usize,
    live_event_channel_capacity: usize,
    effects: &mut EffectCandidates,
) -> Result<Option<(String, RunRecord)>, ServiceError> {
    // Serialize with preparation before even probing the journal. Contention
    // (including a shard collision) skips this entry, never a half-built prefix.
    let Some(_admission) = crate::run_dirs::try_lock_run_admission(run_dir)? else {
        effects.insert(run_dir.to_owned(), EffectProvenance::ScanHint);
        return Ok(None);
    };
    let Some((journal_values, state)) = read_recovery_state(run_dir)? else {
        return Ok(None);
    };
    let run_id = run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?
        .to_owned();
    if !state.effects.is_empty() {
        effects.insert(run_dir.to_owned(), EffectProvenance::ConfirmedEffects);
    }
    if state.terminal.is_some() {
        return Ok(None);
    }
    validate_recovery_events(&journal_values)?;
    if crate::run_dirs::is_direct_run(run_dir, &state) {
        return Ok(None);
    }
    *nonterminal_runs = nonterminal_runs.saturating_add(1);
    let (record_state, question, confirm) = match state.settled_pending().cloned() {
        Some(Interaction::Question { question }) => (RunStatus::Waiting, Some(question), None),
        Some(Interaction::Confirmation { confirm }) => (RunStatus::Confirming, None, Some(confirm)),
        None => (RunStatus::Queued, None, None),
    };
    // A mailbox cancel observed at rehydration is acceptance, not
    // settlement: only a journaled terminal state reports `Canceled`.
    let record_state = if record_state == RunStatus::Queued
        && crate::run_dirs::has_pending_cancel_control(run_dir)
    {
        RunStatus::CancelRequested
    } else {
        record_state
    };
    let mut contract = crate::run_package::load(run_dir, &state)?;
    // H02: re-apply the journaled per-run raise so a restart does not
    // drop it. The deployment floor is re-applied at spawn time from
    // the current policy (see `spawn_engine_run`); both are idempotent.
    contract.apply_audit_raise(audit_raise_from_values(&journal_values));
    // Folded inputs, not the admission event: a fork patches inputs, and
    // the folded state is the only view that includes the patch.
    let inputs = state.inputs.clone().unwrap_or_default();
    let queued_identity = (state.priority, state.parent_run_id.clone());
    // Journal I/O failures fail rehydration instead of recovering with
    // half-read maps. Continuations live in the typed journal store, so
    // only user answers join the memory map.
    let (answers, confirmations) = (state.answers.clone(), state.confirmations.clone());
    // Restore the durable FIFO key, including a scheduler-preserved
    // queued_at override, instead of re-stamping on recovery.
    let queued_at = read_last_queued_at_from_values(&journal_values);
    // Recovery runs before a LocalService exists; the shared constructor is
    // the one ownership point for this path and fresh-run admission alike.
    let (events, _) = crate::types::new_live_event_channel(live_event_channel_capacity);
    Ok(Some((
        run_id,
        RunRecord {
            contract_sha256: contract.sha256.clone(),
            package_sha256: state.package_sha256.clone(),
            contract,
            inputs,
            answers,
            confirmations,
            priority: queued_identity.0,
            parent_run_id: queued_identity.1,
            preempted: false,
            state: record_state,
            run_dir: run_dir.to_owned(),
            artifacts: read_optional_output_manifest(run_dir)?,
            question,
            confirm,
            events,
            cancellation: CancellationToken::new(),
            scheduler_yield: CancellationToken::new(),
            task: Arc::new(Mutex::new(None)),
            queued_at,
            owner_id: String::new(),
            ephemeral: false,
        },
    )))
}

fn validate_recovery_events(events: &[Value]) -> Result<(), ServiceError> {
    for event in events {
        RunEvent::from_flat(event).map_err(ServiceError::Invalid)?;
    }
    Ok(())
}

/// Read and gate a recovery snapshot under admission ownership. In particular,
/// an incomplete fork must never lend its copied effects to cleanup.
pub(crate) fn read_recovery_state(
    run_dir: &Utf8Path,
) -> Result<Option<(Vec<Value>, RunState)>, ServiceError> {
    let meta_dir = run_meta_dir(run_dir);
    let journal_path = meta_dir.join("journal.jsonl");
    if !is_regular_directory(run_dir)? || !is_regular_directory(&meta_dir)? {
        return Ok(None);
    }
    let pending_marker = crate::run_dirs::fork_admission_pending(run_dir)?;
    if !is_regular_file(&journal_path)? {
        if pending_marker {
            crate::run_dirs::discard_pending_fork(run_dir)?;
        }
        return Ok(None);
    }
    if journal_is_empty(&journal_path)? {
        if pending_marker {
            crate::run_dirs::discard_pending_fork(run_dir)?;
        }
        return Ok(None);
    }
    // One bounded journal read serves the fold and every derivation
    // below (identity, scheduling, HITL maps, queue key): a restart
    // never pays a scan per field per run.
    let journal_values = read_journal_events(run_dir)?;
    let state = RunState::fold_values(&journal_values).map_err(ServiceError::invalid)?;
    let run_id = run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?
        .to_string();
    if pending_marker
        || journal_values
            .iter()
            .any(|event| event["t"] == "run_forked")
    {
        match crate::run_dirs::check_run_admission(run_dir, &run_id, &journal_values, &state) {
            Ok(true) => {}
            // Incomplete fork wiped; its cancel mailbox is unchanged.
            Ok(false) => return Ok(None),
            Err(error @ ServiceError::ForkTreeCommitUnavailable(_)) if state.effects.is_empty() => {
                validate_recovery_events(&journal_values)?;
                if state.terminal.is_none() {
                    if !crate::run_dirs::try_interrupt_admission(
                        run_dir,
                        model::FailureCode::Refused,
                        &error.to_string(),
                        Some(state.last_seq),
                    )? {
                        return Err(error);
                    }
                    let events = read_journal_events(run_dir)?;
                    let settled = RunState::fold_values(&events).map_err(ServiceError::invalid)?;
                    if settled.terminal.is_none() {
                        return Err(error);
                    }
                    return Ok(Some((events, settled)));
                }
                // Keep the manifest and visibility marker untouched. Retried
                // recovery sees the terminal journal; adoption still fails.
            }
            Err(error) => return Err(error),
        }
    }
    Ok(Some((journal_values, state)))
}

#[cfg(test)]
mod tests {
    use super::audit_raise_from_values;
    use policy::AuditLevel;
    use serde_json::json;

    #[test]
    fn audit_raise_respects_fork_boundary_and_same_run_requeues() {
        assert_eq!(audit_raise_from_values(&[]), None);
        assert_eq!(
            audit_raise_from_values(&[json!({"t": "run_queued", "audit_raise": null})]),
            None
        );
        let raised = json!({"t": "run_queued", "audit_raise": "standard"});
        assert_eq!(
            audit_raise_from_values(&[
                raised.clone(),
                json!({"t": "run_forked"}),
                json!({"t": "run_queued", "audit_raise": null}),
            ]),
            None,
            "a fork must not inherit its source's per-run raise"
        );
        assert_eq!(
            audit_raise_from_values(&[
                raised,
                json!({"t": "run_queued", "audit_raise": null}),
                json!({"t": "run_queued"}),
            ]),
            Some(AuditLevel::Standard),
            "same-run requeues must preserve the admission's raise"
        );
    }
}
