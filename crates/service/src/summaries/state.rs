use super::super::run_dirs::{is_regular_directory, is_regular_file, journal_is_empty};
use super::super::types::{RunRecord, ServiceError};
use api::{RunEvent, RunStatus};
use camino::{Utf8Path, Utf8PathBuf};
use contract::Contract;
use engine::{Interaction, RunState, read_output_manifest};
use model::OutputManifest;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use super::reads::read_journal_events;
use super::summary::run_meta_dir;

pub(crate) fn fold_run_state(run_dir: &Utf8Path) -> Result<RunState, ServiceError> {
    RunState::fold_journal(&run_meta_dir(run_dir).join("journal.jsonl"))
        .map_err(|error| ServiceError::Invalid(error.to_string()))
}

/// Latest durable admission instant: the explicit `queued_at` payload field
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

/// Latest queue instant from already-read journal values. Malformed
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

pub(crate) fn rehydrate_runs(
    runs_dir: &Utf8Path,
    max_tracked_runs: usize,
    max_scan_entries: usize,
) -> Result<BTreeMap<String, RunRecord>, ServiceError> {
    let mut records = BTreeMap::new();
    if !runs_dir.exists() {
        return Ok(records);
    }
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        let entry = entry?;
        // G03: coordination/metadata entries (dotfiles, legacy per-run
        // admission locks, the sharded admission directory, idempotency
        // bookkeeping) are skipped WITHOUT consuming the scan budget, so
        // lifetime admissions can never exhaust recovery. The name probe
        // uses the directory entry name only, never following links.
        if entry
            .file_name()
            .to_str()
            .is_some_and(crate::run_dirs::is_store_coordination_name)
        {
            continue;
        }
        scanned = scanned.saturating_add(1);
        if scanned > max_scan_entries {
            return Err(ServiceError::Invalid(format!(
                "run store contains more than {max_scan_entries} entries"
            )));
        }
        let run_dir = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
        })?;
        let journal_path = run_meta_dir(&run_dir).join("journal.jsonl");
        if !is_regular_directory(&run_dir)? || !is_regular_file(&journal_path)? {
            continue;
        }
        if journal_is_empty(&journal_path)? {
            continue;
        }
        // One bounded journal read serves the fold and every derivation
        // below (identity, scheduling, HITL maps, queue instant): a restart
        // never pays a scan per field per run.
        let journal_values = read_journal_events(&run_dir)?;
        let mut state = RunState::fold_values(&journal_values)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        if state.terminal.is_some() {
            continue;
        }
        let journal_events = journal_values
            .iter()
            .map(|event| RunEvent::from_flat(event).map_err(ServiceError::Invalid))
            .collect::<Result<Vec<_>, _>>()?;
        if records.len() >= max_tracked_runs {
            return Err(ServiceError::Invalid(format!(
                "run store contains more than {max_tracked_runs} non-terminal runs"
            )));
        }
        let run_id = run_dir
            .file_name()
            .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?
            .to_string();
        let (record_state, question, confirm) = match state.pending.take() {
            Some(Interaction::Question { question }) => (RunStatus::Waiting, Some(question), None),
            Some(Interaction::Confirmation { confirm }) => {
                (RunStatus::Confirming, None, Some(confirm))
            }
            None => (RunStatus::Queued, None, None),
        };
        // A mailbox cancel observed at rehydration is acceptance, not
        // settlement: only a journaled terminal state reports `Canceled`.
        let record_state = if record_state == RunStatus::Queued
            && crate::run_dirs::has_pending_cancel_control(&run_dir)
        {
            RunStatus::CancelRequested
        } else {
            record_state
        };
        let (_, started) = super::run_identity_event(&run_dir, &journal_events)?;
        let generator_path = Utf8PathBuf::from(&started.generator_path);
        let contract = Contract::load(&generator_path)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        // Folded inputs, not the admission event: a fork patches inputs, and
        // the folded state is the only view that includes the patch.
        let inputs = state.inputs.clone().unwrap_or_default();
        let queued_identity = (state.priority, state.parent_run_id.clone());
        // Journal I/O failures fail rehydration instead of recovering with
        // half-read maps. Continuations live in the typed journal store, so
        // only user answers join the memory map.
        let (answers, confirmations) = (state.answers.clone(), state.confirmations.clone());
        // Restore FIFO admission order from the last run_queued timestamp so
        // a restart preserves cross-generator submission order instead of
        // falling back to run_id string order.
        let queued_at = read_last_queued_at_from_values(&journal_values);
        let (events, _) = broadcast::channel(policy::LIVE_EVENT_CHANNEL_CAPACITY);
        records.insert(
            run_id,
            RunRecord {
                contract_sha256: contract.sha256.clone(),
                contract,
                inputs,
                answers,
                confirmations,
                priority: queued_identity.0,
                parent_run_id: queued_identity.1,
                preempted: false,
                state: record_state,
                run_dir: run_dir.clone(),
                artifacts: read_optional_output_manifest(&run_dir)?,
                question,
                confirm,
                events,
                cancellation: CancellationToken::new(),
                task: Arc::new(Mutex::new(None)),
                queued_at,
                owner_id: String::new(),
                ephemeral: false,
            },
        );
    }
    Ok(records)
}
