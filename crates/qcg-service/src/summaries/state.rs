use super::super::run_dirs::journal_is_empty;
use super::super::types::{RunRecord, ServiceError};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::RunStatus;
use qcg_contract::Contract;
use qcg_engine::{Interaction, RunState, read_output_manifest};
use qcg_types::OutputManifest;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use super::reads::read_persisted_hitl;
use super::reads::read_persisted_mcp_pending;
use super::reads::read_queued_identity;
use super::runs::{read_run_generator_path, read_run_inputs};
use super::summary::run_meta_dir;
use qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES;

pub(crate) fn fold_run_state(run_dir: &Utf8Path) -> Result<RunState, ServiceError> {
    RunState::fold_journal(&run_meta_dir(run_dir).join("journal.jsonl"))
        .map_err(|error| ServiceError::Invalid(error.to_string()))
}

fn read_last_queued_at(run_dir: &Utf8Path) -> Option<chrono::DateTime<chrono::Utc>> {
    let events = crate::summaries::read_journal_events(run_dir).ok()?;
    events
        .iter()
        .rev()
        .find(|event| event.get("t").and_then(serde_json::Value::as_str) == Some("run_queued"))
        .and_then(|event| event.get("ts").and_then(serde_json::Value::as_str))
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|ts| ts.with_timezone(&chrono::Utc))
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
) -> Result<BTreeMap<String, RunRecord>, ServiceError> {
    let mut records = BTreeMap::new();
    if !runs_dir.exists() {
        return Ok(records);
    }
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
            return Err(ServiceError::Invalid(format!(
                "run store contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
            )));
        }
        let entry = entry?;
        let run_dir = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
        })?;
        let journal_path = run_meta_dir(&run_dir).join("journal.jsonl");
        if !run_dir.is_dir() || !journal_path.is_file() {
            continue;
        }
        if journal_is_empty(&journal_path)? {
            continue;
        }
        let mut state = RunState::fold_journal(&journal_path)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        if state.terminal.is_some() {
            continue;
        }
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
        let generator_path = read_run_generator_path(&run_dir)?;
        let contract = Contract::load(&generator_path)
            .map_err(|error| ServiceError::Invalid(error.to_string()))?;
        let inputs = read_run_inputs(&run_dir)?;
        let queued_identity = read_queued_identity(&run_dir).unwrap_or((0, None));
        let (mut answers, confirmations) =
            read_persisted_hitl(&run_dir).unwrap_or((BTreeMap::new(), BTreeMap::new()));
        // Journaled MCP continuations ride along in the answers map under
        // reserved #__mcp_pending keys so resumed steps can continue the
        // original remote request.
        if let Ok(pending) = read_persisted_mcp_pending(&run_dir) {
            answers.extend(pending);
        }
        // Restore FIFO admission order from the last run_queued timestamp so
        // a restart preserves cross-generator submission order instead of
        // falling back to run_id string order.
        let queued_at = read_last_queued_at(&run_dir);
        let (events, _) = broadcast::channel(512);
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
            },
        );
    }
    Ok(records)
}
