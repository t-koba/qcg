use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::{RunCompletionStatus, RunEventData};
use serde_json::Value;
use std::collections::BTreeMap;

use super::gc::run_identity_event;
use super::reads::{read_journal_events, read_run_events};
use super::state::read_optional_output_manifest;
use super::summary::RunSummary;
use qcg_api::RunEvent;

pub fn read_run_generator_path(run_dir: &Utf8Path) -> Result<Utf8PathBuf, ServiceError> {
    let events = read_run_events(run_dir)?;
    read_run_generator_path_from_events(run_dir, &events)
}

pub fn read_run_generator_path_from_events(
    run_dir: &Utf8Path,
    events: &[RunEvent],
) -> Result<Utf8PathBuf, ServiceError> {
    let (_, started) = run_identity_event(run_dir, events)?;
    Ok(Utf8PathBuf::from(&started.generator_path))
}

pub(crate) fn read_run_contract_sha256(
    run_dir: &Utf8Path,
    events: &[RunEvent],
) -> Result<String, ServiceError> {
    let (_, started) = run_identity_event(run_dir, events)?;
    Ok(started.contract_sha256.clone())
}

pub fn read_run_inputs(run_dir: &Utf8Path) -> Result<BTreeMap<String, Value>, ServiceError> {
    let events = read_run_events(run_dir)?;
    read_run_inputs_from_events(run_dir, &events)
}

pub fn read_run_inputs_from_events(
    run_dir: &Utf8Path,
    events: &[RunEvent],
) -> Result<BTreeMap<String, Value>, ServiceError> {
    let (_, started) = run_identity_event(run_dir, events)?;
    Ok(started.inputs.clone())
}

pub fn run_summary(run_dir: &Utf8Path) -> Result<RunSummary, ServiceError> {
    run_summary_with_seq(run_dir).map(|(summary, _)| summary)
}

/// Summary plus the folded `last_seq` from a single journal read: listing
/// paths use this so one run never costs a summary scan plus a second fold.
pub fn run_summary_with_seq(run_dir: &Utf8Path) -> Result<(RunSummary, u64), ServiceError> {
    let values = read_journal_events(run_dir)?;
    let folded = qcg_engine::RunState::fold_values(&values)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    let events = values
        .iter()
        .map(|event| RunEvent::from_flat(event).map_err(ServiceError::Invalid))
        .collect::<Result<Vec<_>, _>>()?;
    let summary = run_summary_from_events(run_dir, &events)?;
    Ok((summary, folded.last_seq))
}

fn run_summary_from_events(
    run_dir: &Utf8Path,
    events: &[RunEvent],
) -> Result<RunSummary, ServiceError> {
    let (started_event, started) = run_identity_event(run_dir, events)?;
    let lifecycle = events
        .iter()
        .rev()
        .find(|event| {
            matches!(
                event.kind.as_str(),
                "run_queued"
                    | "run_started"
                    | "run_waiting"
                    | "confirm_request"
                    | "run_finished"
                    | "run_error"
                    | "run_canceled"
                    | "run_interrupted"
            )
        })
        .ok_or_else(|| ServiceError::Invalid("run has no lifecycle event".into()))?;
    let status = match &lifecycle.data {
        RunEventData::RunQueued(_) => "queued",
        RunEventData::RunStarted(_) => "running",
        RunEventData::RunWaiting(_) => "waiting",
        RunEventData::ConfirmRequest(_) => "confirming",
        RunEventData::RunError(_) => "failed",
        RunEventData::RunCanceled(_) => "canceled",
        RunEventData::RunInterrupted(_) => "interrupted",
        RunEventData::RunFinished(data) => match data.status {
            RunCompletionStatus::Success => "success",
            RunCompletionStatus::Failed => "failed",
        },
        _ => return Err(ServiceError::Invalid("unsupported lifecycle event".into())),
    };
    let artifacts = read_optional_output_manifest(run_dir)?
        .map(|manifest| manifest.artifacts)
        .unwrap_or_default();
    let retain_days = started
        .retain_days
        .map(u32::try_from)
        .transpose()
        .map_err(|_| ServiceError::Invalid("retain_days exceeds u32".into()))?;
    Ok(RunSummary {
        run_id: run_dir
            .file_name()
            .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?
            .to_string(),
        status: status.to_string(),
        generator: started.generator.clone(),
        generator_path: started.generator_path.clone(),
        contract_sha256: started.contract_sha256.clone(),
        inputs: started.inputs.clone(),
        started_at: started_event.ts.clone(),
        finished_at: matches!(status, "success" | "failed" | "canceled" | "interrupted")
            .then(|| lifecycle.ts.clone()),
        artifacts,
        retain_days,
    })
}
