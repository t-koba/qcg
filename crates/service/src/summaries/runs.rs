use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use serde_json::Value;
use std::collections::BTreeMap;

use super::gc::run_identity_event;
use super::reads::{read_durable_run_events, read_journal_events};
use super::state::read_optional_output_manifest;
use super::summary::RunSummary;
use api::RunEvent;

pub fn read_run_generator_path(run_dir: &Utf8Path) -> Result<Utf8PathBuf, ServiceError> {
    let events = read_durable_run_events(run_dir)?;
    let (_, started) = run_identity_event(run_dir, &events)?;
    Ok(Utf8PathBuf::from(&started.generator_path))
}

pub fn read_run_inputs(run_dir: &Utf8Path) -> Result<BTreeMap<String, Value>, ServiceError> {
    let events = read_durable_run_events(run_dir)?;
    let (_, started) = run_identity_event(run_dir, &events)?;
    Ok(started.inputs.clone())
}

pub fn run_summary(run_dir: &Utf8Path) -> Result<RunSummary, ServiceError> {
    run_summary_with_seq(run_dir).map(|(summary, _)| summary)
}

/// Summary plus the folded `last_seq` from a single journal read: listing
/// paths use this so one run never costs a summary scan plus a second fold.
pub fn run_summary_with_seq(run_dir: &Utf8Path) -> Result<(RunSummary, u64), ServiceError> {
    let values = read_journal_events(run_dir)?;
    let state = engine::RunState::fold_values(&values)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    let events = values
        .iter()
        .map(|event| RunEvent::from_flat(event).map_err(ServiceError::Invalid))
        .collect::<Result<Vec<_>, _>>()?;
    let summary = run_summary_from_state(run_dir, &events, &state)?;
    Ok((summary, state.last_seq))
}

pub(crate) fn status_name_from_state(state: &engine::RunState) -> &'static str {
    use engine::{Interaction, TerminalState};
    match (&state.terminal, &state.pending) {
        (Some(TerminalState::Succeeded), _) => "success",
        (Some(TerminalState::Failed), _) => "failed",
        (Some(TerminalState::Canceled), _) => "canceled",
        (Some(TerminalState::Interrupted), _) => "interrupted",
        // A journaled cancel is acceptance, not settlement.
        (None, _) if state.cancel_requested => "cancel_requested",
        (None, Some(Interaction::Question { .. })) => "waiting",
        (None, Some(Interaction::Confirmation { .. })) => "confirming",
        (None, None) if state.execution_started => "running",
        (None, None) => "queued",
    }
}

/// Status and settled time come from the folded state, not from a second scan
/// of lifecycle events: one journal read, one projection, no second source of
/// truth for what a run is doing.
pub(crate) fn run_summary_from_state(
    run_dir: &Utf8Path,
    events: &[RunEvent],
    state: &engine::RunState,
) -> Result<RunSummary, ServiceError> {
    let (started_event, started) = run_identity_event(run_dir, events)?;
    let status = status_name_from_state(state);
    let artifacts = read_optional_output_manifest(run_dir)?
        .map(|manifest| manifest.artifacts)
        .unwrap_or_default();
    let retention_days = started
        .retention_days
        .map(u32::try_from)
        .transpose()
        .map_err(|_| ServiceError::Invalid("retention_days exceeds u32".into()))?;
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
        finished_at: state.finished_at.clone(),
        artifacts,
        retention_days,
    })
}
