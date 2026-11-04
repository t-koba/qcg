use super::super::types::ServiceError;
use crate::run_package::{load_identity, package_dir};
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

/// The run's generator root: verified pinned package, or recorded path for legacy runs.
pub fn run_generator_root(run_dir: &Utf8Path) -> Result<Utf8PathBuf, ServiceError> {
    let events = read_durable_run_events(run_dir)?;
    let (_, started) = run_identity_event(run_dir, &events)?;
    let Some(expected) = started.package_sha256.as_deref() else {
        return Ok(Utf8PathBuf::from(&started.generator_path));
    };
    load_identity(run_dir, Some(expected), Some(&started.contract_sha256))?;
    Ok(package_dir(run_dir))
}

/// The contract the run used: pinned package bytes, or the recorded path for legacy runs.
pub fn load_run_contract(run_dir: &Utf8Path) -> Result<contract::Contract, ServiceError> {
    contract::Contract::load(run_generator_root(run_dir)?)
        .map_err(|error| ServiceError::Invalid(error.to_string()))
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
    let (mut summary, state) = run_summary_core_with_state(run_dir)?;
    summary.invalidation = crate::invalidation::read_invalidation(run_dir)?;
    Ok((summary, state.last_seq))
}

/// Journal-derived summary without auxiliary invalidation metadata, used by
/// retention so a broken invalidation sidecar cannot change expiry behavior.
pub(super) fn run_summary_core_with_state(
    run_dir: &Utf8Path,
) -> Result<(RunSummary, engine::RunState), ServiceError> {
    let values = read_journal_events(run_dir)?;
    let state = engine::RunState::fold_values(&values)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    let events = values
        .iter()
        .map(|event| RunEvent::from_flat(event).map_err(ServiceError::Invalid))
        .collect::<Result<Vec<_>, _>>()?;
    let summary = run_summary_from_state(run_dir, &events, &state)?;
    Ok((summary, state))
}

/// Minimal journal projection for run-reference selector discovery. It does
/// not read invalidation sidecars or output manifests: those are checked only
/// after the journal identifies a selected candidate.
pub(crate) struct RunSelectorInfo {
    pub run_id: String,
    pub generator: String,
    pub status: String,
    pub started_at: String,
}

pub(crate) fn run_selector_info(run_dir: &Utf8Path) -> Result<RunSelectorInfo, ServiceError> {
    let values = read_journal_events(run_dir)?;
    let state = engine::RunState::fold_values(&values)
        .map_err(|error| ServiceError::Invalid(error.to_string()))?;
    let events = values
        .iter()
        .map(|event| RunEvent::from_flat(event).map_err(ServiceError::Invalid))
        .collect::<Result<Vec<_>, _>>()?;
    let (started_event, started) = run_identity_event(run_dir, &events)?;
    let run_id = run_dir
        .file_name()
        .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?
        .to_string();
    Ok(RunSelectorInfo {
        run_id,
        generator: started.generator.clone(),
        status: status_name_from_state(&state).to_string(),
        started_at: started_event.ts.clone(),
    })
}

pub(crate) fn status_name_from_state(state: &engine::RunState) -> &'static str {
    use engine::{Interaction, TerminalState};
    match (&state.terminal, state.settled_pending()) {
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
        invalidation: None,
        run_id: run_dir
            .file_name()
            .ok_or_else(|| ServiceError::Invalid("run directory has no file name".into()))?
            .to_string(),
        status: status.to_string(),
        direct_owner: if state.terminal.is_none() && crate::run_dirs::is_direct_run(run_dir, state)
        {
            Some(
                if crate::run_dirs::try_lock_existing_run_execution(run_dir)?.is_some() {
                    "inactive"
                } else {
                    "active"
                },
            )
        } else {
            None
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package::{PackageLimits, package_identity_sha256};
    use contract::Contract;
    use engine::JournalWriter;
    use serde_json::json;

    fn write_contract(dir: &Utf8Path, input: f64) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(
            dir.join(contract::MANIFEST_FILE),
            format!(
                r#"
[generator]
id = "pricing-fixture"
name = "Pricing Fixture"
version = "0.1.0"

[llm]
max_tokens = 1024
[llm.model]
provider = "provider"
model = "model"
input_cost_per_million_usd = {input}
output_cost_per_million_usd = 10.0

[[flow]]
id = "emit"
type = "write"
params = {{ content = "x", output_file = "x.txt" }}
"#
            ),
        )
        .unwrap();
    }

    fn write_run(
        run_dir: &Utf8Path,
        generator_path: &Utf8Path,
        package_sha256: Option<&str>,
        contract_sha256: &str,
    ) {
        let meta = run_dir.join("meta");
        std::fs::create_dir_all(&meta).unwrap();
        let journal =
            JournalWriter::create(&meta.join("journal.jsonl"), "run", false, None).unwrap();
        journal
            .event(
                "run_started",
                json!({
                    "generator": "pricing-fixture@0.1.0",
                    "generator_path": generator_path.as_str(),
                    "contract_sha256": contract_sha256,
                    "package_sha256": package_sha256,
                    "inputs": {},
                    "schema_version": api::JOURNAL_SCHEMA_VERSION,
                    "elapsed_budget_mode": "wall_clock"
                }),
            )
            .unwrap();
        journal
            .event("run_finished", json!({"status": "success"}))
            .unwrap();
    }

    fn input_rate(contract: &Contract) -> Option<f64> {
        contract
            .manifest
            .llm
            .as_ref()
            .and_then(|llm| llm.model.as_ref())
            .and_then(|model| model.input_cost_per_million_usd)
    }

    #[test]
    fn pinned_package_prices_the_bytes_the_run_used() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-load-contract-{}", uuid::Uuid::now_v7()));
        let source = root.join("source");
        write_contract(&source, 2.0);
        let original = Contract::load(&source).unwrap();
        let package_sha256 = package_identity_sha256(&source, &PackageLimits::default()).unwrap();
        let run = root.join("run");
        let package = run.join("generator-package");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::copy(
            source.join(contract::MANIFEST_FILE),
            package.join(contract::MANIFEST_FILE),
        )
        .unwrap();
        write_run(&run, &source, Some(&package_sha256), &original.sha256);

        write_contract(&source, 5.0);
        assert_ne!(Contract::load(&source).unwrap().sha256, original.sha256);

        let loaded = load_run_contract(&run).unwrap();
        assert_eq!(loaded.sha256, original.sha256);
        assert_eq!(input_rate(&loaded), Some(2.0));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_run_falls_back_to_the_recorded_path() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-load-contract-legacy-{}", uuid::Uuid::now_v7()));
        let source = root.join("source");
        write_contract(&source, 2.0);
        let original = Contract::load(&source).unwrap();
        let run = root.join("run");
        write_run(&run, &source, None, &original.sha256);

        write_contract(&source, 5.0);
        let loaded = load_run_contract(&run).unwrap();
        assert_eq!(input_rate(&loaded), Some(5.0));
        assert_eq!(run_generator_root(&run).unwrap(), source);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn pinned_generator_root_survives_a_deleted_source() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-pinned-root-{}", uuid::Uuid::now_v7()));
        let source = root.join("source");
        write_contract(&source, 2.0);
        let original = Contract::load(&source).unwrap();
        let package_sha256 = package_identity_sha256(&source, &PackageLimits::default()).unwrap();
        let run = root.join("run");
        let package = run.join("generator-package");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::copy(
            source.join(contract::MANIFEST_FILE),
            package.join(contract::MANIFEST_FILE),
        )
        .unwrap();
        write_run(&run, &source, Some(&package_sha256), &original.sha256);
        std::fs::remove_dir_all(&source).unwrap();
        assert_eq!(run_generator_root(&run).unwrap(), package);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn pinned_generator_root_refuses_a_changed_package() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("qcg-pinned-root-bad-{}", uuid::Uuid::now_v7()));
        let source = root.join("source");
        write_contract(&source, 2.0);
        let original = Contract::load(&source).unwrap();
        let package_sha256 = package_identity_sha256(&source, &PackageLimits::default()).unwrap();
        let run = root.join("run");
        let package = run.join("generator-package");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::copy(
            source.join(contract::MANIFEST_FILE),
            package.join(contract::MANIFEST_FILE),
        )
        .unwrap();
        write_run(&run, &source, Some(&package_sha256), &original.sha256);
        write_contract(&package, 5.0);
        assert!(run_generator_root(&run).is_err());
        let _ = std::fs::remove_dir_all(root);
    }
}
