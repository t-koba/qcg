use super::super::run_dirs::journal_is_empty;
use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};

use super::runs::run_summary_with_seq;
use super::summary::{RunSummary, run_meta_dir};

/// Summaries with their folded `last_seq`, oldest first. Each run reads
/// and folds its journal exactly once; callers project from these pairs
/// instead of re-folding per run.
pub fn list_run_summaries(
    runs_dir: &Utf8Path,
    max_scan_entries: usize,
) -> Result<Vec<(RunSummary, u64)>, ServiceError> {
    if !runs_dir.exists() {
        return Ok(Vec::new());
    }
    let mut summaries = Vec::new();
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        let entry = entry?;
        // G03: coordination entries never consume the scan budget (see
        // rehydrate_runs for the rationale).
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
                "runs directory contains more than {max_scan_entries} entries"
            )));
        }
        let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
            ServiceError::Invalid(format!("run path is not valid UTF-8: {}", path.display()))
        })?;
        let journal_path = run_meta_dir(&path).join("journal.jsonl");
        if !path.is_dir() || !journal_path.exists() || journal_is_empty(&journal_path)? {
            continue;
        }
        summaries.push(run_summary_with_seq(&path)?);
    }
    // Chronological order across generators; run_id prefixes never sort
    // globally by time.
    summaries.sort_by(|left, right| {
        left.0
            .started_at
            .cmp(&right.0.started_at)
            .then_with(|| left.0.run_id.cmp(&right.0.run_id))
    });
    Ok(summaries)
}

pub(crate) fn list_items_cached(
    runs_dir: &Utf8Path,
    cap: usize,
    store: &crate::read_store::ReadStore,
) -> Result<Vec<api::RunListItem>, ServiceError> {
    let mut summaries = Vec::new();
    if !runs_dir.exists() {
        return Ok(summaries);
    }
    let mut scanned = 0;
    for entry in std::fs::read_dir(runs_dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(crate::run_dirs::is_store_coordination_name)
        {
            continue;
        }
        scanned += 1;
        if scanned > cap {
            return Err(ServiceError::Invalid(format!(
                "runs directory contains more than {cap} entries"
            )));
        }
        let path = Utf8PathBuf::from_path_buf(entry.path())
            .map_err(|_| ServiceError::Invalid("run path is not UTF-8".into()))?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let journal = run_meta_dir(&path).join("journal.jsonl");
        let metadata = match std::fs::symlink_metadata(&journal) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_file() || metadata.len() == 0 {
            continue;
        }
        let view = store.read(&path)?;
        let (identity, started) = super::gc::run_identity_event(&path, &view.events)?;
        summaries.push(api::RunListItem {
            run_id: path.file_name().unwrap().to_owned(),
            state: super::state::status_from_journal(super::runs::status_name_from_state(
                &view.state,
            ))?,
            generator_id: started
                .generator
                .split_once('@')
                .map(|(id, _)| id)
                .unwrap_or(&started.generator)
                .to_owned(),
            started_at: identity.ts.clone(),
            seq: view.state.last_seq,
        });
    }
    summaries.sort_by(|a, b| {
        a.started_at
            .cmp(&b.started_at)
            .then_with(|| a.run_id.cmp(&b.run_id))
    });
    Ok(summaries)
}
