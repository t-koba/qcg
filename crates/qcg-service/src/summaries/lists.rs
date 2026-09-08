use super::super::run_dirs::journal_is_empty;
use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};

use super::runs::run_summary_with_seq;
use super::summary::{RunSummary, run_meta_dir};
use qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES;

/// Summaries with their folded `last_seq`, oldest first. Each run reads
/// and folds its journal exactly once; callers project from these pairs
/// instead of re-folding per run.
pub fn list_run_summaries(runs_dir: &Utf8Path) -> Result<Vec<(RunSummary, u64)>, ServiceError> {
    if !runs_dir.exists() {
        return Ok(Vec::new());
    }
    let mut summaries = Vec::new();
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
            return Err(ServiceError::Invalid(format!(
                "runs directory contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
            )));
        }
        let entry = entry?;
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
