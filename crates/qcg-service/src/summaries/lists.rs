use super::super::run_dirs::journal_is_empty;
use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};

use super::runs::run_summary;
use super::summary::{RunSummary, run_meta_dir};
use qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES;

pub fn list_run_summaries(runs_dir: &Utf8Path) -> Result<Vec<RunSummary>, ServiceError> {
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
        summaries.push(run_summary(&path)?);
    }
    summaries.sort_by(|left, right| left.run_id.cmp(&right.run_id));
    Ok(summaries)
}
