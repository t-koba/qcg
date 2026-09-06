use super::super::run_dirs::journal_is_empty;
use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::RunEvent;

use super::runs::run_summary;
use super::summary::run_meta_dir;
use qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES;

pub(crate) fn run_identity_event<'a>(
    run_dir: &Utf8Path,
    events: &'a [RunEvent],
) -> Result<(&'a RunEvent, &'a qcg_api::RunStartedEventData), ServiceError> {
    events
        .iter()
        .find_map(|event| event.data.run_started().map(|data| (event, data)))
        .ok_or_else(|| ServiceError::Invalid(format!("run `{run_dir}` has no run identity event")))
}

pub fn gc_run_directories(
    runs_dir: &Utf8Path,
    keep: usize,
    delete: bool,
) -> Result<Vec<Utf8PathBuf>, ServiceError> {
    if !runs_dir.exists() {
        return Ok(Vec::new());
    }
    let now = chrono::Utc::now();
    let mut candidates = Vec::new();
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
        let summary = run_summary(&path)?;
        if !matches!(
            summary.status.as_str(),
            "success" | "failed" | "canceled" | "interrupted"
        ) {
            continue;
        }
        let expired = match summary.retain_days {
            Some(days) => {
                let started =
                    chrono::DateTime::parse_from_rfc3339(&summary.started_at).map_err(|error| {
                        ServiceError::Invalid(format!(
                            "run `{}` has invalid started_at: {error}",
                            summary.run_id
                        ))
                    })?;
                started.with_timezone(&chrono::Utc) < now - chrono::Duration::days(i64::from(days))
            }
            None => false,
        };
        candidates.push((summary.started_at, path, expired));
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0));
    let mut deleted = Vec::new();
    for (index, (_, path, expired)) in candidates.into_iter().enumerate() {
        if index < keep && !expired {
            continue;
        }
        if delete {
            std::fs::remove_dir_all(&path)?;
            deleted.push(path);
        }
    }
    Ok(deleted)
}
