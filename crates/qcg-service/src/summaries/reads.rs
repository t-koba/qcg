use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::RunEvent;
use qcg_engine::{JournalLimits, read_journal_values};
use serde_json::Value;

use super::summary::run_meta_dir;

pub fn resolve_run_dir(runs_dir: &Utf8Path, id: &str) -> Result<Utf8PathBuf, ServiceError> {
    if id.contains('/') || id.contains('\\') || id == "." || id == ".." {
        return Err(ServiceError::Invalid(format!(
            "run id `{id}` is not allowed"
        )));
    }
    let run_dir = runs_dir.join(id);
    if !run_meta_dir(&run_dir).join("journal.jsonl").exists() {
        return Err(ServiceError::Invalid(format!(
            "run `{id}` was not found under `{runs_dir}`"
        )));
    }
    Ok(run_dir)
}

pub fn read_journal_events(run_dir: &Utf8Path) -> Result<Vec<Value>, ServiceError> {
    read_events_from_meta(&run_meta_dir(run_dir))
}

/// Drop trailing `run_canceled` bookkeeping events written by an engine task
/// that was preempted before terminal settlement. Returns whether anything
/// was removed. Never touches a journal whose tail is anything else, so a
/// genuine user cancellation is preserved.
pub(crate) fn truncate_trailing_canceled_events(run_dir: &Utf8Path) -> Result<bool, ServiceError> {
    let path = run_meta_dir(run_dir).join("journal.jsonl");
    let content = std::fs::read_to_string(&path)?;
    let mut lines: Vec<&str> = content.lines().collect();
    let mut removed = false;
    while let Some(last) = lines.last() {
        let canceled = serde_json::from_str::<Value>(last)
            .map(|event| event.get("t").and_then(Value::as_str) == Some("run_canceled"))
            .unwrap_or(false);
        if !canceled {
            break;
        }
        lines.pop();
        removed = true;
    }
    if removed {
        let mut rewritten = lines.join("\n");
        if !rewritten.is_empty() {
            rewritten.push('\n');
        }
        std::fs::write(&path, rewritten)?;
    }
    Ok(removed)
}

/// Latest scheduling identity from the trailing `run_queued` event:
/// priority and fork parent. Journals written before these fields existed
/// read as zero priority with no parent.
pub(crate) fn read_queued_identity(
    run_dir: &Utf8Path,
) -> Result<(i32, Option<String>), ServiceError> {
    let mut priority = 0;
    let mut parent = None;
    for event in read_journal_events(run_dir)? {
        if event.get("t").and_then(Value::as_str) != Some("run_queued") {
            continue;
        }
        if let Some(value) = event.get("priority").and_then(Value::as_i64) {
            priority = value.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        }
        parent = event
            .get("parent_run_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(parent);
    }
    Ok((priority, parent))
}

pub(crate) fn read_events_from_meta(meta_dir: &Utf8Path) -> Result<Vec<Value>, ServiceError> {
    read_journal_values(&meta_dir.join("journal.jsonl"), JournalLimits::default())
        .map(|scan| scan.events)
        .map_err(|error| ServiceError::Invalid(error.to_string()))
}

pub fn read_run_events(run_dir: &Utf8Path) -> Result<Vec<RunEvent>, ServiceError> {
    read_journal_events(run_dir)?
        .into_iter()
        .map(|event| RunEvent::from_flat(&event).map_err(ServiceError::Invalid))
        .collect()
}
