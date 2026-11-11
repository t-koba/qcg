use super::super::run_dirs::{is_regular_file, journal_is_empty};
use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use std::collections::BTreeSet;

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
        if crate::run_dirs::is_coordination_entry(&entry) {
            continue;
        }
        crate::run_dirs::bump_scan_count(&mut scanned, max_scan_entries)?;
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

/// Names of run-candidate directories, bounded by the scan cap. Names alone
/// are enumerated; no per-run file is touched.
fn run_dir_names(runs_dir: &Utf8Path, cap: usize) -> Result<BTreeSet<String>, ServiceError> {
    let entries = match std::fs::read_dir(runs_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(error.into()),
    };
    let (mut names, mut scanned) = (BTreeSet::new(), 0);
    for entry in entries {
        let entry = entry?;
        if crate::run_dirs::is_coordination_entry(&entry) {
            continue;
        }
        crate::run_dirs::bump_scan_count(&mut scanned, cap)?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| ServiceError::Invalid("run path is not UTF-8".into()))?;
        if entry.file_type()?.is_dir() {
            names.insert(name);
        }
    }
    Ok(names)
}

/// Fresh list view of one run directory; `None` when it has no non-empty
/// regular-file journal (not a run yet).
fn read_run(
    runs_dir: &Utf8Path,
    name: &str,
    store: &crate::read_store::ReadStore,
) -> Result<Option<api::RunListItem>, ServiceError> {
    let path = runs_dir.join(name);
    let journal = run_meta_dir(&path).join("journal.jsonl");
    if !is_regular_file(&journal)? || journal_is_empty(&journal)? {
        return Ok(None);
    }
    store.list_item(&path).map(Some)
}

/// One page of run history. `cursor` is `(started_at, run_id)`.
#[derive(Default)]
pub struct RunPage {
    pub limit: usize,
    pub since: Option<chrono::DateTime<chrono::FixedOffset>>,
    pub cursor: Option<(String, String)>,
    pub state: Option<api::RunStatus>,
    pub generator_id: Option<String>,
    pub descending: bool,
}

/// Lists one page ordered by `(started_at, run_id)`. Names come from a
/// directory read; per-run ordering facts are remembered in `store`, so a
/// warm request re-reads only the runs it examines to fill the page (fresh
/// revision and invalidation checks), and fails if one of those is corrupt.
pub(crate) fn list_items_page(
    runs_dir: &Utf8Path,
    cap: usize,
    store: &crate::read_store::ReadStore,
    page: &RunPage,
) -> Result<api::RunListResponse, ServiceError> {
    let names = run_dir_names(runs_dir, cap)?;
    store.prune(runs_dir, &names.iter().map(String::as_str).collect());
    for name in names.iter().filter(|name| !store.is_known(name)) {
        if let Some(item) = read_run(runs_dir, name, store)? {
            store.remember(&item);
        }
    }
    'order: loop {
        let mut ordered = store.ordered(|started, name, generator| {
            page.generator_id
                .as_deref()
                .is_none_or(|want| want == generator)
                && page.cursor.as_ref().is_none_or(|(at, id)| {
                    let key = (started, name);
                    let cursor = (at.as_str(), id.as_str());
                    if page.descending {
                        key < cursor
                    } else {
                        key > cursor
                    }
                })
        });
        if page.descending {
            ordered.reverse();
        }
        let mut items = Vec::new();
        for (started, name, generator) in &ordered {
            if let Some(since) = page.since {
                let started_at =
                    chrono::DateTime::parse_from_rfc3339(started).map_err(|error| {
                        ServiceError::Invalid(format!(
                            "run `{name}` has invalid started_at: {error}"
                        ))
                    })?;
                if started_at < since {
                    continue;
                }
            }
            let Some(item) = read_run(runs_dir, name, store)? else {
                store.forget(&runs_dir.join(name));
                continue;
            };
            if (&item.started_at, &item.generator_id) != (started, generator) {
                // The journal was replaced: order again from what was read.
                store.remember(&item);
                continue 'order;
            }
            if page.state.is_none_or(|state| item.state == state) {
                items.push(item);
                if items.len() > page.limit {
                    break;
                }
            }
        }
        let next_cursor = (items.len() > page.limit).then(|| {
            let last = &items[page.limit - 1];
            format!("{}|{}", last.started_at, last.run_id)
        });
        items.truncate(page.limit);
        return Ok(api::RunListResponse { items, next_cursor });
    }
}
