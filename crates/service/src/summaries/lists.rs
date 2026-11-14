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
        if !path.is_dir()
            || crate::run_dirs::fork_admission_pending(&path)?
            || !journal_path.exists()
            || journal_is_empty(&journal_path)?
        {
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
fn run_dir_names(
    runs_dir: &Utf8Path,
    cap: usize,
    store: &crate::read_store::ReadStore,
) -> Result<BTreeSet<String>, ServiceError> {
    let _ = store;
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
        #[cfg(test)]
        store
            .enumerated
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
    if crate::run_dirs::fork_admission_pending(&path)?
        || !is_regular_file(&journal)?
        || journal_is_empty(&journal)?
    {
        store.forget_view(&path);
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
    census_page(runs_dir, cap, store, page, false)
}

/// Exclusive, unfiltered pages may use bounded membership, but every candidate
/// still passes the normal fresh read, including the lookahead row.
pub(crate) fn indexed_items_page(
    runs_dir: &Utf8Path,
    cap: usize,
    store: &crate::read_store::ReadStore,
    page: &RunPage,
) -> Result<api::RunListResponse, ServiceError> {
    let lease = crate::run_dirs::try_lock_store_maintenance(runs_dir)?;
    if lease.is_none() {
        store.invalidate_index();
        return census_page(runs_dir, cap, store, page, false);
    }
    if let Some(keys) = store.candidates(page) {
        let mut items = Vec::new();
        let mut valid = true;
        for (started, name) in keys {
            match read_run(runs_dir, &name, store) {
                Ok(Some(item))
                    if (item.started_at.as_str(), item.run_id.as_str())
                        == (started.as_str(), name.as_str()) =>
                {
                    items.push(item)
                }
                Ok(_) => {
                    valid = false;
                    break;
                }
                Err(ServiceError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    valid = false;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        if valid {
            return Ok(response(items, page.limit));
        }
        store.invalidate_index();
        // Replacements must not supply stale ordering facts to the rebuild.
    }
    census_page(runs_dir, cap, store, page, true)
}

fn response(mut items: Vec<api::RunListItem>, limit: usize) -> api::RunListResponse {
    let next_cursor = (items.len() > limit && limit > 0).then(|| {
        let last = &items[limit - 1];
        format!("{}|{}", last.started_at, last.run_id)
    });
    items.truncate(limit);
    api::RunListResponse { items, next_cursor }
}

fn census_page(
    runs_dir: &Utf8Path,
    cap: usize,
    store: &crate::read_store::ReadStore,
    page: &RunPage,
    rebuild: bool,
) -> Result<api::RunListResponse, ServiceError> {
    let revision = store.index_revision();
    let names = run_dir_names(runs_dir, cap, store)?;
    store.prune(runs_dir, &names.iter().map(String::as_str).collect());
    let mut facts = store.known_facts();
    facts.retain(|name, _| names.contains(name));
    for name in &names {
        if !facts.contains_key(name)
            && let Some(item) = read_run(runs_dir, name, store)?
        {
            facts.insert(item.run_id, (item.started_at, item.generator_id));
        }
    }
    'order: loop {
        let mut ordered: Vec<_> = facts
            .iter()
            .filter(|(name, (started, generator))| {
                page.generator_id
                    .as_deref()
                    .is_none_or(|want| want == generator)
                    && page.cursor.as_ref().is_none_or(|(at, id)| {
                        let key = (started.as_str(), name.as_str());
                        let cursor = (at.as_str(), id.as_str());
                        if page.descending {
                            key < cursor
                        } else {
                            key > cursor
                        }
                    })
            })
            .map(|(name, (started, generator))| (started.clone(), name.clone(), generator.clone()))
            .collect();
        ordered.sort();
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
                facts.remove(name);
                continue;
            };
            if (&item.started_at, &item.generator_id) != (started, generator) {
                // The journal was replaced: order again from what was read.
                facts.insert(item.run_id, (item.started_at, item.generator_id));
                continue 'order;
            }
            if page.state.is_none_or(|state| item.state == state) {
                items.push(item);
                if items.len() > page.limit {
                    break;
                }
            }
        }
        store.rebuild_index(facts, revision, rebuild || store.has_complete_index());
        return Ok(response(items, page.limit));
    }
}
