use super::super::types::ServiceError;
use api::RunEvent;
use camino::{Utf8Path, Utf8PathBuf};

use super::runs::run_summary_core_with_seq;
use super::summary::run_meta_dir;

pub(crate) fn run_identity_event<'a>(
    run_dir: &Utf8Path,
    events: &'a [RunEvent],
) -> Result<(&'a RunEvent, &'a api::RunStartedEventData), ServiceError> {
    events
        .iter()
        .find_map(|event| event.data.run_started().map(|data| (event, data)))
        .ok_or_else(|| ServiceError::Invalid(format!("run `{run_dir}` has no run identity event")))
}

/// Selects the terminal runs a retention sweep collects.
///
/// Successful, canceled, and interrupted runs consume `keep`; failed runs
/// consume only the separate `keep_failed` post-mortem budget, mirroring
/// `runs gc`. A run past its journal-recorded retention window is always
/// selected regardless of the counts. Unreadable or malformed candidates
/// are logged and left untouched without stopping the sweep.
///
/// The returned directories are the selected runs, and their meaning depends
/// on the mode: in delete mode they are the directories removed successfully,
/// in dry-run mode they are the candidates the sweep would remove. A removal
/// that fails is an error, never a reported removal.
pub fn gc_run_directories(
    runs_dir: &Utf8Path,
    keep: usize,
    keep_failed: usize,
    delete: bool,
    max_scan_entries: usize,
) -> Result<Vec<Utf8PathBuf>, ServiceError> {
    let entries = match std::fs::read_dir(runs_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let now = chrono::Utc::now();
    let mut candidates = Vec::new();
    let mut scanned = 0_usize;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                crate::run_dirs::bump_scan_count(&mut scanned, max_scan_entries)?;
                tracing::warn!(%error, "skipping unreadable runs-directory entry during retention GC");
                continue;
            }
        };
        if crate::run_dirs::is_coordination_entry(&entry) {
            continue;
        }
        crate::run_dirs::bump_scan_count(&mut scanned, max_scan_entries)?;
        let path = match Utf8PathBuf::from_path_buf(entry.path()) {
            Ok(path) => path,
            Err(path) => {
                tracing::warn!(path = %path.display(), "skipping run with non-UTF-8 path during retention GC");
                continue;
            }
        };
        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::warn!(run_path = %path, %error, "skipping unreadable run candidate during retention GC");
                continue;
            }
        };
        if !metadata.is_dir() {
            continue;
        }
        let journal_path = run_meta_dir(&path).join("journal.jsonl");
        let journal_metadata = match std::fs::metadata(&journal_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                tracing::warn!(run_path = %path, %error, "skipping run with unreadable journal during retention GC");
                continue;
            }
        };
        if journal_metadata.len() == 0 {
            continue;
        }
        let (summary, _) = match run_summary_core_with_seq(&path) {
            Ok(summary) => summary,
            Err(error) => {
                tracing::warn!(run_path = %path, %error, "skipping run with unreadable or corrupt journal during retention GC");
                continue;
            }
        };
        if !matches!(
            summary.status.as_str(),
            "success" | "failed" | "canceled" | "interrupted"
        ) {
            continue;
        }
        let started_at = match chrono::DateTime::parse_from_rfc3339(&summary.started_at) {
            Ok(started_at) => started_at.with_timezone(&chrono::Utc),
            Err(error) => {
                tracing::warn!(run_path = %path, %error, "skipping run with invalid started_at during retention GC");
                continue;
            }
        };
        candidates.push((started_at, path));
    }
    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.0));
    let mut selected = Vec::new();
    let mut kept_non_failed = 0_usize;
    let mut kept_failed = 0_usize;
    for (started_at, path) in candidates {
        // A temporary interrupt terminal is not collectible until the entire
        // preemption handoff ends, including the gap between execution leases.
        // Probe admission, handoff, and execution ownership before charging a
        // keep slot. Admission fences same-ID recreation; busy/changed runs
        // must not evict older, genuinely terminal runs from the budget.
        let protection = (|| {
            let Some(admission) = crate::run_dirs::try_lock_run_admission(&path)? else {
                return Ok(None);
            };
            let Some(handoff) = crate::run_dirs::try_lock_run_handoff(&path)? else {
                return Ok(None);
            };
            let Some(execution) = crate::run_dirs::try_lock_existing_run_execution(&path)? else {
                return Ok(None);
            };
            let (summary, _) = run_summary_core_with_seq(&path)?;
            Ok::<_, ServiceError>(Some((admission, handoff, execution, summary)))
        })();
        let (_admission, _handoff, _execution, summary) = match protection {
            Ok(Some(protection)) => protection,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(run_path = %path, %error, "skipping unprotected or changed run during retention GC");
                continue;
            }
        };
        if !matches!(
            summary.status.as_str(),
            "success" | "failed" | "canceled" | "interrupted"
        ) {
            continue;
        }
        // A replaced identity must be sorted afresh on the next sweep.
        if chrono::DateTime::parse_from_rfc3339(&summary.started_at)
            .ok()
            .is_none_or(|current| current != started_at)
        {
            continue;
        }
        let expired = summary.retention_days.is_some_and(|days| {
            chrono::Duration::try_days(i64::from(days))
                .and_then(|duration| now.checked_sub_signed(duration))
                .is_some_and(|cutoff| started_at < cutoff)
        });
        let failed = summary.status == "failed";
        let keep_candidate = if !expired && failed && kept_failed < keep_failed {
            kept_failed += 1;
            true
        } else if !expired && !failed && kept_non_failed < keep {
            kept_non_failed += 1;
            true
        } else {
            false
        };
        if keep_candidate {
            continue;
        }
        if delete {
            std::fs::remove_dir_all(&path)?;
        }
        selected.push(path);
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_run(run_dir: &Utf8Path, status: &str, started_at: &str, retention_days: Option<u32>) {
        std::fs::create_dir_all(run_meta_dir(run_dir)).expect("meta dir should be created");
        let retain = retention_days
            .map(|days| format!(",\"retention_days\":{days}"))
            .unwrap_or_default();
        let trace = "ab".repeat(16);
        let lines = format!(
            "{{\"t\":\"run_started\",\"seq\":1,\"ts\":\"{started_at}\",\"run_id\":\"{}\",\"trace_id\":\"{trace}\",\"span_id\":\"cd01\",\"generator\":\"gc@0.1.0\",\"generator_path\":\"gc\",\"contract_sha256\":\"abc\",\"inputs\":{{}},\"resource_hashes\":[],\"schema_version\":1{retain}}}\n{{\"t\":\"run_finished\",\"seq\":2,\"ts\":\"{started_at}\",\"run_id\":\"{}\",\"trace_id\":\"{trace}\",\"span_id\":\"cd02\",\"status\":\"{status}\",\"failures\":[],\"metrics\":{{}}}}\n",
            run_dir.file_name().unwrap_or("run"),
            run_dir.file_name().unwrap_or("run"),
        );
        std::fs::write(run_meta_dir(run_dir).join("journal.jsonl"), lines)
            .expect("journal should be written");
    }

    #[test]
    fn admission_owned_candidate_is_skipped_without_consuming_keep_slots() {
        let root = crate::tests::support::temp_run_dir("gc-admission-owned");
        std::fs::create_dir_all(&root).expect("temp root should be created");
        let _temp = crate::tests::support::TempGuard(root.clone());
        let stamp =
            |minutes| (chrono::Utc::now() - chrono::Duration::minutes(minutes)).to_rfc3339();
        let admission_owned = root.join("admission-owned");
        let older = root.join("older-unlocked");
        let oldest = root.join("oldest-unlocked");
        write_run(&admission_owned, "success", &stamp(1), None);
        write_run(&older, "success", &stamp(2), None);
        write_run(&oldest, "success", &stamp(3), None);
        let _admission = crate::run_dirs::try_lock_run_admission(&admission_owned)
            .unwrap()
            .unwrap();
        let deleted = gc_run_directories(&root, 1, 0, true, 16).unwrap();
        assert_eq!(
            deleted,
            vec![oldest],
            "the busy candidate must not consume the only keep slot"
        );
        assert!(admission_owned.exists());
        assert!(older.exists());
    }

    #[test]
    fn dry_run_reports_candidates_without_removing_them() {
        let root = crate::tests::support::temp_run_dir("gc-dry-run");
        std::fs::create_dir_all(&root).expect("temp root should be created");
        let _temp = crate::tests::support::TempGuard(root.clone());
        let stamp =
            |minutes| (chrono::Utc::now() - chrono::Duration::minutes(minutes)).to_rfc3339();
        let expired = root.join("expired");
        let kept = root.join("kept");
        write_run(&expired, "success", &stamp(1), Some(0));
        write_run(&kept, "success", &stamp(2), None);
        let selected = gc_run_directories(&root, 1, 0, false, 16).unwrap();
        assert_eq!(
            selected,
            vec![expired.clone()],
            "a dry run reports the candidate it would remove"
        );
        assert!(
            expired.is_dir(),
            "a dry run must not remove the candidate it reports"
        );
        assert!(kept.is_dir(), "a kept run stays in the store");
    }

    #[test]
    fn execution_owned_terminals_survive_gc_without_consuming_keep_slots() {
        let runs = crate::tests::support::temp_run_dir("gc-execution-owned");
        std::fs::create_dir_all(&runs).unwrap();
        let _temp = crate::tests::support::TempGuard(runs.clone());
        let stamp =
            |minutes| (chrono::Utc::now() - chrono::Duration::minutes(minutes)).to_rfc3339();
        let active = runs.join("active");
        let expired_active = runs.join("expired-active");
        let older = runs.join("older");
        let expired_idle = runs.join("expired-idle");
        write_run(&active, "success", &stamp(1), None);
        write_run(&older, "success", &stamp(2), None);
        write_run(&expired_active, "success", &stamp(3), Some(0));
        write_run(&expired_idle, "success", &stamp(4), Some(0));
        let active_lease = crate::run_dirs::try_lock_run_execution(&active)
            .unwrap()
            .unwrap();
        let expired_lease = crate::run_dirs::try_lock_run_execution(&expired_active)
            .unwrap()
            .unwrap();

        let deleted = gc_run_directories(&runs, 1, 0, true, 4).unwrap();
        assert_eq!(
            deleted,
            vec![expired_idle],
            "ordinary unlocked expiry still wins"
        );
        assert!(active.is_dir());
        assert!(
            expired_active.is_dir(),
            "even expired terminals are protected by execution ownership"
        );
        assert!(
            older.is_dir(),
            "a busy newer candidate must not consume the keep slot"
        );
        drop(active_lease);
        drop(expired_lease);
        let deleted = gc_run_directories(&runs, 1, 0, true, 4).unwrap();
        assert_eq!(deleted, vec![older, expired_active]);
        assert!(
            active.is_dir(),
            "normal newest-first retention resumes after unlock"
        );
    }

    #[test]
    fn failed_runs_get_their_own_retention_budget_and_expiry_wins() {
        let root = std::env::temp_dir().join(format!("gc-{}", uuid::Uuid::now_v7()));
        let runs = Utf8PathBuf::from_path_buf(root.clone()).expect("path should be UTF-8");
        std::fs::create_dir_all(&runs).expect("runs dir should be created");
        let now = chrono::Utc::now();
        let stamp = |minutes: i64| {
            (now - chrono::Duration::minutes(minutes))
                .to_rfc3339()
                .replace("+00:00", "Z")
        };
        write_run(&runs.join("failed-new"), "failed", &stamp(1), None);
        write_run(&runs.join("success"), "success", &stamp(2), None);
        write_run(&runs.join("failed-middle"), "failed", &stamp(3), None);
        write_run(&runs.join("success-old"), "success", &stamp(4), None);
        write_run(&runs.join("failed-old"), "failed", &stamp(5), None);
        write_run(&runs.join("expired"), "success", &stamp(30), Some(0));

        let deleted = gc_run_directories(
            &runs,
            1,
            1,
            true,
            policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
        )
        .expect("gc should succeed");
        assert!(
            deleted.iter().any(|path| path.ends_with("expired")),
            "an expired run is deleted regardless of keep counts: {deleted:?}"
        );
        assert!(
            deleted.iter().any(|path| path.ends_with("failed-middle"))
                && deleted.iter().any(|path| path.ends_with("failed-old")),
            "failed runs beyond keep_failed are deleted: {deleted:?}"
        );
        assert!(
            deleted.iter().any(|path| path.ends_with("success-old")),
            "successful runs beyond keep are deleted: {deleted:?}"
        );
        assert!(
            runs.join("failed-new").is_dir(),
            "the newest failed run stays inside keep_failed even though it is newer than success"
        );
        assert!(
            runs.join("success").is_dir(),
            "the newest success run stays inside the global keep count"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn malformed_invalidation_does_not_block_retention_gc() {
        let root = std::env::temp_dir().join(format!("gc-invalidation-{}", uuid::Uuid::now_v7()));
        let runs = Utf8PathBuf::from_path_buf(root.clone()).expect("path should be UTF-8");
        std::fs::create_dir_all(&runs).expect("runs dir should be created");
        let run = runs.join("expired");
        let started_at = (chrono::Utc::now() - chrono::Duration::minutes(30))
            .to_rfc3339()
            .replace("+00:00", "Z");
        write_run(&run, "success", &started_at, Some(0));
        std::fs::write(run_meta_dir(&run).join("invalidation.json"), "not-json")
            .expect("malformed invalidation should be written");

        assert!(
            crate::summaries::runs::run_summary(&run).is_err(),
            "normal run summaries fail closed for malformed invalidation sidecars"
        );
        let deleted = gc_run_directories(
            &runs,
            10,
            10,
            true,
            policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
        )
        .expect("retention gc should use the core summary projection");
        assert_eq!(deleted, vec![run]);
        assert!(
            !runs.join("expired").exists(),
            "expired run should be collected"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unrepresentably_long_retention_does_not_expire_a_run() {
        let root = std::env::temp_dir().join(format!("gc-long-retention-{}", uuid::Uuid::now_v7()));
        let runs = Utf8PathBuf::from_path_buf(root.clone()).expect("path should be UTF-8");
        std::fs::create_dir_all(&runs).expect("runs dir should be created");
        let now = chrono::Utc::now();
        let started_at = (now - chrono::Duration::days(30))
            .to_rfc3339()
            .replace("+00:00", "Z");
        write_run(
            &runs.join("long-retention"),
            "success",
            &started_at,
            Some(u32::MAX),
        );

        gc_run_directories(
            &runs,
            1,
            0,
            true,
            policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
        )
        .expect("a representable run with huge retention should not panic");
        assert!(
            runs.join("long-retention").is_dir(),
            "an out-of-range retention interval is not treated as expired"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn corrupt_candidate_is_skipped_while_valid_over_budget_run_is_collected() {
        let root = std::env::temp_dir().join(format!("gc-corrupt-{}", uuid::Uuid::now_v7()));
        let runs = Utf8PathBuf::from_path_buf(root.clone()).expect("path should be UTF-8");
        std::fs::create_dir_all(&runs).expect("runs dir should be created");
        let now = chrono::Utc::now();
        let stamp = now.to_rfc3339().replace("+00:00", "Z");
        let corrupt = runs.join("corrupt");
        std::fs::create_dir_all(run_meta_dir(&corrupt)).expect("meta dir should be created");
        std::fs::write(run_meta_dir(&corrupt).join("journal.jsonl"), "not-json\n")
            .expect("corrupt journal should be written");
        write_run(&runs.join("valid"), "success", &stamp, None);

        let deleted = gc_run_directories(
            &runs,
            0,
            0,
            true,
            policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
        )
        .expect("corrupt candidate should not abort gc");
        assert!(
            runs.join("corrupt").is_dir(),
            "corrupt run remains untouched"
        );
        assert!(
            !runs.join("valid").exists(),
            "valid over-budget run is collected"
        );
        assert_eq!(deleted, vec![runs.join("valid")]);
        let _ = std::fs::remove_dir_all(&root);
    }
}
