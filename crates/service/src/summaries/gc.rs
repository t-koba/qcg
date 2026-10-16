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

/// Deletes expired or over-retention terminal runs.
///
/// Successful, canceled, and interrupted runs consume `keep`; failed runs
/// consume only the separate `keep_failed` post-mortem budget, mirroring
/// `runs gc`. A run past its contract `[retention].days` window is
/// always deleted regardless of the counts. Unreadable or malformed
/// candidates are logged and left untouched without stopping the sweep.
pub(crate) fn gc_run_directories(
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
                scanned = scanned.saturating_add(1);
                if scanned > max_scan_entries {
                    return Err(ServiceError::Invalid(format!(
                        "runs directory contains more than {max_scan_entries} entries"
                    )));
                }
                tracing::warn!(%error, "skipping unreadable runs-directory entry during retention GC");
                continue;
            }
        };
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
        let expired = summary.retention_days.is_some_and(|days| {
            chrono::Duration::try_days(i64::from(days))
                .and_then(|duration| now.checked_sub_signed(duration))
                .is_some_and(|cutoff| started_at < cutoff)
        });
        let failed = summary.status == "failed";
        candidates.push((started_at, path, expired, failed));
    }
    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.0));
    let mut deleted = Vec::new();
    let mut kept_non_failed = 0_usize;
    let mut kept_failed = 0_usize;
    for (_, path, expired, failed) in candidates {
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
            deleted.push(path);
        }
    }
    Ok(deleted)
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
