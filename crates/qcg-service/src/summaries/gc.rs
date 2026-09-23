use super::super::run_dirs::journal_is_empty;
use super::super::types::ServiceError;
use camino::{Utf8Path, Utf8PathBuf};
use qcg_api::RunEvent;

use super::runs::run_summary;
use super::summary::run_meta_dir;

pub(crate) fn run_identity_event<'a>(
    run_dir: &Utf8Path,
    events: &'a [RunEvent],
) -> Result<(&'a RunEvent, &'a qcg_api::RunStartedEventData), ServiceError> {
    events
        .iter()
        .find_map(|event| event.data.run_started().map(|data| (event, data)))
        .ok_or_else(|| ServiceError::Invalid(format!("run `{run_dir}` has no run identity event")))
}

/// Deletes expired or over-retention terminal runs.
///
/// Successful, canceled, and interrupted runs count against `keep`; failed
/// runs get an additional `keep_failed` post-mortem budget, mirroring
/// `qcg runs gc`. A run past its contract `[retention].days` window is
/// always deleted regardless of the counts.
pub fn gc_run_directories(
    runs_dir: &Utf8Path,
    keep: usize,
    keep_failed: usize,
    delete: bool,
    max_scan_entries: usize,
) -> Result<Vec<Utf8PathBuf>, ServiceError> {
    if !runs_dir.exists() {
        return Ok(Vec::new());
    }
    let now = chrono::Utc::now();
    let mut candidates = Vec::new();
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        scanned = scanned.saturating_add(1);
        if scanned > max_scan_entries {
            return Err(ServiceError::Invalid(format!(
                "runs directory contains more than {max_scan_entries} entries"
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
        let expired = match summary.retention_days {
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
        let failed = summary.status == "failed";
        candidates.push((summary.started_at, path, expired, failed));
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0));
    let mut deleted = Vec::new();
    let mut seen_failed = 0_usize;
    for (index, (_, path, expired, failed)) in candidates.into_iter().enumerate() {
        if failed {
            // Failed runs keep their additional budget before the global
            // count applies, so post-mortem data survives normal churn.
            if seen_failed < keep_failed && !expired {
                seen_failed += 1;
                continue;
            }
            seen_failed += 1;
        }
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
            "{{\"t\":\"run_started\",\"seq\":1,\"ts\":\"{started_at}\",\"run_id\":\"{}\",\"trace_id\":\"{trace}\",\"span_id\":\"cd01\",\"generator\":\"gc@0.1.0\",\"generator_path\":\"gc\",\"contract_sha256\":\"abc\",\"inputs\":{{}},\"resource_hashes\":[],\"qcg\":\"0.1.0\",\"schema_version\":1{retain}}}\n{{\"t\":\"run_finished\",\"seq\":2,\"ts\":\"{started_at}\",\"run_id\":\"{}\",\"trace_id\":\"{trace}\",\"span_id\":\"cd02\",\"status\":\"{status}\",\"failures\":[],\"metrics\":{{}}}}\n",
            run_dir.file_name().unwrap_or("run"),
            run_dir.file_name().unwrap_or("run"),
        );
        std::fs::write(run_meta_dir(run_dir).join("journal.jsonl"), lines)
            .expect("journal should be written");
    }

    #[test]
    fn failed_runs_get_their_own_retention_budget_and_expiry_wins() {
        let root = std::env::temp_dir().join(format!("qcg-gc-{}", uuid::Uuid::now_v7()));
        let runs = Utf8PathBuf::from_path_buf(root.clone()).expect("path should be UTF-8");
        std::fs::create_dir_all(&runs).expect("runs dir should be created");
        let now = chrono::Utc::now();
        let stamp = |minutes: i64| {
            (now - chrono::Duration::minutes(minutes))
                .to_rfc3339()
                .replace("+00:00", "Z")
        };
        write_run(&runs.join("failed-new"), "failed", &stamp(2), None);
        write_run(&runs.join("failed-old"), "failed", &stamp(5), None);
        write_run(&runs.join("success"), "success", &stamp(1), None);
        write_run(&runs.join("expired"), "success", &stamp(30), Some(0));

        let deleted = gc_run_directories(
            &runs,
            1,
            1,
            true,
            qcg_policy::DEFAULT_MAX_DIRECTORY_SCAN_ENTRIES,
        )
        .expect("gc should succeed");
        assert!(
            deleted.iter().any(|path| path.ends_with("expired")),
            "an expired run is deleted regardless of keep counts: {deleted:?}"
        );
        assert!(
            deleted.iter().any(|path| path.ends_with("failed-old")),
            "the second failed run is over its post-mortem budget: {deleted:?}"
        );
        assert!(
            runs.join("failed-new").is_dir(),
            "the newest failed run stays inside keep_failed"
        );
        assert!(
            runs.join("success").is_dir(),
            "the newest success run stays inside the global keep count"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
