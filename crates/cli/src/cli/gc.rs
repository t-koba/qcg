use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use contract::Contract;
use policy::MAX_DIRECTORY_SCAN_ENTRIES;
use service::{run_meta_dir, run_summary};

pub(crate) fn gc_runs(
    runs_dir: &Utf8Path,
    keep: usize,
    keep_failed: usize,
    delete: bool,
) -> Result<()> {
    gc_runs_impl(runs_dir, keep, keep_failed, delete, true)
}

pub(crate) fn gc_runs_impl(
    runs_dir: &Utf8Path,
    keep: usize,
    keep_failed: usize,
    delete: bool,
    report: bool,
) -> Result<()> {
    let entries = match std::fs::read_dir(runs_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let now = chrono::Utc::now();
    let mut runs: Vec<RunGcCandidate> = Vec::new();
    let mut scanned = 0_usize;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                scanned = scanned.saturating_add(1);
                if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
                    anyhow::bail!(
                        "runs directory contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
                    );
                }
                eprintln!("warning: skipping unreadable runs-directory entry during GC: {error}");
                continue;
            }
        };
        scanned = scanned.saturating_add(1);
        if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
            anyhow::bail!("runs directory contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries");
        }
        let path = match Utf8PathBuf::from_path_buf(entry.path()) {
            Ok(path) => path,
            Err(path) => {
                eprintln!(
                    "warning: skipping run with non-UTF-8 path during GC: {}",
                    path.display()
                );
                continue;
            }
        };
        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) => {
                eprintln!("warning: skipping unreadable run candidate `{path}` during GC: {error}");
                continue;
            }
        };
        if !metadata.is_dir() {
            continue;
        }
        let journal_path = run_meta_dir(&path).join("journal.jsonl");
        match std::fs::metadata(&journal_path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                eprintln!(
                    "warning: skipping run with unreadable journal `{journal_path}` during GC: {error}"
                );
                continue;
            }
        }
        let summary = match run_summary(&path) {
            Ok(summary) => summary,
            Err(error) => {
                eprintln!(
                    "warning: skipping run with unreadable or corrupt journal/summary `{path}` during GC: {error}"
                );
                continue;
            }
        };
        if !matches!(
            summary.status.as_str(),
            "success" | "failed" | "canceled" | "interrupted"
        ) {
            continue;
        }
        let retention_days = match run_retention_days(&summary) {
            Ok(retention_days) => retention_days,
            Err(error) => {
                eprintln!(
                    "warning: skipping run with invalid retention metadata `{path}` during GC: {error:#}"
                );
                continue;
            }
        };
        let started_at = match chrono::DateTime::parse_from_rfc3339(&summary.started_at) {
            Ok(started_at) => started_at.with_timezone(&chrono::Utc),
            Err(error) => {
                eprintln!(
                    "warning: skipping run with invalid timestamp `{path}` during GC: {error}"
                );
                continue;
            }
        };
        let expired_by_retain = retention_days.is_some_and(|days| {
            chrono::Duration::try_days(i64::from(days))
                .and_then(|duration| now.checked_sub_signed(duration))
                .is_some_and(|cutoff| started_at < cutoff)
        });
        runs.push(RunGcCandidate {
            started_at,
            status: summary.status,
            path,
            expired_by_retain,
        });
    }
    runs.sort_by_key(|run| std::cmp::Reverse(run.started_at));
    let mut kept_non_failed = 0_usize;
    let mut kept_failed = 0_usize;
    for run in runs {
        let is_failed = run.status == "failed";
        let keep_candidate = if !run.expired_by_retain && is_failed && kept_failed < keep_failed {
            kept_failed += 1;
            true
        } else if !run.expired_by_retain && !is_failed && kept_non_failed < keep {
            kept_non_failed += 1;
            true
        } else {
            false
        };
        if keep_candidate {
            continue;
        }
        if delete {
            std::fs::remove_dir_all(&run.path)
                .with_context(|| format!("failed to remove run directory `{}`", run.path))?;
            if report {
                println!("deleted {}", run.path);
            }
        } else if report {
            println!("would_delete {}", run.path);
        }
    }
    Ok(())
}

pub(crate) fn auto_gc_runs(runs_dir: &Utf8Path) -> Result<()> {
    let enabled = policy::parse_bool_env("AUTO_GC", true)
        .map_err(|detail| anyhow::anyhow!("invalid GC configuration: {detail}"))?;
    if enabled {
        gc_runs_impl(runs_dir, 50, 10, true, false)?;
    }
    Ok(())
}

struct RunGcCandidate {
    started_at: chrono::DateTime<chrono::Utc>,
    status: String,
    path: Utf8PathBuf,
    expired_by_retain: bool,
}

fn run_retention_days(summary: &service::RunSummary) -> Result<Option<u32>> {
    if summary.retention_days.is_some() {
        return Ok(summary.retention_days);
    }
    let contract =
        Contract::load(Utf8PathBuf::from(&summary.generator_path)).with_context(|| {
            format!(
                "failed to load generator contract `{}` for run `{}` retention",
                summary.generator_path, summary.run_id
            )
        })?;
    Ok(contract.manifest.retention.days)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_run(run_dir: &Utf8Path, status: &str, started_at: &str, retention_days: u32) {
        std::fs::create_dir_all(run_meta_dir(run_dir)).expect("meta dir should be created");
        let trace = "ab".repeat(16);
        let run_id = run_dir.file_name().unwrap_or("run");
        let lines = format!(
            "{{\"t\":\"run_started\",\"seq\":1,\"ts\":\"{started_at}\",\"run_id\":\"{run_id}\",\"trace_id\":\"{trace}\",\"span_id\":\"cd01\",\"generator\":\"gc@0.1.0\",\"generator_path\":\"gc\",\"contract_sha256\":\"abc\",\"inputs\":{{}},\"resource_hashes\":[],\"schema_version\":1,\"retention_days\":{retention_days}}}\n{{\"t\":\"run_finished\",\"seq\":2,\"ts\":\"{started_at}\",\"run_id\":\"{run_id}\",\"trace_id\":\"{trace}\",\"span_id\":\"cd02\",\"status\":\"{status}\",\"failures\":[],\"metrics\":{{}}}}\n"
        );
        std::fs::write(run_meta_dir(run_dir).join("journal.jsonl"), lines)
            .expect("journal should be written");
    }

    fn temp_runs_dir(label: &str) -> (std::path::PathBuf, Utf8PathBuf) {
        let root = std::env::temp_dir().join(format!("gc-{label}-{}", uuid::Uuid::now_v7()));
        let runs = Utf8PathBuf::from_path_buf(root.clone()).expect("path should be UTF-8");
        std::fs::create_dir_all(&runs).expect("runs dir should be created");
        (root, runs)
    }

    #[test]
    fn failed_runs_use_separate_retention_budget_from_successful_runs() {
        let (root, runs) = temp_runs_dir("budgets");
        let now = chrono::Utc::now();
        let stamp = |minutes: i64| {
            (now - chrono::Duration::minutes(minutes))
                .to_rfc3339()
                .replace("+00:00", "Z")
        };
        write_run(&runs.join("failed-new"), "failed", &stamp(1), 10);
        write_run(&runs.join("success"), "success", &stamp(2), 10);
        write_run(&runs.join("failed-middle"), "failed", &stamp(3), 10);
        write_run(&runs.join("success-old"), "success", &stamp(4), 10);
        write_run(&runs.join("failed-old"), "failed", &stamp(5), 10);

        gc_runs_impl(&runs, 1, 1, true, false).expect("GC should succeed");
        assert!(runs.join("failed-new").is_dir(), "newest failure is kept");
        assert!(runs.join("success").is_dir(), "newest success is kept");
        assert!(
            !runs.join("failed-middle").exists(),
            "over-budget failure is deleted"
        );
        assert!(
            !runs.join("failed-old").exists(),
            "over-budget failure is deleted"
        );
        assert!(
            !runs.join("success-old").exists(),
            "over-budget success is deleted"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn unrepresentably_long_retention_does_not_expire_a_run() {
        let (root, runs) = temp_runs_dir("long-retention");
        let started_at = (chrono::Utc::now() - chrono::Duration::days(30)).to_rfc3339();
        write_run(
            &runs.join("long-retention"),
            "success",
            &started_at,
            u32::MAX,
        );

        gc_runs_impl(&runs, 1, 0, true, false)
            .expect("a representable run with huge retention should not panic");
        assert!(
            runs.join("long-retention").is_dir(),
            "an out-of-range retention interval is not treated as expired"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_candidate_is_skipped_while_valid_over_budget_run_is_collected() {
        let (root, runs) = temp_runs_dir("corrupt");
        let corrupt = runs.join("corrupt");
        std::fs::create_dir_all(run_meta_dir(&corrupt)).expect("meta dir should be created");
        std::fs::write(run_meta_dir(&corrupt).join("journal.jsonl"), "not-json\n")
            .expect("corrupt journal should be written");
        write_run(
            &runs.join("valid"),
            "success",
            &chrono::Utc::now().to_rfc3339(),
            10,
        );

        gc_runs_impl(&runs, 0, 0, true, false)
            .expect("corrupt candidate should not abort the sweep");
        assert!(corrupt.is_dir(), "corrupt run remains untouched");
        assert!(
            !runs.join("valid").exists(),
            "valid over-budget run is collected"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
