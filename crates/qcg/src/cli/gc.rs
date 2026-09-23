use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::Contract;
use qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES;
use qcg_service::{run_meta_dir, run_summary};

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
    if !runs_dir.exists() {
        return Ok(());
    }
    let mut runs: Vec<RunGcCandidate> = Vec::new();
    let mut scanned = 0_usize;
    for entry in std::fs::read_dir(runs_dir)? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_DIRECTORY_SCAN_ENTRIES {
            anyhow::bail!("runs directory contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries");
        }
        let entry = entry?;
        let path = Utf8PathBuf::from_path_buf(entry.path())
            .map_err(|path| anyhow::anyhow!("run path is not valid UTF-8: {}", path.display()))?;
        if !path.is_dir() || !run_meta_dir(&path).join("journal.jsonl").exists() {
            continue;
        }
        let summary = run_summary(&path)?;
        if !matches!(summary.status.as_str(), "success" | "failed" | "canceled") {
            continue;
        }
        let retention_days = run_retention_days(&summary)?;
        let expired_by_retain = retention_days
            .map(|days| run_is_older_than(&summary.started_at, days))
            .transpose()?
            .unwrap_or(false);
        runs.push(RunGcCandidate {
            started_at: summary.started_at,
            status: summary.status,
            path,
            expired_by_retain,
        });
    }
    runs.sort_by(|left, right| right.started_at.cmp(&left.started_at));
    let mut seen_failed = 0_usize;
    for (index, run) in runs.into_iter().enumerate() {
        let is_failed = run.status == "failed";
        if is_failed {
            // Failed runs get an additional retention budget for post-mortems.
            if seen_failed < keep_failed && !run.expired_by_retain {
                seen_failed += 1;
                continue;
            }
            seen_failed += 1;
        }
        if index < keep && !run.expired_by_retain {
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
    let enabled = qcg_policy::parse_bool_env("QCG_AUTO_GC", true)
        .map_err(|detail| anyhow::anyhow!("invalid GC configuration: {detail}"))?;
    if enabled {
        gc_runs_impl(runs_dir, 50, 10, true, false)?;
    }
    Ok(())
}

struct RunGcCandidate {
    started_at: String,
    status: String,
    path: Utf8PathBuf,
    expired_by_retain: bool,
}

fn run_retention_days(summary: &qcg_service::RunSummary) -> Result<Option<u32>> {
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

fn run_is_older_than(started_at: &str, retention_days: u32) -> Result<bool> {
    if started_at.trim().is_empty() {
        return Ok(false);
    }
    let started = chrono::DateTime::parse_from_rfc3339(started_at)
        .with_context(|| format!("failed to parse run timestamp `{started_at}`"))?
        .with_timezone(&chrono::Utc);
    let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(retention_days));
    Ok(started < cutoff)
}
