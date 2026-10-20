use anyhow::{Context, Result};
use camino::Utf8Path;

/// Manual retention sweep behind `qcg runs gc`.
///
/// The sweep mechanism belongs to the service: this adapter resolves the
/// budget, serializes a destructive pass against the store maintenance
/// lock, and reports the runs the sweep selected.
pub(crate) fn gc_runs(
    runs_dir: &Utf8Path,
    policy: &policy::RetentionPolicy,
    keep: Option<usize>,
    keep_failed: Option<usize>,
    delete: bool,
) -> Result<()> {
    // An explicit count overrides the resolved policy; an unset count keeps
    // the value the deployment resolved.
    let policy = policy.with_counts(keep, keep_failed);
    // Cleanup never creates a store that is not there.
    if !runs_dir.exists() {
        return Ok(());
    }
    // A destructive pass holds the store maintenance lock so two sweeps
    // never collect the same run. A dry run removes nothing, so it needs no
    // lock.
    let _maintenance = if delete {
        Some(
            service::try_lock_store_maintenance(runs_dir)?
                .context("run store maintenance is busy; retry")?,
        )
    } else {
        None
    };
    let selected = service::gc_run_directories(
        runs_dir,
        policy.keep,
        policy.keep_failed,
        delete,
        policy.max_scan_entries,
    )?;
    for path in selected {
        if delete {
            println!("deleted {path}");
        } else {
            println!("would_delete {path}");
        }
    }
    Ok(())
}

/// Automatic retention sweep that precedes `qcg run`.
///
/// It stays silent on stdout because stdout carries the run's JSON output;
/// removals are summarized on stderr.
pub(crate) fn auto_gc_runs(runs_dir: &Utf8Path, policy: &policy::RetentionPolicy) -> Result<()> {
    let enabled = policy::parse_bool_env("AUTO_GC", true)
        .map_err(|detail| anyhow::anyhow!("invalid GC configuration: {detail}"))?;
    if !enabled {
        return Ok(());
    }
    // Cleanup never creates a store that is not there.
    if !runs_dir.exists() {
        return Ok(());
    }
    // A busy store means another holder is already maintaining it. The
    // automatic sweep skips instead of failing the run it precedes.
    let _maintenance = match service::try_lock_store_maintenance(runs_dir)? {
        Some(lock) => lock,
        None => return Ok(()),
    };
    let removed = service::gc_run_directories(
        runs_dir,
        policy.keep,
        policy.keep_failed,
        true,
        policy.max_scan_entries,
    )?;
    if !removed.is_empty() {
        eprintln!(
            "retention: removed {} run(s) from {runs_dir}",
            removed.len()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use service::run_meta_dir;

    fn write_run(run_dir: &Utf8Path, status: &str, started_at: &str, retention_days: Option<u32>) {
        std::fs::create_dir_all(run_meta_dir(run_dir)).expect("meta dir should be created");
        let trace = "ab".repeat(16);
        let run_id = run_dir.file_name().unwrap_or("run");
        let retain = retention_days
            .map(|days| format!(",\"retention_days\":{days}"))
            .unwrap_or_default();
        let started = format!(
            "{{\"t\":\"run_started\",\"seq\":1,\"ts\":\"{started_at}\",\"run_id\":\"{run_id}\",\"trace_id\":\"{trace}\",\"span_id\":\"cd01\",\"generator\":\"gc@0.1.0\",\"generator_path\":\"gc\",\"contract_sha256\":\"abc\",\"inputs\":{{}},\"resource_hashes\":[],\"schema_version\":1{retain}}}"
        );
        // An interrupted run is a terminal event of its own, not a failure.
        let terminal = if status == "interrupted" {
            format!(
                "{{\"t\":\"run_interrupted\",\"seq\":2,\"ts\":\"{started_at}\",\"run_id\":\"{run_id}\",\"trace_id\":\"{trace}\",\"span_id\":\"cd02\",\"reason\":{{\"code\":\"interrupted\",\"message\":\"service shutdown\"}}}}"
            )
        } else {
            format!(
                "{{\"t\":\"run_finished\",\"seq\":2,\"ts\":\"{started_at}\",\"run_id\":\"{run_id}\",\"trace_id\":\"{trace}\",\"span_id\":\"cd02\",\"status\":\"{status}\",\"failures\":[],\"metrics\":{{}}}}"
            )
        };
        std::fs::write(
            run_meta_dir(run_dir).join("journal.jsonl"),
            format!("{started}\n{terminal}\n"),
        )
        .expect("journal should be written");
    }

    fn temp_runs_dir(label: &str) -> (std::path::PathBuf, Utf8PathBuf) {
        let root = std::env::temp_dir().join(format!("gc-{label}-{}", uuid::Uuid::now_v7()));
        let runs = Utf8PathBuf::from_path_buf(root.clone()).expect("path should be UTF-8");
        std::fs::create_dir_all(&runs).expect("runs dir should be created");
        (root, runs)
    }

    fn stamp(minutes: i64) -> String {
        (chrono::Utc::now() - chrono::Duration::minutes(minutes))
            .to_rfc3339()
            .replace("+00:00", "Z")
    }

    fn policy(keep: usize, keep_failed: usize) -> policy::RetentionPolicy {
        policy::RetentionPolicy {
            keep,
            keep_failed,
            max_scan_entries: 16,
        }
    }

    #[test]
    fn explicit_counts_override_the_resolved_policy() {
        let (root, runs) = temp_runs_dir("override");
        write_run(&runs.join("success-new"), "success", &stamp(1), None);
        write_run(&runs.join("success-old"), "success", &stamp(2), None);

        gc_runs(&runs, &policy(1, 0), Some(0), None, true).expect("the sweep should run");

        assert!(
            !runs.join("success-new").exists(),
            "an explicit keep of zero collects every run"
        );
        assert!(
            !runs.join("success-old").exists(),
            "an explicit keep of zero collects every run"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn resolved_policy_applies_when_no_count_is_given() {
        if env_process::isolated() {
            return;
        }
        // SAFETY: the isolated child process owns this environment.
        unsafe {
            std::env::set_var("GC_KEEP", "1");
            std::env::set_var("GC_KEEP_FAILED", "1");
        }
        let (root, runs) = temp_runs_dir("resolved");
        write_run(&runs.join("success-new"), "success", &stamp(1), None);
        write_run(&runs.join("success-old"), "success", &stamp(2), None);
        let resolved = policy::RetentionPolicy::from_env().expect("the policy should resolve");
        assert_eq!(resolved.keep, 1, "the environment resolves the keep budget");

        gc_runs(&runs, &resolved, None, None, true).expect("the sweep should run");

        assert!(
            runs.join("success-new").is_dir(),
            "the newest run stays inside the resolved keep budget"
        );
        assert!(
            !runs.join("success-old").exists(),
            "a run beyond the resolved budget is collected"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn automatic_sweep_removes_an_expired_run_and_keeps_a_fresh_one() {
        if env_process::isolated() {
            return;
        }
        // SAFETY: the isolated child process owns this environment.
        unsafe {
            std::env::set_var("AUTO_GC", "true");
        }
        let (root, runs) = temp_runs_dir("auto");
        write_run(&runs.join("expired"), "success", &stamp(1), Some(0));
        write_run(&runs.join("fresh"), "success", &stamp(2), Some(365));

        auto_gc_runs(&runs, &policy(1, 0)).expect("the automatic sweep should run");

        assert!(
            !runs.join("expired").exists(),
            "a run past its journal-recorded expiry is collected"
        );
        assert!(
            runs.join("fresh").is_dir(),
            "a fresh run inside the keep budget survives"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn manual_dry_run_leaves_run_directories_on_disk() {
        let (root, runs) = temp_runs_dir("dry-run");
        write_run(&runs.join("over-budget"), "success", &stamp(1), None);

        gc_runs(&runs, &policy(0, 0), None, None, false).expect("the dry run should succeed");

        assert!(
            runs.join("over-budget").is_dir(),
            "a dry run reports its candidates without removing them"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn interrupted_runs_consume_the_keep_budget() {
        let (root, runs) = temp_runs_dir("interrupted");
        write_run(
            &runs.join("interrupted-new"),
            "interrupted",
            &stamp(1),
            None,
        );
        write_run(&runs.join("success-old"), "success", &stamp(2), None);
        write_run(
            &runs.join("interrupted-expired"),
            "interrupted",
            &stamp(3),
            Some(0),
        );

        gc_runs(&runs, &policy(1, 0), None, None, true).expect("the sweep should run");

        assert!(
            runs.join("interrupted-new").is_dir(),
            "an interrupted run consumes the keep budget like any other terminal run"
        );
        assert!(
            !runs.join("success-old").exists(),
            "a run beyond the keep budget is collected"
        );
        assert!(
            !runs.join("interrupted-expired").exists(),
            "an expired run is collected regardless of the keep budget"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(test)]
    mod env_process {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../scripts/test-support/environment.rs"
        ));
    }
}
