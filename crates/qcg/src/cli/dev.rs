use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use qcg_server::ServerConfig;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

/// Per-generator eval suite run by `qcg dev --eval <generator-id>`.
pub(crate) const EVAL_SUITE_FILE: &str = "suite.json";

/// Human-readable reports always land under this root, matching `qcg eval`.
const EVAL_OUTPUT_ROOT: &str = ".qcg/evals";

/// Hidden eval run store. The dev server owns `--runs-dir` exclusively for
/// its whole lifetime, so eval cannot lock the same store; a hidden child
/// directory has its own lock and is skipped by run listing, queue scans,
/// and shutdown orphans (all ignore dot-prefixed entries).
const EVAL_RUNS_DIR: &str = ".dev-eval";

/// Modification times of every entry under a generator tree, keyed by path.
pub(crate) type MtimeFingerprint = BTreeMap<Utf8PathBuf, SystemTime>;

/// Fingerprint a generator tree by recursively collecting entry mtimes.
/// A missing root fingerprints as empty so a directory created later is
/// detected as a change.
pub(crate) fn fingerprint_tree(root: &Utf8Path) -> Result<MtimeFingerprint> {
    let mut fingerprint = MtimeFingerprint::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to scan generator directory `{directory}`"));
            }
        };
        for entry in entries {
            let entry = entry
                .with_context(|| format!("failed to scan generator directory `{directory}`"))?;
            let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                anyhow::anyhow!("generator path is not valid UTF-8: {}", path.display())
            })?;
            let metadata = entry
                .metadata()
                .with_context(|| format!("failed to inspect `{path}`"))?;
            if metadata.is_dir() {
                pending.push(path.clone());
            }
            let modified = metadata
                .modified()
                .with_context(|| format!("failed to read mtime of `{path}`"))?;
            fingerprint.insert(path, modified);
        }
    }
    Ok(fingerprint)
}

/// Paths added, removed, or re-stamped between two fingerprints.
pub(crate) fn changed_paths(
    previous: &MtimeFingerprint,
    current: &MtimeFingerprint,
) -> Vec<Utf8PathBuf> {
    let mut changed = Vec::new();
    for (path, modified) in current {
        if previous.get(path) != Some(modified) {
            changed.push(path.clone());
        }
    }
    for path in previous.keys() {
        if !current.contains_key(path) {
            changed.push(path.clone());
        }
    }
    changed.sort();
    changed
}

/// mtime poller over one generator root.
pub(crate) struct PollWatcher {
    root: Utf8PathBuf,
    fingerprint: MtimeFingerprint,
}

impl PollWatcher {
    pub(crate) fn new(root: Utf8PathBuf) -> Result<Self> {
        let fingerprint = fingerprint_tree(&root)?;
        Ok(Self { root, fingerprint })
    }

    /// Rescan and return the paths that changed since the previous scan.
    /// The fingerprint advances even when the caller ignores the result,
    /// so each change is reported once.
    pub(crate) fn poll(&mut self) -> Result<Vec<Utf8PathBuf>> {
        let current = fingerprint_tree(&self.root)?;
        let changed = changed_paths(&self.fingerprint, &current);
        self.fingerprint = current;
        Ok(changed)
    }
}

/// Run the serve server on loopback plus a watch loop that reports
/// generator changes and optionally re-runs `qcg eval` after each change.
/// Ctrl-C shuts down through the same serve path as `qcg serve`.
pub(crate) async fn run_dev(
    config: ServerConfig,
    bind: String,
    port: u16,
    watch_interval_ms: u64,
    eval_generator: Option<String>,
) -> Result<()> {
    if watch_interval_ms == 0 {
        anyhow::bail!("watch interval must be greater than zero");
    }
    let generators_dir = config.generators_dir.clone();
    let eval_target = eval_generator.map(|generator_id| {
        EvalTarget::new(
            &generators_dir,
            &config.runs_dir,
            config.providers_path.clone(),
            generator_id,
        )
    });
    if let Some(target) = &eval_target
        && !target.suite.is_file()
    {
        eprintln!(
            "dev eval warning: suite `{}` does not exist yet; evaluation is retried after each change",
            target.suite
        );
    }
    let watcher = PollWatcher::new(generators_dir.clone())?;
    eprintln!("qcg dev watching `{generators_dir}` every {watch_interval_ms} ms");
    tokio::spawn(watch_generators(
        watcher,
        Duration::from_millis(watch_interval_ms),
        eval_target,
    ));
    crate::serve_with_config(config, &bind, port).await
}

async fn watch_generators(
    mut watcher: PollWatcher,
    interval: Duration,
    eval_target: Option<EvalTarget>,
) {
    loop {
        tokio::time::sleep(interval).await;
        let changed = match watcher.poll() {
            Ok(changed) => changed,
            Err(error) => {
                eprintln!("generator watch scan failed: {error:#}");
                continue;
            }
        };
        if changed.is_empty() {
            continue;
        }
        for path in &changed {
            eprintln!("reloaded generators: {path}");
        }
        if let Some(target) = &eval_target {
            run_dev_eval(target).await;
        }
    }
}

struct EvalTarget {
    generator: Utf8PathBuf,
    suite: Utf8PathBuf,
    runs_dir: Utf8PathBuf,
    providers_path: Option<Utf8PathBuf>,
}

impl EvalTarget {
    fn new(
        generators_dir: &Utf8Path,
        runs_dir: &Utf8Path,
        providers_path: Option<Utf8PathBuf>,
        generator_id: String,
    ) -> Self {
        let generator = generators_dir.join(&generator_id);
        let suite = generator.join(EVAL_SUITE_FILE);
        Self {
            generator,
            suite,
            runs_dir: runs_dir.join(EVAL_RUNS_DIR),
            providers_path,
        }
    }
}

/// Eval failures are never fatal in the dev loop: report and retry on the
/// next change.
async fn run_dev_eval(target: &EvalTarget) {
    let result = super::eval::run_eval(
        target.generator.clone(),
        &target.suite,
        Utf8Path::new(EVAL_OUTPUT_ROOT),
        target.runs_dir.clone(),
        target.providers_path.clone(),
        None,
        false,
    )
    .await;
    match result {
        Ok(report) => eprintln!("dev eval {}", report.summary_line()),
        Err(error) => eprintln!("dev eval failed for `{}`: {error:#}", target.generator),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::args::{Cli, Command};
    use clap::Parser;
    use std::fs::File;
    use uuid::Uuid;

    fn temp_root(label: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory should be UTF-8")
            .join(format!("qcg-dev-{label}-{}", Uuid::now_v7()))
    }

    #[test]
    fn dev_arguments_default_and_reject_zero_watch_interval() {
        let cli = Cli::try_parse_from(["qcg", "dev"]).expect("bare dev should parse");
        let Command::Dev {
            bind,
            port,
            generators_dir,
            runs_dir,
            max_active_runs,
            watch_interval_ms,
            eval,
        } = cli.command
        else {
            panic!("expected the dev subcommand");
        };
        assert_eq!(bind, "127.0.0.1");
        assert_eq!(port, 0);
        assert_eq!(generators_dir, Utf8PathBuf::from("generators"));
        assert_eq!(runs_dir, Utf8PathBuf::from(".qcg/runs"));
        assert_eq!(max_active_runs, qcg_policy::DEFAULT_MAX_ACTIVE_RUNS);
        assert_eq!(watch_interval_ms, 500);
        assert!(eval.is_none());

        let error = Cli::try_parse_from(["qcg", "dev", "--watch-interval-ms", "0"])
            .expect_err("zero watch interval must be rejected");
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(
            error.to_string().contains("greater than zero"),
            "rejection must explain the bound: {error}"
        );

        let cli =
            Cli::try_parse_from(["qcg", "dev", "--watch-interval-ms", "250", "--eval", "demo"])
                .expect("explicit interval and eval should parse");
        let Command::Dev {
            watch_interval_ms,
            eval,
            ..
        } = cli.command
        else {
            panic!("expected the dev subcommand");
        };
        assert_eq!(watch_interval_ms, 250);
        assert_eq!(eval.as_deref(), Some("demo"));
    }

    #[test]
    fn dev_help_renders() {
        let error = Cli::try_parse_from(["qcg", "dev", "--help"])
            .expect_err("help exits through a clap error");
        assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
        let help = error.to_string();
        assert!(help.contains("--watch-interval-ms"), "{help}");
        assert!(help.contains("--eval"), "{help}");
    }

    #[test]
    fn poll_watcher_reports_only_mtime_changes() {
        let root = temp_root("watch");
        let generator = root.join("demo");
        let manifest = generator.join("qcg.toml");
        std::fs::create_dir_all(&generator).expect("generator directory should be created");
        std::fs::write(&manifest, "fixture").expect("manifest should be written");

        let mut watcher = PollWatcher::new(root.clone()).expect("initial fingerprint should scan");
        // Filesystem timestamp updates can lag behind the writes that
        // caused them (notably NTFS directory times), so two back-to-back
        // scans of a quiescent tree may disagree. Settle until consecutive
        // scans agree instead of asserting on the first poll.
        let mut changed = Vec::new();
        for _ in 0..100 {
            changed = watcher.poll().expect("settle poll should scan");
            if changed.is_empty() {
                break;
            }
        }
        assert!(
            changed.is_empty(),
            "an unchanged tree must not notify: {changed:?}"
        );

        // Re-stamp explicitly instead of sleeping: the check must not depend
        // on filesystem timestamp granularity.
        let file = File::options()
            .write(true)
            .open(&manifest)
            .expect("manifest should open for writing");
        file.set_modified(SystemTime::now() + Duration::from_secs(60))
            .expect("manifest mtime should be set");
        let changed = watcher.poll().expect("changed poll should scan");
        assert!(
            changed.contains(&manifest),
            "an mtime change must notify for `{manifest}`: {changed:?}"
        );
        assert!(
            watcher.poll().expect("settled poll should scan").is_empty(),
            "a change must be reported once"
        );

        std::fs::remove_file(&manifest).expect("manifest should be removed");
        let changed = watcher.poll().expect("removed poll should scan");
        assert!(
            changed.contains(&manifest),
            "a removed path must notify: {changed:?}"
        );

        std::fs::remove_dir_all(&root).ok();
    }
}
