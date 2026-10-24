//! Fake daemons run only in an isolated test subprocess; no process-wide
//! environment mutation can race unrelated tests.
use std::{path::PathBuf, time::Duration};

/// Wall-clock budget for a fixture client to start and publish its pid.
///
/// A deadline that has to cover the client's start cannot be smaller than
/// the start itself: the measured publish latency is a few milliseconds on
/// an idle runner, stayed under 120 ms on a runner oversubscribed threefold,
/// and reached 444 ms on the same runner, so every deadline that covers a
/// client start uses this budget. It stays below the client's own lifetime
/// so a timeout still fires before the client exits on its own.
#[cfg(target_os = "linux")]
pub(super) const CLIENT_START_BUDGET: Duration = Duration::from_secs(2);

/// Slack allowed between a stage deadline and the operation's return.
///
/// The deadline is honored to within the timer wakeup delay, which a loaded
/// runner can stretch; the probe count carries the deterministic evidence
/// that the deadline is global.
pub(super) const DEADLINE_OVERSHOOT_SLACK: Duration = Duration::from_secs(1);

/// How long a test waits for the container's outstanding cleanups to join.
///
/// The cleanup itself is bounded by the container timeout, but it starts on a
/// worker that a loaded runner can starve, so the wait has to outlast the
/// starvation rather than only the cleanup.
///
/// The value is measured rather than chosen to keep a test green: joining
/// the outstanding cleanups took 1.0 s to 2.1 s and 4.06 s at worst, over
/// 72 joins measured with the whole container suite running in parallel on
/// one CPU. The bound sits at 10 s, more than twice that worst case and
/// above the fake daemon's eight-second hang.
pub(crate) const CLEANUP_JOIN_BOUND: Duration = Duration::from_secs(10);

/// The largest gap the fake daemon may show between a container's stop
/// attempt and the removal that follows it.
///
/// The fake daemon hangs for eight seconds on the stop stage while these
/// tests freeze the stop timeout at one second, so a teardown that honors
/// the captured timeout removes the instance within a second or two and one
/// that ignores it waits out the whole hang. The bound sits above the
/// frozen timeout plus the log's one-second granularity, and below the hang,
/// so it separates the two without being tight enough to flake.
///
/// The gap is measured from the daemon's own arrival stamps, so a loaded
/// runner's starvation of the cleanup worker lands directly in it: the
/// product can honor the frozen timeout exactly and still show a wide gap.
///
/// The value is measured rather than chosen to keep a test green: the gap
/// was 2 s at worst and 1 s in the usual case, over 122 observations taken
/// on an unpinned runner, on one pinned CPU, on one CPU with eight
/// competing processes, and with the whole container and MCP suites running
/// in parallel on one CPU. The bound sits at 4 s, twice the worst observed
/// gap and below the eight-second hang it has to separate.
pub(crate) const STOP_TIMEOUT_GAP_BOUND: Duration = Duration::from_secs(4);

pub struct Daemon {
    pub root: PathBuf,
}

impl Daemon {
    pub fn isolated(test: &str) -> Option<Self> {
        const CHILD: &str = "QCG_DAEMON_POLICY_TEST_CHILD";
        if let Some(root) = std::env::var_os(CHILD) {
            return Some(Self { root: root.into() });
        }
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("qcg-daemon-policy-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let script = format!(
            r##"#!/bin/sh
root='{}'
printf '%s %s\n' "$(/bin/date +%s)" "$*" >> "$root/log"
mode=$(/bin/cat "$root/mode")
case "$1:$mode" in
  init:hang-init|profile:hang-admin|start:hang-start|stop:hang-stop|kill:hang-stop|delete:hang-delete|rm:hang-delete) exec /bin/sleep 8 ;;
  exec:hang-probe) case "$*" in *'-- true') exec /bin/sleep 8 ;; esac ;;
esac
case "$1" in
  profile) echo pool ;;
  exec)
    case "$*" in *'-- true') ;; *)
      if [ -f "$root/server" ]; then exec "$(/bin/cat "$root/server")" --session-id daemon-policy; fi ;;
    esac
    ;;
  run)
    shift
    while [ "$#" -gt 0 ]; do
      if [ "$1" = --cidfile ]; then shift; echo test-id > "$1"; fi
      shift
    done
    if [ "$mode" = docker-workload ]; then exec /bin/sleep 2; fi
    if [ -f "$root/server" ]; then exec "$(/bin/cat "$root/server")" --session-id daemon-policy; fi
    ;;
esac
exit 0
"##,
            root.display()
        );
        for name in ["incus", "docker"] {
            let path = root.join(name);
            std::fs::write(&path, &script).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        std::fs::write(root.join("mode"), "ok").unwrap();
        let paths = std::env::join_paths(std::iter::once(root.clone()).chain(
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
        ))
        .unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", test, "--nocapture"])
            .env(CHILD, &root)
            .env("PATH", paths);
        for name in policy::ContainerTimeouts::ENV_NAMES {
            command.env(name, "must-not-be-reread");
        }
        let output = command.output().unwrap();
        let _ = std::fs::remove_dir_all(root);
        assert!(
            output.status.success(),
            "child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        None
    }

    pub fn mode(&self, mode: &str) {
        std::fs::write(self.root.join("mode"), mode).unwrap();
        std::fs::write(self.root.join("log"), "").unwrap();
    }

    pub fn log(&self) -> String {
        std::fs::read_to_string(self.root.join("log")).unwrap_or_default()
    }

    /// The seconds between the first logged command containing `first` and
    /// the first later command containing `second`.
    ///
    /// The fake daemon stamps every command with the second it arrived, so a
    /// caller can measure how long the product waited between two daemon
    /// stages without depending on the caller's own scheduling.
    pub fn gap_between(&self, first: &str, second: &str) -> Option<Duration> {
        let mut first_at = None;
        for line in self.log().lines() {
            let Some((stamp, command)) = line.split_once(' ') else {
                continue;
            };
            let Ok(stamp) = stamp.parse::<u64>() else {
                continue;
            };
            let Some(first_at) = first_at else {
                if command.contains(first) {
                    first_at = Some(stamp);
                }
                continue;
            };
            if command.contains(second) {
                return Some(Duration::from_secs(stamp.saturating_sub(first_at)));
            }
        }
        None
    }

    pub async fn wait_for(&self, text: &str) {
        tokio::time::timeout(Duration::from_secs(4), async {
            while !self.log().contains(text) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fake daemon should reach stage");
    }
}
