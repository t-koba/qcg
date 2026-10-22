//! Fake daemons run only in an isolated test subprocess; no process-wide
//! environment mutation can race unrelated tests.
use std::{path::PathBuf, time::Duration};

/// How long a test waits for the container's outstanding cleanups to join.
///
/// The cleanup itself is bounded by the container timeout, but it starts on a
/// worker that a loaded runner can starve, so the wait has to outlast the
/// starvation rather than only the cleanup.
pub(crate) const CLEANUP_JOIN_BOUND: Duration = Duration::from_secs(30);

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
/// The bound therefore has to outlast the starvation a loaded runner
/// inflicts, the same way `CLEANUP_JOIN_BOUND` does for the same cleanup
/// window, rather than sit just above the timeout it measures.
pub(crate) const STOP_TIMEOUT_GAP_BOUND: Duration = Duration::from_secs(6);

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
