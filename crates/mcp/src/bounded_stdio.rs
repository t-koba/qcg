use rmcp::RoleClient;
use rmcp::service::{RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::transport::Transport;
use rmcp::transport::async_rw::AsyncRwTransport;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;
use tokio::io::{AsyncRead, ReadBuf};

const READ_CHUNK_BYTES: usize = 8 * 1024;
const CHILD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(unix)]
const CHILD_EXIT_POLL_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(target_os = "linux")]
const GROUP_STOP_SETTLE: Duration = Duration::from_millis(500);
#[cfg(target_os = "linux")]
const LIVE_GROUP_MEMBER_ERROR: &str =
    "owned MCP stdio process group still has a live member after the group signal";

/// Outcome of an owned stdio transport close, shared with the session that
/// settles the launch. The SDK service loop logs a failed transport close but
/// still reports a completed close, so the real outcome must be observed here.
#[derive(Clone)]
pub struct CloseReceipt {
    state: Arc<Mutex<Option<Result<(), String>>>>,
}

impl CloseReceipt {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn record(&self, result: &std::io::Result<()>) {
        let recorded = result
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.to_string());
        *self.state.lock().unwrap_or_else(|error| error.into_inner()) = Some(recorded);
    }

    pub(crate) fn confirmed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .is_some_and(Result::is_ok)
    }
}

pub(crate) struct BoundedChildTransport {
    child: tokio::process::Child,
    #[cfg(unix)]
    process_group_id: Option<i32>,
    transport: AsyncRwTransport<
        RoleClient,
        BoundedLineReader<tokio::process::ChildStdout>,
        tokio::process::ChildStdin,
    >,
    container_cleanup: Option<container::Session>,
    close_receipt: CloseReceipt,
    #[cfg(feature = "test-probe")]
    inject_close_failure: bool,
    #[cfg(all(unix, feature = "test-probe"))]
    inject_group_signal_failure: bool,
    /// Overrides the errno observed from the owned group's `killpg`, so the
    /// real failure classifier runs deterministically on every host.
    #[cfg(all(unix, feature = "test-probe"))]
    inject_group_signal_errno: Option<i32>,
    /// Overrides the owned-group liveness answer used by the classifier:
    /// `Ok(true)` live member, `Ok(false)` zombie-only, `Err` unreadable.
    #[cfg(all(unix, feature = "test-probe"))]
    inject_group_live_member: Option<std::io::Result<bool>>,
}

impl BoundedChildTransport {
    #[allow(dead_code)]
    pub(crate) fn spawn(
        command: tokio::process::Command,
        max_message_bytes: usize,
    ) -> std::io::Result<Self> {
        Self::spawn_with_scope(command, max_message_bytes, None)
    }

    /// Spawns with race-free cgroup self-placement before payload execution
    /// when a run scope is present. Fails before spawn when the delegation
    /// is invalid; `None` keeps existing live process-group behavior.
    pub(crate) fn spawn_with_scope(
        mut command: tokio::process::Command,
        max_message_bytes: usize,
        scope: Option<&policy::HostCgroupScope>,
    ) -> std::io::Result<Self> {
        // The SDK service task runs via tokio::spawn, so a thread-local probe
        // flag can only be read here, on the constructing (test) thread.
        #[cfg(feature = "test-probe")]
        let inject_close_failure = crate::mcp::session::close_probe::should_fail_close();
        #[cfg(all(unix, feature = "test-probe"))]
        let inject_group_signal_failure =
            crate::mcp::session::close_probe::should_fail_group_signal();
        #[cfg(all(unix, feature = "test-probe"))]
        let inject_group_signal_errno = None;
        #[cfg(all(unix, feature = "test-probe"))]
        let inject_group_live_member = None;
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            command.as_std_mut().process_group(0);
        }
        #[cfg(target_os = "linux")]
        if let Some(scope) = scope {
            apply_host_scope(&mut command, scope)?;
        }
        #[cfg(not(target_os = "linux"))]
        if scope.is_some() {
            return Err(std::io::Error::other(
                "host cgroup launch is Linux cgroup-v2 only",
            ));
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        #[cfg(unix)]
        // OS pids always fit i32; a conversion failure only skips the
        // group kill while the owned child handle (kill_on_drop) still
        // terminates the child itself.
        let process_group_id = child.id().and_then(|pid| i32::try_from(pid).ok());
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("MCP child stdout was not piped"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("MCP child stdin was not piped"))?;
        Ok(Self {
            child,
            #[cfg(unix)]
            process_group_id,
            transport: AsyncRwTransport::new(
                BoundedLineReader::new(stdout, max_message_bytes),
                stdin,
            ),
            container_cleanup: None,
            close_receipt: CloseReceipt::new(),
            #[cfg(feature = "test-probe")]
            inject_close_failure,
            #[cfg(all(unix, feature = "test-probe"))]
            inject_group_signal_failure,
            #[cfg(all(unix, feature = "test-probe"))]
            inject_group_signal_errno,
            #[cfg(all(unix, feature = "test-probe"))]
            inject_group_live_member,
        })
    }

    pub(crate) fn with_container_cleanup(mut self, session: container::Session) -> Self {
        self.container_cleanup = Some(session);
        self
    }

    pub(crate) fn close_receipt(&self) -> CloseReceipt {
        self.close_receipt.clone()
    }
}

impl Transport<RoleClient> for BoundedChildTransport {
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.transport.send(item)
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        self.transport.receive()
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        let result = self.close_inner().await;
        self.close_receipt.record(&result);
        result
    }
}

impl BoundedChildTransport {
    async fn close_inner(&mut self) -> std::io::Result<()> {
        // Captured before terminate consumes the field: confirmation must use
        // the owned PGID only, never a reaped PID or a saved signal anchor.
        #[cfg(target_os = "linux")]
        let group_to_verify: Option<i32> = self.process_group_id;
        #[cfg(not(target_os = "linux"))]
        let group_to_verify: Option<i32> = None;
        #[cfg(feature = "test-probe")]
        if self.inject_close_failure {
            return Err(std::io::Error::other(
                "injected MCP stdio transport close failure",
            ));
        }
        match tokio::time::timeout(CHILD_SHUTDOWN_TIMEOUT, self.transport.close()).await {
            Ok(result) => result?,
            Err(_) => {
                let group_result = self.terminate_process_group();
                self.terminate_child().await?;
                self.kill_container_blocking();
                group_result?;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "MCP stdio transport close timed out",
                ));
            }
        }
        match tokio::time::timeout(CHILD_SHUTDOWN_TIMEOUT, self.wait_for_child()).await {
            Ok(result) => {
                result?;
            }
            Err(_) => {
                let group_result = self.terminate_process_group();
                self.terminate_child().await?;
                self.kill_container_blocking();
                group_result?;
            }
        }
        self.confirm_group_stopped(group_to_verify).await?;
        self.kill_container_blocking();
        Ok(())
    }

    /// Confirms the owned group has stopped after a successful group signal.
    ///
    /// Only Linux needs this: POSIX `killpg` returns 0 when *at least one*
    /// member was signalled, so a member with different credentials can
    /// survive. BSD/macOS instead reports EPERM when any member cannot be
    /// signalled, so success there already proves the whole group was
    /// signalled; a post-reap libproc re-query cannot distinguish an emptied
    /// group from a failed lookup and would reject a clean close. Everywhere
    /// but Linux this is therefore a no-op and pre-existing behaviour stands.
    ///
    /// A kernel null probe proves a truly empty group (ESRCH) without trusting
    /// procfs. While the group is still occupied, a null signal counts dead
    /// zombies as members, so a per-thread `/proc` scan distinguishes a live
    /// survivor (fail closed) from a zombie-only group that holds no effect
    /// (confirm). The scan first verifies procfs is in this PID namespace and
    /// not `hidepid`-mounted; otherwise the view is incomplete and the close
    /// fails closed rather than certifying an unproven cessation.
    async fn confirm_group_stopped(&mut self, pgid: Option<i32>) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let Some(pgid) = pgid else {
                // No owned group was established; nothing to verify.
                return Ok(());
            };
            #[cfg(feature = "test-probe")]
            if let Some(answer) = self.inject_group_live_member.take() {
                // An injected answer is the settled liveness state: no polling.
                return match answer {
                    Ok(false) => Ok(()),
                    Ok(true) => Err(std::io::Error::other(LIVE_GROUP_MEMBER_ERROR)),
                    Err(error) => Err(error),
                };
            }
            let deadline = Instant::now() + GROUP_STOP_SETTLE;
            loop {
                // Kernel-scoped proof of an empty group needs no procfs trust.
                if owned_group_is_empty(pgid)? {
                    return Ok(());
                }
                // Still occupied: a null signal also counts unreaped zombies,
                // so inspect per-thread states. An untrusted/incomplete procfs
                // view fails closed instead of confirming an unproven stop.
                match owned_group_has_live_member(pgid) {
                    Ok(false) => return Ok(()),
                    Ok(true) if Instant::now() >= deadline => {
                        return Err(std::io::Error::other(LIVE_GROUP_MEMBER_ERROR));
                    }
                    Ok(true) => tokio::time::sleep(CHILD_EXIT_POLL_INTERVAL).await,
                    Err(error) => return Err(error),
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (self, pgid);
            Ok(())
        }
    }
}

impl BoundedChildTransport {
    async fn wait_for_child(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        if let Some(pid) = self.process_group_id {
            // This relies on Tokio being the sole child reaper: callers must not
            // externally waitpid this child or configure SIGCHLD auto-reaping.
            // Do not poll Child::wait/try_wait before the group signal. Tokio's
            // owned Unix child (both signal and pidfd reapers) only reaps when
            // polled or dropped; its background queue only reaps dropped children.
            // WNOWAIT keeps the exited leader waitable, anchoring its PID/PGID
            // until the signal even when no descendants remain in the group.
            loop {
                match child_exit_is_waitable(pid) {
                    Ok(true) => {
                        self.terminate_process_group()?;
                        break;
                    }
                    Ok(false) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) => {
                        if error.raw_os_error() == Some(libc::ECHILD) {
                            // The ownership anchor is gone: never signal a saved
                            // PGID (including on Drop), since it may be reused.
                            self.process_group_id = None;
                        }
                        return Err(error);
                    }
                }
                // WNOHANG never blocks the executor; yielding even on EINTR
                // also lets the existing shutdown timeout bound this loop.
                tokio::time::sleep(CHILD_EXIT_POLL_INTERVAL).await;
            }
        }
        self.child.wait().await.map(|_| ())
    }

    fn terminate_process_group(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            let Some(process_group_id) = self.process_group_id.take() else {
                return Ok(());
            };
            // SAFETY: the child is spawned into a dedicated process group with
            // this id and has not been reaped. Only descendants still in this
            // group are covered; processes that leave it are outside our scope.
            let result = unsafe { libc::killpg(process_group_id, libc::SIGKILL) };
            #[cfg(feature = "test-probe")]
            if self.inject_group_signal_failure {
                return Err(std::io::Error::other(
                    "injected MCP stdio process-group signal failure",
                ));
            }
            // Test probes override the errno and the group-state answer that
            // feed the classifier below, so the real decision runs unchanged on
            // any host; production reads both from the live system.
            #[cfg(feature = "test-probe")]
            let injected_errno = self.inject_group_signal_errno;
            #[cfg(not(feature = "test-probe"))]
            let injected_errno: Option<i32> = None;
            if result == -1 || injected_errno.is_some() {
                let error = match injected_errno {
                    Some(errno) => std::io::Error::from_raw_os_error(errno),
                    None => std::io::Error::last_os_error(),
                };
                // Only consume the classifier probe when it is actually used;
                // a successful signal leaves it for close confirmation.
                #[cfg(feature = "test-probe")]
                let injected_live_member = self.inject_group_live_member.take();
                #[cfg(not(feature = "test-probe"))]
                let injected_live_member: Option<std::io::Result<bool>> = None;
                // The injected answer drives the real classifier when set;
                // otherwise the platform default decides benign EPERM.
                let benign = is_benign_group_signal_failure(
                    error.raw_os_error().unwrap_or_default(),
                    || match injected_live_member {
                        Some(answer) => answer,
                        None => platform_group_live_answer(process_group_id),
                    },
                );
                if benign {
                    return Ok(());
                }
                return Err(error);
            }
        }
        Ok(())
    }

    async fn terminate_child(&mut self) -> std::io::Result<()> {
        match self.child.start_kill() {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {}
            Err(error) => return Err(error),
        }
        self.child.wait().await.map(|_| ())
    }

    fn kill_container_blocking(&mut self) {
        let Some(session) = self.container_cleanup.take() else {
            return;
        };
        // Best effort across every backend family. Timeouts are short
        // because this also runs on Drop. Failures keep the session
        // identity in the error for operator retry.
        if let Err(error) = container::teardown_sync(&session) {
            tracing::warn!(%error, "container teardown failed");
        }
    }
}

/// Race-free self-placement into the run's cgroup root before payload
/// execution. The child writes its own pid, so a qcg death between fork and
/// exec cannot leave it outside. Async-signal-safe: only raw syscalls on a
/// pre-built path and stack pid buffer.
#[cfg(target_os = "linux")]
fn apply_host_scope(
    command: &mut tokio::process::Command,
    scope: &policy::HostCgroupScope,
) -> std::io::Result<()> {
    policy::host_cgroup_launch::apply_scope(command.as_std_mut(), scope)
}

impl Drop for BoundedChildTransport {
    fn drop(&mut self) {
        // Drop stays best effort: no receipt can be settled here.
        let _ = self.terminate_process_group();
        self.kill_container_blocking();
    }
}

/// Decide whether a failed owned-group signal is benign and may confirm close.
///
/// `live_member` is evaluated lazily and only for XNU's ambiguous EPERM. A
/// vanished group (`ESRCH`) holds no effect; EPERM is benign only for a proven
/// zombie-only group (`Ok(false)`). A live member (`Ok(true)`), unreadable group
/// state (`Err`), and every other errno keep the close unconfirmed.
#[cfg(unix)]
fn is_benign_group_signal_failure(
    errno: i32,
    live_member: impl FnOnce() -> std::io::Result<bool>,
) -> bool {
    match errno {
        libc::ESRCH => true,
        libc::EPERM => matches!(live_member(), Ok(false)),
        _ => false,
    }
}

/// Whether the macOS owned process group still contains a live (non-zombie)
/// member. Used only to disambiguate XNU's ambiguous `killpg` EPERM while the
/// leader is still a waitable anchor. The group leader's PID equals the PGID
/// and is a known-exited zombie, so it is excluded. An error reading group
/// state is an `Err` so callers fail closed.
#[cfg(target_os = "macos")]
fn owned_group_has_live_member(process_group_id: i32) -> std::io::Result<bool> {
    if process_group_id <= 0 {
        return Err(std::io::Error::other("invalid process group id"));
    }
    // libproc's PROC_PGRP_ONLY lists the pids in a process group. `proc_listpids`
    // is used directly because proc_listpgrppids is unreliable for live groups.
    const PROC_PGRP_ONLY: u32 = 2;
    const MAX_GROUP_PIDS: usize = 1 << 16;

    // SAFETY: a null buffer with size zero only queries the required byte count.
    let needed = unsafe {
        libc::proc_listpids(
            PROC_PGRP_ONLY,
            process_group_id as u32,
            std::ptr::null_mut(),
            0,
        )
    };
    if needed <= 0 {
        // libproc collapses a __proc_info failure to 0, and a successful
        // sizing query is always strictly positive. An error must fail closed:
        // callers treat Err as "live" and keep the close unconfirmed.
        return Err(std::io::Error::other("proc_listpids sizing failed"));
    }
    let needed = usize::try_from(needed)
        .map_err(|_| std::io::Error::other("process group size overflow"))?;
    let count = needed / std::mem::size_of::<libc::pid_t>();
    if count == 0 || count > MAX_GROUP_PIDS {
        return Err(std::io::Error::other("unreasonable process group size"));
    }
    let mut pids = vec![0 as libc::pid_t; count];
    let buffer_bytes = libc::c_int::try_from(std::mem::size_of_val(pids.as_slice()))
        .map_err(|_| std::io::Error::other("process group buffer too large"))?;
    // SAFETY: `pids` is a writable buffer of exactly `buffer_bytes` bytes.
    let filled = unsafe {
        libc::proc_listpids(
            PROC_PGRP_ONLY,
            process_group_id as u32,
            pids.as_mut_ptr().cast(),
            buffer_bytes,
        )
    };
    if filled <= 0 {
        // Either a listing failure or the group vanished; both are ambiguous
        // here, so fail closed rather than confirming an unproven cleanup.
        return Err(std::io::Error::other("proc_listpids listing failed"));
    }
    let filled = usize::try_from(filled)
        .map_err(|_| std::io::Error::other("process group count overflow"))?
        .min(pids.len());
    for &pid in pids.iter().take(filled) {
        // The group leader (PGID) is a known-exited zombie; it cannot hold a
        // protected effect. Only other members can be unsignaled live processes.
        if pid <= 0 || pid == process_group_id {
            continue;
        }
        if macos_process_is_live(pid, process_group_id)? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_os = "macos")]
fn macos_process_is_live(pid: libc::pid_t, process_group_id: i32) -> std::io::Result<bool> {
    // SAFETY: zeroed storage is valid for proc_bsdinfo; the query does not
    // retain the pointer after returning.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>();
    let size_i32 = libc::c_int::try_from(size)
        .map_err(|_| std::io::Error::other("process information size exceeds i32"))?;
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            std::ptr::addr_of_mut!(info).cast(),
            size_i32,
        )
    };
    if written < size_i32 {
        // State is unreadable (e.g. a live setuid/setgid member owned by another
        // user). A zero-signal existence probe distinguishes a survivor from a
        // gone process; an unexpected error fails closed.
        // SAFETY: signal 0 only probes the numeric PID and changes no state.
        let probe = unsafe { libc::kill(pid, 0) };
        if probe == 0 {
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(false),
            Some(libc::EPERM) => Ok(true),
            _ => Err(error),
        };
    }
    // Guard against PID reuse (a member that now belongs to a different group)
    // and treat only genuinely running members as live blockers; exited zombies
    // hold no protected effect.
    Ok(info.pbi_pgid == process_group_id as u32 && info.pbi_status != libc::SZOMB)
}

/// Kernel-scoped proof that the owned process group is empty. `kill(-pgid, 0)`
/// consults the kernel's own group table, so ESRCH proves no process (live or
/// zombie) remains. This is only the empty fast path: a null signal also
/// succeeds for unreaped zombies, so an occupied answer says nothing about
/// liveness and must be resolved by `owned_group_has_live_member`.
#[cfg(target_os = "linux")]
fn owned_group_is_empty(process_group_id: i32) -> std::io::Result<bool> {
    if process_group_id <= 0 {
        return Err(std::io::Error::other("invalid process group id"));
    }
    // SAFETY: signal 0 probes the numeric group and changes no process state.
    let result = unsafe { libc::kill(-process_group_id, 0) };
    if result == 0 {
        return Ok(false);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        // ESRCH is the kernel's statement that no process remains in the group.
        Some(libc::ESRCH) => Ok(true),
        // A member exists that we cannot signal; still occupied.
        Some(libc::EPERM) => Ok(false),
        _ => Err(error),
    }
}

/// Whether a member of the owned process group is still live.
///
/// Uses per-thread states, not the process-entry state: a `pthread_exit` leader
/// leaves a `Z` process entry while a sibling thread is still runnable, so a
/// process-level `Z` check would confirm while a member is alive. Genuine
/// SIGKILL zombies have every task in `Z`/`X`/`x` and hold no effect.
///
/// Before trusting an empty scan, this verifies procfs is mounted in our PID
/// namespace (otherwise numeric PGIDs are meaningless) and without `hidepid`
/// (otherwise a foreign-UID survivor is invisible). If either check fails, an
/// error fails closed rather than certifying an unproven cessation.
#[cfg(target_os = "linux")]
fn owned_group_has_live_member(process_group_id: i32) -> std::io::Result<bool> {
    validate_procfs_coverage()?;
    for entry in std::fs::read_dir("/proc")? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if proc_entry_vanished(&error) => continue,
            Err(error) => return Err(error),
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Ok(pid) = name.parse::<i32>() else {
            continue;
        };
        if pid == process_group_id {
            // The leader is the known-exited, reaped child; its old PGID can be
            // reused by an unrelated process that must never keep our close open
            // forever. Its own (empty or Z) task list holds no live effect.
            continue;
        }
        let stat = match std::fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if proc_entry_vanished(&error) => continue,
            Err(error) => return Err(error),
        };
        if process_stat_group(&stat)? != process_group_id {
            continue;
        }
        // Inspect every thread: a Z process entry can hide a live sibling.
        for task in std::fs::read_dir(entry.path().join("task"))? {
            let task = match task {
                Ok(task) => task,
                Err(error) if proc_entry_vanished(&error) => continue,
                Err(error) => return Err(error),
            };
            let stat = match std::fs::read_to_string(task.path().join("stat")) {
                Ok(stat) => stat,
                Err(error) if proc_entry_vanished(&error) => continue,
                Err(error) => return Err(error),
            };
            if !matches!(process_stat_state(&stat)?, "Z" | "X" | "x") {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Verifies that a `/proc` scan can see exactly the processes in our PID
/// namespace without `hidepid` filtering. `/proc/self/stat` reports the reader
/// in the mounted namespace, so its PGID must equal our own `getpgrp()`. A
/// `hidepid` mount can hide a foreign-UID survivor that survived group
/// delivery; a mismatched namespace makes numeric PGIDs from the scan unsafe.
#[cfg(target_os = "linux")]
fn validate_procfs_coverage() -> std::io::Result<()> {
    let self_stat = std::fs::read_to_string("/proc/self/stat")?;
    if process_stat_group(&self_stat)? != unsafe { libc::getpgrp() } {
        return Err(std::io::Error::other(
            "/proc is not mounted in this PID namespace",
        ));
    }
    let mounts = std::fs::read_to_string("/proc/self/mountinfo")?;
    for line in mounts.lines() {
        // mountinfo: ... mount_point ... - fstype source super_options
        let fields: Vec<&str> = line.split_whitespace().collect();
        let Some(sep) = fields.iter().position(|field| *field == "-") else {
            continue;
        };
        if fields.get(4) != Some(&"/proc") || fields.get(sep + 1) != Some(&"proc") {
            continue;
        }
        let options = fields.get(sep + 3).copied().unwrap_or("");
        if options.split(',').any(|option| {
            option
                .strip_prefix("hidepid=")
                .is_some_and(|value| value != "0" && !value.is_empty())
        }) {
            return Err(std::io::Error::other(
                "/proc is mounted with hidepid; group membership is not observable",
            ));
        }
    }
    Ok(())
}

/// Parses the process-group field (fifth field, index 2 after `comm`) from a
/// Linux `/proc/<pid>/stat` or `/proc/<pid>/task/<tid>/stat` record.
#[cfg(target_os = "linux")]
fn process_stat_group(stat: &str) -> std::io::Result<i32> {
    let fields = stat
        .rsplit_once(')')
        .ok_or_else(|| std::io::Error::other("malformed process stat"))?
        .1;
    let mut fields = fields.split_whitespace();
    let _state = fields.next();
    let _ppid = fields.next();
    let group = fields
        .next()
        .ok_or_else(|| std::io::Error::other("incomplete process stat"))?;
    group
        .parse::<i32>()
        .map_err(|_| std::io::Error::other("invalid process group in stat"))
}

/// Parses the single-character process state from a Linux procfs stat record.
#[cfg(target_os = "linux")]
fn process_stat_state(stat: &str) -> std::io::Result<&str> {
    stat.rsplit_once(')')
        .and_then(|(_, fields)| fields.split_whitespace().next())
        .ok_or_else(|| std::io::Error::other("incomplete process stat"))
}

#[cfg(target_os = "linux")]
fn proc_entry_vanished(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

/// Default group-liveness answer for the ambiguous EPERM classifier when no
/// test probe supplies one. macOS libproc can prove a zombie-only group;
/// elsewhere (Linux included) EPERM has no benign reading — a gone group
/// reports ESRCH and zombies accept the signal — so it stays fatal.
#[cfg(target_os = "macos")]
fn platform_group_live_answer(process_group_id: i32) -> std::io::Result<bool> {
    owned_group_has_live_member(process_group_id)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn platform_group_live_answer(_process_group_id: i32) -> std::io::Result<bool> {
    Ok(true)
}

#[cfg(unix)]
fn child_exit_is_waitable(pid: i32) -> std::io::Result<bool> {
    // SAFETY: a zeroed siginfo_t is valid storage. POSIX waitid with WNOHANG
    // reports si_pid == 0 if no exit is available (zeroing also covers older
    // implementations that leave the buffer untouched in that case).
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: info is writable; WNOHANG is nonblocking, WEXITED selects only
    // exited children, and WNOWAIT preserves the child for Tokio to reap later.
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: after successful waitid the child-event si_pid field is valid.
    Ok(unsafe { info.si_pid() } == pid)
}

struct BoundedLineReader<R> {
    inner: R,
    max_line_bytes: usize,
    current_line_bytes: usize,
    failed: bool,
}

impl<R> BoundedLineReader<R> {
    fn new(inner: R, max_line_bytes: usize) -> Self {
        Self {
            inner,
            max_line_bytes,
            current_line_bytes: 0,
            failed: false,
        }
    }

    fn observe(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        for byte in bytes {
            if *byte == b'\n' {
                self.current_line_bytes = 0;
                continue;
            }
            self.current_line_bytes = self.current_line_bytes.saturating_add(1);
            if self.current_line_bytes > self.max_line_bytes {
                self.failed = true;
                return Err(std::io::Error::other(format!(
                    "MCP stdio message exceeded {} bytes",
                    self.max_line_bytes
                )));
            }
        }
        Ok(())
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BoundedLineReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.failed {
            return Poll::Ready(Err(std::io::Error::other(
                "MCP stdio message limit was exceeded",
            )));
        }
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut storage = [0_u8; READ_CHUNK_BYTES];
        let capacity = output.remaining().min(storage.len());
        let mut temporary = ReadBuf::new(&mut storage[..capacity]);
        match Pin::new(&mut self.inner).poll_read(context, &mut temporary) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                let bytes = temporary.filled();
                if let Err(error) = self.observe(bytes) {
                    return Poll::Ready(Err(error));
                }
                output.put_slice(bytes);
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt as _;

    #[cfg(target_os = "linux")]
    #[test]
    fn kernel_group_probe_distinguishes_a_live_group_from_an_emptied_one() {
        // A real leader and same-group worker: the kernel probe must report
        // the group as occupied while they live, and as empty only after the
        // whole group is killed and the leader reaped. This exercises the
        // mechanism that establishes whole-group cessation without trusting
        // procfs visibility (hidepid, PID namespaces, zombie/thread entries).
        use std::os::unix::process::CommandExt as _;
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 30 & wait"])
            .process_group(0)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("child should spawn");
        let pgid = i32::try_from(child.id()).expect("pid fits i32");
        assert!(
            !owned_group_is_empty(pgid).expect("probe should read the live group"),
            "a live owned group must be reported as occupied"
        );
        // SAFETY: this process group was created solely for this test by the
        // child spawned above with process_group(0).
        unsafe { libc::killpg(pgid, libc::SIGKILL) };
        let _ = child.wait();
        let deadline = Instant::now() + GROUP_STOP_SETTLE;
        while Instant::now() < deadline {
            if matches!(owned_group_is_empty(pgid), Ok(true)) {
                return;
            }
            std::thread::sleep(CHILD_EXIT_POLL_INTERVAL);
        }
        panic!("kernel probe never observed the emptied group");
    }

    /// Spawns `/bin/sleep` as the group leader and a second `/bin/sleep` joined
    /// into the same group via a pre-exec `setpgid`; returns (leader, worker).
    #[cfg(target_os = "linux")]
    fn spawn_group_with_worker(worker_seconds: &str) -> (std::process::Child, std::process::Child) {
        use std::os::unix::process::CommandExt as _;
        let leader = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("leader should spawn");
        let pgid = i32::try_from(leader.id()).expect("pid fits i32");
        // Let the leader establish the group before the worker joins it.
        std::thread::sleep(std::time::Duration::from_millis(50));
        // SAFETY: the pre-exec hook calls only async-signal-safe setpgid to
        // join the leader's group before the child execs the shell.
        let worker = unsafe {
            std::process::Command::new("/bin/sleep")
                .arg(worker_seconds)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .pre_exec(move || {
                    if libc::setpgid(0, pgid) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                })
                .spawn()
        }
        .expect("worker should spawn");
        // Ensure the worker actually joined before any liveness observation.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", worker.id())).unwrap();
        assert_eq!(
            process_stat_group(&stat).expect("group should parse"),
            pgid,
            "worker should be in the owned group"
        );
        (leader, worker)
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn live_non_leader_member_is_reported_live() {
        let (mut leader, mut worker) = spawn_group_with_worker("30");
        let pgid = i32::try_from(leader.id()).expect("pid fits i32");
        let worker_pid = i32::try_from(worker.id()).expect("pid fits i32");
        assert_eq!(
            process_stat_state(
                &std::fs::read_to_string(format!("/proc/{worker_pid}/stat")).unwrap()
            )
            .expect("state should parse"),
            "S"
        );
        assert!(
            owned_group_has_live_member(pgid).expect("coverage should be established"),
            "a live same-group worker must keep the close unconfirmed"
        );
        // SAFETY: this group was created solely for this test.
        unsafe { libc::killpg(pgid, libc::SIGKILL) };
        let _ = leader.wait();
        let _ = worker.wait();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn zombie_only_non_leader_member_is_not_reported_live() {
        // Reproduces the non-reaping-supervisor case: a same-group worker that
        // died but stays an unreaped zombie (held as an unreaped direct child).
        // A null signal counts it as an occupied group, but the per-thread scan
        // must see every task dead so a clean close is confirmed, not rejected.
        let (mut leader, mut worker) = spawn_group_with_worker("0.1");
        let pgid = i32::try_from(leader.id()).expect("pid fits i32");
        let worker_pid = i32::try_from(worker.id()).expect("pid fits i32");
        // Do not reap the worker: it becomes a zombie held by this test.
        std::thread::sleep(std::time::Duration::from_millis(300));
        assert_eq!(
            process_stat_state(
                &std::fs::read_to_string(format!("/proc/{worker_pid}/stat")).unwrap()
            )
            .expect("state should parse"),
            "Z",
            "the worker should be an unreaped zombie"
        );
        assert!(
            !owned_group_is_empty(pgid).expect("probe should read the group"),
            "an unreaped zombie keeps the group occupied"
        );
        assert!(
            !owned_group_has_live_member(pgid).expect("coverage should be established"),
            "a zombie-only member must not keep the close unconfirmed"
        );
        // SAFETY: this group was created solely for this test.
        unsafe { libc::killpg(pgid, libc::SIGKILL) };
        let _ = leader.wait();
        let _ = worker.wait();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn procfs_coverage_is_established_in_this_namespace() {
        // The real coverage gate must accept a procfs mounted in our own PID
        // namespace without hidepid, and stat parsing must agree with getpgrp.
        validate_procfs_coverage().expect("ordinary procfs coverage should be established");
        let self_stat = std::fs::read_to_string("/proc/self/stat").expect("self stat readable");
        assert_eq!(
            process_stat_group(&self_stat).expect("group should parse"),
            unsafe { libc::getpgrp() }
        );
    }

    #[test]
    fn close_receipt_is_confirmed_only_by_a_recorded_success() {
        let receipt = CloseReceipt::new();
        assert!(!receipt.confirmed(), "a missing outcome is unconfirmed");
        let clone = receipt.clone();
        receipt.record(&Ok(()));
        assert!(receipt.confirmed());
        assert!(clone.confirmed(), "cloned handles share the close outcome");
        receipt.record(&Err(std::io::Error::other("close failed")));
        assert!(!receipt.confirmed(), "a failed close is unconfirmed");
        assert!(!clone.confirmed());
    }

    #[tokio::test]
    async fn line_reader_rejects_oversized_messages_before_forwarding_the_chunk() {
        let input = b"123456789\n".as_slice();
        let mut reader = BoundedLineReader::new(input, 8);
        let mut output = Vec::new();
        let error = reader
            .read_to_end(&mut output)
            .await
            .expect_err("oversized line must fail");
        assert!(error.to_string().contains("exceeded 8 bytes"));
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn line_reader_resets_the_limit_after_each_message() {
        let input = b"12345678\nabcdefgh\n".as_slice();
        let mut reader = BoundedLineReader::new(input, 8);
        let mut output = Vec::new();
        reader
            .read_to_end(&mut output)
            .await
            .expect("individually bounded lines should pass");
        assert_eq!(output, input);
    }

    #[cfg(unix)]
    #[test]
    fn group_signal_failure_classification_is_state_aware_and_fails_closed() {
        // A vanished group is benign: there is nothing left to signal.
        assert!(is_benign_group_signal_failure(libc::ESRCH, || panic!(
            "ESRCH must not consult group state"
        )));
        // XNU's ambiguous EPERM is benign only for a proven zombie-only group.
        assert!(is_benign_group_signal_failure(libc::EPERM, || Ok(false)));
        // A live unsignaled member keeps the close unconfirmed.
        assert!(!is_benign_group_signal_failure(libc::EPERM, || Ok(true)));
        // Unreadable group state must fail closed, never confirm cleanup.
        assert!(!is_benign_group_signal_failure(libc::EPERM, || {
            Err(std::io::Error::other("unreadable"))
        }));
        // Every other signal failure stays fatal.
        assert!(!is_benign_group_signal_failure(libc::EINVAL, || Ok(false)));
    }

    #[cfg(all(unix, feature = "test-probe"))]
    #[tokio::test]
    async fn live_member_group_signal_eperm_keeps_the_close_unconfirmed() {
        // XNU's EPERM is ambiguous: a live setuid/setgid member that refused
        // SIGKILL must never be mistaken for a benign zombie-only group. The
        // injected errno and answer flow through the real classifier.
        let mut transport = shell_transport("cat >/dev/null; exit 0");
        transport.inject_group_signal_errno = Some(libc::EPERM);
        transport.inject_group_live_member = Some(Ok(true));
        let error = transport
            .close()
            .await
            .expect_err("a live unsignaled member must fail the owned close");
        assert_eq!(error.raw_os_error(), Some(libc::EPERM), "{error}");
        assert!(
            !transport.close_receipt().confirmed(),
            "an unconfirmed close must not settle the launch"
        );
        assert!(
            transport.process_group_id.is_none(),
            "the PGID was consumed before signalling, so Drop cannot re-signal"
        );
    }

    #[cfg(all(unix, feature = "test-probe"))]
    #[tokio::test]
    async fn unreadable_group_state_eperm_keeps_the_close_unconfirmed() {
        // If group membership cannot be read, the EPERM cannot be proven benign.
        let mut transport = shell_transport("cat >/dev/null; exit 0");
        transport.inject_group_signal_errno = Some(libc::EPERM);
        transport.inject_group_live_member = Some(Err(std::io::Error::other("unreadable")));
        let error = transport
            .close()
            .await
            .expect_err("unreadable group state must fail the owned close");
        assert_eq!(error.raw_os_error(), Some(libc::EPERM), "{error}");
        assert!(!transport.close_receipt().confirmed());
    }

    #[cfg(all(unix, feature = "test-probe"))]
    #[tokio::test]
    async fn zombie_only_group_signal_eperm_confirms_the_close() {
        // A zombie-only group holds no protected effect, so its EPERM is benign.
        let mut transport = shell_transport("cat >/dev/null; exit 0");
        transport.inject_group_signal_errno = Some(libc::EPERM);
        transport.inject_group_live_member = Some(Ok(false));
        transport
            .close()
            .await
            .expect("a zombie-only group must confirm the owned close");
        assert!(transport.close_receipt().confirmed());
    }

    #[cfg(all(target_os = "linux", feature = "test-probe"))]
    #[tokio::test]
    async fn successful_group_signal_with_live_member_leaves_close_unconfirmed() {
        // Linux kill(2) can return 0 after signalling only part of a group
        // whose members have different credentials. A survivor must keep the
        // close unconfirmed even though the group signal itself succeeded.
        let mut transport = shell_transport("cat >/dev/null; exit 0");
        transport.inject_group_signal_errno = None;
        transport.inject_group_live_member = Some(Ok(true));
        let error = transport
            .close()
            .await
            .expect_err("a live group member must fail the owned close");
        assert!(error.to_string().contains("live member"), "{error}");
        assert!(!transport.close_receipt().confirmed());
    }

    #[cfg(all(target_os = "linux", feature = "test-probe"))]
    #[tokio::test]
    async fn successful_group_signal_with_no_live_member_confirms_close() {
        let mut transport = shell_transport("cat >/dev/null; exit 0");
        transport.inject_group_signal_errno = None;
        transport.inject_group_live_member = Some(Ok(false));
        transport
            .close()
            .await
            .expect("a stopped group must confirm the owned close");
        assert!(transport.close_receipt().confirmed());
    }

    #[cfg(all(target_os = "linux", feature = "test-probe"))]
    #[tokio::test]
    async fn unreadable_liveness_after_signal_fails_closed() {
        let mut transport = shell_transport("cat >/dev/null; exit 0");
        transport.inject_group_signal_errno = None;
        transport.inject_group_live_member = Some(Err(std::io::Error::other("unreadable")));
        let error = transport
            .close()
            .await
            .expect_err("unreadable group state must fail the owned close");
        assert!(error.to_string().contains("unreadable"), "{error}");
        assert!(!transport.close_receipt().confirmed());
    }

    #[cfg(unix)]
    fn shell_transport(script: &str) -> BoundedChildTransport {
        let mut command = tokio::process::Command::new("/bin/sh");
        command.args(["-c", script]);
        BoundedChildTransport::spawn(command, 1024).expect("shell transport should spawn")
    }

    #[cfg(all(unix, feature = "test-probe"))]
    #[tokio::test]
    async fn injected_group_signal_failure_leaves_the_close_unconfirmed() {
        let _fail_group_signal = crate::mcp::session::close_probe::fail_group_signal();
        let mut transport = shell_transport("cat >/dev/null; exit 0");
        let error = transport
            .close()
            .await
            .expect_err("a failed group signal must fail the owned close");
        assert!(
            error
                .to_string()
                .contains("injected MCP stdio process-group signal failure"),
            "{error}"
        );
        assert!(
            !transport.close_receipt().confirmed(),
            "an unconfirmed close must not settle the launch"
        );
        assert!(
            transport.process_group_id.is_none(),
            "the PGID was consumed before signalling, so Drop cannot re-signal"
        );
    }

    // Unix-only: process-group child reaping has no Windows counterpart, so
    // this assertion is not registered on that platform.
    #[cfg(unix)]
    #[tokio::test]
    async fn close_reaps_a_gracefully_exited_child_without_descendants() {
        let mut transport = shell_transport("cat >/dev/null; exit 0");
        transport.close().await.expect("close should succeed");
        assert!(transport.close_receipt().confirmed());
        assert!(transport.child.id().is_none());
        assert!(transport.child.wait().await.unwrap().success());
        assert!(transport.process_group_id.is_none());
        transport
            .close()
            .await
            .expect("repeated close should succeed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn close_preserves_success_for_a_nonzero_child_exit() {
        let mut transport = shell_transport("cat >/dev/null; exit 7");
        transport
            .close()
            .await
            .expect("nonzero status is not a close error");
        assert_eq!(transport.child.wait().await.unwrap().code(), Some(7));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn tokio_keeps_an_unpolled_child_waitable_until_explicit_reap() {
        let mut transport = shell_transport("exit 0");
        let pid = transport.process_group_id.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !child_exit_is_waitable(pid).unwrap() {
                tokio::time::sleep(CHILD_EXIT_POLL_INTERVAL).await;
            }
        })
        .await
        .expect("leader should exit");
        // Exercise Tokio's runtime and SIGCHLD/orphan handling after this child
        // is already waitable, without ever polling its Child handle.
        let status = tokio::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .status()
            .await
            .unwrap();
        assert!(status.success());
        assert!(child_exit_is_waitable(pid).unwrap());
        assert_eq!(transport.child.id(), Some(pid as u32));
        transport
            .close()
            .await
            .expect("close should still reap the leader");
        assert!(transport.child.wait().await.unwrap().success());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn close_fails_closed_if_the_child_was_already_reaped() {
        let mut transport = shell_transport("exit 0");
        // Deliberately violate the ownership invariant to exercise ECHILD.
        transport.child.wait().await.unwrap();
        let error = transport.close().await.expect_err("the PID anchor is gone");
        assert!(!transport.close_receipt().confirmed());
        assert_eq!(error.raw_os_error(), Some(libc::ECHILD));
        assert!(
            transport.process_group_id.is_none(),
            "Drop must not signal a stale PGID"
        );
    }

    #[cfg(target_os = "linux")]
    fn process_is_gone(error: &std::io::Error) -> bool {
        error.kind() == std::io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
    }

    #[cfg(target_os = "linux")]
    fn process_is_running(pid: i32) -> bool {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => {
                // The command name may contain spaces or parentheses. Zombies
                // can remain visible to kill(pid, 0), but are no longer running.
                let state = stat.rsplit_once(") ").unwrap().1.as_bytes()[0];
                state != b'Z' && state != b'X'
            }
            // The task may exit between opening and reading procfs (ESRCH).
            Err(error) if process_is_gone(&error) => false,
            Err(error) => panic!("cannot inspect worker: {error}"),
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_probe_treats_disappearance_errors_as_stopped() {
        assert!(process_is_gone(&std::io::Error::from_raw_os_error(
            libc::ESRCH
        )));
        assert!(process_is_gone(&std::io::Error::from(
            std::io::ErrorKind::NotFound
        )));
        assert!(!process_is_gone(&std::io::Error::from(
            std::io::ErrorKind::PermissionDenied
        )));
        assert!(!process_is_running(i32::MAX));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn close_terminates_a_ready_same_group_worker_after_graceful_leader_exit() {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::net::UnixStream;
        use std::os::unix::process::CommandExt as _;

        // A separate lifeline guarantees the worker exits even if any assertion
        // fails or the implementation regresses: dropping it delivers EOF. The
        // worker's own stdin/stdout/stderr are null, not MCP pipes. No cleanup
        // signal uses a PID or PGID after the leader has been reaped.
        let (lifeline, worker_lifeline) = UnixStream::pair().unwrap();
        let mut command = tokio::process::Command::new("/bin/sh");
        command.args(["-c", r#"
            sh -c '
                exec 4>&1
                exec </dev/null >/dev/null 2>&1
                printf "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{\"level\":\"info\",\"data\":{\"pid\":%s}}}\n" "$$" >&4
                exec 4>&-
                cat <&3
            ' &
            exec 3<&-
            cat >/dev/null
            exit 0
        "#]);
        // SAFETY: the pre-exec hook only calls async-signal-safe dup2/fcntl.
        // UnixStream descriptors are CLOEXEC, so only the read-side duplicate
        // on fd 3 is inherited, never the test's write-side lifeline.
        unsafe {
            command.as_std_mut().pre_exec(move || {
                if libc::dup2(worker_lifeline.as_raw_fd(), 3) == -1
                    || libc::fcntl(3, libc::F_SETFD, 0) == -1
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut transport = BoundedChildTransport::spawn(command, 1024).unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(2), transport.receive())
            .await
            .expect("worker readiness should be bounded")
            .expect("worker should report readiness after redirecting stdio");
        let ready = serde_json::to_value(ready).unwrap();
        let worker_pid = i32::try_from(ready["params"]["data"]["pid"].as_i64().unwrap()).unwrap();
        let leader_pid = transport.process_group_id.unwrap();
        assert_ne!(worker_pid, leader_pid);
        // SAFETY: getpgid only inspects the synchronized, live worker.
        assert_eq!(unsafe { libc::getpgid(worker_pid) }, leader_pid);
        assert!(process_is_running(worker_pid));

        transport
            .close()
            .await
            .expect("graceful close should succeed");
        assert!(
            transport.child.wait().await.unwrap().success(),
            "leader must exit normally"
        );
        assert!(transport.child.id().is_none(), "leader must be reaped");
        tokio::time::timeout(Duration::from_secs(2), async {
            while process_is_running(worker_pid) {
                tokio::time::sleep(CHILD_EXIT_POLL_INTERVAL).await;
            }
        })
        .await
        .expect("same-group worker must be terminated, not left alive");
        // Keep the fallback EOF alive until after the assertion so it cannot
        // make a leaking close implementation appear to pass.
        drop(lifeline);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod host_cgroup_tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn enabled_scope(
        label: &str,
    ) -> Option<(
        std::path::PathBuf,
        policy::HostCgroupScope,
        policy::HostCgroupConfig,
    )> {
        if std::env::var(policy::HOST_CGROUP_PARENT_ENV)
            .ok()
            .is_none_or(|value| value.trim().is_empty())
        {
            eprintln!("SKIP {label}: no delegation env");
            return None;
        }
        let config = policy::HostCgroupConfig::from_env().expect("invalid delegation config");
        let canonical = config
            .parent()
            .unwrap()
            .canonicalize()
            .expect("delegated parent missing");
        for leaf in ["cgroup.procs", "cgroup.kill", "cgroup.events"] {
            assert!(canonical.join(leaf).is_file(), "missing {leaf}");
        }
        std::fs::OpenOptions::new()
            .write(true)
            .open(canonical.join("cgroup.procs"))
            .expect("delegation not writable");
        let meta = std::env::temp_dir().join(format!(
            "qcg-mcp-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7().as_simple()
        ));
        std::fs::create_dir_all(&meta).unwrap();
        // Bind via raw directory creation (engine owns the JSON binding; this
        // probe only needs the run root for launch placement).
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .unwrap_or_default()
            .trim()
            .to_string();
        let nonce = uuid::Uuid::now_v7().as_simple().to_string();
        let name = policy::run_root_name(&format!("mcp-{label}"), &nonce);
        let run_root = canonical.join(&name);
        std::fs::create_dir(&run_root).expect("cannot create run cgroup");
        std::fs::write(run_root.join("cgroup.kill"), "1").expect("cgroup.kill unusable");
        let scope = policy::HostCgroupScope {
            delegated_parent: canonical,
            run_root,
            run_id: format!("mcp-{label}"),
            nonce,
            boot_id: boot,
        };
        Some((meta, scope, config))
    }

    fn procs_contains(root: &std::path::Path, pid: u32) -> bool {
        let Ok(contents) = std::fs::read_to_string(root.join("cgroup.procs")) else {
            return false;
        };
        contents.lines().any(|line| line.trim() == pid.to_string())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn mcp_stdio_launch_self_places_before_payload() {
        let Some((meta, scope, _)) = enabled_scope("launch") else {
            return;
        };
        let mut command = tokio::process::Command::new("/bin/sleep");
        command.arg("30");
        let mut transport = BoundedChildTransport::spawn_with_scope(command, 1024, Some(&scope))
            .expect("MCP spawn with scope should succeed");
        let pid = transport.child.id().expect("child should have pid");
        let start = Instant::now();
        let mut placed = false;
        while start.elapsed() < Duration::from_secs(5) {
            if procs_contains(&scope.run_root, pid) {
                placed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            placed,
            "MCP child {pid} should self-place into {}",
            scope.run_root.display()
        );
        // Transport close must not orphan the test sleeper; kill via cgroup.
        std::fs::write(scope.run_root.join("cgroup.kill"), "1").unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = transport.child.kill().await;
        let _ = tokio::time::timeout(Duration::from_secs(3), transport.child.wait()).await;
        std::fs::remove_dir(&scope.run_root).unwrap();
        let _ = std::fs::remove_dir_all(&meta);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn mcp_invalid_scope_fails_before_spawn() {
        // Outside-mount scope must be refused before spawn, never migrated.
        let scope = policy::HostCgroupScope {
            delegated_parent: std::path::PathBuf::from("/tmp"),
            run_root: std::path::PathBuf::from("/tmp/qcg-bad"),
            run_id: "bad".into(),
            nonce: "n".into(),
            boot_id: "b".into(),
        };
        let command = tokio::process::Command::new("/bin/sleep");
        assert!(
            BoundedChildTransport::spawn_with_scope(command, 1024, Some(&scope)).is_err(),
            "outside-mount scope must fail before spawn"
        );
    }

    #[tokio::test]
    async fn mcp_disabled_scope_is_noop() {
        // Always runnable: None preserves existing live behavior.
        let command = tokio::process::Command::new("/bin/true");
        // spawn_with_scope with None must behave like spawn (no cgroup).
        let mut transport = BoundedChildTransport::spawn_with_scope(command, 1024, None)
            .expect("None scope must not fail launch");
        let _ = tokio::time::timeout(Duration::from_secs(3), transport.close()).await;
    }
}
