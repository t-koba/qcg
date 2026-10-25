use rmcp::RoleClient;
use rmcp::service::{RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::transport::Transport;
use rmcp::transport::async_rw::AsyncRwTransport;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};

const READ_CHUNK_BYTES: usize = 8 * 1024;
const CHILD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(unix)]
const CHILD_EXIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

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
}

impl BoundedChildTransport {
    pub(crate) fn spawn(
        mut command: tokio::process::Command,
        max_message_bytes: usize,
    ) -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            command.as_std_mut().process_group(0);
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
        })
    }

    pub(crate) fn with_container_cleanup(mut self, session: container::Session) -> Self {
        self.container_cleanup = Some(session);
        self
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
        match tokio::time::timeout(CHILD_SHUTDOWN_TIMEOUT, self.transport.close()).await {
            Ok(result) => result?,
            Err(_) => {
                self.terminate_process_group();
                self.terminate_child().await?;
                self.kill_container_blocking();
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
                self.terminate_process_group();
                self.terminate_child().await?;
                self.kill_container_blocking();
            }
        }
        self.kill_container_blocking();
        Ok(())
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
                        self.terminate_process_group();
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

    fn terminate_process_group(&mut self) {
        #[cfg(unix)]
        if let Some(process_group_id) = self.process_group_id.take() {
            // SAFETY: the child is spawned into a dedicated process group with
            // this id and has not been reaped. Only descendants still in this
            // group are covered; processes that leave it are outside our scope.
            unsafe {
                libc::killpg(process_group_id, libc::SIGKILL);
            }
        }
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

impl Drop for BoundedChildTransport {
    fn drop(&mut self) {
        self.terminate_process_group();
        self.kill_container_blocking();
    }
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
    fn shell_transport(script: &str) -> BoundedChildTransport {
        let mut command = tokio::process::Command::new("/bin/sh");
        command.args(["-c", script]);
        BoundedChildTransport::spawn(command, 1024).expect("shell transport should spawn")
    }

    // Unix-only: process-group child reaping has no Windows counterpart, so
    // this assertion is not registered on that platform.
    #[cfg(unix)]
    #[tokio::test]
    async fn close_reaps_a_gracefully_exited_child_without_descendants() {
        let mut transport = shell_transport("cat >/dev/null; exit 0");
        transport.close().await.expect("close should succeed");
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
