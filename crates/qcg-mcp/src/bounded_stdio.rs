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

pub(crate) struct BoundedChildTransport {
    child: tokio::process::Child,
    #[cfg(unix)]
    process_group_id: Option<i32>,
    transport: AsyncRwTransport<
        RoleClient,
        BoundedLineReader<tokio::process::ChildStdout>,
        tokio::process::ChildStdin,
    >,
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
        })
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
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "MCP stdio transport close timed out",
                ));
            }
        }
        match tokio::time::timeout(CHILD_SHUTDOWN_TIMEOUT, self.child.wait()).await {
            Ok(result) => {
                result?;
            }
            Err(_) => {
                self.terminate_process_group();
                self.terminate_child().await?;
            }
        }
        #[cfg(unix)]
        {
            self.process_group_id = None;
        }
        Ok(())
    }
}

impl BoundedChildTransport {
    fn terminate_process_group(&mut self) {
        #[cfg(unix)]
        if let Some(process_group_id) = self.process_group_id.take() {
            // SAFETY: the child is spawned into a dedicated process group with this id.
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
}

impl Drop for BoundedChildTransport {
    fn drop(&mut self) {
        self.terminate_process_group();
    }
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
}
