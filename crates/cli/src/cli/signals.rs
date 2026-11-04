use anyhow::Result;
use std::future::Future;
use tokio_util::sync::CancellationToken;

/// Keep polling execution after cancellation: dropping it would skip terminal
/// settlement. Register before polling the run so children never start unguarded.
pub(crate) async fn with_run_signals<T>(
    cancellation: CancellationToken,
    run: impl Future<Output = T>,
) -> Result<T> {
    #[cfg(unix)]
    let signal = {
        use tokio::signal::unix::{SignalKind, signal};
        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        async move {
            tokio::select! {
                _ = interrupt.recv() => {},
                _ = terminate.recv() => {},
            }
            Ok::<_, std::io::Error>(())
        }
    };
    #[cfg(windows)]
    let signal = {
        let mut interrupt = tokio::signal::windows::ctrl_c()?;
        async move {
            interrupt.recv().await;
            Ok::<_, std::io::Error>(())
        }
    };
    #[cfg(not(any(unix, windows)))]
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(run);
    tokio::select! {
        result = &mut run => Ok(result),
        received = signal => {
            cancellation.cancel();
            let result = run.await;
            received?;
            Ok(result)
        }
    }
}
