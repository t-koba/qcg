//! Staging ownership guards for atomic writes (E13).
//!
//! One guard per platform, symmetric in role: the Unix guard reclaims
//! handle-relative, the non-Unix guard reclaims by path. Both hold the
//! blocking task's abort handle when one exists, so dropping the owning
//! scope (timeout, cancellation, shutdown, producer failure) aborts a
//! tracked detached task and reclaims the staging file instead of
//! orphaning it. Best-effort reclaim: `Drop` cannot propagate, and the
//! startup sweep reaps anything left behind.

use camino::Utf8PathBuf;

/// Owns the Unix staging lifecycle across `spawn_blocking` awaits: the
/// guard holds every stage/commit task handle plus the staging identity.
/// Dropping it aborts owned tasks (no detached continuation can commit
/// after the outer future is gone) and reclaims the staging file
/// handle-relative, so an aborted outer future leaves no `.qcg-part-*`
/// behind (E13). Reclaiming is best-effort and documented here: `Drop`
/// cannot propagate, and the startup sweep reaps anything left behind.
#[cfg(unix)]
pub(crate) struct AsyncStagingGuard {
    workspace: Utf8PathBuf,
    resolved: Utf8PathBuf,
    staging: String,
    tasks: Vec<tokio::task::AbortHandle>,
    disarmed: bool,
}

#[cfg(unix)]
impl AsyncStagingGuard {
    pub(crate) fn new(workspace: Utf8PathBuf, resolved: Utf8PathBuf, staging: String) -> Self {
        Self {
            workspace,
            resolved,
            staging,
            tasks: Vec::new(),
            disarmed: false,
        }
    }

    /// Takes ownership of a stage/commit task: aborting a finished task is
    /// a no-op, so tracking is safe on both success and abort paths.
    pub(crate) fn track<T>(&mut self, handle: &tokio::task::JoinHandle<T>) {
        self.tasks.push(handle.abort_handle());
    }

    pub(crate) fn staging(&self) -> &str {
        &self.staging
    }

    pub(crate) fn disarm(&mut self) {
        self.disarmed = true;
    }
}

#[cfg(unix)]
impl Drop for AsyncStagingGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        for task in &self.tasks {
            task.abort();
        }
        super::handle::remove_named_staging(&self.workspace, &self.resolved, &self.staging);
    }
}

/// Unified non-Unix staging ownership (E13): the single guard for every
/// non-Unix atomic write, symmetric with the Unix `AsyncStagingGuard` above.
/// Holds the staging path and optionally the blocking task's abort handle,
/// so dropping the owning scope (timeout, cancellation, shutdown, producer
/// failure) aborts a tracked detached task and reclaims the staging file
/// instead of orphaning it. Best-effort reclaim: `Drop` cannot propagate,
/// and the startup sweep reaps anything left behind.
#[cfg(not(unix))]
pub(crate) struct AsyncStreamGuard {
    staging: Option<Utf8PathBuf>,
    task: Option<tokio::task::AbortHandle>,
}

#[cfg(not(unix))]
impl AsyncStreamGuard {
    pub(crate) fn new(staging: Utf8PathBuf) -> Self {
        Self {
            staging: Some(staging),
            task: None,
        }
    }

    pub(crate) fn track<T>(&mut self, handle: &tokio::task::JoinHandle<T>) {
        self.task = Some(handle.abort_handle());
    }

    pub(crate) fn disarm(&mut self) {
        self.staging = None;
        self.task = None;
    }
}

#[cfg(not(unix))]
impl Drop for AsyncStreamGuard {
    fn drop(&mut self) {
        if self.staging.is_none() {
            return;
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
        if let Some(path) = self.staging.take() {
            let _ = std::fs::remove_file(path.as_std_path());
        }
    }
}
