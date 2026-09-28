//! Staging ownership guards for atomic writes (E13).
//!
//! One guard per platform, symmetric in role: the Unix guard reclaims
//! handle-relative, the non-Unix guard reclaims by path. Both hold the
//! blocking task's abort handle when one exists, so dropping the owning
//! scope (timeout, cancellation, shutdown, producer failure) aborts a
//! tracked detached task and reclaims the staging file instead of
//! orphaning it. Best-effort reclaim: `Drop` cannot propagate, and the
//! startup sweep reaps anything left behind.
//!
//! H03: `spawn_blocking` abort never stops an already-started worker
//! (Tokio documents this). Each guard therefore owns a [`CommitFence`]:
//! dropping the guard revokes commit rights, and every blocking worker
//! checks the fence immediately before any commit (replace). A cancelled
//! outer future can no longer have its stale worker overwrite a newer
//! writer's result. The fence is the commit authority; the abort handle
//! is only a best-effort scheduling hint.

use camino::Utf8PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// H03 commit authority shared between an outer future and its blocking
/// worker(s). Cloned into every `spawn_blocking` closure that can commit;
/// the worker checks [`Self::is_revoked`] immediately before creating,
/// writing, or replacing, and refuses with `Interrupted` when revoked.
/// The owning guard revokes on drop unless disarmed (successful commit).
#[derive(Clone, Debug)]
pub(crate) struct CommitFence {
    revoked: Arc<AtomicBool>,
}

impl CommitFence {
    pub(crate) fn new() -> Self {
        Self {
            revoked: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn revoke(&self) {
        self.revoked.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::SeqCst)
    }
}

pub(crate) fn interrupted_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "atomic write cancelled; commit revoked (H03)",
    )
}

/// Owns the Unix staging lifecycle across `spawn_blocking` awaits: the
/// guard holds every stage/commit task handle plus the staging identity.
/// Dropping it aborts owned tasks (no detached continuation can commit
/// after the outer future is gone) and reclaims the staging file
/// handle-relative, so an aborted outer future leaves no `.part-*`
/// behind (E13). Reclaiming is best-effort and documented here: `Drop`
/// cannot propagate, and the startup sweep reaps anything left behind.
#[cfg(unix)]
pub(crate) struct AsyncStagingGuard {
    workspace: Utf8PathBuf,
    resolved: Utf8PathBuf,
    staging: String,
    tasks: Vec<tokio::task::AbortHandle>,
    disarmed: bool,
    fence: CommitFence,
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
            fence: CommitFence::new(),
        }
    }

    /// H03 commit authority: cloned into every stage/commit worker, which
    /// refuses to commit once the outer future is gone.
    pub(crate) fn fence(&self) -> CommitFence {
        self.fence.clone()
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
        // H03: revoke commit rights first so a still-running worker cannot
        // commit after this point; abort is best-effort only.
        self.fence.revoke();
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
    fence: CommitFence,
}

#[cfg(not(unix))]
impl AsyncStreamGuard {
    pub(crate) fn new(staging: Utf8PathBuf) -> Self {
        Self {
            staging: Some(staging),
            task: None,
            fence: CommitFence::new(),
        }
    }

    /// H03 commit authority: cloned into the single blocking worker, which
    /// refuses to commit once the outer future is gone.
    pub(crate) fn fence(&self) -> CommitFence {
        self.fence.clone()
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
        // H03: revoke first (see Unix guard); abort cannot stop a started worker.
        self.fence.revoke();
        if let Some(task) = self.task.take() {
            task.abort();
        }
        if let Some(path) = self.staging.take() {
            let _ = std::fs::remove_file(path.as_std_path());
        }
    }
}
