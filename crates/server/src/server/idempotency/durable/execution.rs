//! No open waiter survives the registry critical section. Thus an idle gate
//! can be closed/unlinked under that registry without splitting its inode.
use super::*;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
static RETRY_WORKERS: AtomicUsize = AtomicUsize::new(0);

/// Test-only join point for detached final-drop reclaim workers. A test that
/// removes a temporary runs directory must first let any deferred worker
/// finish; production callers never wait here.
#[cfg(test)]
pub(super) fn wait_for_retry_workers() {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while RETRY_WORKERS.load(Ordering::SeqCst) != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "a deferred gate reclaim worker did not finish"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
pub(crate) struct ExecutionGate {
    file: Option<Lock>,
    pub(super) runs_dir: Utf8PathBuf,
    key: String,
    path: Utf8PathBuf,
}
impl ExecutionGate {
    pub(crate) fn try_acquire(runs: &Utf8PathBuf, key: &str) -> Result<Option<Self>, String> {
        let Some(_registry) = registry(runs)? else {
            return Ok(None);
        };
        let path = directory(runs).join(format!("{}.lock", hex::encode(Sha256::digest(key))));
        let file =
            try_lock(open_no_follow(&path, true, true, true, false).map_err(|e| e.to_string())?)?;
        Ok(file.map(|file| Self {
            file: Some(file),
            runs_dir: runs.clone(),
            key: key.into(),
            path,
        }))
    }
    pub(super) fn validate(&self, key: &str) -> Result<(), String> {
        if self.key != key {
            return Err("wrong idempotency execution gate".into());
        }
        Ok(())
    }
}
impl Drop for ExecutionGate {
    fn drop(&mut self) {
        drop(self.file.take()); // Unlock and close before registry reclamation.
        if let Err(error) = reclaim_after_drop(&self.runs_dir, &self.path) {
            tracing::warn!(%error, "idempotency execution gate cleanup deferred");
        }
    }
}
// Explicit unlock prevents pre-exec inherited descriptions extending a critical section.
struct Lock(File);
impl Drop for Lock {
    fn drop(&mut self) {
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "idempotency execution lock unlock failed");
        }
    }
}
fn try_lock(file: File) -> Result<Option<Lock>, String> {
    match file.try_lock() {
        Ok(()) => Ok(Some(Lock(file))),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(e)) => Err(e.to_string()),
    }
}
fn registry(runs: &Utf8PathBuf) -> Result<Option<Lock>, String> {
    std::fs::create_dir_all(directory(runs)).map_err(|e| e.to_string())?;
    let path = idempotency_dir(runs).join(".execution-registry.lock");
    try_lock(open_no_follow(&path, true, true, true, false).map_err(|e| e.to_string())?)
}

/// Test-only observation of one key's lock, independent of the shared registry.
/// `try_acquire` can return `None` on transient registry contention before it
/// opens the key file, so it cannot distinguish "this key is held" from "the
/// registry is briefly busy". Opening the key's lock directly makes the result
/// definitive: `true` means another owner really holds this key.
#[cfg(test)]
pub(crate) fn key_lock_is_held(runs: &Utf8PathBuf, key: &str) -> Result<bool, String> {
    let path = directory(runs).join(format!("{}.lock", hex::encode(Sha256::digest(key))));
    let file = open_no_follow(&path, true, true, true, false).map_err(|e| e.to_string())?;
    Ok(try_lock(file).map_err(|e| e.to_string())?.is_none())
}
pub(super) fn directory(runs: &Utf8PathBuf) -> Utf8PathBuf {
    idempotency_dir(runs).join(".execution")
}
/// Reclamation outcome: only registry contention is worth retrying at the
/// final drop. A live owner must keep its path until that owner's own drop.
#[derive(PartialEq, Eq)]
enum ReclaimOutcome {
    Removed,
    Absent,
    Held,
    RegistryBusy,
}

/// The last owner may drop while another gate briefly holds the registry.
/// Try once inline so an uncontended close stays prompt; hand a busy registry
/// to a non-async worker rather than sleeping on an executor thread.
fn reclaim_after_drop(runs: &Utf8PathBuf, path: &camino::Utf8Path) -> Result<(), String> {
    if reclaim_outcome(runs, path)? != ReclaimOutcome::RegistryBusy {
        return Ok(());
    }
    let runs = runs.clone();
    let path = path.to_owned();
    #[cfg(test)]
    RETRY_WORKERS.fetch_add(1, Ordering::SeqCst);
    let spawned = std::thread::Builder::new()
        .name("qcg-gate-reclaim".into())
        .spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
            while std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(1));
                match reclaim_outcome(&runs, &path) {
                    Ok(ReclaimOutcome::RegistryBusy) => continue,
                    Ok(_) | Err(_) => break,
                }
            }
            #[cfg(test)]
            RETRY_WORKERS.fetch_sub(1, Ordering::SeqCst);
        });
    if spawned.is_err() {
        #[cfg(test)]
        RETRY_WORKERS.fetch_sub(1, Ordering::SeqCst);
        // The resident sweep still reclaims deferred leftovers; do not block
        // the owner's thread just because a retry worker could not start.
        tracing::warn!("idempotency execution gate retry worker unavailable");
    }
    Ok(())
}

pub(super) fn reclaim(runs: &Utf8PathBuf, path: &camino::Utf8Path) -> Result<(), String> {
    reclaim_outcome(runs, path).map(|_| ())
}

fn reclaim_outcome(runs: &Utf8PathBuf, path: &camino::Utf8Path) -> Result<ReclaimOutcome, String> {
    let Some(_registry) = registry(runs)? else {
        return Ok(ReclaimOutcome::RegistryBusy);
    };
    let file = match open_no_follow(path, true, true, false, false) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(ReclaimOutcome::Absent),
        Err(e) => return Err(e.to_string()),
    };
    if let Some(file) = try_lock(file)? {
        // Close before unlink (also on Windows). Registry excludes new opens;
        // a live owner would have made try_lock fail. Pollers retain no file.
        drop(file);
        std::fs::remove_file(path).map_err(|e| e.to_string())?;
        return Ok(ReclaimOutcome::Removed);
    }
    Ok(ReclaimOutcome::Held)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn final_drop_reclaims_when_registry_is_briefly_contended() {
        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("gate-registry-contention-{}", uuid::Uuid::now_v7()));
        let gate = ExecutionGate::try_acquire(&runs, "key").unwrap().unwrap();
        let path = gate.path.clone();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let holder = {
            let runs = runs.clone();
            std::thread::spawn(move || {
                let registry = registry(&runs).unwrap().unwrap();
                acquired_tx.send(()).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(20));
                drop(registry);
            })
        };
        acquired_rx.recv().unwrap();
        drop(gate);
        holder.join().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while path.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "the final drop must reclaim its lock file"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        std::fs::remove_dir_all(runs).unwrap();
    }

    #[tokio::test]
    async fn final_drop_retry_does_not_stall_an_async_worker() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("gate-retry-async-{}", uuid::Uuid::now_v7()));
        let gate = ExecutionGate::try_acquire(&runs, "key").unwrap().unwrap();
        let path = gate.path.clone();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder_runs = runs.clone();
        let holder = std::thread::spawn(move || {
            let registry = registry(&holder_runs).unwrap().unwrap();
            acquired_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(registry);
        });
        acquired_rx.recv().unwrap();

        let ticks = Arc::new(AtomicUsize::new(0));
        let ticker = {
            let ticks = Arc::clone(&ticks);
            tokio::spawn(async move {
                for _ in 0..100 {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    ticks.fetch_add(1, Ordering::SeqCst);
                }
            })
        };
        let drop_started = std::time::Instant::now();
        drop(gate);
        let drop_elapsed = drop_started.elapsed();
        // A busy registry must be handed off, not retried on this worker.
        // The old inline retry held the worker for about a second; keep this
        // bound well below that budget but above a normal spawn.
        assert!(
            drop_elapsed < std::time::Duration::from_millis(200),
            "final reclamation must return promptly under contention: {drop_elapsed:?}"
        );
        // The registry is still held: read progress before releasing it, so a
        // regression to inline sleeping on this worker cannot pass by awaiting
        // a ticker that eventually finishes.
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(
            ticks.load(Ordering::SeqCst) > 0,
            "final reclamation must not occupy the async worker while retrying"
        );
        release_tx.send(()).unwrap();
        ticker.await.unwrap();
        holder.join().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while path.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "reclamation must complete after the registry releases"
            );
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        std::fs::remove_dir_all(runs).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn execution_gate_unlocks_on_final_arc_even_with_inherited_descriptor() {
        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("gate-final-drop-{}", uuid::Uuid::now_v7()));
        let gate = ExecutionGate::try_acquire(&runs, "key").unwrap().unwrap();
        // dup shares the open file description, just as a pre-exec fork
        // does, without forking a multithreaded test process.
        let inherited = gate.file.as_ref().unwrap().0.try_clone().unwrap();
        let owner = std::sync::Arc::new(gate);
        let blocking_writer = owner.clone();
        drop(owner);
        assert!(ExecutionGate::try_acquire(&runs, "key").unwrap().is_none());
        drop(blocking_writer);
        let successor = ExecutionGate::try_acquire(&runs, "key").unwrap().unwrap();
        drop(inherited);
        assert!(ExecutionGate::try_acquire(&runs, "key").unwrap().is_none());
        drop(successor);
        std::fs::remove_dir_all(runs).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn gate_paths_reject_symlinks_without_touching_targets() {
        use std::os::unix::fs::symlink;
        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("gate-no-follow-{}", uuid::Uuid::now_v7()));
        let gate = ExecutionGate::try_acquire(&runs, "key").unwrap().unwrap();
        let path = gate.path.clone();
        drop(gate);
        let target = runs.join("target");
        std::fs::write(&target, b"untouched").unwrap();
        symlink(&target, &path).unwrap();
        assert!(ExecutionGate::try_acquire(&runs, "key").is_err());
        assert!(reclaim(&runs, &path).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"untouched");
        std::fs::remove_file(&path).unwrap();
        let registry = idempotency_dir(&runs).join(".execution-registry.lock");
        std::fs::remove_file(&registry).unwrap();
        symlink(&target, &registry).unwrap();
        assert!(ExecutionGate::try_acquire(&runs, "key").is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"untouched");
        std::fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn open_and_reclamation_share_registry_without_retaining_waiters() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(open_and_reclaim());
    }
    async fn open_and_reclaim() {
        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("gate-open-race-{}", uuid::Uuid::now_v7()));
        let registry = registry(&runs).unwrap().unwrap();
        let path = directory(&runs).join(format!("{}.lock", hex::encode(Sha256::digest("key"))));
        // Pause an opener at the vulnerable open-before-try_lock boundary.
        let opened = open_no_follow(&path, true, true, true, false).unwrap();
        reclaim(&runs, &path).unwrap();
        assert!(path.exists(), "cleanup cannot race this open");
        assert!(ExecutionGate::try_acquire(&runs, "key").unwrap().is_none());
        {
            let wait = super::super::super::acquire_execution_gate(
                runs.clone(),
                "key".into(),
                "digest".into(),
                super::IdempotencyTiming::DEFAULT,
            );
            tokio::pin!(wait);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(150), &mut wait)
                    .await
                    .is_err()
            );
        }
        // The sole blocking worker is available even though the registry is
        // still held: cancellation cannot leave a lock() queued behind it.
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio::task::spawn_blocking(|| ()),
        )
        .await
        .unwrap()
        .unwrap();
        opened.try_lock().unwrap();
        drop(registry);
        reclaim(&runs, &path).unwrap();
        assert!(ExecutionGate::try_acquire(&runs, "key").unwrap().is_none());
        // The opener closes before reclamation releases the registry. No handle
        // can later acquire this inode; every retry must open under the registry.
        drop(opened);
        reclaim(&runs, &path).unwrap();
        assert!(!path.exists());
        let gate = ExecutionGate::try_acquire(&runs, "key").unwrap().unwrap();
        assert!(ExecutionGate::try_acquire(&runs, "key").unwrap().is_none());
        drop(gate);
        assert!(!path.exists());
        std::fs::remove_dir_all(runs).unwrap();
    }
}
