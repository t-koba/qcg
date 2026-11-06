//! No open waiter survives the registry critical section. Thus an idle gate
//! can be closed/unlinked under that registry without splitting its inode.
use super::*;
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
        if let Err(error) = reclaim(&self.runs_dir, &self.path) {
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
pub(super) fn directory(runs: &Utf8PathBuf) -> Utf8PathBuf {
    idempotency_dir(runs).join(".execution")
}
pub(super) fn reclaim(runs: &Utf8PathBuf, path: &camino::Utf8Path) -> Result<(), String> {
    let Some(_registry) = registry(runs)? else {
        return Ok(());
    };
    let file = match open_no_follow(path, true, true, false, false) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    if let Some(file) = try_lock(file)? {
        // Close before unlink (also on Windows). Registry excludes new opens;
        // a live owner would have made try_lock fail. Pollers retain no file.
        drop(file);
        std::fs::remove_file(path).map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
