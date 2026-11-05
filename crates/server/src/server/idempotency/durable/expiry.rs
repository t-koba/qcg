//! Idle, shared-lock expiry ownership. The on-disk radix index orders creation
//! seconds, not deadlines: each peer applies its frozen TTL, even after restart.
use super::*;
use std::io::ErrorKind::{DirectoryNotEmpty, NotFound};
use std::sync::{Arc, Mutex};
use std::{collections::VecDeque, fs, time::Duration};
use tokio_util::sync::CancellationToken;
#[derive(Debug)]
pub(crate) struct IdempotencySweep {
    interval: Duration,
    max_entries: usize,
    temps: Mutex<Option<fs::ReadDir>>,
    migration: Mutex<Option<VecDeque<Utf8PathBuf>>>,
    due: Mutex<DueCursor>,
}
impl Default for IdempotencySweep {
    fn default() -> Self {
        Self::new(
            policy::IDEMPOTENCY_SWEEP_INTERVAL,
            policy::IDEMPOTENCY_SWEEP_MAX_ENTRIES,
        )
    }
}
impl IdempotencySweep {
    pub(crate) fn new(interval: Duration, max_entries: usize) -> Self {
        Self {
            interval,
            max_entries,
            temps: Default::default(),
            migration: Default::default(),
            due: Default::default(),
        }
    }
    pub(crate) async fn run(
        self: Arc<Self>,
        runs: Utf8PathBuf,
        ttl: Duration,
        shutdown: CancellationToken,
    ) {
        while !shutdown.is_cancelled() {
            let sweep = Arc::clone(&self);
            let directory = runs.clone();
            let stop = shutdown.clone();
            match tokio::task::spawn_blocking(move || {
                sweep.pass_at(&directory, ttl, now_unix, &stop)
            })
            .await
            {
                Ok(Ok(_)) => {}
                result => tracing::warn!(?result, "idempotency maintenance incomplete; will retry"),
            }
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
    }
    // Independent bounded migration, retirement, traversal and temp work (see server guide).
    fn pass_at(
        &self,
        runs: &Utf8PathBuf,
        ttl: Duration,
        unix: impl FnOnce() -> Result<u64, String>,
        stop: &CancellationToken,
    ) -> Result<usize, String> {
        let _lock = acquire_maintenance_lock(runs, stop)?;
        check_stop(stop)?;
        let now = unix()?;
        let root = idempotency_dir(runs).join(".expiry");
        let mut consumed = 0;
        let mut pending = self.migration.lock().map_err(|_| "migration poisoned")?;
        let mut failure = migrate(runs, stop, &mut pending, self.max_entries).err();
        let mut cursor = self.due.lock().map_err(|_| "expiry cursor poisoned")?;
        let result = (|| {
            ensure_dir(&root)?;
            let mut probes = self.max_entries.saturating_mul(16 * 16);
            if now < ttl.as_secs().max(1) {
                return Ok(());
            }
            let cutoff = format!("{:016x}", now - ttl.as_secs().max(1));
            for _ in 0..self.max_entries.saturating_mul(2) {
                if consumed >= self.max_entries {
                    break;
                }
                let Some(path) = due_work(&root, &cutoff, &mut probes, stop, &mut cursor)? else {
                    break;
                };
                if let Err(error) = reap(runs, &path, now, ttl) {
                    failure.get_or_insert(error);
                } else {
                    consumed += 1;
                }
            }
            Ok(())
        })();
        let temp_result = self.clean_temps(runs, now, stop);
        check_stop(stop)?;
        result
            .and(temp_result)
            .and(failure.map_or(Ok(()), Err))
            .map(|()| consumed)
    }
    fn clean_temps(
        &self,
        runs: &Utf8PathBuf,
        now: u64,
        stop: &CancellationToken,
    ) -> Result<(), String> {
        let dir = temp_dir(runs)?;
        let mut cursor = self.temps.lock().map_err(|_| "temp cursor poisoned")?;
        if cursor.is_none() {
            *cursor = Some(fs::read_dir(dir).map_err(|e| e.to_string())?);
        }
        for _ in 0..self.max_entries {
            check_stop(stop)?;
            let Some(entry) = cursor.as_mut().unwrap().next() else {
                *cursor = None;
                break;
            };
            clean_temp(entry.map_err(|e| e.to_string())?, now)?;
        }
        Ok(())
    }
}
fn check_stop(stop: &CancellationToken) -> Result<(), String> {
    if stop.is_cancelled() {
        Err("cancelled".into())
    } else {
        Ok(())
    }
}
fn ensure_dir(path: &Utf8PathBuf) -> Result<(), String> {
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.to_string()),
    }
    let metadata = fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !metadata.is_dir() {
        return Err(format!("not an idempotency directory: {path}"));
    }
    // Sync even on retry after an earlier failed parent sync.
    sync_dir(&path.parent().unwrap().to_path_buf()).map_err(|e| e.to_string())
}
pub(super) fn temp_dir(runs: &Utf8PathBuf) -> Result<Utf8PathBuf, String> {
    let path = idempotency_dir(runs).join(".temps");
    ensure_dir(&path)?;
    Ok(path)
}
// Index before Ready; UUID prevents generation reuse. Preserve exact legacy bytes.
pub(super) fn register(runs: &Utf8PathBuf, bytes: &[u8]) -> Result<(), String> {
    let record: DurableIdempotencyRecord =
        serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let mut dir = idempotency_dir(runs).join(".expiry");
    ensure_dir(&dir)?;
    for digit in format!("{:016x}", record.created_at_unix).chars() {
        dir.push(digit.to_string());
        ensure_dir(&dir)?;
    }
    let path = dir.join(hex::encode(Sha256::digest(bytes)));
    publish(runs, &path, bytes)
}
fn publish(runs: &Utf8PathBuf, path: &Utf8PathBuf, bytes: &[u8]) -> Result<(), String> {
    let tmp = temp_dir(runs)?.join(format!(
        "{}.tmp-{}",
        hex::encode(Sha256::digest(bytes)),
        uuid::Uuid::now_v7()
    ));
    let mut file = open_no_follow(&tmp, false, true, false, true).map_err(|e| e.to_string())?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| e.to_string())?;
    super::rename_verified(&tmp, path).map_err(|e| e.to_string())?;
    sync_dir(&path.parent().unwrap().to_path_buf()).map_err(|e| e.to_string())
}
fn required_bytes(path: &camino::Utf8Path) -> Result<Vec<u8>, String> {
    match read_bounded_idempotency_file(&path.to_path_buf()) {
        BoundedRead::Present(bytes) => Ok(bytes),
        BoundedRead::Absent => Err(format!("missing expiry record: {path}")),
        BoundedRead::Unreadable(e) => Err(e),
    }
}
// Discover once per owner; batch registrations and round-robin failed paths.
// Retain incomplete state until a peer or this owner durably completes migration.
fn migrate(
    runs: &Utf8PathBuf,
    stop: &CancellationToken,
    pending: &mut Option<VecDeque<Utf8PathBuf>>,
    budget: usize,
) -> Result<(), String> {
    let marker = idempotency_dir(runs).join(".expiry-complete");
    match read_bounded_idempotency_file(&marker) {
        BoundedRead::Present(bytes) if bytes == b"1" => {
            *pending = None;
            return Ok(());
        }
        BoundedRead::Present(_) => return Err("invalid expiry migration marker".into()),
        BoundedRead::Unreadable(e) => return Err(e),
        BoundedRead::Absent => {}
    }
    let temps = temp_dir(runs)?;
    if pending.is_none() {
        let mut paths = VecDeque::new();
        for entry in fs::read_dir(idempotency_dir(runs)).map_err(|e| e.to_string())? {
            check_stop(stop)?;
            if let Ok(path) = Utf8PathBuf::from_path_buf(entry.map_err(|e| e.to_string())?.path()) {
                let name = path.file_name().unwrap();
                if name.strip_suffix(".json").is_some_and(canonical_key_hash)
                    || generated_temp(name)
                {
                    paths.push_back(path);
                }
            }
        }
        *pending = Some(paths);
    }
    let paths = pending.as_mut().unwrap();
    let mut failure = None;
    for _ in 0..budget.min(paths.len()) {
        check_stop(stop)?;
        let path = paths.pop_front().unwrap();
        if let Err(error) = discover(runs, &temps, &path) {
            paths.push_back(path);
            failure.get_or_insert(error);
        }
    }
    if !paths.is_empty() {
        return Err(failure.unwrap_or_else(|| "expiry migration incomplete".into()));
    }
    check_stop(stop)?;
    publish(runs, &marker, b"1")?;
    *pending = None;
    Ok(())
}
fn discover(runs: &Utf8PathBuf, temps: &Utf8PathBuf, path: &Utf8PathBuf) -> Result<(), String> {
    let name = path.file_name().unwrap();
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    if name.strip_suffix(".json").is_some_and(canonical_key_hash) {
        let bytes = required_bytes(path)?;
        let record: DurableIdempotencyRecord =
            serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if idempotency_path(runs, &record.key) != *path {
            return Err(format!("migration key/hash mismatch: {path}"));
        }
        register(runs, &bytes)?;
    } else if meta.file_type().is_symlink() {
        // Exact generated staging leaves under locked, trusted runs-dir ancestry:
        // unlink never follows the leaf; no target content or identity is trusted.
        #[cfg(windows)]
        if std::os::windows::fs::FileTypeExt::is_symlink_dir(&meta.file_type()) {
            fs::remove_dir(path).map_err(|e| e.to_string())?;
        } else {
            fs::remove_file(path).map_err(|e| e.to_string())?;
        }
        #[cfg(not(windows))]
        fs::remove_file(path).map_err(|e| e.to_string())?;
        sync_dir(&idempotency_dir(runs)).map_err(|e| e.to_string())?;
    } else if meta.is_file() {
        let target = temps.join(name);
        if fs::symlink_metadata(&target).is_ok() {
            return Err(format!("migration temp collision: {target}"));
        }
        super::rename_verified(path, &target).map_err(|e| e.to_string())?;
        sync_dir(temps).map_err(|e| e.to_string())?;
        sync_dir(&idempotency_dir(runs)).map_err(|e| e.to_string())?;
    }
    Ok(())
}
#[derive(Debug, Default)]
struct DueCursor {
    branches: Vec<(Utf8PathBuf, u8)>,
    leaf: Option<fs::ReadDir>,
}
// Sixteen hex levels are intrinsic to u64. Each budgeted traversal step reads
// at most one leaf entry and probes one child or finishes one exhausted branch.
// Both branch position and leaf iterator survive pass limits and damaged work.
// Exhausted paths are pruning obligations too: publication can fail before
// creating an entry. Retain position on removal/sync failure; later cycles
// revisit nonempty paths without a separate tree scan.
fn due_work(
    root: &Utf8PathBuf,
    cutoff: &str,
    probes: &mut usize,
    stop: &CancellationToken,
    cursor: &mut DueCursor,
) -> Result<Option<Utf8PathBuf>, String> {
    let ceiling = cutoff
        .chars()
        .fold(root.clone(), |dir, digit| dir.join(digit.to_string()));
    if cursor.branches.is_empty() {
        cursor.branches.push((root.clone(), 0));
    }
    while *probes > 0 && !cursor.branches.is_empty() {
        check_stop(stop)?;
        *probes -= 1;
        if let Some(entries) = &mut cursor.leaf {
            if let Some(entry) = entries.next() {
                return Utf8PathBuf::from_path_buf(entry.map_err(|e| e.to_string())?.path())
                    .map(Some)
                    .map_err(|_| "non UTF8 expiry path".into());
            }
            cursor.leaf = None;
        }
        let (dir, next) = cursor.branches.last_mut().unwrap();
        if *next == 16 {
            if dir != root {
                match fs::remove_dir(&*dir) {
                    Ok(()) => {}
                    Err(e) if matches!(e.kind(), DirectoryNotEmpty | NotFound) => {}
                    Err(e) => return Err(e.to_string()),
                }
                // Keep the exhausted branch until its removal is durable.
                match sync_dir(&dir.parent().unwrap().to_path_buf()) {
                    Ok(()) => {}
                    Err(e) if e.kind() == NotFound => {} // Peer pruned ancestors; climb to a surviving parent.
                    Err(e) => return Err(e.to_string()),
                }
            }
            cursor.branches.pop();
            continue;
        }
        let child = dir.join((b"0123456789abcdef"[*next as usize] as char).to_string());
        *next += 1;
        if child > ceiling {
            *next = 16;
            continue;
        }
        match fs::symlink_metadata(&child) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.to_string()),
            Ok(meta) if !meta.is_dir() => continue,
            Ok(_) if cursor.branches.len() == 16 => {
                cursor.leaf = Some(fs::read_dir(&child).map_err(|e| e.to_string())?);
                cursor.branches.push((child, 16));
            }
            Ok(_) => cursor.branches.push((child, 0)),
        }
    }
    Ok(None)
}
fn reap(runs: &Utf8PathBuf, work: &Utf8PathBuf, now: u64, ttl: Duration) -> Result<(), String> {
    let scheduled = required_bytes(work)?;
    let record: DurableIdempotencyRecord =
        serde_json::from_slice(&scheduled).map_err(|e| e.to_string())?;
    let expected = format!("{:016x}", record.created_at_unix)
        .chars()
        .fold(idempotency_dir(runs).join(".expiry"), |dir, c| {
            dir.join(c.to_string())
        })
        .join(hex::encode(Sha256::digest(&scheduled)));
    if *work != expected {
        return Err(format!("expiry work identity mismatch: {work}"));
    }
    let ready = idempotency_path(runs, &record.key);
    match read_bounded_idempotency_file(&ready) {
        BoundedRead::Present(bytes) => {
            let current: DurableIdempotencyRecord =
                serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
            if current.key != record.key {
                return Err(format!("expiry Ready key/hash mismatch: {ready}"));
            }
            if bytes == scheduled {
                if !current.expired_at(now, ttl) {
                    return Err("expiry work is not due".into());
                }
                fs::remove_file(&ready).map_err(|e| e.to_string())?;
            }
        }
        BoundedRead::Absent => {}
        BoundedRead::Unreadable(e) => return Err(e),
    }
    // Retry this sync even if Ready is absent after an earlier failed sync.
    sync_dir(&idempotency_dir(runs)).map_err(|e| e.to_string())?;
    fs::remove_file(work).map_err(|e| e.to_string())?;
    if let Err(error) = sync_dir(&work.parent().unwrap().to_path_buf()) {
        // Restore retryable evidence if retirement could not be made durable.
        register(runs, &scheduled)?;
        return Err(error.to_string());
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    fn migrate(runs: &Utf8PathBuf, stop: &CancellationToken) -> Result<(), String> {
        super::migrate(runs, stop, &mut None, usize::MAX)
    }

    fn runs() -> Utf8PathBuf {
        let dir = std::env::temp_dir().join(format!("qcg-expiry-{}", uuid::Uuid::now_v7()));
        let runs = Utf8PathBuf::from_path_buf(dir).unwrap();
        drop(acquire_idempotency_lock(&runs).unwrap());
        runs
    }

    fn ready(runs: &Utf8PathBuf, key: &str, created: u64, indexed: bool) -> Utf8PathBuf {
        let record = DurableIdempotencyRecord {
            key: key.into(),
            digest: "digest".into(),
            run_id: "run".into(),
            created_at_unix: created,
            generation: 1,
            publication_id: indexed.then(|| uuid::Uuid::now_v7().to_string()),
        };
        let bytes = serde_json::to_vec(&record).unwrap();
        if indexed {
            register(runs, &bytes).unwrap();
        }
        let path = idempotency_path(runs, key);
        fs::write(&path, bytes).unwrap();
        path
    }

    fn index_files(dir: &camino::Utf8Path) -> Vec<Utf8PathBuf> {
        fs::read_dir(dir)
            .unwrap()
            .flat_map(|e| {
                let path = Utf8PathBuf::from_path_buf(e.unwrap().path()).unwrap();
                if path.is_dir() {
                    index_files(&path)
                } else {
                    vec![path]
                }
            })
            .collect()
    }

    #[test]
    fn bounded_idle_passes_migrate_once_and_recover_after_restart() {
        let runs = runs();
        let ttl = Duration::from_secs(10);
        let old: Vec<_> = (0..5)
            .map(|i| ready(&runs, &format!("old-{i}"), 90, false))
            .collect();
        let live = ready(&runs, "live", 91, false);
        let sweep = IdempotencySweep::new(Duration::from_secs(60), 2);
        let stop = CancellationToken::new();
        let root = idempotency_dir(&runs).join(".expiry");
        let marker = idempotency_dir(&runs).join(".expiry-complete");
        // Discovery may enumerate all six keys, but even the first pass may
        // register only two. Keep all unvisited paths until their batch runs.
        for remaining in [4, 2, 0] {
            let result = sweep.pass_at(&runs, ttl, || Ok(90), &stop);
            if remaining > 0 {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), 0);
            }
            let state = sweep.migration.lock().unwrap();
            assert_eq!(state.as_ref().map_or(0, VecDeque::len), remaining);
            assert_eq!(state.is_none(), remaining == 0);
            assert_eq!(marker.exists(), remaining == 0);
            assert_eq!(index_files(&root).len(), 6 - remaining);
            assert!(old.iter().all(|p| p.exists()));
        }
        // A completed migration never re-reads unrelated Ready keys.
        let foreign = idempotency_dir(&runs).join(format!("{}.json", "a".repeat(64)));
        fs::write(&foreign, b"not json").unwrap();
        assert_eq!(sweep.pass_at(&runs, ttl, || Ok(100), &stop).unwrap(), 2);
        assert_eq!(old.iter().filter(|p| !p.exists()).count(), 2);
        drop(sweep);
        let restart = IdempotencySweep::new(Duration::from_secs(60), 2);
        assert_eq!(restart.pass_at(&runs, ttl, || Ok(100), &stop).unwrap(), 2);
        assert_eq!(restart.pass_at(&runs, ttl, || Ok(100), &stop).unwrap(), 1);
        assert!(old.iter().all(|p| !p.exists()));
        assert!(live.exists());
        assert!(
            load_durable_ready_result(&runs, "live", Duration::from_secs(u64::MAX))
                .unwrap()
                .is_some()
        );
        assert_eq!(index_files(&root).len(), 1);
        assert_eq!(
            restart
                .pass_at(&runs, ttl, || Ok(101), &CancellationToken::new())
                .unwrap(),
            1
        );
        assert!(index_files(&root).is_empty());
        assert_eq!(fs::read(&foreign).unwrap(), b"not json");
        fs::remove_dir_all(runs).unwrap();
    }

    #[tokio::test]
    async fn resident_progress_needs_no_claim_and_joins_on_cancellation() {
        let runs = runs();
        let first = ready(&runs, "old-1", 0, true);
        let second = ready(&runs, "old-2", 0, true);
        let sweep = Arc::new(IdempotencySweep::new(Duration::from_millis(1), 1));
        let stop = CancellationToken::new();
        let future = sweep.run(runs.clone(), Duration::from_secs(1), stop.clone());
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                tokio::select! {
                    _ = &mut future => panic!("resident stopped before cancellation"),
                    _ = tokio::task::yield_now() => {}
                }
                // A one-entry budget requires a later resident pass as well
                // as startup. Both timestamps are controlled, already due.
                if !first.exists() && !second.exists() {
                    break;
                }
            }
            stop.cancel();
            future.await;
        })
        .await
        .unwrap();
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn stale_work_cannot_delete_replacement_even_with_generation_and_time_reuse() {
        let runs = runs();
        migrate(&runs, &CancellationToken::new()).unwrap();
        let path = ready(&runs, "key", 90, true);
        let old_bytes = fs::read(&path).unwrap();
        let root = idempotency_dir(&runs).join(".expiry");
        let old_work = index_files(&root).pop().unwrap();
        ready(&runs, "key", 90, true);
        let new_bytes = fs::read(&path).unwrap();
        assert_ne!(old_bytes, new_bytes);
        let _lock = acquire_idempotency_lock(&runs).unwrap();
        // Even an expired replacement must not be deleted by old work.
        reap(&runs, &old_work, 100, Duration::from_secs(10)).unwrap();
        assert_eq!(fs::read(&path).unwrap(), new_bytes);
        assert_eq!(index_files(&root).len(), 1);
        drop(_lock);
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn pending_evidence_and_corrupt_or_foreign_ready_survive_errors_and_retry() {
        let runs = runs();
        let sweep = IdempotencySweep::new(Duration::from_secs(60), 4);
        let ttl = Duration::from_secs(10);
        let bad = idempotency_path(&runs, "bad");
        fs::write(&bad, b"broken").unwrap();
        let valid = ready(&runs, "valid", 90, false);
        assert!(
            sweep
                .pass_at(&runs, ttl, || Ok(100), &CancellationToken::new())
                .is_err()
        );
        assert!(!idempotency_dir(&runs).join(".expiry-complete").exists());
        assert!(
            !valid.exists(),
            "damage must not prevent safe indexed progress"
        );
        fs::remove_file(&bad).unwrap();
        let ready_path = ready(&runs, "old", 90, false);
        // Migration failure followed by retry is idempotent.
        migrate(&runs, &CancellationToken::new()).unwrap();
        migrate(&runs, &CancellationToken::new()).unwrap();
        assert_eq!(
            index_files(&idempotency_dir(&runs).join(".expiry")).len(),
            1
        );
        let bytes = fs::read(&ready_path).unwrap();
        let live_created = now_unix().unwrap();
        for created in [1, live_created] {
            let key = format!("pending-{created}");
            let record = DurablePendingRecord {
                key: key.clone(),
                digest: "digest".into(),
                run_id: Some("reserved".into()),
                owner: "owner".into(),
                created_at_unix: created,
                generation: 1,
            };
            let pending = pending_path(&runs, &key);
            let original = serde_json::to_vec(&record).unwrap();
            fs::write(&pending, &original).unwrap();
            assert!(
                sweep
                    .pass_at(
                        &runs,
                        ttl,
                        || Err("clock failed".into()),
                        &CancellationToken::new()
                    )
                    .is_err()
            );
            assert_eq!(fs::read(&pending).unwrap(), original);
            // Check evidence again after actual maintenance, below.
        }
        for bad_bytes in [b"broken".to_vec(), {
            let mut foreign: DurableIdempotencyRecord = serde_json::from_slice(&bytes).unwrap();
            foreign.key = "foreign".into();
            serde_json::to_vec(&foreign).unwrap()
        }] {
            fs::write(&ready_path, &bad_bytes).unwrap();
            assert!(
                sweep
                    .pass_at(&runs, ttl, || Ok(100), &CancellationToken::new())
                    .is_err()
            );
            assert_eq!(fs::read(&ready_path).unwrap(), bad_bytes);
            assert_eq!(
                index_files(&idempotency_dir(&runs).join(".expiry")).len(),
                1
            );
        }
        fs::write(&ready_path, bytes).unwrap();
        assert_eq!(
            sweep
                .pass_at(&runs, ttl, || Ok(100), &CancellationToken::new())
                .unwrap(),
            1
        );
        // Expired pending is still adoptable with the reserved run identity.
        let adopted =
            claim_durable_pending(&runs, "pending-1", "digest", None, ttl, &sweep).unwrap();
        assert!(
            matches!(adopted, ClaimOutcome::Owner { run_id: Some(id), .. } if id == "reserved")
        );
        let live = format!("pending-{live_created}");
        assert!(matches!(
            claim_durable_pending(&runs, &live, "digest", None, ttl, &sweep).unwrap(),
            ClaimOutcome::Peer
        ));
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn peers_serialize_publication_migration_and_expiry() {
        let runs = runs();
        let ttl = Duration::from_secs(100);
        let key = "peer";
        let owner = match claim_durable_pending(
            &runs,
            key,
            "digest",
            Some("run".into()),
            ttl,
            &IdempotencySweep::default(),
        )
        .unwrap()
        {
            ClaimOutcome::Owner {
                owner,
                generation: 1,
                ..
            } => owner,
            _ => panic!("expected owner"),
        };
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let peer_runs = runs.clone();
        let peer_barrier = barrier.clone();
        let peer = std::thread::spawn(move || {
            peer_barrier.wait();
            IdempotencySweep::default()
                .pass_at(&peer_runs, ttl, || Ok(100), &CancellationToken::new())
                .unwrap();
        });
        barrier.wait();
        let committed = store_durable_ready(&runs, key, "digest", "run", &owner, 1, ttl).unwrap();
        peer.join().unwrap();
        let root = idempotency_dir(&runs).join(".expiry");
        assert_eq!(index_files(&root).len(), 1);
        assert_eq!(
            load_durable_ready_result(&runs, key, ttl)
                .unwrap()
                .unwrap()
                .publication_id,
            committed.publication_id
        );
        // Independent peers can expire together without double-removing.
        let now = committed.created_at_unix + 100;
        std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    IdempotencySweep::default()
                        .pass_at(&runs, ttl, || Ok(now), &CancellationToken::new())
                        .unwrap();
                });
            }
        });
        assert!(index_files(&root).is_empty());
        assert!(!idempotency_path(&runs, key).exists());
        fs::remove_dir_all(runs).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn linked_ready_is_never_followed_by_migration_or_expiry() {
        let runs = runs();
        let path = ready(&runs, "linked", 1, true);
        let target = runs.join("outside");
        let bytes = fs::read(&path).unwrap();
        fs::rename(&path, &target).unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        let sweep = IdempotencySweep::default();
        assert!(
            sweep
                .pass_at(
                    &runs,
                    Duration::from_secs(1),
                    || Ok(100),
                    &CancellationToken::new()
                )
                .is_err()
        );
        assert!(!idempotency_dir(&runs).join(".expiry-complete").exists());
        assert_eq!(fs::read(&target).unwrap(), bytes);
        assert!(
            fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            index_files(&idempotency_dir(&runs).join(".expiry")).len(),
            1
        );
        // Repair and retry complete discovery and expiry without lost work.
        fs::remove_file(&path).unwrap();
        fs::rename(target, &path).unwrap();
        assert_eq!(
            sweep
                .pass_at(
                    &runs,
                    Duration::from_secs(1),
                    || Ok(100),
                    &CancellationToken::new()
                )
                .unwrap(),
            1
        );
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn index_failure_never_publishes_ready_or_removes_pending() {
        let runs = runs();
        let ttl = Duration::from_secs(100);
        let sweep = IdempotencySweep::default();
        let owner =
            match claim_durable_pending(&runs, "key", "digest", Some("run".into()), ttl, &sweep)
                .unwrap()
            {
                ClaimOutcome::Owner { owner, .. } => owner,
                _ => panic!("owner"),
            };
        let pending = fs::read(pending_path(&runs, "key")).unwrap();
        fs::write(idempotency_dir(&runs).join(".expiry"), b"blocked").unwrap();
        assert!(store_durable_ready(&runs, "key", "digest", "run", &owner, 1, ttl).is_err());
        assert!(!idempotency_path(&runs, "key").exists());
        assert_eq!(fs::read(pending_path(&runs, "key")).unwrap(), pending);
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn temp_cleanup_has_its_own_bound_and_preserves_unverified_names_and_age() {
        let runs = runs();
        let legacy_name = format!("{}.pending.tmp-{}", "a".repeat(64), uuid::Uuid::now_v7());
        fs::write(idempotency_dir(&runs).join(&legacy_name), b"old").unwrap();
        for i in 0..9 {
            ready(&runs, &format!("old-{i}"), 1, true);
        }
        migrate(&runs, &CancellationToken::new()).unwrap();
        let sweep = IdempotencySweep::new(Duration::from_secs(60), 1);
        // Controlled time relative to filesystem mtime, no real ageing sleep.
        let now = now_unix().unwrap();
        assert_eq!(
            sweep
                .pass_at(
                    &runs,
                    Duration::from_secs(1),
                    || Ok(now),
                    &CancellationToken::new()
                )
                .unwrap(),
            1
        );
        let temps = temp_dir(&runs).unwrap();
        assert!(temps.join(&legacy_name).exists()); // Fresh temp is retained.
        let ignored = temps.join("invalid.tmp-name");
        fs::write(&ignored, b"keep").unwrap();
        #[cfg(unix)]
        let link = {
            let link = temps.join(format!("{}.tmp-{}", "b".repeat(64), uuid::Uuid::now_v7()));
            std::os::unix::fs::symlink(&ignored, &link).unwrap();
            link
        };
        for digit in ["c", "d"] {
            let path = temps.join(format!("{}.tmp-{}", digit.repeat(64), uuid::Uuid::now_v7()));
            fs::write(path, b"stale").unwrap();
        }
        let count = || fs::read_dir(&temps).unwrap().count();
        let before = count();
        sweep
            .pass_at(
                &runs,
                Duration::from_secs(1),
                || Ok(now + 61),
                &CancellationToken::new(),
            )
            .unwrap();
        assert!(
            before - count() <= 1,
            "temp budget is independent and bounded"
        );
        // Cursor may reach EOF before reopening. Each pass inspects at most
        // one temp, independently of the remaining expired Ready backlog.
        for _ in 0..8 {
            sweep
                .pass_at(
                    &runs,
                    Duration::from_secs(1),
                    || Ok(now + 61),
                    &CancellationToken::new(),
                )
                .unwrap();
        }
        assert!(!temps.join(&legacy_name).exists());
        assert_eq!(fs::read(&ignored).unwrap(), b"keep");
        #[cfg(unix)]
        assert!(fs::symlink_metadata(link).is_err());
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn two_corrupt_obligations_cannot_starve_later_healthy_work_with_budget_one() {
        // Cover both leaf-entry and radix-branch cursor persistence. All bad
        // work precedes the healthy timestamp regardless of directory order.
        for stamps in [[70, 70], [70, 71]] {
            let runs = runs();
            let ttl = Duration::from_secs(10);
            let root = idempotency_dir(&runs).join(".expiry");
            let corrupt: Vec<_> = stamps
                .into_iter()
                .enumerate()
                .map(|(i, stamp)| {
                    let dir = format!("{stamp:016x}")
                        .chars()
                        .fold(root.clone(), |p, c| p.join(c.to_string()));
                    fs::create_dir_all(&dir).unwrap();
                    let path = dir.join(if i == 0 { "a" } else { "b" }.repeat(64));
                    fs::write(&path, b"broken").unwrap();
                    path
                })
                .collect();
            let healthy = ready(&runs, "valid-late", 90, true);
            let sweep = IdempotencySweep::new(Duration::from_secs(60), 1);
            let stop = CancellationToken::new();
            assert!(sweep.pass_at(&runs, ttl, || Ok(100), &stop).is_err());
            assert!(healthy.exists(), "both attempts were spent on corruption");
            assert_eq!(sweep.pass_at(&runs, ttl, || Ok(100), &stop).unwrap(), 1);
            assert!(
                !healthy.exists(),
                "the next pass must resume past both failures"
            );
            for path in corrupt {
                assert_eq!(fs::read(&path).unwrap(), b"broken");
            }
            assert_eq!(index_files(&root).len(), 2);
            fs::remove_dir_all(runs).unwrap();
        }
    }

    #[test]
    fn radix_cursor_resumes_after_probe_exhaustion_instead_of_restarting() {
        let runs = runs();
        ready(&runs, "due", 90, true);
        let root = idempotency_dir(&runs).join(".expiry");
        let expected = index_files(&root).pop().unwrap();
        let stop = CancellationToken::new();
        let mut cursor = DueCursor::default();
        // One traversal step cannot reach a sixteen-level timestamp leaf.
        let mut probes = 1;
        assert!(
            due_work(&root, "000000000000005a", &mut probes, &stop, &mut cursor)
                .unwrap()
                .is_none()
        );
        assert_eq!(probes, 0);
        let mut found = None;
        for _ in 0..256 {
            let mut probes = 1;
            found = due_work(&root, "000000000000005a", &mut probes, &stop, &mut cursor).unwrap();
            assert_eq!(probes, 0);
            if found.is_some() {
                break;
            }
        }
        assert_eq!(found, Some(expected));
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn same_owner_retries_failed_legacy_registration_after_index_obstruction_repair() {
        let runs = runs();
        let ttl = Duration::from_secs(10);
        let legacy: Vec<_> = (0..3)
            .map(|i| ready(&runs, &format!("legacy-{i}"), 90, false))
            .collect();
        let root = idempotency_dir(&runs).join(".expiry");
        let marker = idempotency_dir(&runs).join(".expiry-complete");
        fs::write(&root, b"blocked").unwrap();
        let sweep = IdempotencySweep::new(Duration::from_secs(60), 1);
        let stop = CancellationToken::new();
        assert!(sweep.pass_at(&runs, ttl, || Ok(100), &stop).is_err());
        assert!(!marker.exists());
        assert!(legacy.iter().all(|p| p.exists()));
        assert_eq!(sweep.migration.lock().unwrap().as_ref().unwrap().len(), 3);
        // This is deliberately introduced after discovery. Same-owner retry
        // must not enumerate it; an absent marker lets a restart discover it.
        let later = ready(&runs, "later-legacy", 90, false);
        fs::remove_file(&root).unwrap();
        for remaining in (0..3).rev() {
            let result = sweep.pass_at(&runs, ttl, || Ok(100), &stop);
            if remaining > 0 {
                assert!(result.is_err());
            } else {
                result.unwrap();
            }
            assert_eq!(
                sweep
                    .migration
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map_or(0, VecDeque::len),
                remaining
            );
            assert_eq!(marker.exists(), remaining == 0);
            assert_eq!(sweep.migration.lock().unwrap().is_none(), remaining == 0);
        }
        for _ in 0..3 {
            sweep.pass_at(&runs, ttl, || Ok(100), &stop).unwrap();
        }
        assert!(legacy.iter().all(|p| !p.exists()));
        assert!(index_files(&root).is_empty());
        assert!(later.exists());
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn persistent_migration_damage_skips_rescan_but_new_indexed_work_still_proceeds() {
        let runs = runs();
        let ttl = Duration::from_secs(10);
        let bad = idempotency_path(&runs, "bad-legacy");
        fs::write(&bad, b"broken").unwrap();
        let old = ready(&runs, "old-legacy", 90, false);
        let sweep = IdempotencySweep::new(Duration::from_secs(60), 1);
        let stop = CancellationToken::new();
        for _ in 0..2 {
            assert!(sweep.pass_at(&runs, ttl, || Ok(100), &stop).is_err());
        }
        assert!(
            !old.exists(),
            "a permanent failure cannot monopolize a one-item batch"
        );
        assert_eq!(fs::read(&bad).unwrap(), b"broken");
        assert!(!idempotency_dir(&runs).join(".expiry-complete").exists());
        let later = ready(&runs, "later-legacy", 90, false);
        assert!(sweep.pass_at(&runs, ttl, || Ok(100), &stop).is_err());
        assert!(
            later.exists(),
            "retry must not rescan unrelated Ready paths"
        );
        let fresh = ready(&runs, "fresh-indexed", 90, true);
        assert!(sweep.pass_at(&runs, ttl, || Ok(100), &stop).is_err());
        for _ in 0..2 {
            assert!(sweep.pass_at(&runs, ttl, || Ok(100), &stop).is_err());
        }
        assert!(!fresh.exists());
        assert_eq!(fs::read(&bad).unwrap(), b"broken");
        let restart = IdempotencySweep::new(Duration::from_secs(60), 1);
        for _ in 0..2 {
            assert!(restart.pass_at(&runs, ttl, || Ok(100), &stop).is_err());
        }
        assert!(
            !later.exists(),
            "restart discovers while the marker is absent"
        );
        // Repairing malformed evidence also resolves the retained retry path.
        ready(&runs, "bad-legacy", 90, false);
        for _ in 0..3 {
            sweep.pass_at(&runs, ttl, || Ok(100), &stop).unwrap();
        }
        assert!(!bad.exists());
        assert!(idempotency_dir(&runs).join(".expiry-complete").exists());
        fs::remove_dir_all(runs).unwrap();
    }
    #[test]
    fn peer_completion_releases_retained_failed_migration_state() {
        let runs = runs();
        let ttl = Duration::from_secs(10);
        let bad = idempotency_path(&runs, "legacy");
        fs::write(&bad, b"broken").unwrap();
        let a = IdempotencySweep::new(Duration::from_secs(60), 1);
        let b = IdempotencySweep::new(Duration::from_secs(60), 1);
        let stop = CancellationToken::new();
        assert!(a.pass_at(&runs, ttl, || Ok(100), &stop).is_err());
        assert_eq!(a.migration.lock().unwrap().as_ref().unwrap().len(), 1);
        let marker = idempotency_dir(&runs).join(".expiry-complete");
        // An invalid marker is not permission to discard retry evidence.
        fs::write(&marker, b"invalid").unwrap();
        assert!(a.pass_at(&runs, ttl, || Ok(100), &stop).is_err());
        assert!(a.migration.lock().unwrap().is_some());
        fs::remove_file(&marker).unwrap();
        ready(&runs, "legacy", 90, false);
        assert_eq!(b.pass_at(&runs, ttl, || Ok(100), &stop).unwrap(), 1);
        assert!(b.migration.lock().unwrap().is_none());
        assert_eq!(fs::read(&marker).unwrap(), b"1");
        assert_eq!(a.pass_at(&runs, ttl, || Ok(100), &stop).unwrap(), 0);
        assert!(a.migration.lock().unwrap().is_none());
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn failed_registration_empty_radix_branches_are_eventually_pruned() {
        let runs = runs();
        let stop = CancellationToken::new();
        migrate(&runs, &stop).unwrap();
        let root = idempotency_dir(&runs).join(".expiry");
        let temps = temp_dir(&runs).unwrap();
        fs::remove_dir(&temps).unwrap();
        fs::write(&temps, b"publication obstruction").unwrap();
        let record = DurableIdempotencyRecord {
            key: "unpublished".into(),
            digest: "digest".into(),
            run_id: "run".into(),
            created_at_unix: 90,
            generation: 1,
            publication_id: None,
        };
        assert!(register(&runs, &serde_json::to_vec(&record).unwrap()).is_err());
        assert!(index_files(&root).is_empty());
        assert_ne!(fs::read_dir(&root).unwrap().count(), 0);
        fs::remove_file(temps).unwrap();
        let sweep = IdempotencySweep::new(Duration::from_secs(60), 1);
        // The radix cursor's bounded probes also finish empty branches, even
        // though there was never an obligation for the normal reaper to prune.
        for _ in 0..4 {
            assert_eq!(
                sweep
                    .pass_at(&runs, Duration::from_secs(10), || Ok(100), &stop)
                    .unwrap(),
                0
            );
        }
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        fs::remove_dir_all(runs).unwrap();
    }

    #[test]
    fn exhausted_cursor_retains_pruning_failure_and_tolerates_peer_removal() {
        let runs = runs();
        let root = idempotency_dir(&runs).join(".expiry");
        let ttl = Duration::from_secs(10);
        let ready = ready(&runs, "retired", 90, true);
        let stop = CancellationToken::new();
        let mut cursor = DueCursor::default();
        let work = due_work(&root, "000000000000005a", &mut 256, &stop, &mut cursor)
            .unwrap()
            .unwrap();
        reap(&runs, &work, 100, ttl).unwrap();
        assert!(!ready.exists());
        assert!(!work.exists());
        cursor.leaf = None; // Close enumeration before simulating a peer on Windows.
        let leaf = work.parent().unwrap();
        // A transient replacement obstructs pruning after the work is gone.
        fs::remove_dir(leaf).unwrap();
        fs::write(leaf, b"obstruction").unwrap();
        assert!(due_work(&root, "000000000000005a", &mut 1, &stop, &mut cursor).is_err());
        assert_eq!(cursor.branches.len(), 17);
        assert_eq!(cursor.branches.last().unwrap().0, leaf);
        fs::remove_file(leaf).unwrap();
        // A peer can remove all those branches before the retained retry.
        fs::remove_dir_all(root.join("0")).unwrap();
        assert!(
            due_work(&root, "000000000000005a", &mut 256, &stop, &mut cursor)
                .unwrap()
                .is_none()
        );
        assert!(cursor.branches.is_empty());
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        fs::remove_dir_all(runs).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn legacy_generated_symlinks_retire_without_following_targets() {
        let runs = runs();
        let target = runs.join("target");
        let directory = runs.join("directory");
        fs::write(&target, b"untouched").unwrap();
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("child"), b"also untouched").unwrap();
        for (is_dir, target) in [(false, &target), (true, &directory)] {
            let link = idempotency_dir(&runs).join(format!(
                "{}.tmp-{}",
                "a".repeat(64),
                uuid::Uuid::now_v7()
            ));
            let result = if is_dir {
                std::os::windows::fs::symlink_dir(target, &link)
            } else {
                std::os::windows::fs::symlink_file(target, &link)
            };
            match result {
                Ok(()) => {}
                Err(e) if matches!(e.raw_os_error(), Some(1314 | 50)) => {
                    eprintln!("Windows symlink privilege/filesystem unavailable: {e}");
                    fs::remove_dir_all(runs).unwrap();
                    return;
                }
                Err(e) => panic!("create legacy symlink: {e}"),
            }
            // Ordinary record opens still refuse reparse leaves.
            assert!(open_no_follow(&link, true, false, false, false).is_err());
        }
        let sweep = IdempotencySweep::new(Duration::from_secs(60), 1);
        let stop = CancellationToken::new();
        assert!(
            sweep
                .pass_at(&runs, Duration::from_secs(1), || Ok(100), &stop)
                .is_err()
        );
        sweep
            .pass_at(&runs, Duration::from_secs(1), || Ok(100), &stop)
            .unwrap();
        assert!(sweep.migration.lock().unwrap().is_none());
        assert_eq!(
            fs::read(idempotency_dir(&runs).join(".expiry-complete")).unwrap(),
            b"1"
        );
        assert_eq!(fs::read(&target).unwrap(), b"untouched");
        assert_eq!(
            fs::read(directory.join("child")).unwrap(),
            b"also untouched"
        );
        assert!(
            !fs::read_dir(idempotency_dir(&runs))
                .unwrap()
                .any(|e| generated_temp(e.unwrap().file_name().to_str().unwrap()))
        );
        fs::remove_dir_all(runs).unwrap();
    }

    #[tokio::test]
    async fn maintenance_lock_contention_is_cancellable_without_mutation() {
        let runs = runs();
        let ttl = Duration::from_secs(1);
        let target = ready(&runs, "blocked", 0, true);
        let sweep = Arc::new(IdempotencySweep::new(Duration::from_secs(60), 4));
        let stop = CancellationToken::new();
        let _held = acquire_idempotency_lock(&runs).unwrap();
        let runs2 = runs.clone();
        let stop2 = stop.clone();
        let sweep2 = Arc::clone(&sweep);
        let handle =
            tokio::task::spawn_blocking(move || sweep2.pass_at(&runs2, ttl, || Ok(100), &stop2));
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.cancel();
        let outcome = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("join")
            .expect("blocking");
        assert!(outcome.is_err());
        assert!(outcome.unwrap_err().contains("cancelled"));
        assert!(target.exists());
        drop(_held);
        fs::remove_dir_all(runs).unwrap();
    }
}
