//! Durable cross-process idempotency protocol: pending claims and Ready
//! records on the shared filesystem. Claims publish atomically (temp +
//! rename under the claim lock) and every unreadable state fails closed,
//! so two processes with the same key never both start a run (A03).

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::server::error::ApiHttpError;

use super::config::effective_idempotency_ttl;
use super::idempotency_conflict;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableIdempotencyRecord {
    pub(crate) key: String,
    pub(crate) digest: String,
    pub(crate) run_id: String,
    pub(crate) created_at_unix: u64,
}

fn idempotency_dir(runs_dir: &Utf8PathBuf) -> Utf8PathBuf {
    runs_dir.join("idempotency")
}

fn idempotency_path(runs_dir: &Utf8PathBuf, key: &str) -> Utf8PathBuf {
    idempotency_dir(runs_dir).join(format!("{:x}.json", Sha256::digest(key.as_bytes())))
}

fn pending_path(runs_dir: &Utf8PathBuf, key: &str) -> Utf8PathBuf {
    idempotency_dir(runs_dir).join(format!("{:x}.pending.json", Sha256::digest(key.as_bytes())))
}

/// Flush a directory entry so a just-published rename survives a crash.
/// Windows cannot open a directory with `File::open`
/// (`ERROR_ACCESS_DENIED`); NTFS journals the rename itself, so skipping
/// the entry flush there preserves atomicity without spurious failures.
/// Matches the `#[cfg(unix)]` directory sync in `qcg-engine` state
/// persistence.
#[cfg(unix)]
fn sync_dir(dir: &Utf8PathBuf) -> std::io::Result<()> {
    std::fs::File::open(dir.as_std_path())?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Utf8PathBuf) -> std::io::Result<()> {
    Ok(())
}

/// Unfinished reservation lifetime in seconds. Run creation is
/// millisecond-scale, so 60 seconds bounds crash recovery without pinning a
/// dead claim anywhere near the 24h Ready TTL. The peer waiter (30s) is
/// strictly shorter: on waiter deadline with a live claim it reports 503,
/// and on an expired or vanished claim it retries the claim loop, adopting
/// the orphaned run id instead of wedging.
const PENDING_TTL_SECS: u64 = 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurablePendingRecord {
    key: String,
    digest: String,
    /// Run id reserved before execution. A retry that adopts an expired
    /// claim reuses this id, so the crash window between run creation and
    /// Ready commit converges onto one run instead of orphaning one run and
    /// creating another.
    run_id: Option<String>,
    owner: String,
    created_at_unix: u64,
}

/// Cross-process claim before execution so two processes with the same key
/// never both start a run (A03). The first process to publish the pending
/// file under the claim lock owns the key; peers with the same digest wait
/// for the Ready record instead of executing. Different digests conflict
/// without starting a run. An expired claim for the same digest is adopted
/// with its run id.
pub(crate) fn claim_durable_pending(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
    reserved_run_id: Option<String>,
) -> Result<ClaimOutcome, ApiHttpError> {
    std::fs::create_dir_all(idempotency_dir(runs_dir).as_std_path()).map_err(|error| {
        ApiHttpError::internal(format!("failed to persist idempotency record: {error}"))
    })?;
    // Serialize check-then-publish under a cross-process claim lock: temp +
    // rename alone cannot arbitrate two concurrent publishers (the second
    // rename would silently replace the first claim and crown two owners),
    // so the predecessor read and the publish below are one critical
    // section. Waiters only read and never take this lock.
    let lock_path = idempotency_dir(runs_dir).join(".claim.lock");
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path.as_std_path())
        .map_err(|error| {
            ApiHttpError::internal(format!("failed to persist idempotency record: {error}"))
        })?;
    {
        use fs2::FileExt as _;
        lock_file.lock_exclusive().map_err(|error| {
            ApiHttpError::internal(format!("failed to persist idempotency record: {error}"))
        })?;
    }
    let _lock_held = lock_file;
    // Reap an expired predecessor first so a dead claim never wedges
    // retries and an adopted run id survives for the same digest. A live
    // (Valid) predecessor means another owner holds the key: report Peer
    // without writing, since the atomic rename below would otherwise
    // replace its claim and crown two owners.
    let adopted = match read_durable_pending(runs_dir, key) {
        PendingRead::Valid(_) => return Ok(ClaimOutcome::Peer),
        PendingRead::Expired(record) if record.digest == digest => record.run_id.clone(),
        PendingRead::Expired(_) | PendingRead::Absent => None,
        PendingRead::Unusable(detail) => {
            return Err(ApiHttpError::internal(format!(
                "failed to persist idempotency record: {detail}"
            )));
        }
    };
    let path = pending_path(runs_dir, key);
    let run_id = adopted.or(reserved_run_id);
    let record = DurablePendingRecord {
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.clone(),
        owner: uuid::Uuid::now_v7().to_string(),
        created_at_unix: now_unix(),
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| {
        ApiHttpError::internal(format!("failed to persist idempotency record: {error}"))
    })?;
    // Atomic publication via temp + rename: readers observe either absence
    // or a complete claim, never a torn write. A torn read misclassified
    // as corrupt would delete a live owner's claim and split the key into
    // two owners (A03).
    reap_stale_claim_tmps(runs_dir);
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp.as_std_path())
    {
        Ok(mut file) => {
            use std::io::Write as _;
            let write_result = file
                .write_all(&bytes)
                .and_then(|()| file.sync_data())
                .and_then(|()| {
                    std::fs::rename(tmp.as_std_path(), path.as_std_path())?;
                    sync_dir(&idempotency_dir(runs_dir))
                });
            if let Err(error) = write_result {
                let _ = std::fs::remove_file(tmp.as_std_path());
                return Err(ApiHttpError::internal(format!(
                    "failed to persist idempotency record: {error}"
                )));
            }
            // Under the claim lock no concurrent publisher exists, so this
            // rename cannot replace a live claim: a Valid predecessor
            // observed above returns Peer before reaching this write.
            Ok(ClaimOutcome::Owner { run_id })
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            // Our temp name is unique per attempt; a collision means a
            // leftover temp raced us. Reap and retry instead of assuming
            // peer status.
            reap_stale_claim_tmps(runs_dir);
            Err(ApiHttpError::internal(
                "failed to persist idempotency record: claim temp collision; retry".to_string(),
            ))
        }
        Err(error) => Err(ApiHttpError::internal(format!(
            "failed to persist idempotency record: {error}"
        ))),
    }
}

/// Best-effort reaping of claim temp files left by crashed publishers.
/// Only files older than the pending TTL are removed; anything newer may
/// belong to a live publisher mid-write.
fn reap_stale_claim_tmps(runs_dir: &Utf8PathBuf) {
    let dir = idempotency_dir(runs_dir);
    let entries = match std::fs::read_dir(dir.as_std_path()) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    let now = now_unix();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.contains(".tmp-") {
            continue;
        }
        let stale = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .map(|modified| {
                modified
                    .duration_since(UNIX_EPOCH)
                    .map(|elapsed| now.saturating_sub(elapsed.as_secs()) >= PENDING_TTL_SECS)
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

pub(crate) enum ClaimOutcome {
    Owner { run_id: Option<String> },
    Peer,
}

pub(crate) fn release_durable_pending(runs_dir: &Utf8PathBuf, key: &str) {
    let _ = std::fs::remove_file(pending_path(runs_dir, key).as_std_path());
}

enum PendingRead {
    Valid(DurablePendingRecord),
    /// Expired claim whose run id a same-digest retry may adopt.
    Expired(DurablePendingRecord),
    Absent,
    /// An unreadable, corrupt, or foreign claim. Claiming over it or
    /// waiting on it would risk splitting the key into two owners, so both
    /// paths fail closed with operator guidance.
    Unusable(String),
}

/// Our own claim and Ready records are a few hundred bytes: anything
/// larger is foreign damage. Reads are bounded so a planted huge file
/// can never exhaust memory; oversize content fails closed as unusable.
const MAX_IDEMPOTENCY_FILE_BYTES: u64 = 64 * 1024;

/// Absence is reported explicitly so a file vanishing mid-check (a
/// concurrent release) keeps its historical meaning instead of failing.
enum BoundedRead {
    Present(Vec<u8>),
    Absent,
    Unreadable(String),
}

fn read_bounded_idempotency_file(path: &camino::Utf8Path) -> BoundedRead {
    use std::io::Read as _;
    let file = match std::fs::File::open(path.as_std_path()) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return BoundedRead::Absent;
        }
        Err(error) => return BoundedRead::Unreadable(error.to_string()),
    };
    let mut bytes = Vec::new();
    if let Err(error) = file
        .take(MAX_IDEMPOTENCY_FILE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
    {
        return BoundedRead::Unreadable(error.to_string());
    }
    if bytes.len() as u64 > MAX_IDEMPOTENCY_FILE_BYTES {
        return BoundedRead::Unreadable(format!(
            "idempotency file `{path}` exceeds {MAX_IDEMPOTENCY_FILE_BYTES} bytes"
        ));
    }
    BoundedRead::Present(bytes)
}

fn read_durable_pending(runs_dir: &Utf8PathBuf, key: &str) -> PendingRead {
    let path = pending_path(runs_dir, key);
    let bytes = match read_bounded_idempotency_file(&path) {
        BoundedRead::Present(bytes) => bytes,
        BoundedRead::Absent => return PendingRead::Absent,
        BoundedRead::Unreadable(error) => {
            return PendingRead::Unusable(format!(
                "pending claim for key `{key}` is unreadable: {error}"
            ));
        }
    };
    let record: DurablePendingRecord = match serde_json::from_slice(&bytes) {
        Ok(record) => record,
        // Claims publish atomically (temp + rename under the claim lock),
        // so observed bytes are never a torn write: corrupt content means
        // real damage. Deleting it could destroy a live owner's claim and
        // split the key into two owners, so fail closed with operator
        // guidance instead of healing into Absent (A03).
        Err(error) => {
            return PendingRead::Unusable(format!(
                "corrupt pending claim for key `{key}` ({error}); operator action required"
            ));
        }
    };
    if record.key != key {
        // Never delete another key's file on a hash-path collision:
        // fail closed instead of breaking its owner.
        return PendingRead::Unusable(format!(
            "pending claim at key `{key}` belongs to a different key; operator action required"
        ));
    }
    if now_unix().saturating_sub(record.created_at_unix) >= PENDING_TTL_SECS {
        let _ = std::fs::remove_file(path.as_std_path());
        return PendingRead::Expired(record);
    }
    PendingRead::Valid(record)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn ttl_secs() -> Result<u64, String> {
    effective_idempotency_ttl().map(|ttl| ttl.as_secs().max(1))
}

/// Fail-closed load: absent and expired map to Ok(None), while corrupt
/// content or I/O errors other than NotFound map to Err so callers never
/// mistake an unreadable record for absence and start a duplicate run.
pub(crate) fn load_durable_ready_result(
    runs_dir: &Utf8PathBuf,
    key: &str,
) -> Result<Option<DurableIdempotencyRecord>, String> {
    let path = idempotency_path(runs_dir, key);
    let bytes = match read_bounded_idempotency_file(&path) {
        BoundedRead::Present(bytes) => bytes,
        BoundedRead::Absent => return Ok(None),
        BoundedRead::Unreadable(error) => return Err(error),
    };
    if bytes.is_empty() {
        // Partial write observed mid-rename: treat as absent only when the
        // writer is still active is undecidable here, so fail closed and
        // let the peer wait path retry.
        return Err("idempotency record is empty; writer may be in progress".into());
    }
    let record: DurableIdempotencyRecord =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if record.key != key {
        return Err("idempotency record key mismatch".into());
    }
    if now_unix().saturating_sub(record.created_at_unix) >= ttl_secs()? {
        let _ = std::fs::remove_file(path.as_std_path());
        return Ok(None);
    }
    Ok(Some(record))
}

pub(crate) enum WaitOutcome {
    Ready(String),
    /// The owner died before committing: the caller loops back and claims,
    /// adopting the orphaned run id instead of wedging. The id rides along
    /// because observing expiry already reaped the pending file; re-reading
    /// it in the claim loop would find nothing and orphan the run (A03).
    RetryClaim {
        adopted_run_id: Option<String>,
    },
}

/// Waits for a peer owner to publish the Ready record for the same digest.
/// Different digests conflict without starting a run. A vanished or expired
/// pending claim ends the wait with `RetryClaim` so a crashed owner never
/// wedges retries; only a live claim held past the deadline reports 503.
pub(crate) async fn wait_for_peer_ready(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
) -> Result<WaitOutcome, ApiHttpError> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match load_durable_ready_result(runs_dir, key) {
            Ok(Some(record)) => {
                if record.digest != digest {
                    return Err(idempotency_conflict());
                }
                return Ok(WaitOutcome::Ready(record.run_id));
            }
            Ok(None) => {}
            Err(_) => {
                // Partial/corrupt read while the owner writes: keep waiting
                // until the deadline instead of duplicating the run.
            }
        }
        match read_durable_pending(runs_dir, key) {
            PendingRead::Valid(pending) => {
                if pending.digest != digest {
                    return Err(idempotency_conflict());
                }
            }
            PendingRead::Expired(pending) => {
                if pending.digest != digest {
                    return Err(idempotency_conflict());
                }
                return Ok(WaitOutcome::RetryClaim {
                    adopted_run_id: pending.run_id.clone(),
                });
            }
            PendingRead::Absent => {
                // No Ready and no pending: the owner failed before commit.
                return Ok(WaitOutcome::RetryClaim {
                    adopted_run_id: None,
                });
            }
            PendingRead::Unusable(detail) => {
                return Err(ApiHttpError::internal(format!(
                    "failed to load idempotency record: {detail}"
                )));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ApiHttpError::service_unavailable(
                "idempotent request is still in progress elsewhere; retry with the same key",
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Persist a Ready record atomically (create_new temp + rename) so restart
/// and peer processes observe the same operation id to run id mapping.
/// Same key + same digest is idempotent; same key + different digest is a
/// conflict preserved from the existing record.
pub(crate) fn store_durable_ready(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
    run_id: &str,
) -> Result<(), String> {
    std::fs::create_dir_all(idempotency_dir(runs_dir).as_std_path())
        .map_err(|error| error.to_string())?;
    let path = idempotency_path(runs_dir, key);
    let record = DurableIdempotencyRecord {
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.to_string(),
        created_at_unix: now_unix(),
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
    // Serialize check-then-publish under a cross-process lock so two owners
    // never interleave temp writes and renames (A03). Readers use temp +
    // rename, so they never observe empty or partial files.
    let lock_path = idempotency_dir(runs_dir).join(".store.lock");
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path.as_std_path())
        .map_err(|error| error.to_string())?;
    let _lock_held = {
        use fs2::FileExt as _;
        lock_file
            .lock_exclusive()
            .map_err(|error| error.to_string())?;
        lock_file
    };
    match load_durable_ready_result(runs_dir, key) {
        Ok(Some(existing)) => {
            if existing.digest != digest {
                return Err("Idempotency-Key was already used with a different request".into());
            }
            return Ok(());
        }
        Ok(None) => {}
        Err(error) => return Err(error),
    }
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
    {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(tmp.as_std_path())
            .map_err(|error| error.to_string())?;
        file.write_all(&bytes).map_err(|error| error.to_string())?;
        file.sync_data().map_err(|error| error.to_string())?;
    }
    std::fs::rename(tmp.as_std_path(), path.as_std_path()).map_err(|error| error.to_string())?;
    // A lost directory fsync loses the rename on crash and resurrects the
    // key as unclaimed: propagate instead of silently risking a duplicate.
    sync_dir(&idempotency_dir(runs_dir)).map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_runs_dir(name: &str) -> Utf8PathBuf {
        let dir = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
            "qcg-idempotency-test-{name}-{}",
            uuid::Uuid::now_v7()
        )))
        .expect("temp dir should be UTF-8");
        std::fs::create_dir_all(dir.as_std_path()).expect("temp dir should be created");
        dir
    }

    #[test]
    fn corrupt_or_foreign_claim_is_unusable_not_absent() {
        let runs_dir = temp_runs_dir("unusable");
        let key = "key-1";
        // Corrupt content must fail closed: deleting it could destroy a live
        // owner's claim and split the key into two owners.
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        std::fs::write(pending_path(&runs_dir, key).as_std_path(), b"{torn")
            .expect("torn claim should be written");
        assert!(
            matches!(
                read_durable_pending(&runs_dir, key),
                PendingRead::Unusable(_)
            ),
            "corrupt claim must be unusable"
        );
        assert!(
            pending_path(&runs_dir, key).as_std_path().exists(),
            "corrupt claim must be retained for the operator"
        );
        // A foreign key's file must never be removed by this key's reader.
        let foreign = DurablePendingRecord {
            key: "other".into(),
            digest: "d".into(),
            run_id: None,
            owner: "o".into(),
            created_at_unix: now_unix(),
        };
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&foreign).expect("record should serialize"),
        )
        .expect("foreign claim should be written");
        assert!(
            matches!(
                read_durable_pending(&runs_dir, key),
                PendingRead::Unusable(_)
            ),
            "foreign claim must be unusable"
        );
        assert!(
            pending_path(&runs_dir, key).as_std_path().exists(),
            "foreign claim must be retained for the operator"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn second_concurrent_claimant_is_peer() {
        let runs_dir = temp_runs_dir("peer");
        let key = "key-1";
        let first = claim_durable_pending(&runs_dir, key, "digest", Some("run-1".into()))
            .expect("first claim should win");
        assert!(matches!(first, ClaimOutcome::Owner { .. }));
        let second = claim_durable_pending(&runs_dir, key, "digest", Some("run-2".into()))
            .expect("second claim should not error");
        assert!(
            matches!(second, ClaimOutcome::Peer),
            "live predecessor must report peer without overwriting its claim"
        );
        // The winner's record survives intact.
        match read_durable_pending(&runs_dir, key) {
            PendingRead::Valid(record) => assert_eq!(record.run_id.as_deref(), Some("run-1")),
            PendingRead::Expired(_) | PendingRead::Absent | PendingRead::Unusable(_) => {
                panic!("expected the winning claim to survive")
            }
        }
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }
}
