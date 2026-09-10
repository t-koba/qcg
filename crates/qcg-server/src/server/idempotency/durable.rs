//! Durable cross-process idempotency protocol: pending claims and Ready
//! records on the shared filesystem. Claims publish atomically (temp +
//! rename under the claim lock) and every unreadable state fails closed,
//! so two processes with the same key never both start a run (A03).

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    /// Claim generation that committed this record. Missing on pre-upgrade
    /// files and reads as 0.
    #[serde(default)]
    pub(crate) generation: u64,
}

fn idempotency_dir(runs_dir: &Utf8PathBuf) -> Utf8PathBuf {
    runs_dir.join("idempotency")
}

fn idempotency_path(runs_dir: &Utf8PathBuf, key: &str) -> Utf8PathBuf {
    idempotency_dir(runs_dir).join(format!(
        "{}.json",
        hex::encode(Sha256::digest(key.as_bytes()))
    ))
}

fn pending_path(runs_dir: &Utf8PathBuf, key: &str) -> Utf8PathBuf {
    idempotency_dir(runs_dir).join(format!(
        "{}.pending.json",
        hex::encode(Sha256::digest(key.as_bytes()))
    ))
}

/// The single cross-process lock for one idempotency key space: claim,
/// expiry reaping, Ready commit, and owner-checked release all serialize
/// here. Separate claim/store locks cannot order generation checks against
/// generation changes, and an unlocked read-compare-delete is not a
/// conditional delete (C01).
fn acquire_idempotency_lock(runs_dir: &Utf8PathBuf) -> Result<std::fs::File, String> {
    std::fs::create_dir_all(idempotency_dir(runs_dir).as_std_path())
        .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    let lock_path = idempotency_dir(runs_dir).join(".idempotency.lock");
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path.as_std_path())
        .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    {
        use fs2::FileExt as _;
        lock_file
            .lock_exclusive()
            .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    }
    Ok(lock_file)
}

/// Removes an expired Ready record. Callers must hold the idempotency
/// lock: deleting outside it lets an unlockED expiry check remove a Ready
/// record published after the check (C01).
fn reap_expired_ready_locked(runs_dir: &Utf8PathBuf, key: &str) -> Result<(), String> {
    let path = idempotency_path(runs_dir, key);
    let bytes = match read_bounded_idempotency_file(&path) {
        BoundedRead::Present(bytes) => bytes,
        BoundedRead::Absent => return Ok(()),
        BoundedRead::Unreadable(error) => return Err(error),
    };
    if bytes.is_empty() {
        return Err("idempotency record is empty; writer may be in progress".into());
    }
    let record: DurableIdempotencyRecord =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if record.key != key {
        return Err("idempotency record key mismatch".into());
    }
    if now_unix().saturating_sub(record.created_at_unix) >= ttl_secs()? {
        std::fs::remove_file(path.as_std_path()).map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Next claim generation for a key: one past the highest generation among
/// the live pending and Ready records. Must run under the claim lock with
/// the predecessor observation so two publishers cannot share a number.
/// Absent files count as 0; present-but-unparseable files fail closed
/// instead of silently restarting the chain at 0, which would let a
/// superseded owner present a seemingly current generation.
fn pending_generation(runs_dir: &Utf8PathBuf, key: &str) -> Result<u64, ApiHttpError> {
    fn generation_of(path: &camino::Utf8Path) -> Result<u64, ApiHttpError> {
        match read_bounded_idempotency_file(path) {
            BoundedRead::Absent => Ok(0),
            BoundedRead::Present(bytes) => serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|value| value.get("generation").and_then(Value::as_u64))
                .ok_or_else(|| {
                    ApiHttpError::internal(format!(
                        "idempotency record `{path}` has no usable claim generation; operator action required"
                    ))
                }),
            BoundedRead::Unreadable(error) => Err(ApiHttpError::internal(format!(
                "idempotency record `{path}` is unreadable: {error}"
            ))),
        }
    }
    let pending = generation_of(&pending_path(runs_dir, key))?;
    let ready = generation_of(&idempotency_path(runs_dir, key));
    let ready = ready?;
    Ok(pending.max(ready).saturating_add(1).max(1))
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
    /// Monotonic claim generation for this key. A committer holding a
    /// superseded generation must not overwrite a newer owner's claim or
    /// Ready record; it converges onto the committed result instead.
    #[serde(default)]
    generation: u64,
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
    // Serialize check-then-publish under the single cross-process
    // idempotency lock: temp + rename alone cannot arbitrate two
    // concurrent publishers (the second rename would silently replace the
    // first claim and crown two owners), so the predecessor read and the
    // publish below are one critical section. Waiters only read and never
    // take this lock.
    let _lock_held = acquire_idempotency_lock(runs_dir).map_err(ApiHttpError::internal)?;
    // Reap an expired Ready record under the same lock before observing:
    // an unlocked expiry check could delete a Ready record published
    // after the check (C01).
    if let Err(error) = reap_expired_ready_locked(runs_dir, key) {
        return Err(ApiHttpError::internal(format!(
            "failed to load idempotency record: {error}"
        )));
    }
    // Unified transition: re-check the Ready record under the same lock
    // before touching the pending claim. A commit landing between the
    // waiter's last look and this claim converges here instead of slipping
    // through into a second execution (B02).
    match load_durable_ready_result(runs_dir, key) {
        Ok(Some(committed)) => {
            if committed.digest != digest {
                return Ok(ClaimOutcome::Conflict);
            }
            return Ok(ClaimOutcome::Ready {
                run_id: committed.run_id,
            });
        }
        Ok(None) => {}
        Err(error) => {
            return Err(ApiHttpError::internal(format!(
                "failed to load idempotency record: {error}"
            )));
        }
    }
    // Reap an expired predecessor first so a dead claim never wedges
    // retries and an adopted run id survives for the same digest. A live
    // (Valid) predecessor means another owner holds the key: report Peer
    // without writing, since the atomic rename below would otherwise
    // replace its claim and crown two owners.
    // The expired predecessor is NOT unlinked here. The publication below
    // replaces it by atomic rename, so a crash at any point leaves either
    // the old mapping (before rename) or the new mapping adopting the same
    // run id (after rename). Unlinking first opened a window where both
    // were gone and a retry started a new run, losing the run association
    // (D04). The predecessor's generation still floors the next one.
    let mut expired_generation = 0u64;
    let adopted = match read_durable_pending(runs_dir, key) {
        PendingRead::Valid(_) => return Ok(ClaimOutcome::Peer),
        PendingRead::Expired(record) => {
            expired_generation = record.generation;
            if record.digest == digest {
                record.run_id.clone()
            } else {
                None
            }
        }
        PendingRead::Absent => None,
        PendingRead::Unusable(detail) => {
            return Err(ApiHttpError::internal(format!(
                "failed to persist idempotency record: {detail}"
            )));
        }
    };
    let path = pending_path(runs_dir, key);
    let run_id = adopted.or(reserved_run_id);
    let owner = uuid::Uuid::now_v7().to_string();
    // Next generation after every live record for this key, floored by the
    // expired predecessor (still on disk until the rename below). Absent
    // files count as 0, so a fresh chain starts at 1; a superseded owner
    // can never present a current generation.
    let generation = pending_generation(runs_dir, key)?.max(expired_generation.saturating_add(1));
    let record = DurablePendingRecord {
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.clone(),
        owner: owner.clone(),
        created_at_unix: now_unix(),
        generation,
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| {
        ApiHttpError::internal(format!("failed to persist idempotency record: {error}"))
    })?;
    // Atomic publication via temp + rename: readers observe either the
    // predecessor or a complete new claim, never a torn write. A torn read
    // misclassified as corrupt would delete a live owner's claim and split
    // the key into two owners (A03). Staging and replacement are separate
    // steps: a crash between them leaves the predecessor mapping intact
    // (D04).
    reap_stale_claim_tmps(runs_dir);
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
    match stage_pending(&tmp, &bytes) {
        Ok(()) => {
            let publish_result =
                replace_pending(&tmp, &path).and_then(|()| sync_dir(&idempotency_dir(runs_dir)));
            if let Err(error) = publish_result {
                return Err(ApiHttpError::internal(
                    match std::fs::remove_file(tmp.as_std_path()) {
                        Ok(()) => format!("failed to persist idempotency record: {error}"),
                        Err(cleanup_error) => format!(
                            "failed to persist idempotency record: {error}; leftover staging file `{tmp}` could not be removed: {cleanup_error}"
                        ),
                    },
                ));
            }
            // Under the claim lock no concurrent publisher exists, so this
            // rename cannot replace a live claim: a Valid predecessor
            // observed above returns Peer before reaching this write.
            Ok(ClaimOutcome::Owner {
                run_id,
                owner,
                generation,
            })
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

/// Writes and syncs a staged claim. Publication is a separate step so a
/// crash between staging and [`replace_pending`] leaves the predecessor
/// claim untouched (D04).
fn stage_pending(tmp: &camino::Utf8Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp.as_std_path())?;
    file.write_all(bytes)?;
    file.sync_data()
}

/// Atomically replaces the predecessor claim with the staged claim.
fn replace_pending(tmp: &camino::Utf8Path, path: &camino::Utf8Path) -> std::io::Result<()> {
    std::fs::rename(tmp.as_std_path(), path.as_std_path())
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
    Owner {
        run_id: Option<String>,
        owner: String,
        generation: u64,
    },
    Peer,
    /// A Ready record already commits this key: converge onto its run id
    /// instead of executing. Checked under the claim lock so a commit
    /// landing between the waiter's last look and this claim cannot slip
    /// through into a second execution (B02).
    Ready {
        run_id: String,
    },
    /// The key is committed for a different request digest.
    Conflict,
}

/// Releases a pending claim only when owner and generation still match
/// ours. A claim that expired mid-execution may have been adopted and
/// re-published by a peer; deleting it would break the new owner, so
/// anything foreign is left alone.
/// Releases our own pending claim. The owner/generation check and the
/// delete run under the single idempotency lock: an unlocked
/// read-compare-delete lets a stale reader remove the claim a successor
/// published after the read (C01). A lock failure keeps the claim for TTL
/// expiry and adoption instead of deleting unverified.
pub(crate) fn release_durable_pending(
    runs_dir: &Utf8PathBuf,
    key: &str,
    owner: &str,
    generation: u64,
) -> Result<(), String> {
    let _lock_held = acquire_idempotency_lock(runs_dir)?;
    let path = pending_path(runs_dir, key);
    let current = std::fs::read(path.as_std_path())
        .ok()
        .and_then(|bytes| serde_json::from_slice::<DurablePendingRecord>(&bytes).ok());
    if current
        .as_ref()
        .is_some_and(|record| record.owner == owner && record.generation == generation)
    {
        std::fs::remove_file(path.as_std_path()).map_err(|error| error.to_string())?;
    }
    Ok(())
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
    // Pure observation: expiry reaping happens only under the claim lock
    // in the claim path. Deleting here would let a waiter destroy a claim
    // another claimant is about to adopt.
    if now_unix().saturating_sub(record.created_at_unix) >= PENDING_TTL_SECS {
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
/// Pure observation: expiry reaping happens only under the idempotency
/// lock (claim/store paths), never here. Deleting from a reader would let
/// an old observation remove a Ready record published after it (C01).
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
        return Ok(None);
    }
    Ok(Some(record))
}

pub(crate) enum WaitOutcome {
    Ready(String),
    /// The owner died before committing: the caller loops back and claims,
    /// adopting the orphaned run id instead of wedging. The id rides along
    /// so a caller that cannot re-observe the claim still adopts the run
    /// instead of orphaning it (A03).
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
///
/// `expected_generation` fences stale committers: a publisher whose claim
/// was superseded (expiry adoption, key reuse) must not overwrite the
/// newer generation's outcome. A superseded commit for the same digest
/// still converges onto the committed run; anything else fails instead of
/// replacing it (B02).
/// Why a Ready commit was refused. The caller branches on the variant,
/// never on message text: a digest conflict releases the claim and
/// reports 409, while any storage failure fails closed without
/// advertising Ready.
#[derive(Debug)]
pub(crate) enum StoreReadyError {
    /// The key already committed a different request digest.
    DigestConflict,
    /// I/O, corrupt reads, or a superseded generation: details for the
    /// 500 path, never a second terminal outcome.
    Storage(String),
}

impl std::fmt::Display for StoreReadyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DigestConflict => {
                write!(
                    f,
                    "Idempotency-Key was already used with a different request"
                )
            }
            Self::Storage(detail) => write!(f, "{detail}"),
        }
    }
}

impl From<String> for StoreReadyError {
    fn from(detail: String) -> Self {
        Self::Storage(detail)
    }
}

pub(crate) fn store_durable_ready(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
    run_id: &str,
    expected_generation: u64,
) -> Result<(), StoreReadyError> {
    std::fs::create_dir_all(idempotency_dir(runs_dir).as_std_path())
        .map_err(|error| error.to_string())?;
    let path = idempotency_path(runs_dir, key);
    // Serialize check-then-publish under the single cross-process
    // idempotency lock (shared with claim and release) so generation
    // checks and generation changes are one ordered history (C01).
    // Readers use temp + rename, so they never observe empty or partial
    // files.
    let _lock_held = acquire_idempotency_lock(runs_dir)?;
    // Reap an expired Ready record under the same lock: the generation
    // fence below must observe exactly what it supersedes (C01).
    reap_expired_ready_locked(runs_dir, key)?;
    match load_durable_ready_result(runs_dir, key) {
        Ok(Some(existing)) => {
            if existing.digest != digest {
                return Err(StoreReadyError::DigestConflict);
            }
            return Ok(());
        }
        Ok(None) => {}
        Err(error) => return Err(error.into()),
    }
    // Generation fence: a claim superseded while executing (expiry
    // adoption, key reuse) must not overwrite the newer generation's
    // outcome. Same-digest supersession still converges onto whatever the
    // current generation committed; anything else fails instead.
    match read_durable_pending(runs_dir, key) {
        PendingRead::Valid(pending) | PendingRead::Expired(pending) => {
            let current = pending.generation;
            if current != expected_generation {
                match load_durable_ready_result(runs_dir, key) {
                    Ok(Some(committed)) if committed.digest == digest => return Ok(()),
                    _ => {
                        return Err(StoreReadyError::Storage(format!(
                            "idempotency claim superseded by generation {current}; retry converges onto the committed run"
                        )));
                    }
                }
            }
        }
        PendingRead::Absent | PendingRead::Unusable(_) => {}
    }
    let record = DurableIdempotencyRecord {
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.to_string(),
        created_at_unix: now_unix(),
        generation: expected_generation,
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
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
    fn stale_release_never_removes_a_successor_claim() {
        // C01: owner A reads its claim (owner=A, generation=1), stalls,
        // and owner B adopts and publishes (owner=B, generation=2). A's
        // late release must compare under the lock and leave B's claim.
        // The same holds for a superseded generation under the live
        // owner's own id.
        let runs_dir = temp_runs_dir("stale-release");
        let key = "key-1";
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        let claim = |owner: &str, generation: u64| DurablePendingRecord {
            key: key.into(),
            digest: "d".into(),
            run_id: None,
            owner: owner.into(),
            created_at_unix: now_unix(),
            generation,
        };
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&claim("A", 1)).expect("claim should serialize"),
        )
        .expect("claim A should be written");
        // Successor publishes after A's read.
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&claim("B", 2)).expect("claim should serialize"),
        )
        .expect("claim B should be written");
        release_durable_pending(&runs_dir, key, "A", 1).expect("release should not fail");
        release_durable_pending(&runs_dir, key, "B", 3)
            .expect("superseded generation should not fail");
        release_durable_pending(&runs_dir, key, "someone-else", 2)
            .expect("foreign owner should not fail");
        let survivor: DurablePendingRecord = serde_json::from_slice(
            &std::fs::read(pending_path(&runs_dir, key).as_std_path())
                .expect("claim file should survive"),
        )
        .expect("claim should parse");
        assert_eq!(
            survivor.owner, "B",
            "successor claim must survive a stale release"
        );
        assert_eq!(survivor.generation, 2);
        // The matching owner+generation still releases.
        release_durable_pending(&runs_dir, key, "B", 2).expect("release should not fail");
        assert!(
            !pending_path(&runs_dir, key).as_std_path().exists(),
            "matching release must remove the claim"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn ready_reap_keeps_a_fresh_record() {
        // C01: expiry reaping runs under the lock and only removes expired
        // records. A fresh Ready published after an old observation (real
        // or simulated) must survive the reap.
        let runs_dir = temp_runs_dir("ready-reap");
        let key = "key-1";
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        let ready = |age_secs: u64| DurableIdempotencyRecord {
            key: key.into(),
            digest: "d".into(),
            run_id: "run-1".into(),
            created_at_unix: now_unix().saturating_sub(age_secs),
            generation: 1,
        };
        let expired_age = qcg_policy::IDEMPOTENCY_TTL.as_secs().saturating_add(60);
        std::fs::write(
            idempotency_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&ready(expired_age)).expect("record should serialize"),
        )
        .expect("expired record should be written");
        // A fresh record lands (as it would under an interleaved publish).
        std::fs::write(
            idempotency_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&ready(0)).expect("record should serialize"),
        )
        .expect("fresh record should be written");
        reap_expired_ready_locked(&runs_dir, key).expect("reap should not fail");
        assert!(
            idempotency_path(&runs_dir, key).as_std_path().exists(),
            "fresh Ready must survive expiry reaping"
        );
        // An actually-expired record is reaped.
        std::fs::write(
            idempotency_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&ready(expired_age)).expect("record should serialize"),
        )
        .expect("expired record should be written");
        reap_expired_ready_locked(&runs_dir, key).expect("reap should not fail");
        assert!(
            !idempotency_path(&runs_dir, key).as_std_path().exists(),
            "expired Ready must be reaped"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
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
            generation: 1,
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
    fn store_refusal_is_typed_not_string_matched() {
        // The commit caller branches on the variant, never on message
        // text: a digest conflict releases the claim and reports 409.
        // (Superseded-generation typing lives with the generation-fence
        // test that owns that behavior.)
        let runs_dir = temp_runs_dir("store-typed");
        let key = "key-1";
        store_durable_ready(&runs_dir, key, "digest", "run-A", 1)
            .expect("ready commit should succeed");
        assert!(
            matches!(
                store_durable_ready(&runs_dir, key, "other-digest", "run-B", 1),
                Err(StoreReadyError::DigestConflict)
            ),
            "a committed key with a different digest must type as a conflict"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn committed_ready_converges_claim_without_executing() {
        // B02: a commit landing between the waiter's last look and the
        // claim must converge onto the committed run, never execute again.
        let runs_dir = temp_runs_dir("ready-converge");
        let key = "key-1";
        store_durable_ready(&runs_dir, key, "digest", "run-A", 1)
            .expect("ready commit should succeed");
        match claim_durable_pending(&runs_dir, key, "digest", Some("run-B".into()))
            .expect("claim should not error")
        {
            ClaimOutcome::Ready { run_id } => assert_eq!(run_id, "run-A"),
            other => panic!("committed key must converge, got {}", outcome_name(&other)),
        }
        match claim_durable_pending(&runs_dir, key, "other-digest", Some("run-C".into()))
            .expect("claim should not error")
        {
            ClaimOutcome::Conflict => {}
            other => panic!("foreign digest must conflict, got {}", outcome_name(&other)),
        }
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    fn outcome_name(outcome: &ClaimOutcome) -> &'static str {
        match outcome {
            ClaimOutcome::Owner { .. } => "Owner",
            ClaimOutcome::Peer => "Peer",
            ClaimOutcome::Ready { .. } => "Ready",
            ClaimOutcome::Conflict => "Conflict",
        }
    }

    #[test]
    fn generations_increase_across_expiry_chains() {
        // B02: adoption of an expired claim advances the generation, so a
        // superseded owner can never present a current one.
        let runs_dir = temp_runs_dir("generations");
        let key = "key-1";
        let first = claim_durable_pending(&runs_dir, key, "digest", Some("run-1".into()))
            .expect("first claim should win");
        let gen1 = match first {
            ClaimOutcome::Owner { generation, .. } => generation,
            other => panic!("expected owner, got {}", outcome_name(&other)),
        };
        assert_eq!(gen1, 1, "fresh chain starts at generation 1");
        // Age the claim past its TTL without touching anything else.
        let path = pending_path(&runs_dir, key);
        let mut record: DurablePendingRecord =
            serde_json::from_slice(&std::fs::read(path.as_std_path()).expect("claim readable"))
                .expect("claim parses");
        record.created_at_unix = 0;
        std::fs::write(
            path.as_std_path(),
            serde_json::to_vec(&record).expect("record serializes"),
        )
        .expect("aged claim should write");
        let second = claim_durable_pending(&runs_dir, key, "digest", Some("run-9".into()))
            .expect("adoption claim should win");
        match second {
            ClaimOutcome::Owner {
                run_id, generation, ..
            } => {
                assert_eq!(
                    run_id.as_deref(),
                    Some("run-1"),
                    "adoption reuses the run id"
                );
                assert_eq!(generation, 2, "adoption advances the generation");
            }
            other => panic!("expected adopting owner, got {}", outcome_name(&other)),
        }
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn staged_adoption_leaves_the_predecessor_until_replacement() {
        // D04: staging the successor must not touch the predecessor; only
        // the atomic replacement changes the mapping. This simulates a
        // crash between stage and replace and works under root, where the
        // read-only-directory injection below cannot fire.
        let runs_dir = temp_runs_dir("adopt-stage");
        let key = "key-1";
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        let predecessor = DurablePendingRecord {
            key: key.into(),
            digest: "digest".into(),
            run_id: Some("run-A".into()),
            owner: "dead-owner".into(),
            created_at_unix: 0,
            generation: 1,
        };
        let pending = pending_path(&runs_dir, key);
        std::fs::write(
            pending.as_std_path(),
            serde_json::to_vec(&predecessor).expect("record should serialize"),
        )
        .expect("predecessor claim should be written");
        let successor = DurablePendingRecord {
            key: key.into(),
            digest: "digest".into(),
            run_id: Some("run-A".into()),
            owner: "adopting-owner".into(),
            created_at_unix: now_unix(),
            generation: 2,
        };
        let tmp = pending.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
        stage_pending(
            &tmp,
            &serde_json::to_vec(&successor).expect("record should serialize"),
        )
        .expect("staging the successor should succeed");
        // Crash before replacement: the predecessor mapping is intact.
        let survivor: DurablePendingRecord = serde_json::from_slice(
            &std::fs::read(pending.as_std_path()).expect("predecessor should still exist"),
        )
        .expect("predecessor should parse");
        assert_eq!(survivor.run_id.as_deref(), Some("run-A"));
        assert_eq!(survivor.generation, 1);
        // Replacement converges onto the same run with the next generation.
        replace_pending(&tmp, &pending).expect("replacement should succeed");
        let published: DurablePendingRecord = serde_json::from_slice(
            &std::fs::read(pending.as_std_path()).expect("successor should exist"),
        )
        .expect("successor should parse");
        assert_eq!(published.run_id.as_deref(), Some("run-A"));
        assert_eq!(published.generation, 2);
        assert!(
            !tmp.as_std_path().exists(),
            "the replacement must consume the staged file"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[cfg(unix)]
    #[test]
    fn adoption_keeps_the_predecessor_mapping_until_publication() {
        // D04: an interrupted adoption must leave the predecessor's run
        // mapping in place. Publication is an atomic rename over the old
        // claim; when the rename cannot happen (here: the directory
        // refuses new files), the old mapping must survive instead of
        // having been unlinked first. The old code removed it before
        // publishing, so a crash in that window orphaned the run.
        use std::os::unix::fs::PermissionsExt as _;
        let runs_dir = temp_runs_dir("adopt-crash");
        let key = "key-1";
        let dir = idempotency_dir(&runs_dir);
        std::fs::create_dir_all(dir.as_std_path()).expect("idempotency dir should be created");
        // The lock file must exist before the directory turns read-only.
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(".idempotency.lock").as_std_path())
            .expect("lock file should be created");
        let expired = DurablePendingRecord {
            key: key.into(),
            digest: "digest".into(),
            run_id: Some("run-A".into()),
            owner: "dead-owner".into(),
            created_at_unix: 0,
            generation: 1,
        };
        let pending = pending_path(&runs_dir, key);
        std::fs::write(
            pending.as_std_path(),
            serde_json::to_vec(&expired).expect("record should serialize"),
        )
        .expect("expired claim should be written");
        let original = std::fs::metadata(dir.as_std_path())
            .expect("idempotency dir should have metadata")
            .permissions();
        let mut readonly = original.clone();
        readonly.set_mode(0o555);
        std::fs::set_permissions(dir.as_std_path(), readonly)
            .expect("idempotency dir should become read-only");
        // Probe: root ignores directory permissions, so skip rather than
        // assert on an environment where the fault cannot be injected.
        let probe = dir.join("probe");
        if std::fs::write(probe.as_std_path(), b"x").is_ok() {
            let _ = std::fs::remove_file(probe.as_std_path());
            std::fs::set_permissions(dir.as_std_path(), original)
                .expect("permissions should be restored");
            let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
            return;
        }
        let result = claim_durable_pending(&runs_dir, key, "digest", Some("run-B".into()));
        std::fs::set_permissions(dir.as_std_path(), original)
            .expect("permissions should be restored");
        assert!(
            result.is_err(),
            "publication into a read-only directory must fail"
        );
        let survivor: DurablePendingRecord = serde_json::from_slice(
            &std::fs::read(pending.as_std_path())
                .expect("predecessor mapping must survive the failed publication"),
        )
        .expect("surviving claim should parse");
        assert_eq!(
            survivor.run_id.as_deref(),
            Some("run-A"),
            "the adopted run must stay attributable"
        );
        assert_eq!(survivor.generation, 1, "the predecessor is unchanged");
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn superseded_commit_converges_or_fails_without_overwriting() {
        // B02: a committer whose claim was superseded must not replace the
        // newer generation's outcome.
        let runs_dir = temp_runs_dir("superseded-commit");
        let key = "key-1";
        let first = claim_durable_pending(&runs_dir, key, "digest", Some("run-1".into()))
            .expect("first claim should win");
        let gen1 = match first {
            ClaimOutcome::Owner { generation, .. } => generation,
            other => panic!("expected owner, got {}", outcome_name(&other)),
        };
        // A newer generation takes over the key for the same digest.
        let path = pending_path(&runs_dir, key);
        let mut record: DurablePendingRecord =
            serde_json::from_slice(&std::fs::read(path.as_std_path()).expect("claim readable"))
                .expect("claim parses");
        record.owner = "new-owner".into();
        record.generation = gen1 + 1;
        record.created_at_unix = now_unix();
        std::fs::write(
            path.as_std_path(),
            serde_json::to_vec(&record).expect("record serializes"),
        )
        .expect("superseding claim should write");
        // The stale owner commits nothing new: without a Ready record it
        // fails instead of overwriting.
        let error = store_durable_ready(&runs_dir, key, "digest", "run-1", gen1)
            .expect_err("superseded commit must not overwrite");
        assert!(
            matches!(error, StoreReadyError::Storage(_))
                && error.to_string().contains("superseded"),
            "supersession must type as an explicit storage failure, got: {error}"
        );
        // Once the current generation commits the same digest, the stale
        // owner converges onto it.
        store_durable_ready(&runs_dir, key, "digest", "run-1", gen1 + 1)
            .expect("current generation should commit");
        store_durable_ready(&runs_dir, key, "digest", "run-1", gen1)
            .expect("stale same-digest commit converges");
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
