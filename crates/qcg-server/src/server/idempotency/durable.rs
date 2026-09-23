//! Durable cross-process idempotency protocol: pending claims and Ready
//! records on the shared filesystem. Claims publish atomically (temp +
//! rename under the claim lock) and every unreadable state fails closed,
//! so two processes with the same key never both start a run (A03).

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::server::error::ApiHttpError;

use super::idempotency_conflict;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableIdempotencyRecord {
    pub(crate) key: String,
    pub(crate) digest: String,
    pub(crate) run_id: String,
    pub(crate) created_at_unix: u64,
    /// Claim generation that committed this record. Required: records
    /// without a generation are corrupt, never defaulted.
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
    // Open with O_NOFOLLOW relative to the parent directory file
    // descriptor (Unix) or open plus immediate post-open re-verification
    // (non-Unix), so a symlink planted or swapped at the lock path fails
    // closed instead of redirecting mutual exclusion outside the store
    // (E02). No separate pre-open probe exists: a probe plus open is a
    // TOCTOU window.
    let lock_file = open_no_follow(&lock_path, true, true, true, false)
        .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    lock_file
        .lock()
        .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    Ok(lock_file)
}

/// Opens a filesystem record without following a final-component symlink,
/// failing closed on planted links (E02). Unix opens relative to a parent
/// directory descriptor acquired with `O_DIRECTORY | O_NOFOLLOW`, then
/// `openat` with `O_NOFOLLOW`, so neither the directory nor the file can be
/// swapped to a symlink between any probe and the open: there is no probe.
/// Non-Unix has no `O_NOFOLLOW`, so it opens then immediately re-checks
/// `symlink_metadata` and fails closed on mismatch. Both paths share this
/// one helper so lock, bounded-read, and staging opens cannot diverge.
/// `create` allows creation, `create_new` requires exclusive creation
/// (`O_EXCL` semantics for staging temps). A non-exclusive `create` is
/// implemented as atomic `O_CREAT | O_EXCL` first and a plain `O_NOFOLLOW`
/// open of the existing file on `AlreadyExists`: macOS fails a racing
/// `O_NOFOLLOW | O_CREAT` (without `O_EXCL`) open of a not-yet-existing
/// name with a spurious `ENOENT`, so the two steps must never be merged.
/// A planted symlink reports `EEXIST` at creation and is then refused by
/// the `O_NOFOLLOW` open, never followed (E02). A delete landing between
/// the two steps retries boundedly; persistent churn fails closed.
fn open_no_follow(
    path: &camino::Utf8Path,
    read: bool,
    write: bool,
    create: bool,
    create_new: bool,
) -> std::io::Result<std::fs::File> {
    if create && !create_new {
        return open_no_follow_or_create(path, read, write);
    }
    open_no_follow_once(path, read, write, create, create_new)
}

/// Non-exclusive create spelled with race-safe primitives; see
/// [`open_no_follow`].
fn open_no_follow_or_create(
    path: &camino::Utf8Path,
    read: bool,
    write: bool,
) -> std::io::Result<std::fs::File> {
    let mut last_error = None;
    for _ in 0..8 {
        match open_no_follow_once(path, read, write, true, true) {
            Ok(file) => return Ok(file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                match open_no_follow_once(path, read, write, false, false) {
                    Ok(file) => return Ok(file),
                    // Deleted between creation-attempt and open: retry.
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        last_error = Some(error);
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            // Deleted between our observation and creation: retry.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                last_error = Some(error);
                continue;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::other(format!(
            "idempotency file `{path}` create/open churned repeatedly; refusing"
        ))
    }))
}

fn open_no_follow_once(
    path: &camino::Utf8Path,
    read: bool,
    write: bool,
    create: bool,
    create_new: bool,
) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::io::FromRawFd as _;
        if !read && !write {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("idempotency file `{path}` requires read and/or write"),
            ));
        }
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("idempotency file `{path}` has no parent"),
            )
        })?;
        let file_name = path.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("idempotency file `{path}` has no file name"),
            )
        })?;
        if file_name.is_empty() || file_name == "." || file_name == ".." {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("idempotency file `{path}` has an unsafe file name"),
            ));
        }
        let parent_c = CString::new(parent.as_str()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("idempotency file `{path}` contains a NUL byte"),
            )
        })?;
        let file_c = CString::new(file_name).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("idempotency file `{path}` contains a NUL byte"),
            )
        })?;
        // Pin the parent directory first: a swap of the directory itself
        // between any check and the file open cannot redirect the open.
        let dir_fd = unsafe {
            libc::open(
                parent_c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if dir_fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW;
        if read && write {
            flags |= libc::O_RDWR;
        } else if read {
            flags |= libc::O_RDONLY;
        } else {
            flags |= libc::O_WRONLY;
        }
        if create_new {
            flags |= libc::O_CREAT | libc::O_EXCL;
        } else if create {
            flags |= libc::O_CREAT;
        }
        let fd = unsafe {
            libc::openat(
                dir_fd,
                file_c.as_ptr(),
                flags,
                0o644 as libc::mode_t as libc::c_uint,
            )
        };
        // Capture the openat errno BEFORE closing dir_fd: close is
        // allowed to overwrite errno, so reading it afterwards
        // misattributes a stale error (e.g. ENOENT left by an internal
        // probe) to this open (E02).
        let open_errno = if fd < 0 {
            Some(std::io::Error::last_os_error())
        } else {
            None
        };
        // The directory descriptor is pinned only for the openat above.
        unsafe {
            libc::close(dir_fd);
        }
        if let Some(os) = open_errno {
            return Err(os);
        }
        // O_NOFOLLOW already refused a final-component symlink, but a FIFO,
        // directory, or other non-regular file would still open: verify the
        // opened description is a regular file and fail closed otherwise.
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: fd is a valid open descriptor from openat above.
        // Capture errno before closing: see the openat site above (E02).
        if unsafe { libc::fstat(fd, &mut stat) } != 0 {
            let os = std::io::Error::last_os_error();
            unsafe {
                libc::close(fd);
            }
            return Err(os);
        }
        if (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
            unsafe {
                libc::close(fd);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("idempotency file `{path}` is not a regular file"),
            ));
        }
        // SAFETY: fd is a valid owned descriptor from openat above.
        Ok(unsafe { std::fs::File::from_raw_fd(fd) })
    }
    #[cfg(not(unix))]
    {
        let mut options = std::fs::OpenOptions::new();
        options
            .read(read)
            .write(write)
            .create(create)
            .create_new(create_new)
            .truncate(false);
        let file = options.open(path.as_std_path())?;
        // Re-check metadata immediately after open and fail closed on
        // mismatch: a symlink swapped in before the open was followed
        // (no O_NOFOLLOW here), so a post-open symlink observation refuses
        // the opened handle instead of trusting it (E02).
        match std::fs::symlink_metadata(path.as_std_path()) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("idempotency file `{path}` is a symlink"),
                ));
            }
            Ok(meta) if !meta.is_file() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("idempotency file `{path}` is not a regular file"),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // The path vanished after a successful open: the handle
                // names an unlinked file, so report absence for read paths
                // and fail the staging paths via the same NotFound.
                return Err(error);
            }
            Err(error) => return Err(error),
        }
        // The path is a regular non-symlink, but the opened handle could
        // still be a non-regular file on odd filesystems: verify the handle.
        if !file.metadata().map(|meta| meta.is_file()).unwrap_or(false) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("idempotency file `{path}` is not a regular file"),
            ));
        }
        Ok(file)
    }
}

/// One observation of both key files, read once per critical section so
/// the claim path never re-reads the same files it just inspected (E02).
/// `None` is an absent file; I/O failures are already folded into `Err`
/// by the bounded reader.
struct KeyFilesSnapshot {
    ready: Option<Vec<u8>>,
    pending: Option<Vec<u8>>,
}

fn read_key_files_snapshot(runs_dir: &Utf8PathBuf, key: &str) -> Result<KeyFilesSnapshot, String> {
    let snapshot_of = |path: camino::Utf8PathBuf| match read_bounded_idempotency_file(&path) {
        BoundedRead::Present(bytes) => Ok(Some(bytes)),
        BoundedRead::Absent => Ok(None),
        BoundedRead::Unreadable(error) => Err(error),
    };
    Ok(KeyFilesSnapshot {
        ready: snapshot_of(idempotency_path(runs_dir, key))?,
        pending: snapshot_of(pending_path(runs_dir, key))?,
    })
}

/// Test-only helper sharing the production snapshot reap path: removes an
/// expired Ready record under the idempotency lock (E02).
#[cfg(test)]
fn reap_expired_ready_locked(
    runs_dir: &Utf8PathBuf,
    key: &str,
    ttl: std::time::Duration,
) -> Result<(), String> {
    let snapshot = read_key_files_snapshot(runs_dir, key)?;
    reap_expired_ready_snapshot(runs_dir, key, snapshot.ready.as_deref(), ttl).map(|_| ())
}

/// Removes an expired Ready record from an already-read snapshot.
/// Callers must hold the idempotency lock: deleting outside it lets an
/// unlocked expiry check remove a Ready record published after the check
/// (C01). Returns the live record and the generation floor: an expired
/// record still floors the next generation so a superseded owner cannot
/// restart the chain at 0. Absent counts as 0.
fn reap_expired_ready_snapshot(
    runs_dir: &Utf8PathBuf,
    key: &str,
    ready_bytes: Option<&[u8]>,
    ttl: std::time::Duration,
) -> Result<(Option<DurableIdempotencyRecord>, u64), String> {
    let Some(bytes) = ready_bytes else {
        return Ok((None, 0));
    };
    if bytes.is_empty() {
        return Err("idempotency record is empty; writer may be in progress".into());
    }
    let record: DurableIdempotencyRecord =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    if record.key != key {
        return Err("idempotency record key mismatch".into());
    }
    let generation = record.generation;
    if now_unix()?.saturating_sub(record.created_at_unix) >= ttl.as_secs().max(1) {
        std::fs::remove_file(idempotency_path(runs_dir, key).as_std_path())
            .map_err(|error| error.to_string())?;
        return Ok((None, generation));
    }
    Ok((Some(record), generation))
}

/// Claim generation from already-parsed records: one past the highest
/// generation among the pending and Ready records. Absent counts as 0.
/// Callers pass the generations obtained from the single parse per file
/// above, so the same bytes are never parsed twice under different rules
/// (E02). Present-but-unparseable files never reach here: both parse paths
/// fail closed before generation is computed.
fn pending_generation_from_parsed(pending_generation: u64, ready_generation: u64) -> u64 {
    pending_generation.max(ready_generation).saturating_add(1)
}

/// Flush a directory entry so a just-published rename survives a crash:
/// delegated to the single platform implementation in `qcg_service`
/// so Unix `O_DIRECTORY | O_NOFOLLOW` handling lives in exactly one place
/// (E01).
fn sync_dir(dir: &Utf8PathBuf) -> std::io::Result<()> {
    qcg_service::sync_dir_entry(dir)
}

/// Unfinished reservation lifetime in seconds. Run creation is
/// millisecond-scale, so 60 seconds bounds crash recovery without pinning a
/// dead claim anywhere near the 24h Ready TTL. The peer waiter (30s) is
/// strictly shorter: on waiter deadline with a live claim it reports 503,
/// and on an expired or vanished claim it retries the claim loop, adopting
/// the orphaned run id instead of wedging.
/// The Ready TTL is never read here: every production entry point receives
/// it as a `ttl` parameter frozen from the boot environment
/// (`AppState::idempotency_ttl`), so an operator override cannot drift from
/// the running server. The `qcg_policy::IDEMPOTENCY_TTL` uses below this
/// line are all inside `#[cfg(test)]` and pin tests to the default (E04).
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
    /// Required: records without a generation are corrupt, never defaulted.
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
    ttl: std::time::Duration,
) -> Result<ClaimOutcome, ApiHttpError> {
    // Serialize check-then-publish under the single cross-process
    // idempotency lock: temp + rename alone cannot arbitrate two
    // concurrent publishers (the second rename would silently replace the
    // first claim and crown two owners), so the predecessor read and the
    // publish below are one critical section. Waiters only read and never
    // take this lock.
    let _lock_held = acquire_idempotency_lock(runs_dir).map_err(ApiHttpError::internal)?;
    // Both key files are read exactly once per critical section; every
    // decision below folds the same snapshot instead of re-reading (E02).
    // Reaping the expired Ready record under the same lock first keeps an
    // unlocked expiry check from deleting a Ready record published after
    // the check (C01), and re-checking it before touching the pending
    // claim converges a commit that landed between the waiter's last look
    // and this claim instead of slipping through into a second execution
    // (B02).
    let snapshot = read_key_files_snapshot(runs_dir, key).map_err(ApiHttpError::internal)?;
    // Single parse per file: the Ready parse below yields both the live
    // record and the generation floor (expired records still floor).
    // The pending parse below yields the pending generation. No second
    // Value reparsing occurs (E02).
    let (committed_opt, ready_generation) =
        match reap_expired_ready_snapshot(runs_dir, key, snapshot.ready.as_deref(), ttl) {
            Ok((committed, generation)) => (committed, generation),
            Err(error) => {
                return Err(ApiHttpError::internal(format!(
                    "failed to load idempotency record: {error}"
                )));
            }
        };
    if let Some(committed) = committed_opt {
        if committed.digest != digest {
            return Ok(ClaimOutcome::Conflict);
        }
        return Ok(ClaimOutcome::Ready {
            run_id: committed.run_id,
        });
    }
    // A live (Valid) predecessor means another owner holds the key. When
    // the digest differs, conflict immediately at claim time instead of
    // delegating to a waiter round-trip (E02): the waiter would conflict
    // after one 100 ms iteration for the same decision. Same-digest live
    // claims report Peer so the waiter converges on the owner's Ready.
    // Without writing in either case, since the atomic rename below would
    // otherwise replace its claim and crown two owners.
    // The expired predecessor is NOT unlinked here. The publication below
    // replaces it by atomic rename, so a crash at any point leaves either
    // the old mapping (before rename) or the new mapping adopting the same
    // run id (after rename). Unlinking first opened a window where both
    // were gone and a retry started a new run, losing the run association
    // (D04). The predecessor's generation still floors the next one.
    let mut adopted: Option<String> = None;
    let pending_generation: u64 = match match snapshot.pending.as_deref() {
        Some(bytes) => parse_pending_bytes(key, bytes),
        None => PendingRead::Absent,
    } {
        PendingRead::Valid(record) => {
            if record.digest != digest {
                return Ok(ClaimOutcome::Conflict);
            }
            return Ok(ClaimOutcome::Peer);
        }
        PendingRead::Expired(record) => {
            let generation = record.generation;
            if record.digest == digest {
                adopted = record.run_id.clone();
            }
            generation
        }
        PendingRead::Absent => 0,
        PendingRead::Unusable(detail) => {
            return Err(ApiHttpError::internal(format!(
                "failed to persist idempotency record: {detail}"
            )));
        }
    };
    let path = pending_path(runs_dir, key);
    let run_id = adopted.or(reserved_run_id);
    let owner = uuid::Uuid::now_v7().to_string();
    // Next generation after every record for this key, derived from the
    // single parse per file above and never re-read or reparsed (E02).
    // Expired predecessors (pending and Ready) still floor the chain so
    // a superseded owner cannot restart at 0. Absent counts as 0.
    let generation = pending_generation_from_parsed(pending_generation, ready_generation);
    let record = DurablePendingRecord {
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.clone(),
        owner: owner.clone(),
        created_at_unix: now_unix().map_err(ApiHttpError::internal)?,
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
    // (D04). Stale-temp reclamation is periodic (shared interval) and its
    // partial-sweep error fails the claim instead of looking clean (E02).
    reap_stale_claim_tmps(runs_dir).map_err(ApiHttpError::internal)?;
    // Temp names are unique per attempt; on collision retry with a fresh id
    // instead of failing the admission (E02). Under the claim lock no live
    // publisher contends, so repeated collisions mean planted files.
    let mut attempts = 0;
    let tmp = loop {
        if attempts >= 3 {
            return Err(ApiHttpError::internal(
                "failed to persist idempotency record: claim temp collided repeatedly; refusing to overwrite".to_string(),
            ));
        }
        attempts += 1;
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
        match stage_pending(&tmp, &bytes) {
            Ok(()) => break tmp,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                tracing::warn!(path = %tmp, "claim staging collision; retrying with a fresh id");
                // Periodic sweep already ran above; a second scan here would
                // double-scan per claim, so retry with a fresh id directly.
                continue;
            }
            Err(error) => {
                return Err(ApiHttpError::internal(format!(
                    "failed to persist idempotency record: {error}"
                )));
            }
        }
    };
    // Publication and durability are separate steps: the temp is consumed
    // by a successful rename, so only a rename failure may attempt tmp
    // removal. A directory-sync failure after a successful rename must not
    // claim a leftover staging file (the tmp no longer exists by
    // construction); it propagates as a persistence error instead (E02).
    if let Err(error) = replace_pending(&tmp, &path) {
        return Err(ApiHttpError::internal(
            match std::fs::remove_file(tmp.as_std_path()) {
                Ok(()) => format!("failed to persist idempotency record: {error}"),
                Err(cleanup_error) => format!(
                    "failed to persist idempotency record: {error}; leftover staging file `{tmp}` could not be removed: {cleanup_error}"
                ),
            },
        ));
    }
    match sync_dir(&idempotency_dir(runs_dir)) {
        Ok(()) => {
            // Under the claim lock no concurrent publisher exists, so this
            // rename cannot replace a live claim: a Valid predecessor
            // observed above returns Peer before reaching this write.
            Ok(ClaimOutcome::Owner {
                run_id,
                owner,
                generation,
            })
        }
        Err(error) => Err(ApiHttpError::internal(format!(
            "failed to persist idempotency record: {error}"
        ))),
    }
}

/// Writes and syncs a staged claim. Publication is a separate step so a
/// crash between staging and [`replace_pending`] leaves the predecessor
/// claim untouched (D04). `create_new` never overwrites, and the shared
/// `open_no_follow` helper fails closed on a planted symlink at the temp
/// name on every platform (E02).
fn stage_pending(tmp: &camino::Utf8Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = open_no_follow(tmp, false, true, false, true)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Atomically replaces the predecessor claim with the staged claim.
/// Plain rename (not NOREPLACE) is correct here: the caller holds the
/// cross-process idempotency lock, the predecessor is expired or absent,
/// and the new claim intentionally supersedes it. A crash before rename
/// leaves the predecessor intact (D04).
fn replace_pending(tmp: &camino::Utf8Path, path: &camino::Utf8Path) -> std::io::Result<()> {
    std::fs::rename(tmp.as_std_path(), path.as_std_path())
}

/// Best-effort reaping of claim temp files left by crashed publishers.
/// Only files older than the pending TTL are removed; anything newer may
/// belong to a live publisher mid-write. Claim temps are staged under the
/// cross-process idempotency lock (not per-file locks), so the mtime gate
/// is the correct liveness signal; symlinks are never followed (E02).
/// Periodic, not per-claim: at most one directory scan per `PENDING_TTL_SECS`
/// interval (shared with cancel sweeps) so steady traffic never pays a
/// `read_dir` per admission, and a collision retry never double-scans (E02).
/// Iteration errors are collected best-effort and the first is returned
/// after finishing the sweep, so a partial sweep never looks clean (E02).
/// Last stale-temp sweep per idempotency store. Per-store (not global):
/// parallel tests and independent stores must never suppress each other's
/// sweep, and one hot store must not starve another's reclamation (E02).
static LAST_TEMP_SWEEP_UNIX: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, u64>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
fn reset_temp_sweep_for_tests() {
    LAST_TEMP_SWEEP_UNIX
        .lock()
        .expect("temp sweep clock should lock")
        .clear();
}

fn reap_stale_claim_tmps(runs_dir: &Utf8PathBuf) -> Result<(), String> {
    let now_for_gate = match now_unix() {
        Ok(now) => now,
        Err(_) => {
            tracing::warn!(
                "stale idempotency temp sweep skipped: system clock is before the Unix epoch"
            );
            return Ok(());
        }
    };
    let dir = idempotency_dir(runs_dir);
    let dir_key = dir.as_str().to_string();
    {
        // Fail closed on a poisoned sweep clock: a poisoned mutex means a
        // previous sweep panicked while holding the lock, so proceeding
        // would run the sweep on unverified throttle state. The caller
        // surfaces this as a persistence error instead of claiming cleanly.
        let mut last = match LAST_TEMP_SWEEP_UNIX.lock() {
            Ok(guard) => guard,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "stale idempotency temp sweep skipped: sweep clock is poisoned"
                );
                return Err(format!(
                    "stale idempotency temp sweep clock is poisoned: {error}"
                ));
            }
        };
        let previous = last.get(&dir_key).copied().unwrap_or(0);
        if previous != 0 && now_for_gate.saturating_sub(previous) < PENDING_TTL_SECS {
            return Ok(());
        }
        last.insert(dir_key, now_for_gate);
    }
    let entries = match std::fs::read_dir(dir.as_std_path()) {
        Ok(entries) => entries,
        // A missing store directory simply has nothing to reap; any other
        // scan failure is surfaced instead of looking like a clean sweep
        // (E02).
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            tracing::warn!(dir = %dir, %error, "stale idempotency temp sweep failed");
            return Err(error.to_string());
        }
    };
    let Ok(now) = now_unix() else {
        tracing::warn!(
            "stale idempotency temp sweep skipped: system clock is before the Unix epoch"
        );
        return Ok(());
    };
    let mut first_error: Option<String> = None;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(dir = %dir, %error, "stale idempotency temp sweep incomplete");
                if first_error.is_none() {
                    first_error = Some(error.to_string());
                }
                continue;
            }
        };
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            // Non-UTF8 names never match temp patterns via lossy conversion (E02).
            continue;
        };
        if !name.contains(".tmp-") {
            continue;
        }
        // Never follow a symlink temp: remove the link only (E02).
        // A single metadata probe serves both checks: no second stat (E02).
        let meta = match std::fs::symlink_metadata(entry.path()) {
            Ok(meta) => meta,
            Err(error) => {
                tracing::warn!(path = %entry.path().display(), %error, "idempotency temp unreadable; skipping reap");
                if first_error.is_none() {
                    first_error = Some(error.to_string());
                }
                continue;
            }
        };
        if meta.file_type().is_symlink() {
            if let Err(error) = std::fs::remove_file(entry.path()) {
                tracing::warn!(path = %entry.path().display(), %error, "failed to reap symlinked idempotency temp");
                if first_error.is_none() {
                    first_error = Some(error.to_string());
                }
            }
            continue;
        }
        let stale = meta
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .is_some_and(|elapsed| now.saturating_sub(elapsed.as_secs()) >= PENDING_TTL_SECS);
        if stale && let Err(error) = std::fs::remove_file(entry.path()) {
            tracing::warn!(path = %entry.path().display(), %error, "failed to reap stale idempotency temp");
            if first_error.is_none() {
                first_error = Some(error.to_string());
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[derive(Debug)]
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

/// Releases our own pending claim. The owner/generation check and the
/// delete run under the single idempotency lock: an unlocked
/// read-compare-delete lets a stale reader remove the claim a successor
/// published after the read (C01). Only an exact owner+generation match
/// removes the file; anything foreign is left alone for TTL expiry or the
/// operator, and a lock failure keeps the claim instead of deleting
/// unverified.
pub(crate) fn release_durable_pending(
    runs_dir: &Utf8PathBuf,
    key: &str,
    owner: &str,
    generation: u64,
) -> Result<(), String> {
    let _lock_held = acquire_idempotency_lock(runs_dir)?;
    let path = pending_path(runs_dir, key);
    // Absence is not an error (already released or adopted), but an
    // unreadable or corrupt file is: silently treating it as released
    // would hide a stuck claim that wedges the key until operator action
    // (E02). Callers warn and fall back to TTL expiry.
    let bytes = match read_bounded_idempotency_file(&path) {
        BoundedRead::Present(bytes) => bytes,
        BoundedRead::Absent => return Ok(()),
        BoundedRead::Unreadable(error) => {
            return Err(format!(
                "idempotency claim for key `{key}` is unreadable: {error}"
            ));
        }
    };
    let current: DurablePendingRecord = serde_json::from_slice(&bytes).map_err(|error| {
        format!("idempotency claim for key `{key}` is corrupt ({error}); operator action required")
    })?;
    // Key-equality gate (E02): a hash-path collision or planted foreign-key
    // file must never be deleted by a mismatched claim. Fail closed without
    // touching the file so the foreign owner's claim survives for the
    // operator.
    if current.key != key {
        return Err(format!(
            "idempotency claim at key `{key}` belongs to a different key; refusing release; operator action required"
        ));
    }
    if current.owner == owner && current.generation == generation {
        std::fs::remove_file(path.as_std_path()).map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// What a pre-execution ownership recheck found (E02).
pub(crate) enum OwnerCheck {
    /// Our claim still owns the key: safe to execute.
    Current,
    /// Another owner replaced our claim (expiry adoption or foreign
    /// write): executing now would orphan our run, so re-enter the claim
    /// loop instead.
    Superseded,
    /// Our claim file is absent without replacement: same treatment.
    /// Named `Absent` to share one vocabulary with `PendingRead::Absent`
    /// for the same meaning (E02).
    Absent,
}

/// Re-reads our pending claim under the idempotency lock just before
/// execution. A descheduled owner whose claim expired and was adopted
/// while it slept must not start executing its stale reservation: that
/// run could never commit and would linger as an orphan (E02). The digest
/// is also verified so a claim adopted for different content fails fast
/// instead of executing a run that the commit-time check would refuse.
/// Key equality and TTL expiry are verified through the shared
/// [`parse_pending_bytes`] classifier (fail-closed): a foreign-key file or
/// an expired claim never reads as current, and corrupt content fails
/// instead of reading as absent (E02).
pub(crate) fn check_pending_owner(
    runs_dir: &Utf8PathBuf,
    key: &str,
    owner: &str,
    generation: u64,
    expected_digest: &str,
) -> Result<OwnerCheck, String> {
    let _lock_held = acquire_idempotency_lock(runs_dir)?;
    let path = pending_path(runs_dir, key);
    let bytes = match read_bounded_idempotency_file(&path) {
        BoundedRead::Present(bytes) => bytes,
        BoundedRead::Absent => return Ok(OwnerCheck::Absent),
        BoundedRead::Unreadable(error) => {
            return Err(format!(
                "idempotency claim for key `{key}` is unreadable: {error}"
            ));
        }
    };
    // Single classifier so key-mismatch and expiry rules cannot diverge
    // from the claim path (E02). Expired reads as superseded (the owner
    // must re-claim and adopt), unusable fails closed.
    match parse_pending_bytes(key, &bytes) {
        PendingRead::Valid(current) => {
            if current.owner == owner
                && current.generation == generation
                && current.digest == expected_digest
            {
                Ok(OwnerCheck::Current)
            } else {
                Ok(OwnerCheck::Superseded)
            }
        }
        PendingRead::Expired(_) => Ok(OwnerCheck::Superseded),
        PendingRead::Absent => Ok(OwnerCheck::Absent),
        PendingRead::Unusable(detail) => Err(detail),
    }
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
    // Single shared open without a pre-open probe: `open_no_follow` opens
    // relative to the parent directory descriptor with O_NOFOLLOW on Unix
    // and re-verifies immediately after open elsewhere, so a symlink swap
    // between a probe and the open cannot redirect the read (E02). A
    // symlink or non-regular file is planted damage, never a record, and a
    // FIFO would block the open: all fail closed as unreadable.
    let file = match open_no_follow(path, true, false, false, false) {
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
    parse_pending_bytes(key, &bytes)
}

/// Pure parse-and-classify half of [`read_durable_pending`], shared by the
/// single-read waiter and the snapshotted claim path so the same bytes are
/// never parsed twice under different rules (E02).
fn parse_pending_bytes(key: &str, bytes: &[u8]) -> PendingRead {
    let record: DurablePendingRecord = match serde_json::from_slice(bytes) {
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
    let Ok(now) = now_unix() else {
        return PendingRead::Unusable(
            "system clock is before the Unix epoch; refusing idempotency decision".into(),
        );
    };
    if now.saturating_sub(record.created_at_unix) >= PENDING_TTL_SECS {
        return PendingRead::Expired(record);
    }
    PendingRead::Valid(record)
}

/// Wall-clock seconds since the Unix epoch. A pre-epoch clock is broken
/// infrastructure, not a time to guess from: falling back to 0 would mark
/// every record expired and let claimants adopt live owners' runs, so
/// every caller fails closed instead (E02).
fn now_unix() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| "system clock is before the Unix epoch; refusing idempotency decision".into())
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
    ttl: std::time::Duration,
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
    if now_unix()?.saturating_sub(record.created_at_unix) >= ttl.as_secs().max(1) {
        return Ok(None);
    }
    Ok(Some(record))
}

#[derive(Debug)]
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
    ttl: std::time::Duration,
) -> Result<WaitOutcome, ApiHttpError> {
    wait_for_peer_ready_with_deadline(
        runs_dir,
        key,
        digest,
        ttl,
        tokio::time::Instant::now() + std::time::Duration::from_secs(30),
    )
    .await
}

/// Deadline-injectable half of [`wait_for_peer_ready`]: production passes
/// the 30 s deadline above; tests pass a short one so the live-claim 503
/// path is verified without waiting 30 s (E02).
async fn wait_for_peer_ready_with_deadline(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
    ttl: std::time::Duration,
    deadline: tokio::time::Instant,
) -> Result<WaitOutcome, ApiHttpError> {
    loop {
        // Filesystem reads are blocking: run them off the async runtime so
        // a slow disk never stalls Tokio workers while waiting. Both reads
        // ride one blocking task per iteration instead of two round trips
        // (E02). The two reads are sequential, not atomic: Ready is checked
        // first every pass, and Ready is re-read after a pending decision
        // before returning, so a commit landing between the two reads (or
        // between the last read and the deadline) converges instead of
        // retrying or reporting 503 (E02).
        let (ready, pending) = {
            let runs_dir = runs_dir.clone();
            let key = key.to_string();
            tokio::task::spawn_blocking(move || {
                (
                    load_durable_ready_result(&runs_dir, &key, ttl),
                    read_durable_pending(&runs_dir, &key),
                )
            })
            .await
            .map_err(|error| {
                ApiHttpError::internal(format!("idempotency wait task failed: {error}"))
            })?
        };
        match ready {
            Ok(Some(record)) => {
                if record.digest != digest {
                    return Err(idempotency_conflict());
                }
                return Ok(WaitOutcome::Ready(record.run_id));
            }
            Ok(None) => {}
            Err(detail) => {
                // Fail fast, never hang: publishes are temp + rename, so
                // readers observe complete files and a corrupt record is
                // real damage, not a torn write. Waiting 30 s would only
                // delay the same 500, and the client's same-key retry
                // converges once the owner commits or the operator heals
                // the file (E02).
                return Err(ApiHttpError::internal(format!(
                    "failed to load idempotency record: {detail}"
                )));
            }
        }
        // Re-read Ready after the pending observation and before acting on
        // it, closing the deadline-arrival window where a commit lands
        // between the two sequential reads (E02). A Ready arriving here
        // converges instead of retrying or timing out.
        let recheck_ready = |runs_dir: &Utf8PathBuf, key: &str| {
            let runs_dir = runs_dir.clone();
            let key = key.to_string();
            async move {
                tokio::task::spawn_blocking(move || load_durable_ready_result(&runs_dir, &key, ttl))
                    .await
                    .map_err(|error| {
                        ApiHttpError::internal(format!("idempotency wait task failed: {error}"))
                    })?
                    .map_err(|detail| {
                        ApiHttpError::internal(format!(
                            "failed to load idempotency record: {detail}"
                        ))
                    })
            }
        };
        match pending {
            PendingRead::Valid(pending) => {
                if pending.digest != digest {
                    return Err(idempotency_conflict());
                }
            }
            PendingRead::Expired(pending) => {
                if pending.digest != digest {
                    return Err(idempotency_conflict());
                }
                if let Some(record) = recheck_ready(runs_dir, key).await? {
                    if record.digest != digest {
                        return Err(idempotency_conflict());
                    }
                    return Ok(WaitOutcome::Ready(record.run_id));
                }
                return Ok(WaitOutcome::RetryClaim {
                    adopted_run_id: pending.run_id.clone(),
                });
            }
            PendingRead::Absent => {
                // No Ready and no pending: the owner failed before commit.
                // Re-check Ready first: a commit may have landed after the
                // first read (E02).
                if let Some(record) = recheck_ready(runs_dir, key).await? {
                    if record.digest != digest {
                        return Err(idempotency_conflict());
                    }
                    return Ok(WaitOutcome::Ready(record.run_id));
                }
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
            // Deadline arrived with a live claim: re-read Ready once more
            // before reporting 503 so a commit landing in the final window
            // converges (E02).
            if let Some(record) = recheck_ready(runs_dir, key).await? {
                if record.digest != digest {
                    return Err(idempotency_conflict());
                }
                return Ok(WaitOutcome::Ready(record.run_id));
            }
            return Err(ApiHttpError::service_unavailable(
                "idempotent request is still in progress elsewhere; retry with the same key",
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

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

/// Persist a Ready record atomically (create_new temp + rename) so restart
/// and peer processes observe the same operation id to run id mapping.
///
/// The commit must prove it still owns the durable claim: under the single
/// idempotency lock the current pending record must be `Valid` with matching
/// claim id (`owner`), generation, digest, and reserved run id. A missing,
/// expired, or unreadable claim is refused instead of publishing Ready.
/// An expired claim is always refused, even with no successor: the owner
/// must re-claim (which adopts the run id) instead of committing a stale
/// reservation (E02).
/// A committed record for the same digest converges onto its run id only
/// when the caller proves current ownership, or when no reservation exists
/// at all (`Absent` replay after release): any ownership mismatch with a
/// reservation present fails closed instead of converging (E02).
pub(crate) fn store_durable_ready(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
    run_id: &str,
    owner: &str,
    generation: u64,
    ttl: std::time::Duration,
) -> Result<String, StoreReadyError> {
    std::fs::create_dir_all(idempotency_dir(runs_dir).as_std_path())
        .map_err(|error| error.to_string())?;
    let path = idempotency_path(runs_dir, key);
    // Serialize check-then-publish under the single cross-process
    // idempotency lock (shared with claim and release) so ownership
    // checks and ownership changes are one ordered history (C01/E02).
    // Readers use temp + rename, so they never observe empty or partial
    // files.
    let _lock_held = acquire_idempotency_lock(runs_dir)?;
    // Single snapshot under the lock: the Ready observation, the expiry
    // reap decision, and the pending ownership proof all see the same
    // bytes, so the claim's single-snapshot discipline holds on the
    // commit path too (E02).
    let snapshot = read_key_files_snapshot(runs_dir, key)?;
    // Parse the pending claim once for both the Ready-convergence decision
    // and the ownership proof below, so the same bytes are never parsed
    // twice under different rules (E02).
    let pending_read = snapshot
        .pending
        .as_deref()
        .map(|bytes| parse_pending_bytes(key, bytes))
        .unwrap_or(PendingRead::Absent);
    // Ownership proof helper for a live claim: owner, generation, digest,
    // and reserved run id must all match. Expired claims never pass, even
    // with matching identity: the owner must re-claim instead (E02).
    let check_live_ownership = |pending: &DurablePendingRecord| -> Result<(), StoreReadyError> {
        if pending.owner != owner {
            return Err(StoreReadyError::Storage(
                "idempotency claim is owned by a different claim; commit refused".into(),
            ));
        }
        if pending.generation != generation {
            return Err(StoreReadyError::Storage(format!(
                "idempotency claim superseded by generation {}; commit refused",
                pending.generation
            )));
        }
        if pending.digest != digest {
            return Err(StoreReadyError::Storage(
                "idempotency claim was admitted for different content; commit refused".into(),
            ));
        }
        match &pending.run_id {
            Some(reserved) if reserved == run_id => Ok(()),
            _ => Err(StoreReadyError::Storage(
                "idempotency claim has no matching reserved run; commit refused".into(),
            )),
        }
    };
    if let (Some(existing), _) =
        reap_expired_ready_snapshot(runs_dir, key, snapshot.ready.as_deref(), ttl)?
    {
        if existing.digest != digest {
            return Err(StoreReadyError::DigestConflict);
        }
        // Same digest already committed. Converge only with proof: a live
        // matching reservation converges onto the committed run (the caller
        // settles its surplus execution as an orphan), and an absent
        // reservation replays onto the committed run. Any other pending
        // state (mismatched reservation, expired, unusable) fails closed
        // instead of converging onto a mismatched run id (E02).
        // The Absent-vs-Unusable asymmetry is intentional and must be
        // preserved: Absent carries no information (release already ran or
        // the claim was never written), so converging onto the committed
        // Ready is safe; Unusable is positive evidence of damage (corrupt,
        // foreign-key, or unreadable claim), so converging would hide the
        // damage and refuse path fails closed for operator action (E02).
        match &pending_read {
            PendingRead::Valid(pending) => {
                check_live_ownership(pending)?;
                return Ok(existing.run_id);
            }
            PendingRead::Absent => {
                return Ok(existing.run_id);
            }
            PendingRead::Expired(_) => {
                return Err(StoreReadyError::Storage(
                    "idempotency claim expired; commit refused".into(),
                ));
            }
            PendingRead::Unusable(detail) => {
                return Err(StoreReadyError::Storage(format!(
                    "idempotency claim is unusable: {detail}"
                )));
            }
        }
    }
    // No live Ready: ownership proof for a new publication. A stale owner
    // (stopped, superseded, released, or whose claim was reaped) must never
    // publish a new Ready. The claim id is unique per claim, so matching it
    // proves current ownership even when a later chain restarts at
    // generation 1 (E02). Expired claims are refused even with matching
    // identity: no successor could have slipped in (check and publication
    // share the lock), but the reservation is stale and must be re-claimed
    // (E02).
    match &pending_read {
        PendingRead::Valid(pending) => {
            check_live_ownership(pending)?;
        }
        PendingRead::Expired(_) => {
            return Err(StoreReadyError::Storage(
                "idempotency claim expired; commit refused".into(),
            ));
        }
        PendingRead::Absent => {
            return Err(StoreReadyError::Storage(
                "idempotency claim is absent; refusing to publish a new Ready record".into(),
            ));
        }
        PendingRead::Unusable(detail) => {
            return Err(StoreReadyError::Storage(format!(
                "idempotency claim is unusable: {detail}"
            )));
        }
    }
    let record = DurableIdempotencyRecord {
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.to_string(),
        created_at_unix: now_unix()?,
        generation,
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
    // Temp names are unique per attempt; on collision retry with a fresh id
    // instead of failing the commit, mirroring the claim path (E02). Under
    // the commit lock no live publisher contends, so repeated collisions
    // mean planted files.
    let mut attempts = 0;
    let tmp = loop {
        if attempts >= 3 {
            return Err(StoreReadyError::Storage(
                "idempotency ready temp collided repeatedly; refusing to overwrite".into(),
            ));
        }
        attempts += 1;
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
        {
            // Shared no-follow open so a planted symlink at the temp name
            // fails closed on every platform (E02).
            let open_result = open_no_follow(&tmp, false, true, false, true);
            match open_result {
                Ok(mut file) => {
                    if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
                        let _ = std::fs::remove_file(tmp.as_std_path());
                        return Err(error.to_string().into());
                    }
                    break tmp;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    tracing::warn!(path = %tmp, "ready staging collision; retrying with a fresh id");
                    continue;
                }
                Err(error) => return Err(error.to_string().into()),
            }
        }
    };
    if let Err(error) = std::fs::rename(tmp.as_std_path(), path.as_std_path()) {
        // The rename itself failed, so the staged record never became
        // visible: reclaim it. (A later directory-sync failure is handled
        // separately below; the temp is already consumed by then.)
        let _ = std::fs::remove_file(tmp.as_std_path());
        return Err(error.to_string().into());
    }
    // Directory-sync failure is warn-only, unified with the cancel mailbox
    // path (E02/Q2): the staged file already synced via `sync_all`, so the
    // rename is durable on the file itself; a lost directory entry only
    // resurrects the key as unclaimed on crash, and a Ready retry converges
    // onto the same run (idempotent) instead of minting a duplicate.
    // Q2 boundary: this warn-only covers power-loss loss of the directory
    // entry, which is outside the guaranteed process-crash (SIGKILL)
    // boundary. A retry after such loss re-commits the same mapping and
    // converges; it never mints a duplicate run from one published Ready.
    // The possible duplicate execution this implies under power loss is
    // accepted and documented in `docs/operations.md` (Q2).
    if let Err(error) = sync_dir(&idempotency_dir(runs_dir)) {
        tracing::warn!(%error, "idempotency ready directory sync failed; file sync already durable");
    }
    Ok(run_id.to_string())
}

/// Reverse mapping from a run to its idempotency key, stored alongside the
/// run journal so a Ready-record loss still converges onto the original run
/// instead of starting a duplicate (E03). Written once after execution,
/// read only when no Ready record exists. Best-effort on write (a missing
/// sidecar only loses the recovery path; the Ready record remains the
/// primary mapping), fail-closed on read (an unreadable sidecar fails the
/// retry rather than risking a duplicate).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OrphanIdempotencyRecord {
    pub(crate) key: String,
    pub(crate) digest: String,
    pub(crate) run_id: String,
    /// Publication time for TTL expiry: without it a stale sidecar would
    /// pin a key to a permanently conflicting digest (E02). Required, never
    /// defaulted.
    pub(crate) created_at_unix: u64,
}

fn orphan_path_for_run(runs_dir: &Utf8PathBuf, run_id: &str) -> Utf8PathBuf {
    runs_dir.join(run_id).join("meta").join("idempotency.json")
}

/// Records the key-to-run association inside the run directory itself.
/// Called after execution succeeds but before the Ready commit, so a crash
/// between execution and Ready commit (or a later Ready loss) still leaves
/// a durable pointer from the key to the already-created run.
pub(crate) fn write_orphan_record(
    runs_dir: &Utf8PathBuf,
    run_id: &str,
    key: &str,
    digest: &str,
) -> Result<(), String> {
    // Fail closed on unsafe run ids: an absolute or traversing id must
    // never escape the runs directory into an arbitrary write.
    if run_id.is_empty()
        || run_id.contains('/')
        || run_id.contains('\\')
        || run_id.contains('\0')
        || run_id == "."
        || run_id == ".."
        || run_id == "idempotency"
    {
        return Err(format!(
            "refusing to write orphan record for unsafe run id `{run_id}`"
        ));
    }
    let path = orphan_path_for_run(runs_dir, run_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent.as_std_path()).map_err(|error| error.to_string())?;
    }
    let record = OrphanIdempotencyRecord {
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.to_string(),
        // A broken clock fails the write instead of stamping 0, which
        // would read as instantly expired and silently drop the recovery
        // pointer (E02).
        created_at_unix: now_unix()?,
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_IDEMPOTENCY_FILE_BYTES {
        return Err("orphan record exceeds size bound".into());
    }
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
    {
        // Shared no-follow open so a planted symlink at the temp name fails
        // closed on every platform (E02).
        let mut file =
            open_no_follow(&tmp, false, true, false, true).map_err(|error| error.to_string())?;
        file.write_all(&bytes).map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
    }
    if let Err(error) = std::fs::rename(tmp.as_std_path(), path.as_std_path()) {
        let _ = std::fs::remove_file(tmp.as_std_path());
        return Err(error.to_string());
    }
    if let Some(parent) = path.parent() {
        sync_dir(&parent.to_path_buf()).map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Finds the original run for a key by scanning run sidecars. Returns the
/// (run_id, digest) for the sidecar whose key matches and whose TTL has not
/// expired, or `None` when no run was ever associated with the key.
/// The scan takes no lock and stays best-effort by design: a sidecar renamed
/// into place concurrently may be missed, in which case the caller claims
/// fresh and executes a duplicate that the commit-time ownership check
/// settles as an orphan. Commit-time checks own correctness, so the scan
/// never needs to be exact (E02). The Ready record remains the primary
/// mapping; the sidecar only matters after Ready loss, so the residual
/// window needs a crash placed exactly between the scan and the claim.
/// A corrupt or unreadable sidecar whose key cannot be established is
/// skipped with a warning instead of failing every key: the sidecar is a
/// best-effort recovery pointer (Ready is primary), and one damaged run
/// must not wedge unrelated keys (E02). A sidecar that parses but mismatches
/// its directory, or duplicates a key across runs, still fails closed.
/// Expired sidecars are skipped (and best-effort removed) so a key is never
/// pinned to a permanently conflicting digest (E02).
pub(crate) fn find_orphan_run(
    runs_dir: &Utf8PathBuf,
    key: &str,
    ttl: std::time::Duration,
) -> Result<Option<(String, String)>, String> {
    let entries = std::fs::read_dir(runs_dir.as_std_path()).map_err(|error| error.to_string())?;
    let mut found: Option<(String, String)> = None;
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            // Non-UTF8 run dirs never match a key via lossy conversion (E02).
            continue;
        };
        if file_name == "idempotency" || file_name.starts_with('.') {
            continue;
        }
        let file_type = entry.file_type().map_err(|error| error.to_string())?;
        if !file_type.is_dir() {
            continue;
        }
        let orphan_path = Utf8PathBuf::from_path_buf(entry.path())
            .map_err(|path| format!("run path is not UTF-8: {}", path.display()))?;
        let orphan_path = orphan_path.join("meta").join("idempotency.json");
        let bytes = match read_bounded_idempotency_file(&orphan_path) {
            BoundedRead::Present(bytes) => bytes,
            BoundedRead::Absent => continue,
            BoundedRead::Unreadable(error) => {
                tracing::warn!(path = %orphan_path, %error, "skipping unreadable orphan sidecar");
                continue;
            }
        };
        let record: OrphanIdempotencyRecord = match serde_json::from_slice(&bytes) {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(path = %orphan_path, %error, "skipping corrupt orphan sidecar");
                continue;
            }
        };
        if record.key != key {
            continue;
        }
        // TTL expiry: a stale sidecar never pins the key (E02).
        // Removal failures are warned, never silent (E02).
        {
            let now = now_unix()?;
            if now.saturating_sub(record.created_at_unix) >= ttl.as_secs().max(1) {
                if let Err(error) = std::fs::remove_file(orphan_path.as_std_path()) {
                    tracing::warn!(path = %orphan_path, %error, "failed to reap expired orphan sidecar");
                }
                continue;
            }
        }
        // The sidecar must name its own directory: a mismatched run id is
        // damage, never an adoption pointer.
        if record.run_id != file_name {
            return Err(format!(
                "orphan record for key `{key}` names `{}` but lives in `{file_name}`",
                record.run_id
            ));
        }
        // A key names at most one run with one digest: duplicate sidecars
        // for the same key with different runs or different digests are
        // damage, never readdir-order dependent (E02).
        if let Some((existing_run, existing_digest)) = &found {
            if existing_run != &record.run_id || existing_digest != &record.digest {
                return Err(format!(
                    "duplicate orphan records for key `{key}`: (`{existing_run}`, digest `{existing_digest}`) vs (`{}`, digest `{}`)",
                    record.run_id, record.digest
                ));
            }
            continue;
        }
        found = Some((record.run_id, record.digest));
    }
    Ok(found)
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

    /// Removes the temp root on drop so a failed assertion cannot leak test
    /// directories (E04).
    struct TempGuard(Utf8PathBuf);
    impl Drop for TempGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0.as_std_path());
        }
    }

    #[tokio::test]
    async fn corrupt_ready_fails_fast_instead_of_waiting_out_the_deadline() {
        // E02: publishes are temp + rename, so a corrupt Ready record is
        // real damage, never a torn write. The waiter must fail fast
        // instead of hanging 30 s for the same 500.
        let runs_dir = temp_runs_dir("corrupt-wait");
        let _temp_guard = TempGuard(runs_dir.clone());
        let key = "key-1";
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        std::fs::write(
            idempotency_path(&runs_dir, key).as_std_path(),
            b"{not valid json",
        )
        .expect("corrupt record should be written");
        let started = tokio::time::Instant::now();
        let Err(error) =
            wait_for_peer_ready(&runs_dir, key, "d", qcg_policy::IDEMPOTENCY_TTL).await
        else {
            panic!("a corrupt Ready record must fail the wait");
        };
        assert!(
            format!("{error:?}").contains("failed to load idempotency record"),
            "the failure must name the corrupt record: {error:?}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(25),
            "the waiter must fail fast, not wait out the deadline: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn pending_owner_recheck_distinguishes_current_superseded_and_gone() {
        // E02: the pre-execution recheck must tell an intact claim from a
        // replaced or vanished one so a descheduled owner never executes a
        // stale reservation.
        let runs_dir = temp_runs_dir("owner-recheck");
        let _temp_guard = TempGuard(runs_dir.clone());
        let key = "key-1";
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        // No claim file at all: absent, never current.
        assert!(matches!(
            check_pending_owner(&runs_dir, key, "nobody", 1, "d"),
            Ok(OwnerCheck::Absent)
        ));
        let claim = |owner: &str, generation: u64| DurablePendingRecord {
            key: key.into(),
            digest: "d".into(),
            run_id: None,
            owner: owner.into(),
            created_at_unix: now_unix().expect("test clock should be after the Unix epoch"),
            generation,
        };
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&claim("A", 1)).expect("claim should serialize"),
        )
        .expect("claim A should be written");
        assert!(matches!(
            check_pending_owner(&runs_dir, key, "A", 1, "d"),
            Ok(OwnerCheck::Current)
        ));
        // Same owner, superseded generation: another owner adopted since.
        assert!(matches!(
            check_pending_owner(&runs_dir, key, "A", 0, "d"),
            Ok(OwnerCheck::Superseded)
        ));
        // Same identity but different content: the commit would refuse,
        // so the pre-execution check already reports superseded.
        assert!(matches!(
            check_pending_owner(&runs_dir, key, "A", 1, "other"),
            Ok(OwnerCheck::Superseded)
        ));
        // Successor's claim: foreign to A.
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&claim("B", 2)).expect("claim should serialize"),
        )
        .expect("claim B should be written");
        assert!(matches!(
            check_pending_owner(&runs_dir, key, "A", 1, "d"),
            Ok(OwnerCheck::Superseded)
        ));
        assert!(matches!(
            check_pending_owner(&runs_dir, key, "B", 2, "d"),
            Ok(OwnerCheck::Current)
        ));
        // Corrupt content fails closed instead of reading as gone.
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            b"{not valid json",
        )
        .expect("corrupt claim should be written");
        assert!(
            check_pending_owner(&runs_dir, key, "B", 2, "d").is_err(),
            "a corrupt claim must fail the recheck, not read as gone"
        );
    }

    #[test]
    fn pending_owner_recheck_verifies_key_and_expiry_fail_closed() {
        // E02: the recheck must verify key equality and TTL expiry through
        // the shared classifier, not just owner+generation+digest. A
        // foreign-key file fails closed (never Current/Superseded), and an
        // expired claim reads as Superseded so the owner re-claims instead
        // of executing a stale reservation.
        let runs_dir = temp_runs_dir("owner-key-expiry");
        let _temp_guard = TempGuard(runs_dir.clone());
        let key = "key-1";
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        // Foreign-key file: key field differs from the lookup key.
        let foreign = DurablePendingRecord {
            key: "other".into(),
            digest: "d".into(),
            run_id: None,
            owner: "A".into(),
            created_at_unix: now_unix().expect("test clock should be after the Unix epoch"),
            generation: 1,
        };
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&foreign).expect("claim should serialize"),
        )
        .expect("foreign claim should be written");
        assert!(
            check_pending_owner(&runs_dir, key, "A", 1, "d").is_err(),
            "a foreign-key file must fail the recheck, never read as owned"
        );
        assert!(
            pending_path(&runs_dir, key).as_std_path().exists(),
            "a foreign-key file must survive the failed recheck for the operator"
        );
        // Expired claim with matching identity: superseded, never current.
        let expired = DurablePendingRecord {
            key: key.into(),
            digest: "d".into(),
            run_id: Some("run-A".into()),
            owner: "A".into(),
            created_at_unix: 0,
            generation: 1,
        };
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&expired).expect("claim should serialize"),
        )
        .expect("expired claim should be written");
        assert!(
            matches!(
                check_pending_owner(&runs_dir, key, "A", 1, "d"),
                Ok(OwnerCheck::Superseded)
            ),
            "an expired claim must read as superseded even with matching identity"
        );
    }

    #[test]
    fn ready_present_unusable_pending_is_refused_not_converged() {
        // E02: the Ready-present Absent-vs-Unusable asymmetry is intentional:
        // Absent (no information) converges onto the committed run, while
        // Unusable (positive evidence of damage) fails closed. This test
        // pins the rejection so a future refactor cannot silently converge
        // corrupt claims onto a mismatched run id.
        let runs_dir = temp_runs_dir("ready-unusable");
        let _temp_guard = TempGuard(runs_dir.clone());
        let key = "key-ready-unusable";
        let (owner, generation) = claim_owner(&runs_dir, key, "digest", "run-A");
        store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("ready commit should succeed");
        // Release the live claim so the commit path observes a chosen
        // pending state; Absent must converge.
        release_durable_pending(&runs_dir, key, &owner, generation)
            .expect("release should succeed");
        let converged = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("absent reservation with committed Ready must converge");
        assert_eq!(converged, "run-A");
        // Corrupt the pending file: same Ready present, pending Unusable.
        std::fs::write(pending_path(&runs_dir, key).as_std_path(), b"{torn")
            .expect("torn claim should be written");
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("unusable pending with committed Ready must be refused, not converged");
        assert!(
            matches!(error, StoreReadyError::Storage(_)),
            "unusable refusal must be storage-typed: {error}"
        );
        assert!(
            error.to_string().contains("unusable"),
            "unusable refusal must name the damage: {error}"
        );
    }

    #[test]
    fn release_never_deletes_a_foreign_key_file() {
        // E02: a foreign-key file (hash-path collision or planted damage)
        // must never be deleted by a mismatched release claim. The release
        // fails closed and the file survives for the operator.
        let runs_dir = temp_runs_dir("foreign-key-release");
        let _temp_guard = TempGuard(runs_dir.clone());
        let key = "key-1";
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        let foreign = DurablePendingRecord {
            key: "other".into(),
            digest: "d".into(),
            run_id: None,
            owner: "o".into(),
            created_at_unix: now_unix().expect("test clock should be after the Unix epoch"),
            generation: 1,
        };
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&foreign).expect("record should serialize"),
        )
        .expect("foreign claim should be written");
        let error = release_durable_pending(&runs_dir, key, "o", 1)
            .expect_err("a foreign-key release must fail closed");
        assert!(
            error.contains("different key"),
            "foreign-key refusal must name the mismatch: {error}"
        );
        assert!(
            pending_path(&runs_dir, key).as_std_path().exists(),
            "a foreign-key file must survive a mismatched release"
        );
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
            created_at_unix: now_unix().expect("test clock should be after the Unix epoch"),
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
    fn expired_orphan_sidecar_never_pins_the_key() {
        // E02: an orphan sidecar older than the TTL is skipped (and
        // reaped), so a key is never pinned to a permanently conflicting
        // digest. A corrupt sidecar is skipped as well: Ready is primary.
        let runs_dir = temp_runs_dir("orphan-ttl");
        let run_dir = runs_dir.join("run-old");
        std::fs::create_dir_all(run_dir.join("meta").as_std_path())
            .expect("meta dir should be created");
        let stale = OrphanIdempotencyRecord {
            key: "key-ttl".to_string(),
            digest: "old-digest".to_string(),
            run_id: "run-old".to_string(),
            created_at_unix: 1,
        };
        std::fs::write(
            run_dir.join("meta").join("idempotency.json").as_std_path(),
            serde_json::to_vec(&stale).expect("record should serialize"),
        )
        .expect("sidecar should be written");
        // Corrupt sidecar in another run must not fail this key either.
        let bad_dir = runs_dir.join("run-bad");
        std::fs::create_dir_all(bad_dir.join("meta").as_std_path())
            .expect("meta dir should be created");
        std::fs::write(
            bad_dir.join("meta").join("idempotency.json").as_std_path(),
            b"{not json",
        )
        .expect("bad sidecar should be written");
        let found = find_orphan_run(&runs_dir, "key-ttl", std::time::Duration::from_secs(3600))
            .expect("expired and corrupt sidecars must be skipped, not fail the key");
        assert!(found.is_none(), "expired orphan must not pin the key");
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn records_without_generation_are_corrupt_not_defaulted() {
        // E02: no backward-compatibility shim. A record without a generation
        // must fail deserialization instead of defaulting to 0 and aliasing
        // a newer owner's claim.
        let pending_without_generation = serde_json::json!({
            "key": "k",
            "digest": "d",
            "run_id": "run-1",
            "owner": "owner-1",
            "created_at_unix": 1,
        });
        assert!(
            serde_json::from_value::<DurablePendingRecord>(pending_without_generation).is_err(),
            "pending without generation must be corrupt"
        );
        let ready_without_generation = serde_json::json!({
            "key": "k",
            "digest": "d",
            "run_id": "run-1",
            "created_at_unix": 1,
        });
        assert!(
            serde_json::from_value::<DurableIdempotencyRecord>(ready_without_generation).is_err(),
            "ready without generation must be corrupt"
        );
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
            created_at_unix: now_unix()
                .expect("test clock should be after the Unix epoch")
                .saturating_sub(age_secs),
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
        reap_expired_ready_locked(&runs_dir, key, qcg_policy::IDEMPOTENCY_TTL)
            .expect("reap should not fail");
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
        reap_expired_ready_locked(&runs_dir, key, qcg_policy::IDEMPOTENCY_TTL)
            .expect("reap should not fail");
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
            created_at_unix: now_unix().expect("test clock should be after the Unix epoch"),
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
    fn live_claim_with_different_digest_conflicts_immediately() {
        // E02: a live claim for another digest conflicts at claim time
        // instead of delegating to a waiter round-trip. The waiter would
        // conflict after one iteration for the same decision.
        let runs_dir = temp_runs_dir("peer-digest");
        let key = "key-peer";
        claim_owner(&runs_dir, key, "digest-a", "run-A");
        match claim_durable_pending(
            &runs_dir,
            key,
            "digest-b",
            Some("run-B".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("claim should not error")
        {
            ClaimOutcome::Conflict => {}
            other => panic!(
                "live foreign digest must conflict immediately, got {}",
                outcome_name(&other)
            ),
        }
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn concurrent_claims_converge_on_one_owner() {
        // E02: parallel claimants serialize on the claim lock: exactly one
        // becomes Owner and the rest observe Peer.
        let runs_dir = temp_runs_dir("parallel-claim");
        let key = "key-parallel";
        let outcomes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for index in 0..8 {
            let runs_dir = runs_dir.clone();
            let outcomes = std::sync::Arc::clone(&outcomes);
            handles.push(std::thread::spawn(move || {
                let outcome = claim_durable_pending(
                    &runs_dir,
                    key,
                    "digest",
                    Some(format!("run-{index}")),
                    qcg_policy::IDEMPOTENCY_TTL,
                )
                .expect("claim should not error");
                outcomes
                    .lock()
                    .expect("outcomes lock")
                    .push(outcome_name(&outcome));
            }));
        }
        for handle in handles {
            handle.join().expect("claim thread should finish");
        }
        let outcomes = outcomes.lock().expect("outcomes lock");
        assert_eq!(
            outcomes.iter().filter(|o| **o == "Owner").count(),
            1,
            "exactly one parallel claimant must own, got {outcomes:?}"
        );
        assert!(
            outcomes.iter().all(|o| *o == "Owner" || *o == "Peer"),
            "parallel claimants must be owner or peer, got {outcomes:?}"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn expired_own_claim_without_successor_is_refused() {
        // E02: an expired claim is refused even with no successor and
        // matching identity: the reservation is stale and must be re-claimed
        // (which adopts the run id) instead of committing directly.
        let runs_dir = temp_runs_dir("expired-commit");
        let key = "key-expired";
        let (owner, generation) = claim_owner(&runs_dir, key, "digest", "run-A");
        // Age the claim past the pending TTL without replacing it.
        let path = pending_path(&runs_dir, key);
        let mut record: DurablePendingRecord =
            serde_json::from_slice(&std::fs::read(path.as_std_path()).expect("claim should exist"))
                .expect("claim should parse");
        record.created_at_unix = record.created_at_unix.saturating_sub(3600);
        std::fs::write(
            path.as_std_path(),
            serde_json::to_vec(&record).expect("record should serialize"),
        )
        .expect("aged claim should be written");
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("expired commit without a live successor must be refused");
        assert!(
            error.to_string().contains("expired"),
            "expiry must be named, got: {error}"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[tokio::test]
    async fn live_claim_held_past_deadline_reports_unavailable() {
        // E02: a live same-digest claim held past the waiter deadline
        // reports 503 instead of executing a duplicate. The production
        // deadline is 30 s; the injected short deadline exercises the same
        // branch in milliseconds.
        let runs_dir = temp_runs_dir("waiter-deadline");
        let key = "key-wait";
        claim_owner(&runs_dir, key, "digest", "run-A");
        let error = wait_for_peer_ready_with_deadline(
            &runs_dir,
            key,
            "digest",
            qcg_policy::IDEMPOTENCY_TTL,
            tokio::time::Instant::now() + std::time::Duration::from_millis(300),
        )
        .await
        .expect_err("a held live claim must exhaust the deadline");
        assert!(
            error.to_string().contains("still in progress"),
            "deadline exhaustion must report 503, got: {error}"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn store_refusal_is_typed_not_string_matched() {
        // The commit caller branches on the variant, never on message
        // text: a digest conflict releases the claim and reports 409.
        let runs_dir = temp_runs_dir("store-typed");
        let key = "key-1";
        let owner = claim_owner(&runs_dir, key, "digest", "run-A");
        store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner.0,
            owner.1,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("ready commit should succeed");
        assert!(
            matches!(
                store_durable_ready(
                    &runs_dir,
                    key,
                    "other-digest",
                    "run-B",
                    &owner.0,
                    owner.1,
                    qcg_policy::IDEMPOTENCY_TTL
                ),
                Err(StoreReadyError::DigestConflict)
            ),
            "a committed key with a different digest must type as a conflict"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn same_digest_converges_to_the_committed_run_id() {
        // E02: committing the same digest converges at claim time onto the
        // committed mapping without a new owner. A direct commit with a
        // mismatched reserved run id fails closed instead of converging:
        // convergence with a reservation present requires proving current
        // ownership with the matching reserved run.
        let runs_dir = temp_runs_dir("converge-value");
        let key = "key-converge";
        let owner = claim_owner(&runs_dir, key, "digest", "run-A");
        let first = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner.0,
            owner.1,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("first commit should succeed");
        assert_eq!(first, "run-A");
        // A retry for the same digest converges at claim time onto run-A
        // without a new owner.
        match claim_durable_pending(
            &runs_dir,
            key,
            "digest",
            Some("run-B".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("claim should not error")
        {
            ClaimOutcome::Ready { run_id } => assert_eq!(run_id, "run-A"),
            other => panic!(
                "same digest must converge at claim, got {}",
                outcome_name(&other)
            ),
        }
        // A stale owner committing a different run id for the same digest
        // is refused instead of converging: the reservation (run-A) does
        // not match the request (run-B), so ownership fails closed (E02).
        // Convergence with a reservation happens only through the claim
        // path above or a current-owner commit with the matching run.
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-B",
            &owner.0,
            owner.1,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("mismatched reserved run must be refused, not converged");
        assert!(
            error.to_string().contains("reserved run"),
            "reserved-run mismatch must be named, got: {error}"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn absent_and_corrupt_claims_leave_no_ready_behind() {
        // E02: refusing a commit over an absent or corrupt reservation must
        // not publish a Ready record.
        let runs_dir = temp_runs_dir("refuse-clean");
        let key = "key-refuse";
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            "nobody",
            1,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("absent reservation must refuse");
        assert!(
            matches!(error, StoreReadyError::Storage(_)),
            "absent refusal must be storage-typed: {error}"
        );
        assert!(
            load_durable_ready_result(&runs_dir, key, qcg_policy::IDEMPOTENCY_TTL)
                .expect("load should not fail")
                .is_none(),
            "refused absent commit must leave no Ready"
        );
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        std::fs::write(pending_path(&runs_dir, key).as_std_path(), b"{torn")
            .expect("torn claim should be written");
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            "nobody",
            1,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("corrupt reservation must refuse");
        assert!(
            matches!(error, StoreReadyError::Storage(_)),
            "corrupt refusal must be storage-typed: {error}"
        );
        assert!(
            load_durable_ready_result(&runs_dir, key, qcg_policy::IDEMPOTENCY_TTL)
                .expect("load should not fail")
                .is_none(),
            "refused corrupt commit must leave no Ready"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    /// Claims the key and returns `(owner, generation)` for the winning
    /// owner, adopting `run_id` as the reserved run.
    fn claim_owner(runs_dir: &Utf8PathBuf, key: &str, digest: &str, run_id: &str) -> (String, u64) {
        match claim_durable_pending(
            runs_dir,
            key,
            digest,
            Some(run_id.to_string()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("claim should not error")
        {
            ClaimOutcome::Owner {
                owner, generation, ..
            } => (owner, generation),
            other => panic!("expected owner, got {}", outcome_name(&other)),
        }
    }

    #[test]
    fn committed_ready_converges_claim_without_executing() {
        // B02: a commit landing between the waiter's last look and the
        // claim must converge onto the committed run, never execute again.
        let runs_dir = temp_runs_dir("ready-converge");
        let key = "key-1";
        let owner = claim_owner(&runs_dir, key, "digest", "run-A");
        store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner.0,
            owner.1,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("ready commit should succeed");
        match claim_durable_pending(
            &runs_dir,
            key,
            "digest",
            Some("run-B".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("claim should not error")
        {
            ClaimOutcome::Ready { run_id } => assert_eq!(run_id, "run-A"),
            other => panic!("committed key must converge, got {}", outcome_name(&other)),
        }
        match claim_durable_pending(
            &runs_dir,
            key,
            "other-digest",
            Some("run-C".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("claim should not error")
        {
            ClaimOutcome::Conflict => {}
            other => panic!("foreign digest must conflict, got {}", outcome_name(&other)),
        }
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn foreign_release_is_an_idempotent_noop() {
        // E02: releasing a claim owned by someone else must succeed
        // without touching the file; only an owner+generation match
        // deletes.
        let runs_dir = temp_runs_dir("foreign-release");
        let key = "key-1";
        let (owner, generation) = claim_owner(&runs_dir, key, "digest", "run-1");
        release_durable_pending(&runs_dir, key, "someone-else", generation)
            .expect("foreign release should succeed");
        assert!(
            pending_path(&runs_dir, key).exists(),
            "a foreign release must not delete the live claim"
        );
        release_durable_pending(&runs_dir, key, &owner, generation)
            .expect("own release should succeed");
        assert!(
            !pending_path(&runs_dir, key).exists(),
            "a matching release must delete the claim"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn stale_claim_tmps_reap_by_age_only() {
        // E02: only abandoned publisher temps are reaped; a fresh temp
        // from a live publisher is never touched. Reclamation is periodic,
        // so the test forces the interval gate open first.
        use std::time::{Duration, SystemTime};
        reset_temp_sweep_for_tests();
        let runs_dir = temp_runs_dir("claim-tmp-reap");
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        let stale = idempotency_dir(&runs_dir).join("abc.tmp-old");
        let fresh = idempotency_dir(&runs_dir).join("abc.tmp-new");
        std::fs::write(stale.as_std_path(), b"{}").expect("stale temp should be written");
        std::fs::write(fresh.as_std_path(), b"{}").expect("fresh temp should be written");
        let aged = SystemTime::now()
            .checked_sub(Duration::from_secs(3600))
            .expect("aged time should exist");
        std::fs::File::options()
            .write(true)
            .open(stale.as_std_path())
            .expect("stale temp should open")
            .set_modified(aged)
            .expect("stale temp should age");
        reap_stale_claim_tmps(&runs_dir).expect("temp sweep should succeed");
        assert!(!stale.exists(), "an abandoned temp must be reaped");
        assert!(fresh.exists(), "a fresh temp must survive the reap");
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
        let first = claim_durable_pending(
            &runs_dir,
            key,
            "digest",
            Some("run-1".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
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
        let second = claim_durable_pending(
            &runs_dir,
            key,
            "digest",
            Some("run-9".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
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
            created_at_unix: now_unix().expect("test clock should be after the Unix epoch"),
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

    #[test]
    fn stale_owner_commit_after_successor_failure_is_refused() {
        // E02 acceptance: old claim -> successor fails/releases -> new claim -> old commit refused.
        let runs_dir = temp_runs_dir("stale-commit");
        let key = "key-stale";
        let (old_owner, old_gen) = claim_owner(&runs_dir, key, "digest", "run-old");
        // Successor path: old owner fails and releases, new owner claims.
        release_durable_pending(&runs_dir, key, &old_owner, old_gen)
            .expect("release should succeed");
        let (new_owner, new_gen) = claim_owner(&runs_dir, key, "digest", "run-new");
        assert_ne!((new_owner.clone(), new_gen), (old_owner.clone(), old_gen));
        let err = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-old",
            &old_owner,
            old_gen,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("stale owner commit must be refused");
        assert!(
            err.to_string().contains("different claim") || err.to_string().contains("superseded"),
            "{err}"
        );
        // New owner can still commit its own run.
        store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-new",
            &new_owner,
            new_gen,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("current owner commit should succeed");
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
        let result = claim_durable_pending(
            &runs_dir,
            key,
            "digest",
            Some("run-B".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        );
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
        // B02/E02: a committer whose claim was superseded by an expiry
        // adoption must not replace the newer owner's outcome. Before any
        // Ready exists it fails; once the successor commits, the stale owner
        // is still refused (fail-closed) instead of converging: convergence
        // with a reservation requires current ownership.
        let runs_dir = temp_runs_dir("superseded-commit");
        let key = "key-1";
        let (stale_owner, gen1) = claim_owner(&runs_dir, key, "digest", "run-1");
        // Age the first claim past its TTL so the next claim adopts it.
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
        let second = claim_durable_pending(
            &runs_dir,
            key,
            "digest",
            Some("run-2".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("adoption should win");
        let (new_owner, gen2) = match second {
            ClaimOutcome::Owner {
                owner, generation, ..
            } => (owner, generation),
            other => panic!("expected adopting owner, got {}", outcome_name(&other)),
        };
        assert!(gen2 > gen1, "adoption advances the generation");
        // The stale owner commits nothing new: without a Ready record and
        // with a different claim id it fails instead of overwriting.
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-1",
            &stale_owner,
            gen1,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("superseded commit must not overwrite");
        assert!(
            matches!(error, StoreReadyError::Storage(_)),
            "supersession must type as an explicit storage failure, got: {error}"
        );
        let committed = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-1",
            &new_owner,
            gen2,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("current generation should commit");
        assert_eq!(committed, "run-1");
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-1",
            &stale_owner,
            gen1,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("stale commit must be refused even after the successor committed");
        assert!(
            matches!(error, StoreReadyError::Storage(_)),
            "stale commit must type as storage failure, got: {error}"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn digest_conflict_is_typed_and_never_overwrites() {
        // E02: the same key committed for a different request digest is a
        // typed conflict, never an overwrite of the committed record.
        let runs_dir = temp_runs_dir("digest-conflict");
        let key = "key-1";
        let (owner, generation) = claim_owner(&runs_dir, key, "digest-a", "run-1");
        let committed = store_durable_ready(
            &runs_dir,
            key,
            "digest-a",
            "run-1",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("first commit should succeed");
        assert_eq!(committed, "run-1");
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest-b",
            "run-9",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("a different digest must conflict");
        assert!(
            matches!(error, StoreReadyError::DigestConflict),
            "digest mismatch must type as a conflict, got: {error}"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn stale_owner_commit_is_refused_after_release_and_reclaim() {
        // E02: old owner stops, TTL expires, the successor adopts and then
        // fails and releases, a new chain starts at generation 1. The old
        // owner's commit must still be refused because the claim id is a
        // different one.
        let runs_dir = temp_runs_dir("stale-reclaim");
        let key = "key-1";
        let (old_owner, old_generation) = claim_owner(&runs_dir, key, "digest", "run-A");
        // Age and let a successor adopt the claim.
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
        let (successor_owner, successor_generation) =
            claim_owner(&runs_dir, key, "digest", "run-B");
        // The successor fails and releases its claim.
        release_durable_pending(&runs_dir, key, &successor_owner, successor_generation)
            .expect("successor release should succeed");
        // A fresh chain claims again; generation restarts at 1.
        let (fresh_owner, fresh_generation) = claim_owner(&runs_dir, key, "digest", "run-C");
        assert_eq!(fresh_generation, 1, "a fresh chain starts at generation 1");
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &old_owner,
            old_generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("the old owner must not commit against a different claim");
        assert!(matches!(error, StoreReadyError::Storage(_)), "{error}");
        let committed = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-C",
            &fresh_owner,
            fresh_generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("the current owner should commit");
        assert_eq!(committed, "run-C");
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn commit_requires_a_live_matching_claim() {
        // E02: Absent/Unusable/Expired/None-reservation claims and a
        // mismatched reserved run id are refused instead of publishing a new
        // Ready record.
        let runs_dir = temp_runs_dir("commit-requires-claim");
        let key = "key-1";
        let (owner, generation) = claim_owner(&runs_dir, key, "digest", "run-A");
        // A different run id than the claim reserved is refused.
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-B",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("a mismatched reserved run must be refused");
        assert!(
            error.to_string().contains("reserved run"),
            "reserved-run mismatch must be named, got: {error}"
        );
        // An absent claim is refused.
        std::fs::remove_file(pending_path(&runs_dir, key).as_std_path())
            .expect("claim should be removable");
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("an absent claim must be refused");
        assert!(
            error.to_string().contains("absent"),
            "absence must be named, got: {error}"
        );
        assert!(
            !idempotency_path(&runs_dir, key).as_std_path().exists(),
            "no Ready may be published without a claim"
        );
        // An unusable claim is refused.
        let (owner, generation) = claim_owner(&runs_dir, key, "digest", "run-A");
        std::fs::write(pending_path(&runs_dir, key).as_std_path(), b"{torn")
            .expect("corrupt claim should write");
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("an unusable claim must be refused");
        assert!(
            error.to_string().contains("unusable"),
            "unusable must be named, got: {error}"
        );
        // A claim without a reservation (None run id) is refused even when
        // the owner, generation, and digest all match: committing an
        // arbitrary run without a reservation must fail closed (E02).
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        let reservationless = DurablePendingRecord {
            key: key.into(),
            digest: "digest".into(),
            run_id: None,
            owner: "owner-none".into(),
            created_at_unix: now_unix().expect("test clock should be after the Unix epoch"),
            generation: 1,
        };
        std::fs::write(
            pending_path(&runs_dir, key).as_std_path(),
            serde_json::to_vec(&reservationless).expect("record should serialize"),
        )
        .expect("reservationless claim should be written");
        let error = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            "owner-none",
            1,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect_err("a None-reservation claim must be refused");
        assert!(
            error.to_string().contains("reserved run"),
            "None reservation must name the reserved run, got: {error}"
        );
        assert!(
            load_durable_ready_result(&runs_dir, key, qcg_policy::IDEMPOTENCY_TTL)
                .expect("load should not fail")
                .is_none(),
            "refused None-reservation commit must leave no Ready"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn durability_model_survives_an_abrupt_process_stop() {
        // Q2: the guarantee is process termination. A synced claim and
        // Ready mapping must survive an abrupt stop (no release, no
        // graceful close) and a fresh process must converge onto the same
        // run instead of starting a second one.
        let runs_dir = temp_runs_dir("durability-model");
        let key = "key-1";
        let (owner, generation) = claim_owner(&runs_dir, key, "digest", "run-A");
        store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            &owner,
            generation,
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("ready commit should succeed");
        // Simulate SIGKILL: drop every handle without releasing the claim.
        match claim_durable_pending(
            &runs_dir,
            key,
            "digest",
            Some("run-B".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("claim should not error")
        {
            ClaimOutcome::Ready { run_id } => {
                assert_eq!(run_id, "run-A", "the mapping must survive the stop");
            }
            other => panic!(
                "an abrupt stop must keep the committed mapping, got {}",
                outcome_name(&other)
            ),
        }
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[test]
    fn second_concurrent_claimant_is_peer() {
        let runs_dir = temp_runs_dir("peer");
        let key = "key-1";
        let first = claim_durable_pending(
            &runs_dir,
            key,
            "digest",
            Some("run-1".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
        .expect("first claim should win");
        assert!(matches!(first, ClaimOutcome::Owner { .. }));
        let second = claim_durable_pending(
            &runs_dir,
            key,
            "digest",
            Some("run-2".into()),
            qcg_policy::IDEMPOTENCY_TTL,
        )
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
