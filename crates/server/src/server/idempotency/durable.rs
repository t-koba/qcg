//! Cross-process pending/Ready protocol: locked atomic publication,
//! conservative reads and owner-checked mutation.
use super::idempotency_conflict;
use crate::server::error::ApiHttpError;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use service::sync_dir_entry as sync_dir;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Write as _};
use std::time::{SystemTime, UNIX_EPOCH};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableIdempotencyRecord {
    pub(crate) key: String,
    pub(crate) digest: String,
    pub(crate) run_id: String,
    pub(crate) created_at_unix: u64,
    /// Claim generation that committed this record. Required: records
    /// without a generation are corrupt, never defaulted.
    pub(crate) generation: u64,
    /// Fresh identity per publication, including when generations restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) publication_id: Option<String>,
}
impl DurableIdempotencyRecord {
    pub(crate) fn expired_at(&self, now: u64, ttl: std::time::Duration) -> bool {
        now.saturating_sub(self.created_at_unix) >= ttl.as_secs().max(1)
    }
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
/// Permanent 256 execution shards (first SHA-256 byte): never unlink/rotate.
/// Collisions serialize; upgrades require quiescing old protocol peers.
pub(crate) struct ExecutionGate {
    file: File,
    runs_dir: Utf8PathBuf,
    shard: u8,
}
impl ExecutionGate {
    pub(crate) fn try_acquire(runs_dir: &Utf8PathBuf, key: &str) -> Result<Option<Self>, String> {
        let dir = idempotency_dir(runs_dir);
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let shard = Sha256::digest(key.as_bytes())[0];
        let path = dir.join(format!(".execution-{shard:02x}.lock"));
        let file = open_no_follow(&path, true, true, true, false).map_err(|e| e.to_string())?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self {
                file,
                runs_dir: runs_dir.clone(),
                shard,
            })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(e)) => Err(e.to_string()),
        }
    }
    fn validate(&self, key: &str) -> Result<(), String> {
        if self.shard != Sha256::digest(key.as_bytes())[0] {
            return Err("wrong idempotency execution gate".into());
        }
        Ok(())
    }
}
impl Drop for ExecutionGate {
    fn drop(&mut self) {
        // Final Arc drop explicitly unlocks; fork/pre-exec descriptors must not extend ownership.
        if let Err(error) = self.file.unlock() {
            tracing::warn!(%error, "idempotency execution gate unlock failed");
        }
    }
}
// Reuse existing peer wait policy, not an additional gate timeout.
pub(super) const PEER_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
pub(super) const PEER_POLL: std::time::Duration = std::time::Duration::from_millis(100);
// Test callers exercise individual metadata operations, not request lifetimes.
#[cfg(test)]
fn test_gate(runs_dir: &Utf8PathBuf, key: &str) -> Result<ExecutionGate, String> {
    let deadline = std::time::Instant::now() + PEER_WAIT;
    loop {
        if let Some(gate) = ExecutionGate::try_acquire(runs_dir, key)? {
            return Ok(gate);
        }
        if std::time::Instant::now() >= deadline {
            return Err("idempotency gate busy".into());
        }
        std::thread::sleep(PEER_POLL);
    }
}
/// Claim, store, release and expiry share one cross-process lock.
/// A generation check and its conditional mutation must be indivisible.
fn acquire_idempotency_lock(runs_dir: &Utf8PathBuf) -> Result<File, String> {
    std::fs::create_dir_all(idempotency_dir(runs_dir).as_std_path())
        .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    let lock_path = idempotency_dir(runs_dir).join(".idempotency.lock");
    // Lock opens use the same no-follow primitive as records.
    let lock_file = open_no_follow(&lock_path, true, true, true, false)
        .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    lock_file
        .lock()
        .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    Ok(lock_file)
}
/// No-follow leaf opens relative to pinned immediate parents on Unix/Windows.
/// Ancestors of that parent, including the configured store, must stay trusted.
/// Create via O_EXCL then open existing: avoids macOS racing-create ENOENT.
/// Bounded retry on deletion churn; links and persistent churn fail closed.
fn open_no_follow(
    path: &camino::Utf8Path,
    read: bool,
    write: bool,
    create: bool,
    create_new: bool,
) -> io::Result<File> {
    if create && !create_new {
        return open_no_follow_or_create(path, read, write);
    }
    open_no_follow_once(path, read, write, create, create_new)
}
fn open_no_follow_or_create(path: &camino::Utf8Path, read: bool, write: bool) -> io::Result<File> {
    let mut last_error = io::Error::from(io::ErrorKind::NotFound);
    for _ in 0..8 {
        let opened = match open_no_follow_once(path, read, write, true, true) {
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                open_no_follow_once(path, read, write, false, false)
            }
            result => result,
        };
        match opened {
            // Deleted during either creation or the existing-file open: retry.
            Err(error) if error.kind() == io::ErrorKind::NotFound => last_error = error,
            result => return result,
        }
    }
    Err(last_error)
}
fn file_parts(path: &camino::Utf8Path) -> io::Result<(&camino::Utf8Path, &str)> {
    match (path.parent(), path.file_name()) {
        (Some(dir), Some(name))
            if !["", ".", ".."].contains(&name) && !name.contains(['\0', '/', '\\', ':']) =>
        {
            Ok((dir, name))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsafe idempotency path",
        )),
    }
}
#[cfg(unix)]
fn pin_parent(dir: &camino::Utf8Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(dir)
}
fn open_no_follow_once(
    path: &camino::Utf8Path,
    read: bool,
    write: bool,
    create: bool,
    create_new: bool,
) -> io::Result<File> {
    if !read && !write {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "open requires read or write",
        ));
    }
    let (dir, name) = file_parts(path)?;
    #[cfg(unix)]
    let file = {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        let parent = pin_parent(dir)?;
        let name = std::ffi::CString::new(name)?;
        let mut flags = libc::O_CLOEXEC | libc::O_NOFOLLOW;
        flags |= match (read, write) {
            (true, true) => libc::O_RDWR,
            (true, false) => libc::O_RDONLY | libc::O_NONBLOCK,
            _ => libc::O_WRONLY,
        };
        if create || create_new {
            flags |= libc::O_CREAT;
        }
        if create_new {
            flags |= libc::O_EXCL;
        }
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                flags,
                0o644 as libc::c_uint,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // Own the descriptor before any fallible handle inspection.
        unsafe { File::from_raw_fd(fd) }
    };
    #[cfg(windows)]
    let file = {
        use windows_sys::Wdk::Storage::FileSystem::{FILE_CREATE, FILE_OPEN, FILE_OPEN_IF};
        use windows_sys::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_GENERIC_WRITE};
        let access =
            if read { FILE_GENERIC_READ } else { 0 } | if write { FILE_GENERIC_WRITE } else { 0 };
        let disposition = match (create_new, create) {
            (true, _) => FILE_CREATE,
            (_, true) => FILE_OPEN_IF,
            _ => FILE_OPEN,
        };
        windows_io::open_at(&windows_io::pin_parent(dir)?, name, access, disposition)?
    };
    #[cfg(not(any(unix, windows)))]
    let file = {
        // Unsupported platforms retain the best-effort path check, not a race-safe guarantee.
        let _ = (dir, name);
        let file = std::fs::OpenOptions::new()
            .read(read)
            .write(write)
            .create(create)
            .create_new(create_new)
            .truncate(false)
            .open(path)?;
        if !std::fs::symlink_metadata(path)?.is_file() {
            return Err(io::Error::other("idempotency path is not a regular file"));
        }
        file
    };
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("idempotency handle is not a regular file"));
    }
    Ok(file)
}
#[cfg(windows)]
mod windows_io {
    use super::{File, io};
    use std::os::windows::{
        fs::{MetadataExt as _, OpenOptionsExt as _},
        io::{AsRawHandle as _, FromRawHandle as _},
    };
    use std::{mem, ptr};
    use windows_sys::{
        Wdk::{Foundation::OBJECT_ATTRIBUTES, Storage::FileSystem::*},
        Win32::{
            Foundation::{OBJ_CASE_INSENSITIVE, RtlNtStatusToDosError, UNICODE_STRING},
            Storage::FileSystem::*,
            System::IO::IO_STATUS_BLOCK,
        },
    };
    pub(super) fn status(status: i32) -> io::Result<()> {
        if status < 0 {
            Err(io::Error::from_raw_os_error(
                unsafe { RtlNtStatusToDosError(status) } as i32,
            ))
        } else {
            Ok(())
        }
    }
    pub(super) fn pin_parent(dir: &camino::Utf8Path) -> io::Result<File> {
        let file = std::fs::OpenOptions::new()
            .access_mode(FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE) // Synchronous parent anchor, not target access.
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(dir)?;
        let meta = file.metadata()?;
        if !meta.is_dir() || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(io::Error::other("parent is not a plain directory"));
        }
        Ok(file)
    }
    pub(super) fn open_at(dir: &File, name: &str, access: u32, creation: u32) -> io::Result<File> {
        let mut name: Vec<u16> = name.encode_utf16().collect();
        let bytes =
            u16::try_from(name.len() * 2).map_err(|_| io::Error::other("NT name too long"))?;
        let mut name = UNICODE_STRING {
            Length: bytes,
            MaximumLength: bytes,
            Buffer: name.as_mut_ptr(),
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: dir.as_raw_handle(),
            ObjectName: &mut name,
            Attributes: OBJ_CASE_INSENSITIVE,
            ..Default::default()
        };
        let mut handle = ptr::null_mut();
        // A single validated component relative to the verified directory; never follow a leaf reparse point.
        status(unsafe {
            NtCreateFile(
                &mut handle,
                access | SYNCHRONIZE | FILE_READ_ATTRIBUTES,
                &attributes,
                &mut IO_STATUS_BLOCK::default(),
                ptr::null(),
                FILE_ATTRIBUTE_NORMAL,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                creation,
                FILE_OPEN_REPARSE_POINT | FILE_NON_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
                ptr::null(),
                0,
            )
        })?;
        // SAFETY: successful synchronous NtCreateFile transferred one owned, non-inheritable handle.
        let file = unsafe { File::from_raw_handle(handle) };
        let meta = file.metadata()?;
        if !meta.is_file() || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(io::Error::other("idempotency leaf is not a plain file"));
        }
        Ok(file)
    }
    pub(super) fn rename_at(file: &File, dir: &File, name: &str) -> io::Result<()> {
        // NtSetInformationFile/FileRenameInformation documents RootDirectory for relative names:
        // https://learn.microsoft.com/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information
        let name: Vec<u16> = name.encode_utf16().collect();
        let offset = mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
        let len = (offset + name.len() * 2).max(mem::size_of::<FILE_RENAME_INFORMATION>());
        // Pointer-aligned storage for the native variable-length structure, including its trailing UTF-16 name.
        let mut storage = vec![0usize; len.div_ceil(mem::size_of::<usize>())];
        let info = storage.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
        unsafe {
            (*info).Anonymous.ReplaceIfExists = true;
            (*info).RootDirectory = dir.as_raw_handle();
            (*info).FileNameLength = (name.len() * 2) as u32;
            ptr::copy_nonoverlapping(
                name.as_ptr(),
                ptr::addr_of_mut!((*info).FileName).cast::<u16>(),
                name.len(),
            );
            status(NtSetInformationFile(
                file.as_raw_handle(),
                &mut IO_STATUS_BLOCK::default(),
                info.cast(),
                len as u32,
                FileRenameInformation,
            ))
        }
    }
}
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
/// Locked snapshot expiry returns its generation floor (absent: zero),
/// preventing an older owner from restarting the chain. Readers never mutate.
fn reap_expired_ready_snapshot(
    runs_dir: &Utf8PathBuf,
    key: &str,
    ready_bytes: Option<&[u8]>,
    ttl: std::time::Duration,
) -> Result<(Option<DurableIdempotencyRecord>, u64), String> {
    let Some(bytes) = ready_bytes else {
        return Ok((None, 0));
    };
    let record = parse_ready(key, bytes)?;
    let generation = record.generation;
    if record.expired_at(now_unix()?, ttl) {
        std::fs::remove_file(idempotency_path(runs_dir, key).as_std_path())
            .map_err(|error| error.to_string())?;
        return Ok((None, generation));
    }
    Ok((Some(record), generation))
}
/// One past the highest parsed generation; absent counts as zero. Corrupt
/// records fail before reaching this computation.
fn pending_generation_from_parsed(pending_generation: u64, ready_generation: u64) -> u64 {
    pending_generation.max(ready_generation).saturating_add(1)
}
/// Rename relative to verified immediate parents, not to mutable leaf paths.
/// As with open_no_follow, ancestors above those parents are a trusted boundary.
#[cfg(unix)]
fn rename_verified(src: &camino::Utf8Path, dst: &camino::Utf8Path) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    let (sdir, sname) = file_parts(src)?;
    let (ddir, dname) = file_parts(dst)?;
    let sname = std::ffi::CString::new(sname)?;
    let dname = std::ffi::CString::new(dname)?;
    let sdir = pin_parent(sdir)?;
    let ddir = pin_parent(ddir)?; // RAII also closes sdir if this open fails.
    let (sfd, dfd) = (sdir.as_raw_fd(), ddir.as_raw_fd());
    let rc = unsafe { libc::renameat(sfd, sname.as_ptr(), dfd, dname.as_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(windows)]
fn rename_verified(src: &camino::Utf8Path, dst: &camino::Utf8Path) -> io::Result<()> {
    use windows_sys::{Wdk::Storage::FileSystem::FILE_OPEN, Win32::Storage::FileSystem::DELETE};
    let (sdir, sname) = file_parts(src)?;
    let (ddir, dname) = file_parts(dst)?;
    let source = windows_io::open_at(&windows_io::pin_parent(sdir)?, sname, DELETE, FILE_OPEN)?;
    windows_io::rename_at(&source, &windows_io::pin_parent(ddir)?, dname)
}
#[cfg(not(any(unix, windows)))]
fn rename_verified(src: &camino::Utf8Path, dst: &camino::Utf8Path) -> io::Result<()> {
    // Best effort only on unsupported platforms: pathname rename is not swap-safe.
    std::fs::rename(src.as_std_path(), dst.as_std_path())
}
fn acquire_maintenance_lock(
    runs_dir: &Utf8PathBuf,
    stop: &tokio_util::sync::CancellationToken,
) -> Result<File, String> {
    std::fs::create_dir_all(idempotency_dir(runs_dir).as_std_path())
        .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    let lock_path = idempotency_dir(runs_dir).join(".idempotency.lock");
    let lock_file = open_no_follow(&lock_path, true, true, true, false)
        .map_err(|error| format!("failed to persist idempotency record: {error}"))?;
    loop {
        match lock_file.try_lock() {
            Ok(()) => return Ok(lock_file),
            Err(std::fs::TryLockError::WouldBlock) if !stop.is_cancelled() => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err("idempotency maintenance cancelled".into());
            }
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(format!("failed to persist idempotency record: {error}"));
            }
        }
    }
}
/// Pending lasts 60s (30s waiter reports live as 503, adopts expired); Ready uses frozen TTL.
const PENDING_TTL_SECS: u64 = 60;
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurablePendingRecord {
    key: String,
    digest: String,
    /// Adoption retains the reserved run across the execution/Ready crash gap.
    run_id: Option<String>,
    /// Missing only in legacy claims, which cannot be adopted after expiry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    operation_id: Option<String>,
    owner: String,
    created_at_unix: u64,
    /// Required generation; a superseded committer cannot replace a successor.
    generation: u64,
}
/// Claim before execution. Live equal-digest peers wait; other digests
/// conflict. Expired equal-digest claims adopt the predecessor's run id.
#[cfg(test)]
pub(crate) fn claim_durable_pending(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
    reserved_run_id: Option<String>,
    ttl: std::time::Duration,
    _sweep: &IdempotencySweep,
) -> Result<ClaimOutcome, ApiHttpError> {
    let gate = test_gate(runs_dir, key).map_err(ApiHttpError::internal)?;
    claim_durable_pending_gated(&gate, key, digest, reserved_run_id, ttl, _sweep)
}
#[cfg(test)]
pub(crate) fn claim_durable_pending_gated(
    gate: &ExecutionGate,
    key: &str,
    digest: &str,
    reserved_run_id: Option<String>,
    ttl: std::time::Duration,
    _sweep: &IdempotencySweep,
) -> Result<ClaimOutcome, ApiHttpError> {
    claim_durable_pending_for_target_gated(gate, key, digest, reserved_run_id, ttl, _sweep, false)
}
pub(crate) fn claim_durable_pending_for_target_gated(
    gate: &ExecutionGate,
    key: &str,
    digest: &str,
    reserved_run_id: Option<String>,
    ttl: std::time::Duration,
    _sweep: &IdempotencySweep,
    require_target: bool,
) -> Result<ClaimOutcome, ApiHttpError> {
    let runs_dir = &gate.runs_dir;
    gate.validate(key).map_err(ApiHttpError::internal)?;
    // Do not wait for maintenance while holding the key shard. GC snapshots
    // associations under this lease, then releases metadata before run locks.
    let _maintenance = service::try_lock_store_maintenance(runs_dir)
        .map_err(|e| ApiHttpError::internal(e.to_string()))?
        .ok_or_else(|| {
            ApiHttpError::service_unavailable(
                "run store maintenance is busy; retry with the same key",
            )
        })?;
    // Temp+rename is atomic but cannot arbitrate owners; hold the shared lock.
    let _lock_held = acquire_idempotency_lock(runs_dir).map_err(ApiHttpError::internal)?;
    // One snapshot orders Ready convergence before any pending mutation.
    let snapshot = read_key_files_snapshot(runs_dir, key).map_err(ApiHttpError::internal)?;
    // Parse each file once, retaining the expired Ready generation floor.
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
        return Ok(ClaimOutcome::Ready(committed));
    }
    // A live predecessor is never overwritten. An expired predecessor stays
    // until atomic replacement, retaining its run association across a crash
    // during adoption. Its generation still floors the successor.
    let mut adopted: Option<String> = None;
    let mut operation_id = uuid::Uuid::now_v7().to_string();
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
            let accepted_id = record.operation_id.as_deref().ok_or_else(|| {
                ApiHttpError::internal("expired legacy idempotency claim has no operation identity; quiescence and reconciliation required")
            })?;
            if !uuid::Uuid::parse_str(accepted_id).is_ok_and(|id| id.to_string() == accepted_id) {
                return Err(ApiHttpError::internal(
                    "invalid idempotency operation identity",
                ));
            }
            let generation = record.generation;
            if record.digest == digest {
                operation_id = accepted_id.to_string();
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
    if require_target {
        let id = run_id
            .as_deref()
            .filter(|id| policy::is_safe_path_component(id))
            .ok_or_else(|| ApiHttpError::internal("mutation claim lacks target"))?;
        let run_dir = runs_dir.join(id);
        service::require_run_directory(&run_dir).map_err(ApiHttpError::from_api)?;
        let journal = run_dir.join("meta/journal.jsonl");
        // Revalidate under maintenance: a target deleted after route preflight
        // must not acquire a dangling Pending association. No run lock needed.
        open_no_follow(&journal, true, false, false, false).map_err(|e| {
            ApiHttpError::from_api(if e.kind() == io::ErrorKind::NotFound {
                api::ApiError::not_found(format!("run `{id}` was not found"))
            } else {
                api::ApiError::internal(e.to_string())
            })
        })?;
    }
    let owner = uuid::Uuid::now_v7().to_string();
    // Expired records still floor the generation; absence floors at zero.
    let generation = pending_generation_from_parsed(pending_generation, ready_generation);
    let record = DurablePendingRecord {
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.clone(),
        operation_id: Some(operation_id.clone()),
        owner: owner.clone(),
        created_at_unix: now_unix().map_err(ApiHttpError::internal)?,
        generation,
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| {
        ApiHttpError::internal(format!("failed to persist idempotency record: {error}"))
    })?;
    // Stage separately so a crash leaves the predecessor intact. Collisions
    // get fresh names; repeated collisions fail closed. Expiry is resident-owned.
    let mut attempts = 0;
    let tmp = loop {
        if attempts >= 3 {
            return Err(ApiHttpError::internal(
                "failed to persist idempotency record: claim temp collided repeatedly; refusing to overwrite".to_string(),
            ));
        }
        attempts += 1;
        let tmp = expiry::temp_dir(runs_dir)
            .map_err(ApiHttpError::internal)?
            .join(
                path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()))
                    .file_name()
                    .unwrap(),
            );
        match stage_pending(&tmp, &bytes) {
            Ok(()) => break tmp,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                tracing::warn!(path = %tmp, "claim staging collision; retrying with a fresh id");
                // Retry with a fresh id; never overwrite a staging collision.
                continue;
            }
            Err(error) => {
                return Err(ApiHttpError::internal(format!(
                    "failed to persist idempotency record: {error}"
                )));
            }
        }
    };
    // Only failed rename leaves a temp. A subsequent directory-sync failure
    // is a persistence error, not evidence of a leftover temp.
    if let Err(error) = rename_verified(&tmp, &path) {
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
            // The locked predecessor is expired/absent; live claims returned Peer.
            Ok(ClaimOutcome::Owner {
                operation_id,
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
/// Stage with exclusive no-follow creation; until rename the
/// predecessor remains authoritative.
fn stage_pending(tmp: &camino::Utf8Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = open_no_follow(tmp, false, true, false, true)?;
    file.write_all(bytes)?;
    file.sync_all()
}
mod expiry;
pub(crate) use expiry::IdempotencySweep;
fn canonical_key_hash(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn generated_temp(name: &str) -> bool {
    let Some((stem, suffix)) = name.split_once(".tmp-") else {
        return false;
    };
    canonical_key_hash(stem.strip_suffix(".pending").unwrap_or(stem))
        && uuid::Uuid::parse_str(suffix).is_ok_and(|id| {
            id.get_version_num() == 7
                && id.get_variant() == uuid::Variant::RFC4122
                && id.to_string() == suffix
        })
}
fn clean_temp(entry: std::fs::DirEntry, now: u64) -> Result<(), String> {
    let name = entry.file_name();
    let Some(name) = name.to_str() else {
        return Ok(());
    };
    let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|_| "non-UTF8 temp path")?;
    if generated_temp(name) {
        let meta = std::fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
        // Preserve the existing rule: unlink exact staging symlinks, never
        // follow them. Regular staging files require verified stale mtime.
        if !meta.file_type().is_symlink() {
            if !meta.is_file() {
                return Ok(());
            }
            let modified = meta
                .modified()
                .map_err(|e| e.to_string())?
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_secs();
            if now.saturating_sub(modified) < PENDING_TTL_SECS {
                return Ok(());
            }
        }
    } else {
        // In particular, never reclaim pending claims, even expired ones.
        return Ok(());
    }
    std::fs::remove_file(&path).map_err(|e| e.to_string())?;
    sync_dir(&path.parent().unwrap().to_path_buf()).map_err(|e| e.to_string())
}
#[derive(Debug)]
pub(crate) enum ClaimOutcome {
    Owner {
        operation_id: String,
        run_id: Option<String>,
        owner: String,
        generation: u64,
    },
    Peer,
    /// Locked Ready convergence catches commits racing the last waiter read.
    Ready(DurableIdempotencyRecord),
    /// The key is committed for a different request digest.
    Conflict,
}
/// Release only an exact owner+generation under the shared lock. Lock/read
/// failures retain evidence; a stale reader must never delete its successor.
#[cfg(test)]
pub(crate) fn release_durable_pending(
    runs_dir: &Utf8PathBuf,
    key: &str,
    owner: &str,
    generation: u64,
) -> Result<(), String> {
    let gate = test_gate(runs_dir, key)?;
    release_durable_pending_gated(&gate, key, owner, generation)
}
pub(crate) fn release_durable_pending_gated(
    gate: &ExecutionGate,
    key: &str,
    owner: &str,
    generation: u64,
) -> Result<(), String> {
    let runs_dir = &gate.runs_dir;
    gate.validate(key)?;
    let _lock_held = acquire_idempotency_lock(runs_dir)?;
    let path = pending_path(runs_dir, key);
    // Absence is released/adopted; unreadable evidence must not be hidden.
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
    // A foreign-key file must survive for operator reconciliation.
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
    /// Successor ownership: re-enter claim rather than execute an orphan.
    Superseded,
    /// Absent claim: re-enter claim as for a superseded owner.
    Absent,
}
/// Recheck owner, generation, digest and key under the shared lock before
/// execution. The caller holds this key's execution gate, so wall-clock
/// expiry alone cannot supersede the exact holder: a successor must acquire
/// the gate to replace the claim. A descheduled, adopted owner still cannot
/// commit and must not start a stale reservation. Unreadable/foreign claims
/// fail closed. Expiry remains takeover authority only for a successor that
/// acquires the gate after release (claim adoption).
#[cfg(test)]
pub(crate) fn check_pending_owner(
    runs_dir: &Utf8PathBuf,
    key: &str,
    owner: &str,
    generation: u64,
    expected_digest: &str,
) -> Result<OwnerCheck, String> {
    let gate = test_gate(runs_dir, key)?;
    check_pending_owner_gated(&gate, key, owner, generation, expected_digest)
}
pub(crate) fn check_pending_owner_gated(
    gate: &ExecutionGate,
    key: &str,
    owner: &str,
    generation: u64,
    expected_digest: &str,
) -> Result<OwnerCheck, String> {
    let runs_dir = &gate.runs_dir;
    gate.validate(key)?;
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
    // A matching identity stays current while gated, even past the pending lease.
    match parse_pending_bytes(key, &bytes) {
        PendingRead::Valid(current) | PendingRead::Expired(current) => {
            if current.owner == owner
                && current.generation == generation
                && current.digest == expected_digest
            {
                Ok(OwnerCheck::Current)
            } else {
                Ok(OwnerCheck::Superseded)
            }
        }
        PendingRead::Absent => Ok(OwnerCheck::Absent),
        PendingRead::Unusable(detail) => Err(detail),
    }
}
enum PendingRead {
    Valid(DurablePendingRecord),
    /// Expired claim whose run id a same-digest retry may adopt.
    Expired(DurablePendingRecord),
    Absent,
    /// Unusable evidence: never overwrite or wait over potentially live ownership.
    Unusable(String),
}
/// Bound record reads: oversize content is foreign damage, never usable state.
use service::MAX_IDEMPOTENCY_FILE_BYTES;
/// Absence is reported explicitly so a file vanishing mid-check (a
/// concurrent release) keeps its historical meaning instead of failing.
enum BoundedRead {
    Present(Vec<u8>),
    Absent,
    Unreadable(String),
}
fn read_bounded_idempotency_file(path: &camino::Utf8Path) -> BoundedRead {
    use io::Read as _;
    // No pre-open probe: planted links, non-regular files and FIFOs fail
    // closed through the shared no-follow, nonblocking open.
    let file = match open_no_follow(path, true, false, false, false) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
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
/// Retire only evidence positively resolved by this Ready. The caller holds
/// the metadata lock; publication also checks the exact claim owner. Expiry
/// has no owner in Ready, so it matches the complete recorded operation identity.
/// Ambiguous evidence is retained regardless of age.
fn retire_ready_pending(
    runs_dir: &Utf8PathBuf,
    ready: &DurableIdempotencyRecord,
    owner: Option<&str>,
) -> Result<(), String> {
    let path = pending_path(runs_dir, &ready.key);
    let bytes = match read_bounded_idempotency_file(&path) {
        // Retry the directory sync if a prior retirement unlinked Pending
        // but failed to sync, before expiry is allowed to remove Ready.
        BoundedRead::Absent => {
            return sync_dir(&idempotency_dir(runs_dir)).map_err(|error| error.to_string());
        }
        BoundedRead::Present(bytes) => bytes,
        BoundedRead::Unreadable(error) => {
            tracing::warn!(%error, "unreadable pending retained after Ready resolution");
            return Ok(());
        }
    };
    let pending: DurablePendingRecord = match serde_json::from_slice(&bytes) {
        Ok(pending) => pending,
        Err(error) => {
            tracing::warn!(%error, "corrupt pending retained after Ready resolution");
            return Ok(());
        }
    };
    if pending.key != ready.key
        || pending.digest != ready.digest
        || pending.run_id.as_deref() != Some(ready.run_id.as_str())
        || pending.generation != ready.generation
        || owner.is_some_and(|owner| pending.owner != owner)
    {
        tracing::warn!("mismatched pending retained after Ready resolution");
        return Ok(());
    }
    std::fs::remove_file(&path).map_err(|error| error.to_string())?;
    sync_dir(&idempotency_dir(runs_dir)).map_err(|error| error.to_string())
}
/// Read-only digest lookup used before package preflight: an exact retry of a
/// live pending start must join its owner even if the source package changed.
pub(crate) fn load_durable_pending_digest_result(
    runs_dir: &Utf8PathBuf,
    key: &str,
) -> Result<Option<String>, String> {
    match read_durable_pending(runs_dir, key) {
        PendingRead::Valid(record) => Ok(Some(record.digest)),
        PendingRead::Expired(_) | PendingRead::Absent => Ok(None),
        PendingRead::Unusable(detail) => Err(detail),
    }
}
/// Shared pending classifier for claim snapshots and waiter observations.
fn parse_pending_bytes(key: &str, bytes: &[u8]) -> PendingRead {
    let record: DurablePendingRecord = match serde_json::from_slice(bytes) {
        Ok(record) => record,
        // Atomic publishers cannot tear reads; corruption must never become Absent.
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
    // Read-only: preserve the claim another caller may be about to adopt.
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
/// A pre-epoch clock fails closed; guessed time could adopt a live claim.
pub(crate) fn now_unix() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| "system clock is before the Unix epoch; refusing idempotency decision".into())
}
/// Read-only: absent/expired is None, but corruption and I/O failures are
/// errors, never permission to execute twice. Mutation requires the lock.
pub(crate) fn load_durable_ready_result(
    runs_dir: &Utf8PathBuf,
    key: &str,
    ttl: std::time::Duration,
) -> Result<Option<DurableIdempotencyRecord>, String> {
    load_durable_ready_at(runs_dir, key, ttl, now_unix)
}
fn load_durable_ready_at(
    runs_dir: &Utf8PathBuf,
    key: &str,
    ttl: std::time::Duration,
    now: impl FnOnce() -> Result<u64, String>,
) -> Result<Option<DurableIdempotencyRecord>, String> {
    let Some(record) = read_ready(runs_dir, key)? else {
        return Ok(None);
    };
    Ok((!record.expired_at(now()?, ttl)).then_some(record))
}
pub(super) fn read_ready(
    runs_dir: &Utf8PathBuf,
    key: &str,
) -> Result<Option<DurableIdempotencyRecord>, String> {
    let path = idempotency_path(runs_dir, key);
    let bytes = match read_bounded_idempotency_file(&path) {
        BoundedRead::Present(bytes) => bytes,
        BoundedRead::Absent => return Ok(None),
        BoundedRead::Unreadable(error) => return Err(error),
    };
    parse_ready(key, &bytes).map(Some)
}
fn parse_ready(key: &str, bytes: &[u8]) -> Result<DurableIdempotencyRecord, String> {
    if bytes.is_empty() {
        // An empty published record is unusable, never assumed absent.
        return Err("idempotency record is empty; writer may be in progress".into());
    }
    let record: DurableIdempotencyRecord =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    if record.key != key {
        return Err("idempotency record key mismatch".into());
    }
    Ok(record)
}

#[derive(Debug)]
pub(crate) enum WaitOutcome {
    Ready(DurableIdempotencyRecord),
    /// Retry claim with the orphan run id, even if the claim later vanishes.
    RetryClaim {
        adopted_run_id: Option<String>,
    },
}
/// Wait for equal-digest Ready; conflict on other digests. Retry vanished/
/// expired pending, but report 503 for live claims held past the deadline.
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
        tokio::time::Instant::now() + PEER_WAIT,
    )
    .await
}
/// Inject a short deadline in tests; production uses 30s.
async fn wait_for_peer_ready_with_deadline(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
    ttl: std::time::Duration,
    deadline: tokio::time::Instant,
) -> Result<WaitOutcome, ApiHttpError> {
    loop {
        // Keep blocking I/O off Tokio. These reads are sequential, not atomic:
        // re-read Ready before any pending-derived return to catch a racing commit.
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
                return Ok(WaitOutcome::Ready(record));
            }
            Ok(None) => {}
            Err(detail) => {
                // Atomic publication makes corrupt bytes real damage: fail immediately.
                return Err(ApiHttpError::internal(format!(
                    "failed to load idempotency record: {detail}"
                )));
            }
        }
        // Catch commits between sequential reads before retry/deadline decisions.
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
                    return Ok(WaitOutcome::Ready(record));
                }
                return Ok(WaitOutcome::RetryClaim {
                    adopted_run_id: pending.run_id.clone(),
                });
            }
            PendingRead::Absent => {
                // Catch a commit racing the initial read before retrying.
                if let Some(record) = recheck_ready(runs_dir, key).await? {
                    if record.digest != digest {
                        return Err(idempotency_conflict());
                    }
                    return Ok(WaitOutcome::Ready(record));
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
            // Recheck Ready at deadline before reporting a live-claim 503.
            if let Some(record) = recheck_ready(runs_dir, key).await? {
                if record.digest != digest {
                    return Err(idempotency_conflict());
                }
                return Ok(WaitOutcome::Ready(record));
            }
            return Err(ApiHttpError::service_unavailable(
                "idempotent request is still in progress elsewhere; retry with the same key",
            ));
        }
        tokio::time::sleep(PEER_POLL).await;
    }
}
/// Typed refusal: digest conflict is 409; storage errors never advertise Ready.
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
/// Publish only for a matching owner/generation/digest/reserved-run claim
/// observed while holding this key's execution gate. Wall-clock expiry alone
/// does not refuse the exact holder: a successor must acquire the gate to
/// replace the claim, so an aged exact claim may still commit. Mismatched or
/// foreign identities are still refused, and expiry remains takeover
/// authority for a successor that acquires the gate after release. Equal-
/// digest Ready converges without renewing its timestamp, but only for a
/// matching claim or Absent replay; unusable pending evidence is refused.
#[cfg(test)]
pub(crate) fn store_durable_ready(
    runs_dir: &Utf8PathBuf,
    key: &str,
    digest: &str,
    run_id: &str,
    owner: &str,
    generation: u64,
    ttl: std::time::Duration,
) -> Result<DurableIdempotencyRecord, StoreReadyError> {
    let gate = test_gate(runs_dir, key)?;
    store_durable_ready_gated(&gate, key, digest, run_id, owner, generation, ttl)
}
pub(crate) fn store_durable_ready_gated(
    gate: &ExecutionGate,
    key: &str,
    digest: &str,
    run_id: &str,
    owner: &str,
    generation: u64,
    ttl: std::time::Duration,
) -> Result<DurableIdempotencyRecord, StoreReadyError> {
    let runs_dir = &gate.runs_dir;
    gate.validate(key)?;
    std::fs::create_dir_all(idempotency_dir(runs_dir).as_std_path())
        .map_err(|error| error.to_string())?;
    let path = idempotency_path(runs_dir, key);
    // Temp+rename is atomic but cannot arbitrate owners; hold the shared lock.
    let _lock_held = acquire_idempotency_lock(runs_dir)?;
    // Ready convergence and pending ownership use one locked snapshot.
    let snapshot = read_key_files_snapshot(runs_dir, key)?;
    // Parse pending once for both convergence and publication.
    let pending_read = snapshot
        .pending
        .as_deref()
        .map(|bytes| parse_pending_bytes(key, bytes))
        .unwrap_or(PendingRead::Absent);
    // Gate-aware ownership: the exact holder may commit even after the
    // wall-clock timestamp, because no gate-aware successor could have
    // replaced the claim while this gate is held. Mismatched identities
    // are still refused; expiry remains takeover authority after release.
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
        // Absent means replay after release; Unusable is positive damage
        // evidence. A matching claim converges even after its wall-clock
        // timestamp while this gate is held; mismatched identities are
        // refused by the ownership check below.
        match &pending_read {
            PendingRead::Valid(pending) | PendingRead::Expired(pending) => {
                check_live_ownership(pending)?;
                if existing.run_id == run_id
                    && existing.generation == generation
                    && let Err(error) = retire_ready_pending(runs_dir, &existing, Some(owner))
                {
                    tracing::warn!(%error, "Ready committed; pending retirement incomplete");
                }
                return Ok(existing);
            }
            PendingRead::Absent => {
                return Ok(existing);
            }
            PendingRead::Unusable(detail) => {
                return Err(StoreReadyError::Storage(format!(
                    "idempotency claim is unusable: {detail}"
                )));
            }
        }
    }
    // Claim UUID protects even chains that restart at generation one.
    // Ownership checks remain locked through publication; the exact holder
    // may publish after its wall-clock timestamp while this gate is held.
    match &pending_read {
        PendingRead::Valid(pending) | PendingRead::Expired(pending) => {
            check_live_ownership(pending)?;
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
        publication_id: Some(uuid::Uuid::now_v7().to_string()),
        key: key.to_string(),
        digest: digest.to_string(),
        run_id: run_id.to_string(),
        created_at_unix: now_unix()?,
        generation,
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
    expiry::register(runs_dir, &bytes)?;
    // Retry unique staging collisions boundedly; never overwrite a temp.
    let mut attempts = 0;
    let tmp = loop {
        if attempts >= 3 {
            return Err(StoreReadyError::Storage(
                "idempotency ready temp collided repeatedly; refusing to overwrite".into(),
            ));
        }
        attempts += 1;
        let tmp = expiry::temp_dir(runs_dir)?.join(
            path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()))
                .file_name()
                .unwrap(),
        );
        {
            // No-follow leaf opens relative to pinned immediate parents on Unix/Windows.
            // Ancestors of that parent, including the configured store, must stay trusted.
            // Create via O_EXCL then open existing: avoids macOS racing-create ENOENT.
            // Bounded retry on deletion churn; links and persistent churn fail closed.
            let open_result = open_no_follow(&tmp, false, true, false, true);
            match open_result {
                Ok(mut file) => {
                    if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
                        let _ = std::fs::remove_file(tmp.as_std_path());
                        return Err(error.to_string().into());
                    }
                    break tmp;
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    tracing::warn!(path = %tmp, "ready staging collision; retrying with a fresh id");
                    continue;
                }
                Err(error) => return Err(error.to_string().into()),
            }
        }
    };
    if let Err(error) = rename_verified(&tmp, &path) {
        // Failed rename leaves a temp; successful rename consumes it.
        let _ = std::fs::remove_file(tmp.as_std_path());
        return Err(error.to_string().into());
    }
    // Q2: Ready directory-sync failure remains warn-only. The synced file
    // covers process crashes, not power loss of the directory entry; duplicate
    // execution under power loss is accepted in docs/operations.md.
    if let Err(error) = sync_dir(&idempotency_dir(runs_dir)) {
        tracing::warn!(%error, "idempotency ready directory sync failed; file sync already durable");
    }
    // Ready is committed: retirement failure must not turn success into a
    // retryable mutation. Exact Ready expiry can retry leftover retirement.
    if let Err(error) = retire_ready_pending(runs_dir, &record, Some(owner)) {
        tracing::warn!(%error, "Ready committed; pending retirement incomplete");
    }
    Ok(record)
}
/// Best-effort reverse association beside the journal, used only without
/// Ready. Unreadable sidecars fail closed rather than risk duplicate execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OrphanIdempotencyRecord {
    pub(crate) key: String,
    pub(crate) digest: String,
    pub(crate) run_id: String,
    /// Required TTL origin; a stale sidecar must not pin a conflicting digest.
    pub(crate) created_at_unix: u64,
}
fn orphan_path_for_run(runs_dir: &Utf8PathBuf, run_id: &str) -> Utf8PathBuf {
    runs_dir.join(run_id).join("meta").join("idempotency.json")
}
/// Best-effort sidecar written after successful execution, before Ready.
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
        // Do not lose the recovery pointer by guessing time zero on clock failure.
        created_at_unix: now_unix()?,
    };
    let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_IDEMPOTENCY_FILE_BYTES {
        return Err("orphan record exceeds size bound".into());
    }
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
    {
        // No-follow leaf opens relative to pinned immediate parents on Unix/Windows.
        // Ancestors of that parent, including the configured store, must stay trusted.
        // Create via O_EXCL then open existing: avoids macOS racing-create ENOENT.
        // Bounded retry on deletion churn; links and persistent churn fail closed.
        let mut file =
            open_no_follow(&tmp, false, true, false, true).map_err(|error| error.to_string())?;
        file.write_all(&bytes).map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
    }
    if let Err(error) = rename_verified(&tmp, &path) {
        let _ = std::fs::remove_file(tmp.as_std_path());
        return Err(error.to_string());
    }
    if let Some(parent) = path.parent() {
        sync_dir(&parent.to_path_buf()).map_err(|error| error.to_string())?;
    }
    Ok(())
}
/// Best-effort, unlocked recovery scan after Ready loss. Return an unexpired
/// matching (run_id, digest); skip unknown corrupt sidecars with a warning,
/// but refuse mismatched run identities or duplicate associations. Concurrent
/// sidecar publication may be missed; commit ownership settles surplus runs.
/// Expired sidecars are skipped and best-effort removed.
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
    #[cfg(unix)]
    #[test]
    fn execution_gate_unlocks_on_final_arc_even_with_inherited_descriptor() {
        let runs = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("gate-final-drop-{}", uuid::Uuid::now_v7()));
        let gate = ExecutionGate::try_acquire(&runs, "key").unwrap().unwrap();
        // dup shares the open file description, just as a pre-exec fork
        // does, without forking a multithreaded test process.
        let inherited = gate.file.try_clone().unwrap();
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

    use super::*;

    fn temp_runs_dir(name: &str) -> Utf8PathBuf {
        let dir = Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("idempotency-test-{name}-{}", uuid::Uuid::now_v7())),
        )
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

    #[cfg(unix)]
    #[test]
    fn fifo_records_fail_closed_without_blocking() {
        const CHILD_ROOT: &str = "QCG_IDEMPOTENCY_FIFO_TEST_ROOT";
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let runs_dir = Utf8PathBuf::from_path_buf(root.into()).expect("UTF-8 test root");
            std::fs::create_dir_all(idempotency_dir(&runs_dir)).unwrap();
            for (key, path) in [
                ("ready-fifo", idempotency_path(&runs_dir, "ready-fifo")),
                ("pending-fifo", pending_path(&runs_dir, "pending-fifo")),
            ] {
                let name = std::ffi::CString::new(path.as_str()).unwrap();
                // No writer is ever opened: a blocking read-open cannot finish.
                assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
                match read_bounded_idempotency_file(&path) {
                    BoundedRead::Unreadable(error) => {
                        assert!(error.contains("not a regular file"), "{error}");
                    }
                    _ => panic!("FIFO must be unreadable, not absent or a record"),
                }
                let error = claim_pending(&runs_dir, key, "digest", "run-A")
                    .expect_err("claim must fail closed on a FIFO record");
                assert_eq!(error.problem.status, 500);
                assert!(error.problem.detail.contains("not a regular file"));
                // The failed snapshot must release the shared admission lock.
                assert!(matches!(
                    claim_pending(&runs_dir, &format!("unrelated-{key}"), "digest", "run-B"),
                    Ok(ClaimOutcome::Owner { .. })
                ));
            }
            std::fs::write(runs_dir.join("checked"), b"ok").unwrap();
            return;
        }

        // A regression may block inside openat while holding the claim lock.
        // Only the child performs those calls; the parent owns cleanup and
        // kills/reaps it on a short deadline, never stranding the test suite.
        let runs_dir = temp_runs_dir("fifo-records");
        let _guard = TempGuard(runs_dir.clone());
        // libtest names omit the crate-name prefix from module_path!().
        let name = concat!(
            module_path!(),
            "::fifo_records_fail_closed_without_blocking"
        )
        .split_once("::")
        .unwrap()
        .1;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(CHILD_ROOT, runs_dir.as_str())
            .spawn()
            .expect("spawn isolated FIFO reader");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    assert!(status.success(), "FIFO child failed: {status}");
                    assert!(
                        runs_dir.join("checked").is_file(),
                        "child must run the test"
                    );
                    break;
                }
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                result => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("FIFO child timed out or could not be polled: {result:?}");
                }
            }
        }
    }

    /// Claims `key` for `digest` under the live default idempotency TTL. The
    /// runs directory, key, frozen TTL, and always-present reserved run id
    /// are the shared shape every claim test needs; only the digest and the
    /// reserved run id differ. The raw `Result` is returned so a caller can
    /// `match` on the outcome instead of unwrapping.
    fn claim_pending(
        runs_dir: &Utf8PathBuf,
        key: &str,
        digest: &str,
        reserved_run_id: &str,
    ) -> Result<ClaimOutcome, ApiHttpError> {
        claim_durable_pending(
            runs_dir,
            key,
            digest,
            Some(reserved_run_id.to_string()),
            policy::IDEMPOTENCY_TTL,
            &IdempotencySweep::default(),
        )
    }

    /// Publishes the Ready record for an owned claim under the live default
    /// idempotency TTL. The runs directory, key, and frozen TTL are the shared
    /// shape; only the digest, run id, owner, and generation differ. The raw
    /// `Result` is returned so a caller can `match`, `expect`, or `expect_err`
    /// on it without a silent unwrap.
    fn store_ready(
        runs_dir: &Utf8PathBuf,
        key: &str,
        digest: &str,
        run_id: &str,
        owner: &str,
        generation: u64,
    ) -> Result<String, StoreReadyError> {
        store_durable_ready(
            runs_dir,
            key,
            digest,
            run_id,
            owner,
            generation,
            policy::IDEMPOTENCY_TTL,
        )
        .map(|record| record.run_id)
    }

    /// Exercise the real cache constructor and pruning with a controlled wall
    /// clock, without advancing monotonic time or sleeping near a deadline.
    fn assert_cache_keeps_deadline(record: DurableIdempotencyRecord, created: u64) {
        use super::super::config::{IdempotencyEntry, prune_idempotency};
        use std::{collections::BTreeMap, time::Instant};

        assert_eq!(record.created_at_unix, created);
        let key = record.key.clone();
        let mut cache = BTreeMap::from([(key.clone(), IdempotencyEntry::ready(record))]);
        let ttl = policy::IDEMPOTENCY_TTL;
        let deadline = created + ttl.as_secs();
        let now = Instant::now();
        prune_idempotency(&mut cache, now, deadline - 1, ttl, 2).expect("prune before expiry");
        assert!(cache.contains_key(&key), "last retained second must replay");
        prune_idempotency(&mut cache, now, deadline, ttl, 2).expect("prune at expiry");
        assert!(cache.is_empty(), "cache insertion must not restart the TTL");
    }

    #[test]
    fn fresh_cache_expires_at_the_persisted_ready_boundary() {
        let runs_dir = temp_runs_dir("ready-cache-deadline");
        let _guard = TempGuard(runs_dir.clone());
        let created = 1_000_000;
        let record = DurableIdempotencyRecord {
            publication_id: Some(uuid::Uuid::now_v7().to_string()),
            key: "near-expired".into(),
            digest: "digest".into(),
            run_id: "run-A".into(),
            created_at_unix: created,
            generation: 7,
        };
        std::fs::create_dir_all(idempotency_dir(&runs_dir)).expect("idempotency directory");
        let path = idempotency_path(&runs_dir, &record.key);
        let bytes = serde_json::to_vec(&record).expect("serialize Ready");
        std::fs::write(&path, &bytes).expect("persist near-expired Ready");
        let ttl = policy::IDEMPOTENCY_TTL;
        let deadline = created + ttl.as_secs();
        let loaded = load_durable_ready_at(&runs_dir, &record.key, ttl, || Ok(deadline - 1))
            .expect("fresh reader")
            .expect("still retained");
        assert_cache_keeps_deadline(loaded, created);
        for now in [deadline, deadline + 1] {
            assert!(
                load_durable_ready_at(&runs_dir, &record.key, ttl, || Ok(now))
                    .expect("expired reader")
                    .is_none()
            );
        }
        assert_eq!(
            std::fs::read(path).expect("observation never deletes"),
            bytes
        );
    }

    #[tokio::test]
    async fn waiter_claim_and_commit_winner_keep_the_ready_timestamp() {
        let runs_dir = temp_runs_dir("ready-protocol-deadline");
        let _guard = TempGuard(runs_dir.clone());
        let key = "key-ready-deadline";
        let (owner, generation) = claim_owner(&runs_dir, key, "digest", "run-A");
        let ttl = policy::IDEMPOTENCY_TTL;
        let before = now_unix().expect("clock");
        let mut record =
            store_durable_ready(&runs_dir, key, "digest", "run-A", &owner, generation, ttl)
                .expect("new local commit");
        let after = now_unix().expect("clock");
        assert!((before..=after).contains(&record.created_at_unix));
        let persisted = load_durable_ready_result(&runs_dir, key, ttl)
            .expect("read")
            .expect("Ready");
        assert_eq!(record.created_at_unix, persisted.created_at_unix);
        assert_cache_keeps_deadline(record.clone(), record.created_at_unix);

        // A 23-hour-old record leaves an hour of real time for the protocol
        // calls; its final second and exact expiry are checked by virtual
        // timestamps above. No scheduling race or arbitrary sleep is needed.
        let created = before - ttl.as_secs() + 3600;
        record.created_at_unix = created;
        let path = idempotency_path(&runs_dir, key);
        let bytes = serde_json::to_vec(&record).expect("serialize old Ready");
        std::fs::write(&path, &bytes).expect("age Ready");

        let WaitOutcome::Ready(waited) = wait_for_peer_ready(&runs_dir, key, "digest", ttl)
            .await
            .expect("same-digest waiter")
        else {
            panic!("Ready must win")
        };
        assert_cache_keeps_deadline(waited, created);
        let ClaimOutcome::Ready(claimed) =
            claim_pending(&runs_dir, key, "digest", "run-B").expect("claim observes Ready")
        else {
            panic!("Ready must win")
        };
        assert_cache_keeps_deadline(claimed, created);
        // Both commit convergence arms must return the original record:
        // a matching live reservation, then an absent (released) reservation.
        let winner =
            store_durable_ready(&runs_dir, key, "digest", "run-A", &owner, generation, ttl)
                .expect("commit with live reservation");
        assert_eq!(winner.run_id, "run-A");
        assert_cache_keeps_deadline(winner, created);
        release_durable_pending(&runs_dir, key, &owner, generation).expect("release reservation");
        let winner = store_durable_ready(
            &runs_dir,
            key,
            "digest",
            "run-surplus",
            &owner,
            generation,
            ttl,
        )
        .expect("commit adopts existing winner");
        assert_eq!(winner.run_id, "run-A");
        assert_cache_keeps_deadline(winner, created);
        assert_eq!(std::fs::read(path).expect("winner unchanged"), bytes);
    }

    /// Rewrites the live pending claim for `key` with `created_at_unix: 0`, so
    /// a test reaches the adoption path without sleeping out the real TTL. The
    /// read-modify-write, JSON encoding, and write step are the shared shape;
    /// the caller only names the key whose claim must look expired.
    fn expire_pending_claim(runs_dir: &Utf8PathBuf, key: &str) {
        let path = pending_path(runs_dir, key);
        let mut record: DurablePendingRecord =
            serde_json::from_slice(&std::fs::read(path.as_std_path()).expect("claim readable"))
                .expect("claim parses");
        record.created_at_unix = 0;
        std::fs::write(
            path.as_std_path(),
            serde_json::to_vec(&record).expect("record serializes"),
        )
        .expect("aged claim should write");
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
        let Err(error) = wait_for_peer_ready(&runs_dir, key, "d", policy::IDEMPOTENCY_TTL).await
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
            operation_id: Some(uuid::Uuid::now_v7().to_string()),
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
        // The gate-aware recheck verifies key equality through the shared
        // classifier, not just owner+generation+digest. A foreign-key file
        // fails closed (never Current/Superseded). An expired claim whose
        // key/digest/owner/generation still match remains Current while its
        // gate is held, because no gate-aware successor could have replaced
        // it; mismatched expired identities are still superseded.
        let runs_dir = temp_runs_dir("owner-key-expiry");
        let _temp_guard = TempGuard(runs_dir.clone());
        let key = "key-1";
        std::fs::create_dir_all(idempotency_dir(&runs_dir).as_std_path())
            .expect("idempotency dir should be created");
        // Foreign-key file: key field differs from the lookup key.
        let foreign = DurablePendingRecord {
            operation_id: Some(uuid::Uuid::now_v7().to_string()),
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
        // Expired claim with matching identity: still Current while its
        // gate is held; mismatched identities are still superseded. Each
        // `check_pending_owner` call acquires the same key's gate, so the
        // exact holder cannot have been replaced between aging and recheck.
        let expired = DurablePendingRecord {
            operation_id: Some(uuid::Uuid::now_v7().to_string()),
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
                Ok(OwnerCheck::Current)
            ),
            "an expired exact claim must remain current while its gate is held"
        );
        assert!(
            matches!(
                check_pending_owner(&runs_dir, key, "B", 1, "d"),
                Ok(OwnerCheck::Superseded)
            ),
            "an expired claim with a foreign owner must read as superseded"
        );
        assert!(
            matches!(
                check_pending_owner(&runs_dir, key, "A", 2, "d"),
                Ok(OwnerCheck::Superseded)
            ),
            "an expired claim with a superseded generation must read as superseded"
        );
        assert!(
            matches!(
                check_pending_owner(&runs_dir, key, "A", 1, "other"),
                Ok(OwnerCheck::Superseded)
            ),
            "an expired claim with a different digest must read as superseded"
        );
    }

    #[test]
    fn gate_held_expired_exact_claim_may_still_publish_ready() {
        // Gate-aware expiry: aging the exact claim while its gate is held
        // must not refuse the holder. The gate is acquired once and retained
        // across aging, recheck and commit, so no gate-aware successor could
        // have replaced the claim. Mismatched identities are still refused
        // and publish nothing.
        let runs_dir = temp_runs_dir("gate-held-expiry-commit");
        let _guard = TempGuard(runs_dir.clone());
        let ttl = policy::IDEMPOTENCY_TTL;
        let sweep = IdempotencySweep::default();
        // Exact holder: claim, age past the 60s timestamp while holding the
        // gate, then recheck Current and publish Ready.
        let key = "gate-expiry-exact";
        let gate = ExecutionGate::try_acquire(&runs_dir, key)
            .expect("gate should acquire")
            .expect("gate should be free");
        let (owner, generation) = match claim_durable_pending_gated(
            &gate,
            key,
            "digest",
            Some("run-A".into()),
            ttl,
            &sweep,
        )
        .expect("claim should succeed")
        {
            ClaimOutcome::Owner {
                owner, generation, ..
            } => (owner, generation),
            other => panic!("expected owner, got {other:?}"),
        };
        expire_pending_claim(&runs_dir, key);
        assert!(
            matches!(
                check_pending_owner_gated(&gate, key, &owner, generation, "digest"),
                Ok(OwnerCheck::Current)
            ),
            "aged exact claim must remain current while its gate is held"
        );
        let committed =
            store_durable_ready_gated(&gate, key, "digest", "run-A", &owner, generation, ttl)
                .expect("aged exact claim must publish Ready while its gate is held");
        assert_eq!(committed.run_id, "run-A");
        assert_eq!(committed.digest, "digest");
        drop(gate);
        // Mismatched holder: same aging, but a foreign owner, generation,
        // digest or run id must still be refused and publish nothing.
        let key = "gate-expiry-mismatch";
        let gate = ExecutionGate::try_acquire(&runs_dir, key)
            .expect("gate should acquire")
            .expect("gate should be free");
        let (owner, generation) = match claim_durable_pending_gated(
            &gate,
            key,
            "digest",
            Some("run-A".into()),
            ttl,
            &sweep,
        )
        .expect("claim should succeed")
        {
            ClaimOutcome::Owner {
                owner, generation, ..
            } => (owner, generation),
            other => panic!("expected owner, got {other:?}"),
        };
        expire_pending_claim(&runs_dir, key);
        assert!(
            matches!(
                check_pending_owner_gated(&gate, key, "foreign-owner", generation, "digest"),
                Ok(OwnerCheck::Superseded)
            ),
            "aged foreign owner must read as superseded"
        );
        for (mismatched_owner, mismatched_generation, mismatched_digest, mismatched_run) in [
            ("foreign-owner", generation, "digest", "run-A"),
            (owner.as_str(), generation + 1, "digest", "run-A"),
            (owner.as_str(), generation, "other-digest", "run-A"),
            (owner.as_str(), generation, "digest", "run-other"),
        ] {
            let error = store_durable_ready_gated(
                &gate,
                key,
                mismatched_digest,
                mismatched_run,
                mismatched_owner,
                mismatched_generation,
                ttl,
            )
            .expect_err("mismatched aged claim must be refused");
            assert!(
                matches!(error, StoreReadyError::Storage(_)),
                "mismatch refusal must be storage-typed, got: {error}"
            );
        }
        assert!(
            load_durable_ready_result(&runs_dir, key, ttl)
                .expect("load should not fail")
                .is_none(),
            "refused mismatched commits must publish no Ready"
        );
    }

    #[test]
    fn pending_digest_lookup_is_read_only_and_reports_live_claims() {
        let runs_dir = temp_runs_dir("pending-digest-read");
        let _temp_guard = TempGuard(runs_dir.clone());
        let key = "pending-read";
        let digest = "reviewed-package-digest";
        let outcome = claim_pending(&runs_dir, key, digest, "run-pending-read")
            .expect("durable pending claim should be created");
        assert!(matches!(outcome, ClaimOutcome::Owner { .. }));
        let path = pending_path(&runs_dir, key);
        let before = std::fs::read(path.as_std_path()).expect("pending record bytes");

        assert_eq!(
            load_durable_pending_digest_result(&runs_dir, key)
                .expect("read-only pending lookup should succeed"),
            Some(digest.to_string())
        );
        assert_eq!(
            std::fs::read(path.as_std_path()).expect("pending record remains present"),
            before,
            "lookup must not mutate or reap the pending record"
        );
    }

    #[test]
    fn ready_publication_and_exact_convergence_retire_pending() {
        let runs = temp_runs_dir("ready-retirement");
        let _guard = TempGuard(runs.clone());
        let (owner, generation) = claim_owner(&runs, "key", "digest", "run");
        let pending = pending_path(&runs, "key");
        let original = std::fs::read(&pending).unwrap();
        store_ready(&runs, "key", "digest", "run", &owner, generation).unwrap();
        assert!(!pending.exists());
        let ready = std::fs::read(idempotency_path(&runs, "key")).unwrap();
        // Simulate interruption after Ready publication but before retirement.
        std::fs::write(&pending, original).unwrap();
        store_ready(&runs, "key", "digest", "run", &owner, generation).unwrap();
        assert!(!pending.exists());
        assert_eq!(
            std::fs::read(idempotency_path(&runs, "key")).unwrap(),
            ready
        );
    }

    #[test]
    fn ready_convergence_preserves_nonmatching_pending_evidence() {
        let runs = temp_runs_dir("ready-retirement-mismatch");
        let _guard = TempGuard(runs.clone());
        let (owner, generation) = claim_owner(&runs, "key", "digest", "run");
        let path = pending_path(&runs, "key");
        let original: DurablePendingRecord =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        store_ready(&runs, "key", "digest", "run", &owner, generation).unwrap();
        let ready = std::fs::read(idempotency_path(&runs, "key")).unwrap();
        for field in ["key", "digest", "run_id", "owner", "generation", "corrupt"] {
            let mut pending = serde_json::to_value(&original).unwrap();
            pending[field] = if field == "generation" {
                serde_json::json!(generation + 1)
            } else {
                serde_json::json!("different")
            };
            let bytes = if field == "corrupt" {
                b"{torn".to_vec()
            } else {
                serde_json::to_vec(&pending).unwrap()
            };
            std::fs::write(&path, &bytes).unwrap();
            assert!(store_ready(&runs, "key", "digest", "run", &owner, generation).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes, "{field}");
            assert_eq!(
                std::fs::read(idempotency_path(&runs, "key")).unwrap(),
                ready
            );
        }
        // A claim matching the caller but not the existing Ready may converge
        // under the existing protocol, but cannot retire unresolved evidence.
        for (run, next_generation) in [("other-run", generation), ("run", generation + 1)] {
            let mut pending = original.clone();
            pending.run_id = Some(run.into());
            pending.generation = next_generation;
            let bytes = serde_json::to_vec(&pending).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            store_ready(&runs, "key", "digest", run, &owner, next_generation).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            assert_eq!(
                std::fs::read(idempotency_path(&runs, "key")).unwrap(),
                ready
            );
        }
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
        store_ready(&runs_dir, key, "digest", "run-A", &owner, generation)
            .expect("ready commit should succeed");
        // Release the live claim so the commit path observes a chosen
        // pending state; Absent must converge.
        release_durable_pending(&runs_dir, key, &owner, generation)
            .expect("release should succeed");
        let converged = store_ready(&runs_dir, key, "digest", "run-A", &owner, generation)
            .expect("absent reservation with committed Ready must converge");
        assert_eq!(converged, "run-A");
        // Corrupt the pending file: same Ready present, pending Unusable.
        std::fs::write(pending_path(&runs_dir, key).as_std_path(), b"{torn")
            .expect("torn claim should be written");
        let error = store_ready(&runs_dir, key, "digest", "run-A", &owner, generation)
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
            operation_id: Some(uuid::Uuid::now_v7().to_string()),
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
            operation_id: Some(uuid::Uuid::now_v7().to_string()),
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
            publication_id: None,
            key: key.into(),
            digest: "d".into(),
            run_id: "run-1".into(),
            created_at_unix: now_unix()
                .expect("test clock should be after the Unix epoch")
                .saturating_sub(age_secs),
            generation: 1,
        };
        let expired_age = policy::IDEMPOTENCY_TTL.as_secs().saturating_add(60);
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
        reap_expired_ready_locked(&runs_dir, key, policy::IDEMPOTENCY_TTL)
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
        reap_expired_ready_locked(&runs_dir, key, policy::IDEMPOTENCY_TTL)
            .expect("reap should not fail");
        assert!(
            !idempotency_path(&runs_dir, key).as_std_path().exists(),
            "expired Ready must be reaped"
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
        match claim_pending(&runs_dir, key, "digest-b", "run-B").expect("claim should not error") {
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
                let outcome = claim_pending(&runs_dir, key, "digest", &format!("run-{index}"))
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
    fn expired_own_claim_without_successor_may_commit_while_gate_held() {
        // Gate-aware expiry: an aged exact claim may still commit while its
        // gate is held, because no gate-aware successor could have replaced
        // it. Each `store_ready` call acquires the same key's gate, so the
        // holder cannot have been superseded between aging and commit.
        // Mismatched identities are still refused.
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
        let committed = store_ready(&runs_dir, key, "digest", "run-A", &owner, generation)
            .expect("aged exact claim must commit while its gate is held");
        assert_eq!(committed, "run-A");
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
        // Mismatched aged claims are still refused: a foreign owner cannot
        // commit even with no successor.
        let runs_dir = temp_runs_dir("expired-commit-mismatch");
        let key = "key-expired-mismatch";
        let (owner, generation) = claim_owner(&runs_dir, key, "digest", "run-A");
        expire_pending_claim(&runs_dir, key);
        let error = store_ready(
            &runs_dir,
            key,
            "digest",
            "run-A",
            "foreign-owner",
            generation,
        )
        .expect_err("foreign aged commit must be refused");
        assert!(
            matches!(error, StoreReadyError::Storage(_)),
            "mismatch refusal must be storage-typed, got: {error}"
        );
        assert!(
            store_ready(&runs_dir, key, "digest", "run-other", &owner, generation)
                .expect_err("mismatched run must be refused")
                .to_string()
                .contains("reserved run"),
            "reserved-run mismatch must be named"
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
            policy::IDEMPOTENCY_TTL,
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
    fn same_digest_converges_to_the_committed_run_id() {
        // E02: committing the same digest converges at claim time onto the
        // committed mapping without a new owner. A direct commit with a
        // mismatched reserved run id fails closed instead of converging:
        // convergence with a reservation present requires proving current
        // ownership with the matching reserved run.
        let runs_dir = temp_runs_dir("converge-value");
        let key = "key-converge";
        let owner = claim_owner(&runs_dir, key, "digest", "run-A");
        let reservation = std::fs::read(pending_path(&runs_dir, key)).unwrap();
        let first = store_ready(&runs_dir, key, "digest", "run-A", &owner.0, owner.1)
            .expect("first commit should succeed");
        assert_eq!(first, "run-A");
        // A retry for the same digest converges at claim time onto run-A
        // without a new owner.
        match claim_pending(&runs_dir, key, "digest", "run-B").expect("claim should not error") {
            ClaimOutcome::Ready(record) => assert_eq!(record.run_id, "run-A"),
            other => panic!(
                "same digest must converge at claim, got {}",
                outcome_name(&other)
            ),
        }
        // Restore a leftover reservation to exercise fail-closed convergence
        // after an interrupted retirement.
        std::fs::write(pending_path(&runs_dir, key), reservation).unwrap();
        // A stale owner committing a different run id for the same digest
        // is refused instead of converging: the reservation (run-A) does
        // not match the request (run-B), so ownership fails closed (E02).
        // Convergence with a reservation happens only through the claim
        // path above or a current-owner commit with the matching run.
        let error = store_ready(&runs_dir, key, "digest", "run-B", &owner.0, owner.1)
            .expect_err("mismatched reserved run must be refused, not converged");
        assert!(
            error.to_string().contains("reserved run"),
            "reserved-run mismatch must be named, got: {error}"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    /// Claims the key and returns `(owner, generation)` for the winning
    /// owner, adopting `run_id` as the reserved run.
    fn claim_owner(runs_dir: &Utf8PathBuf, key: &str, digest: &str, run_id: &str) -> (String, u64) {
        match claim_pending(runs_dir, key, digest, run_id).expect("claim should not error") {
            ClaimOutcome::Owner {
                owner, generation, ..
            } => (owner, generation),
            other => panic!("expected owner, got {}", outcome_name(&other)),
        }
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

    fn outcome_name(outcome: &ClaimOutcome) -> &'static str {
        match outcome {
            ClaimOutcome::Owner { .. } => "Owner",
            ClaimOutcome::Peer => "Peer",
            ClaimOutcome::Ready(_) => "Ready",
            ClaimOutcome::Conflict => "Conflict",
        }
    }

    #[test]
    fn expired_legacy_or_invalid_operation_identity_is_retained_not_adopted() {
        let runs_dir = temp_runs_dir("legacy-operation-id");
        let key = "legacy";
        claim_pending(&runs_dir, key, "digest", "run-1").unwrap();
        expire_pending_claim(&runs_dir, key);
        let path = pending_path(&runs_dir, key);
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        for invalid in [
            None,
            Some(serde_json::json!(null)),
            Some(serde_json::json!("not-an-id")),
            Some(serde_json::json!(7)),
        ] {
            record.as_object_mut().unwrap().remove("operation_id");
            if let Some(invalid) = invalid {
                record["operation_id"] = invalid;
            }
            let bytes = serde_json::to_vec(&record).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            assert!(claim_pending(&runs_dir, key, "digest", "run-2").is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        std::fs::remove_dir_all(&runs_dir).unwrap();
    }

    #[test]
    fn generations_increase_across_expiry_chains() {
        // B02: adoption of an expired claim advances the generation, so a
        // superseded owner can never present a current one.
        let runs_dir = temp_runs_dir("generations");
        let key = "key-1";
        let first =
            claim_pending(&runs_dir, key, "digest", "run-1").expect("first claim should win");
        let gen1 = match first {
            ClaimOutcome::Owner { generation, .. } => generation,
            other => panic!("expected owner, got {}", outcome_name(&other)),
        };
        assert_eq!(gen1, 1, "fresh chain starts at generation 1");
        let original: DurablePendingRecord =
            serde_json::from_slice(&std::fs::read(pending_path(&runs_dir, key)).unwrap()).unwrap();
        let operation_id = original.operation_id.unwrap();
        assert!(uuid::Uuid::parse_str(&operation_id).is_ok());
        // Age the claim past its TTL without touching anything else.
        expire_pending_claim(&runs_dir, key);
        let second =
            claim_pending(&runs_dir, key, "digest", "run-9").expect("adoption claim should win");
        match second {
            ClaimOutcome::Owner {
                run_id,
                generation,
                operation_id: adopted_id,
                ..
            } => {
                assert_eq!(adopted_id, operation_id);
                let adopted: DurablePendingRecord =
                    serde_json::from_slice(&std::fs::read(pending_path(&runs_dir, key)).unwrap())
                        .unwrap();
                assert_eq!(adopted.operation_id.as_ref(), Some(&operation_id));
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
            operation_id: Some(uuid::Uuid::now_v7().to_string()),
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
            operation_id: Some(uuid::Uuid::now_v7().to_string()),
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
        rename_verified(&tmp, &pending).expect("replacement should succeed");
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
    fn superseded_commit_converges_or_fails_without_overwriting() {
        // B02/E02: a committer whose claim was superseded by an expiry
        // adoption must not replace the newer owner's outcome. Before any
        // Ready exists it fails; after the successor commits it converges
        // without Pending, but any leftover reservation still requires current
        // ownership (fail-closed).
        let runs_dir = temp_runs_dir("superseded-commit");
        let key = "key-1";
        let (stale_owner, gen1) = claim_owner(&runs_dir, key, "digest", "run-1");
        // Age the first claim past its TTL so the next claim adopts it.
        expire_pending_claim(&runs_dir, key);
        let second = claim_pending(&runs_dir, key, "digest", "run-2").expect("adoption should win");
        let (new_owner, gen2) = match second {
            ClaimOutcome::Owner {
                owner, generation, ..
            } => (owner, generation),
            other => panic!("expected adopting owner, got {}", outcome_name(&other)),
        };
        assert!(gen2 > gen1, "adoption advances the generation");
        // The stale owner commits nothing new: without a Ready record and
        // with a different claim id it fails instead of overwriting.
        let error = store_ready(&runs_dir, key, "digest", "run-1", &stale_owner, gen1)
            .expect_err("superseded commit must not overwrite");
        assert!(
            matches!(error, StoreReadyError::Storage(_)),
            "supersession must type as an explicit storage failure, got: {error}"
        );
        let reservation = std::fs::read(pending_path(&runs_dir, key)).unwrap();
        let committed = store_ready(&runs_dir, key, "digest", "run-1", &new_owner, gen2)
            .expect("current generation should commit");
        assert_eq!(committed, "run-1");
        let ready = std::fs::read(idempotency_path(&runs_dir, key)).unwrap();
        assert_eq!(
            store_ready(&runs_dir, key, "digest", "run-1", &stale_owner, gen1).unwrap(),
            "run-1"
        );
        assert_eq!(
            std::fs::read(idempotency_path(&runs_dir, key)).unwrap(),
            ready
        );
        std::fs::write(pending_path(&runs_dir, key), reservation).unwrap();
        let error = store_ready(&runs_dir, key, "digest", "run-1", &stale_owner, gen1)
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
        let committed = store_ready(&runs_dir, key, "digest-a", "run-1", &owner, generation)
            .expect("first commit should succeed");
        assert_eq!(committed, "run-1");
        let error = store_ready(&runs_dir, key, "digest-b", "run-9", &owner, generation)
            .expect_err("a different digest must conflict");
        assert!(
            matches!(error, StoreReadyError::DigestConflict),
            "digest mismatch must type as a conflict, got: {error}"
        );
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
        let error = store_ready(&runs_dir, key, "digest", "run-B", &owner, generation)
            .expect_err("a mismatched reserved run must be refused");
        assert!(
            error.to_string().contains("reserved run"),
            "reserved-run mismatch must be named, got: {error}"
        );
        // An absent claim is refused.
        std::fs::remove_file(pending_path(&runs_dir, key).as_std_path())
            .expect("claim should be removable");
        let error = store_ready(&runs_dir, key, "digest", "run-A", &owner, generation)
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
        let error = store_ready(&runs_dir, key, "digest", "run-A", &owner, generation)
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
            operation_id: Some(uuid::Uuid::now_v7().to_string()),
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
        let error = store_ready(&runs_dir, key, "digest", "run-A", "owner-none", 1)
            .expect_err("a None-reservation claim must be refused");
        assert!(
            error.to_string().contains("reserved run"),
            "None reservation must name the reserved run, got: {error}"
        );
        assert!(
            load_durable_ready_result(&runs_dir, key, policy::IDEMPOTENCY_TTL)
                .expect("load should not fail")
                .is_none(),
            "refused None-reservation commit must leave no Ready"
        );
        let _ = std::fs::remove_dir_all(runs_dir.as_std_path());
    }

    #[cfg(unix)]
    #[test]
    fn verified_rename_fails_closed_when_staging_parent_is_swapped_for_symlink() {
        let runs_dir = temp_runs_dir("rename-race");
        let _guard = TempGuard(runs_dir.clone());
        drop(acquire_idempotency_lock(&runs_dir).unwrap());
        let staged_dir = expiry::temp_dir(&runs_dir).unwrap();
        let staged = staged_dir.join(format!("{}.tmp-{}", "a".repeat(64), uuid::Uuid::now_v7()));
        let mut file = open_no_follow(&staged, false, true, false, true).unwrap();
        use io::Write as _;
        file.write_all(b"staged").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let dest = idempotency_path(&runs_dir, "race-key");
        let outside = runs_dir.join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        let backup = runs_dir.join("temps-backup");
        std::fs::rename(&staged_dir, &backup).unwrap();
        std::os::unix::fs::symlink(&outside, &staged_dir).unwrap();
        let staged_via_link = staged_dir.join(staged.file_name().unwrap());
        assert!(rename_verified(&staged_via_link, &dest).is_err());
        assert!(!dest.exists());
        assert!(!outside.join(staged.file_name().unwrap()).exists());
        assert!(backup.join(staged.file_name().unwrap()).exists());
        std::fs::remove_file(&staged_dir).unwrap();
        std::fs::rename(&backup, &staged_dir).unwrap();
    }
    #[cfg(any(unix, windows))]
    #[test]
    fn verified_rename_replaces_an_existing_destination() {
        let root = temp_runs_dir("rename-replace");
        let _guard = TempGuard(root.clone());
        let source_dir = root.join("staging");
        let dest_dir = root.join("destination");
        std::fs::create_dir(&source_dir).unwrap();
        std::fs::create_dir(&dest_dir).unwrap();
        let source = source_dir.join("ready-生成-🦀.tmp");
        let dest = dest_dir.join("ready-生成-🦀.json");
        std::fs::write(&source, b"new").unwrap();
        std::fs::write(&dest, b"old").unwrap();
        rename_verified(&source, &dest).unwrap();
        assert!(!source.exists());
        assert_eq!(std::fs::read(&dest).unwrap(), b"new");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_destination_open_does_not_leak_source_parent() {
        let root = temp_runs_dir("rename-fd-error");
        let _guard = TempGuard(root.clone());
        let source = root.join("source");
        std::fs::write(&source, b"preserved").unwrap();
        for _ in 0..16 {
            assert!(rename_verified(&source, &root.join("missing/dest")).is_err());
        }
        let canonical = std::fs::canonicalize(&root).unwrap();
        // Count only this test's unique directory, not other tests' descriptors.
        for entry in std::fs::read_dir("/proc/self/fd").unwrap() {
            if let Ok(target) = std::fs::read_link(entry.unwrap().path()) {
                assert_ne!(target, canonical, "source directory descriptor leaked");
            }
        }
        assert_eq!(std::fs::read(&source).unwrap(), b"preserved");
    }

    #[cfg(windows)]
    mod windows {
        use super::*;
        use windows_sys::{
            Wdk::Storage::FileSystem::{FILE_CREATE, FILE_OPEN},
            Win32::Storage::FileSystem::{DELETE, FILE_GENERIC_READ, FILE_GENERIC_WRITE},
        };

        fn symlink_supported(result: io::Result<()>) -> bool {
            match result {
                Ok(()) => true,
                Err(error) if matches!(error.raw_os_error(), Some(1314 | 50)) => {
                    eprintln!("symlink regression unavailable on this Windows host: {error}");
                    false
                }
                Err(error) => panic!("could not create test symlink: {error}"),
            }
        }

        #[test]
        fn narrow_parent_anchor_supports_relative_leaf_access_and_replacement() {
            let root = temp_runs_dir("windows-parent-rights");
            let _guard = TempGuard(root.clone());
            // FILE_RENAME_INFORMATION RootDirectory guidance permits FILE_TRAVERSE | FILE_READ_ATTRIBUTES.
            // Its internal target open requests FILE_WRITE_DATA | SYNCHRONIZE; that is not a parent
            // HANDLE requirement. Filesystem ACL checks still apply. Retain SYNCHRONIZE for this
            // non-overlapped OpenOptions open (see the RootDirectory documentation linked in rename_at).
            let parent = windows_io::pin_parent(&root).unwrap();
            let mut source = windows_io::open_at(
                &parent,
                "source",
                FILE_GENERIC_READ | FILE_GENERIC_WRITE | DELETE,
                FILE_CREATE,
            )
            .unwrap();
            io::Write::write_all(&mut source, b"new").unwrap();
            std::fs::write(root.join("dest"), b"old").unwrap();
            windows_io::open_at(&parent, "created", FILE_GENERIC_WRITE, FILE_CREATE).unwrap();
            let mut reader =
                windows_io::open_at(&parent, "source", FILE_GENERIC_READ, FILE_OPEN).unwrap();
            let mut bytes = Vec::new();
            io::Read::read_to_end(&mut reader, &mut bytes).unwrap();
            assert_eq!(bytes, b"new");
            windows_io::rename_at(&source, &parent, "dest").unwrap();
            assert!(!root.join("source").exists());
            assert_eq!(std::fs::read(root.join("dest")).unwrap(), b"new");
        }

        #[test]
        fn reparse_leaf_and_destination_parent_are_refused_without_mutation() {
            let root = temp_runs_dir("windows-reparse");
            let _guard = TempGuard(root.clone());
            let outside = root.join("outside");
            std::fs::create_dir(&outside).unwrap();
            let target = outside.join("target");
            std::fs::write(&target, b"outside").unwrap();
            let link = root.join("leaf");
            if !symlink_supported(std::os::windows::fs::symlink_file(&target, &link)) {
                return;
            }
            assert!(open_no_follow(&link, true, false, false, false).is_err());
            assert!(open_no_follow(&link, false, true, false, false).is_err());
            assert!(rename_verified(&link, &root.join("dest")).is_err());
            assert_eq!(std::fs::read(&target).unwrap(), b"outside");
            let parent_link = root.join("parent");
            assert!(symlink_supported(std::os::windows::fs::symlink_dir(
                &outside,
                &parent_link
            )));
            let source = root.join("source");
            std::fs::write(&source, b"source").unwrap();
            assert!(
                open_no_follow(&parent_link.join("target"), true, false, false, false).is_err()
            );
            assert!(rename_verified(&source, &parent_link.join("target")).is_err());
            assert_eq!(std::fs::read(&source).unwrap(), b"source");
            assert_eq!(std::fs::read(&target).unwrap(), b"outside");
        }

        #[test]
        fn pinned_open_and_rename_ignore_parent_and_source_leaf_swaps() {
            let root = temp_runs_dir("windows-pinned-swap");
            let _guard = TempGuard(root.clone());
            let source_dir = root.join("source");
            let dest_dir = root.join("destination");
            let outside = root.join("outside");
            for dir in [&source_dir, &dest_dir, &outside] {
                std::fs::create_dir(dir).unwrap();
            }
            std::fs::write(source_dir.join("payload"), b"original").unwrap();
            std::fs::write(dest_dir.join("dest"), b"old").unwrap();
            std::fs::write(outside.join("payload"), b"outside source").unwrap();
            std::fs::write(outside.join("dest"), b"outside destination").unwrap();
            let source_parent = windows_io::pin_parent(&source_dir).unwrap();
            let dest_parent = windows_io::pin_parent(&dest_dir).unwrap();
            let source_backup = root.join("source-backup");
            let dest_backup = root.join("destination-backup");
            std::fs::rename(&source_dir, &source_backup).unwrap();
            std::fs::rename(&dest_dir, &dest_backup).unwrap();
            if !symlink_supported(std::os::windows::fs::symlink_dir(&outside, &source_dir)) {
                return;
            }
            assert!(symlink_supported(std::os::windows::fs::symlink_dir(
                &outside, &dest_dir
            )));
            // Both pathnames now resolve outside; pinned operations must still use the originals.
            assert!(windows_io::pin_parent(&source_dir).is_err());
            assert!(windows_io::pin_parent(&dest_dir).is_err());
            let mut source = windows_io::open_at(
                &source_parent,
                "payload",
                FILE_GENERIC_READ | DELETE,
                FILE_OPEN,
            )
            .unwrap();
            let mut bytes = Vec::new();
            io::Read::read_to_end(&mut source, &mut bytes).unwrap();
            assert_eq!(bytes, b"original");
            // The rename must also use the source HANDLE, not re-resolve its old leaf name.
            std::fs::rename(source_backup.join("payload"), source_backup.join("moved")).unwrap();
            std::fs::write(source_backup.join("payload"), b"replacement leaf").unwrap();
            windows_io::rename_at(&source, &dest_parent, "dest").unwrap();
            assert_eq!(
                std::fs::read(dest_backup.join("dest")).unwrap(),
                b"original"
            );
            assert!(!source_backup.join("moved").exists());
            assert_eq!(
                std::fs::read(source_backup.join("payload")).unwrap(),
                b"replacement leaf"
            );
            assert_eq!(
                std::fs::read(outside.join("payload")).unwrap(),
                b"outside source"
            );
            assert_eq!(
                std::fs::read(outside.join("dest")).unwrap(),
                b"outside destination"
            );
        }
    }
}
