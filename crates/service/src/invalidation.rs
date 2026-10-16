//! Immutable metadata, deliberately outside the execution journal and its cache.
use crate::{LocalService, ServiceError};
use api::{ApiError, InvalidateRun, RunInvalidation};
use camino::{Utf8Path, Utf8PathBuf};
use std::io::Write as _;

fn validate_text(value: &str, limit: usize, name: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > limit || value.chars().any(char::is_control) {
        return Err(format!(
            "{name} must be nonblank, contain no control characters, and be at most {limit} UTF-8 bytes"
        ));
    }
    Ok(())
}

fn validate_request(request: &InvalidateRun) -> Result<(), String> {
    validate_text(&request.reason, 4096, "reason")?;
    validate_text(&request.actor_label, 128, "actor_label")
}

/// Do not allow an execution lease (which creates meta/) to resurrect a run.
/// Called only under the stable maintenance lease by mutations.
pub(crate) fn require_run_directory(run_dir: &Utf8Path) -> Result<(), ApiError> {
    if !crate::run_dirs::is_regular_directory(run_dir).map_err(crate::artifacts::api_internal)?
        || !crate::run_dirs::is_regular_directory(&run_dir.join("meta"))
            .map_err(crate::artifacts::api_internal)?
        || !crate::run_dirs::is_regular_file(&run_dir.join("meta/journal.jsonl"))
            .map_err(crate::artifacts::api_internal)?
    {
        return Err(ApiError::NotFound {
            detail: "run was not found".into(),
        });
    }
    Ok(())
}

pub(crate) fn read_invalidation(
    run_dir: &Utf8Path,
) -> Result<Option<RunInvalidation>, ServiceError> {
    // Refuse linked parents, not just a linked leaf. Missing parents are not
    // absence of metadata on a real run.
    for directory in [run_dir.to_owned(), run_dir.join("meta")] {
        if !crate::run_dirs::is_regular_directory(&directory)? {
            return Err(ServiceError::Invalid(
                "invalidation parent is not a regular directory".into(),
            ));
        }
    }
    let path = run_dir.join("meta/invalidation.json");
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            return Err(ServiceError::Invalid(
                "invalidation is not a regular file".into(),
            ));
        }
        Ok(_) => {}
    }
    let file = open_record(&path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if file.metadata()?.nlink() != 1 {
            return Err(ServiceError::Invalid("invalidation is hard-linked".into()));
        }
    }
    use std::io::Read as _;
    let mut bytes = Vec::new();
    file.take(32769).read_to_end(&mut bytes)?;
    if bytes.len() > 32768 {
        return Err(ServiceError::Invalid(
            "invalidation exceeds 32768 bytes".into(),
        ));
    }
    let record: RunInvalidation = serde_json::from_slice(&bytes)
        .map_err(|error| ServiceError::Invalid(format!("invalid invalidation: {error}")))?;
    validate_request(&InvalidateRun {
        reason: record.reason.clone(),
        actor_label: record.actor_label.clone(),
    })
    .map_err(ServiceError::Invalid)?;
    if Some(record.run_id.as_str()) != run_dir.file_name()
        || chrono::DateTime::parse_from_rfc3339(&record.invalidated_at).is_err()
    {
        return Err(ServiceError::Invalid(
            "invalidation identity or timestamp is invalid".into(),
        ));
    }
    Ok(Some(record))
}

#[cfg(not(windows))]
fn open_record(path: &Utf8Path) -> std::io::Result<std::fs::File> {
    files::open_read_nofollow(path)
}

#[cfg(windows)]
fn open_record(path: &Utf8Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::{
        fs::{MetadataExt as _, OpenOptionsExt as _},
        io::AsRawHandle as _,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_OPEN_REPARSE_POINT,
        GetFileInformationByHandle,
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(std::io::Error::other(
            "invalidation is not a regular, unlinked file",
        ));
    }
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    // SAFETY: the handle is live and info is writable storage of the required
    // type. The API initializes it on success; no uninitialized read on error.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: success above initialized all fields.
    if unsafe { info.assume_init() }.nNumberOfLinks != 1 {
        return Err(std::io::Error::other("invalidation is hard-linked"));
    }
    Ok(file)
}

fn stage(run_dir: &Utf8Path, record: &RunInvalidation) -> Result<Utf8PathBuf, ServiceError> {
    let parent = run_dir.join("meta");
    let temp = parent.join(format!(".invalidation-{}.tmp", uuid::Uuid::now_v7()));
    let bytes =
        serde_json::to_vec(record).map_err(|error| ServiceError::Invalid(error.to_string()))?;
    if bytes.len() > 32768 {
        return Err(ServiceError::Invalid(
            "invalidation exceeds 32768 bytes".into(),
        ));
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let staged = file.write_all(&bytes).and_then(|()| file.sync_all());
    drop(file);
    if let Err(error) = staged {
        let _ = std::fs::remove_file(&temp);
        return Err(error.into());
    }
    Ok(temp)
}

fn rename_no_replace(temp: &Utf8Path, target: &Utf8Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        let from = CString::new(temp.as_std_path().as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalidation staging path contains a NUL byte",
            )
        })?;
        let to = CString::new(target.as_std_path().as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalidation target path contains a NUL byte",
            )
        })?;
        // SAFETY: both paths are valid NUL-terminated strings for this call.
        let rc = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        // All qcg publishers/removers hold the stable store maintenance lease.
        // The existence check therefore excludes qcg races; std::fs::rename
        // is the portable atomic same-filesystem publication primitive. Linux
        // additionally uses RENAME_NOREPLACE to exclude lock-bypassing writers.
        match std::fs::symlink_metadata(target) {
            Ok(_) => Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "invalidation already exists",
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::rename(temp, target)
            }
            Err(error) => Err(error),
        }
    }
}

fn publish_staged(temp: &Utf8Path, run_dir: &Utf8Path) -> Result<(), ServiceError> {
    let parent = run_dir.join("meta");
    rename_no_replace(temp, &parent.join("invalidation.json"))?;
    // Directory fsync is supported on Unix. Other platforms sync the file
    // before rename and follow the same durability convention as journals.
    #[cfg(unix)]
    std::fs::File::open(&parent)?.sync_all()?;
    Ok(())
}

fn publish(run_dir: &Utf8Path, record: &RunInvalidation) -> Result<(), ServiceError> {
    let temp = stage(run_dir, record)?;
    let result = publish_staged(&temp, run_dir);
    if temp.exists() {
        std::fs::remove_file(&temp)?;
    }
    result
}

impl LocalService {
    /// Invalidate only a durable success. Exact retries preserve the first record.
    pub async fn invalidate_run(
        &self,
        id: &str,
        request: InvalidateRun,
    ) -> Result<RunInvalidation, ApiError> {
        use crate::artifacts::api_internal;
        self.ensure_running()?;
        validate_request(&request).map_err(ApiError::invalid)?;
        let _maintenance = crate::run_dirs::try_lock_store_maintenance(&self.inner.runs_dir)
            .map_err(api_internal)?
            .ok_or_else(|| ApiError::Conflict {
                detail: "run store maintenance is busy; retry".into(),
            })?;
        let run_dir = self.run_dir_for(id).await?;
        require_run_directory(&run_dir)?;
        let _execution = crate::run_dirs::try_lock_run_execution(&run_dir)
            .map_err(api_internal)?
            .ok_or_else(|| ApiError::Conflict {
                detail: "run is executing; wait for settlement".into(),
            })?;
        self.ensure_running()?;
        require_run_directory(&run_dir)?;
        // Bypass read-cache and memory: successful completion must be durable.
        let state = self.authoritative_state(&run_dir).await?;
        if state.terminal != Some(engine::TerminalState::Succeeded) {
            return Err(ApiError::Conflict {
                detail: "only durably succeeded runs can be invalidated".into(),
            });
        }
        if let Some(record) = read_invalidation(&run_dir).map_err(api_internal)? {
            if record.reason == request.reason && record.actor_label == request.actor_label {
                return Ok(record);
            }
            return Err(ApiError::Conflict {
                detail: "run already has a different immutable invalidation".into(),
            });
        }
        let record = RunInvalidation {
            run_id: id.into(),
            reason: request.reason,
            actor_label: request.actor_label,
            invalidated_at: chrono::Utc::now().to_rfc3339(),
        };
        publish(&run_dir, &record).map_err(api_internal)?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalidation_publication_is_atomic_no_clobber_and_restart_readable() {
        let root = crate::tests::support::temp_run_dir("invalidation-publication");
        let _guard = crate::tests::support::TempGuard(root.clone());
        let run = root.join("run");
        std::fs::create_dir_all(run.join("meta")).unwrap();
        let first = RunInvalidation {
            run_id: "run".into(),
            reason: "first".into(),
            actor_label: "reviewer".into(),
            invalidated_at: chrono::Utc::now().to_rfc3339(),
        };

        // A crash after staging but before atomic publication leaves no
        // authoritative record; a retry can safely publish the same record.
        let staged = stage(&run, &first).unwrap();
        assert_eq!(read_invalidation(&run).unwrap(), None);
        publish_staged(&staged, &run).unwrap();
        assert!(!staged.exists());
        assert_eq!(read_invalidation(&run).unwrap(), Some(first.clone()));

        // A restart-equivalent fresh read sees the complete single-link file.
        assert_eq!(read_invalidation(&run).unwrap(), Some(first.clone()));
        let second = RunInvalidation {
            reason: "second".into(),
            ..first.clone()
        };
        let staged = stage(&run, &second).unwrap();
        assert!(publish_staged(&staged, &run).is_err());
        let _ = std::fs::remove_file(staged);
        assert_eq!(read_invalidation(&run).unwrap(), Some(first));
        assert_eq!(std::fs::read_dir(run.join("meta")).unwrap().count(), 1);
    }
}
