//! Ephemeral canonical-record retention snapshot. No persistent reverse index.
use crate::ServiceError;
use camino::Utf8Path;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// Existing durable idempotency protocol read bound, shared with HTTP readers.
pub const MAX_IDEMPOTENCY_FILE_BYTES: u64 = 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Association {
    key: String,
    digest: String,
    run_id: Option<String>,
    generation: u64,
    created_at_unix: u64,
    publication_id: Option<String>,
    owner: Option<String>,
    operation_id: Option<String>,
}

fn invalid(reason: impl std::fmt::Display) -> ServiceError {
    ServiceError::Invalid(format!(
        "idempotency retention snapshot incomplete: {reason}"
    ))
}

/// Caller holds store maintenance from before this snapshot through deletion.
/// Claims take maintenance before publishing, expiry only removes references.
/// Release the metadata lock before taking *any* run lock or deleting a run.
pub(crate) fn retained_runs(
    runs: &Utf8Path,
    max_entries: usize,
) -> Result<BTreeSet<String>, ServiceError> {
    let root = runs.join("idempotency");
    match std::fs::symlink_metadata(&root) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(e) => return Err(invalid(e)),
        Ok(m) if !m.is_dir() || m.file_type().is_symlink() => {
            return Err(invalid("canonical directory is not a real directory"));
        }
        Ok(_) => {}
    }
    // Create only the stable protocol lock, never an association/index/marker.
    let lock_path = root.join(".idempotency.lock");
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
    {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(invalid(e)),
    }
    let lock = open_read_nofollow(&lock_path).map_err(invalid)?;
    lock.lock().map_err(invalid)?;
    let result = scan(&root, max_entries);
    lock.unlock().map_err(invalid)?;
    result
}

fn scan(root: &Utf8Path, max_entries: usize) -> Result<BTreeSet<String>, ServiceError> {
    let mut pins = BTreeSet::new();
    for (index, entry) in std::fs::read_dir(root).map_err(invalid)?.enumerate() {
        if index >= max_entries {
            return Err(invalid(format!("more than {max_entries} entries")));
        }
        let entry = entry.map_err(invalid)?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| invalid("non-UTF-8 entry"))?;
        // Fixed protocol locks and staging/expiry namespaces are not associations.
        // Count them, but never recurse into them.
        if name.starts_with('.') {
            continue;
        }
        let (hash, is_pending) = if let Some(hash) = name.strip_suffix(".pending.json") {
            (hash, true)
        } else if let Some(hash) = name.strip_suffix(".json") {
            (hash, false)
        } else {
            return Err(invalid("unknown canonical-directory entry"));
        };
        let bytes = read_record(&root.join(name)).map_err(invalid)?;
        let record: Association = serde_json::from_slice(&bytes).map_err(invalid)?;
        if record.key.is_empty()
            || hex::encode(Sha256::digest(record.key.as_bytes())) != hash
            || record.digest.is_empty()
            || record.generation == 0
            || record.run_id.as_deref().is_none_or(|id| {
                !policy::is_safe_path_component(id)
                    || crate::run_dirs::is_store_coordination_name(id)
            })
            || (is_pending && record.owner.as_deref().is_none_or(str::is_empty))
            || (!is_pending && (record.owner.is_some() || record.operation_id.is_some()))
            || (is_pending && record.publication_id.is_some())
        {
            return Err(invalid("unknown or malformed association"));
        }
        // Physical presence pins past TTL; only the expiry writer retires Ready.
        // A resolved Pending has the same run as Ready, so set union suffices.
        let _ = record.created_at_unix;
        pins.insert(record.run_id.expect("validated run association"));
    }
    Ok(pins)
}

fn read_record(path: &Utf8Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    let mut bytes = Vec::new();
    open_read_nofollow(path)?
        .take(MAX_IDEMPOTENCY_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_IDEMPOTENCY_FILE_BYTES {
        return Err(std::io::Error::other("oversized idempotency record"));
    }
    Ok(bytes)
}

#[cfg(not(windows))]
fn open_read_nofollow(path: &Utf8Path) -> std::io::Result<std::fs::File> {
    files::open_read_nofollow(path)
}

// Locks and records must refuse a reparse leaf on the *opened* handle,
// not just in the general files reader's non-Unix pre-open probe.
#[cfg(windows)]
fn open_read_nofollow(path: &Utf8Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_OPEN_REPARSE_POINT,
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(std::io::Error::other("non-regular idempotency file"));
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Root(camino::Utf8PathBuf);
    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn root() -> Root {
        let r = Root(
            camino::Utf8PathBuf::from_path_buf(std::env::temp_dir())
                .unwrap()
                .join(format!("qcg-retained-{}", uuid::Uuid::now_v7())),
        );
        std::fs::create_dir_all(r.0.join("idempotency/.expiry/deep")).unwrap();
        std::fs::write(r.0.join("idempotency/.expiry/deep/invalid.json"), b"bad").unwrap();
        r
    }
    fn write(r: &Root, key: &str, pending: bool, value: &serde_json::Value) -> camino::Utf8PathBuf {
        let hash = hex::encode(Sha256::digest(key.as_bytes()));
        let p = r.0.join("idempotency").join(format!(
            "{hash}{}.json",
            if pending { ".pending" } else { "" }
        ));
        std::fs::write(&p, serde_json::to_vec(value).unwrap()).unwrap();
        p
    }
    fn record(key: &str) -> serde_json::Value {
        json!({"key":key,"digest":"digest","run_id":"run","generation":1,"created_at_unix":1})
    }
    #[test]
    fn physical_ready_and_unresolved_pending_are_independent_pins() {
        let r = root();
        let a = write(&r, "a", false, &record("a"));
        let b = write(&r, "b", false, &record("b"));
        let mut p = record("a");
        p["owner"] = json!("owner");
        let pending = write(&r, "a", true, &p);
        assert_eq!(
            retained_runs(&r.0, 20).unwrap(),
            BTreeSet::from(["run".into()])
        );
        // Exact Ready resolves Pending; another run/generation/digest is not a match.
        for field in ["digest", "run_id", "generation"] {
            let mut changed = p.clone();
            changed[field] = if field == "generation" {
                json!(2)
            } else {
                json!("different")
            };
            write(&r, "a", true, &changed);
            let pins = retained_runs(&r.0, 20).unwrap();
            assert!(pins.contains("run"));
            assert_eq!(pins.contains("different"), field == "run_id");
        }
        write(&r, "a", true, &p);
        std::fs::remove_file(a).unwrap();
        std::fs::remove_file(b).unwrap();
        assert!(
            retained_runs(&r.0, 20).unwrap().contains("run"),
            "age never retires Pending"
        );
        std::fs::remove_file(pending).unwrap();
        assert!(retained_runs(&r.0, 20).unwrap().is_empty());
    }
    #[test]
    fn incomplete_unknown_and_corrupt_associations_fail_closed() {
        let r = root();
        let path = write(&r, "key", false, &record("key"));
        assert!(
            retained_runs(&r.0, 1).is_err(),
            "even coordination entries consume scan budget"
        );
        for value in [
            json!({}),
            json!({"key":"key","digest":"d","run_id":null,"generation":1,"created_at_unix":1}),
            record("foreign"),
        ] {
            std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
            assert!(retained_runs(&r.0, 20).is_err());
        }
        std::fs::write(&path, vec![b' '; MAX_IDEMPOTENCY_FILE_BYTES as usize + 1]).unwrap();
        assert!(retained_runs(&r.0, 20).is_err());
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(retained_runs(&r.0, 20).is_err());
        std::fs::remove_dir(path).unwrap();
        assert!(retained_runs(&r.0, 20).unwrap().is_empty());
    }
    #[cfg(unix)]
    #[test]
    fn canonical_record_and_directory_symlinks_are_refused() {
        let r = root();
        let path = write(&r, "key", false, &record("key"));
        let target = r.0.join("target");
        std::fs::rename(&path, &target).unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(retained_runs(&r.0, 20).is_err());
        let root = r.0.join("idempotency");
        std::fs::rename(&root, r.0.join("moved")).unwrap();
        std::os::unix::fs::symlink(r.0.join("moved"), &root).unwrap();
        assert!(retained_runs(&r.0, 20).is_err());
    }
}
