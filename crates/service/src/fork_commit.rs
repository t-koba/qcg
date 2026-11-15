//! Fresh service forks only. The journal names the protocol before publication;
//! the commit binds the stable tree after the child's queue append. This is an
//! fsync protocol, not a claim about hardware or physical-media durability.
use crate::{summaries::run_meta_dir, types::ServiceError};
use camino::Utf8Path;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
};

pub(crate) const VERSION: u64 = 1;
const MANIFEST: &str = "fork-tree.commit.json";
type DirectorySync<'a> = dyn FnMut(&Utf8Path) -> std::io::Result<()> + 'a;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Durability {
    Fsync,
    DirectorySyncUnsupported,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Commit {
    version: u64,
    run_id: String,
    queue_seq: u64,
    journal_bytes: u64,
    journal_sha256: String,
    audit_bytes: u64,
    tree_sha256: String,
    durability: Durability,
    commit_sha256: String,
}

impl Commit {
    fn checksum(&self) -> Result<String, ServiceError> {
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            self.version,
            &self.run_id,
            self.queue_seq,
            self.journal_bytes,
            &self.journal_sha256,
            self.audit_bytes,
            &self.tree_sha256,
            &self.durability,
        ))?)))
    }
}

fn invalid(detail: &str) -> ServiceError {
    ServiceError::Invalid(format!("fork tree commit: {detail}"))
}

fn open_plain(path: &Utf8Path) -> Result<std::fs::File, ServiceError> {
    if !std::fs::symlink_metadata(path)?.is_file() {
        return Err(invalid("expected a regular file"));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("expected a regular file"));
    }
    Ok(file)
}

fn hash_file(path: &Utf8Path, bytes: u64, sync: bool) -> Result<String, ServiceError> {
    let file = open_plain(path)?;
    if file.metadata()?.len() < bytes {
        return Err(invalid("committed file was truncated"));
    }
    let mut hash = Sha256::new();
    let mut input = (&file).take(bytes);
    let mut buffer = [0u8; 8192];
    let mut read = 0;
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        read += count as u64;
    }
    if read != bytes {
        return Err(invalid("committed file was truncated"));
    }
    if sync {
        #[cfg(unix)]
        file.sync_all()?;
        #[cfg(not(unix))]
        {
            // The non-Unix writer paths already sync their individual files;
            // opening arbitrary copied payload files for write can fail for
            // read-only package entries. Directory sync is unsupported here,
            // so this tree cannot claim the stronger commit capability.
            let _ = path;
        }
    }
    Ok(hex::encode(hash.finalize()))
}

/// These are coordination, not child payload. Keep in sync with operations.md.
/// state.json and its atomic staging files are disposable journal-fold caches,
/// not replay truth. Journal and audit *prefixes* are bound; later control/queue
/// appends may legitimately grow.
fn excluded(relative: &str) -> bool {
    relative == "meta/idempotency.json"
        || relative.starts_with("meta/idempotency.tmp-")
        || relative.starts_with("meta/.state.json.part-")
        || matches!(
            relative,
            "meta/fork-admission.pending"
                | "meta/fork-tree.commit.json"
                | "meta/state.json"
                | "meta/.journal.lock"
                | "meta/execution.lock"
                | "meta/.clean_shutdown"
                | "meta/control"
        )
}

fn unsupported(error: &std::io::Error) -> bool {
    #[cfg(unix)]
    let unsupported_code = error.raw_os_error().is_some_and(|code| {
        code == libc::EINVAL || code == libc::ENOTSUP || code == libc::EOPNOTSUPP
    });
    #[cfg(not(unix))]
    let unsupported_code = false;
    error.kind() == std::io::ErrorKind::Unsupported || unsupported_code
}

fn required_dir_sync(path: &Utf8Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        crate::run_dirs::sync_dir_entry(path)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(std::io::ErrorKind::Unsupported.into())
    }
}

/// Existing ordinary fork operations remain available without directory fsync.
/// Only the final commit barrier may advertise the capability.
pub(crate) fn sync_fork_dir(path: &Utf8Path) -> std::io::Result<()> {
    match required_dir_sync(path) {
        Err(error) if unsupported(&error) => Ok(()),
        result => result,
    }
}

fn sync_directory(
    path: &Utf8Path,
    sync: &mut impl FnMut(&Utf8Path) -> std::io::Result<()>,
    supported: &mut bool,
) -> Result<(), ServiceError> {
    match sync(path) {
        Err(error) if unsupported(&error) => *supported = false,
        result => result?,
    }
    Ok(())
}

fn tree_digest(
    root: &Utf8Path,
    commit: &Commit,
    mut sync: Option<&mut DirectorySync<'_>>,
    supported: &mut bool,
) -> Result<String, ServiceError> {
    fn visit(
        root: &Utf8Path,
        path: &Utf8Path,
        commit: &Commit,
        hash: &mut Sha256,
        sync: &mut Option<&mut DirectorySync<'_>>,
        supported: &mut bool,
    ) -> Result<(), ServiceError> {
        let relative = path
            .strip_prefix(root)
            .map_err(ServiceError::invalid)?
            .components()
            .map(|c| c.as_str())
            .collect::<Vec<_>>()
            .join("/");
        if excluded(&relative) {
            return Ok(());
        }
        let meta = std::fs::symlink_metadata(path)?;
        if !meta.is_file() && !meta.is_dir() {
            return Err(invalid("tree contains a symlink or special file"));
        }
        hash.update((relative.len() as u64).to_be_bytes());
        hash.update(relative.as_bytes());
        hash.update(if meta.is_dir() { b"d" } else { b"f" });
        let bytes = match relative.as_str() {
            "meta/journal.jsonl" => commit.journal_bytes,
            "meta/audit.jsonl" => commit.audit_bytes,
            _ if meta.is_file() => meta.len(),
            _ => 0,
        };
        hash.update(bytes.to_be_bytes());
        if meta.is_file() {
            hash.update(hash_file(path, bytes, sync.is_some())?.as_bytes());
        } else {
            let mut children = std::fs::read_dir(path)?
                .map(|entry| {
                    let path = entry?.path();
                    camino::Utf8PathBuf::from_path_buf(path).map_err(|_| invalid("non-UTF-8 path"))
                })
                .collect::<Result<Vec<_>, ServiceError>>()?;
            children.sort();
            for child in children {
                visit(root, &child, commit, hash, sync, supported)?;
            }
            if let Some(sync) = sync {
                sync_directory(path, sync, supported)?;
            }
        }
        Ok(())
    }
    let mut hash = Sha256::new();
    hash.update(b"qcg-fork-tree-v1\0");
    visit(root, root, commit, &mut hash, &mut sync, supported)?;
    Ok(hex::encode(hash.finalize()))
}

pub(crate) fn publish(
    run_dir: &Utf8Path,
    run_id: &str,
    queue_seq: u64,
) -> Result<(), ServiceError> {
    publish_with_sync(run_dir, run_id, queue_seq, required_dir_sync)
}

fn publish_with_sync(
    run_dir: &Utf8Path,
    run_id: &str,
    queue_seq: u64,
    mut sync: impl FnMut(&Utf8Path) -> std::io::Result<()>,
) -> Result<(), ServiceError> {
    let meta = run_meta_dir(run_dir);
    let journal = meta.join("journal.jsonl");
    // The prepared-fork fold and queue append validate reset evidence before
    // this tree barrier. Ordinary commits remain a hash/sync operation.
    // Some Minimal checkpoints have no observation stream yet. Materialize
    // its empty initial prefix so later writer creation does not change shape.
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(meta.join("audit.jsonl"))
    {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let mut commit = Commit {
        version: VERSION,
        run_id: run_id.into(),
        queue_seq,
        journal_bytes: open_plain(&journal)?.metadata()?.len(),
        journal_sha256: String::new(),
        audit_bytes: open_plain(&meta.join("audit.jsonl"))?.metadata()?.len(),
        tree_sha256: String::new(),
        durability: Durability::Fsync,
        commit_sha256: String::new(),
    };
    commit.journal_sha256 = hash_file(&journal, commit.journal_bytes, false)?;
    let mut supported = true;
    commit.tree_sha256 = tree_digest(run_dir, &commit, Some(&mut sync), &mut supported)?;
    let parent = run_dir
        .parent()
        .ok_or_else(|| invalid("missing run parent"))?;
    sync_directory(parent, &mut sync, &mut supported)?;
    if !supported {
        commit.durability = Durability::DirectorySyncUnsupported;
    }
    commit.commit_sha256 = commit.checksum()?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(meta.join(MANIFEST))?;
    serde_json::to_writer(&mut file, &commit)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    // Publish the manifest only after every payload file and directory. Include
    // the run-store entry, not just the metadata directory. A partial/failed
    // publication leaves the visibility marker and cannot be adopted.
    for directory in [&meta, run_dir, parent] {
        let mut still_supported = supported;
        sync_directory(directory, &mut sync, &mut still_supported)?;
        if supported && !still_supported {
            return Err(invalid("directory sync capability changed"));
        }
    }
    Ok(())
}

/// Called under admission ownership and again under execution ownership. After
/// the child's first start, its workspace is mutable: ordinary replay applies.
/// The immutable journal prefix and commit identity remain required thereafter.
pub(crate) fn verify(
    run_dir: &Utf8Path,
    run_id: &str,
    events: &[Value],
) -> Result<(), ServiceError> {
    let Some((fork_index, fork)) = events
        .iter()
        .enumerate()
        .rev()
        .find(|(_, event)| event["t"] == "run_forked")
    else {
        return Ok(());
    };
    let Some(version) = fork.get("fork_tree_commit_version") else {
        return Ok(());
    };
    if version.as_u64() != Some(VERSION) {
        return Err(invalid("unsupported protocol version"));
    }
    let fork_seq = fork["seq"]
        .as_u64()
        .ok_or_else(|| invalid("missing fork sequence"))?;
    let Some((queue_index, queue)) = events.iter().enumerate().find(|(_, event)| {
        event["t"] == "run_queued"
            && event["run_id"] == run_id
            && event["seq"].as_u64().is_some_and(|seq| seq > fork_seq)
    }) else {
        return Err(invalid("missing child queue identity"));
    };
    validate_reset_admission(fork, queue, events, fork_index, queue_index)?;
    let commit: Commit =
        serde_json::from_reader(open_plain(&run_meta_dir(run_dir).join(MANIFEST))?)?;
    if commit.commit_sha256 != commit.checksum()?
        || commit.version != VERSION
        || commit.run_id != run_id
        || Some(commit.queue_seq) != queue["seq"].as_u64()
    {
        return Err(invalid("manifest identity mismatch"));
    }
    if hash_file(
        &run_meta_dir(run_dir).join("journal.jsonl"),
        commit.journal_bytes,
        false,
    )? != commit.journal_sha256
    {
        return Err(invalid("journal prefix mismatch"));
    }
    let started = events.iter().any(|event| {
        event["run_id"] == run_id
            && event["seq"]
                .as_u64()
                .is_some_and(|seq| seq > commit.queue_seq)
            && matches!(event["t"].as_str(), Some("run_started" | "run_resumed"))
    });
    let pending = crate::run_dirs::fork_admission_pending(run_dir)?;
    if pending || !started {
        let mut supported = true;
        let mut sync = required_dir_sync;
        let actual = tree_digest(
            run_dir,
            &commit,
            pending.then_some(&mut sync as &mut DirectorySync<'_>),
            &mut supported,
        )?;
        if actual != commit.tree_sha256 {
            return Err(invalid("staged tree mismatch"));
        }
        if pending {
            let meta = run_meta_dir(run_dir);
            #[cfg(unix)]
            open_plain(&meta.join(MANIFEST))?.sync_all()?;
            #[cfg(not(unix))]
            OpenOptions::new()
                .write(true)
                .open(meta.join(MANIFEST))?
                .sync_all()?;
            for directory in [
                &meta,
                run_dir,
                run_dir
                    .parent()
                    .ok_or_else(|| invalid("missing run parent"))?,
            ] {
                sync_directory(directory, &mut sync, &mut supported)?;
            }
            if !supported && commit.durability == Durability::Fsync {
                return Err(invalid("recorded directory sync capability is unavailable"));
            }
        }
    }
    Ok(())
}

fn validate_reset_admission(
    fork: &Value,
    queue: &Value,
    events: &[Value],
    fork_index: usize,
    queue_index: usize,
) -> Result<(), ServiceError> {
    let has_candidate = fork
        .get("binding")
        .and_then(|binding| binding.get("reset_candidate_sha256"))
        .is_some();
    let has_overlay_digest = fork.get("reset_overlay_sha256").is_some();
    if queue_index <= fork_index {
        return Err(invalid("child queue precedes fork marker"));
    }
    let window = events
        .iter()
        .enumerate()
        .take(queue_index)
        .skip(fork_index + 1);
    let any_overlay = window
        .clone()
        .any(|(_, event)| event["t"] == "child_reset_overlay");
    if !has_candidate && !has_overlay_digest {
        return if any_overlay {
            Err(invalid("reset overlay is not bound by the fork marker"))
        } else {
            Ok(())
        };
    }
    if !has_candidate || !has_overlay_digest {
        return Err(invalid("reset binding or overlay digest is missing"));
    }
    let mut patch_seen = false;
    let mut overlay_index = None;
    for (index, event) in window {
        match event["t"].as_str() {
            Some("state_patched") if !patch_seen && overlay_index.is_none() => {
                patch_seen = true;
            }
            Some("child_reset_overlay") if overlay_index.is_none() => {
                overlay_index = Some(index);
            }
            _ => return Err(invalid("unexpected event in fork reset admission window")),
        }
    }
    let candidate_digest = fork["binding"]["reset_candidate_sha256"]
        .as_str()
        .ok_or_else(|| invalid("reset candidate digest is malformed"))?;
    let overlay_digest = fork["reset_overlay_sha256"]
        .as_str()
        .ok_or_else(|| invalid("reset overlay digest is malformed"))?;
    let index = overlay_index.ok_or_else(|| invalid("reset overlay event is missing"))?;
    let event = &events[index];
    let overlay = event
        .get("overlay")
        .ok_or_else(|| invalid("reset overlay payload is missing"))?;
    let candidate = overlay
        .get("candidate")
        .ok_or_else(|| invalid("reset candidate payload is missing"))?;
    let actual_candidate_digest = hex::encode(Sha256::digest(serde_json::to_vec(candidate)?));
    let actual_overlay_digest = hex::encode(Sha256::digest(serde_json::to_vec(overlay)?));
    if overlay["version"].as_u64() != Some(1)
        || events.get(index + 1).is_none_or(|next| {
            next["t"] != "run_queued"
                || next["run_id"] != fork["run_id"]
                || next["seq"] != queue["seq"]
        })
        || event["run_id"] != fork["run_id"]
        || overlay["binding"] != fork["binding"]
        || candidate["source_run_id"] != fork["source_run_id"]
        || candidate["source_last_seq"] != fork["source_seq"]
        || candidate["source_contract_sha256"] != fork["source_contract_sha256"]
        || queue["contract_sha256"] != fork["source_contract_sha256"]
        || actual_candidate_digest != candidate_digest
        || actual_overlay_digest != overlay_digest
    {
        return Err(invalid("reset admission identity mismatch"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{run_dirs, summaries::read_journal_events, tests::support::*};
    use camino::Utf8PathBuf;
    use serde_json::json;

    struct Fixture {
        _root: TempGuard,
        child: crate::types::RunRecord,
        events: Vec<Value>,
    }

    impl Fixture {
        fn new() -> Self {
            let root = temp_run_dir("fork-tree-commit");
            let source = synthetic_hitl_record(&root, "source");
            let mut child = source.clone();
            child.run_dir = root.join("runs/child");
            run_dirs::prepare_checkpoint_fork(
                &source.run_dir,
                "source",
                &child.run_dir,
                "child",
                1,
                &api::ForkStatePatch::default(),
            )
            .unwrap();
            crate::run_package::copy(
                &crate::run_package::package_dir(&source.run_dir),
                &child.run_dir,
                source.package_sha256.as_deref().unwrap(),
            )
            .unwrap();
            std::fs::create_dir_all(child.run_dir.join("workspace/nested/empty")).unwrap();
            std::fs::write(child.run_dir.join("workspace/nested/data"), b"staged bytes").unwrap();
            let mut payload = read_journal_events(&source.run_dir).unwrap().remove(0);
            for key in ["seq", "ts", "trace_id", "span_id", "t"] {
                payload.as_object_mut().unwrap().remove(key);
            }
            payload["run_id"] = json!("child");
            payload["parent_run_id"] = json!("source");
            run_dirs::write_run_event(&child, "run_queued", payload).unwrap();
            let events = read_journal_events(&child.run_dir).unwrap();
            Self {
                _root: TempGuard(root),
                child,
                events,
            }
        }
        fn dir(&self) -> &Utf8Path {
            &self.child.run_dir
        }
        fn manifest(&self) -> Utf8PathBuf {
            run_meta_dir(self.dir()).join(MANIFEST)
        }
        fn seq(&self) -> u64 {
            self.events.last().unwrap()["seq"].as_u64().unwrap()
        }
        fn publish(&self) {
            publish(self.dir(), "child", self.seq()).unwrap();
        }
        fn adopt(&self) -> Result<run_dirs::AdoptSnapshot, ServiceError> {
            run_dirs::try_adopt_run_dir_with_snapshot(self.dir(), "child")
        }
    }

    #[test]
    fn ordinary_fork_admission_does_not_constrain_non_reset_events() {
        let fork = json!({"t":"run_forked", "seq":1, "run_id":"child",
            "binding":{"binding_version":1, "plan_digest":"b".repeat(64)}});
        let queue = json!({"t":"run_queued", "seq":3, "run_id":"child"});
        let events = vec![
            fork.clone(),
            json!({"t":"legacy_internal_admission_note", "seq":2, "run_id":"child"}),
            queue.clone(),
        ];
        validate_reset_admission(&fork, &queue, &events, 0, 2).unwrap();
    }

    #[test]
    fn commit_syncs_bottom_up_and_adopts_only_the_bound_tree() {
        let f = Fixture::new();
        let mut synced = Vec::new();
        publish_with_sync(f.dir(), "child", f.seq(), |path| {
            synced.push(path.to_owned());
            required_dir_sync(path)
        })
        .unwrap();
        let index = |relative| {
            synced
                .iter()
                .position(|path| path == &f.dir().join(relative))
                .unwrap()
        };
        assert!(index("workspace/nested/empty") < index("workspace/nested"));
        assert!(index("workspace/nested") < index("workspace"));
        assert!(index("workspace") < index(""));
        assert_eq!(synced.last().unwrap(), f.dir().parent().unwrap());
        let commit: Commit = serde_json::from_reader(open_plain(&f.manifest()).unwrap()).unwrap();
        assert_eq!(commit.queue_seq, f.seq());
        assert_eq!(
            commit.durability == Durability::Fsync,
            required_dir_sync(f.dir()).is_ok()
        );
        assert!(f.adopt().unwrap().adopted);
        assert!(!run_dirs::fork_admission_pending(f.dir()).unwrap());
        verify(f.dir(), "child", &f.events).unwrap();
    }

    #[test]
    fn committed_reset_binding_refuses_a_missing_private_overlay() {
        let mut f = Fixture::new();
        let fork = f
            .events
            .iter_mut()
            .find(|event| event["t"] == "run_forked")
            .unwrap();
        fork["binding"] = json!({
            "binding_version": 1,
            "plan_digest": "b".repeat(64),
            "reset_candidate_sha256": "c".repeat(64),
        });
        fork["reset_overlay_sha256"] = json!("a".repeat(64));
        let error = verify(f.dir(), "child", &f.events).unwrap_err();
        assert!(error.to_string().contains("reset overlay event is missing"));
        assert!(!f.manifest().exists());
    }

    #[test]
    fn incomplete_reset_admission_without_overlay_is_wiped_not_adopted() {
        let f = Fixture::new();
        let mut events = f
            .events
            .iter()
            .filter(|event| event["t"] != "run_queued")
            .cloned()
            .collect::<Vec<_>>();
        let fork = events
            .iter_mut()
            .find(|event| event["t"] == "run_forked")
            .unwrap();
        fork["binding"] = json!({
            "binding_version": 1,
            "plan_digest": "b".repeat(64),
            "reset_candidate_sha256": "c".repeat(64),
        });
        fork["reset_overlay_sha256"] = json!("a".repeat(64));
        let path = run_meta_dir(f.dir()).join("journal.jsonl");
        std::fs::write(
            path,
            events
                .iter()
                .map(|event| format!("{event}\n"))
                .collect::<String>(),
        )
        .unwrap();
        let adopted = run_dirs::try_adopt_run_dir_with_snapshot(f.dir(), "child").unwrap();
        assert!(!adopted.adopted);
        assert!(
            !f.dir().exists(),
            "incomplete pending child is safely removed"
        );
    }

    #[test]
    fn interrupted_state_cache_publication_does_not_change_the_committed_tree() {
        let f = Fixture::new();
        f.publish();
        // JournalWriter creates this disposable cache before run_resumed. A
        // process kill inside write_sidecar_atomic leaves the staging name,
        // with either partial or complete cache bytes, but no new start event.
        let cache = std::fs::read(run_meta_dir(f.dir()).join("state.json")).unwrap();
        let name = format!(".state.json.part-{}-0", std::process::id());
        let staging = run_meta_dir(f.dir()).join(&name);
        for bytes in [b"{".as_slice(), cache.as_slice()] {
            std::fs::write(&staging, bytes).unwrap();
            assert!(f.adopt().unwrap().adopted);
            verify(f.dir(), "child", &f.events).unwrap();
        }
        // Only metadata-cache staging is disposable, not workspace payload or
        // private trace staging. Neither may disappear from tree verification.
        for relative in [
            format!("workspace/{name}"),
            "meta/.dependency-traces.jsonl.part-1-0".into(),
        ] {
            let path = f.dir().join(relative);
            std::fs::write(&path, b"uncommitted payload").unwrap();
            assert!(verify(f.dir(), "child", &f.events).is_err());
            std::fs::remove_file(path).unwrap();
        }
        std::fs::remove_file(staging).unwrap();
        verify(f.dir(), "child", &f.events).unwrap();
    }

    #[test]
    fn missing_altered_or_foreign_commit_evidence_fails_closed() {
        let f = Fixture::new();
        assert!(f.adopt().is_err());
        assert!(run_dirs::fork_admission_pending(f.dir()).unwrap());
        f.publish();
        let original = std::fs::read(f.manifest()).unwrap();
        for (key, value) in [
            ("run_id", json!("other")),
            ("queue_seq", json!(99)),
            ("version", json!(99)),
            ("tree_sha256", json!("altered")),
            ("journal_bytes", json!(0)),
            ("durability", json!("unknown")),
        ] {
            let mut manifest: Value = serde_json::from_slice(&original).unwrap();
            manifest[key] = value;
            if key != "durability" {
                let mut altered: Commit = serde_json::from_value(manifest.clone()).unwrap();
                altered.commit_sha256 = altered.checksum().unwrap();
                manifest = serde_json::to_value(altered).unwrap();
            }
            std::fs::write(f.manifest(), serde_json::to_vec(&manifest).unwrap()).unwrap();
            assert!(f.adopt().is_err(), "accepted altered {key}");
            assert!(run_dirs::fork_admission_pending(f.dir()).unwrap());
        }
        std::fs::write(f.manifest(), b"{").unwrap();
        assert!(f.adopt().is_err());
        std::fs::write(f.manifest(), original).unwrap();
        assert!(f.adopt().unwrap().adopted);
    }

    #[test]
    fn tree_contents_paths_types_and_lengths_are_bound_before_first_execution() {
        let f = Fixture::new();
        f.publish();
        assert!(f.adopt().unwrap().adopted);
        let file = f.dir().join("workspace/nested/data");
        for bytes in [b"altered byte".as_slice(), b"short"] {
            std::fs::write(&file, bytes).unwrap();
            assert!(verify(f.dir(), "child", &f.events).is_err());
        }
        std::fs::write(&file, b"staged bytes").unwrap();
        let moved = file.with_file_name("renamed");
        std::fs::rename(&file, &moved).unwrap();
        assert!(verify(f.dir(), "child", &f.events).is_err());
        std::fs::rename(&moved, &file).unwrap();
        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir(&file).unwrap();
        assert!(verify(f.dir(), "child", &f.events).is_err());
        std::fs::remove_dir(&file).unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("missing", &file).unwrap();
            assert!(verify(f.dir(), "child", &f.events).is_err());
            std::fs::remove_file(&file).unwrap();
        }
        std::fs::write(&file, b"staged bytes").unwrap();
        verify(f.dir(), "child", &f.events).unwrap();

        // Mutable cache/coordination and later control appends do not invalidate
        // the initial payload. Actual execution then releases the workspace to
        // ordinary replay, but still cannot lose its commit or journal prefix.
        run_dirs::write_run_event(
            &f.child,
            "user_cancel_requested",
            json!({"requester":"test"}),
        )
        .unwrap();
        let events = read_journal_events(f.dir()).unwrap();
        verify(f.dir(), "child", &events).unwrap();
        let mut payload = f.events.last().unwrap().clone();
        payload.as_object_mut().unwrap().remove("seq");
        run_dirs::write_run_event(&f.child, "run_started", payload).unwrap();
        let events = read_journal_events(f.dir()).unwrap();
        std::fs::write(&file, b"ordinary execution output").unwrap();
        verify(f.dir(), "child", &events).unwrap();
        let mut altered: Commit =
            serde_json::from_reader(open_plain(&f.manifest()).unwrap()).unwrap();
        altered.tree_sha256 = "altered after execution".into();
        std::fs::write(f.manifest(), serde_json::to_vec(&altered).unwrap()).unwrap();
        assert!(verify(f.dir(), "child", &events).is_err());
        std::fs::remove_file(f.manifest()).unwrap();
        assert!(verify(f.dir(), "child", &events).is_err());
    }

    #[test]
    fn server_idempotency_sidecar_added_after_commit_does_not_change_tree() {
        let f = Fixture::new();
        f.publish();
        let meta = run_meta_dir(f.dir());
        std::fs::write(
            meta.join("idempotency.tmp-race"),
            b"server-owned staged idempotency association",
        )
        .unwrap();
        verify(f.dir(), "child", &f.events).unwrap();
        std::fs::write(
            meta.join("idempotency.json"),
            b"server-owned idempotency association",
        )
        .unwrap();
        std::fs::remove_file(meta.join("idempotency.tmp-race")).unwrap();
        verify(f.dir(), "child", &f.events).unwrap();
    }

    #[test]
    fn legacy_forks_keep_existing_adoption_without_a_manifest() {
        let mut f = Fixture::new();
        for event in &mut f.events {
            event
                .as_object_mut()
                .unwrap()
                .remove("fork_tree_commit_version");
        }
        let bytes = f
            .events
            .iter()
            .map(|event| format!("{event}\n"))
            .collect::<String>();
        std::fs::write(run_meta_dir(f.dir()).join("journal.jsonl"), bytes).unwrap();
        assert!(f.adopt().unwrap().adopted);
        assert!(!f.manifest().exists());
    }

    #[test]
    fn sync_failure_keeps_the_fork_pending_and_recovery_reestablishes_the_barrier() {
        let f = Fixture::new();
        let error = publish_with_sync(f.dir(), "child", f.seq(), |_| {
            Err(std::io::Error::other("injected sync failure"))
        })
        .unwrap_err();
        assert!(error.to_string().contains("injected sync failure"));
        assert!(!f.manifest().exists());
        assert!(f.adopt().is_err());
        let mut parent_syncs = 0;
        assert!(
            publish_with_sync(f.dir(), "child", f.seq(), |path| {
                if Some(path) == f.dir().parent() {
                    parent_syncs += 1;
                    if parent_syncs == 2 {
                        return Err(std::io::Error::other("injected final sync failure"));
                    }
                }
                required_dir_sync(path)
            })
            .is_err()
        );
        assert!(f.manifest().exists());
        assert!(run_dirs::fork_admission_pending(f.dir()).unwrap());
        assert!(f.adopt().unwrap().adopted);
    }

    #[test]
    fn unsupported_directory_sync_records_no_durable_capability() {
        let f = Fixture::new();
        publish_with_sync(f.dir(), "child", f.seq(), |_| {
            Err(std::io::ErrorKind::Unsupported.into())
        })
        .unwrap();
        let commit: Commit = serde_json::from_reader(open_plain(&f.manifest()).unwrap()).unwrap();
        assert_eq!(commit.durability, Durability::DirectorySyncUnsupported);
        assert!(
            f.adopt().unwrap().adopted,
            "ordinary forks remain available"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn first_execution_rechecks_the_committed_tree_after_queue_delay() {
        let root = temp_run_dir("fork-tree-execution-gate");
        let _guard = TempGuard(root.clone());
        let generators =
            Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/generators");
        let service = test_service(vec![generators], root.join("runs"));
        let source = start_ask_user_and_answer(&service, "brief").await;
        wait_for_terminal_snapshot(&service, &source).await;
        let checkpoint = read_journal_events(&root.join("runs").join(&source))
            .unwrap()
            .iter()
            .find(|event| event["t"] == "run_started")
            .unwrap()["seq"]
            .as_u64()
            .unwrap();
        let child_dir = root.join("runs/child");
        let probe = QueueProbe(child_dir.clone());
        let (admitted, release) = probe.gate("admitted");
        service
            .fork_run_with_id(
                &source,
                api::ForkRun {
                    at_seq: checkpoint,
                    ..Default::default()
                },
                Some("child".into()),
            )
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), admitted)
            .await
            .unwrap()
            .unwrap();
        assert!(child_dir.join("meta/fork-tree.commit.json").exists());
        let queued_events = read_journal_events(&child_dir).unwrap();
        std::fs::write(
            child_dir.join("workspace/uncommitted"),
            b"unexpected payload",
        )
        .unwrap();
        let task_slot = service.inner.runs.read().await["child"].task.clone();
        let task = task_slot.lock().unwrap().take().unwrap();
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        let events = read_journal_events(&child_dir).unwrap();
        assert_eq!(events.last().unwrap()["t"], "run_queued");
        assert_eq!(
            events, queued_events,
            "verification failure must not execute or append effects"
        );
        assert_eq!(
            service.inner.runs.read().await["child"].state,
            api::RunStatus::Queued
        );
    }
}
