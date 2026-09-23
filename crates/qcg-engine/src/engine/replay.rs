use crate::{FilePin, JournalWriter, NodeOutcome, RunState, StepError};
use qcg_contract::RuntimeLimits;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::checkpoint::{CheckpointAccounting, hash_file, is_symlink_no_follow};
use super::types::{EngineError, RunContext};

#[derive(Debug)]
pub(crate) struct JournalReplay {
    pub(crate) steps: BTreeMap<String, ReplayedStep>,
    pub(crate) state: RunState,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplayedStep {
    pub(crate) status: String,
    pub(crate) output: Option<Value>,
    pub(crate) files: Vec<FilePin>,
}

impl JournalReplay {
    pub(crate) fn from_state(state: RunState) -> Self {
        let steps = state
            .nodes
            .iter()
            .filter_map(|(path, outcome)| match outcome {
                NodeOutcome::Success { output, files } => Some((
                    path.as_str().to_string(),
                    ReplayedStep {
                        status: "success".into(),
                        output: output.clone(),
                        files: files.clone(),
                    },
                )),
                NodeOutcome::Skipped { .. } | NodeOutcome::Failed { .. } => None,
            })
            .collect();
        Self { steps, state }
    }

    pub(crate) fn verify_files(
        &self,
        workspace: &camino::Utf8Path,
        metadata: &camino::Utf8Path,
        limits: &RuntimeLimits,
        accounting: &Arc<Mutex<CheckpointAccounting>>,
    ) -> Result<BTreeMap<String, String>, EngineError> {
        // One collection walk over successful steps and historical pins
        // builds the deduplicated blob set, the workspace-projection set,
        // and the success set together (E06). Verification then runs in two
        // passes over those sets (blobs, then workspace projections);
        // Failed-only paths and successful-step pins both contribute blob
        // requirements; the deduplicated union is verified exactly once per
        // digest (digest-unit dedup via the in-memory cache below, not once
        // per path-revision pair). Node iteration order is irrelevant under
        // this rule.
        // Single-handle semantics (E06): every file is opened once with
        // `O_NOFOLLOW`, validated via `fstat` as a regular file, read FROM
        // THAT FD, hashed over THOSE bytes, compared to the pin, and those
        // bytes (via their digest/count) are what the caller uses — never a
        // second open/hash of the same source. The parent walk below is a
        // pre-check only; the leaf `O_NOFOLLOW` open is authoritative. A
        // parent swapped between walk and open can still redirect the open;
        // deployments with untrusted concurrent workspace writers must
        // isolate the workspace (Unix handle isolation in the gateway path)
        // and must not rely on this pre-check alone. Non-Unix is pre+post
        // best-effort (see `qcg_fs::open_read_nofollow`).
        // Residual verify-to-use window (E06): verification pins the digest
        // at verify time, but a non-cooperative writer can replace workspace
        // bytes before a later step uses them; that window is outside the
        // guaranteed boundary and relies on gateway plus operational
        // isolation above.
        // Handoff (E06): the returned map threads verified workspace
        // digests to the caller (`materialize_file_inputs_with_preverified`)
        // so it must not re-hash the same source. `run_dirs` (FOREIGN)
        // double-hashing and copy-follows remain foreign; this side
        // eliminates its own duplication via this map.
        // Invariant (E06): the latest revision per path is decided by
        // journal order, never by iteration order. All pins are collected
        // into order-independent sets first (see below), so `BTreeMap`
        // traversal order cannot affect which revision wins.
        let mut successful_paths: BTreeSet<camino::Utf8PathBuf> = BTreeSet::new();
        let mut blobs: BTreeSet<(camino::Utf8PathBuf, String)> = BTreeSet::new();
        let mut paths: BTreeSet<camino::Utf8PathBuf> = BTreeSet::new();
        for step in self.steps.values() {
            for pin in &step.files {
                successful_paths.insert(pin.path.clone());
                blobs.insert((pin.path.clone(), pin.sha256.clone()));
                paths.insert(pin.path.clone());
            }
        }
        for (path, digests) in &self.state.historical_file_pins {
            let path = camino::Utf8PathBuf::from(path);
            paths.insert(path.clone());
            for digest in digests {
                blobs.insert((path.clone(), digest.clone()));
            }
        }
        for path in self.state.latest_file_pins.keys() {
            paths.insert(camino::Utf8PathBuf::from(path));
        }
        // The latest pin value itself is an explicit blob requirement: do
        // not rely on the fold invariant that latest is always a subset of
        // historical (E06). A journal that names a latest revision without
        // its blob must fail closed here.
        for (path, digest) in &self.state.latest_file_pins {
            blobs.insert((camino::Utf8PathBuf::from(path), digest.clone()));
        }
        for path in &paths {
            // Fork journals and workspace writes share one safety rule:
            // absolute or traversing paths are never resumable.
            if path.is_absolute() || !qcg_policy::is_safe_relative_path(path.as_str()) {
                return Err(EngineError::Failed(format!(
                    "cannot safely resume: journal contains unsafe output path `{path}`"
                )));
            }
        }
        // Per-digest in-memory cache for this resume: the same immutable
        // blob is hashed once even when many paths or revisions name it
        // (E06). The cache holds digests verified during this call only.
        let mut verified_digests: HashSet<String> = HashSet::new();
        for (path, digest) in &blobs {
            if !verified_digests.contains(digest) {
                verify_pinned_blob(metadata, path, digest, limits)?;
                verified_digests.insert(digest.clone());
            }
        }
        // Verified workspace digests threaded to the caller to avoid a
        // second open/hash of the same source (E06 handoff).
        let mut preverified: BTreeMap<String, String> = BTreeMap::new();
        for path in &paths {
            let successful = successful_paths.contains(path);
            let has_latest_pin = self.state.latest_file_pins.contains_key(path.as_str());
            // Blobs are always verified, but the workspace projection is
            // only required for paths a successful step produced or a
            // latest pin names: requiring it for failed-only revisions
            // would reject a legitimate resume, while skipping a named
            // latest pin would accept a phantom revision (E06).
            if !successful && !has_latest_pin {
                // Fail-closed for failed-only workspace bytes (E06): a
                // failed step replays from its verified blob, but stray
                // workspace bytes that match no journaled revision are
                // tampering, not irrelevant. When the file is absent the
                // revision verifies by blob alone; when present its bytes
                // must match a historical digest, otherwise the file is
                // quarantined aside and resume is refused.
                // Single-handle: no separate existence probe before the
                // hash. The parent walk is the pre-check, the `O_NOFOLLOW`
                // open inside `hash_file` is authoritative, and `NotFound`
                // from that single open reads as absent-ok (E06).
                let candidate = workspace.join(path);
                reject_symlinked_workspace_path(workspace, &candidate).map_err(|error| {
                    EngineError::Failed(format!(
                        "cannot safely resume: output `{path}` is not a directly rooted regular file: {error}"
                    ))
                })?;
                let digest = match hash_file(&candidate, limits.output_file_limit_bytes) {
                    Ok(digest) => digest,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        return Err(EngineError::Failed(format!(
                            "cannot safely resume: output `{path}` is unreadable: {error}"
                        )));
                    }
                };
                let Some(digests) = self.state.historical_file_pins.get(path.as_str()) else {
                    quarantine_workspace_file(workspace, metadata, path, &candidate);
                    return Err(EngineError::Failed(format!(
                        "cannot safely resume: output `{path}` has workspace bytes without a journaled revision; quarantined"
                    )));
                };
                if !digests.contains(&digest.sha256) {
                    quarantine_workspace_file(workspace, metadata, path, &candidate);
                    return Err(EngineError::Failed(format!(
                        "cannot safely resume: output `{path}` does not match any journaled revision (got {}); quarantined",
                        digest.sha256
                    )));
                }
                // Thread the verified historical digest for the file-input
                // handoff (E06): the caller reuses it without re-hashing.
                preverified.insert(path.to_string(), digest.sha256.clone());
                continue;
            }
            let candidate = workspace.join(path);
            // The workspace projection follows the same symlink standard as
            // the blob side: a terminal symlink or any symlinked parent
            // between the workspace root and the file is planted tampering
            // and refuses resume (E06). The parent walk and the hash open
            // are separate syscalls: the walk is a pre-check, the leaf
            // open with O_NOFOLLOW is authoritative. A parent swapped
            // between walk and open can still redirect the open; deployments
            // with untrusted concurrent workspace writers must isolate the
            // workspace (Unix handle isolation in the gateway path) and must
            // not rely on this pre-check alone.
            reject_symlinked_workspace_path(workspace, &candidate).map_err(|error| {
                EngineError::Failed(format!(
                    "cannot safely resume: output `{path}` is not a directly rooted regular file: {error}"
                ))
            })?;
            let digest =
                hash_file(&candidate, limits.output_file_limit_bytes).map_err(|error| {
                    // Mirror the blob-side distinction (E06): a missing
                    // workspace projection and an unreadable one direct the
                    // operator differently, so they must not collapse into
                    // one "unavailable" message.
                    if error.kind() == std::io::ErrorKind::NotFound {
                        EngineError::Failed(format!(
                            "cannot safely resume: output `{path}` is missing: {error}"
                        ))
                    } else {
                        EngineError::Failed(format!(
                            "cannot safely resume: output `{path}` is unreadable: {error}"
                        ))
                    }
                })?;
            match self.state.latest_file_pins.get(path.as_str()) {
                // The workspace must project the latest pinned revision;
                // matching an older revision is a rollback (rewind), not
                // corruption and not a resume. It carries the explicit
                // Rewound variant so operators can tell it apart (E06).
                Some(expected) => {
                    if &digest.sha256 != expected {
                        quarantine_workspace_file(workspace, metadata, path, &candidate);
                        return Err(EngineError::Rewound(format!(
                            "cannot safely resume: output `{path}` does not match its latest pinned revision {expected} (got {}); quarantined",
                            digest.sha256
                        )));
                    }
                }
                // A successful step that pinned this path always records the
                // latest revision; missing bookkeeping is corruption.
                None => {
                    return Err(EngineError::Failed(format!(
                        "cannot safely resume: output `{path}` has no recorded latest pin"
                    )));
                }
            }
            let mut accounting = accounting.lock().map_err(|error| {
                EngineError::Failed(format!(
                    "cannot safely resume: checkpoint accounting lock for `{path}` was poisoned: {error}"
                ))
            })?;
            accounting.record(path, digest.bytes, limits)?;
            // Thread the verified digest to the caller so file-input
            // materialization reuses THOSE bytes without a second open.
            preverified.insert(path.to_string(), digest.sha256.clone());
        }
        // Failed-only present files already threaded their digest via `preverified` above.
        Ok(preverified)
    }
}

/// Moves a mismatched workspace file aside under the run metadata
/// quarantine directory so it can neither influence a later resume nor be
/// silently left in place after a fail-closed refusal (E06). Best-effort:
/// a quarantine failure never masks the original refusal.
/// Quarantine-parent pinning (E06): the parent is created then verified
/// post-create with `symlink_metadata` fail-closed. On Unix the ideal is
/// `O_NOFOLLOW` open + `mkdirat` semantics; the portable minimum here
/// verifies every component from the metadata root to the quarantine parent
/// is not a symlink after `create_dir_all`, and refuses the move (leaving
/// the original in place with a warn) when it is.
fn quarantine_workspace_file(
    workspace: &camino::Utf8Path,
    metadata: &camino::Utf8Path,
    path: &camino::Utf8Path,
    candidate: &camino::Utf8Path,
) {
    let quarantine = metadata.join("quarantine").join(path);
    if let Some(parent) = quarantine.parent()
        && let Err(error) = std::fs::create_dir_all(parent.as_std_path())
    {
        tracing::warn!(
            workspace = %workspace,
            path = %path,
            error = %error,
            "workspace quarantine parent creation failed; the mismatched file was left in place"
        );
        return;
    }
    // Post-create pin: every component from the metadata root to the
    // quarantine parent must not be a symlink; otherwise the move could be
    // redirected outside the run metadata dir (E06).
    if let Some(parent) = quarantine.parent()
        && let Err(error) = reject_symlinked_workspace_path(metadata, parent)
    {
        tracing::warn!(
            workspace = %workspace,
            path = %path,
            error = %error,
            "workspace quarantine parent is symlinked; the mismatched file was left in place"
        );
        return;
    }
    if std::fs::rename(candidate.as_std_path(), quarantine.as_std_path()).is_err() {
        // A rename across filesystems or over an existing quarantine entry
        // falls back to copy-then-remove; every failure is best-effort.
        if std::fs::copy(candidate.as_std_path(), quarantine.as_std_path()).is_ok() {
            let _ = std::fs::remove_file(candidate.as_std_path());
        } else {
            // Operational note (E06): the mismatched bytes stay in place, so
            // every later resume re-detects and re-refuses them — noisy but
            // fail-closed. Remove or restore the file out of band to stop
            // the repeat refusals; never hand-edit it into a different
            // tampered shape.
            tracing::warn!(
                workspace = %workspace,
                path = %path,
                "workspace quarantine failed; the mismatched file was left in place"
            );
        }
    }
}

/// Rejects a workspace candidate whose terminal component or any parent
/// strictly inside the workspace is a symlink. Only the workspace-relative
/// prefix is walked (top-down from the workspace root): ancestors above the
/// root (for example a symlinked `/var` on macOS) belong to the platform,
/// not to the run, and the run setup already anchors on the canonical root.
/// Inspection failures fail closed: an unreadable path is refused, never
/// treated as safe (E06).
fn reject_symlinked_workspace_path(
    workspace: &camino::Utf8Path,
    candidate: &camino::Utf8Path,
) -> Result<(), std::io::Error> {
    if is_symlink_no_follow(candidate)? {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("`{candidate}` is a symbolic link"),
        ));
    }
    let relative = candidate.strip_prefix(workspace).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("`{candidate}` is outside the workspace"),
        )
    })?;
    let Some(parent) = relative.parent() else {
        return Ok(());
    };
    let mut current = workspace.to_path_buf();
    for component in parent.components() {
        let camino::Utf8Component::Normal(part) = component else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("`{candidate}` is outside the workspace"),
            ));
        };
        current = current.join(part);
        if is_symlink_no_follow(&current)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("parent `{current}` of `{candidate}` is a symbolic link"),
            ));
        }
    }
    Ok(())
}

/// Verifies a historical revision against its immutable checkpoint blob.
/// Every current writer stores the blob with the pin, so a missing blob is
/// corruption rather than a resumable state (E06).
fn verify_pinned_blob(
    metadata: &camino::Utf8Path,
    path: &camino::Utf8Path,
    sha256: &str,
    limits: &RuntimeLimits,
) -> Result<(), EngineError> {
    // Blob digests name store paths: validate hex shape before any stat so
    // a malformed journal pin cannot probe arbitrary paths (E06).
    if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(EngineError::Failed(format!(
            "cannot safely resume: historical blob for `{path}` has a malformed sha256 digest"
        )));
    }
    let blob = metadata.join("checkpoint-blobs").join(sha256);
    // A symlink inside the blob store is never followed: it can only be
    // planted tampering, and resolving it could escape the store (E06).
    // Both the leaf and every parent strictly inside the metadata dir are
    // inspected so a swapped parent cannot redirect the blob read.
    // Inspection failures propagate with context instead of reading as
    // absent: an unreadable blob path refuses resume (E06).
    reject_symlinked_workspace_path(metadata, &blob).map_err(|error| {
        EngineError::Failed(format!(
            "cannot safely resume: historical blob for `{path}` is not a directly rooted regular file: {error}"
        ))
    })?;
    let digest = hash_file(&blob, limits.output_file_limit_bytes).map_err(|error| {
        // A permission (or other non-NotFound) failure is unreadable, not
        // missing: conflating them would misdirect the operator toward
        // restoring a blob that is present but inaccessible (E06).
        if error.kind() == std::io::ErrorKind::NotFound {
            EngineError::Failed(format!(
                "cannot safely resume: historical blob for `{path}` is missing: {error}"
            ))
        } else {
            EngineError::Failed(format!(
                "cannot safely resume: historical blob for `{path}` is unreadable: {error}"
            ))
        }
    })?;
    if digest.sha256 != sha256 {
        return Err(EngineError::Failed(format!(
            "cannot safely resume: historical blob for `{path}` was tampered (expected {sha256}, got {})",
            digest.sha256
        )));
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) struct ExecutionEnv<'a> {
    pub(crate) context: &'a RunContext,
    pub(crate) journal: &'a JournalWriter,
}

#[derive(Clone)]
pub(crate) struct BudgetTracker {
    pub(crate) max_total_steps: usize,
    pub(crate) executed_steps: Arc<AtomicUsize>,
}

impl BudgetTracker {
    pub(crate) fn new(max_total_steps: usize, executed_steps: usize) -> Self {
        Self {
            max_total_steps: max_total_steps.max(1),
            executed_steps: Arc::new(AtomicUsize::new(executed_steps)),
        }
    }

    pub(crate) fn consume(&self, node_id: &str) -> Result<(), StepError> {
        let result =
            self.executed_steps
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |executed| {
                    (executed < self.max_total_steps).then_some(executed.saturating_add(1))
                });
        if let Err(executed) = result {
            return Err(StepError::failed(
                node_id,
                format!(
                    "global step budget exceeded: {} >= {}",
                    executed, self.max_total_steps
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeOutcome, RunState};
    use qcg_types::NodePath;
    use sha2::{Digest as _, Sha256};

    fn digest(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    #[test]
    fn failed_only_revisions_do_not_require_a_workspace_projection() {
        // E06: a path pinned only by a failed step has a verified blob but
        // no expected workspace revision; requiring one would reject a
        // legitimate resume.
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v1 = b"failed revision";
        let digest = digest(v1);
        std::fs::write(
            metadata
                .join("checkpoint-blobs")
                .join(&digest)
                .as_std_path(),
            v1,
        )
        .expect("blob");
        // No workspace file, no node outcome, no latest pin: the revision
        // only exists in the historical map.
        let mut state = RunState::default();
        state
            .historical_file_pins
            .entry("out.txt".to_string())
            .or_default()
            .insert(digest);
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect("failed-only revisions must verify by blob alone");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn historical_symlink_blob_refuses_resume() {
        // E06: a symlink planted in the blob store is refused instead of
        // followed, even when it points at content with the right bytes.
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v1 = b"v1";
        let digest = digest(v1);
        let outside = root.join("outside.txt");
        std::fs::write(&outside, v1).expect("outside file");
        std::os::unix::fs::symlink(
            &outside,
            metadata
                .join("checkpoint-blobs")
                .join(&digest)
                .as_std_path(),
        )
        .expect("planted symlink");
        std::fs::write(workspace.join("out.txt").as_std_path(), v1).expect("workspace v1");
        let mut state = RunState::default();
        state.nodes.insert(
            NodePath::root("build"),
            NodeOutcome::Success {
                output: None,
                files: vec![FilePin {
                    path: camino::Utf8PathBuf::from("out.txt"),
                    sha256: digest.clone(),
                }],
            },
        );
        state.latest_file_pins.insert("out.txt".to_string(), digest);
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        let error = replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("a symlinked blob must refuse resume");
        assert!(
            error.to_string().contains("symbolic link"),
            "the planted link must be named: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn folded_journal_latest_drives_projection_not_name_order() {
        // E06 three-way binding: fold journal events (journal order, not
        // node name order) into pins, then verify the projected workspace
        // against those pins. Neither hand-built state nor the fold alone
        // proves the chain; this test runs fold → latest → verify end to
        // end, mirroring what resume and fork projection consume.
        let root = std::env::temp_dir().join(format!("qcg-replay-fold-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v1 = b"v1";
        let v2 = b"v2";
        let d1 = digest(v1);
        let d2 = digest(v2);
        for (bytes, d) in [(v1.as_slice(), &d1), (v2.as_slice(), &d2)] {
            std::fs::write(
                metadata.join("checkpoint-blobs").join(d).as_std_path(),
                bytes,
            )
            .expect("blob");
        }
        std::fs::write(workspace.join("out.txt").as_std_path(), v2).expect("workspace v2");
        let events = ["z_first", "a_second"]
            .into_iter()
            .zip([d1.clone(), d2.clone()])
            .enumerate()
            .map(|(index, (node, digest))| {
                serde_json::json!({
                    "t": "step_finished",
                    "seq": index as u64 + 1,
                    "ts": "2026-01-01T00:00:00Z",
                    "run_id": "fold-order",
                    "trace_id": "fold-order-trace",
                    "span_id": "fold-order-span",
                    "node": node,
                    "status": "success",
                    "files": [{"path": "out.txt", "sha256": digest}],
                })
            })
            .collect::<Vec<_>>();
        let state = RunState::fold_values(&events).expect("journal should fold");
        assert_eq!(
            state.latest_file_pins.get("out.txt").map(String::as_str),
            Some(d2.as_str()),
            "fold must resolve journal order, not name order"
        );
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect("folded latest projection must verify");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn journal_order_decides_latest_not_node_name_order() {
        // E06: z_first writes v1 and a_second overwrites v2 for the same
        // path; resume projects v2 (journal order), never v1 (name order).
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v1 = b"v1";
        let v2 = b"v2";
        let d1 = digest(v1);
        let d2 = digest(v2);
        for (bytes, d) in [(v1.as_slice(), &d1), (v2.as_slice(), &d2)] {
            std::fs::write(
                metadata.join("checkpoint-blobs").join(d).as_std_path(),
                bytes,
            )
            .expect("blob");
        }
        std::fs::write(workspace.join("out.txt").as_std_path(), v2).expect("workspace v2");
        let mut state = RunState::default();
        for (node, d) in [("z_first", d1.clone()), ("a_second", d2.clone())] {
            state.nodes.insert(
                NodePath::root(node),
                NodeOutcome::Success {
                    output: None,
                    files: vec![FilePin {
                        path: camino::Utf8PathBuf::from("out.txt"),
                        sha256: d,
                    }],
                },
            );
        }
        // Journal-latest write wins regardless of node name order.
        state
            .latest_file_pins
            .insert("out.txt".to_string(), d2.clone());
        for d in [d1.clone(), d2.clone()] {
            state
                .historical_file_pins
                .entry("out.txt".to_string())
                .or_default()
                .insert(d);
        }
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect("v2 projection must verify");
        // A rolled-back workspace (v1) is refused, not resumed.
        std::fs::write(workspace.join("out.txt").as_std_path(), v1).expect("workspace v1");
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        let error = replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("a rolled-back projection must refuse resume");
        assert!(
            error.to_string().contains("latest pinned revision"),
            "{error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn tampered_workspace_or_blob_refuses_resume() {
        // E06 checklist: a tampered latest file or a tampered history blob
        // refuses resume.
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v2 = b"v2";
        let d2 = digest(v2);
        std::fs::write(
            metadata.join("checkpoint-blobs").join(&d2).as_std_path(),
            v2,
        )
        .expect("blob");
        std::fs::write(workspace.join("out.txt").as_std_path(), v2).expect("workspace v2");
        let mut state = RunState::default();
        state.nodes.insert(
            NodePath::root("build"),
            NodeOutcome::Success {
                output: None,
                files: vec![FilePin {
                    path: camino::Utf8PathBuf::from("out.txt"),
                    sha256: d2.clone(),
                }],
            },
        );
        state
            .latest_file_pins
            .insert("out.txt".to_string(), d2.clone());
        state
            .historical_file_pins
            .entry("out.txt".to_string())
            .or_default()
            .insert(d2.clone());
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        // Tampered workspace bytes are refused.
        std::fs::write(workspace.join("out.txt").as_std_path(), b"tampered").expect("tamper");
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("tampered workspace must refuse resume");
        // Tampered blob bytes are refused.
        std::fs::write(workspace.join("out.txt").as_std_path(), v2).expect("restore");
        std::fs::write(
            metadata.join("checkpoint-blobs").join(&d2).as_std_path(),
            b"tampered",
        )
        .expect("tamper blob");
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("tampered blob must refuse resume");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn historical_pins_missing_blobs_refuse_resume() {
        // E06: every revision recorded in the durable historical map needs
        // its immutable blob, even when a later revision is the workspace
        // latest and its own blob exists.
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v1 = b"v1";
        let v2 = b"v2";
        let latest = FilePin {
            path: camino::Utf8PathBuf::from("out.txt"),
            sha256: digest(v2),
        };
        std::fs::write(
            metadata
                .join("checkpoint-blobs")
                .join(&latest.sha256)
                .as_std_path(),
            v2,
        )
        .expect("v2 blob");
        std::fs::write(workspace.join("out.txt").as_std_path(), v2).expect("workspace v2");
        let mut state = RunState::default();
        state.nodes.insert(
            NodePath::root("build"),
            NodeOutcome::Success {
                output: None,
                files: vec![latest.clone()],
            },
        );
        state
            .latest_file_pins
            .insert("out.txt".to_string(), latest.sha256.clone());
        state
            .historical_file_pins
            .entry("out.txt".to_string())
            .or_default()
            .insert(digest(v1));
        state
            .historical_file_pins
            .get_mut("out.txt")
            .expect("entry")
            .insert(latest.sha256.clone());
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        let error = replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("a missing historical blob must refuse resume");
        assert!(
            error.to_string().contains("historical blob"),
            "the missing revision must be named: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn historical_revisions_verify_against_blobs_not_the_workspace() {
        // E06: step A pinned v1, step B overwrote the same path with v2.
        // Resume must verify A against its immutable blob and the workspace
        // against the latest revision, not reject A because v1 is gone.
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v1 = b"v1";
        let v2 = b"v2";
        let pin1 = FilePin {
            path: camino::Utf8PathBuf::from("out.txt"),
            sha256: digest(v1),
        };
        let pin2 = FilePin {
            path: camino::Utf8PathBuf::from("out.txt"),
            sha256: digest(v2),
        };
        std::fs::write(
            metadata
                .join("checkpoint-blobs")
                .join(&pin1.sha256)
                .as_std_path(),
            v1,
        )
        .expect("v1 blob");
        std::fs::write(
            metadata
                .join("checkpoint-blobs")
                .join(&pin2.sha256)
                .as_std_path(),
            v2,
        )
        .expect("v2 blob");
        std::fs::write(workspace.join("out.txt").as_std_path(), v2).expect("workspace v2");
        let mut state = RunState::default();
        state.nodes.insert(
            NodePath::root("a"),
            NodeOutcome::Success {
                output: None,
                files: vec![pin1.clone()],
            },
        );
        state.nodes.insert(
            NodePath::root("b"),
            NodeOutcome::Success {
                output: None,
                files: vec![pin2.clone()],
            },
        );
        state
            .latest_file_pins
            .insert("out.txt".to_string(), pin2.sha256.clone());
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect("a later revision must not block resume");
        // Restoring the older genuine revision is a rollback, not a resume.
        std::fs::write(workspace.join("out.txt").as_std_path(), v1).expect("rollback");
        assert!(
            replay
                .verify_files(&workspace, &metadata, &limits, &accounting)
                .is_err(),
            "the workspace must project the latest pinned revision"
        );
        std::fs::write(workspace.join("out.txt").as_std_path(), v2).expect("restore");
        // A tampered workspace projection is rejected.
        std::fs::write(workspace.join("out.txt").as_std_path(), b"tampered").expect("tamper");
        assert!(
            replay
                .verify_files(&workspace, &metadata, &limits, &accounting)
                .is_err(),
            "tampered workspace must be rejected"
        );
        std::fs::write(workspace.join("out.txt").as_std_path(), v2).expect("restore");
        // A tampered historical blob is rejected.
        std::fs::write(
            metadata
                .join("checkpoint-blobs")
                .join(&pin1.sha256)
                .as_std_path(),
            b"tampered",
        )
        .expect("tamper blob");
        assert!(
            replay
                .verify_files(&workspace, &metadata, &limits, &accounting)
                .is_err(),
            "tampered historical blob must be rejected"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn workspace_terminal_symlink_refuses_resume() {
        // E06: the workspace projection follows the same symlink standard
        // as the blob side. A terminal symlink at the pinned path is
        // refused even when it points at bytes matching the pin.
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v1 = b"v1";
        let digest = digest(v1);
        std::fs::write(
            metadata
                .join("checkpoint-blobs")
                .join(&digest)
                .as_std_path(),
            v1,
        )
        .expect("blob");
        let outside = root.join("outside.txt");
        std::fs::write(&outside, v1).expect("outside file");
        std::os::unix::fs::symlink(&outside, workspace.join("out.txt").as_std_path())
            .expect("planted terminal symlink");
        let mut state = RunState::default();
        state.nodes.insert(
            NodePath::root("build"),
            NodeOutcome::Success {
                output: None,
                files: vec![FilePin {
                    path: camino::Utf8PathBuf::from("out.txt"),
                    sha256: digest.clone(),
                }],
            },
        );
        state.latest_file_pins.insert("out.txt".to_string(), digest);
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        let error = replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("a terminal symlink must refuse resume");
        assert!(
            error.to_string().contains("symbolic link"),
            "the planted link must be named: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn workspace_parent_symlink_refuses_resume() {
        // E06: a symlinked parent directory between the workspace root and
        // the pinned file is refused, matching the blob-side standard, so
        // verification and use never diverge.
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.join("sub").as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v1 = b"v1";
        let digest = digest(v1);
        std::fs::write(
            metadata
                .join("checkpoint-blobs")
                .join(&digest)
                .as_std_path(),
            v1,
        )
        .expect("blob");
        let outside = root.join("outside-dir");
        std::fs::create_dir_all(&outside).expect("outside dir");
        std::fs::write(outside.join("out.txt"), v1).expect("outside file");
        std::fs::remove_dir_all(workspace.join("sub").as_std_path()).expect("remove real parent");
        std::os::unix::fs::symlink(&outside, workspace.join("sub").as_std_path())
            .expect("planted parent symlink");
        let mut state = RunState::default();
        state.nodes.insert(
            NodePath::root("build"),
            NodeOutcome::Success {
                output: None,
                files: vec![FilePin {
                    path: camino::Utf8PathBuf::from("sub/out.txt"),
                    sha256: digest.clone(),
                }],
            },
        );
        state
            .latest_file_pins
            .insert("sub/out.txt".to_string(), digest);
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        let error = replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("a parent symlink must refuse resume");
        assert!(
            error.to_string().contains("symbolic link"),
            "the planted parent link must be named: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn hash_file_refuses_a_symlink_leaf_without_following_it() {
        // E06: `hash_file` opens with O_NOFOLLOW, so a symlink leaf is
        // refused even when its target holds bytes with a known digest.
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).expect("root dir");
        let target = root.join("target.txt");
        std::fs::write(&target, b"v1").expect("target file");
        let link = camino::Utf8PathBuf::from_path_buf(root.join("link.txt"))
            .expect("link path must be UTF-8");
        std::os::unix::fs::symlink(&target, link.as_std_path()).expect("planted symlink");
        let error = match hash_file(&link, None) {
            Ok(_) => panic!("a symlink leaf must be refused"),
            Err(error) => error,
        };
        assert!(
            error.kind() == std::io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(libc::ELOOP),
            "the refusal must come from O_NOFOLLOW, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_blob_digest_verifies_once_for_two_paths() {
        // E06: two workspace paths pinning identical bytes name one
        // immutable blob. Single-pass collection plus the per-digest
        // in-memory cache verifies it once and projects both paths.
        let root = std::env::temp_dir().join(format!("qcg-replay-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let shared = b"shared bytes";
        let digest = digest(shared);
        std::fs::write(
            metadata
                .join("checkpoint-blobs")
                .join(&digest)
                .as_std_path(),
            shared,
        )
        .expect("shared blob");
        std::fs::write(workspace.join("a.txt").as_std_path(), shared).expect("workspace a");
        std::fs::write(workspace.join("b.txt").as_std_path(), shared).expect("workspace b");
        let mut state = RunState::default();
        for (node, path) in [("first", "a.txt"), ("second", "b.txt")] {
            state.nodes.insert(
                NodePath::root(node),
                NodeOutcome::Success {
                    output: None,
                    files: vec![FilePin {
                        path: camino::Utf8PathBuf::from(path),
                        sha256: digest.clone(),
                    }],
                },
            );
            state
                .latest_file_pins
                .insert(path.to_string(), digest.clone());
        }
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect("one shared blob must verify both projections");
        // Removing the single blob breaks both projections at once.
        std::fs::remove_file(
            metadata
                .join("checkpoint-blobs")
                .join(&digest)
                .as_std_path(),
        )
        .expect("remove shared blob");
        let error = replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("a missing shared blob must refuse resume");
        assert!(
            error.to_string().contains("historical blob"),
            "the missing blob must be named: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn quarantine_file_exists_with_expected_content_after_mismatch() {
        // E06: a tampered workspace projection is quarantined aside with
        // its mismatched bytes preserved for forensics, not silently left
        // in place.
        let root = std::env::temp_dir().join(format!("qcg-replay-quar-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let v2 = b"v2";
        let d2 = digest(v2);
        std::fs::write(
            metadata.join("checkpoint-blobs").join(&d2).as_std_path(),
            v2,
        )
        .expect("blob");
        std::fs::write(workspace.join("out.txt").as_std_path(), b"tampered")
            .expect("tampered workspace");
        let mut state = RunState::default();
        state.nodes.insert(
            NodePath::root("build"),
            NodeOutcome::Success {
                output: None,
                files: vec![FilePin {
                    path: camino::Utf8PathBuf::from("out.txt"),
                    sha256: d2.clone(),
                }],
            },
        );
        state
            .latest_file_pins
            .insert("out.txt".to_string(), d2.clone());
        let replay = JournalReplay::from_state(state);
        let limits = RuntimeLimits::default();
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("tampered workspace must refuse resume");
        let quarantined = metadata.join("quarantine").join("out.txt");
        let bytes = std::fs::read(quarantined.as_std_path())
            .expect("quarantine file must exist after mismatch");
        assert_eq!(
            bytes, b"tampered",
            "quarantine must preserve the mismatched bytes"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unsafe_path_and_malformed_sha_are_rejected() {
        // E06: absolute/traversing journal paths and malformed blob digests
        // fail closed with explicit messages.
        let root = std::env::temp_dir().join(format!("qcg-replay-unsafe-{}", uuid::Uuid::now_v7()));
        let workspace = camino::Utf8PathBuf::from_path_buf(root.join("workspace"))
            .expect("workspace path must be UTF-8");
        let metadata = camino::Utf8PathBuf::from_path_buf(root.join("meta"))
            .expect("metadata path must be UTF-8");
        std::fs::create_dir_all(workspace.as_std_path()).expect("workspace dir");
        std::fs::create_dir_all(metadata.join("checkpoint-blobs").as_std_path()).expect("blob dir");
        let limits = RuntimeLimits::default();
        // Unsafe absolute path.
        let mut unsafe_state = RunState::default();
        unsafe_state.nodes.insert(
            NodePath::root("build"),
            NodeOutcome::Success {
                output: None,
                files: vec![FilePin {
                    path: camino::Utf8PathBuf::from("/abs/out.txt"),
                    sha256: "a".repeat(64),
                }],
            },
        );
        let replay = JournalReplay::from_state(unsafe_state);
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        let error = replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("absolute journal path must be rejected");
        assert!(
            error.to_string().contains("unsafe output path"),
            "unsafe path must be named: {error}"
        );
        // Traversing path.
        let mut traverse_state = RunState::default();
        traverse_state.nodes.insert(
            NodePath::root("build"),
            NodeOutcome::Success {
                output: None,
                files: vec![FilePin {
                    path: camino::Utf8PathBuf::from("../escape.txt"),
                    sha256: "b".repeat(64),
                }],
            },
        );
        let replay = JournalReplay::from_state(traverse_state);
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("traversing journal path must be rejected");
        // Malformed sha.
        let mut malformed_state = RunState::default();
        malformed_state
            .historical_file_pins
            .entry("out.txt".to_string())
            .or_default()
            .insert("not-hex".to_string());
        let replay = JournalReplay::from_state(malformed_state);
        let accounting = Arc::new(Mutex::new(CheckpointAccounting::default()));
        let error = replay
            .verify_files(&workspace, &metadata, &limits, &accounting)
            .expect_err("malformed sha must be rejected");
        assert!(
            error.to_string().contains("malformed sha256"),
            "malformed digest must be named: {error}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
