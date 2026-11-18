//! Private protocol plumbing for a future prepared-fork caller, not an HTTP API.
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

/// A plan identity, not the plan itself. Construction and decoding are checked.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "BindingWire")]
pub struct ForkPlanBinding {
    binding_version: u8,
    plan_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reset_candidate_sha256: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingWire {
    binding_version: u8,
    plan_digest: String,
    #[serde(default)]
    reset_candidate_sha256: Option<String>,
}
impl TryFrom<BindingWire> for ForkPlanBinding {
    type Error = String;
    fn try_from(value: BindingWire) -> Result<Self, Self::Error> {
        if value.binding_version != 1
            || !valid_hash(&value.plan_digest)
            || value
                .reset_candidate_sha256
                .as_deref()
                .is_some_and(|digest| !valid_hash(digest))
        {
            return Err("invalid fork plan binding".into());
        }
        Ok(Self {
            binding_version: 1,
            plan_digest: value.plan_digest,
            reset_candidate_sha256: value.reset_candidate_sha256,
        })
    }
}
fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Exact source-relative regular files used by checkpoint/package projection.
/// Unlisted bytes are never read by the projection: it reads the owned staging tree.
#[doc(hidden)]
#[derive(Debug, Clone, Serialize)]
pub struct ForkFileObservation {
    pub path: Utf8PathBuf,
    pub length: u64,
    pub sha256: String,
}
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct PreparedForkPlan {
    binding: ForkPlanBinding,
    files: Vec<ForkFileObservation>,
    candidate: Option<engine::ResetPlanCandidate>,
}

#[cfg(test)]
thread_local! {
    static SOURCE_OPENED: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

// Lives inside the child admission, never in a separate temporary namespace.
// Until projection, package copy and removal finish, the child remains pending;
// existing incomplete-admission recovery owns removal after a process crash.
pub(crate) struct StagedSource(pub Utf8PathBuf);
impl StagedSource {
    pub(crate) fn finish(self) -> Result<(), crate::ServiceError> {
        std::fs::remove_dir_all(&self.0)?;
        Ok(())
    }
}
impl Drop for StagedSource {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn invalid(detail: &str) -> crate::ServiceError {
    crate::ServiceError::Invalid(format!("prepared fork: {detail}"))
}

// Pin and walk every ancestor, not just the leaf. No source link is followed.
#[cfg(unix)]
fn open_source(root: &Utf8Path, relative: &Utf8Path) -> Result<std::fs::File, crate::ServiceError> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::fs::OpenOptionsExt;
    let mut parent = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(root)?;
    let parts: Vec<_> = relative.components().collect();
    for (index, part) in parts.iter().enumerate() {
        let name = std::ffi::CString::new(part.as_str()).map_err(|_| invalid("invalid path"))?;
        let flags = libc::O_RDONLY
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | libc::O_NONBLOCK
            | if index + 1 < parts.len() {
                libc::O_DIRECTORY
            } else {
                0
            };
        let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        parent = unsafe { std::fs::File::from_raw_fd(fd) };
    }
    Ok(parent)
}
#[cfg(not(unix))]
fn open_source(
    _root: &Utf8Path,
    _relative: &Utf8Path,
) -> Result<std::fs::File, crate::ServiceError> {
    // No race-safe ancestor traversal implementation on this platform yet.
    Err(invalid(
        "no-follow source staging is unsupported on this platform",
    ))
}

impl PreparedForkPlan {
    /// Bind a diagnostic identity AND the complete projection file manifest.
    /// The engine diagnostic alone is not a prepared plan. No independent final
    /// binding can be supplied, and neither the manifest nor binding is mutable.
    /// Include metadata, referenced blobs and the pinned package, not merely the
    /// engine diagnostic's observed-file subset. Missing inputs fail projection.
    #[doc(hidden)]
    pub fn new(
        diagnostic_digest: &str,
        files: Vec<ForkFileObservation>,
    ) -> Result<Self, crate::ServiceError> {
        if !valid_hash(diagnostic_digest) {
            return Err(invalid("invalid diagnostic digest"));
        }
        Self::bind("qcg-prepared-fork-v1", diagnostic_digest, files)
    }

    /// Bind opaque source diagnostic evidence and the complete file manifest.
    /// This retains the candidate for an internal child overlay; it does not
    /// authorize continuation. A digest alone cannot supply the reset payload.
    /// Every present file read by analysis must occur at its exact source-relative
    /// path and SHA in the full manifest; every observed missing path must remain
    /// absent. Unrelated projection inputs remain bound.
    /// This is not an analysis-to-copy TOCTOU guarantee.
    #[doc(hidden)]
    pub fn from_reset_candidate(
        candidate: &engine::ResetPlanCandidate,
        files: Vec<ForkFileObservation>,
    ) -> Result<Self, crate::ServiceError> {
        let requirements = candidate
            .required_file_observations()
            .ok_or_else(|| invalid("reset candidate lacks live file observations"))?;
        let mut plan = Self::bind_reset_evidence(candidate, files, requirements)?;
        plan.candidate = Some(candidate.clone());
        Ok(plan)
    }

    fn bind_reset_evidence<'a, T: Serialize>(
        candidate: &T,
        files: Vec<ForkFileObservation>,
        requirements: impl Iterator<Item = (&'a Utf8Path, Option<&'a str>)>,
    ) -> Result<Self, crate::ServiceError> {
        let mut plan = Self::bind("qcg-prepared-reset-candidate-v1", candidate, files)?;
        for (path, sha256) in requirements {
            let observation = plan
                .files
                .binary_search_by(|file| file.path.as_path().cmp(path))
                .ok()
                .map(|index| &plan.files[index]);
            if observation.map(|file| file.sha256.as_str()) != sha256 {
                return Err(invalid("manifest does not match reset file observations"));
            }
            // Preserve NotFound for absent reads: a staged descendant would
            // create a directory at the path, while a staged file ancestor
            // would change the lookup to NotADirectory. Paths are normalized
            // before these component-aware prefix checks.
            if sha256.is_none()
                && plan.files.iter().any(|file| {
                    file.path.as_path() != path
                        && (file.path.starts_with(path) || path.starts_with(&file.path))
                })
            {
                return Err(invalid("manifest changes a path observed missing"));
            }
        }
        let candidate_wire = serde_json::to_value(candidate)?;
        plan.binding.reset_candidate_sha256 = Some(hex::encode(Sha256::digest(
            serde_json::to_vec(&candidate_wire)?,
        )));
        Ok(plan)
    }

    fn bind<T: Serialize + ?Sized>(
        domain: &str,
        diagnostic: &T,
        mut files: Vec<ForkFileObservation>,
    ) -> Result<Self, crate::ServiceError> {
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let mut previous = None;
        for observation in &files {
            let path = &observation.path;
            if path.as_str().is_empty()
                || !path
                    .components()
                    .all(|p| matches!(p, camino::Utf8Component::Normal(_)))
                || path.as_str().contains(['\\', ':', '\0'])
                || path
                    .components()
                    .map(|p| p.as_str())
                    .collect::<Vec<_>>()
                    .join("/")
                    != path.as_str()
                || previous == Some(path)
                || !valid_hash(&observation.sha256)
            {
                return Err(invalid("unsafe, duplicate or malformed observation"));
            }
            previous = Some(path);
        }
        // Domain-separated, deterministic encoding of the full sorted manifest.
        // This is a composite service identity, NOT the engine diagnostic digest.
        let identity = serde_json::to_vec(&(domain, diagnostic, &files))?;
        Ok(Self {
            binding: ForkPlanBinding {
                binding_version: 1,
                plan_digest: hex::encode(Sha256::digest(identity)),
                reset_candidate_sha256: None,
            },
            files,
            candidate: None,
        })
    }

    pub(crate) fn reset_bound(&self) -> bool {
        self.binding.reset_candidate_sha256.is_some()
    }

    pub(crate) fn reset_overlay(&self) -> Result<Option<serde_json::Value>, crate::ServiceError> {
        self.candidate
            .as_ref()
            .map(|candidate| {
                candidate
                    .fork_overlay_evidence(serde_json::to_value(&self.binding)?)
                    .map_err(crate::ServiceError::invalid)
            })
            .transpose()
    }

    pub fn binding(&self) -> &ForkPlanBinding {
        &self.binding
    }

    pub(crate) fn stage(
        &self,
        source: &Utf8Path,
        target: &Utf8Path,
    ) -> Result<StagedSource, crate::ServiceError> {
        // The caller owns target admission and has published its pending marker.
        // One fixed tree per child; a retry first wipes incomplete admission.
        let root = target.join(".prepared-source");
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().mode(0o700).create(&root)?;
        }
        #[cfg(not(unix))]
        std::fs::create_dir(&root)?;
        let staged = StagedSource(root);
        for observation in &self.files {
            let path = &observation.path;
            let mut input = open_source(source, path)?;
            let metadata = input.metadata()?;
            if !metadata.is_file() || metadata.len() != observation.length {
                return Err(invalid("source type or length changed"));
            }
            #[cfg(test)]
            SOURCE_OPENED.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            let destination = staged.0.join(path);
            std::fs::create_dir_all(destination.parent().unwrap())?;
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&destination)?;
            let mut hash = Sha256::new();
            let mut length = 0u64;
            let mut buffer = [0; 8192];
            loop {
                let count = input.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                length = length
                    .checked_add(count as u64)
                    .ok_or_else(|| invalid("source length overflow"))?;
                if length > observation.length {
                    return Err(invalid("source grew during copy"));
                }
                hash.update(&buffer[..count]);
                output.write_all(&buffer[..count])?;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                // Copy only ordinary permission bits: a staged source must
                // never inherit setuid/setgid/sticky bits from the source.
                let mode = metadata.permissions().mode() & 0o777;
                output.set_permissions(std::fs::Permissions::from_mode(mode))?;
            }
            #[cfg(not(unix))]
            output.set_permissions(metadata.permissions())?;
            if length != observation.length || hex::encode(hash.finalize()) != observation.sha256 {
                return Err(invalid("source changed during copy"));
            }
        }
        Ok(staged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::support::*;
    use serde_json::json;

    fn binding() -> ForkPlanBinding {
        serde_json::from_value(json!({"binding_version":1,"plan_digest":"a".repeat(64)})).unwrap()
    }
    #[test]
    fn binding_is_strict_and_canonical() {
        for value in [
            json!({"binding_version":2,"plan_digest":"a".repeat(64)}),
            json!({"binding_version":1,"plan_digest":"A".repeat(64)}),
            json!({"binding_version":1,"plan_digest":"a".repeat(63)}),
            json!({"binding_version":1,"plan_digest":"a".repeat(64),"extra":true}),
        ] {
            assert!(serde_json::from_value::<ForkPlanBinding>(value).is_err());
        }
        assert_eq!(
            serde_json::to_value(binding()).unwrap(),
            json!({"binding_version":1,"plan_digest":"a".repeat(64)})
        );
    }
    #[test]
    #[cfg(unix)]
    fn staging_hashes_the_copied_stream_and_refuses_link_and_length_races() {
        let root = temp_run_dir("prepared-copy");
        let _guard = TempGuard(root.clone());
        let source = root.join("source");
        std::fs::create_dir_all(&source).unwrap();
        let path = source.join("data");
        let bytes = b"prepared bytes";
        let plan = PreparedForkPlan::new(
            &"a".repeat(64),
            vec![ForkFileObservation {
                path: "data".into(),
                length: bytes.len() as u64,
                sha256: hex::encode(Sha256::digest(bytes)),
            }],
        )
        .unwrap();
        let target = root.join("child");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(&path, bytes).unwrap();
        let staged = plan.stage(&source, &target).unwrap();
        assert_eq!(std::fs::read(staged.0.join("data")).unwrap(), bytes);
        let staging_path = staged.0.clone();
        drop(staged);
        assert!(!staging_path.exists());
        for replacement in [
            b"different data".to_vec(),
            b"short".to_vec(),
            b"prepared bytes with growth".to_vec(),
        ] {
            std::fs::write(&path, bytes).unwrap();
            let changing = path.clone();
            SOURCE_OPENED.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    std::fs::write(&changing, replacement).unwrap();
                }))
            });
            assert!(plan.stage(&source, &target).is_err());
        }
        std::fs::remove_file(&path).unwrap();
        let outside = root.join("outside");
        std::fs::write(&outside, bytes).unwrap();
        std::os::unix::fs::symlink(&outside, &path).unwrap();
        assert!(plan.stage(&source, &target).is_err());
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&root, source.join("ancestor")).unwrap();
        let linked = PreparedForkPlan::new(
            &"a".repeat(64),
            vec![ForkFileObservation {
                path: "ancestor/outside".into(),
                length: bytes.len() as u64,
                sha256: hex::encode(Sha256::digest(bytes)),
            }],
        )
        .unwrap();
        assert!(linked.stage(&source, &target).is_err());
        assert!(!target.join(".prepared-source").exists());
    }
    #[test]
    #[cfg(unix)]
    fn staging_strips_privilege_bits_from_source_files() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = temp_run_dir("prepared-copy-modes");
        let _guard = TempGuard(root.clone());
        let source = root.join("source");
        std::fs::create_dir_all(&source).unwrap();
        let path = source.join("data");
        let bytes = b"mode bytes";
        std::fs::write(&path, bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o6755)).unwrap();
        let plan = PreparedForkPlan::new(
            &"a".repeat(64),
            vec![ForkFileObservation {
                path: "data".into(),
                length: bytes.len() as u64,
                sha256: hex::encode(Sha256::digest(bytes)),
            }],
        )
        .unwrap();
        let target = root.join("child");
        std::fs::create_dir(&target).unwrap();
        let staged = plan.stage(&source, &target).unwrap();
        let mode = std::fs::metadata(staged.0.join("data"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o7777,
            0o755,
            "staged copy must drop setuid/setgid/sticky bits but keep ordinary mode"
        );
    }
    #[cfg(unix)]
    fn observe_tree(root: &Utf8Path, dir: &Utf8Path) -> Vec<ForkFileObservation> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = Utf8PathBuf::from_path_buf(entry.path()).unwrap();
            if entry.file_type().unwrap().is_dir() {
                files.extend(observe_tree(root, &path));
            } else {
                let bytes = std::fs::read(&path).unwrap();
                files.push(ForkFileObservation {
                    // Manifest paths are canonical source-relative, forward-slash
                    // paths on every platform; native separators (Windows `\\`)
                    // are rejected by the binder by design.
                    path: policy::portable_relative_path(path.strip_prefix(root).unwrap()).into(),
                    length: bytes.len() as u64,
                    sha256: hex::encode(Sha256::digest(&bytes)),
                });
            }
        }
        files
    }

    #[test]
    fn reset_binding_requires_exact_reads_and_keeps_full_manifest_extras() {
        let requirements = [
            (
                Utf8Path::new("generator-package/schema.json"),
                Some("b".repeat(64)),
            ),
            (
                Utf8Path::new("workspace/files/upload/input.json"),
                Some("c".repeat(64)),
            ),
            (Utf8Path::new("workspace/files/upload/missing.json"), None),
        ];
        // Exercise the private reset binder without opening candidate construction
        // or decoding to callers. Engine tests cover requirements from real analysis.
        let diagnostic = json!({"identity": "opaque evidence"});
        let bind = |files| {
            PreparedForkPlan::bind_reset_evidence(
                &diagnostic,
                files,
                requirements
                    .iter()
                    .map(|(path, sha256)| (*path, sha256.as_deref())),
            )
        };
        let required = requirements
            .iter()
            .filter_map(|(path, sha256)| {
                sha256.as_ref().map(|sha256| ForkFileObservation {
                    path: (*path).to_owned(),
                    length: 2,
                    sha256: sha256.clone(),
                })
            })
            .collect::<Vec<_>>();
        let extra = ForkFileObservation {
            path: "meta/journal.jsonl".into(),
            length: 10,
            sha256: "d".repeat(64),
        };
        let mut full = required.clone();
        full.push(extra.clone());
        full.reverse(); // The whole manifest is canonicalized before lookup.
        let plan = bind(full.clone()).unwrap();
        assert_eq!(plan.files.len(), 3);
        assert!(plan.files.iter().any(|file| file.path == extra.path));
        assert!(plan.reset_bound());
        assert_ne!(plan.binding(), bind(required.clone()).unwrap().binding());
        for index in 0..required.len() {
            let mut missing = required.clone();
            missing.remove(index);
            assert!(bind(missing).is_err());
            let mut mismatched = required.clone();
            mismatched[index].sha256 = "e".repeat(64);
            assert!(bind(mismatched).is_err());
            let mut wrong_path = required.clone();
            wrong_path[index].path = "unrelated/input.json".into();
            assert!(bind(wrong_path).is_err());
        }
        assert!(bind(Vec::new()).is_err());
        let forbidden = ForkFileObservation {
            path: "workspace/files/upload/missing.json".into(),
            length: 2,
            sha256: "f".repeat(64),
        };
        // Negative-only evidence requires absence, not an arbitrary digest.
        let bind_negative = |files| {
            PreparedForkPlan::bind_reset_evidence(
                &diagnostic,
                files,
                std::iter::once((forbidden.path.as_path(), None)),
            )
        };
        assert!(bind_negative(Vec::new()).is_ok());
        assert!(bind_negative(vec![extra.clone()]).is_ok());
        assert!(bind_negative(vec![forbidden.clone()]).is_err());
        let ancestor = ForkFileObservation {
            path: "workspace/files/upload".into(),
            length: 2,
            sha256: "d".repeat(64),
        };
        assert!(bind_negative(vec![ancestor.clone()]).is_err());
        // The same conflict is refused when the manifest also has matching
        // positive observations elsewhere in the missing path's subtree.
        let mut shadowed = full.clone();
        shadowed.push(ancestor);
        assert!(bind(shadowed).is_err());
        let descendant = ForkFileObservation {
            path: "workspace/files/upload/missing.json/child".into(),
            length: 2,
            sha256: "e".repeat(64),
        };
        assert!(bind_negative(vec![descendant.clone()]).is_err());
        let mut nested = full.clone();
        nested.push(descendant);
        assert!(bind(nested).is_err());
        // All positive reads match, but a forbidden extra still refuses.
        let mut reintroduced = full.clone();
        reintroduced.push(forbidden);
        assert!(bind(reintroduced).is_err());
        // Even an unrequired extra participates in identity and validation.
        full[0].length += 1;
        assert_ne!(plan.binding(), bind(full.clone()).unwrap().binding());
        full.push(extra);
        assert!(bind(full).is_err());
    }

    #[test]
    fn binding_commits_to_the_canonical_manifest_not_just_the_diagnostic() {
        let file = ForkFileObservation {
            path: "meta/journal.jsonl".into(),
            length: 10,
            sha256: "b".repeat(64),
        };
        let other = ForkFileObservation {
            path: "workspace/data".into(),
            ..file.clone()
        };
        let files = vec![file.clone(), other.clone()];
        let plan = PreparedForkPlan::new(&"a".repeat(64), files.clone()).unwrap();
        assert_ne!(plan.binding(), &binding());
        assert_eq!(
            plan.binding(),
            PreparedForkPlan::new(&"a".repeat(64), vec![other, file.clone()])
                .unwrap()
                .binding()
        );
        for changed in [
            ForkFileObservation {
                path: "meta/other".into(),
                ..file.clone()
            },
            ForkFileObservation {
                length: 11,
                ..file.clone()
            },
            ForkFileObservation {
                sha256: "c".repeat(64),
                ..file.clone()
            },
        ] {
            assert_ne!(
                plan.binding(),
                PreparedForkPlan::new(&"a".repeat(64), vec![changed, files[1].clone()])
                    .unwrap()
                    .binding()
            );
        }
        assert_ne!(
            plan.binding(),
            PreparedForkPlan::new(&"a".repeat(64), vec![file.clone()])
                .unwrap()
                .binding()
        );
        assert_ne!(
            plan.binding(),
            PreparedForkPlan::new(&"d".repeat(64), files)
                .unwrap()
                .binding()
        );
        assert!(PreparedForkPlan::new(&"a".repeat(64), vec![file.clone(), file.clone()]).is_err());
        for path in [
            "",
            "/absolute",
            "../escape",
            "a/../b",
            "./a",
            "a//b",
            "a/",
            "a\\b",
            "a:b",
        ] {
            assert!(
                PreparedForkPlan::new(
                    &"a".repeat(64),
                    vec![ForkFileObservation {
                        path: path.into(),
                        ..file.clone()
                    }]
                )
                .is_err(),
                "{path}"
            );
        }
        assert!(PreparedForkPlan::new("not a digest", vec![file]).is_err());
    }

    #[test]
    fn composite_binding_commits_to_the_entire_reset_payload_and_keeps_ordinary_encoding() {
        let files = vec![ForkFileObservation {
            path: "meta/journal.jsonl".into(),
            length: 10,
            sha256: "b".repeat(64),
        }];
        let digest = "a".repeat(64);
        // The ordinary constructor's exact pre-existing wire encoding is stable,
        // including the empty-manifest case; it is not a reset candidate.
        for manifest in [vec![], files.clone()] {
            let expected = hex::encode(Sha256::digest(
                serde_json::to_vec(&("qcg-prepared-fork-v1", &digest, &manifest)).unwrap(),
            ));
            assert_eq!(
                PreparedForkPlan::new(&digest, manifest)
                    .unwrap()
                    .binding
                    .plan_digest,
                expected
            );
        }
        // Exercise the shared encoding primitive, not an arbitrary candidate
        // constructor: engine alone owns construction of the opaque typed value.
        let payload = serde_json::json!({
            "identity": digest, "source_run_id": "source", "source_last_seq": 10,
            "reset_nodes": {"a": {"output_alias": "result", "predecessors": ["b"]}},
        });
        let bind = |value: &serde_json::Value, files| {
            PreparedForkPlan::bind("qcg-prepared-reset-candidate-v1", value, files).unwrap()
        };
        let original = bind(&payload, files.clone());
        assert_ne!(
            original.binding(),
            PreparedForkPlan::new(&digest, files.clone())
                .unwrap()
                .binding()
        );
        assert_eq!(original.binding(), bind(&payload, files.clone()).binding());
        for (pointer, value) in [
            ("/source_run_id", serde_json::json!("other")),
            ("/source_last_seq", serde_json::json!(11)),
            ("/reset_nodes/a/output_alias", serde_json::json!("other")),
            ("/reset_nodes/a/predecessors", serde_json::json!([])),
        ] {
            let mut changed = payload.clone();
            *changed.pointer_mut(pointer).unwrap() = value;
            assert_ne!(
                original.binding(),
                bind(&changed, files.clone()).binding(),
                "{pointer}"
            );
        }
        assert_ne!(original.binding(), bind(&payload, vec![]).binding());
    }

    #[test]
    #[cfg(unix)]
    fn reset_bound_preparation_refuses_state_patches_before_writing() {
        let root = temp_run_dir("prepared-patch");
        let _guard = TempGuard(root.clone());
        let source = synthetic_hitl_record(&root, "source");
        let files = observe_tree(&source.run_dir, &source.run_dir);
        for (index, payload) in [
            json!({"inputs": {"changed": "new input"}}),
            json!({"step_outputs": {"unrelated": "new output"}}),
            json!({"step_statuses": {"unrelated": "success"}}),
        ]
        .into_iter()
        .enumerate()
        {
            let patch: api::ForkStatePatch = serde_json::from_value(payload).unwrap();
            let mut plan = PreparedForkPlan::new(&"a".repeat(64), files.clone()).unwrap();
            let ordinary = root.join(format!("runs/ordinary-{index}"));
            let (_, staged) = crate::run_dirs::prepare_checkpoint_fork_prepared(
                &source.run_dir,
                "source",
                &ordinary,
                "ordinary",
                1,
                &patch,
                Some(&plan),
            )
            .unwrap();
            drop(staged);
            let events = crate::summaries::read_journal_events(&ordinary).unwrap();
            let patches = events
                .iter()
                .filter(|e| e["t"] == "state_patched")
                .collect::<Vec<_>>();
            assert_eq!(patches.len(), 1);
            assert_eq!(
                patches[0]["inputs"],
                serde_json::to_value(&patch.inputs).unwrap()
            );
            assert_eq!(
                patches[0]["step_outputs"],
                serde_json::to_value(&patch.step_outputs).unwrap()
            );
            assert_eq!(
                patches[0]["step_statuses"],
                serde_json::to_value(&patch.step_statuses).unwrap()
            );
            engine::RunState::fold_admission_values(&events).unwrap();

            // Test the private binding guard without constructing/decoding an
            // opaque engine candidate. This is not a positive reset-plan fixture.
            plan.binding.reset_candidate_sha256 = Some("b".repeat(64));
            for existing in [false, true] {
                let target = root.join(format!("runs/reset-{index}-{existing}"));
                if existing {
                    std::fs::create_dir(&target).unwrap();
                    std::fs::write(target.join("sentinel"), "untouched").unwrap();
                }
                let error = match crate::run_dirs::prepare_checkpoint_fork_prepared(
                    &source.run_dir,
                    "source",
                    &target,
                    "child",
                    1,
                    &patch,
                    Some(&plan),
                ) {
                    Err(error) => error,
                    Ok(_) => panic!("reset-bound patch was prepared"),
                };
                assert!(
                    error
                        .to_string()
                        .contains("reset-bound fork cannot accept a state patch")
                );
                if existing {
                    assert_eq!(
                        std::fs::read_to_string(target.join("sentinel")).unwrap(),
                        "untouched"
                    );
                    assert_eq!(std::fs::read_dir(&target).unwrap().count(), 1);
                } else {
                    assert!(!target.exists());
                }
            }
        }
    }

    #[test]
    #[cfg(unix)]
    fn child_lineage_and_crash_staging_share_incomplete_admission_ownership() {
        let root = temp_run_dir("prepared-lineage");
        let _guard = TempGuard(root.clone());
        let source = synthetic_hitl_record(&root, "source");
        // Use the complete projection tree, including metadata/blobs, not just
        // an engine observed-file subset.
        let files = observe_tree(&source.run_dir, &source.run_dir);
        let plan = PreparedForkPlan::new(&"a".repeat(64), files).unwrap();
        let target = root.join("runs/child");
        let (_, staged) = crate::run_dirs::prepare_checkpoint_fork_prepared(
            &source.run_dir,
            "source",
            &target,
            "child",
            1,
            &api::ForkStatePatch::default(),
            Some(&plan),
        )
        .unwrap();
        let events = crate::summaries::read_journal_events(&target).unwrap();
        let lineage = events
            .iter()
            .find(|event| event["t"] == "run_forked")
            .unwrap();
        assert_eq!(
            lineage["binding"],
            serde_json::to_value(plan.binding()).unwrap()
        );
        assert!(!events.iter().any(|e| e["t"] == "state_patched"));
        assert!(lineage.get("files").is_none());
        assert!(plan.candidate.is_none());
        assert!(plan.reset_overlay().unwrap().is_none());
        assert!(lineage.get("reset_overlay_sha256").is_none());
        assert!(!events.iter().any(|e| e["t"] == "child_reset_overlay"));
        assert_eq!(lineage["fork_tree_commit_version"], 1);
        let staged = staged.unwrap();
        let staging_path = staged.0.clone();
        assert!(staging_path.starts_with(&target));
        assert!(staging_path.join("meta/journal.jsonl").is_file());
        assert!(crate::run_dirs::fork_admission_pending(&target).unwrap());
        // Skip Drop to simulate process death with a fully copied source tree.
        std::mem::forget(staged);
        assert!(staging_path.exists());
        assert!(
            !crate::run_dirs::try_adopt_run_dir_with_snapshot(&target, "child")
                .unwrap()
                .adopted
        );
        assert!(!staging_path.exists());
        // Recovery cannot accumulate a second staging tree: it reuses the same
        // reserved child and the same path after wiping incomplete admission.
        let (_, staged) = crate::run_dirs::prepare_checkpoint_fork_prepared(
            &source.run_dir,
            "source",
            &target,
            "child",
            1,
            &api::ForkStatePatch::default(),
            Some(&plan),
        )
        .unwrap();
        let staged = staged.unwrap();
        assert_eq!(staged.0, staging_path);
        staged.finish().unwrap();
        assert!(!staging_path.exists());

        // A crash during staging (before checkpoint projection) leaves the
        // empty child journal plus pending marker. Boot recovery owns it too.
        crate::run_dirs::try_adopt_run_dir_with_snapshot(&target, "child").unwrap();
        crate::run_dirs::prepare_api_run_directory(&target).unwrap();
        crate::run_dirs::mark_fork_admission_pending(&target).unwrap();
        let staged = plan.stage(&source.run_dir, &target).unwrap();
        std::mem::forget(staged);
        assert!(staging_path.exists());
        assert!(
            crate::summaries::read_recovery_state(&target)
                .unwrap()
                .is_none()
        );
        assert!(!staging_path.exists());
    }
    #[test]
    fn private_reset_overlay_is_hidden_from_public_event_projection() {
        let root = temp_run_dir("private-reset-events");
        let _guard = TempGuard(root.clone());
        let source = synthetic_hitl_record(&root, "source");
        let meta = crate::summaries::run_meta_dir(&source.run_dir);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(meta.join("journal.jsonl"))
            .unwrap();
        let event = serde_json::json!({
            "t": "child_reset_overlay", "seq": 2, "run_id": "source",
            "ts": "2026-01-01T00:00:00Z", "trace_id": "trace", "span_id": "span",
            "overlay": {"private": "evidence"},
        });
        writeln!(file, "{event}").unwrap();
        assert!(
            crate::summaries::read_journal_events(&source.run_dir)
                .unwrap()
                .iter()
                .any(|e| e["t"] == "child_reset_overlay")
        );
        assert!(
            !crate::summaries::read_run_events(&source.run_dir)
                .unwrap()
                .iter()
                .any(|e| e.kind == "child_reset_overlay")
        );
        assert!(
            !crate::summaries::read_events_with_audit(&source.run_dir)
                .unwrap()
                .iter()
                .any(|e| e["t"] == "child_reset_overlay")
        );
        assert!(!api::all_run_event_kinds().contains(&"child_reset_overlay"));
        // Hiding is not validation or authority: the fold still refuses it.
        assert!(engine::RunState::fold_journal(&meta.join("journal.jsonl")).is_err());
    }
}
