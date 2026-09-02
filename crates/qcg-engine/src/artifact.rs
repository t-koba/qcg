use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{Manifest, RuntimeLimits, ValueBag};
use qcg_types::{OutputArtifact, OutputManifest};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read as _, Write as _};
use tempfile::NamedTempFile;
use walkdir::WalkDir;

const MAX_ARTIFACT_GLOB_BYTES: usize = 4 * 1024;
const MAX_ARTIFACT_PATH_BYTES: usize = 4 * 1024;
const MAX_ARTIFACT_GLOB_STATES: usize = 4 * 1024 * 1024;

pub fn collect_outputs(
    workspace: &Utf8Path,
    manifest: &Manifest,
    vars: &ValueBag,
    templates: &crate::TemplateService,
) -> Result<OutputManifest, std::io::Error> {
    let limits = OutputLimits::from_runtime(&manifest.runtime)?;
    let mut artifacts = BTreeMap::new();
    let mut accounting = OutputAccounting::new(limits);
    for node in manifest.flow.iter().filter(|node| node.artifact.is_some()) {
        let declaration = node.artifact.as_ref().ok_or_else(|| {
            std::io::Error::other(format!("node `{}` lost its artifact declaration", node.id))
        })?;
        let template = node.artifact_path_template().ok_or_else(|| {
            std::io::Error::other(format!(
                "node `{}` artifact has no output_file, target, or destination",
                node.id
            ))
        })?;
        let path = templates
            .render_inline(template, vars.to_json(), &manifest.runtime)
            .map_err(std::io::Error::other)?;
        collect_exact(
            workspace,
            &path,
            ArtifactMetadata {
                label: &declaration.label,
                required: declaration.required,
                mime: declaration.mime.as_deref(),
                description: &declaration.description,
                preview: declaration.preview,
            },
            &mut artifacts,
            &mut accounting,
            limits.file_bytes,
        )?;
    }
    for extra in &manifest.outputs.extras {
        let pattern = templates
            .render_inline(&extra.glob, vars.to_json(), &manifest.runtime)
            .map_err(std::io::Error::other)?;
        let matches = matching_files(
            workspace,
            &pattern,
            manifest.runtime.file_count_limit,
            limits.artifact_count,
        )?;
        if matches.is_empty() && extra.required {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("required output glob `{pattern}` matched no files"),
            ));
        }
        for path in matches {
            let artifact = build_artifact(
                workspace,
                &path,
                ArtifactMetadata {
                    label: &extra.label,
                    required: extra.required,
                    mime: extra.mime.as_deref(),
                    description: &extra.description,
                    preview: extra.preview,
                },
                limits.file_bytes,
            )?;
            insert_artifact(&mut artifacts, &mut accounting, artifact)?;
        }
    }
    Ok(OutputManifest {
        artifacts: artifacts.into_values().collect(),
    })
}

#[derive(Clone, Copy)]
struct OutputLimits {
    file_bytes: u64,
    total_bytes: u64,
    artifact_count: usize,
}

impl OutputLimits {
    fn from_runtime(runtime: &RuntimeLimits) -> Result<Self, std::io::Error> {
        for (name, value) in [
            ("output_file_limit_bytes", runtime.output_file_limit_bytes),
            ("output_total_limit_bytes", runtime.output_total_limit_bytes),
            ("output_artifact_limit", runtime.output_artifact_limit),
        ] {
            if value == 0 {
                return Err(std::io::Error::other(format!(
                    "runtime.{name} must be greater than zero"
                )));
            }
        }
        Ok(Self {
            file_bytes: u64::try_from(runtime.output_file_limit_bytes).map_err(|_| {
                std::io::Error::other("runtime.output_file_limit_bytes does not fit in u64")
            })?,
            total_bytes: u64::try_from(runtime.output_total_limit_bytes).map_err(|_| {
                std::io::Error::other("runtime.output_total_limit_bytes does not fit in u64")
            })?,
            artifact_count: runtime.output_artifact_limit,
        })
    }
}

struct OutputAccounting {
    limits: OutputLimits,
    total_bytes: u64,
}

impl OutputAccounting {
    fn new(limits: OutputLimits) -> Self {
        Self {
            limits,
            total_bytes: 0,
        }
    }
}

fn insert_artifact(
    artifacts: &mut BTreeMap<String, OutputArtifact>,
    accounting: &mut OutputAccounting,
    artifact: OutputArtifact,
) -> Result<(), std::io::Error> {
    let path = artifact.path.clone();
    let previous_bytes = artifacts.get(&path).map_or(0, |old| old.bytes);
    let next_count = artifacts
        .len()
        .checked_add(if artifacts.contains_key(&path) { 0 } else { 1 })
        .ok_or_else(|| std::io::Error::other("output artifact count overflowed"))?;
    if next_count > accounting.limits.artifact_count {
        return Err(std::io::Error::other(format!(
            "output artifact count exceeds {}",
            accounting.limits.artifact_count
        )));
    }
    let total_without_previous = accounting
        .total_bytes
        .checked_sub(previous_bytes)
        .ok_or_else(|| std::io::Error::other("output byte accounting underflowed"))?;
    let next_total = total_without_previous
        .checked_add(artifact.bytes)
        .ok_or_else(|| std::io::Error::other("output byte accounting overflowed"))?;
    if next_total > accounting.limits.total_bytes {
        return Err(std::io::Error::other(format!(
            "output bytes exceed {}",
            accounting.limits.total_bytes
        )));
    }
    accounting.total_bytes = next_total;
    artifacts.insert(path, artifact);
    Ok(())
}

fn collect_exact(
    workspace: &Utf8Path,
    relative: &str,
    metadata: ArtifactMetadata<'_>,
    artifacts: &mut BTreeMap<String, OutputArtifact>,
    accounting: &mut OutputAccounting,
    file_limit: u64,
) -> Result<(), std::io::Error> {
    match resolve_artifact_path(workspace, relative) {
        Ok(path) => {
            let artifact = build_artifact(workspace, &path, metadata, file_limit)?;
            insert_artifact(artifacts, accounting, artifact)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !metadata.required => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn matching_files(
    workspace: &Utf8Path,
    pattern: &str,
    count_limit: usize,
    artifact_limit: usize,
) -> Result<Vec<Utf8PathBuf>, std::io::Error> {
    validate_relative_artifact_path(pattern, true)?;
    if pattern.len() > MAX_ARTIFACT_GLOB_BYTES {
        return Err(io::Error::other(format!(
            "artifact glob exceeds {MAX_ARTIFACT_GLOB_BYTES} bytes"
        )));
    }
    if !pattern.contains(['*', '?']) {
        return match resolve_artifact_path(workspace, pattern) {
            Ok(path) => Ok(vec![path]),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error),
        };
    }
    let mut matches = Vec::new();
    let mut entries = 0_usize;
    for entry in WalkDir::new(workspace).follow_links(false) {
        let entry = entry.map_err(std::io::Error::other)?;
        entries = entries
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("artifact entry count overflowed"))?;
        if entries > count_limit {
            return Err(std::io::Error::other(format!(
                "artifact scan contains more than {count_limit} entries"
            )));
        }
        if !entry.file_type().is_file() {
            continue;
        }
        let path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).map_err(|path| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("artifact path is not UTF-8: {}", path.display()),
            )
        })?;
        let relative = path
            .strip_prefix(workspace)
            .map_err(std::io::Error::other)?;
        let portable = portable_relative_path(relative);
        if portable.len() > MAX_ARTIFACT_PATH_BYTES {
            return Err(io::Error::other(format!(
                "artifact path exceeds {MAX_ARTIFACT_PATH_BYTES} bytes"
            )));
        }
        if glob_matches(pattern.as_bytes(), portable.as_bytes())? {
            if matches.len() >= artifact_limit {
                return Err(std::io::Error::other(format!(
                    "artifact glob matches more than {artifact_limit} files"
                )));
            }
            matches.push(path);
        }
    }
    matches.sort();
    Ok(matches)
}

fn glob_matches(pattern: &[u8], path: &[u8]) -> Result<bool, io::Error> {
    let rows = pattern
        .len()
        .checked_add(1)
        .ok_or_else(|| io::Error::other("artifact glob state count overflowed"))?;
    let columns = path
        .len()
        .checked_add(1)
        .ok_or_else(|| io::Error::other("artifact glob state count overflowed"))?;
    let states = rows
        .checked_mul(columns)
        .ok_or_else(|| io::Error::other("artifact glob state count overflowed"))?;
    if states > MAX_ARTIFACT_GLOB_STATES {
        return Err(io::Error::other(format!(
            "artifact glob matching exceeds {MAX_ARTIFACT_GLOB_STATES} states"
        )));
    }
    let mut matches = vec![false; states];
    let index = |pattern_index: usize, path_index: usize| pattern_index * columns + path_index;
    matches[index(pattern.len(), path.len())] = true;
    for pattern_index in (0..pattern.len()).rev() {
        for path_index in (0..=path.len()).rev() {
            matches[index(pattern_index, path_index)] = if pattern[pattern_index] == b'*' {
                let recursive = pattern.get(pattern_index + 1) == Some(&b'*');
                let next_pattern = pattern_index + if recursive { 2 } else { 1 };
                matches[index(next_pattern, path_index)]
                    || path_index < path.len()
                        && (recursive || path[path_index] != b'/')
                        && matches[index(pattern_index, path_index + 1)]
            } else {
                path_index < path.len()
                    && (if pattern[pattern_index] == b'?' {
                        path[path_index] != b'/'
                    } else {
                        pattern[pattern_index] == path[path_index]
                    })
                    && matches[index(pattern_index + 1, path_index + 1)]
            };
        }
    }
    Ok(matches[index(0, 0)])
}

pub fn write_output_manifest(
    workspace: &Utf8Path,
    manifest: &OutputManifest,
) -> Result<(), std::io::Error> {
    write_output_manifest_with_limits(workspace, manifest, &RuntimeLimits::default())
}

pub fn write_output_manifest_with_limits(
    workspace: &Utf8Path,
    manifest: &OutputManifest,
    runtime: &RuntimeLimits,
) -> Result<(), std::io::Error> {
    let limits = OutputLimits::from_runtime(runtime)?;
    validate_output_manifest(manifest, limits)?;
    let mut writer = BoundedManifestWriter::new(
        usize::try_from(limits.file_bytes)
            .map_err(|_| io::Error::other("output file limit does not fit in usize"))?,
    );
    serde_json::to_writer_pretty(&mut writer, manifest).map_err(|error| {
        if writer.exceeded {
            io::Error::other(format!("outputs.json exceeds {} bytes", limits.file_bytes))
        } else {
            io::Error::other(error)
        }
    })?;
    let path = workspace.join("outputs.json");
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("outputs.json has no parent directory"))?;
    let mut temporary = NamedTempFile::new_in(parent.as_std_path())?;
    temporary.write_all(writer.bytes())?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(path.as_std_path())
        .map_err(|error| io::Error::other(error.error))?;
    Ok(())
}

pub fn read_output_manifest(workspace: &Utf8Path) -> Result<OutputManifest, std::io::Error> {
    read_output_manifest_with_limits(workspace, &RuntimeLimits::default())
}

pub fn read_output_manifest_with_limits(
    workspace: &Utf8Path,
    runtime: &RuntimeLimits,
) -> Result<OutputManifest, std::io::Error> {
    let limits = OutputLimits::from_runtime(runtime)?;
    let path = workspace.join("outputs.json");
    let file = fs::File::open(&path)?;
    let mut bytes = Vec::new();
    let mut limited = file.take(limits.file_bytes.saturating_add(1));
    std::io::Read::read_to_end(&mut limited, &mut bytes)?;
    if bytes.len() as u64 > limits.file_bytes {
        return Err(io::Error::other(format!(
            "outputs.json exceeds {} bytes",
            limits.file_bytes
        )));
    }
    serde_json::from_slice(&bytes).map_err(std::io::Error::other)
}

fn validate_output_manifest(
    manifest: &OutputManifest,
    limits: OutputLimits,
) -> Result<(), std::io::Error> {
    if manifest.artifacts.len() > limits.artifact_count {
        return Err(io::Error::other(format!(
            "output artifact count exceeds {}",
            limits.artifact_count
        )));
    }
    let mut paths = BTreeSet::new();
    let mut total = 0_u64;
    for artifact in &manifest.artifacts {
        if !paths.insert(&artifact.path) {
            return Err(io::Error::other(format!(
                "output manifest contains duplicate artifact `{}`",
                artifact.path
            )));
        }
        if artifact.bytes > limits.file_bytes {
            return Err(io::Error::other(format!(
                "output artifact `{}` exceeds {} bytes",
                artifact.path, limits.file_bytes
            )));
        }
        total = total
            .checked_add(artifact.bytes)
            .ok_or_else(|| io::Error::other("output byte accounting overflowed"))?;
        if total > limits.total_bytes {
            return Err(io::Error::other(format!(
                "output bytes exceed {}",
                limits.total_bytes
            )));
        }
    }
    Ok(())
}

struct BoundedManifestWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedManifestWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl io::Write for BoundedManifestWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("outputs.json size overflowed"))?;
        if next > self.limit {
            self.exceeded = true;
            return Err(io::Error::other("outputs.json exceeds its limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn resolve_artifact_path(
    workspace: &Utf8Path,
    artifact_path: &str,
) -> Result<Utf8PathBuf, std::io::Error> {
    validate_relative_artifact_path(artifact_path, false)?;
    let path = workspace.join(artifact_path);
    if !path.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("artifact `{artifact_path}` was not found"),
        ));
    }
    let canonical_workspace = dunce::canonicalize(workspace).map_err(std::io::Error::other)?;
    let canonical_path = dunce::canonicalize(&path).map_err(std::io::Error::other)?;
    if !canonical_path.starts_with(&canonical_workspace) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("artifact `{artifact_path}` resolves outside the workspace"),
        ));
    }
    Ok(path)
}

fn validate_relative_artifact_path(
    artifact_path: &str,
    allow_glob: bool,
) -> Result<(), std::io::Error> {
    if artifact_path.is_empty()
        || artifact_path.starts_with('/')
        || artifact_path.contains('\0')
        || artifact_path.contains('\\')
        || artifact_path.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || (!allow_glob && part.contains(['*', '?']))
        })
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("artifact path `{artifact_path}` is not allowed"),
        ));
    }
    Ok(())
}

fn build_artifact(
    workspace: &Utf8Path,
    path: &Utf8Path,
    metadata: ArtifactMetadata<'_>,
    file_limit: u64,
) -> Result<OutputArtifact, std::io::Error> {
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let bytes = {
        let mut writer = DigestWriter {
            digest: &mut digest,
            bytes: 0,
            limit: file_limit,
        };
        std::io::copy(&mut file, &mut writer)?;
        writer.bytes
    };
    let sha256 = hex::encode(digest.finalize());
    let rel = path
        .strip_prefix(workspace)
        .map_err(std::io::Error::other)?;
    let rel = portable_relative_path(rel);
    Ok(OutputArtifact {
        path: rel,
        sha256,
        bytes,
        label: metadata.label.to_string(),
        required: metadata.required,
        mime: metadata.mime.map(str::to_string),
        description: metadata.description.to_string(),
        preview: metadata.preview,
    })
}

fn portable_relative_path(path: &Utf8Path) -> String {
    path.components()
        .map(|component| component.as_str())
        .collect::<Vec<_>>()
        .join("/")
}

#[derive(Clone, Copy)]
struct ArtifactMetadata<'a> {
    label: &'a str,
    required: bool,
    mime: Option<&'a str>,
    description: &'a str,
    preview: qcg_types::ArtifactPreview,
}

struct DigestWriter<'a> {
    digest: &'a mut Sha256,
    bytes: u64,
    limit: u64,
}

impl std::io::Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next =
            self.bytes
                .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                    std::io::Error::other("artifact byte count does not fit in u64")
                })?)
                .ok_or_else(|| std::io::Error::other("artifact byte count overflowed"))?;
        if next > self.limit {
            return Err(std::io::Error::other(format!(
                "artifact file exceeds {} bytes",
                self.limit
            )));
        }
        self.digest.update(bytes);
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_workspace() -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(std::env::temp_dir().join("qcg-artifact-test"))
            .expect("temporary directory path must be utf-8")
    }

    #[test]
    fn resolve_artifact_path_rejects_escape() {
        let workspace = temp_workspace();
        assert!(resolve_artifact_path(&workspace, "../secret").is_err());
        assert!(resolve_artifact_path(&workspace, "/tmp/secret").is_err());
    }

    #[test]
    fn artifact_glob_distinguishes_segment_and_recursive_wildcards() {
        assert!(glob_matches(b"reports/*.json", b"reports/one.json").unwrap());
        assert!(!glob_matches(b"reports/*.json", b"reports/archive/one.json").unwrap());
        assert!(glob_matches(b"reports/**/*.json", b"reports/archive/one.json").unwrap());
        assert!(glob_matches(b"reports/?.json", b"reports/a.json").unwrap());
        assert!(!glob_matches(b"reports/?.json", b"reports/ab.json").unwrap());
    }

    #[test]
    fn artifact_glob_rejects_oversized_patterns_and_state_spaces() {
        let oversized_pattern = vec![b'a'; MAX_ARTIFACT_GLOB_BYTES + 1];
        let workspace = temp_workspace();
        let error = matching_files(
            &workspace,
            std::str::from_utf8(&oversized_pattern).expect("ASCII pattern"),
            1,
            1,
        )
        .expect_err("oversized artifact glob must fail before scanning");
        assert!(
            error.to_string().contains("artifact glob exceeds"),
            "{error}"
        );

        let pattern = vec![b'?'; 2_048];
        let path = vec![b'a'; 2_048];
        let error = glob_matches(&pattern, &path)
            .expect_err("oversized artifact glob state space must fail before allocation");
        assert!(
            error.to_string().contains("glob matching exceeds"),
            "{error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_artifact_path_rejects_symlink_escape() {
        let workspace = temp_workspace().join(format!("symlink-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        let outside = workspace
            .parent()
            .expect("workspace should have a parent")
            .join("outside-artifact.txt");
        std::fs::write(&outside, "outside").expect("outside file should be written");
        std::os::unix::fs::symlink(&outside, workspace.join("escaped.txt"))
            .expect("symlink should be created");
        let error = resolve_artifact_path(&workspace, "escaped.txt")
            .expect_err("symlink escape must be rejected");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn output_manifest_round_trips() {
        let workspace = temp_workspace();
        std::fs::create_dir_all(&workspace).unwrap();
        let manifest = OutputManifest {
            artifacts: vec![OutputArtifact {
                path: "done.txt".into(),
                sha256: "abc".into(),
                bytes: 3,
                label: "Done".into(),
                required: true,
                mime: Some("text/plain".into()),
                description: "Generated text".into(),
                preview: qcg_types::ArtifactPreview::Text,
            }],
        };
        write_output_manifest(&workspace, &manifest).unwrap();
        let loaded = read_output_manifest(&workspace).unwrap();
        assert_eq!(loaded.artifacts[0].path, "done.txt");
        assert_eq!(loaded.artifacts[0].mime.as_deref(), Some("text/plain"));
    }

    #[test]
    fn output_manifest_limits_reject_oversized_artifacts_before_writing() {
        let workspace = temp_workspace().join(format!("limits-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&workspace).expect("workspace should be created");
        let manifest = OutputManifest {
            artifacts: vec![OutputArtifact {
                path: "large.bin".into(),
                sha256: "abc".into(),
                bytes: 3,
                label: "Large".into(),
                required: true,
                mime: None,
                description: String::new(),
                preview: qcg_types::ArtifactPreview::None,
            }],
        };
        let runtime = RuntimeLimits {
            output_file_limit_bytes: 2,
            ..RuntimeLimits::default()
        };
        let error = write_output_manifest_with_limits(&workspace, &manifest, &runtime)
            .expect_err("oversized output artifact must be rejected");
        assert!(error.to_string().contains("output artifact `large.bin`"));
        assert!(!workspace.join("outputs.json").exists());
    }
}
