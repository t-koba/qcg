use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{Manifest, RuntimeLimits, ValueBag};
use qcg_types::{OutputArtifact, OutputManifest};
use std::collections::BTreeMap;
use std::io;
use walkdir::WalkDir;

use super::resolve::{
    ArtifactMetadata, build_artifact, resolve_artifact_path, validate_relative_artifact_path,
};

pub(crate) const MAX_ARTIFACT_GLOB_BYTES: usize = 4 * 1024;
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
pub(crate) struct OutputLimits {
    pub(crate) file_bytes: Option<u64>,
    pub(crate) total_bytes: Option<u64>,
    pub(crate) artifact_count: Option<usize>,
}

impl OutputLimits {
    pub(crate) fn from_runtime(runtime: &RuntimeLimits) -> Result<Self, std::io::Error> {
        for (name, value) in [
            ("output_file_limit_bytes", runtime.output_file_limit_bytes),
            ("output_total_limit_bytes", runtime.output_total_limit_bytes),
            ("output_artifact_limit", runtime.output_artifact_limit),
        ] {
            if value == Some(0) {
                return Err(std::io::Error::other(format!(
                    "runtime.{name} must be greater than zero"
                )));
            }
        }
        Ok(Self {
            file_bytes: runtime
                .output_file_limit_bytes
                .map(|limit| {
                    u64::try_from(limit).map_err(|_| {
                        std::io::Error::other("runtime.output_file_limit_bytes does not fit in u64")
                    })
                })
                .transpose()?,
            total_bytes: runtime
                .output_total_limit_bytes
                .map(|limit| {
                    u64::try_from(limit).map_err(|_| {
                        std::io::Error::other(
                            "runtime.output_total_limit_bytes does not fit in u64",
                        )
                    })
                })
                .transpose()?,
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
    if accounting
        .limits
        .artifact_count
        .is_some_and(|limit| next_count > limit)
    {
        return Err(std::io::Error::other(format!(
            "output artifact count exceeds {}",
            accounting.limits.artifact_count.unwrap_or(usize::MAX)
        )));
    }
    let total_without_previous = accounting
        .total_bytes
        .checked_sub(previous_bytes)
        .ok_or_else(|| std::io::Error::other("output byte accounting underflowed"))?;
    let next_total = total_without_previous
        .checked_add(artifact.bytes)
        .ok_or_else(|| std::io::Error::other("output byte accounting overflowed"))?;
    if accounting
        .limits
        .total_bytes
        .is_some_and(|limit| next_total > limit)
    {
        return Err(std::io::Error::other(format!(
            "output bytes exceed {}",
            accounting.limits.total_bytes.unwrap_or(u64::MAX)
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
    file_limit: Option<u64>,
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

pub(crate) fn matching_files(
    workspace: &Utf8Path,
    pattern: &str,
    count_limit: Option<usize>,
    artifact_limit: Option<usize>,
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
        if count_limit.is_some_and(|limit| entries > limit) {
            return Err(std::io::Error::other(format!(
                "artifact scan contains more than {} entries",
                count_limit.unwrap_or(usize::MAX)
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
        let portable = qcg_policy::portable_relative_path(relative);
        if portable.len() > MAX_ARTIFACT_PATH_BYTES {
            return Err(io::Error::other(format!(
                "artifact path exceeds {MAX_ARTIFACT_PATH_BYTES} bytes"
            )));
        }
        if glob_matches(pattern.as_bytes(), portable.as_bytes())? {
            if artifact_limit.is_some_and(|limit| matches.len() >= limit) {
                return Err(std::io::Error::other(format!(
                    "artifact glob matches more than {} files",
                    artifact_limit.unwrap_or(usize::MAX)
                )));
            }
            matches.push(path);
        }
    }
    matches.sort();
    Ok(matches)
}

pub(crate) fn glob_matches(pattern: &[u8], path: &[u8]) -> Result<bool, io::Error> {
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
