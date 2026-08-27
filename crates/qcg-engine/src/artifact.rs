use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{Manifest, ValueBag};
use qcg_types::{OutputArtifact, OutputManifest};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use walkdir::WalkDir;

pub fn collect_outputs(
    workspace: &Utf8Path,
    manifest: &Manifest,
    vars: &ValueBag,
    templates: &crate::TemplateService,
) -> Result<OutputManifest, std::io::Error> {
    let mut artifacts = BTreeMap::new();
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
            .render_inline(template, vars.to_json())
            .map_err(std::io::Error::other)?;
        collect_exact(
            workspace,
            &path,
            &declaration.label,
            declaration.required,
            declaration.mime.as_deref(),
            &mut artifacts,
        )?;
    }
    for extra in &manifest.outputs.extras {
        let pattern = templates
            .render_inline(&extra.glob, vars.to_json())
            .map_err(std::io::Error::other)?;
        let matches = matching_files(workspace, &pattern)?;
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
                &extra.label,
                extra.required,
                extra.mime.as_deref(),
            )?;
            artifacts.insert(artifact.path.clone(), artifact);
        }
    }
    Ok(OutputManifest {
        artifacts: artifacts.into_values().collect(),
    })
}

fn collect_exact(
    workspace: &Utf8Path,
    relative: &str,
    label: &str,
    required: bool,
    mime: Option<&str>,
    artifacts: &mut BTreeMap<String, OutputArtifact>,
) -> Result<(), std::io::Error> {
    match resolve_artifact_path(workspace, relative) {
        Ok(path) => {
            let artifact = build_artifact(workspace, &path, label, required, mime)?;
            artifacts.insert(artifact.path.clone(), artifact);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn matching_files(workspace: &Utf8Path, pattern: &str) -> Result<Vec<Utf8PathBuf>, std::io::Error> {
    validate_relative_artifact_path(pattern, true)?;
    if !pattern.contains(['*', '?']) {
        return match resolve_artifact_path(workspace, pattern) {
            Ok(path) => Ok(vec![path]),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error),
        };
    }
    let mut matches = Vec::new();
    for entry in WalkDir::new(workspace).follow_links(false) {
        let entry = entry.map_err(std::io::Error::other)?;
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
        if glob_matches(pattern.as_bytes(), relative.as_str().as_bytes()) {
            matches.push(path);
        }
    }
    matches.sort();
    Ok(matches)
}

fn glob_matches(pattern: &[u8], path: &[u8]) -> bool {
    let mut memo = BTreeMap::new();
    glob_matches_at(pattern, path, 0, 0, &mut memo)
}

fn glob_matches_at(
    pattern: &[u8],
    path: &[u8],
    pattern_index: usize,
    path_index: usize,
    memo: &mut BTreeMap<(usize, usize), bool>,
) -> bool {
    if let Some(result) = memo.get(&(pattern_index, path_index)) {
        return *result;
    }
    let result = if pattern_index == pattern.len() {
        path_index == path.len()
    } else if pattern[pattern_index] == b'*' {
        let recursive = pattern.get(pattern_index + 1) == Some(&b'*');
        let next_pattern = pattern_index + if recursive { 2 } else { 1 };
        glob_matches_at(pattern, path, next_pattern, path_index, memo)
            || path_index < path.len()
                && (recursive || path[path_index] != b'/')
                && glob_matches_at(pattern, path, pattern_index, path_index + 1, memo)
    } else if path_index < path.len()
        && if pattern[pattern_index] == b'?' {
            path[path_index] != b'/'
        } else {
            pattern[pattern_index] == path[path_index]
        }
    {
        glob_matches_at(pattern, path, pattern_index + 1, path_index + 1, memo)
    } else {
        false
    };
    memo.insert((pattern_index, path_index), result);
    result
}

pub fn write_output_manifest(
    workspace: &Utf8Path,
    manifest: &OutputManifest,
) -> Result<(), std::io::Error> {
    let path = workspace.join("outputs.json");
    let bytes = serde_json::to_vec_pretty(manifest)?;
    fs::write(path, bytes)
}

pub fn read_output_manifest(workspace: &Utf8Path) -> Result<OutputManifest, std::io::Error> {
    let bytes = fs::read(workspace.join("outputs.json"))?;
    serde_json::from_slice(&bytes).map_err(std::io::Error::other)
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
    label: &str,
    required: bool,
    mime: Option<&str>,
) -> Result<OutputArtifact, std::io::Error> {
    let bytes = fs::read(path)?;
    let sha256 = hex::encode(Sha256::digest(&bytes));
    let rel = path
        .strip_prefix(workspace)
        .map_err(std::io::Error::other)?
        .to_string();
    Ok(OutputArtifact {
        path: rel,
        sha256,
        bytes: bytes.len() as u64,
        label: label.to_string(),
        required,
        mime: mime.map(str::to_string),
    })
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
        assert!(glob_matches(b"reports/*.json", b"reports/one.json"));
        assert!(!glob_matches(
            b"reports/*.json",
            b"reports/archive/one.json"
        ));
        assert!(glob_matches(
            b"reports/**/*.json",
            b"reports/archive/one.json"
        ));
        assert!(glob_matches(b"reports/?.json", b"reports/a.json"));
        assert!(!glob_matches(b"reports/?.json", b"reports/ab.json"));
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
            }],
        };
        write_output_manifest(&workspace, &manifest).unwrap();
        let loaded = read_output_manifest(&workspace).unwrap();
        assert_eq!(loaded.artifacts[0].path, "done.txt");
        assert_eq!(loaded.artifacts[0].mime.as_deref(), Some("text/plain"));
    }
}
