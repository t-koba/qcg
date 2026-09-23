use crate::{EngineError, RunContext};
use async_trait::async_trait;
use camino::Utf8Path;
use qcg_contract::{ResourceDef, SkillDoc, is_skill_library_entry, skill_metadata_value};
use serde_json::json;

use super::content::{
    read_skill_doc, read_to_string_bounded, render_skill_resource, resolve_resource_file,
    resource_trust_label,
};
use super::hash::{hash_resource_dir, resolve_resource_path, resolve_resource_path_for_engine};
use super::snapshot::{DirectoryLimits, directory_limits};
use super::types::{
    ResourceCacheStatus, ResourceError, ResourceLoader, ResourceSelector, ResourceSnapshot,
    ResourceSnapshotSource,
};

pub(crate) struct SkillResourceLoader;

#[async_trait]
impl ResourceLoader for SkillResourceLoader {
    fn type_id(&self) -> &'static str {
        "skill"
    }

    async fn snapshot(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
    ) -> Result<ResourceSnapshot, EngineError> {
        let path = resource.path.as_deref().ok_or_else(|| {
            EngineError::Failed(format!("skill resource `{name}` requires `path`"))
        })?;
        let full_path = resolve_resource_path_for_engine(context, name, path)?;
        if !full_path.is_dir() {
            return Err(EngineError::Failed(format!(
                "skill resource `{name}` must be a directory containing SKILL.md"
            )));
        }
        let limits = directory_limits(name, resource)
            .map_err(|error| EngineError::Failed(error.to_string()))?;
        let (sha256, files) = hash_resource_dir(&full_path, limits)?;
        validate_resource_pin(name, resource, &sha256)?;
        let skill = read_skill_doc(name, &full_path, limits.max_selected_bytes)
            .map_err(|error| EngineError::Failed(error.to_string()))?;
        let diagnostics: Vec<String> = skill
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.message.clone())
            .collect();
        for diagnostic in &diagnostics {
            tracing::warn!("skill resource `{name}`: {diagnostic}");
        }
        let bytes = files.iter().map(|file| file.bytes).sum();
        Ok(ResourceSnapshot {
            name: name.to_string(),
            resource_type: self.type_id().into(),
            source: ResourceSnapshotSource::Path { path: full_path },
            snapshot: None,
            sha256,
            bytes,
            files,
            cache: ResourceCacheStatus::NotApplicable,
            pin_sha256: resource.pin_sha256.clone(),
            trust: resource_trust_label(&resource.trust).into(),
            llm_visible: resource.llm_visible,
            diagnostics,
        })
    }

    fn select(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
        selector: Option<&ResourceSelector>,
    ) -> Result<String, ResourceError> {
        if let Some(path) = resource.path.as_deref() {
            let root = resolve_resource_path(context, name, path)?;
            if root.is_dir() {
                let limits = directory_limits(name, resource)?;
                hash_resource_dir(&root, limits)
                    .map_err(|source| ResourceError::Read { path: root, source })?;
            }
        }
        let limits = directory_limits(name, resource)?;
        render_skill_resource(context, name, resource, selector, limits)
    }
}

pub(crate) fn validate_resource_pin(
    name: &str,
    resource: &ResourceDef,
    sha256: &str,
) -> Result<(), EngineError> {
    if let Some(expected) = &resource.pin_sha256
        && expected != sha256
    {
        return Err(EngineError::Failed(format!(
            "resource `{name}` sha256 pin mismatch: expected {expected}, got {sha256}"
        )));
    }
    Ok(())
}

pub(crate) struct SkillLibraryResourceLoader;

#[async_trait]
impl ResourceLoader for SkillLibraryResourceLoader {
    fn type_id(&self) -> &'static str {
        "skill_library"
    }

    async fn snapshot(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
    ) -> Result<ResourceSnapshot, EngineError> {
        let path = resource.path.as_deref().ok_or_else(|| {
            EngineError::Failed(format!("skill_library resource `{name}` requires `path`"))
        })?;
        let full_path = resolve_resource_path_for_engine(context, name, path)?;
        if !full_path.is_dir() {
            return Err(EngineError::Failed(format!(
                "skill_library resource `{name}` must be a directory"
            )));
        }
        let limits = directory_limits(name, resource)
            .map_err(|error| EngineError::Failed(error.to_string()))?;
        let (sha256, files) = hash_resource_dir(&full_path, limits)?;
        validate_resource_pin(name, resource, &sha256)?;
        let skills = scan_skill_library(name, &full_path, limits)
            .map_err(|error| EngineError::Failed(error.to_string()))?;
        let mut diagnostics = Vec::new();
        for (dir, doc) in &skills {
            for diagnostic in &doc.diagnostics {
                diagnostics.push(format!("{dir}: {}", diagnostic.message));
            }
        }
        for diagnostic in &diagnostics {
            tracing::warn!("skill_library resource `{name}`: {diagnostic}");
        }
        let bytes = files.iter().map(|file| file.bytes).sum();
        Ok(ResourceSnapshot {
            name: name.to_string(),
            resource_type: self.type_id().into(),
            source: ResourceSnapshotSource::Path { path: full_path },
            snapshot: None,
            sha256,
            bytes,
            files,
            cache: ResourceCacheStatus::NotApplicable,
            pin_sha256: resource.pin_sha256.clone(),
            trust: resource_trust_label(&resource.trust).into(),
            llm_visible: resource.llm_visible,
            diagnostics,
        })
    }

    fn select(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
        selector: Option<&ResourceSelector>,
    ) -> Result<String, ResourceError> {
        let path = resource
            .path
            .as_deref()
            .ok_or_else(|| ResourceError::MissingField {
                resource: name.to_string(),
                field: "path",
            })?;
        let root = resolve_resource_path(context, name, path)?;
        let limits = directory_limits(name, resource)?;
        let (sha256, files) =
            hash_resource_dir(&root, limits).map_err(|source| ResourceError::Read {
                path: root.clone(),
                source,
            })?;
        match selector {
            None => {
                let skills = scan_skill_library(name, &root, limits)?;
                Ok(serde_json::to_string_pretty(&skill_catalog(&skills))?)
            }
            Some(ResourceSelector::Named(selector)) if selector == "catalog" => {
                let skills = scan_skill_library(name, &root, limits)?;
                Ok(serde_json::to_string_pretty(&skill_catalog(&skills))?)
            }
            Some(ResourceSelector::Named(selector))
                if selector == "tree" || selector == "files" =>
            {
                Ok(serde_json::to_string_pretty(&json!({
                    "sha256": sha256,
                    "files": files,
                }))?)
            }
            Some(ResourceSelector::Named(selector)) => {
                if let Some(skill) = selector.strip_prefix("meta/") {
                    let skills = scan_skill_library(name, &root, limits)?;
                    let doc = library_skill(name, &skills, skill)?;
                    return Ok(serde_json::to_string_pretty(&skill_metadata_value(doc))?);
                }
                if let Some(skill) = selector.strip_prefix("instructions/") {
                    let skills = scan_skill_library(name, &root, limits)?;
                    let doc = library_skill(name, &skills, skill)?;
                    return Ok(doc.instructions.clone());
                }
                if let Some(skill) = selector.strip_prefix("tree/") {
                    let skills = scan_skill_library(name, &root, limits)?;
                    library_skill(name, &skills, skill)?;
                    let skill_root = root.join(skill);
                    let (skill_sha256, skill_files) = hash_resource_dir(&skill_root, limits)
                        .map_err(|source| ResourceError::Read {
                            path: skill_root.clone(),
                            source,
                        })?;
                    return Ok(serde_json::to_string_pretty(&json!({
                        "sha256": skill_sha256,
                        "files": skill_files,
                    }))?);
                }
                Err(ResourceError::UnsupportedNamedSelector {
                    resource: name.to_string(),
                    selector: selector.clone(),
                })
            }
            Some(ResourceSelector::File { path }) => {
                let (skill, relative) =
                    path.split_once('/')
                        .ok_or_else(|| ResourceError::UnsafeFileSelector {
                            resource: name.to_string(),
                        })?;
                if skill.is_empty() || relative.is_empty() {
                    return Err(ResourceError::UnsafeFileSelector {
                        resource: name.to_string(),
                    });
                }
                let skills = scan_skill_library(name, &root, limits)?;
                library_skill(name, &skills, skill)?;
                let skill_root = root.join(skill);
                let file = resolve_resource_file(name, &skill_root, relative)?;
                read_to_string_bounded(&file, limits.max_selected_bytes)
            }
            Some(_) => Err(ResourceError::UnsupportedSelector {
                resource: name.to_string(),
            }),
        }
    }
}

fn scan_skill_library(
    resource: &str,
    root: &Utf8Path,
    limits: DirectoryLimits,
) -> Result<Vec<(String, SkillDoc)>, ResourceError> {
    let entries = std::fs::read_dir(root).map_err(|source| ResourceError::Read {
        path: root.to_path_buf(),
        source,
    })?;
    let mut children: Vec<String> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| ResourceError::Read {
            path: root.to_path_buf(),
            source,
        })?;
        let file_type = entry.file_type().map_err(|source| ResourceError::Read {
            path: root.to_path_buf(),
            source,
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !is_skill_library_entry(&name) {
            continue;
        }
        children.push(name);
    }
    children.sort();
    let mut skills = Vec::new();
    for child in children {
        let skill_root = root.join(&child);
        if !skill_root.join("SKILL.md").is_file() {
            continue;
        }
        let doc = read_skill_doc(resource, &skill_root, limits.max_selected_bytes)?;
        skills.push((child, doc));
    }
    Ok(skills)
}

fn skill_catalog(skills: &[(String, SkillDoc)]) -> Vec<serde_json::Value> {
    skills
        .iter()
        .map(|(dir, doc)| {
            let mut entry = json!({
                "name": dir,
                "description": doc.description,
            });
            if doc.name != *dir
                && let Some(object) = entry.as_object_mut()
            {
                object.insert("frontmatter_name".into(), json!(doc.name));
            }
            if !doc.diagnostics.is_empty()
                && let Some(object) = entry.as_object_mut()
            {
                object.insert("diagnostics".into(), json!(doc.diagnostics));
            }
            entry
        })
        .collect()
}

fn library_skill<'a>(
    resource: &str,
    skills: &'a [(String, SkillDoc)],
    key: &str,
) -> Result<&'a SkillDoc, ResourceError> {
    skills
        .iter()
        .find(|(dir, _)| dir == key)
        .map(|(_, doc)| doc)
        .ok_or_else(|| ResourceError::InvalidSkill {
            resource: resource.to_string(),
            message: format!("skill_library has no skill directory `{key}`"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;

    fn temp_dir(name: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-engine-skill-{name}-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path should be UTF-8")
    }

    fn write_skill(root: &Utf8Path, dir: &str, body: &str) {
        let skill_dir = root.join(dir);
        std::fs::create_dir_all(&skill_dir).expect("skill directory should be created");
        std::fs::write(skill_dir.join("SKILL.md"), body).expect("skill file should be written");
    }

    #[test]
    fn read_skill_doc_reports_directory_name_diagnostics() {
        let root = temp_dir("diagnostics");
        write_skill(
            &root,
            "demo-skill",
            "---\nname: other\ndescription: Demo.\n---\nBody.\n",
        );
        let doc = read_skill_doc("skill", &root.join("demo-skill"), None)
            .expect("valid frontmatter should parse");
        assert_eq!(doc.name, "other");
        assert!(
            doc.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("directory"))
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn read_skill_doc_requires_description_and_skill_file() {
        let root = temp_dir("required");
        write_skill(&root, "demo", "---\nname: demo\n---\n");
        let error = read_skill_doc("skill", &root.join("demo"), None)
            .expect_err("missing description must fail");
        assert!(error.to_string().contains("description"), "{error}");
        let missing = root.join("empty");
        std::fs::create_dir_all(&missing).expect("empty directory should be created");
        let error =
            read_skill_doc("skill", &missing, None).expect_err("missing SKILL.md must fail");
        assert!(error.to_string().contains("SKILL.md"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn scan_skill_library_is_sorted_and_ignores_non_skills() {
        let root = temp_dir("library");
        write_skill(&root, "beta", "---\nname: beta\ndescription: Beta.\n---\n");
        write_skill(
            &root,
            "alpha",
            "---\nname: alpha\ndescription: Alpha.\n---\n",
        );
        write_skill(
            &root,
            "broken",
            "---\nname: broken\ndescription: Broken.\n---\n",
        );
        std::fs::write(root.join("notes.txt"), "not a skill")
            .expect("plain file should be written");
        std::fs::remove_file(root.join("broken/SKILL.md")).expect("skill file should be removed");
        write_skill(
            &root,
            ".git",
            "---\nname: git\ndescription: Must never be scanned.\n---\n",
        );
        write_skill(
            &root,
            "node_modules",
            "---\nname: deps\ndescription: Must never be scanned.\n---\n",
        );
        let skills = scan_skill_library("skills", &root, DirectoryLimits::default())
            .expect("library should scan");
        let names: Vec<&str> = skills.iter().map(|(dir, _)| dir.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta"]);
        let catalog = skill_catalog(&skills);
        assert_eq!(catalog[0]["name"], "alpha");
        assert_eq!(catalog[1]["description"], "Beta.");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn library_skill_lookup_rejects_unknown_names() {
        let root = temp_dir("lookup");
        write_skill(
            &root,
            "alpha",
            "---\nname: alpha\ndescription: Alpha.\n---\n",
        );
        let skills = scan_skill_library("skills", &root, DirectoryLimits::default())
            .expect("library should scan");
        let doc = library_skill("skills", &skills, "alpha").expect("alpha should resolve");
        assert_eq!(doc.description, "Alpha.");
        let error =
            library_skill("skills", &skills, "missing").expect_err("unknown skill must fail");
        assert!(error.to_string().contains("missing"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }
}
