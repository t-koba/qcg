use crate::{EngineError, RunContext};
use async_trait::async_trait;
use camino::Utf8Path;
use contract::{
    ResourceDef, ResourceKind, SkillDiagnostic, SkillDoc, is_skill_library_entry,
    skill_metadata_value,
};
use serde_json::json;

use super::content::{
    read_skill_doc, read_to_string_bounded, render_skill_resource, resolve_resource_file,
    resource_trust_label,
};
use super::hash::{hash_resource_dir, resolve_resource_path, resolve_resource_path_for_engine};
use super::snapshot::{DirectoryLimits, directory_limits};
use super::types::{
    ResourceCacheStatus, ResourceError, ResourceFileSnapshot, ResourceLoader, ResourceSelector,
    ResourceSnapshot, ResourceSnapshotSource,
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
        let scan = scan_skill_library(name, &full_path, limits, &files)
            .map_err(|error| EngineError::Failed(error.to_string()))?;
        let diagnostics = library_diagnostics(&scan);
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
                let scan = scan_skill_library(name, &root, limits, &files)?;
                Ok(serde_json::to_string_pretty(&skill_catalog(&scan))?)
            }
            Some(ResourceSelector::Named(selector)) if selector == "catalog" => {
                let scan = scan_skill_library(name, &root, limits, &files)?;
                Ok(serde_json::to_string_pretty(&skill_catalog(&scan))?)
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
                    let scan = scan_skill_library(name, &root, limits, &files)?;
                    let doc = library_entry(name, &scan, skill)?;
                    return Ok(serde_json::to_string_pretty(&skill_metadata_value(doc))?);
                }
                if let Some(skill) = selector.strip_prefix("instructions/") {
                    let scan = scan_skill_library(name, &root, limits, &files)?;
                    let doc = library_entry(name, &scan, skill)?;
                    return Ok(doc.instructions.clone());
                }
                if let Some(skill) = selector.strip_prefix("tree/") {
                    let scan = scan_skill_library(name, &root, limits, &files)?;
                    library_entry(name, &scan, skill)?;
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
                let scan = scan_skill_library(name, &root, limits, &files)?;
                library_entry(name, &scan, skill)?;
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

/// A library member whose SKILL.md is content-malformed. The directory is
/// kept so catalog/snapshot diagnostics can name it and selectors can refuse
/// it; the original parse/validation reason is never discarded.
#[derive(Debug)]
struct SkippedSkill {
    dir: String,
    reason: String,
}

impl SkippedSkill {
    fn diagnostic(&self) -> String {
        format!("SKILL.md is malformed and was skipped: {}", self.reason)
    }
}

/// Valid members plus content-malformed members isolated from them.
#[derive(Debug)]
struct LibraryScan {
    skills: Vec<(String, SkillDoc)>,
    skipped: Vec<SkippedSkill>,
}

fn scan_skill_library(
    resource: &str,
    root: &Utf8Path,
    limits: DirectoryLimits,
    files: &[ResourceFileSnapshot],
) -> Result<LibraryScan, ResourceError> {
    // Reuse the bounded whole-tree walk's file list instead of starting an
    // unbounded second directory traversal. It already counted every entry
    // against max_entries and rejected symlinks.
    let mut children: Vec<String> = files
        .iter()
        .filter_map(|file| {
            let (child, filename) = file.path.split_once('/')?;
            (filename == "SKILL.md" && is_skill_library_entry(child)).then(|| child.to_string())
        })
        .collect();
    children.sort();
    children.dedup();
    let mut skills = Vec::new();
    let mut skipped = Vec::new();
    for child in children {
        let skill_root = root.join(&child);
        if !skill_root.join("SKILL.md").is_file() {
            continue;
        }
        match read_skill_doc(resource, &skill_root, limits.max_selected_bytes) {
            Ok(doc) => skills.push((child, doc)),
            // `read_skill_doc` only produces this variant for SKILL.md parse
            // and validation failures; bounded reads, symlinks and path
            // safety return other variants and still fail the whole library.
            Err(ResourceError::InvalidSkill { message, .. }) => skipped.push(SkippedSkill {
                dir: child,
                reason: message,
            }),
            Err(error) => return Err(error),
        }
    }
    Ok(LibraryScan { skills, skipped })
}

/// Snapshot diagnostics: soft skill warnings plus isolated content faults,
/// both prefixed with the library member directory.
fn library_diagnostics(scan: &LibraryScan) -> Vec<String> {
    let mut diagnostics = Vec::new();
    for (dir, doc) in &scan.skills {
        for diagnostic in &doc.diagnostics {
            diagnostics.push(format!("{dir}: {}", diagnostic.message));
        }
    }
    for skipped in &scan.skipped {
        diagnostics.push(format!("{}: {}", skipped.dir, skipped.diagnostic()));
    }
    diagnostics
}

/// Catalog entries stay sorted by directory name. Skipped members are visible
/// with their reason but flagged, and `library_entry` keeps them unselectable.
fn skill_catalog(scan: &LibraryScan) -> Vec<serde_json::Value> {
    let mut entries: Vec<(&str, serde_json::Value)> = scan
        .skills
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
            (dir.as_str(), entry)
        })
        .collect();
    entries.extend(scan.skipped.iter().map(|skipped| {
        (
            skipped.dir.as_str(),
            json!({
                "name": skipped.dir,
                "skipped": true,
                "diagnostics": [SkillDiagnostic::warning(skipped.diagnostic())],
            }),
        )
    }));
    entries.sort_by_key(|(dir, _)| *dir);
    entries.into_iter().map(|(_, entry)| entry).collect()
}

/// Refuses an isolated member with its malformed reason before the ordinary
/// lookup; an absent key keeps the existing "no skill directory" refusal.
fn library_entry<'a>(
    resource: &str,
    scan: &'a LibraryScan,
    key: &str,
) -> Result<&'a SkillDoc, ResourceError> {
    if let Some(skipped) = scan.skipped.iter().find(|skipped| skipped.dir == key) {
        return Err(ResourceError::InvalidSkill {
            resource: resource.to_string(),
            message: format!(
                "cannot select skill_library entry `{key}`: {}",
                skipped.diagnostic()
            ),
        });
    }
    library_skill(resource, &scan.skills, key)
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

/// A validated skill document found in a resource.
///
/// `library_dir` is absent for a standalone `skill` and contains the member
/// directory for a `skill_library` resource. Library results are sorted by
/// directory name, matching loader discovery order.
#[derive(Debug, Clone)]
pub struct DiscoveredSkill {
    pub library_dir: Option<String>,
    pub doc: SkillDoc,
}

/// Loads the skill documents selected by a skill resource.
///
/// Takes an already package-resolved directory, parses the resource's
/// configured directory limits (no defaults added), preflights the same
/// bounded whole-tree hash the loaders run, then reads discovery documents
/// with bounded no-follow reads. Rejects unsupported resource kinds and
/// never verifies pins.
pub fn discover_skill_docs(
    name: &str,
    resource: &ResourceDef,
    root: &Utf8Path,
) -> Result<Vec<DiscoveredSkill>, ResourceError> {
    let is_library = match resource.kind {
        ResourceKind::Skill => false,
        ResourceKind::SkillLibrary => true,
        _ => {
            return Err(ResourceError::InvalidSkill {
                resource: name.to_string(),
                message: "resource type must be `skill` or `skill_library`".into(),
            });
        }
    };
    if !root.is_dir() {
        return Err(ResourceError::InvalidSkill {
            resource: name.to_string(),
            message: if is_library {
                "a skill_library resource must be a directory".into()
            } else {
                "a skill resource must be a directory containing SKILL.md".into()
            },
        });
    }
    let limits = directory_limits(name, resource)?;
    let (_, files) = hash_resource_dir(root, limits).map_err(|source| ResourceError::Read {
        path: root.to_path_buf(),
        source,
    })?;
    if is_library {
        let scan = scan_skill_library(name, root, limits, &files)?;
        Ok(scan
            .skills
            .into_iter()
            .map(|(dir, doc)| DiscoveredSkill {
                library_dir: Some(dir),
                doc,
            })
            .collect())
    } else {
        let doc = read_skill_doc(name, root, limits.max_selected_bytes)?;
        Ok(vec![DiscoveredSkill {
            library_dir: None,
            doc,
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;

    fn temp_dir(name: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("engine-skill-{name}-{}", uuid::Uuid::now_v7())),
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
        let (_, files) =
            hash_resource_dir(&root, DirectoryLimits::default()).expect("library should hash");
        let scan = scan_skill_library("skills", &root, DirectoryLimits::default(), &files)
            .expect("library should scan");
        let names: Vec<&str> = scan.skills.iter().map(|(dir, _)| dir.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta"]);
        assert!(scan.skipped.is_empty());
        let catalog = skill_catalog(&scan);
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
        let (_, files) =
            hash_resource_dir(&root, DirectoryLimits::default()).expect("library should hash");
        let scan = scan_skill_library("skills", &root, DirectoryLimits::default(), &files)
            .expect("library should scan");
        let doc = library_skill("skills", &scan.skills, "alpha").expect("alpha should resolve");
        assert_eq!(doc.description, "Alpha.");
        let error =
            library_skill("skills", &scan.skills, "missing").expect_err("unknown skill must fail");
        assert!(error.to_string().contains("missing"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    fn resource_def(kind: &str, params: serde_json::Value) -> ResourceDef {
        serde_json::from_value(json!({"type": kind, "params": params}))
            .expect("resource def should deserialize")
    }

    #[test]
    fn discover_standalone_skill_enforces_selected_bytes() {
        let root = temp_dir("discover-standalone");
        write_skill(
            &root,
            "demo",
            "---\nname: demo\ndescription: Demo.\n---\nBody.\n",
        );
        let skill_root = root.join("demo");
        let len = std::fs::read(skill_root.join("SKILL.md"))
            .expect("skill file should be readable")
            .len();
        let unbounded = resource_def("skill", json!({}));
        let found = discover_skill_docs("demo", &unbounded, &skill_root)
            .expect("no-limit discovery should succeed");
        assert_eq!(found.len(), 1);
        assert!(found[0].library_dir.is_none());
        assert_eq!(found[0].doc.name, "demo");
        let exact = resource_def("skill", json!({"max_selected_bytes": len}));
        let found = discover_skill_docs("demo", &exact, &skill_root)
            .expect("exact-bound discovery should succeed");
        assert!(found[0].library_dir.is_none());
        assert_eq!(found[0].doc.name, "demo");
        let tight = resource_def("skill", json!({"max_selected_bytes": len - 1}));
        let error = discover_skill_docs("demo", &tight, &skill_root)
            .expect_err("oversized SKILL.md must fail at max_selected_bytes");
        assert!(error.to_string().contains("max_bytes"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn discover_library_member_enforces_selected_bytes() {
        let root = temp_dir("discover-library-bytes");
        write_skill(
            &root,
            "alpha",
            "---\nname: alpha\ndescription: Alpha.\n---\nBody.\n",
        );
        let len = std::fs::read(root.join("alpha/SKILL.md"))
            .expect("skill file should be readable")
            .len();
        let tight = resource_def("skill_library", json!({"max_selected_bytes": len - 1}));
        let error = discover_skill_docs("lib", &tight, &root)
            .expect_err("oversized library member must fail at max_selected_bytes");
        assert!(error.to_string().contains("max_bytes"), "{error}");
        let exact = resource_def("skill_library", json!({"max_selected_bytes": len}));
        let found = discover_skill_docs("lib", &exact, &root)
            .expect("exact-bound library discovery should succeed");
        assert_eq!(
            found
                .iter()
                .map(|item| item.library_dir.as_deref().expect("library member"))
                .collect::<Vec<_>>(),
            vec!["alpha"]
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn discover_library_enumeration_honors_max_entries() {
        let root = temp_dir("discover-library-entries");
        write_skill(&root, "beta", "---\nname: beta\ndescription: Beta.\n---\n");
        write_skill(
            &root,
            "alpha",
            "---\nname: alpha\ndescription: Alpha.\n---\n",
        );
        std::fs::create_dir_all(root.join("empty")).expect("empty dir should be created");
        std::fs::write(root.join("notes.txt"), "not a skill").expect("note should be written");
        // Walk entries: alpha/, alpha/SKILL.md, beta/, beta/SKILL.md,
        // empty/, notes.txt (6 total); raw top-level entries: 4.
        let exact = resource_def("skill_library", json!({"max_entries": 6}));
        let found = discover_skill_docs("lib", &exact, &root)
            .expect("exact-bound discovery should pass and stay sorted");
        assert_eq!(
            found
                .iter()
                .map(|item| item.library_dir.as_deref().expect("library member"))
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        let tight = resource_def("skill_library", json!({"max_entries": 5}));
        let error = discover_skill_docs("lib", &tight, &root)
            .expect_err("empty and non-skill entries must count toward max_entries");
        assert!(error.to_string().contains("max_entries"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn discover_skill_docs_refuses_a_symlinked_skill_file() {
        let root = temp_dir("discover-symlink");
        let outside = temp_dir("discover-symlink-outside");
        write_skill(
            &outside,
            "real",
            "---\nname: demo\ndescription: Demo.\n---\nBody.\n",
        );
        let skill_root = root.join("demo");
        std::fs::create_dir_all(&skill_root).expect("skill directory should be created");
        std::os::unix::fs::symlink(outside.join("real/SKILL.md"), skill_root.join("SKILL.md"))
            .expect("symlink should be created");
        let unbounded = resource_def("skill", json!({}));
        let error = discover_skill_docs("demo", &unbounded, &skill_root)
            .expect_err("symlinked SKILL.md must be refused, never followed");
        assert!(error.to_string().contains("demo"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
        std::fs::remove_dir_all(outside).expect("temporary directory should be removed");
    }

    #[test]
    fn discover_skill_docs_rejects_unsupported_kinds() {
        let root = temp_dir("discover-kind");
        std::fs::create_dir_all(&root).expect("directory should be created");
        let resource = resource_def("dir", json!({}));
        let error = discover_skill_docs("docs", &resource, &root)
            .expect_err("unsupported kinds must be rejected");
        assert!(
            error.to_string().contains("skill_library") || error.to_string().contains("skill"),
            "{error}"
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn scan_skill_library_isolates_a_malformed_member_and_keeps_it_unselectable() {
        let root = temp_dir("malformed-member");
        write_skill(
            &root,
            "good",
            "---\nname: good\ndescription: Good.\n---\nBody.\n",
        );
        write_skill(&root, "broken", "plain markdown, no frontmatter\n");
        let (_, files) =
            hash_resource_dir(&root, DirectoryLimits::default()).expect("library should hash");
        let scan = scan_skill_library("lib", &root, DirectoryLimits::default(), &files)
            .expect("a malformed member must not fail the library scan");
        assert_eq!(
            scan.skills
                .iter()
                .map(|(dir, _)| dir.as_str())
                .collect::<Vec<_>>(),
            vec!["good"]
        );
        assert_eq!(scan.skipped.len(), 1);
        assert_eq!(scan.skipped[0].dir, "broken");
        assert!(
            scan.skipped[0].reason.contains("frontmatter"),
            "the parse reason must be retained: {}",
            scan.skipped[0].reason
        );

        let diagnostics = library_diagnostics(&scan);
        assert!(
            diagnostics
                .iter()
                .any(|item| item.starts_with("broken: ") && item.contains("frontmatter")),
            "snapshot diagnostics must name the skipped member: {diagnostics:?}"
        );
        let catalog = skill_catalog(&scan);
        assert_eq!(catalog.len(), 2);
        assert_eq!(catalog[0]["name"], "broken");
        assert_eq!(catalog[0]["skipped"].as_bool(), Some(true));
        assert!(
            catalog[0]["diagnostics"][0]["message"]
                .as_str()
                .expect("catalog diagnostic message")
                .contains("frontmatter")
        );
        assert_eq!(catalog[1]["name"], "good");

        let error = library_entry("lib", &scan, "broken")
            .expect_err("a skipped skill must refuse selection");
        assert!(
            error.to_string().contains("broken") && error.to_string().contains("frontmatter"),
            "the refusal must name the entry and its malformed reason: {error}"
        );
        assert!(library_entry("lib", &scan, "good").is_ok());
        let error =
            library_entry("lib", &scan, "missing").expect_err("absent keys keep their refusal");
        assert!(
            error.to_string().contains("no skill directory"),
            "unexpected refusal: {error}"
        );

        let resource = resource_def("skill_library", json!({}));
        let found = discover_skill_docs("lib", &resource, &root)
            .expect("discovery should succeed with one valid member");
        assert_eq!(
            found
                .iter()
                .map(|item| item.library_dir.as_deref().expect("library member"))
                .collect::<Vec<_>>(),
            vec!["good"]
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn scan_skill_library_keeps_oversized_malformed_files_hard() {
        let root = temp_dir("malformed-oversize");
        write_skill(&root, "broken", "plain markdown, no frontmatter\n");
        let len = std::fs::metadata(root.join("broken/SKILL.md"))
            .expect("skill file should be readable")
            .len() as usize;
        let (_, files) =
            hash_resource_dir(&root, DirectoryLimits::default()).expect("library should hash");
        let limits = DirectoryLimits {
            max_selected_bytes: Some(len - 1),
            ..DirectoryLimits::default()
        };
        let error = scan_skill_library("lib", &root, limits, &files)
            .expect_err("an oversized SKILL.md must fail even when its content is malformed");
        assert!(matches!(error, ResourceError::Read { .. }), "{error}");
        assert!(error.to_string().contains("max_bytes"), "{error}");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn discover_skill_docs_refuses_library_symlinks_and_unsupported_entries() {
        let symlink_root = temp_dir("library-symlink");
        std::fs::create_dir_all(&symlink_root).expect("library root should be created");
        std::os::unix::fs::symlink("nowhere", symlink_root.join("link"))
            .expect("symlink should be created");
        let resource = resource_def("skill_library", json!({}));
        let error = discover_skill_docs("lib", &resource, &symlink_root)
            .expect_err("a symlink in a library must stay a hard failure");
        assert!(error.to_string().contains("symbolic link"), "{error}");
        std::fs::remove_dir_all(symlink_root).expect("temporary directory should be removed");

        let fifo_root = temp_dir("library-fifo");
        std::fs::create_dir_all(&fifo_root).expect("library root should be created");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(fifo_root.join("pipe"))
                .status()
                .expect("mkfifo should run")
                .success()
        );
        let error = discover_skill_docs("lib", &resource, &fifo_root)
            .expect_err("an unsupported library entry must stay a hard failure");
        assert!(error.to_string().contains("unsupported"), "{error}");
        std::fs::remove_dir_all(fifo_root).expect("temporary directory should be removed");
    }
}
