use crate::RunContext;
use camino::Utf8PathBuf;
use qcg_contract::{ResourceDef, parse_skill_doc};
use serde_json::{Value, json};

use super::hash::resolve_resource_path;
use super::types::{ResourceError, ResourceSelector};

pub(crate) fn render_skill_resource(
    context: &RunContext,
    resource_name: &str,
    resource: &ResourceDef,
    selector: Option<&ResourceSelector>,
    max_selected_bytes: Option<usize>,
) -> Result<String, ResourceError> {
    let path = resource
        .path
        .as_deref()
        .ok_or_else(|| ResourceError::MissingField {
            resource: resource_name.to_string(),
            field: "path or library ref",
        })?;
    let skill_root = resolve_resource_path(context, resource_name, path)?;
    let skill_path = if skill_root.is_dir() {
        resolve_resource_file(resource_name, &skill_root, "SKILL.md")?
    } else {
        skill_root.clone()
    };
    let source = read_to_string_bounded(&skill_path, max_selected_bytes)?;
    let skill = parse_skill_doc(&source);
    match selector {
        None => Ok(skill.instructions),
        Some(ResourceSelector::Named(name)) if name == "instructions" => Ok(skill.instructions),
        Some(ResourceSelector::Named(name)) if name == "meta" => {
            Ok(serde_json::to_string_pretty(&json!({
                "name": skill.name,
                "description": skill.description,
            }))?)
        }
        Some(ResourceSelector::File { path: rel }) => {
            let file_path = resolve_resource_file(resource_name, &skill_root, rel)?;
            read_to_string_bounded(&file_path, max_selected_bytes)
        }
        Some(other) => Err(ResourceError::UnsupportedNamedSelector {
            resource: resource_name.to_string(),
            selector: format!("{other:?}"),
        }),
    }
}

pub(crate) fn resolve_resource_file(
    resource: &str,
    root: &camino::Utf8Path,
    relative: &str,
) -> Result<Utf8PathBuf, ResourceError> {
    if relative.is_empty()
        || relative.contains('\\')
        || relative
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(ResourceError::UnsafeFileSelector {
            resource: resource.to_string(),
        });
    }
    let file = root.join(relative);
    let canonical_root = std::fs::canonicalize(root).map_err(|source| ResourceError::Read {
        path: root.to_path_buf(),
        source,
    })?;
    let canonical_file = std::fs::canonicalize(&file).map_err(|source| ResourceError::Read {
        path: file.clone(),
        source,
    })?;
    if !canonical_file.starts_with(&canonical_root) {
        return Err(ResourceError::FileEscapesRoot {
            resource: resource.to_string(),
        });
    }
    Utf8PathBuf::from_path_buf(canonical_file).map_err(|path| ResourceError::Read {
        path: file,
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("resource path is not UTF-8: {}", path.display()),
        ),
    })
}

pub(crate) fn select_openapi(
    resource_name: &str,
    text: &str,
    selector: Option<&ResourceSelector>,
) -> Result<String, ResourceError> {
    match selector {
        Some(ResourceSelector::Named(selector)) if selector == "paths" => {
            summarize_openapi_paths(resource_name, text)
        }
        Some(ResourceSelector::Operations { tag }) => {
            summarize_openapi_operations(resource_name, text, tag.as_deref())
        }
        Some(selector) => Err(ResourceError::UnsupportedNamedSelector {
            resource: resource_name.to_string(),
            selector: format!("{selector:?}"),
        }),
        None => Ok(text.to_string()),
    }
}

fn summarize_openapi_paths(resource_name: &str, text: &str) -> Result<String, ResourceError> {
    let value: Value = serde_json::from_str(text).map_err(|source| ResourceError::OpenApiJson {
        resource: resource_name.to_string(),
        source,
    })?;
    let paths = value
        .get("paths")
        .and_then(Value::as_object)
        .ok_or_else(|| ResourceError::OpenApiMissingPaths {
            resource: resource_name.to_string(),
        })?;
    let mut lines = Vec::new();
    for (path, item) in paths {
        let Some(methods) = item.as_object() else {
            continue;
        };
        for method in methods.keys() {
            if matches!(
                method.as_str(),
                "get" | "put" | "post" | "delete" | "patch" | "head" | "options" | "trace"
            ) {
                lines.push(format!("{} {}", method.to_ascii_uppercase(), path));
            }
        }
    }
    lines.sort();
    Ok(lines.join("\n"))
}

fn summarize_openapi_operations(
    resource_name: &str,
    text: &str,
    tag_filter: Option<&str>,
) -> Result<String, ResourceError> {
    let value: Value = serde_json::from_str(text).map_err(|source| ResourceError::OpenApiJson {
        resource: resource_name.to_string(),
        source,
    })?;
    let paths = value
        .get("paths")
        .and_then(Value::as_object)
        .ok_or_else(|| ResourceError::OpenApiMissingPaths {
            resource: resource_name.to_string(),
        })?;
    let mut operations = Vec::new();
    for (path, item) in paths {
        let Some(methods) = item.as_object() else {
            continue;
        };
        for (method, operation) in methods {
            if !matches!(
                method.as_str(),
                "get" | "put" | "post" | "delete" | "patch" | "head" | "options" | "trace"
            ) {
                continue;
            }
            if let Some(tag) = tag_filter {
                let has_tag = operation
                    .get("tags")
                    .and_then(Value::as_array)
                    .is_some_and(|tags| tags.iter().any(|item| item.as_str() == Some(tag)));
                if !has_tag {
                    continue;
                }
            }
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let summary = operation
                .get("summary")
                .and_then(Value::as_str)
                .unwrap_or("");
            operations.push(format!(
                "{} {} operationId={} {}",
                method.to_ascii_uppercase(),
                path,
                operation_id,
                summary
            ));
        }
    }
    operations.sort();
    Ok(operations.join("\n"))
}

pub(crate) fn snapshot_resource_path(context: &RunContext, resource_name: &str) -> Utf8PathBuf {
    context
        .metadata
        .join("resources")
        .join(format!("{}.snapshot", safe_resource_name(resource_name)))
}

pub(crate) fn read_bytes_bounded(
    path: &camino::Utf8Path,
    max_bytes: Option<usize>,
) -> Result<Vec<u8>, std::io::Error> {
    let file = std::fs::File::open(path)?;
    let Some(max_bytes) = max_bytes else {
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut std::io::BufReader::new(file), &mut bytes)?;
        return Ok(bytes);
    };
    let limit = u64::try_from(max_bytes)
        .map_err(|_| std::io::Error::other("resource byte limit does not fit u64"))?;
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
    let mut limited = std::io::Read::take(file, limit.saturating_add(1));
    std::io::Read::read_to_end(&mut limited, &mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(std::io::Error::other(format!(
            "resource `{path}` exceeds max_bytes ({max_bytes})"
        )));
    }
    Ok(bytes)
}

pub(crate) fn read_to_string_bounded(
    path: &camino::Utf8Path,
    max_bytes: Option<usize>,
) -> Result<String, ResourceError> {
    let bytes = read_bytes_bounded(path, max_bytes).map_err(|source| ResourceError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    String::from_utf8(bytes).map_err(|source| ResourceError::Read {
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })
}

pub(crate) fn resource_trust_label(trust: &qcg_contract::Trust) -> &'static str {
    match trust {
        qcg_contract::Trust::Trusted => "trusted",
        qcg_contract::Trust::Untrusted => "untrusted",
    }
}

pub(crate) fn safe_resource_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}
