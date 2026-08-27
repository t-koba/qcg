use crate::{EngineError, HttpRequest, RunContext};
use async_trait::async_trait;
use camino::Utf8PathBuf;
use qcg_contract::{Contract, ResourceDef, parse_skill_doc};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use walkdir::WalkDir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceSelector {
    Named(String),
    Operations { tag: Option<String> },
    File { path: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceSnapshot {
    pub name: String,
    #[serde(rename = "type")]
    pub resource_type: String,
    pub source: ResourceSnapshotSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<Utf8PathBuf>,
    pub sha256: String,
    pub bytes: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<ResourceFileSnapshot>,
    pub cache: ResourceCacheStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pin_sha256: Option<String>,
    pub trust: String,
    pub llm_visible: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResourceSnapshotSource {
    Path { path: Utf8PathBuf },
    Url { url: String, final_url: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceFileSnapshot {
    pub path: String,
    pub sha256: String,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceCacheStatus {
    NotApplicable,
    Local,
    Hit,
    Miss,
}

#[async_trait]
pub trait ResourceLoader: Send + Sync {
    fn type_id(&self) -> &'static str;

    async fn snapshot(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
    ) -> Result<ResourceSnapshot, EngineError>;

    fn select(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
        selector: Option<&ResourceSelector>,
    ) -> Result<String, ResourceError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    #[error("resource `{resource}` does not support selectors")]
    UnsupportedSelector { resource: String },
    #[error("resource `{resource}` does not support selector `{selector}`")]
    UnsupportedNamedSelector { resource: String, selector: String },
    #[error("resource `{resource}` requires {field}")]
    MissingField {
        resource: String,
        field: &'static str,
    },
    #[error("resource `{resource}` type is not supported in LLM context")]
    UnsupportedContextType { resource: String },
    #[error("skill resource `{resource}` file selector is not safe")]
    UnsafeSkillFileSelector { resource: String },
    #[error("skill resource `{resource}` file escapes skill root")]
    SkillFileEscapesRoot { resource: String },
    #[error("failed to read resource `{path}`: {source}")]
    Read {
        path: Utf8PathBuf,
        source: std::io::Error,
    },
    #[error("invalid OpenAPI JSON for resource `{resource}`: {source}")]
    OpenApiJson {
        resource: String,
        source: serde_json::Error,
    },
    #[error("OpenAPI resource `{resource}` has no paths object")]
    OpenApiMissingPaths { resource: String },
    #[error("unsupported OpenAPI operations selector `{selector}` for resource `{resource}`")]
    UnsupportedOpenApiOperationsSelector { resource: String, selector: String },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Default)]
pub struct ResourceRegistry {
    loaders: BTreeMap<&'static str, Arc<dyn ResourceLoader>>,
}

impl ResourceRegistry {
    pub fn with_builtins() -> Self {
        let mut registry = Self::default();
        registry.register(FileResourceLoader);
        registry.register(DirResourceLoader);
        registry.register(SkillResourceLoader);
        registry.register(RemoteResourceLoader::new("url"));
        registry.register(RemoteResourceLoader::new("openapi"));
        registry
    }

    pub fn register<L: ResourceLoader + 'static>(&mut self, loader: L) {
        self.loaders.insert(loader.type_id(), Arc::new(loader));
    }

    pub fn get(&self, resource: &ResourceDef) -> Option<Arc<dyn ResourceLoader>> {
        self.loaders.get(resource.kind.as_str()).cloned()
    }

    pub fn select(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
        selector: Option<&ResourceSelector>,
    ) -> Result<String, ResourceError> {
        let Some(loader) = self.get(resource) else {
            return Err(ResourceError::UnsupportedContextType {
                resource: name.to_string(),
            });
        };
        loader.select(context, name, resource, selector)
    }
}

pub async fn collect_resource_hashes(
    context: &RunContext,
) -> Result<Vec<ResourceSnapshot>, EngineError> {
    let registry = ResourceRegistry::with_builtins();
    let mut resources = Vec::new();
    for (name, resource) in &context.contract.manifest.resources {
        let Some(loader) = registry.get(resource) else {
            return Err(EngineError::Failed(format!(
                "resource `{name}` type `{}` is not registered",
                resource.kind
            )));
        };
        resources.push(loader.snapshot(context, name, resource).await?);
    }
    Ok(resources)
}

struct FileResourceLoader;

#[async_trait]
impl ResourceLoader for FileResourceLoader {
    fn type_id(&self) -> &'static str {
        "file"
    }

    async fn snapshot(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
    ) -> Result<ResourceSnapshot, EngineError> {
        let path = resource
            .path
            .as_ref()
            .ok_or_else(|| EngineError::Failed(format!("file resource `{name}` requires path")))?;
        let full_path = context.contract.root.join(path);
        let bytes = std::fs::read(&full_path)?;
        let sha256 = hex::encode(Sha256::digest(&bytes));
        validate_resource_pin(name, resource, &sha256)?;
        Ok(ResourceSnapshot {
            name: name.to_string(),
            resource_type: self.type_id().into(),
            source: ResourceSnapshotSource::Path {
                path: path.clone().into(),
            },
            snapshot: None,
            sha256,
            bytes: bytes.len(),
            files: Vec::new(),
            cache: ResourceCacheStatus::NotApplicable,
            pin_sha256: resource.pin_sha256.clone(),
            trust: resource_trust_label(&resource.trust).into(),
            llm_visible: resource.llm_visible,
        })
    }

    fn select(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
        selector: Option<&ResourceSelector>,
    ) -> Result<String, ResourceError> {
        if selector.is_some() {
            return Err(ResourceError::UnsupportedSelector {
                resource: name.to_string(),
            });
        }
        let path = resource
            .path
            .as_deref()
            .ok_or_else(|| ResourceError::MissingField {
                resource: name.to_string(),
                field: "path",
            })?;
        let full_path = context.contract.root.join(path);
        read_to_string(&full_path)
    }
}

struct DirResourceLoader;

#[async_trait]
impl ResourceLoader for DirResourceLoader {
    fn type_id(&self) -> &'static str {
        "dir"
    }

    async fn snapshot(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
    ) -> Result<ResourceSnapshot, EngineError> {
        let path = resource.path.as_ref().ok_or_else(|| {
            EngineError::Failed(format!("directory resource `{name}` requires path"))
        })?;
        let full_path = context.contract.root.join(path);
        let (sha256, files) = hash_resource_dir(&full_path)?;
        validate_resource_pin(name, resource, &sha256)?;
        let bytes = files.iter().map(|file| file.bytes).sum();
        Ok(ResourceSnapshot {
            name: name.to_string(),
            resource_type: self.type_id().into(),
            source: ResourceSnapshotSource::Path {
                path: path.clone().into(),
            },
            snapshot: None,
            sha256,
            bytes,
            files,
            cache: ResourceCacheStatus::NotApplicable,
            pin_sha256: resource.pin_sha256.clone(),
            trust: resource_trust_label(&resource.trust).into(),
            llm_visible: resource.llm_visible,
        })
    }

    fn select(
        &self,
        _context: &RunContext,
        name: &str,
        _resource: &ResourceDef,
        _selector: Option<&ResourceSelector>,
    ) -> Result<String, ResourceError> {
        Err(ResourceError::UnsupportedContextType {
            resource: name.to_string(),
        })
    }
}

struct SkillResourceLoader;

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
        let full_path = resolve_skill_hash_root(&context.contract, resource).ok_or_else(|| {
            EngineError::Failed(format!("skill resource `{name}` requires `path`"))
        })?;
        let (sha256, files, bytes) = if full_path.is_dir() {
            let (sha256, files) = hash_resource_dir(&full_path)?;
            let bytes = files.iter().map(|file| file.bytes).sum();
            (sha256, files, bytes)
        } else {
            let bytes = std::fs::read(&full_path)?;
            (hex::encode(Sha256::digest(&bytes)), Vec::new(), bytes.len())
        };
        validate_resource_pin(name, resource, &sha256)?;
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
        })
    }

    fn select(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
        selector: Option<&ResourceSelector>,
    ) -> Result<String, ResourceError> {
        render_skill_resource(context, name, resource, selector)
    }
}

fn validate_resource_pin(
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

struct RemoteResourceLoader {
    type_id: &'static str,
}

impl RemoteResourceLoader {
    fn new(type_id: &'static str) -> Self {
        Self { type_id }
    }
}

#[async_trait]
impl ResourceLoader for RemoteResourceLoader {
    fn type_id(&self) -> &'static str {
        self.type_id
    }

    async fn snapshot(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
    ) -> Result<ResourceSnapshot, EngineError> {
        snapshot_remote_or_local_resource(context, name, self.type_id(), resource).await
    }

    fn select(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
        selector: Option<&ResourceSelector>,
    ) -> Result<String, ResourceError> {
        let text = if let Some(path) = &resource.path {
            read_to_string(&context.contract.root.join(path))?
        } else {
            read_to_string(&snapshot_resource_path(context, name))?
        };
        if self.type_id() == "openapi" {
            select_openapi(name, &text, selector)
        } else if selector.is_some() {
            Err(ResourceError::UnsupportedSelector {
                resource: name.to_string(),
            })
        } else {
            Ok(text)
        }
    }
}

async fn snapshot_remote_or_local_resource(
    context: &RunContext,
    name: &str,
    resource_type: &str,
    resource: &ResourceDef,
) -> Result<ResourceSnapshot, EngineError> {
    let snapshot_dir = context.metadata.join("resources");
    tokio::fs::create_dir_all(&snapshot_dir).await?;
    let snapshot_path = snapshot_dir.join(format!("{}.snapshot", safe_resource_name(name)));
    let (bytes, source, cache) = if let Some(path) = &resource.path {
        (
            std::fs::read(context.contract.root.join(path))?,
            ResourceSnapshotSource::Path {
                path: path.clone().into(),
            },
            ResourceCacheStatus::Local,
        )
    } else if let Some(url) = &resource.url {
        let cache_is_fresh = resource
            .cache_ttl_seconds
            .map(|ttl| cached_snapshot_is_fresh(&snapshot_path, ttl))
            .transpose()?
            .unwrap_or(false);
        if cache_is_fresh {
            (
                tokio::fs::read(&snapshot_path).await?,
                ResourceSnapshotSource::Url {
                    url: url.clone(),
                    final_url: url.clone(),
                },
                ResourceCacheStatus::Hit,
            )
        } else {
            let response = context
                .http
                .request(HttpRequest {
                    method: "GET".into(),
                    url: url.clone(),
                    headers: BTreeMap::new(),
                    body: None,
                })
                .await?;
            let bytes = response.body.into_bytes();
            tokio::fs::write(&snapshot_path, &bytes).await?;
            (
                bytes,
                ResourceSnapshotSource::Url {
                    url: url.clone(),
                    final_url: response.url,
                },
                ResourceCacheStatus::Miss,
            )
        }
    } else {
        return Err(EngineError::Failed(format!(
            "resource `{name}` requires path or url"
        )));
    };
    if !snapshot_path.exists() {
        tokio::fs::write(&snapshot_path, &bytes).await?;
    }
    let sha256 = hex::encode(Sha256::digest(&bytes));
    if let Some(pin_sha256) = &resource.pin_sha256
        && pin_sha256 != &sha256
    {
        return Err(EngineError::Failed(format!(
            "resource `{name}` sha256 pin mismatch: expected {pin_sha256}, got {sha256}"
        )));
    }
    Ok(ResourceSnapshot {
        name: name.to_string(),
        resource_type: resource_type.to_string(),
        source,
        snapshot: Some(snapshot_path),
        sha256,
        bytes: bytes.len(),
        files: Vec::new(),
        cache,
        pin_sha256: resource.pin_sha256.clone(),
        trust: resource_trust_label(&resource.trust).into(),
        llm_visible: resource.llm_visible,
    })
}

fn cached_snapshot_is_fresh(
    path: &camino::Utf8Path,
    ttl_seconds: u64,
) -> Result<bool, std::io::Error> {
    let modified = path.metadata()?.modified()?;
    let age = modified
        .elapsed()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(age.as_secs() <= ttl_seconds)
}

fn hash_resource_dir(
    path: &camino::Utf8Path,
) -> Result<(String, Vec<ResourceFileSnapshot>), std::io::Error> {
    let mut files = Vec::new();
    for entry in WalkDir::new(path).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let file_path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).map_err(|path| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("resource path is not UTF-8: {}", path.display()),
            )
        })?;
        let rel = file_path
            .strip_prefix(path)
            .map_err(std::io::Error::other)?
            .to_string();
        let bytes = std::fs::read(&file_path)?;
        files.push(ResourceFileSnapshot {
            path: rel,
            sha256: hex::encode(Sha256::digest(&bytes)),
            bytes: bytes.len(),
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let mut digest = Sha256::new();
    for file in &files {
        digest.update(file.path.as_bytes());
        digest.update([0]);
        digest.update(file.sha256.as_bytes());
        digest.update([0]);
        digest.update(file.bytes.to_string().as_bytes());
        digest.update([0]);
    }
    Ok((hex::encode(digest.finalize()), files))
}

fn resolve_skill_hash_root(contract: &Contract, resource: &ResourceDef) -> Option<Utf8PathBuf> {
    let path = resource.path.as_ref()?;
    Some(contract.root.join(path))
}

fn render_skill_resource(
    context: &RunContext,
    resource_name: &str,
    resource: &ResourceDef,
    selector: Option<&ResourceSelector>,
) -> Result<String, ResourceError> {
    let skill_root = resolve_skill_hash_root(&context.contract, resource).ok_or_else(|| {
        ResourceError::MissingField {
            resource: resource_name.to_string(),
            field: "path or library ref",
        }
    })?;
    let skill_path = if skill_root.is_dir() {
        skill_root.join("SKILL.md")
    } else {
        skill_root.clone()
    };
    let source = read_to_string(&skill_path)?;
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
            if rel.is_empty() || rel.contains('\\') || rel.split('/').any(|part| part == "..") {
                return Err(ResourceError::UnsafeSkillFileSelector {
                    resource: resource_name.to_string(),
                });
            }
            let file_path = skill_root.join(rel);
            if !file_path.starts_with(&skill_root) {
                return Err(ResourceError::SkillFileEscapesRoot {
                    resource: resource_name.to_string(),
                });
            }
            read_to_string(&file_path)
        }
        Some(other) => Err(ResourceError::UnsupportedNamedSelector {
            resource: resource_name.to_string(),
            selector: format!("{other:?}"),
        }),
    }
}

fn select_openapi(
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

fn snapshot_resource_path(context: &RunContext, resource_name: &str) -> Utf8PathBuf {
    context
        .metadata
        .join("resources")
        .join(format!("{}.snapshot", safe_resource_name(resource_name)))
}

fn read_to_string(path: &camino::Utf8Path) -> Result<String, ResourceError> {
    std::fs::read_to_string(path).map_err(|source| ResourceError::Read {
        path: path.to_path_buf(),
        source,
    })
}

fn resource_trust_label(trust: &qcg_contract::Trust) -> &'static str {
    match trust {
        qcg_contract::Trust::Trusted => "trusted",
        qcg_contract::Trust::Untrusted => "untrusted",
    }
}

fn safe_resource_name(name: &str) -> String {
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
