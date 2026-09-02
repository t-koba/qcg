use crate::{EngineError, HttpRequest, RunContext};
use async_trait::async_trait;
use camino::Utf8PathBuf;
use qcg_contract::{ResourceDef, ResourceKind, parse_skill_doc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
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
    Command { command: Vec<String> },
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
trait ResourceLoader: Send + Sync {
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
    #[error("resource `{resource}` file selector is not safe")]
    UnsafeFileSelector { resource: String },
    #[error("resource `{resource}` file escapes its root")]
    FileEscapesRoot { resource: String },
    #[error("resource `{resource}` path is invalid: {source}")]
    PackagePath {
        resource: String,
        source: qcg_contract::PackagePathError,
    },
    #[error("resource `{resource}` has invalid configuration: {message}")]
    InvalidConfiguration { resource: String, message: String },
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

static FILE_RESOURCE_LOADER: FileResourceLoader = FileResourceLoader;
static DIR_RESOURCE_LOADER: DirResourceLoader = DirResourceLoader;
static SKILL_RESOURCE_LOADER: SkillResourceLoader = SkillResourceLoader;
static URL_RESOURCE_LOADER: RemoteResourceLoader = RemoteResourceLoader { type_id: "url" };
static OPENAPI_RESOURCE_LOADER: RemoteResourceLoader = RemoteResourceLoader { type_id: "openapi" };
static EXEC_RESOURCE_LOADER: ExecResourceLoader = ExecResourceLoader;

fn resource_loader(kind: ResourceKind) -> &'static dyn ResourceLoader {
    match kind {
        ResourceKind::File => &FILE_RESOURCE_LOADER,
        ResourceKind::Dir => &DIR_RESOURCE_LOADER,
        ResourceKind::Skill => &SKILL_RESOURCE_LOADER,
        ResourceKind::Url => &URL_RESOURCE_LOADER,
        ResourceKind::Openapi => &OPENAPI_RESOURCE_LOADER,
        ResourceKind::Exec => &EXEC_RESOURCE_LOADER,
    }
}

pub fn select_resource(
    context: &RunContext,
    name: &str,
    resource: &ResourceDef,
    selector: Option<&ResourceSelector>,
) -> Result<String, ResourceError> {
    if let Some(path) = resource.path.as_deref() {
        resolve_resource_path(context, name, path)?;
    }
    resource_loader(resource.kind).select(context, name, resource, selector)
}

pub async fn collect_resource_hashes(
    context: &RunContext,
) -> Result<Vec<ResourceSnapshot>, EngineError> {
    let mut resources = Vec::new();
    for (name, resource) in &context.contract.manifest.resources {
        if let Some(path) = resource.path.as_deref() {
            resolve_resource_path_for_engine(context, name, path)?;
        }
        resources.push(
            resource_loader(resource.kind)
                .snapshot(context, name, resource)
                .await?,
        );
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
        let full_path = resolve_resource_path_for_engine(context, name, path)?;
        let limits = single_resource_limits(name, resource)
            .map_err(|error| EngineError::Failed(error.to_string()))?;
        let max_bytes = u64::try_from(limits.max_bytes).map_err(|_| {
            EngineError::Failed(format!("resource `{name}` max_bytes is too large"))
        })?;
        let (sha256, bytes) = hash_resource_file(&full_path, max_bytes)?;
        validate_resource_pin(name, resource, &sha256)?;
        Ok(ResourceSnapshot {
            name: name.to_string(),
            resource_type: self.type_id().into(),
            source: ResourceSnapshotSource::Path {
                path: path.clone().into(),
            },
            snapshot: None,
            sha256,
            bytes,
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
        let full_path = resolve_resource_path(context, name, path)?;
        let limits = single_resource_limits(name, resource)?;
        read_to_string_bounded(&full_path, limits.max_bytes)
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
        let full_path = resolve_resource_path_for_engine(context, name, path)?;
        let limits = directory_limits(name, resource)
            .map_err(|error| EngineError::Failed(error.to_string()))?;
        let (sha256, files) = hash_resource_dir(&full_path, limits)?;
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
        match selector {
            None => {
                let (sha256, files) =
                    hash_resource_dir(&root, limits).map_err(|source| ResourceError::Read {
                        path: root.clone(),
                        source,
                    })?;
                Ok(serde_json::to_string_pretty(&json!({
                    "sha256": sha256,
                    "files": files,
                }))?)
            }
            Some(ResourceSelector::Named(selector))
                if selector == "tree" || selector == "files" =>
            {
                let (sha256, files) =
                    hash_resource_dir(&root, limits).map_err(|source| ResourceError::Read {
                        path: root.clone(),
                        source,
                    })?;
                Ok(serde_json::to_string_pretty(&json!({
                    "sha256": sha256,
                    "files": files,
                }))?)
            }
            Some(ResourceSelector::File { path }) => {
                hash_resource_dir(&root, limits).map_err(|source| ResourceError::Read {
                    path: root.clone(),
                    source,
                })?;
                let file = resolve_resource_file(name, &root, path)?;
                read_to_string_bounded(&file, limits.max_selected_bytes)
            }
            Some(ResourceSelector::Named(selector)) => {
                Err(ResourceError::UnsupportedNamedSelector {
                    resource: name.to_string(),
                    selector: selector.clone(),
                })
            }
            Some(_) => Err(ResourceError::UnsupportedSelector {
                resource: name.to_string(),
            }),
        }
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
        let path = resource.path.as_deref().ok_or_else(|| {
            EngineError::Failed(format!("skill resource `{name}` requires `path`"))
        })?;
        let full_path = resolve_resource_path_for_engine(context, name, path)?;
        let limits = directory_limits(name, resource)
            .map_err(|error| EngineError::Failed(error.to_string()))?;
        let (sha256, files, bytes) = if full_path.is_dir() {
            let (sha256, files) = hash_resource_dir(&full_path, limits)?;
            let bytes = files.iter().map(|file| file.bytes).sum();
            (sha256, files, bytes)
        } else {
            let (sha256, bytes) = hash_resource_file(&full_path, limits.max_bytes)?;
            (sha256, Vec::new(), bytes)
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
        let limits = directory_limits(name, resource)?;
        if let Some(path) = resource.path.as_deref() {
            let root = resolve_resource_path(context, name, path)?;
            if root.is_dir() {
                hash_resource_dir(&root, limits)
                    .map_err(|source| ResourceError::Read { path: root, source })?;
            }
        }
        render_skill_resource(context, name, resource, selector, limits.max_selected_bytes)
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

struct ExecResourceLoader;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecResourceParams {
    command: Vec<String>,
    #[serde(default)]
    max_bytes: Option<usize>,
}

#[async_trait]
impl ResourceLoader for ExecResourceLoader {
    fn type_id(&self) -> &'static str {
        "exec"
    }

    async fn snapshot(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
    ) -> Result<ResourceSnapshot, EngineError> {
        let params = exec_resource_params(name, resource)
            .map_err(|error| EngineError::Failed(error.to_string()))?;
        let max_bytes = params
            .max_bytes
            .unwrap_or(context.contract.manifest.runtime.command_output_limit_bytes);
        let output = context
            .cmd
            .run_with_limits(
                &params.command,
                context.contract.manifest.runtime.command_timeout_seconds,
                max_bytes,
            )
            .await?;
        if output.status != 0 {
            return Err(EngineError::Failed(format!(
                "resource `{name}` command exited with {}",
                output.status
            )));
        }
        let bytes = output.stdout_bytes;
        std::str::from_utf8(&bytes).map_err(|_| {
            EngineError::Failed(format!("resource `{name}` command output must be UTF-8"))
        })?;
        let snapshot_dir = context.metadata.join("resources");
        tokio::fs::create_dir_all(&snapshot_dir).await?;
        let snapshot_path = snapshot_dir.join(format!("{}.snapshot", safe_resource_name(name)));
        tokio::fs::write(&snapshot_path, &bytes).await?;
        let sha256 = hex::encode(Sha256::digest(&bytes));
        validate_resource_pin(name, resource, &sha256)?;
        Ok(ResourceSnapshot {
            name: name.to_string(),
            resource_type: self.type_id().into(),
            source: ResourceSnapshotSource::Command {
                command: params.command,
            },
            snapshot: Some(snapshot_path),
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
        let params = exec_resource_params(name, resource)?;
        let max_bytes = params
            .max_bytes
            .unwrap_or(context.contract.manifest.runtime.command_output_limit_bytes);
        read_to_string_bounded(&snapshot_resource_path(context, name), max_bytes)
    }
}

fn exec_resource_params(
    name: &str,
    resource: &ResourceDef,
) -> Result<ExecResourceParams, ResourceError> {
    let params: ExecResourceParams = serde_json::from_value(Value::Object(resource.params.clone()))
        .map_err(|error| ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: error.to_string(),
        })?;
    if params.command.is_empty() || params.command.iter().any(String::is_empty) {
        return Err(ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: "command must contain non-empty strings".into(),
        });
    }
    if params.max_bytes == Some(0) {
        return Err(ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: "max_bytes must be greater than zero".into(),
        });
    }
    Ok(params)
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
            let limits = single_resource_limits(name, resource)?;
            read_to_string_bounded(
                &resolve_resource_path(context, name, path)?,
                limits.max_bytes,
            )?
        } else {
            let limits = single_resource_limits(name, resource)?;
            read_to_string_bounded(&snapshot_resource_path(context, name), limits.max_bytes)?
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
    let limits = single_resource_limits(name, resource)
        .map_err(|error| EngineError::Failed(error.to_string()))?;
    let (bytes, source, cache) = if let Some(path) = &resource.path {
        (
            read_bytes_bounded(
                &resolve_resource_path_for_engine(context, name, path)?,
                limits.max_bytes,
            )?,
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
                read_bytes_bounded(&snapshot_path, limits.max_bytes)?,
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
                    sensitive_query: BTreeMap::new(),
                    body: None,
                    follow_redirects: true,
                })
                .await?;
            let bytes = response.body;
            if bytes.len() > limits.max_bytes {
                return Err(EngineError::Failed(format!(
                    "resource `{name}` exceeds max_bytes ({})",
                    limits.max_bytes
                )));
            }
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

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DirectoryLimits {
    max_files: usize,
    max_entries: usize,
    max_depth: usize,
    max_bytes: u64,
    max_selected_bytes: usize,
}

const MAX_RESOURCE_FILES: usize = 1_000_000;
const MAX_RESOURCE_ENTRIES: usize = 1_000_000;
const MAX_RESOURCE_DEPTH: usize = 256;
const MAX_RESOURCE_BYTES: u64 = 1024 * 1024 * 1024;

impl Default for DirectoryLimits {
    fn default() -> Self {
        Self {
            max_files: 100_000,
            max_entries: 100_000,
            max_depth: 64,
            max_bytes: 1024 * 1024 * 1024,
            max_selected_bytes: 16 * 1024 * 1024,
        }
    }
}

fn directory_limits(name: &str, resource: &ResourceDef) -> Result<DirectoryLimits, ResourceError> {
    let limits: DirectoryLimits = serde_json::from_value(Value::Object(resource.params.clone()))
        .map_err(|error| ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: error.to_string(),
        })?;
    if limits.max_files == 0
        || limits.max_entries == 0
        || limits.max_depth == 0
        || limits.max_bytes == 0
        || limits.max_selected_bytes == 0
    {
        return Err(ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: "max_files, max_entries, max_depth, max_bytes, and max_selected_bytes must be greater than zero".into(),
        });
    }
    if limits.max_files > MAX_RESOURCE_FILES
        || limits.max_entries > MAX_RESOURCE_ENTRIES
        || limits.max_depth > MAX_RESOURCE_DEPTH
        || limits.max_bytes > MAX_RESOURCE_BYTES
        || limits.max_selected_bytes > MAX_RESOURCE_BYTES as usize
    {
        return Err(ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: format!(
                "directory limits must not exceed max_files={MAX_RESOURCE_FILES}, max_entries={MAX_RESOURCE_ENTRIES}, max_depth={MAX_RESOURCE_DEPTH}, max_bytes={MAX_RESOURCE_BYTES}, or max_selected_bytes={MAX_RESOURCE_BYTES}"
            ),
        });
    }
    Ok(limits)
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SingleResourceLimits {
    max_bytes: usize,
}

impl Default for SingleResourceLimits {
    fn default() -> Self {
        Self {
            max_bytes: default_single_resource_max_bytes(),
        }
    }
}

fn default_single_resource_max_bytes() -> usize {
    16 * 1024 * 1024
}

fn single_resource_limits(
    name: &str,
    resource: &ResourceDef,
) -> Result<SingleResourceLimits, ResourceError> {
    let limits: SingleResourceLimits =
        serde_json::from_value(Value::Object(resource.params.clone())).map_err(|error| {
            ResourceError::InvalidConfiguration {
                resource: name.to_string(),
                message: error.to_string(),
            }
        })?;
    if limits.max_bytes == 0 {
        return Err(ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: "max_bytes must be greater than zero".into(),
        });
    }
    if limits.max_bytes > MAX_RESOURCE_BYTES as usize {
        return Err(ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: format!("max_bytes must not exceed {MAX_RESOURCE_BYTES}"),
        });
    }
    Ok(limits)
}

fn hash_resource_file(
    path: &camino::Utf8Path,
    max_bytes: u64,
) -> Result<(String, usize), std::io::Error> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let bytes = std::io::copy(
        &mut std::io::Read::take(&mut file, max_bytes.saturating_add(1)),
        &mut DigestWriter(&mut digest),
    )?;
    if bytes > max_bytes {
        return Err(std::io::Error::other(format!(
            "resource file `{path}` exceeds max_bytes ({max_bytes})"
        )));
    }
    let bytes = usize::try_from(bytes)
        .map_err(|_| std::io::Error::other(format!("resource file `{path}` is too large")))?;
    Ok((hex::encode(digest.finalize()), bytes))
}

fn hash_resource_dir(
    path: &camino::Utf8Path,
    limits: DirectoryLimits,
) -> Result<(String, Vec<ResourceFileSnapshot>), std::io::Error> {
    let mut files = Vec::new();
    let mut total_bytes = 0_u64;
    let mut entries = 0_usize;
    for entry in WalkDir::new(path).follow_links(false).min_depth(1) {
        let entry = entry.map_err(std::io::Error::other)?;
        if entry.depth() > limits.max_depth {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` exceeds max_depth ({})",
                limits.max_depth
            )));
        }
        entries = entries
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("resource directory entry count overflowed"))?;
        if entries > limits.max_entries {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` exceeds max_entries ({})",
                limits.max_entries
            )));
        }
        if entry.file_type().is_symlink() {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` contains a symbolic link"
            )));
        }
        if !entry.file_type().is_file() && !entry.file_type().is_dir() {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` contains an unsupported entry"
            )));
        }
        if !entry.file_type().is_file() {
            continue;
        }
        if files.len() >= limits.max_files {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` exceeds max_files ({})",
                limits.max_files
            )));
        }
        let file_path = Utf8PathBuf::from_path_buf(entry.path().to_path_buf()).map_err(|path| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("resource path is not UTF-8: {}", path.display()),
            )
        })?;
        let relative = file_path
            .strip_prefix(path)
            .map_err(std::io::Error::other)?;
        let rel = relative
            .components()
            .map(|component| component.as_str())
            .collect::<Vec<_>>()
            .join("/");
        let file = std::fs::File::open(&file_path)?;
        let mut digest = Sha256::new();
        let remaining = limits.max_bytes.saturating_sub(total_bytes);
        let bytes = std::io::copy(
            &mut std::io::Read::take(file, remaining.saturating_add(1)),
            &mut DigestWriter(&mut digest),
        )?;
        if bytes > remaining {
            return Err(std::io::Error::other(format!(
                "resource directory `{path}` exceeds max_bytes ({})",
                limits.max_bytes
            )));
        }
        total_bytes = total_bytes.saturating_add(bytes);
        files.push(ResourceFileSnapshot {
            path: rel,
            sha256: hex::encode(digest.finalize()),
            bytes: usize::try_from(bytes).map_err(|_| {
                std::io::Error::other(format!("resource file `{file_path}` is too large"))
            })?,
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

struct DigestWriter<'a>(&'a mut Sha256);

impl std::io::Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn resolve_resource_path(
    context: &RunContext,
    resource: &str,
    path: &str,
) -> Result<Utf8PathBuf, ResourceError> {
    context
        .contract
        .resolve_package_path(path)
        .map_err(|source| ResourceError::PackagePath {
            resource: resource.to_string(),
            source,
        })
}

fn resolve_resource_path_for_engine(
    context: &RunContext,
    resource: &str,
    path: &str,
) -> Result<Utf8PathBuf, EngineError> {
    context
        .contract
        .resolve_package_path(path)
        .map_err(|error| {
            EngineError::Failed(format!("resource `{resource}` path is invalid: {error}"))
        })
}

fn render_skill_resource(
    context: &RunContext,
    resource_name: &str,
    resource: &ResourceDef,
    selector: Option<&ResourceSelector>,
    max_selected_bytes: usize,
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

fn resolve_resource_file(
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

fn read_bytes_bounded(
    path: &camino::Utf8Path,
    max_bytes: usize,
) -> Result<Vec<u8>, std::io::Error> {
    let file = std::fs::File::open(path)?;
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

fn read_to_string_bounded(
    path: &camino::Utf8Path,
    max_bytes: usize,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_directory(name: &str) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            std::env::temp_dir().join(format!("qcg-resource-{name}-{}", uuid::Uuid::now_v7())),
        )
        .expect("temporary path should be UTF-8")
    }

    #[test]
    fn directory_hash_is_streamed_sorted_and_bounded() {
        let root = test_directory("limits");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("nested")).expect("directory should be created");
        std::fs::write(root.join("z.txt"), "z").expect("file should be written");
        std::fs::write(root.join("nested/a.txt"), "abc").expect("file should be written");

        let (_, files) = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_files: 2,
                max_bytes: 4,
                ..DirectoryLimits::default()
            },
        )
        .expect("directory within limits should hash");
        assert_eq!(
            files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["nested/a.txt", "z.txt"]
        );

        let files_error = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_files: 1,
                max_bytes: 4,
                ..DirectoryLimits::default()
            },
        )
        .expect_err("file limit should be enforced");
        assert!(files_error.to_string().contains("max_files"));
        let bytes_error = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_files: 2,
                max_bytes: 3,
                ..DirectoryLimits::default()
            },
        )
        .expect_err("byte limit should be enforced");
        assert!(bytes_error.to_string().contains("max_bytes"));
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn resource_configuration_rejects_limits_above_hard_ceiling() {
        let directory: ResourceDef = serde_json::from_value(serde_json::json!({
            "type": "dir",
            "params": { "max_files": MAX_RESOURCE_FILES + 1 }
        }))
        .expect("directory resource should deserialize");
        let error =
            directory_limits("docs", &directory).expect_err("excessive directory limit must fail");
        assert!(error.to_string().contains("must not exceed"));

        let file: ResourceDef = serde_json::from_value(serde_json::json!({
            "type": "file",
            "params": { "max_bytes": MAX_RESOURCE_BYTES + 1 }
        }))
        .expect("file resource should deserialize");
        let error =
            single_resource_limits("document", &file).expect_err("excessive file limit must fail");
        assert!(error.to_string().contains("max_bytes must not exceed"));
    }

    #[test]
    fn directory_limits_count_empty_directories_and_bound_depth() {
        let root = test_directory("entry-depth");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("nested/empty"))
            .expect("nested empty directory should be created");
        let entry_error = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_entries: 1,
                ..DirectoryLimits::default()
            },
        )
        .expect_err("empty directories must count toward max_entries");
        assert!(entry_error.to_string().contains("max_entries"));
        let depth_error = hash_resource_dir(
            &root,
            DirectoryLimits {
                max_depth: 1,
                ..DirectoryLimits::default()
            },
        )
        .expect_err("nested directories must respect max_depth");
        assert!(depth_error.to_string().contains("max_depth"));
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn resource_file_selector_rejects_parent_traversal() {
        let root = test_directory("selector");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("directory should be created");
        let error = resolve_resource_file("docs", &root, "../secret.txt")
            .expect_err("parent traversal should be rejected");
        assert!(matches!(error, ResourceError::UnsafeFileSelector { .. }));
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[test]
    fn resource_text_reads_stop_at_the_configured_bound() {
        let root = test_directory("read-bound");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("directory should be created");
        let path = root.join("large.txt");
        std::fs::write(&path, "12345").expect("file should be written");
        let error = read_to_string_bounded(&path, 4).expect_err("oversized read should fail");
        assert!(error.to_string().contains("max_bytes"));
        let hash_error = hash_resource_file(&path, 4)
            .expect_err("oversized files should stop hashing at the configured bound");
        assert!(hash_error.to_string().contains("max_bytes"));
        assert_eq!(
            read_to_string_bounded(&path, 5).expect("bounded read should succeed"),
            "12345"
        );
        assert_eq!(
            hash_resource_file(&path, 5)
                .expect("bounded file hash should succeed")
                .1,
            5
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }
}
