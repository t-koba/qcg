use crate::{EngineError, RunContext};
use async_trait::async_trait;
use camino::Utf8PathBuf;
use qcg_contract::{ResourceDef, ResourceKind};
use serde::Serialize;

use super::file_loaders::{DirResourceLoader, FileResourceLoader};
use super::hash::{resolve_resource_path, resolve_resource_path_for_engine};
use super::remote_exec::{ExecResourceLoader, RemoteResourceLoader};
use super::skill::SkillResourceLoader;

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
pub(crate) trait ResourceLoader: Send + Sync {
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
