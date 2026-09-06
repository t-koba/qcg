use crate::{EngineError, RunContext};
use async_trait::async_trait;
use qcg_contract::ResourceDef;
use serde_json::json;

use super::content::{read_to_string_bounded, resolve_resource_file, resource_trust_label};
use super::hash::{
    hash_resource_dir, hash_resource_file, resolve_resource_path, resolve_resource_path_for_engine,
};
use super::skill::validate_resource_pin;
use super::snapshot::{directory_limits, single_resource_limits};
use super::types::{
    ResourceCacheStatus, ResourceError, ResourceLoader, ResourceSelector, ResourceSnapshot,
    ResourceSnapshotSource,
};

pub(crate) struct FileResourceLoader;

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
        let max_bytes = limits
            .max_bytes
            .map(|limit| {
                u64::try_from(limit).map_err(|_| {
                    EngineError::Failed(format!("resource `{name}` max_bytes is too large"))
                })
            })
            .transpose()?;
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

pub(crate) struct DirResourceLoader;

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
