use crate::{EngineError, RunContext};
use async_trait::async_trait;
use qcg_contract::ResourceDef;

use super::content::{render_skill_resource, resource_trust_label};
use super::hash::{
    hash_resource_dir, hash_resource_file, resolve_resource_path, resolve_resource_path_for_engine,
};
use super::snapshot::directory_limits;
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
