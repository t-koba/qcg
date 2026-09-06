use crate::{EngineError, HttpRequest, RunContext};
use qcg_contract::ResourceDef;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use super::content::{read_bytes_bounded, resource_trust_label, safe_resource_name};
use super::hash::resolve_resource_path_for_engine;
use super::types::{ResourceCacheStatus, ResourceError, ResourceSnapshot, ResourceSnapshotSource};

pub(crate) async fn snapshot_remote_or_local_resource(
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
            if limits.max_bytes.is_some_and(|limit| bytes.len() > limit) {
                return Err(EngineError::Failed(format!(
                    "resource `{name}` exceeds max_bytes ({})",
                    limits.max_bytes.unwrap_or(usize::MAX)
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

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct DirectoryLimits {
    pub(crate) max_files: Option<usize>,
    pub(crate) max_entries: Option<usize>,
    pub(crate) max_depth: Option<usize>,
    pub(crate) max_bytes: Option<u64>,
    pub(crate) max_selected_bytes: Option<usize>,
}

pub(crate) fn directory_limits(
    name: &str,
    resource: &ResourceDef,
) -> Result<DirectoryLimits, ResourceError> {
    let limits: DirectoryLimits = serde_json::from_value(Value::Object(resource.params.clone()))
        .map_err(|error| ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: error.to_string(),
        })?;
    if limits.max_files == Some(0)
        || limits.max_entries == Some(0)
        || limits.max_depth == Some(0)
        || limits.max_bytes == Some(0)
        || limits.max_selected_bytes == Some(0)
    {
        return Err(ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: "max_files, max_entries, max_depth, max_bytes, and max_selected_bytes must be greater than zero".into(),
        });
    }
    Ok(limits)
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct SingleResourceLimits {
    pub(crate) max_bytes: Option<usize>,
}

pub(crate) fn single_resource_limits(
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
    if limits.max_bytes == Some(0) {
        return Err(ResourceError::InvalidConfiguration {
            resource: name.to_string(),
            message: "max_bytes must be greater than zero".into(),
        });
    }
    Ok(limits)
}
