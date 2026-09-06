use crate::{EngineError, RunContext};
use async_trait::async_trait;
use qcg_contract::ResourceDef;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::content::{
    read_to_string_bounded, resource_trust_label, safe_resource_name, select_openapi,
    snapshot_resource_path,
};
use super::hash::resolve_resource_path;
use super::skill::validate_resource_pin;
use super::snapshot::{single_resource_limits, snapshot_remote_or_local_resource};
use super::types::{
    ResourceCacheStatus, ResourceError, ResourceLoader, ResourceSelector, ResourceSnapshot,
    ResourceSnapshotSource,
};

pub(crate) struct RemoteResourceLoader {
    pub(crate) type_id: &'static str,
}

pub(crate) struct ExecResourceLoader;

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
        let max_bytes =
            params
                .max_bytes
                .or(context.contract.manifest.runtime.command_output_limit_bytes);
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
        let max_bytes =
            params
                .max_bytes
                .or(context.contract.manifest.runtime.command_output_limit_bytes);
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
