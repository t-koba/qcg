//! `run_ref` resource loader: materializes a hash-pinned copy of another
//! run's declared artifact into this run's workspace.
//!
//! Resolution (which run, which declared artifact, the byte bound) happens
//! before the engine starts and is passed in as [`crate::RunRefMaterial`].
//! The loader itself is the mechanism: it verifies the bytes against the
//! pinned digest, writes them once to a deterministic workspace path, and
//! fails closed when an existing copy (a resume) no longer matches.

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{ResourceDef, ResourceKind};
use sha2::Digest as _;

use super::content::resource_trust_label;
use super::types::{
    ResourceCacheStatus, ResourceError, ResourceLoader, ResourceSelector, ResourceSnapshot,
    ResourceSnapshotSource,
};
use crate::{EngineError, RunContext};

pub(crate) struct RunRefResourceLoader;

#[async_trait]
impl ResourceLoader for RunRefResourceLoader {
    fn type_id(&self) -> &'static str {
        "run_ref"
    }

    async fn snapshot(
        &self,
        context: &RunContext,
        name: &str,
        resource: &ResourceDef,
    ) -> Result<ResourceSnapshot, EngineError> {
        if resource.kind != ResourceKind::RunRef {
            return Err(EngineError::Failed(format!(
                "resource `{name}` is not a run_ref resource"
            )));
        }
        let material = context.run_refs.get(name).ok_or_else(|| {
            EngineError::Failed(format!(
                "run reference `{name}` was not resolved; the source run or its declared artifact is unavailable"
            ))
        })?;
        // The provided bytes must match the digest resolution verified, so a
        // mismatched hand-off is refused before anything reaches the
        // workspace.
        let digest = hex::encode(sha2::Sha256::digest(&material.bytes));
        if digest != material.sha256 {
            return Err(EngineError::Failed(format!(
                "run reference `{name}` payload does not match its pinned revision (expected {}, got {digest})",
                material.sha256
            )));
        }
        let relative = run_ref_destination(name, &material.artifact);
        let destination = context
            .fs
            .resolve_write(relative.as_str())
            .map_err(|error| EngineError::Failed(format!("run reference `{name}`: {error}")))?;
        match std::fs::read(&destination) {
            // Resume: the pinned copy must still be the exact revision.
            Ok(existing) if existing == material.bytes => {}
            Ok(existing) => {
                let existing_digest = hex::encode(sha2::Sha256::digest(&existing));
                return Err(EngineError::Failed(format!(
                    "run reference `{name}` workspace copy does not match its pinned revision (expected {}, got {existing_digest})",
                    material.sha256
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                context
                    .fs
                    .write_file_atomic(&destination, &material.bytes)
                    .await
                    .map_err(|error| {
                        EngineError::Failed(format!(
                            "run reference `{name}` could not be written: {error}"
                        ))
                    })?;
            }
            Err(error) => {
                return Err(EngineError::Failed(format!(
                    "run reference `{name}` workspace copy is unreadable: {error}"
                )));
            }
        }
        Ok(ResourceSnapshot {
            name: name.to_string(),
            resource_type: self.type_id().into(),
            source: ResourceSnapshotSource::RunRef {
                run_id: material.source_run_id.clone(),
                artifact: material.artifact.clone(),
            },
            snapshot: Some(destination),
            sha256: material.sha256.clone(),
            bytes: material.bytes.len(),
            files: Vec::new(),
            cache: ResourceCacheStatus::Local,
            pin_sha256: resource.pin_sha256.clone(),
            trust: resource_trust_label(&resource.trust).into(),
            llm_visible: resource.llm_visible,
            diagnostics: Vec::new(),
        })
    }

    fn select(
        &self,
        _context: &RunContext,
        name: &str,
        _resource: &ResourceDef,
        _selector: Option<&ResourceSelector>,
    ) -> Result<String, ResourceError> {
        Err(ResourceError::UnsupportedSelector {
            resource: name.to_string(),
        })
    }
}

/// Workspace-relative path a `run_ref` resource materializes at.
pub(crate) fn run_ref_destination(name: &str, artifact: &str) -> Utf8PathBuf {
    let file_name = Utf8Path::new(artifact)
        .file_name()
        .filter(|name| !name.is_empty())
        .unwrap_or("artifact");
    Utf8PathBuf::from(format!("run-refs/{name}/{file_name}"))
}
