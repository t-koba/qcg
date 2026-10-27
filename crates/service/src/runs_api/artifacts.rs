//! Artifacts operations.
use super::*;
impl LocalService {
    pub async fn artifacts(&self, id: String) -> Result<OutputManifest, ApiError> {
        Ok(self.bounded_manifest(&id, None).await?.0)
    }
    pub async fn read_artifact(
        &self,
        id: String,
        path: String,
    ) -> Result<(OutputArtifact, Utf8PathBuf), ApiError> {
        self.read_artifact_inner(id, path, None).await
    }
    async fn bounded_manifest(
        &self,
        id: &str,
        limits: Option<&crate::package::ArtifactZipLimits>,
    ) -> Result<(OutputManifest, Utf8PathBuf), ApiError> {
        let run_dir = self.run_dir_for(id).await?;
        let Some(limits) = limits else {
            return Ok((
                engine::read_output_manifest(&run_meta_dir(&run_dir)).map_err(api_internal)?,
                run_dir,
            ));
        };
        let eff = limits.effective();
        let capped = usize::try_from(eff.max_bytes.unwrap()).map_err(api_internal)?;
        let path = run_meta_dir(&run_dir).join("outputs.json");
        let file = std::fs::File::open(&path).map_err(api_internal)?;
        let staged = file.metadata().map_err(api_internal)?.len();
        if staged > capped as u64 {
            return Err(ApiError::TooLarge {
                actual_bytes: staged as usize,
                limit_bytes: capped,
            });
        }
        let raw = files::read_bounded(&path, Some(capped)).map_err(api_internal)?;
        let manifest: OutputManifest = serde_json::from_slice(&raw).map_err(api_internal)?;
        if manifest.artifacts.len() > eff.max_entries.unwrap() {
            return Err(ApiError::invalid_field(
                "artifacts",
                "artifact count exceeds limit",
            ));
        }
        Ok((manifest, run_dir))
    }
    pub async fn artifacts_json(
        &self,
        id: String,
        limits: &crate::package::ArtifactZipLimits,
    ) -> Result<Vec<u8>, ApiError> {
        let (manifest, _) = self.bounded_manifest(&id, Some(limits)).await?;
        let limit = usize::try_from(limits.effective().max_bytes.unwrap()).map_err(api_internal)?;
        let mut body = crate::artifacts::CountingWriter::new(Vec::new(), Some(limit as u64));
        serde_json::to_writer(&mut body, &manifest).map_err(api_internal)?;
        if body.exceeded {
            return Err(ApiError::TooLarge {
                actual_bytes: body.written as usize,
                limit_bytes: limit,
            });
        }
        Ok(body.inner)
    }
    pub async fn read_artifact_with_limits(
        &self,
        id: String,
        path: String,
        limits: &crate::package::ArtifactZipLimits,
    ) -> Result<(OutputArtifact, Utf8PathBuf), ApiError> {
        self.read_artifact_inner(id, path, Some(limits)).await
    }
    async fn read_artifact_inner(
        &self,
        id: String,
        path: String,
        limits: Option<&crate::package::ArtifactZipLimits>,
    ) -> Result<(OutputArtifact, Utf8PathBuf), ApiError> {
        let (manifest, run_dir) = self.bounded_manifest(&id, limits).await?;
        let artifact = manifest
            .artifacts
            .into_iter()
            .find(|artifact| artifact.path == path)
            .ok_or_else(|| api_not_found(format!("artifact `{path}` was not found")))?;
        if let Some(limit) = limits.and_then(|l| l.effective().max_bytes)
            && artifact.bytes > limit
        {
            return Err(ApiError::TooLarge {
                actual_bytes: artifact.bytes as usize,
                limit_bytes: limit as usize,
            });
        }
        let resolved = resolve_artifact_path(&run_workspace_dir(&run_dir), &artifact.path)
            .map_err(api_internal)?;
        Ok((artifact, resolved))
    }

    /// Opens the run journal for constant-memory streaming delivery. Limits
    /// come from the run contract, and a configured total bound is enforced
    /// against the file size before a single content byte is served, so no
    /// path allocates beyond its checked bound.
    pub async fn open_journal_stream(
        &self,
        id: String,
    ) -> Result<crate::types::JournalStream, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let state = crate::summaries::fold_run_state(&run_dir).map_err(api_internal)?;
        let contract = if state.package_sha256.is_some() {
            crate::run_package::load(&run_dir, &state).map_err(api_internal)?
        } else {
            // Read-only access to historical/direct journals is not recovery.
            let generator_path = read_run_generator_path(&run_dir).map_err(api_internal)?;
            Contract::load(&generator_path).map_err(api_internal)?
        };
        let limits = JournalLimits::from(&contract.manifest.runtime);
        let path = run_meta_dir(&run_dir).join("journal.jsonl");
        let file = tokio::fs::File::open(&path).await.map_err(api_internal)?;
        let len = file.metadata().await.map_err(api_internal)?.len();
        if let Some(limit) = limits.max_total_bytes
            && len > limit as u64
        {
            return Err(ApiError::TooLarge {
                actual_bytes: len as usize,
                limit_bytes: limit,
            });
        }
        let audit_path = path.with_file_name("audit.jsonl");
        let audit_limit =
            policy::AuditLimits::from_config(&contract.manifest.audit).max_total_bytes;
        let (audit, audit_len) = match tokio::fs::File::open(&audit_path).await {
            Ok(file) => {
                let len = file.metadata().await.map_err(api_internal)?.len();
                // Same rule as the durable stream: an oversized observation
                // stream is refused before delivery, never truncated into a
                // silently incomplete merged view.
                if let Some(limit) = audit_limit
                    && len > limit as u64
                {
                    return Err(ApiError::TooLarge {
                        actual_bytes: len as usize,
                        limit_bytes: limit,
                    });
                }
                (Some(file), len)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (None, 0),
            Err(error) => return Err(api_internal(error)),
        };
        Ok(crate::types::JournalStream {
            file,
            len,
            limit: limits.max_total_bytes,
            audit,
            audit_len,
            audit_limit,
        })
    }
}
