//! Artifacts operations.
use super::*;

impl LocalService {
    pub async fn artifacts(&self, id: String) -> Result<OutputManifest, ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        read_output_manifest(&run_meta_dir(&run_dir)).map_err(api_internal)
    }

    pub async fn read_artifact(
        &self,
        id: String,
        path: String,
    ) -> Result<(OutputArtifact, Utf8PathBuf), ApiError> {
        let run_dir = self.run_dir_for(&id).await?;
        let manifest = read_output_manifest(&run_meta_dir(&run_dir)).map_err(api_internal)?;
        let artifact = manifest
            .artifacts
            .into_iter()
            .find(|artifact| artifact.path == path)
            .ok_or_else(|| api_not_found(format!("artifact `{path}` was not found")))?;
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
        let generator_path = read_run_generator_path(&run_dir).map_err(api_internal)?;
        let contract = Contract::load(&generator_path).map_err(api_internal)?;
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
