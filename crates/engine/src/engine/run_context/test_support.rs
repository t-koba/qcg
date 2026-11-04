use super::super::types::RunContext;

impl RunContext {
    /// Test-only context builder for downstream approval/resume tests
    /// (F02). Mirrors production construction (same gateways, same secret
    /// loading) with fresh cancellation and empty replay state. Only
    /// available with the `test-support` feature, never in production
    /// builds; downstream crates opt in through dev-dependencies.
    #[cfg(any(test, feature = "test-support"))]
    pub fn for_tests(
        run_id: String,
        workspace: camino::Utf8PathBuf,
        metadata: camino::Utf8PathBuf,
        contract: contract::Contract,
        confirmations: std::collections::BTreeMap<String, bool>,
        answers: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Result<Self, String> {
        use std::sync::{Arc, Mutex};
        let runtime = contract.manifest.runtime.clone();
        let fs = crate::FsGateway::new(workspace.clone(), &contract.manifest.permissions);
        let cmd = crate::CmdGateway::new(contract.manifest.permissions.clone(), workspace.clone());
        let http = crate::HttpGateway::new(
            contract.manifest.permissions.clone(),
            std::time::Duration::from_secs(runtime.http_timeout_seconds),
            runtime.http_body_limit_bytes,
            runtime.http_redirect_limit,
        )
        .map_err(|error| error.to_string())?;
        let secrets = crate::SecretStore::try_from_env(&contract.manifest.secrets)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            run_id,
            contract,
            workspace: workspace.clone(),
            metadata: metadata.clone(),
            fs,
            cmd,
            http,
            secrets,
            interactive: false,
            answers,
            confirmations,
            llm_provider: None,
            llm_seed_override: None,
            templates: crate::TemplateService,
            cancellation: tokio_util::sync::CancellationToken::new(),
            parallel_compatibility: policy::scheduling::ParallelCompatibility::default(),
            elapsed_deadline: None,
            snapshot_source: None,
            run_refs: Arc::new(std::collections::BTreeMap::new()),
            replay_through: 0,
            replayed_steps: Arc::new(std::collections::BTreeMap::new()),
            checkpoint_accounting: Arc::new(Mutex::new(
                crate::engine::checkpoint::CheckpointAccounting::default(),
            )),
            host_cgroup: None,
        })
    }
}
