use camino::Utf8PathBuf;
use qcg_service::{LocalQcgService, RunStoreMode};
use std::collections::{BTreeMap, BTreeSet};

use super::idempotency::IdempotencyEntry;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub generators_dir: Utf8PathBuf,
    /// Explicit providers registry passed to the service. When set, this
    /// path is authoritative and no registry fallback is attempted.
    pub providers_path: Option<Utf8PathBuf>,
    /// Additional read-only generator roots searched after
    /// `generators_dir` (for example the bundled `share/qcg/generators`).
    /// The first root containing an id wins.
    pub extra_generators_dirs: Vec<Utf8PathBuf>,
    pub runs_dir: Utf8PathBuf,
    pub max_active_runs: usize,
    pub max_tracked_runs: usize,
    pub run_store_mode: RunStoreMode,
    pub cors_origins: Vec<String>,
    /// Optional bearer token. When omitted, the selected listener is unauthenticated.
    pub api_token: Option<String>,
    /// Explicit max only. Omitted means no mechanistic limit.
    pub max_request_bytes: Option<usize>,
    /// Explicit max only. Omitted means no mechanistic limit.
    pub max_artifact_bytes: Option<u64>,
    /// Explicit max only. Omitted means no mechanistic limit.
    pub max_artifact_entries: Option<usize>,
    /// Explicit max only. Omitted means no mechanistic limit.
    pub max_asset_bytes: Option<usize>,
    /// Deployment ceiling for per-run total steps. Omitted means the
    /// engine default. Set to cap every run below its contract budget.
    pub max_total_steps: Option<usize>,
}

/// Effective service step ceiling: `QCG_MAX_TOTAL_STEPS` overrides nothing
/// by itself (the flag carries the env binding); zero is rejected at boot
/// instead of silently blocking every run.
pub(crate) fn effective_max_total_steps(flag: Option<usize>) -> Result<Option<usize>, String> {
    match flag {
        Some(0) => Err("QCG_MAX_TOTAL_STEPS must be greater than zero".into()),
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_total_steps_flag_rejects_zero() {
        assert_eq!(
            effective_max_total_steps(Some(0)),
            Err("QCG_MAX_TOTAL_STEPS must be greater than zero".into())
        );
        assert_eq!(effective_max_total_steps(None), Ok(None));
        assert_eq!(effective_max_total_steps(Some(50)), Ok(Some(50)));
    }
}

#[derive(Debug)]
pub(crate) struct AppState {
    pub(crate) service: LocalQcgService,
    pub(crate) runs_dir: Utf8PathBuf,
    pub(crate) oauth_origin: Option<String>,
    pub(crate) oauth_allowed_origins: BTreeSet<String>,
    pub(crate) oauth_callback_url: Option<String>,
    pub(crate) idempotency: tokio::sync::Mutex<BTreeMap<String, IdempotencyEntry>>,
    pub(crate) api_token_digest: Option<[u8; 32]>,
    pub(crate) artifact_limits: qcg_service::ArtifactZipLimits,
    pub(crate) asset_limit: Option<usize>,
    /// Effective request body limit surfaced by /healthz. None means no
    /// mechanistic limit.
    pub(crate) max_request_bytes: Option<usize>,
}
