use camino::Utf8PathBuf;
use service::{LocalService, RunStoreMode};
use std::collections::{BTreeMap, BTreeSet};
use tokio_util::sync::CancellationToken;

use super::idempotency::IdempotencyEntry;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub generators_dir: Utf8PathBuf,
    /// Explicit providers registry passed to the service. When set, this
    /// path is authoritative and no registry fallback is attempted.
    pub providers_path: Option<Utf8PathBuf>,
    /// Additional read-only generator roots searched after
    /// `generators_dir` (for example the bundled `share/<product>/generators`).
    /// The first root containing an id wins.
    pub extra_generators_dirs: Vec<Utf8PathBuf>,
    pub runs_dir: Utf8PathBuf,
    pub max_active_runs: usize,
    pub max_tracked_runs: usize,
    pub run_store_mode: RunStoreMode,
    pub cors_origins: Vec<String>,
    /// Optional bearer token. When omitted, the selected listener is unauthenticated.
    pub api_token: Option<String>,
    /// Explicit override. Omitted means `policy::DEFAULT_MAX_REQUEST_BYTES`
    /// (Axum's 2 MiB secure default), never unlimited.
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

/// Effective HTTP request body limit: the explicit flag when set,
/// otherwise `policy::DEFAULT_MAX_REQUEST_BYTES`. Omitted never means
/// unlimited: the default restores Axum's 2 MiB secure default.
pub(crate) fn effective_max_request_bytes(flag: Option<usize>) -> usize {
    flag.unwrap_or(policy::DEFAULT_MAX_REQUEST_BYTES)
}

/// Effective service step ceiling: `MAX_TOTAL_STEPS` overrides nothing
/// by itself (the flag carries the env binding); zero is rejected at boot
/// instead of silently blocking every run.
pub(crate) fn effective_max_total_steps(flag: Option<usize>) -> Result<Option<usize>, String> {
    match flag {
        Some(0) => Err("MAX_TOTAL_STEPS must be greater than zero".into()),
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_request_bytes_flag_falls_back_to_default() {
        assert_eq!(
            effective_max_request_bytes(None),
            policy::DEFAULT_MAX_REQUEST_BYTES
        );
        assert_eq!(effective_max_request_bytes(Some(1024)), 1024);
    }

    #[test]
    fn max_total_steps_flag_rejects_zero() {
        assert_eq!(
            effective_max_total_steps(Some(0)),
            Err("MAX_TOTAL_STEPS must be greater than zero".into())
        );
        assert_eq!(effective_max_total_steps(None), Ok(None));
        assert_eq!(effective_max_total_steps(Some(50)), Ok(Some(50)));
    }
}

#[derive(Debug)]
pub(crate) struct AppState {
    pub(crate) service: LocalService,
    pub(crate) runs_dir: Utf8PathBuf,
    pub(crate) oauth_origin: Option<String>,
    pub(crate) oauth_allowed_origins: BTreeSet<String>,
    pub(crate) oauth_callback_url: Option<String>,
    pub(crate) idempotency: tokio::sync::Mutex<BTreeMap<String, IdempotencyEntry>>,
    /// Frozen idempotency policy resolved once at boot. Every request and
    /// prune path uses these values, never re-reads the environment, so a
    /// concurrent environment change cannot make one request judge records
    /// by two standards and boot drift is impossible (E04).
    pub(crate) idempotency_ttl: std::time::Duration,
    pub(crate) idempotency_max_entries: usize,
    pub(crate) api_token_digest: Option<[u8; 32]>,
    pub(crate) artifact_limits: service::ArtifactZipLimits,
    pub(crate) asset_limit: Option<usize>,
    /// Configured request body override surfaced by /healthz. `None`
    /// means the documented default (`policy::DEFAULT_MAX_REQUEST_BYTES`);
    /// the effective bound is never unlimited.
    pub(crate) max_request_bytes: Option<usize>,
    /// Frozen metrics cardinality policy resolved once at boot. Every
    /// scrape uses these values, never re-reads the environment (E04).
    pub(crate) metrics_policy: super::middleware::MetricsPolicy,
    /// Set when graceful shutdown starts: new mutating requests are
    /// rejected, resident tasks stop, and SSE streams close (E05).
    pub(crate) shutdown: CancellationToken,
}
