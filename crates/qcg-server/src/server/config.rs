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
}

#[derive(Debug)]
pub(crate) struct AppState {
    pub(crate) service: LocalQcgService,
    pub(crate) oauth_origin: Option<String>,
    pub(crate) oauth_allowed_origins: BTreeSet<String>,
    pub(crate) oauth_callback_url: Option<String>,
    pub(crate) idempotency: tokio::sync::Mutex<BTreeMap<String, IdempotencyEntry>>,
    pub(crate) api_token_digest: Option<[u8; 32]>,
    pub(crate) artifact_limits: qcg_service::ArtifactZipLimits,
    pub(crate) asset_limit: Option<usize>,
}
